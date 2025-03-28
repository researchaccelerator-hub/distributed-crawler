package telegramhelper

import (
	"fmt"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/researchaccelerator-hub/telegram-scraper/model"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/rs/zerolog/log"
	"github.com/zelenin/go-tdlib/client"
	"runtime/debug"
	"strings"
	"time"
)

func FetchChannelMessages(tdlibClient crawler.TDLibClient, chatID int64, page *state.Page, minPostDate time.Time, maxPosts int) ([]*client.Message, error) {
	log.Info().Msgf("Fetching messages for channel %s since %s", page.URL, minPostDate.Format("2006-01-02 15:04:05"))
	var allMessages []*client.Message
	var fromMessageId int64 = 0 // Start from the latest message
	var oldestMessageId int64 = 0

	// Convert minPostDate to Unix timestamp for comparison
	minPostUnix := minPostDate.Unix()

	for {
		log.Info().Msgf("Fetching message batch for channel %s starting from ID %d at depth: %v", page.URL, fromMessageId, page.Depth)
		chatHistory, err := tdlibClient.GetChatHistory(&client.GetChatHistoryRequest{
			ChatId:        chatID,
			FromMessageId: fromMessageId,
			Limit:         100, // Fetch up to 100 messages at a time
		})
		if err != nil {
			log.Error().Err(err).Stack().Msgf("Failed to get chat history for channel: %v", page.URL)
			return nil, err
		}

		// If no messages are returned, break
		if len(chatHistory.Messages) == 0 {
			break
		}

		// Check messages and add only those newer than minPostDate
		reachedOldMessages := false
		for _, msg := range chatHistory.Messages {
			// Compare message timestamp with minPostDate
			if int64(msg.Date) < minPostUnix {
				log.Info().Msgf("Reached messages older than minimum date (message date: %v, min date: %v)",
					time.Unix(int64(msg.Date), 0).Format("2006-01-02 15:04:05"),
					minPostDate.Format("2006-01-02 15:04:05"))
				reachedOldMessages = true
				break
			}
			allMessages = append(allMessages, msg)
			if maxPosts > -1 && len(allMessages) == maxPosts {
				reachedOldMessages = true
				break
			}
		}

		// If we've reached messages older than minPostDate, break out of the loop
		if reachedOldMessages {
			break
		}

		// Get the ID of the oldest message in this batch
		lastMessageId := chatHistory.Messages[len(chatHistory.Messages)-1].Id

		// If we're seeing the same oldest message ID as before, we've reached the end
		if lastMessageId == oldestMessageId {
			break
		}

		// Save the oldest message ID for comparison in the next iteration
		oldestMessageId = lastMessageId

		// Set the next fromMessageId to fetch the next batch
		fromMessageId = lastMessageId
	}

	log.Info().Msgf("Fetched a total of %d messages for channel %s since %s",
		len(allMessages), page.URL, minPostDate.Format("2006-01-02 15:04:05"))
	return allMessages, nil
}

func GetChannelMemberCount(tdlibClient crawler.TDLibClient, channelUsername string) (int, error) {
	// First, resolve the username to get the chat ID
	chat, err := tdlibClient.SearchPublicChat(&client.SearchPublicChatRequest{
		Username: channelUsername,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to resolve channel username: %w", err)
	}

	// Get full chat info based on the chat type
	chatType := chat.Type
	var memberCount int

	switch v := chatType.(type) {
	case *client.ChatTypeSupergroup:
		// For channels and supergroups
		supergroupId := v.SupergroupId

		// Get supergroup full info
		fullInfo, err := tdlibClient.GetSupergroupFullInfo(&client.GetSupergroupFullInfoRequest{
			SupergroupId: supergroupId,
		})
		if err != nil {
			return 0, fmt.Errorf("failed to get supergroup info: %w", err)
		}
		memberCount = int(fullInfo.MemberCount)

	case *client.ChatTypeBasicGroup:
		// For basic groups
		basicGroupId := v.BasicGroupId

		// Get basic group full info
		fullInfo, err := tdlibClient.GetBasicGroupFullInfo(&client.GetBasicGroupFullInfoRequest{
			BasicGroupId: basicGroupId,
		})
		if err != nil {
			return 0, fmt.Errorf("failed to get basic group info: %w", err)
		}
		memberCount = len(fullInfo.Members)

	default:
		return 0, fmt.Errorf("chat is not a group or channel")
	}

	return memberCount, nil
}

// GetMessageCount retrieves the total number of messages in a specified chat.
// It uses the provided tdlibClient to fetch chat history in batches of up to 100 messages,
// starting from the latest message. The function continues fetching until no more messages
// are returned, and returns the total count of messages or an error if the retrieval fails.
//
// Parameters:
//
//	tdlibClient - a pointer to the TDLib client used for making API requests.
//	chatID - the unique identifier of the chat for which the message count is to be retrieved.
//
// Returns:
//
//	An integer representing the total number of messages in the chat, or an error if the
//	operation fails.
func GetMessageCount(tdlibClient crawler.TDLibClient, messages []*client.Message, chatID int64, channelname string) (int, error) {
	log.Info().Msgf("Getting message count for channel %s", channelname)

	return len(messages), nil
}

// GetViewCount retrieves the view count from a given message's InteractionInfo.
// If InteractionInfo is nil, it returns 0 as the default view count.
func GetViewCount(message *client.Message, channelname string) int {
	log.Info().Msgf("Getting message view count for channel %s", channelname)
	if message.InteractionInfo != nil {
		return int(message.InteractionInfo.ViewCount)
	}
	return 0 // Default to 0 if InteractionInfo is nil
}

// GetMessageShareCount retrieves the share count of a specific message in a chat.
// It uses the provided tdlibClient to fetch message details from Telegram.
// If the message's InteractionInfo is available, it returns the ForwardCount as the share count.
// If InteractionInfo is nil or an error occurs, it returns 0 and an error, respectively.
func GetMessageShareCount(tdlibClient crawler.TDLibClient, chatID, messageID int64, channelname string) (int, error) {
	// Fetch the message details
	log.Info().Msgf("Getting message share count for channel %s", channelname)
	message, err := tdlibClient.GetMessage(&client.GetMessageRequest{
		ChatId:    chatID,
		MessageId: messageID,
	})
	if err != nil {
		return 0, err
	}

	// Check if InteractionInfo is available
	if message.InteractionInfo != nil {
		return int(message.InteractionInfo.ForwardCount), nil
	}

	// If InteractionInfo is nil, return 0
	return 0, nil
}

// GetTotalChannelViews calculates the total number of views for all messages in a specified Telegram channel.
// It iteratively fetches message batches from the channel's history using the provided tdlibClient,
// aggregates the view counts, and returns the total views as an integer.
// Parameters:
//
//	tdlibClient - a pointer to the tdlib client used to interact with the Telegram API.
//	channelID - the unique identifier of the Telegram channel.
//
// Returns:
//
//	An integer representing the total number of views across all messages in the channel, or an error if the operation fails.
func GetTotalChannelViews(tdlibClient crawler.TDLibClient, messages []*client.Message, channelID int64, channelname string) (int, error) {
	var totalViews int64
	log.Info().Msgf("Getting total views for channel %s", channelname)
	for _, m := range messages {

		if m.InteractionInfo != nil {
			totalViews += int64(m.InteractionInfo.ViewCount)
		}

	}

	log.Info().Msgf("Total views for channel %s: %d", channelname, totalViews)
	return int(totalViews), nil
}

// GetMessageComments retrieves comments from a message thread in a Telegram chat.
//
// Parameters:
// - tdlibClient: A pointer to the TDLib client used to interact with Telegram.
// - chatID: The ID of the chat containing the message.
// - messageID: The ID of the message whose comments are to be fetched.
//
// Returns:
// - A slice of Comment structs representing the comments in the message thread.
// - An error if the operation fails.
//
// The function fetches comments in batches of up to 100 and continues until no more comments are available.
// It extracts the text, reactions, view count, and reply count for each comment.
func GetMessageComments(tdlibClient crawler.TDLibClient, chatID, messageID int64, channelname string, maxcomments int) ([]model.Comment, error) {
	if tdlibClient == nil {
		return nil, fmt.Errorf("tdlibClient is nil")
	}

	slog.Debug("Starting GetMessageComments", "channel", channelname, "chatID", chatID, "messageID", messageID)

	// --- Determine the correct Chat ID for fetching comments ---
	var threadChatID int64 = chatID // Default to the provided chatID
	var foundLinkedChat bool = false

	// 1. Get info about the original chat
	originalChat, err := tdlibClient.GetChat(&client.GetChatRequest{
		ChatId: chatID,
	})
	if err != nil {
		// Log the error but proceed cautiously, maybe chatID is already the correct one or accessible anyway
		slog.Warn("Failed to get original chat info, proceeding with provided chatID", "channel", channelname, "chatID", chatID, "error", err)
	} else if originalChat != nil && originalChat.Type != nil {
		// 2. Check if the chat is a channel type
		if chatTypeChannel, ok := originalChat.Type.(*client.ChatTypeChannel); ok && chatTypeChannel != nil {
			// 3. It's a channel. We need its SupergroupId to get full info.
			supergroupId := chatTypeChannel.SupergroupId
			if supergroupId != 0 {
				slog.Debug("Original chat is a channel, attempting to get SupergroupFullInfo", "channel", channelname, "chatID", chatID, "supergroupID", supergroupId)
				// 4. Fetch the full info for the supergroup associated with the channel
				supergroupFullInfo, err := tdlibClient.GetSupergroupFullInfo(&client.GetSupergroupFullInfoRequest{
					SupergroupId: supergroupId,
				})

				if err != nil {
					slog.Warn("Failed to get supergroup full info for channel", "channel", channelname, "chatID", chatID, "supergroupID", supergroupId, "error", err)
				} else if supergroupFullInfo != nil && supergroupFullInfo.LinkedChatId != 0 {
					// 5. Found the linked chat ID! Use this for fetching comments.
					threadChatID = supergroupFullInfo.LinkedChatId
					foundLinkedChat = true
					slog.Info("Found linked discussion group via SupergroupFullInfo", "channel", channelname, "originalChatID", chatID, "linkedChatID", threadChatID)

					// OPTIONAL BUT RECOMMENDED: Ensure the client has access to the linked chat
					_, err = tdlibClient.GetChat(&client.GetChatRequest{ChatId: threadChatID})
					if err != nil {
						slog.Error("Cannot access the determined linked discussion group", "channel", channelname, "linkedChatID", threadChatID, "error", err)
						// Returning error here prevents attempting to fetch from an inaccessible chat
						return nil, fmt.Errorf("cannot access linked discussion group %d for channel %s: %w", threadChatID, channelname, err)
					} else {
						slog.Debug("Successfully accessed linked discussion group info", "linkedChatID", threadChatID)
					}
				} else {
					slog.Debug("SupergroupFullInfo obtained but no LinkedChatId found", "channel", channelname, "supergroupID", supergroupId)
				}
			} else {
				slog.Debug("Channel type found but SupergroupId is 0", "channel", channelname, "chatID", chatID)
			}
		} else {
			slog.Debug("Original chat is not a Channel type", "channel", channelname, "chatID", chatID, "type", originalChat.Type.ChatTypeType())
			// You could add handling here if it's *already* a Supergroup and might be the discussion group
		}
	}

	if !foundLinkedChat {
		slog.Info("Proceeding using the initially provided chat ID (not a channel, no linked group found/accessible, or error getting info)", "channel", channelname, "effectiveChatID", threadChatID)
		// Optional: You might still want to check access to the original chatID here if you didn't above
		// _, err = tdlibClient.GetChat(&client.GetChatRequest{ChatId: threadChatID})
		// if err != nil {
		// 	slog.Error("Cannot access the provided chat ID", "channel", channelname, "chatID", threadChatID, "error", err)
		// 	return nil, fmt.Errorf("cannot access provided chat %d for channel %s: %w", threadChatID, channelname, err)
		// }
	}
	// --- End of determining chat ID ---

	// Fetch the comments in the thread
	comments := make([]model.Comment, 0)
	var fromMessageId int64 = 0 // Start from the beginning of the thread (TDLib interprets 0 as the latest)
	done := false
	const historyLimit = 100 // How many messages to fetch per request

	slog.Debug("Starting comment pagination loop", "channel", channelname, "effectiveChatID", threadChatID, "messageID", messageID)

	for !done {
		var threadHistory *client.Messages
		var historyErr error

		slog.Debug("Requesting message thread history batch", "effectiveChatID", threadChatID, "messageID", messageID, "fromMessageId", fromMessageId, "limit", historyLimit)

		// --- Fetch Batch of Comments ---
		threadHistory, historyErr = tdlibClient.GetMessageThreadHistory(&client.GetMessageThreadHistoryRequest{
			ChatId:        threadChatID, // Use the determined threadChatID
			MessageId:     messageID,    // The ID of the original post
			FromMessageId: fromMessageId,
			Offset:        0, // Offset is usually 0 when using fromMessageId for pagination
			Limit:         historyLimit,
		})

		// --- Handle Errors ---
		if historyErr != nil {
			errStr := historyErr.Error()
			// Check for common errors indicating access issues or non-existence
			// Note: Specific error strings might change slightly between TDLib versions. Adjust if necessary.
			if strings.Contains(errStr, "Receive messages in an unexpected chat") ||
			   strings.Contains(errStr, "CHAT_ID_INVALID") ||
			   strings.Contains(errStr, "MESSAGE_ID_INVALID") || // Check both message ID errors
			   strings.Contains(errStr, "MSG_ID_INVALID") ||
			   strings.Contains(errStr, "Input_Request_Chat_Not_Found") || // More specific errors
			   strings.Contains(errStr, "Chat not found") ||
			   strings.Contains(errStr, "message thread not found") || // Check specific thread errors
			   strings.Contains(errStr, "THREAD_NOT_FOUND") {

				logContext := slog.Default().With(
					"error", historyErr, "channel", channelname,
					"originalChatID", chatID, "effectiveChatID", threadChatID,
					"messageID", messageID, "fromMessageId", fromMessageId,
				)
				logContext.Error("TDLib Error: Cannot access chat or message thread, or it doesn't exist.")

				// If this happens on the *first* request, the thread likely doesn't exist or is inaccessible.
				// If it happens mid-pagination, it might be a temporary issue or data inconsistency.
				if fromMessageId == 0 {
					// Return specific error or empty list depending on desired behavior
					// Returning the error is often better for the caller.
					return comments, fmt.Errorf("message thread %d in chat %d (channel %s) not found or inaccessible: %w", messageID, threadChatID, channelname, historyErr)
				} else {
					// Error occurred mid-pagination. Stop fetching for this thread.
					logContext.Warn("Error occurred mid-pagination, stopping comment retrieval for this thread.")
					done = true // Stop the loop
					// Optionally return the comments fetched so far, or return the error
					// return comments, historyErr // Propagate error
					break // Exit loop, return comments fetched so far below
				}
			} else {
				// Handle other potential errors (rate limits, network issues, etc.)
				slog.Error("Failed to get message thread history due to other error",
					"error", historyErr, "channel", channelname,
					"effectiveChatID", threadChatID, "messageID", messageID)
				return comments, historyErr // Return the unexpected error
			}
		}

		// --- Check if History is Empty ---
		// Check historyErr first, then check if result is nil or empty
		if threadHistory == nil || len(threadHistory.Messages) == 0 {
			slog.Debug("Received empty message history, ending pagination.", "channel", channelname, "messageID", messageID, "fromMessageId", fromMessageId)
			break // No more messages in the thread
		}

		// --- Process Fetched Messages ---
		slog.Debug("Received message batch", "count", len(threadHistory.Messages), "channel", channelname, "messageID", messageID)
		foundNewMessagesInBatch := false
		for _, msg := range threadHistory.Messages {
			if msg == nil {
				slog.Warn("Encountered nil message in history batch", "channel", channelname, "messageID", messageID)
				continue
			}

			// TDLib's GetMessageThreadHistory *should* only return messages from the thread,
			// not the original message, but you could add a check: if msg.Id == messageID { continue }

			comment := model.Comment{
				// Initialize map immediately
				Reactions: make(map[string]int),
			}

			// Safely get sender handle
			comment.Handle = GetPoster(tdlibClient, msg) // Ensure GetPoster is robust

			// Safely extract message text
			if msg.Content != nil {
				if textContent, ok := msg.Content.(*client.MessageText); ok && textContent != nil && textContent.Text != nil {
					comment.Text = textContent.Text.Text
				}
				// Add handling for other content types (photos, videos, etc.) if needed
			}

			// Safely extract reactions
			if msg.InteractionInfo != nil && msg.InteractionInfo.Reactions != nil {
				for _, reaction := range msg.InteractionInfo.Reactions.Reactions {
					// Ensure reaction and type aren't nil
					if reaction != nil && reaction.Type != nil {
						if emojiReaction, ok := reaction.Type.(*client.ReactionTypeEmoji); ok && emojiReaction != nil {
							comment.Reactions[emojiReaction.Emoji] = int(reaction.TotalCount)
						}
						// Add handling for ReactionTypeCustomEmoji if needed
					}
				}
			}

			// Safely extract view count (usually 0 for comments) and reply count (replies *within* the comment thread)
			if msg.InteractionInfo != nil {
				comment.ViewCount = int(msg.InteractionInfo.ViewCount) // Usually 0 or low for thread msgs
				if msg.InteractionInfo.ReplyInfo != nil {
					comment.ReplyCount = int(msg.InteractionInfo.ReplyInfo.ReplyCount) // Replies *to this comment*
				}
			}

			// Add other fields as needed (e.g., msg.Id, msg.Date)
			// comment.Timestamp = time.Unix(int64(msg.Date), 0) // Example

			comments = append(comments, comment)
			foundNewMessagesInBatch = true

			// Check max comments limit
			if maxcomments > -1 && len(comments) >= maxcomments {
				slog.Info("Reached max comments limit", "count", len(comments), "limit", maxcomments, "channel", channelname, "messageID", messageID)
				done = true
				break // Exit inner loop processing messages
			}
		}
		// --- End processing messages ---

		// If the outer loop should terminate (max comments reached), break here
		if done {
			break
		}

		// If the last batch returned by the API contained no usable messages, stop.
		if !foundNewMessagesInBatch {
			slog.Debug("Last batch contained no new processable messages, ending pagination.", "channel", channelname, "messageID", messageID, "fromMessageId", fromMessageId)
			break
		}

		// Determine the ID for the next request (pagination)
		lastMsg := threadHistory.Messages[len(threadHistory.Messages)-1]
		if lastMsg == nil {
			slog.Warn("Last message in the last valid batch was nil, stopping pagination", "channel", channelname, "messageID", messageID)
			break // Safety break
		}

		// Prevent infinite loop if API keeps returning the same last message
		if lastMsg.Id == fromMessageId {
			slog.Warn("Next fromMessageId is the same as the current one, potential loop detected. Stopping pagination.", "channel", channelname, "messageID", messageID, "fromMessageId", fromMessageId)
			break
		}
		fromMessageId = lastMsg.Id

	} // End for loop

	slog.Info("Finished fetching comments", "count", len(comments), "channel", channelname, "messageID", messageID, "originalChatID", chatID, "effectiveChatID", threadChatID)
	return comments, nil
}

func GetPoster(tdlibClient crawler.TDLibClient, msg *client.Message) string {
	// Set default username
	username := "unknown"

	// Comprehensive panic recovery
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			log.Error().
				Interface("panic", r).
				Str("stack", string(stack)).
				Msg("Recovered from panic in GetPoster")

			// Keep the default username on panic
			username = "unknown"
		}
	}()

	// Validate input parameters
	if tdlibClient == nil || msg == nil {
		return username
	}

	// Check if SenderId is nil
	if msg.SenderId == nil {
		return username
	}

	// Process based on sender type with careful nil checks
	switch sender := msg.SenderId.(type) {
	case *client.MessageSenderUser:
		// Check if sender is nil after type assertion
		if sender == nil {
			return username
		}

		// Safely call API and handle errors
		var userInfo *client.User
		var err error

		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Error().
						Interface("panic", r).
						Int64("userId", sender.UserId).
						Msg("Panic in GetUser call")
					err = fmt.Errorf("panic in GetUser call: %v", r)
				}
			}()

			userInfo, err = tdlibClient.GetUser(&client.GetUserRequest{
				UserId: sender.UserId,
			})
		}()

		if err == nil && userInfo != nil {
			// Safely access user information with nil checks
			if userInfo.Usernames != nil && len(userInfo.Usernames.ActiveUsernames) > 0 {
				username = userInfo.Usernames.ActiveUsernames[0]
			} else if userInfo.FirstName != "" {
				username = userInfo.FirstName
				if userInfo.LastName != "" {
					username += " " + userInfo.LastName
				}
			}
		}

	case *client.MessageSenderChat:
		// Check if sender is nil after type assertion
		if sender == nil {
			return username
		}

		// Safely call API and handle errors
		var chatInfo *client.Chat
		var err error

		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Error().
						Interface("panic", r).
						Int64("chatId", sender.ChatId).
						Msg("Panic in GetChat call")
					err = fmt.Errorf("panic in GetChat call: %v", r)
				}
			}()

			chatInfo, err = tdlibClient.GetChat(&client.GetChatRequest{
				ChatId: sender.ChatId,
			})
		}()

		if err == nil && chatInfo != nil && chatInfo.Title != "" {
			username = chatInfo.Title
		}

	default:
		log.Debug().
			Str("senderType", fmt.Sprintf("%T", msg.SenderId)).
			Msg("Unknown sender type")
	}

	return username
}

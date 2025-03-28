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

	// --- Determine the correct Chat ID for fetching comments ---
	var threadChatID int64 = chatID // Default to the provided chatID

	// Get info about the original chat to find the linked discussion group
	originalChat, err := tdlibClient.GetChat(&client.GetChatRequest{
		ChatId: chatID,
	})
	if err != nil {
		// Log the error but proceed cautiously, maybe it's already the discussion group ID
		slog.Warn("Failed to get original chat info, proceeding with provided chatID", "channel", channelname, "chatID", chatID, "error", err)
	} else if originalChat != nil && originalChat.LinkedChatId != 0 {
		// If a linked chat exists, use its ID for fetching comments
		threadChatID = originalChat.LinkedChatId
		slog.Info("Found linked discussion group, using its ID for comments", "channel", channelname, "originalChatID", chatID, "linkedChatID", threadChatID)

        // OPTIONAL BUT RECOMMENDED: Ensure the client has access to the linked chat
        // You might need to call GetChat on threadChatID or even JoinChat if necessary and appropriate
        _, err = tdlibClient.GetChat(&client.GetChatRequest{ChatId: threadChatID})
        if err != nil {
             slog.Error("Cannot access the linked discussion group", "channel", channelname, "linkedChatID", threadChatID, "error", err)
             // Decide how to handle this: return error, try original chat ID, etc.
             return nil, fmt.Errorf("cannot access linked discussion group %d for %s: %w", threadChatID, channelname, err)
        }

	} else {
		slog.Info("No linked discussion group found or failed to get chat info, using original chat ID for comments", "channel", channelname, "chatID", chatID)
	}
    // --- End of determining chat ID ---


	// Fetch the comments in the thread
	comments := make([]model.Comment, 0)
	var fromMessageId int64 = 0 // Start from the beginning of the thread
	done := false

	for !done { // Simplified loop condition
		var threadHistory *client.Messages
		var historyErr error // Use a separate variable for the history error

		// --- Fetch Batch of Comments ---
		// No need for the inner GetChat/GetMessage calls here usually
		// No need for the complex defer/recover inside the loop if the client call itself returns errors properly
		threadHistory, historyErr = tdlibClient.GetMessageThreadHistory(&client.GetMessageThreadHistoryRequest{
			ChatId:        threadChatID, // Use the determined threadChatID
			MessageId:     messageID,    // The ID of the original post
			FromMessageId: fromMessageId,
			Limit:         100,
		})

		if historyErr != nil {
			errStr := historyErr.Error()
			if strings.Contains(errStr, "Receive messages in an unexpected chat") || strings.Contains(errStr, "CHAT_ID_INVALID") || strings.Contains(errStr,"MESSAGE_ID_INVALID") || strings.Contains(errStr, "MSG_ID_INVALID") {
                // This often happens if the message doesn't exist, was deleted, or the chat ID is truly wrong/inaccessible
				slog.Error("TDLib Error: Cannot access chat or message thread",
					"error", historyErr,
					"channel", channelname,
					"effectiveChatID", threadChatID, // Log the ID actually used
					"messageID", messageID,
					"fromMessageId", fromMessageId)
				// Return a more specific error, potentially breaking the loop/function
                // Check if it's the *first* attempt (fromMessageId == 0). If so, the thread might not exist or be inaccessible.
                // If it happens mid-pagination, it could be a temporary issue or data inconsistency.
                if fromMessageId == 0 {
                    return comments, fmt.Errorf("message thread %d in chat %d (channel %s) not found or inaccessible: %w", messageID, threadChatID, channelname, historyErr)
                } else {
                     slog.Warn("Error occurred mid-pagination, stopping comment retrieval for this thread.", "channel", channelname, "messageID", messageID, "error", historyErr)
                     break // Stop pagination for this thread
                }

			} else if strings.Contains(errStr, "THREAD_NOT_FOUND") {
                 slog.Warn("Message thread does not exist or comments might be disabled.",
					"channel", channelname,
					"effectiveChatID", threadChatID,
					"messageID", messageID)
                 return comments, nil // No comments to fetch
            } else {
				// Handle other potential errors (rate limits, network issues, etc.)
				slog.Error("Failed to get message thread history",
					"error", historyErr,
					"channel", channelname,
					"effectiveChatID", threadChatID,
					"messageID", messageID)
				return comments, historyErr // Return the error
			}
		}

		// Check if threadHistory is nil or empty after handling errors
		if threadHistory == nil || len(threadHistory.Messages) == 0 {
			break // No more messages in the thread
		}

		// --- Process fetched messages ---
		foundNewMessages := false
		for _, msg := range threadHistory.Messages {
			if msg == nil {
				continue
			}
            // Skip the original post itself if it appears in the history (sometimes happens with fromMessageId=0)
            // Although GetMessageThreadHistory *should* only return thread messages. Check msg.Id against messageId if needed.

			comment := model.Comment{
				// Initialize map to avoid nil panic later
				Reactions: make(map[string]int),
			}

			// Safely get username
			comment.Handle = GetPoster(tdlibClient, msg) // Assuming GetPoster handles potential errors/nil values

			// Safely extract message text
			if msg.Content != nil {
				if textContent, ok := msg.Content.(*client.MessageText); ok && textContent != nil && textContent.Text != nil {
					comment.Text = textContent.Text.Text
				}
				// Add handling for other content types if needed (photos, stickers etc.)
			}

			// Safely extract reactions
			if msg.InteractionInfo != nil && msg.InteractionInfo.Reactions != nil {
				for _, reaction := range msg.InteractionInfo.Reactions.Reactions {
					if reaction.Type != nil {
						if emojiReaction, ok := reaction.Type.(*client.ReactionTypeEmoji); ok && emojiReaction != nil {
							comment.Reactions[emojiReaction.Emoji] = int(reaction.TotalCount)
						}
						// Add handling for custom reactions if needed
					}
				}
			}

			// Safely extract view count (usually 0 for comments) and reply count (replies *within* the comment thread)
			if msg.InteractionInfo != nil {
				comment.ViewCount = int(msg.InteractionInfo.ViewCount)
				if msg.InteractionInfo.ReplyInfo != nil {
					comment.ReplyCount = int(msg.InteractionInfo.ReplyInfo.ReplyCount)
				}
			}

			comments = append(comments, comment)
			foundNewMessages = true // Mark that we processed at least one message

			// Check max comments limit
			if maxcomments > -1 && len(comments) >= maxcomments {
				done = true
				break
			}
		}
        // --- End processing messages ---

		if done {
			break // Exit outer loop if max comments reached
		}

		// If the last batch returned no processable messages, stop.
		if !foundNewMessages || len(threadHistory.Messages) == 0 {
			break
		}

		// Update fromMessageId for the next iteration
		lastMsg := threadHistory.Messages[len(threadHistory.Messages)-1]
		if lastMsg == nil {
            slog.Warn("Last message in batch was nil, stopping pagination", "channel", channelname, "messageID", messageID)
			break // Safety break
		}
		if lastMsg.Id == fromMessageId {
            slog.Warn("fromMessageId did not change, potential loop detected. Stopping pagination.", "channel", channelname, "messageID", messageID, "fromMessageId", fromMessageId)
            break; // Prevent infinite loop if API keeps returning the same last message
        }
		fromMessageId = lastMsg.Id

	} // End for loop

	slog.Info("Finished fetching comments", "count", len(comments), "channel", channelname, "messageID", messageID)
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

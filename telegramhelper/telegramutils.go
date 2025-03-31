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

	// Added Log: Start of function execution
	log.Info().Str("channel", channelname).Int64("chatID", chatID).Int64("messageID", messageID).Msg("Entered GetMessageComments function")

	// --- Determine the correct Chat ID for fetching comments ---
	var threadChatID int64 = chatID // Default to the provided chatID
	var foundLinkedChat bool = false

	// 1. Get info about the original chat
	originalChat, err := tdlibClient.GetChat(&client.GetChatRequest{
		ChatId: chatID,
	})
	if err != nil {
		log.Warn().Err(err).Str("channel", channelname).Int64("chatID", chatID).Msg("Failed to get original chat info, proceeding with provided chatID")
	} else if originalChat != nil && originalChat.Type != nil {
		// 2. Check if the chat type is Supergroup (correct for v0.7.4)
		if chatTypeSupergroup, ok := originalChat.Type.(*client.ChatTypeSupergroup); ok && chatTypeSupergroup != nil {
			// 3. Check if it's actually a channel via the IsChannel field
			if chatTypeSupergroup.IsChannel {
				supergroupId := chatTypeSupergroup.SupergroupId
				if supergroupId != 0 {
					log.Info().Str("channel", channelname).Int64("chatID", chatID).Int64("supergroupID", supergroupId).Msg("Original chat is a channel (via ChatTypeSupergroup), attempting to get SupergroupFullInfo")
					// 4. Fetch the full info using SupergroupId
					supergroupFullInfo, err := tdlibClient.GetSupergroupFullInfo(&client.GetSupergroupFullInfoRequest{
						SupergroupId: supergroupId,
					})

					// *** Logging added here to check GetSupergroupFullInfo result ***
					if err != nil {
						log.Warn().Err(err).Str("channel", channelname).Int64("chatID", chatID).Int64("supergroupID", supergroupId).Msg("Failed to get supergroup full info for channel")
					} else if supergroupFullInfo != nil {
						// Log exactly what was found
						log.Info().Int64("supergroupID", supergroupId).Int64("foundLinkedChatID", supergroupFullInfo.LinkedChatId).Msg("GetSupergroupFullInfo succeeded")
						if supergroupFullInfo.LinkedChatId != 0 {
							// 5. Found the linked chat ID! Use this for fetching comments.
							threadChatID = supergroupFullInfo.LinkedChatId
							foundLinkedChat = true
							log.Info().Str("channel", channelname).Int64("originalChatID", chatID).Int64("linkedChatID", threadChatID).Msg("Found linked discussion group via SupergroupFullInfo")

							// Optional access check for the linked chat
							_, err = tdlibClient.GetChat(&client.GetChatRequest{ChatId: threadChatID})
							if err != nil {
								log.Error().Err(err).Str("channel", channelname).Int64("linkedChatID", threadChatID).Msg("Cannot access the determined linked discussion group")
								return nil, fmt.Errorf("cannot access linked discussion group %d for channel %s: %w", threadChatID, channelname, err)
							} else {
								log.Info().Int64("linkedChatID", threadChatID).Msg("Successfully accessed linked discussion group info")
							}
						} // else: LinkedChatId is 0, handled below by !foundLinkedChat check
					} else {
						// Log if the call succeeded but returned nil info
						log.Warn().Int64("supergroupID", supergroupId).Msg("GetSupergroupFullInfo returned nil info")
					}
					// *** End added logging ***

				} else {
					log.Info().Str("channel", channelname).Int64("chatID", chatID).Msg("ChatTypeSupergroup is channel, but SupergroupId is 0")
				}
			} else {
				log.Info().Str("channel", channelname).Int64("chatID", chatID).Msg("Chat is a Supergroup, but not a channel.")
			}
		} else {
			log.Info().Str("channel", channelname).Int64("chatID", chatID).Str("type", originalChat.Type.ChatTypeType()).Msg("Original chat is not a Supergroup/Channel type.")
		}
	}

	if !foundLinkedChat {
		log.Info().Str("channel", channelname).Int64("effectiveChatID", threadChatID).Msg("Proceeding using the initially provided chat ID (not a channel, no linked group found/accessible, or error getting info)")
	}
	// --- End of determining chat ID ---

	// Optional preliminary GetMessage check (uncomment if needed for further debugging)
	/*
		log.Info().Int64("threadChatID", threadChatID).Int64("messageID", messageID).Msg("Attempting preliminary GetMessage check")
		_, getMsgErr := tdlibClient.GetMessage(&client.GetMessageRequest{
			ChatId:    threadChatID,
			MessageId: messageID,
		})
		if getMsgErr != nil {
			log.Error().Err(getMsgErr).Int64("threadChatID", threadChatID).Int64("messageID", messageID).Msg("Preliminary GetMessage failed - message might not exist or be inaccessible")
			return comments, fmt.Errorf("preliminary GetMessage failed for message %d in chat %d: %w", messageID, threadChatID, getMsgErr)
		} else {
			log.Info().Int64("threadChatID", threadChatID).Int64("messageID", messageID).Msg("Preliminary GetMessage succeeded")
		}
	*/

	// Fetch the comments in the thread
	comments := make([]model.Comment, 0)
	var fromMessageId int64 = 0
	done := false
	const historyLimit = 100

	// Added Log: Before starting loop
	log.Info().Str("channel", channelname).Int64("effectiveChatID", threadChatID).Int64("messageID", messageID).Msg("Starting comment pagination loop")

	for !done {
		var threadHistory *client.Messages
		var historyErr error

		// Added Log: Before GetMessageThreadHistory call
		log.Info().Int64("fromMessageId", fromMessageId).Msg("Attempting GetMessageThreadHistory")

		// --- Fetch Batch of Comments ---
		threadHistory, historyErr = tdlibClient.GetMessageThreadHistory(&client.GetMessageThreadHistoryRequest{
			ChatId:        threadChatID,
			MessageId:     messageID,
			FromMessageId: fromMessageId,
			Offset:        0,
			Limit:         historyLimit,
		})

		// Added Log: After GetMessageThreadHistory call
		log.Info().Err(historyErr).Msg("GetMessageThreadHistory call completed")

		// --- Handle Errors ---
		if historyErr != nil {
			errStr := historyErr.Error()
			if strings.Contains(errStr, "Receive messages in an unexpected chat") ||
				strings.Contains(errStr, "CHAT_ID_INVALID") ||
				strings.Contains(errStr, "MESSAGE_ID_INVALID") ||
				strings.Contains(errStr, "MSG_ID_INVALID") ||
				strings.Contains(errStr, "Input_Request_Chat_Not_Found") ||
				strings.Contains(errStr, "Chat not found") ||
				strings.Contains(errStr, "message thread not found") ||
				strings.Contains(errStr, "THREAD_NOT_FOUND") ||
				// Explicitly check for the error you saw
				strings.Contains(errStr, "Message not found") { // <-- Added "Message not found" here

				loggerWithError := log.Error().Err(historyErr).Str("channel", channelname).
					Int64("originalChatID", chatID).Int64("effectiveChatID", threadChatID).
					Int64("messageID", messageID).Int64("fromMessageId", fromMessageId)

				loggerWithError.Msg("TDLib Error: Cannot access chat or message thread, or it doesn't exist (Specific Check).")

				if fromMessageId == 0 {
					return comments, fmt.Errorf("message thread %d in chat %d (channel %s) not found or inaccessible: %w", messageID, threadChatID, channelname, historyErr)
				} else {
					log.Warn().Err(historyErr).Str("channel", channelname).
						Int64("originalChatID", chatID).Int64("effectiveChatID", threadChatID).
						Int64("messageID", messageID).Int64("fromMessageId", fromMessageId).
						Msg("Error occurred mid-pagination, stopping comment retrieval for this thread.")
					done = true
					break
				}
			} else {
				// Handle other potential errors (Rate limits, network issues, etc.)
				log.Error().Err(historyErr).Str("channel", channelname).
					Int64("effectiveChatID", threadChatID).Int64("messageID", messageID).
					Msg("Failed to get message thread history due to other error") // <-- This was the log you saw previously
				return comments, historyErr
			}
		}

		// --- Check if History is Empty ---
		if threadHistory == nil || len(threadHistory.Messages) == 0 {
			log.Info().Str("channel", channelname).Int64("messageID", messageID).Int64("fromMessageId", fromMessageId).Msg("Received empty message history, ending pagination.")
			break
		}

		// --- Process Fetched Messages ---
		// Added Log: Before processing batch
		if threadHistory != nil && len(threadHistory.Messages) > 0 {
			log.Info().Int("batch_size", len(threadHistory.Messages)).Msg("Processing message batch")
		}
		foundNewMessagesInBatch := false
		for _, msg := range threadHistory.Messages {
			if msg == nil {
				log.Warn().Str("channel", channelname).Int64("messageID", messageID).Msg("Encountered nil message in history batch")
				continue
			}

			comment := model.Comment{
				Reactions: make(map[string]int),
			}

			comment.Handle = GetPoster(tdlibClient, msg)

			if msg.Content != nil {
				if textContent, ok := msg.Content.(*client.MessageText); ok && textContent != nil && textContent.Text != nil {
					comment.Text = textContent.Text.Text
				}
			}

			if msg.InteractionInfo != nil && msg.InteractionInfo.Reactions != nil {
				for _, reaction := range msg.InteractionInfo.Reactions.Reactions {
					if reaction != nil && reaction.Type != nil {
						if emojiReaction, ok := reaction.Type.(*client.ReactionTypeEmoji); ok && emojiReaction != nil {
							comment.Reactions[emojiReaction.Emoji] = int(reaction.TotalCount)
						}
					}
				}
			}

			if msg.InteractionInfo != nil {
				comment.ViewCount = int(msg.InteractionInfo.ViewCount)
				if msg.InteractionInfo.ReplyInfo != nil {
					comment.ReplyCount = int(msg.InteractionInfo.ReplyInfo.ReplyCount)
				}
			}

			comments = append(comments, comment)
			foundNewMessagesInBatch = true

			if maxcomments > -1 && len(comments) >= maxcomments {
				log.Info().Int("count", len(comments)).Int("limit", maxcomments).Str("channel", channelname).Int64("messageID", messageID).Msg("Reached max comments limit")
				done = true
				break
			}
		}
		// --- End processing messages ---

		if done {
			break
		}

		if !foundNewMessagesInBatch {
			log.Info().Str("channel", channelname).Int64("messageID", messageID).Int64("fromMessageId", fromMessageId).Msg("Last batch contained no new processable messages, ending pagination.")
			break
		}

		lastMsg := threadHistory.Messages[len(threadHistory.Messages)-1]
		if lastMsg == nil {
			log.Warn().Str("channel", channelname).Int64("messageID", messageID).Msg("Last message in the last valid batch was nil, stopping pagination")
			break
		}

		if lastMsg.Id == fromMessageId {
			log.Warn().Str("channel", channelname).Int64("messageID", messageID).Int64("fromMessageId", fromMessageId).Msg("Next fromMessageId is the same as the current one, potential loop detected. Stopping pagination.")
			break
		}
		fromMessageId = lastMsg.Id

		// Added Log: End of loop iteration
		log.Info().Int64("next_fromMessageId", fromMessageId).Bool("done", done).Msg("End of loop iteration")

	} // End for loop

	log.Info().Msgf("Finished fetching comments. Got %d comments for channel %s [messageID: %d, originalChatID: %d, effectiveChatID: %d]",
		len(comments), channelname, messageID, chatID, threadChatID)

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
		log.Info().
			Str("senderType", fmt.Sprintf("%T", msg.SenderId)).
			Msg("Unknown sender type")
	}

	return username
}

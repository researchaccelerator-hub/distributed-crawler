package telegramhelper

import (
	"fmt"
	"math/rand"
	"runtime/debug"
	"strings"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/researchaccelerator-hub/telegram-scraper/model"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/rs/zerolog/log"
	"github.com/zelenin/go-tdlib/client"
)

func FetchChannelMessages(tdlibClient crawler.TDLibClient, chatID int64, page *state.Page, minPostDate time.Time, maxPosts int) ([]*client.Message, error) {
	return FetchChannelMessagesWithDateRange(tdlibClient, chatID, page, minPostDate, time.Time{}, maxPosts)
}

func FetchChannelMessagesWithDateRange(tdlibClient crawler.TDLibClient, chatID int64, page *state.Page, minPostDate time.Time, maxPostDate time.Time, maxPosts int) ([]*client.Message, error) {
	return FetchChannelMessagesWithSampling(tdlibClient, chatID, page, minPostDate, maxPostDate, maxPosts, 0)
}

func FetchChannelMessagesWithSampling(tdlibClient crawler.TDLibClient, chatID int64, page *state.Page, minPostDate time.Time, maxPostDate time.Time, maxPosts int, sampleSize int) ([]*client.Message, error) {
	log.Debug().Msgf("Fetching messages for channel %s since %s", page.URL, minPostDate.Format("2006-01-02 15:04:05"))
	if !maxPostDate.IsZero() {
		log.Debug().Msgf("Max post date filter: %s", maxPostDate.Format("2006-01-02 15:04:05"))
	}
	var allMessages []*client.Message
	var fromMessageId int64 = 0 // Start from the latest message
	var oldestMessageId int64 = 0

	// Convert minPostDate to Unix timestamp for comparison
	minPostUnix := minPostDate.Unix()
	var maxPostUnix int64
	if !maxPostDate.IsZero() {
		maxPostUnix = maxPostDate.Unix()
	}

	for {

		// TODO: Replace with client level rate limiting
		sleepMS := 1600 + rand.Intn(900)
		log.Info().Int("sleep_ms", sleepMS).Str("api_call", "GetChatHistory").Msg("Telegram API Call Sleep")
		time.Sleep(time.Duration(sleepMS) * time.Millisecond)

		log.Debug().Msgf("Fetching message batch for channel %s starting from ID %d at depth: %v", page.URL, fromMessageId, page.Depth)
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

		// Check messages and add only those within the date range
		reachedOldMessages := false
		for _, msg := range chatHistory.Messages {
			msgUnix := int64(msg.Date)

			// Compare message timestamp with minPostDate
			if msgUnix < minPostUnix {
				log.Debug().Msgf("Reached messages older than minimum date (message date: %v, min date: %v)",
					time.Unix(msgUnix, 0).Format("2006-01-02 15:04:05"),
					minPostDate.Format("2006-01-02 15:04:05"))
				reachedOldMessages = true
				break
			}

			// Check if message is newer than maxPostDate (if specified)
			if !maxPostDate.IsZero() && msgUnix > maxPostUnix {
				log.Debug().Msgf("Skipping message newer than maximum date (message date: %v, max date: %v)",
					time.Unix(msgUnix, 0).Format("2006-01-02 15:04:05"),
					maxPostDate.Format("2006-01-02 15:04:05"))
				continue
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

	log.Debug().Msgf("Fetched a total of %d messages for channel %s since %s",
		len(allMessages), page.URL, minPostDate.Format("2006-01-02 15:04:05"))

	// Apply random sampling if sampleSize is specified and we have more messages than requested
	if sampleSize > 0 && len(allMessages) > sampleSize {
		log.Info().
			Int("total_messages", len(allMessages)).
			Int("sample_size", sampleSize).
			Str("channel", page.URL).
			Msg("Applying random sampling to messages")

		// Create a random seed based on current time
		rand.Seed(time.Now().UnixNano())

		// Create a copy of the slice to avoid modifying the original
		messagesCopy := make([]*client.Message, len(allMessages))
		copy(messagesCopy, allMessages)

		// Shuffle the messages using Fisher-Yates algorithm
		for i := len(messagesCopy) - 1; i > 0; i-- {
			j := rand.Intn(i + 1)
			messagesCopy[i], messagesCopy[j] = messagesCopy[j], messagesCopy[i]
		}

		// Take only the first sampleSize messages
		sampledMessages := messagesCopy[:sampleSize]

		log.Info().
			Int("original_count", len(allMessages)).
			Int("sampled_count", len(sampledMessages)).
			Str("channel", page.URL).
			Msg("Random sampling completed")

		return sampledMessages, nil
	}

	return allMessages, nil
}

func GetChannelMemberCount(tdlibClient crawler.TDLibClient, channelId int64) (int, error) {
	// First, resolve the username to get the chat ID
	// chat, err := tdlibClient.SearchPublicChat(&client.SearchPublicChatRequest{
	// 	Username: channelUsername,
	// })

	// chatDetails, err := tdlibClient.GetChat(&client.GetChatRequest{
	// 	ChatId: chat.Id,
	// })

	chat, err := tdlibClient.GetChat(&client.GetChatRequest{
		ChatId: channelId,
	})

	if err != nil {
		return 0, fmt.Errorf("failed to resolve channel username: %w", err)
	}

	// Get full chat info based on the chat type
	chatType := chat.Type
	var memberCount int

	// TODO: Replace with client level rate limiting
	sleepMS := 2500 + rand.Intn(900)

	switch v := chatType.(type) {
	case *client.ChatTypeSupergroup:
		// For channels and supergroups
		supergroupId := v.SupergroupId

		log.Info().Int("sleep_ms", sleepMS).Str("api_call", "GetSuperGroupFullInfo").Msg("Telegram API Call Sleep")
		time.Sleep(time.Duration(sleepMS) * time.Millisecond)
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

		log.Info().Int("sleep_ms", sleepMS).Str("api_call", "GetBasicGroupFullInfo").Msg("Telegram API Call Sleep")
		time.Sleep(time.Duration(sleepMS) * time.Millisecond)
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
	log.Debug().Msgf("Getting message count for channel %s", channelname)

	return len(messages), nil
}

// GetViewCount retrieves the view count from a given message's InteractionInfo.
// If InteractionInfo is nil, it returns 0 as the default view count.
func GetViewCount(message *client.Message, channelname string) int {
	log.Debug().Msgf("Getting message view count for channel %s", channelname)
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

	// TODO: Replace with client level rate limiting
	sleepMS := 600 + rand.Intn(900)
	log.Info().Int("sleep_ms", sleepMS).Str("api_call", "GetMessage").Msg("Telegram API Call Sleep")
	time.Sleep(time.Duration(sleepMS) * time.Millisecond)

	// Fetch the message details
	log.Debug().Msgf("Getting message share count for channel %s", channelname)
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
	log.Debug().Msgf("Getting total views for channel %s", channelname)
	for _, m := range messages {

		if m.InteractionInfo != nil {
			totalViews += int64(m.InteractionInfo.ViewCount)
		}

	}

	log.Debug().Msgf("Total views for channel %s: %d", channelname, totalViews)
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
func GetMessageComments(tdlibClient crawler.TDLibClient, chatID, messageID int64, channelname string, maxcomments int, commentcount int) ([]model.Comment, error) {
	// Check if tdlibClient is nil
	if tdlibClient == nil {
		log.Error().
			Str("channel", channelname).
			Int64("chatID", chatID).
			Int64("messageID", messageID).
			Msg("TDLib client is nil")
		return nil, fmt.Errorf("tdlibClient is nil")
	}

	if maxcomments == 0 {
		return []model.Comment{}, nil
	}

	log.Debug().
		Str("channel", channelname).
		Int64("chatID", chatID).
		Int64("messageID", messageID).
		Int("maxcomments", maxcomments).
		Int("commentcount", commentcount).
		Msg("Starting GetMessageComments function")

	// Fetch the comments in the thread
	comments := make([]model.Comment, 0)
	var fromMessageId int64 = 0
	done := false
	iterationCount := 0

	for {
		iterationCount++
		log.Debug().
			Str("channel", channelname).
			Int64("chatID", chatID).
			Int64("messageID", messageID).
			Int64("fromMessageId", fromMessageId).
			Int("iteration", iterationCount).
			Msg("Starting pagination iteration")

		// Set up variables to store results
		var threadHistory *client.Messages
		var err error

		// Set up panic recovery directly in the main function
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()
				log.Error().
					Str("channel", channelname).
					Interface("panic", r).
					Str("stack", string(stack)).
					Int64("chatID", chatID).
					Int64("messageID", messageID).
					Int64("fromMessageId", fromMessageId).
					Int("iteration", iterationCount).
					Msg("Recovered from panic in GetMessageComments")

				// If we've recovered from a panic, we'll just return what we have so far
				done = true
			}
		}()

		// Verify the chat exists
		log.Debug().
			Str("channel", channelname).
			Int64("chatID", chatID).
			Msg("Verifying chat existence")

		chatInfo, chatErr := tdlibClient.GetChat(&client.GetChatRequest{
			ChatId: chatID,
		})

		if chatErr != nil {
			log.Error().
				Err(chatErr).
				Str("channel", channelname).
				Int64("chatID", chatID).
				Msg("Failed to get chat info")
		} else {
			// Log chat type information for debugging
			if chatInfo != nil && chatInfo.Type != nil {
				log.Debug().
					Str("channel", channelname).
					Int64("chatID", chatID).
					Str("chatTitle", chatInfo.Title).
					Str("chatType", chatInfo.Type.ChatTypeType()).
					Msg("Successfully got chat info")

				// If it's a supergroup, log additional details
				if supergroupType, ok := chatInfo.Type.(*client.ChatTypeSupergroup); ok && supergroupType != nil {
					log.Debug().
						Int64("supergroupId", supergroupType.SupergroupId).
						Bool("isChannel", supergroupType.IsChannel).
						Msg("Chat is a supergroup")
				}
			}
		}

		// Check if the message exists
		log.Debug().
			Str("channel", channelname).
			Int64("chatID", chatID).
			Int64("messageID", messageID).
			Msg("Verifying message existence")

		msgInfo, msgErr := tdlibClient.GetMessage(&client.GetMessageRequest{
			ChatId:    chatID,
			MessageId: messageID,
		})

		if msgErr != nil {
			log.Error().
				Err(msgErr).
				Str("channel", channelname).
				Int64("chatID", chatID).
				Int64("messageID", messageID).
				Msg("Failed to get message")
		} else {
			// Log message info for debugging
			if msgInfo != nil {
				var contentType string
				if msgInfo.Content != nil {
					contentType = msgInfo.Content.MessageContentType()
				}

				log.Debug().
					Str("channel", channelname).
					Int64("chatID", chatID).
					Int64("messageID", messageID).
					Str("contentType", contentType).
					Msg("Successfully got message info")
			}
		}

		// Determine batch size based on remaining comments to fetch
		batchSize := 100 // Default batch size

		// If maxcomments is specified and we know how many more comments we need
		if maxcomments > 0 {
			remainingNeeded := maxcomments - len(comments)
			if remainingNeeded <= 0 {
				// We've already fetched all the comments we need
				done = true
				break
			}

			// If we have a commentcount, check that we're not fetching more than available
			if commentcount > 0 && commentcount < maxcomments {
				// We should only get up to commentcount total comments
				remainingNeeded = commentcount - len(comments)
				if remainingNeeded <= 0 {
					// We've already fetched all available comments
					done = true
					break
				}
			}

			// Only request as many comments as we still need
			if remainingNeeded < batchSize {
				batchSize = remainingNeeded
			}
		}

		// Get the message thread history directly (without function wrapper)
		log.Debug().
			Str("channel", channelname).
			Int64("chatID", chatID).
			Int64("messageID", messageID).
			Int64("fromMessageId", fromMessageId).
			Int("batchSize", batchSize).
			Int("iteration", iterationCount).
			Msg("Attempting to get message thread history")

		// Get the message thread history with proper batch size
		threadHistory, err = tdlibClient.GetMessageThreadHistory(&client.GetMessageThreadHistoryRequest{
			ChatId:        chatID,
			MessageId:     messageID,
			FromMessageId: fromMessageId,
			Limit:         int32(batchSize), // Fetch comments in appropriate batch size
		})

		// Log the result of the GetMessageThreadHistory call
		if err != nil {
			log.Error().
				Err(err).
				Str("channel", channelname).
				Int64("chatID", chatID).
				Int64("messageID", messageID).
				Int64("fromMessageId", fromMessageId).
				Int("iteration", iterationCount).
				Msg("GetMessageThreadHistory failed")
		} else {
			// Log success with message count
			messageCount := 0
			if threadHistory != nil && threadHistory.Messages != nil {
				messageCount = len(threadHistory.Messages)
			}

			log.Debug().
				Str("channel", channelname).
				Int64("chatID", chatID).
				Int64("messageID", messageID).
				Int64("fromMessageId", fromMessageId).
				Int("messagesCount", messageCount).
				Int("iteration", iterationCount).
				Msg("GetMessageThreadHistory succeeded")
		}

		if err != nil {
			// Parse the error string to detect specific errors
			errStr := err.Error()

			if strings.Contains(errStr, "Receive messages in an unexpected chat") {
				log.Warn().
					Err(err).
					Str("channel", channelname).
					Int64("chatID", chatID).
					Int64("messageID", messageID).
					Int("iteration", iterationCount).
					Int("commentsCollected", len(comments)).
					Msg("Received 'unexpected chat' error - ending pagination")

				// End pagination loop - we've collected what we can
				done = true
				break
			}

			log.Error().
				Err(err).
				Stack().
				Str("channel", channelname).
				Int64("chatID", chatID).
				Int64("messageID", messageID).
				Int("iteration", iterationCount).
				Str("errorMsg", errStr).
				Msg("Failed to get message thread history")

			return comments, err
		}

		// Check if threadHistory is nil or empty
		if threadHistory == nil || len(threadHistory.Messages) == 0 {
			log.Debug().
				Str("channel", channelname).
				Int64("chatID", chatID).
				Int64("messageID", messageID).
				Int64("fromMessageId", fromMessageId).
				Int("iteration", iterationCount).
				Msg("Thread history is empty, ending pagination")

			break
		}

		// Log the number of messages retrieved in this batch
		log.Debug().
			Str("channel", channelname).
			Int64("chatID", chatID).
			Int64("messageID", messageID).
			Int("messageCount", len(threadHistory.Messages)).
			Int("iteration", iterationCount).
			Msg("Processing batch of messages")

		// Process each message in the thread
		commentsAddedInBatch := 0
		for i, msg := range threadHistory.Messages {
			// Skip nil messages
			if msg == nil {
				log.Warn().
					Str("channel", channelname).
					Int64("chatID", chatID).
					Int64("messageID", messageID).
					Int("index", i).
					Int("iteration", iterationCount).
					Msg("Encountered nil message in thread history")

				continue
			}

			comment := model.Comment{}

			// Safely get username
			username := GetPoster(tdlibClient, msg)
			comment.Handle = username

			// Safely extract message text
			messageText := ""
			if msg.Content != nil {
				if textContent, ok := msg.Content.(*client.MessageText); ok && textContent != nil && textContent.Text != nil {
					messageText = textContent.Text.Text
					comment.Text = textContent.Text.Text
				}
			}

			// Safely extract reactions
			reactionCount := 0
			if msg.InteractionInfo != nil &&
				msg.InteractionInfo.Reactions != nil &&
				len(msg.InteractionInfo.Reactions.Reactions) > 0 {

				comment.Reactions = make(map[string]int)
				reactionCount = len(msg.InteractionInfo.Reactions.Reactions)

				for _, reaction := range msg.InteractionInfo.Reactions.Reactions {
					if reaction.Type != nil {
						if emojiReaction, ok := reaction.Type.(*client.ReactionTypeEmoji); ok && emojiReaction != nil {
							comment.Reactions[emojiReaction.Emoji] = int(reaction.TotalCount)
						}
					}
				}
			}

			// Safely extract view count and reply count
			viewCount := 0
			replyCount := 0
			if msg.InteractionInfo != nil {
				// Extract reply count
				if msg.InteractionInfo.ReplyInfo != nil {
					replyCount = int(msg.InteractionInfo.ReplyInfo.ReplyCount)
					comment.ReplyCount = replyCount
				}

				// Extract view count
				viewCount = int(msg.InteractionInfo.ViewCount)
				comment.ViewCount = viewCount
			}

			// Log message processing details
			log.Debug().
				Str("channel", channelname).
				Int64("chatID", chatID).
				Int64("messageID", messageID).
				Int64("commentID", msg.Id).
				Str("handle", comment.Handle).
				Int("textLength", len(messageText)).
				Int("viewCount", viewCount).
				Int("replyCount", replyCount).
				Int("reactionCount", reactionCount).
				Int("index", i).
				Int("iteration", iterationCount).
				Msg("Processed comment")

			// Add comment to the results
			comments = append(comments, comment)
			commentsAddedInBatch++

			// Check if we've reached the maximum number of comments
			if maxcomments > -1 && len(comments) >= maxcomments {
				log.Debug().
					Str("channel", channelname).
					Int64("chatID", chatID).
					Int64("messageID", messageID).
					Int("commentsCollected", len(comments)).
					Int("maxComments", maxcomments).
					Int("iteration", iterationCount).
					Msg("Reached maximum comment limit, ending pagination")

				done = true
				break
			}
		}

		// Log the number of comments added in this batch
		log.Debug().
			Str("channel", channelname).
			Int64("chatID", chatID).
			Int64("messageID", messageID).
			Int("commentsAddedInBatch", commentsAddedInBatch).
			Int("totalCommentsCollected", len(comments)).
			Int("iteration", iterationCount).
			Msg("Batch processing complete")

		if done {
			break
		}

		// Check if we have messages to get the last ID
		if len(threadHistory.Messages) == 0 {
			log.Debug().
				Str("channel", channelname).
				Int64("chatID", chatID).
				Int64("messageID", messageID).
				Int("iteration", iterationCount).
				Msg("No messages in batch, ending pagination")

			break
		}

		// Safely get the last message ID for pagination
		lastMsg := threadHistory.Messages[len(threadHistory.Messages)-1]
		if lastMsg == nil {
			log.Warn().
				Str("channel", channelname).
				Int64("chatID", chatID).
				Int64("messageID", messageID).
				Int("iteration", iterationCount).
				Msg("Last message in batch is nil, ending pagination")

			break
		}

		// Log the last message ID for pagination
		previousFromMessageId := fromMessageId
		fromMessageId = lastMsg.Id

		log.Debug().
			Str("channel", channelname).
			Int64("chatID", chatID).
			Int64("messageID", messageID).
			Int64("previousFromMessageId", previousFromMessageId).
			Int64("newFromMessageId", fromMessageId).
			Int("iteration", iterationCount).
			Msg("Updated pagination cursor")

		// Safety check - if we're not making progress, break
		if fromMessageId == 0 || fromMessageId == previousFromMessageId {
			log.Warn().
				Str("channel", channelname).
				Int64("chatID", chatID).
				Int64("messageID", messageID).
				Int64("fromMessageId", fromMessageId).
				Int("iteration", iterationCount).
				Msg("Pagination not making progress or fromMessageId is 0, ending pagination")

			break
		}
	}

	log.Debug().
		Str("channel", channelname).
		Int64("chatID", chatID).
		Int64("messageID", messageID).
		Int("totalComments", len(comments)).
		Int("iterations", iterationCount).
		Msg("Finished fetching comments")

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

package telegramhelper

import (
	"fmt"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/researchaccelerator-hub/telegram-scraper/model"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/rs/zerolog/log"
	"github.com/zelenin/go-tdlib/client"
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
	// Fetch the comments in the thread
	var comments []model.Comment
	var fromMessageId int64 = 0
	done := false
	for {
		threadHistory, err := tdlibClient.GetMessageThreadHistory(&client.GetMessageThreadHistoryRequest{
			ChatId:        chatID,
			MessageId:     messageID,
			FromMessageId: fromMessageId,
			Limit:         100, // Fetch up to 100 comments at a time
		})
		if err != nil {
			log.Error().Err(err).Stack().Msgf("Failed to get message thread history for channel %s: %v", channelname, err)
			return comments, err
		}

		// Stop if no more comments are returned
		if len(threadHistory.Messages) == 0 {
			break
		}

		// Collect the comment text
		for _, msg := range threadHistory.Messages {
			comment := model.Comment{}

			username := GetPoster(tdlibClient, msg)
			comment.Handle = username
			
			if textContent, ok := msg.Content.(*client.MessageText); ok {
				comment.Text = textContent.Text.Text

			}
			if msg.InteractionInfo != nil && len(msg.InteractionInfo.Reactions.Reactions) > 0 {
				comment.Reactions = make(map[string]int)
				for _, reaction := range msg.InteractionInfo.Reactions.Reactions {
					if emojiReaction, ok := reaction.Type.(*client.ReactionTypeEmoji); ok {
						comment.Reactions[emojiReaction.Emoji] = int(reaction.TotalCount)
					}
				}
			}

			// Extract view count
			if msg.InteractionInfo != nil {
				defer func() {
					if r := recover(); r != nil {
						// Log the error and continue
						log.Info().Msgf("Recovered from panic while processing InteractionInfo: %v\n", r)
					}
				}()

				if msg.InteractionInfo.ReplyInfo != nil {
					comment.ReplyCount = int(msg.InteractionInfo.ReplyInfo.ReplyCount)
				}
				comment.ViewCount = int(msg.InteractionInfo.ViewCount)
			}

			// Add to the comments slice
			comments = append(comments, comment)
			if maxcomments > -1 && len(comments) >= maxcomments {
				done = true
				break
			}
		}

		if done {
			break
		}
		// Update `fromMessageId` to fetch older comments
		fromMessageId = threadHistory.Messages[len(threadHistory.Messages)-1].Id
	}

	log.Info().Msgf("Got %d comments for channel %s", len(comments), channelname)
	return comments, nil
}

func GetPoster(tdlibClient crawler.TDLibClient, msg *client.Message) string {
	username := "unknown"
	if msg.SenderId != nil {
		switch sender := msg.SenderId.(type) {
		case *client.MessageSenderUser:
			// Get user info to extract username
			userInfo, err := tdlibClient.GetUser(&client.GetUserRequest{
				UserId: sender.UserId,
			})
			if err == nil && userInfo != nil {
				username := userInfo.Usernames.ActiveUsernames[0]
				// If username is empty, use first name + last name as fallback
				if username == "" && userInfo.FirstName != "" {
					username = userInfo.FirstName
					if userInfo.LastName != "" {
						username += " " + userInfo.LastName
					}
				}
			}
		case *client.MessageSenderChat:
			// Handle messages from channels or groups
			chatInfo, err := tdlibClient.GetChat(&client.GetChatRequest{
				ChatId: sender.ChatId,
			})
			if err == nil && chatInfo != nil {
				username = chatInfo.Title
			}
		}
	}
	return username
}

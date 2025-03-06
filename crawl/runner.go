package crawl

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/researchaccelerator-hub/telegram-scraper/telegramhelper"
	"github.com/rs/zerolog/log"
	"github.com/zelenin/go-tdlib/client"
	"time"
)

// Run connects to a Telegram channel and crawls its messages.
func RunForChannel(p *state.Page, storagePrefix string, sm state.StateManager, cfg common.CrawlerConfig) ([]*state.Page, error) {
	// Initialize Telegram client
	service := &telegramhelper.RealTelegramService{}
	tdlibClient, err := service.InitializeClientWithConfig(storagePrefix, cfg)
	if err != nil {
		log.Error().Err(err).Stack().Msg("Failed to initialize Telegram client")
		return nil, err
	}

	// Ensure tdlibClient is closed after the function finishes
	defer closeClient(tdlibClient)

	// Get channel information
	channelInfo, err := getChannelInfo(tdlibClient, p.URL)
	if err != nil {
		return nil, err
	}
	active, err := IsChannelActiveWithinPeriod(tdlibClient, channelInfo.chatDetails.Id, cfg.PostRecency)
	if err != nil {
		return nil, err
	}
	if !active || channelInfo.messageCount == 0 || (cfg.MinUsers > 0 && channelInfo.memberCount < int32(cfg.MinUsers)) {
		log.Info().Msg("Not enough members in the channel, considering it private and skipping.")
		p.Status = "deadend"
		sm.StoreState()
		return nil, nil
	}

	// Process all messages in the channel
	discoveredChannels, err := processAllMessages(tdlibClient, channelInfo, cfg.CrawlID, p.URL, sm, p, cfg)
	if err != nil {
		return nil, err
	}

	return discoveredChannels, nil
}

func GetLatestMessageTime(tdlibClient crawler.TDLibClient, chatID int64) (time.Time, error) {
	// Fetch the most recent message
	// fromMessageID=0 means from the latest message
	// offset=0 means no offset from the chosen message
	// limit=1 means get only one message
	// Use 0 for the fromMessageID parameter to get the most recent message
	messages, err := tdlibClient.GetChatHistory(&client.GetChatHistoryRequest{
		ChatId:        chatID,
		FromMessageId: 0, // 0 means get from the latest message
		Offset:        0, // No offset from the starting point
		Limit:         1, // Only need 1 message (the latest one)
		OnlyLocal:     false,
	})

	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get chat history: %v", err)
	}

	// If there are no messages, return zero time
	if len(messages.Messages) == 0 {
		return time.Time{}, fmt.Errorf("no messages found in the chat")
	}

	// Get the timestamp from the latest message
	latestMessage := messages.Messages[0]
	timestamp := time.Unix(int64(latestMessage.Date), 0)

	return timestamp, nil
}

func IsChannelActiveWithinPeriod(tdlibClient crawler.TDLibClient, chatID int64, cutoffTime time.Time) (bool, error) {
	latestMessageTime, err := GetLatestMessageTime(tdlibClient, chatID)
	if err != nil {
		return false, err
	}

	// Compare the latest message time with the cutoff time
	return latestMessageTime.After(cutoffTime), nil
}

// closeClient safely closes the Telegram client
func closeClient(tdlibClient crawler.TDLibClient) {
	if tdlibClient != nil {
		log.Debug().Msg("Closing tdlibClient...")
		_, err := tdlibClient.Close()
		if err != nil {
			log.Error().Err(err).Msg("Error closing tdlibClient")
		} else {
			log.Info().Msg("tdlibClient closed successfully")
		}
	}
}

// channelInfo holds all necessary channel data
type channelInfo struct {
	chat           *client.Chat
	chatDetails    *client.Chat
	supergroup     *client.Supergroup
	supergroupInfo *client.SupergroupFullInfo
	totalViews     int32
	messageCount   int32
	memberCount    int32
}

// TotalViewsGetter is a function type for retrieving total view count
type TotalViewsGetter func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error)

// MessageCountGetter is a function type for retrieving message count
type MessageCountGetter func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error)

// MemberCountGetter is a function type for retrieving message count
type MemberCountGetter func(client crawler.TDLibClient, channelUsername string) (int, error)

func getChannelInfo(tdlibClient crawler.TDLibClient, channelUsername string) (*channelInfo, error) {
	return getChannelInfoWithDeps(
		tdlibClient,
		channelUsername,
		telegramhelper.GetTotalChannelViews,
		telegramhelper.GetMessageCount,
		telegramhelper.GetChannelMemberCount,
	)
}

// getChannelInfoWithDeps is the dependency-injected version of getChannelInfo
func getChannelInfoWithDeps(
	tdlibClient crawler.TDLibClient,
	channelUsername string,
	getTotalViewsFn TotalViewsGetter,
	getMessageCountFn MessageCountGetter,
	getMemberCountFn MemberCountGetter,
) (*channelInfo, error) {
	// Search for the channel
	chat, err := tdlibClient.SearchPublicChat(&client.SearchPublicChatRequest{
		Username: channelUsername,
	})
	if err != nil {
		log.Error().Err(err).Stack().Msgf("Failed to find channel: %v", channelUsername)
		return nil, err
	}

	chatDetails, err := tdlibClient.GetChat(&client.GetChatRequest{
		ChatId: chat.Id,
	})
	if err != nil {
		log.Error().Err(err).Stack().Msgf("Failed to get chat details for: %v", channelUsername)
		return nil, err
	}

	// Get channel stats
	totalViews := 0
	if getTotalViewsFn != nil {
		totalViewsVal, err := getTotalViewsFn(tdlibClient, chat.Id, channelUsername)
		if err != nil {
			log.Warn().Err(err).Msgf("Failed to get total views for channel: %v", channelUsername)
			// Continue anyway, this is not critical
		} else {
			totalViews = totalViewsVal
		}
	}

	messageCount := 0
	if getMessageCountFn != nil {
		messageCountVal, err := getMessageCountFn(tdlibClient, chat.Id, channelUsername)
		if err != nil {
			log.Warn().Err(err).Msgf("Failed to get message count for channel: %v", channelUsername)
			// Continue anyway, this is not critical
		} else {
			messageCount = messageCountVal
		}
	}

	memberCount := 0
	if getMemberCountFn != nil {
		memberCountVal, err := getMemberCountFn(tdlibClient, channelUsername)
		if err != nil {
			log.Warn().Err(err).Msgf("Failed to get member count for channel: %v", channelUsername)
		} else {
			memberCount = memberCountVal
		}
	}
	// Get supergroup information if available
	var supergroup *client.Supergroup
	var supergroupInfo *client.SupergroupFullInfo

	if chat.Type != nil {
		if supergroupType, ok := chat.Type.(*client.ChatTypeSupergroup); ok {
			supergroup, err = tdlibClient.GetSupergroup(&client.GetSupergroupRequest{
				SupergroupId: supergroupType.SupergroupId,
			})
			if err != nil {
				log.Warn().Err(err).Msgf("Failed to get supergroup info for: %v", channelUsername)
				// Continue anyway, this is not critical
			}

			if supergroup != nil {
				req := &client.GetSupergroupFullInfoRequest{
					SupergroupId: supergroup.Id,
				}
				supergroupInfo, err = tdlibClient.GetSupergroupFullInfo(req)
				if err != nil {
					log.Warn().Err(err).Msgf("Failed to get supergroup full info for: %v", channelUsername)
					// Continue anyway, this is not critical
				}
			}
		}
	}

	return &channelInfo{
		chat:           chat,
		chatDetails:    chatDetails,
		supergroup:     supergroup,
		supergroupInfo: supergroupInfo,
		totalViews:     int32(totalViews),
		messageCount:   int32(messageCount),
		memberCount:    int32(memberCount),
	}, nil
}

type MessageFetcher interface {
	FetchMessages(tdlibClient crawler.TDLibClient, chatID int64, fromMessageID int64) ([]*client.Message, error)
}

type DefaultMessageFetcher struct{}

func (f *DefaultMessageFetcher) FetchMessages(tdlibClient crawler.TDLibClient, chatID int64, fromMessageID int64) ([]*client.Message, error) {
	// Call the original fetchMessages implementation
	chatHistory, err := tdlibClient.GetChatHistory(&client.GetChatHistoryRequest{
		ChatId:        chatID,
		Limit:         100,
		FromMessageId: fromMessageID,
	})

	if err != nil {
		log.Error().Stack().Err(err).Msg("Failed to get chat history")
		return nil, err
	}

	return chatHistory.Messages, nil
}

type MessageProcessor interface {
	// ProcessMessage processes a single Telegram message.
	ProcessMessage(tdlibClient crawler.TDLibClient, messageId int64, chatId int64, info *channelInfo, crawlID string, channelUsername string, sm *state.StateManager, cfg common.CrawlerConfig) ([]string, error)
}

// DefaultMessageProcessor implements the MessageProcessor interface using the default processMessage function
type DefaultMessageProcessor struct{}

// ProcessMessage implements the MessageProcessor interface
func (p *DefaultMessageProcessor) ProcessMessage(tdlibClient crawler.TDLibClient, messageId int64, chatId int64, info *channelInfo, crawlID string, channelUsername string, sm *state.StateManager, cfg common.CrawlerConfig) ([]string, error) {
	return processMessage(tdlibClient, messageId, chatId, info, crawlID, channelUsername, *sm, cfg)
}

// processAllMessages retrieves and processes all messages from a channel
func processAllMessages(tdlibClient crawler.TDLibClient, info *channelInfo, crawlID, channelUsername string, sm state.StateManager, owner *state.Page, cfg common.CrawlerConfig) ([]*state.Page, error) {
	processor := &DefaultMessageProcessor{}
	fetcher := &DefaultMessageFetcher{}
	return processAllMessagesWithProcessor(tdlibClient, info, crawlID, channelUsername, sm, processor, fetcher, owner, cfg)

}

// processAllMessagesWithProcessor retrieves and processes all messages from a channel.
// This function fetches messages in batches, starting from the most recent message
// and working backward through the history. It uses the provided MessageProcessor
// to process each message, allowing for customized message handling.
func processAllMessagesWithProcessor(
	tdlibClient crawler.TDLibClient,
	info *channelInfo,
	crawlID,
	channelUsername string,
	sm state.StateManager,
	processor MessageProcessor,
	fetcher MessageFetcher, owner *state.Page, cfg common.CrawlerConfig) ([]*state.Page, error) {

	var fromMessageID int64 = 0
	discoveredChannels := make([]*state.Page, 0)
	discoveredMessages := make([]state.Message, 0)

	for {
		log.Info().Msgf("Fetching from message id %d", fromMessageID)

		messages, err := fetcher.FetchMessages(tdlibClient, info.chat.Id, fromMessageID)
		if err != nil {
			return nil, err
		}

		if len(messages) == 0 {
			log.Info().Msgf("No more messages found in the channel %s", channelUsername)
			break
		}

		// Process messages
		for _, message := range messages {
			m := state.Message{
				ChatId:    message.ChatId,
				MessageId: message.Id,
				Status:    "unfetched",
				PageId:    owner.ID,
			}
			discoveredMessages = append(discoveredMessages, m)
		}
		fromMessageID = messages[len(messages)-1].Id

	}
	owner.Messages = addNewMessages(discoveredMessages, owner)

	owner.Messages = resampleMarker(owner.Messages, discoveredMessages)

	//Now we have a list lets set the ones that need resampling for crawling, leave the others as fetched. If the post doesn't exist any more mark it deleted
	sm.UpdateStatePage(*owner)

	for _, message := range owner.Messages {
		if message.Status != "fetched" && message.Status != "deleted" {

			if outlinks, err := processor.ProcessMessage(tdlibClient, message.MessageId, message.ChatId, info, crawlID, channelUsername, &sm, cfg); err != nil {
				log.Error().Err(err).Msgf("Error processing message %d", message.MessageId)
				sm.UpdateStateMessage(message.MessageId, message.ChatId, owner, "failed")
			} else {
				sm.UpdateStateMessage(message.MessageId, message.ChatId, owner, "fetched")
				for _, o := range outlinks {
					page := &state.Page{
						URL:      o,
						Status:   "unfetched",
						ParentID: owner.ID,
						ID:       uuid.New().String(),
						Depth:    owner.Depth + 1,
					}
					discoveredChannels = append(discoveredChannels, page)
				}
			}
		}

	}

	owner.Status = "fetched"
	sm.UpdateStatePage(*owner)
	return discoveredChannels, nil
}

func resampleMarker(messages []state.Message, discoveredMessages []state.Message) []state.Message {
	discoveredMap := make(map[string]bool)
	for _, msg := range discoveredMessages {
		key := fmt.Sprintf("%d_%d", msg.ChatId, msg.MessageId)
		discoveredMap[key] = true
	}

	// Process each message in the original messages slice
	for i := range messages {
		key := fmt.Sprintf("%d_%d", messages[i].ChatId, messages[i].MessageId)

		// If message exists in discoveredMessages, mark as unfetched for re-processing
		if discoveredMap[key] {
			messages[i].Status = "resample"
		} else {
			// If message doesn't exist in discoveredMessages, mark as deleted
			messages[i].Status = "deleted"
		}
	}

	return messages
}

func addNewMessages(discoveredMessages []state.Message, owner *state.Page) []state.Message {
	var newMessages []state.Message
	existingMessages := make(map[string]bool)

	// Create a map of existing messages by combining chat ID and message ID
	// for efficient lookup
	for _, existingMsg := range owner.Messages {
		key := fmt.Sprintf("%d_%d", existingMsg.ChatId, existingMsg.MessageId)
		existingMessages[key] = true
	}

	// Process discovered messages
	for i := range discoveredMessages {
		msg := discoveredMessages[i]
		key := fmt.Sprintf("%d_%d", msg.ChatId, msg.MessageId)

		// If the message doesn't already exist, add it to the new messages list
		if !existingMessages[key] {
			// Add a pointer to the message to the new messages list
			newMessages = append(newMessages, discoveredMessages[i])
		}
	}

	return newMessages
}

// fetchMessages retrieves a batch of messages from a chat
func fetchMessages(tdlibClient crawler.TDLibClient, chatID int64, fromMessageID int64) ([]*client.Message, error) {
	chatHistory, err := tdlibClient.GetChatHistory(&client.GetChatHistoryRequest{
		ChatId:        chatID,
		Limit:         100,
		FromMessageId: fromMessageID,
	})

	if err != nil {
		log.Error().Stack().Err(err).Msg("Failed to get chat history")
		return nil, err
	}

	return chatHistory.Messages, nil
}

// processMessage processes a single message
func processMessage(tdlibClient crawler.TDLibClient, messageId int64, chatId int64, info *channelInfo, crawlID, channelUsername string, sm state.StateManager, cfg common.CrawlerConfig) ([]string, error) {
	// Get detailed message info
	detailedMessage, err := tdlibClient.GetMessage(&client.GetMessageRequest{
		MessageId: messageId,
		ChatId:    chatId,
	})
	if err != nil {
		log.Error().Stack().Err(err).Msgf("Failed to get detailed message %d", messageId)
		return nil, err
	}

	// Get message link
	messageLink, err := tdlibClient.GetMessageLink(&client.GetMessageLinkRequest{
		ChatId:    chatId,
		MessageId: messageId,
	})
	if err != nil {
		log.Warn().Err(err).Msgf("Failed to get link for message %d", messageId)
		// Continue anyway, this is not critical
	}

	// Parse and store the message
	post, err := telegramhelper.ParseMessage(
		crawlID,
		detailedMessage,
		messageLink,
		info.chatDetails,
		info.supergroup,
		info.supergroupInfo,
		int(info.messageCount),
		int(info.totalViews),
		channelUsername,
		tdlibClient,
		sm,
		cfg,
	)

	if err != nil {
		log.Error().Stack().Err(err).Msgf("Failed to parse message %d", messageId)
		return nil, err
	}

	return post.Outlinks, nil
}

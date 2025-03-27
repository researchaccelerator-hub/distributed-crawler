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
	"runtime"
	"time"
)

func Connect(storagePrefix string, cfg common.CrawlerConfig) (crawler.TDLibClient, error) {
	// Initialize Telegram client
	service := &telegramhelper.RealTelegramService{}
	tdlibClient, err := service.InitializeClientWithConfig(storagePrefix, cfg)
	if err != nil {
		log.Error().Err(err).Stack().Msg("Failed to initialize Telegram client")
		return nil, err
	}

	// Ensure tdlibClient is closed after the function finishes
	//defer closeClient(tdlibClient)

	return tdlibClient, nil
}

// Run connects to a Telegram channel and crawls its messages.
func RunForChannel(tdlibClient crawler.TDLibClient, p *state.Page, storagePrefix string, sm state.StateManagementInterface, cfg common.CrawlerConfig) ([]*state.Page, error) {
	_, err := tdlibClient.Close()
	if err != nil {
		return nil, err
	}

	tdlibClient = nil
	runtime.GC()
	time.Sleep(10)
	tdlibClient, err = Connect(cfg.StorageRoot, cfg)
	if err != nil {
		return nil, err
	}
	// Get channel information
	channelInfo, messages, err := getChannelInfo(tdlibClient, p, cfg)
	if err != nil {
		return nil, err
	}
	active, err := isChannelActiveWithinPeriod(tdlibClient, channelInfo.chatDetails.Id, cfg.PostRecency)
	if err != nil {
		return nil, err
	}
	if !active || channelInfo.messageCount == 0 || (cfg.MinUsers > 0 && channelInfo.memberCount < int32(cfg.MinUsers)) {
		log.Info().Msg("Not enough members in the channel, considering it private and skipping.")
		p.Status = "deadend"
		err := sm.SaveState()
		if err != nil {
			return nil, err
		}
		return nil, nil
	}

	// Process all messages in the channel
	discoveredChannels, err := processAllMessages(tdlibClient, channelInfo, messages, cfg.CrawlID, p.URL, sm, p, cfg)
	if err != nil {
		return nil, err
	}

	return discoveredChannels, nil
}

func getLatestMessageTime(tdlibClient crawler.TDLibClient, chatID int64) (time.Time, error) {
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

func isChannelActiveWithinPeriod(tdlibClient crawler.TDLibClient, chatID int64, cutoffTime time.Time) (bool, error) {
	latestMessageTime, err := getLatestMessageTime(tdlibClient, chatID)
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
type TotalViewsGetter func(client crawler.TDLibClient, messages []*client.Message, chatID int64, channelUsername string) (int, error)

// MessageCountGetter is a function type for retrieving message count
type MessageCountGetter func(client crawler.TDLibClient, messages []*client.Message, chatID int64, channelUsername string) (int, error)

// MemberCountGetter is a function type for retrieving message count
type MemberCountGetter func(client crawler.TDLibClient, channelUsername string) (int, error)

func getChannelInfo(tdlibClient crawler.TDLibClient, page *state.Page, cfg common.CrawlerConfig) (*channelInfo, []*client.Message, error) {
	return getChannelInfoWithDeps(
		tdlibClient,
		page,
		telegramhelper.GetTotalChannelViews,
		telegramhelper.GetMessageCount,
		telegramhelper.GetChannelMemberCount,
		cfg,
	)
}

// getChannelInfoWithDeps is the dependency-injected version of getChannelInfo
func getChannelInfoWithDeps(
	tdlibClient crawler.TDLibClient,
	page *state.Page,
	getTotalViewsFn TotalViewsGetter,
	getMessageCountFn MessageCountGetter,
	getMemberCountFn MemberCountGetter,
	cfg common.CrawlerConfig,
) (*channelInfo, []*client.Message, error) {
	// Search for the channel
	chat, err := tdlibClient.SearchPublicChat(&client.SearchPublicChatRequest{
		Username: page.URL,
	})
	if err != nil {
		log.Error().Err(err).Stack().Msgf("Failed to find channel: %v", page.URL)
		return nil, nil, err
	}

	chatDetails, err := tdlibClient.GetChat(&client.GetChatRequest{
		ChatId: chat.Id,
	})
	if err != nil {
		log.Error().Err(err).Stack().Msgf("Failed to get chat details for: %v", page.URL)
		return nil, nil, err
	}

	mess, err := telegramhelper.FetchChannelMessages(tdlibClient, chat.Id, page, cfg.MinPostDate, cfg.MaxPosts)

	// Get channel stats
	totalViews := 0
	if getTotalViewsFn != nil {
		totalViewsVal, err := getTotalViewsFn(tdlibClient, mess, chat.Id, page.URL)
		if err != nil {
			log.Warn().Err(err).Msgf("Failed to get total views for channel: %v", page.URL)
			// Continue anyway, this is not critical
		} else {
			totalViews = totalViewsVal
		}
	}

	messageCount := 0
	if getMessageCountFn != nil {
		messageCountVal, err := getMessageCountFn(tdlibClient, mess, chat.Id, page.URL)
		if err != nil {
			log.Warn().Err(err).Msgf("Failed to get message count for channel: %v", page.URL)
			// Continue anyway, this is not critical
		} else {
			messageCount = messageCountVal
		}
	}

	memberCount := 0
	if getMemberCountFn != nil {
		memberCountVal, err := getMemberCountFn(tdlibClient, page.URL)
		if err != nil {
			log.Warn().Err(err).Msgf("Failed to get member count for channel: %v", page.URL)
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
				log.Warn().Err(err).Msgf("Failed to get supergroup info for: %v", page.URL)
				// Continue anyway, this is not critical
			}

			if supergroup != nil {
				req := &client.GetSupergroupFullInfoRequest{
					SupergroupId: supergroup.Id,
				}
				supergroupInfo, err = tdlibClient.GetSupergroupFullInfo(req)
				if err != nil {
					log.Warn().Err(err).Msgf("Failed to get supergroup full info for: %v", page.URL)
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
	}, mess, nil
}

type MessageFetcher interface {
	FetchMessages(tdlibClient crawler.TDLibClient, chatID int64, fromMessageID int64) ([]*client.Message, error)
}

type DefaultMessageFetcher struct{}

type MessageProcessor interface {
	// ProcessMessage processes a single Telegram message.
	ProcessMessage(tdlibClient crawler.TDLibClient, message *client.Message, messageId int64, chatId int64, info *channelInfo, crawlID string, channelUsername string, sm *state.StateManagementInterface, cfg common.CrawlerConfig) ([]string, error)
}

// DefaultMessageProcessor implements the MessageProcessor interface using the default processMessage function
type DefaultMessageProcessor struct{}

// ProcessMessage implements the MessageProcessor interface
func (p *DefaultMessageProcessor) ProcessMessage(tdlibClient crawler.TDLibClient, message *client.Message, messageId int64, chatId int64, info *channelInfo, crawlID string, channelUsername string, sm *state.StateManagementInterface, cfg common.CrawlerConfig) ([]string, error) {
	return processMessage(tdlibClient, message, messageId, chatId, info, crawlID, channelUsername, *sm, cfg)
}

// processAllMessages retrieves and processes all messages from a channel
func processAllMessages(tdlibClient crawler.TDLibClient, info *channelInfo, messages []*client.Message, crawlID, channelUsername string, sm state.StateManagementInterface, owner *state.Page, cfg common.CrawlerConfig) ([]*state.Page, error) {
	processor := &DefaultMessageProcessor{}
	return processAllMessagesWithProcessor(tdlibClient, info, messages, crawlID, channelUsername, sm, processor, owner, cfg)

}

// processAllMessagesWithProcessor retrieves and processes all messages from a channel.
// This function fetches messages in batches, starting from the most recent message
// and working backward through the history. It uses the provided MessageProcessor
// to process each message, allowing for customized message handling.
func processAllMessagesWithProcessor(
	tdlibClient crawler.TDLibClient,
	info *channelInfo,
	messages []*client.Message,
	crawlID,
	channelUsername string,
	sm state.StateManagementInterface,
	processor MessageProcessor, owner *state.Page, cfg common.CrawlerConfig) ([]*state.Page, error) {

	discoveredChannels := make([]*state.Page, 0)
	discoveredMessages := make([]state.Message, 0)

	// Process messages
	for _, message := range messages {
		m := state.Message{
			ChatID:    message.ChatId,
			MessageID: message.Id,
			Status:    "unfetched",
			PageID:    owner.ID,
		}
		discoveredMessages = append(discoveredMessages, m)
	}

	owner.Messages = addNewMessages(discoveredMessages, owner)

	owner.Messages = resampleMarker(owner.Messages, discoveredMessages)

	//Now we have a list lets set the ones that need resampling for crawling, leave the others as fetched. If the post doesn't exist any more mark it deleted
	err := sm.UpdatePage(*owner)
	if err != nil {
		return nil, err
	}
	var processErrors []error
	for _, message := range owner.Messages {
		if message.Status != "fetched" && message.Status != "deleted" {
			var discMessage *client.Message
			for _, m := range messages {
				if m.Id == message.MessageID {
					discMessage = m
					break
				}
			}

			// Skip messages that don't exist in the latest fetch
			if discMessage == nil {
				log.Warn().Msgf("Message %d not found in latest fetch, marking as deleted", message.MessageID)
				sm.UpdateMessage(owner.ID, message.ChatID, message.MessageID, "deleted")
				continue
			}

			// Try to process the message, but continue even if it fails
			outlinks, err := processor.ProcessMessage(tdlibClient, discMessage, message.MessageID, message.ChatID, info, crawlID, channelUsername, &sm, cfg)

			if err != nil {
				log.Error().Err(err).Msgf("Error processing message %d", message.MessageID)
				processErrors = append(processErrors, err)
				sm.UpdateMessage(owner.ID, message.MessageID, message.ChatID, "failed")
			} else {
				sm.UpdateMessage(owner.ID, message.MessageID, message.ChatID, "fetched")

				if outlinks != nil {
					// Get the current channel URL from the owner's URL
					currentChannelURL := owner.URL

					for _, o := range outlinks {
						// Skip if the outlink is the same as the current channel
						if o == currentChannelURL {
							log.Debug().Msgf("Skipping self-reference outlink: %s", o)
							continue
						}

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
	}

	owner.Status = "fetched"
	err = sm.UpdatePage(*owner)
	if err != nil {
		return nil, err
	}
	return discoveredChannels, nil
}

func resampleMarker(messages []state.Message, discoveredMessages []state.Message) []state.Message {
	discoveredMap := make(map[string]bool)
	for _, msg := range discoveredMessages {
		key := fmt.Sprintf("%d_%d", msg.ChatID, msg.MessageID)
		discoveredMap[key] = true
	}

	// Process each message in the original messages slice
	for i := range messages {
		key := fmt.Sprintf("%d_%d", messages[i].ChatID, messages[i].MessageID)

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
		key := fmt.Sprintf("%d_%d", existingMsg.ChatID, existingMsg.MessageID)
		existingMessages[key] = true
	}

	// Process discovered messages
	for i := range discoveredMessages {
		msg := discoveredMessages[i]
		key := fmt.Sprintf("%d_%d", msg.ChatID, msg.MessageID)

		// If the message doesn't already exist, add it to the new messages list
		if !existingMessages[key] {
			// Add a pointer to the message to the new messages list
			newMessages = append(newMessages, discoveredMessages[i])
		}
	}

	return newMessages
}

// processMessage processes a single message
func processMessage(tdlibClient crawler.TDLibClient, message *client.Message, messageId int64, chatId int64, info *channelInfo, crawlID, channelUsername string, sm state.StateManagementInterface, cfg common.CrawlerConfig) ([]string, error) {
	// Add a defer/recover block at this level to catch any panics
	// This ensures we can continue processing other messages even if this one fails
	var err error
	defer func() {
		if r := recover(); r != nil {
			log.Error().Msgf("Recovered from panic in processMessage for message %d in channel %s: %v",
				messageId, channelUsername, r)
			err = fmt.Errorf("panic occurred while processing message %d: %v", messageId, r)
			// Return empty outlinks to continue processing other messages
		}
	}()
	// Get message link - handle this error specifically
	var messageLink *client.MessageLink
	messageLink, err = tdlibClient.GetMessageLink(&client.GetMessageLinkRequest{
		ChatId:    chatId,
		MessageId: messageId,
	})
	if err != nil {
		log.Warn().Err(err).Msgf("Failed to get link for message %d", messageId)
		// Instead of continuing, return an empty slice and the error
		// This allows the caller to update the message status and continue
		return []string{}, fmt.Errorf("failed to get message link: %w", err)
	}

	// Parse and store the message
	if messageLink != nil {
		// Parse and store the message
		post, parseErr := telegramhelper.ParseMessage(
			crawlID,
			message,
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

		if parseErr != nil {
			log.Error().Stack().Err(parseErr).Msgf("Failed to parse message %d", messageId)
			return []string{}, parseErr
		}

		return post.Outlinks, nil
	}

	return []string{}, fmt.Errorf("could not process message %d: no message link available", messageId)
}

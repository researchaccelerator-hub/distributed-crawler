// Package crawl implements the core functionality for crawling Telegram channels.
// It handles connection management, message processing, and channel traversal
// to extract and store data from Telegram channels in a structured format.
package crawl

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/model"

	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/researchaccelerator-hub/telegram-scraper/telegramhelper"
	"github.com/rs/zerolog/log"
	"github.com/zelenin/go-tdlib/client"
)

// Global connection pool
var connectionPool *telegramhelper.ConnectionPool
var poolMu sync.Mutex

// InitConnectionPool initializes the global connection pool for TDLib clients.
// This pool is used to manage and reuse Telegram client connections efficiently,
// minimizing the overhead of creating new connections for each channel.
//
// Parameters:
//   - maxSize: The maximum number of concurrent connections to maintain
//   - storagePrefix: Path prefix for storing TDLib databases and files
//   - cfg: Common crawler configuration for all connections
//
// The function will create a new connection pool only if one doesn't already exist.
func InitConnectionPool(maxSize int, storagePrefix string, cfg common.CrawlerConfig) {
	poolMu.Lock()
	defer poolMu.Unlock()

	if connectionPool == nil {
		poolConfig := telegramhelper.ConnectionPoolConfig{
			PoolSize:          maxSize,
			TDLibDatabaseURLs: cfg.TDLibDatabaseURLs,
			Verbosity:         cfg.TDLibVerbosity,
			StorageRoot:       storagePrefix,
		}

		pool, err := telegramhelper.NewConnectionPool(poolConfig)
		if err != nil {
			log.Error().Err(err).Msg("Failed to initialize connection pool")
			return
		}

		connectionPool = pool
		log.Info().Int("maxSize", maxSize).Msg("Initialized connection pool")
	}
}

// Connect creates a new standalone Telegram client connection outside the connection pool.
// This is typically used as a fallback when the connection pool is exhausted or not initialized.
//
// Parameters:
//   - storagePrefix: Path prefix for storing TDLib databases and files
//   - cfg: Configuration for the crawler and TDLib client
//
// Returns:
//   - An initialized TDLib client connection
//   - An error if client initialization fails
//
// Unlike pooled connections, clients created with this function need to be explicitly
// closed when they're no longer needed to avoid resource leaks.
func Connect(storagePrefix string, cfg common.CrawlerConfig) (crawler.TDLibClient, error) {
	// Initialize Telegram client
	service := &telegramhelper.RealTelegramService{}
	tdlibClient, err := service.InitializeClientWithConfig(storagePrefix, cfg)
	if err != nil {
		log.Error().Err(err).Stack().Msg("Failed to initialize Telegram client")
		return nil, err
	}

	return tdlibClient, nil
}

// GetConnectionFromPool retrieves a TDLib client connection from the connection pool.
// This is the primary method for obtaining client connections for channel crawling.
//
// Parameters:
//   - ctx: Context for potential cancellation or timeout of the operation
//
// Returns:
//   - An initialized TDLib client connection
//   - A unique connection ID that must be used when releasing the connection
//   - An error if the pool is not initialized or if no connections are available
//
// The function is thread-safe with mutex protection. If the pool is not initialized,
// it returns an error prompting the caller to initialize the pool first or use
// a standalone connection.
func GetConnectionFromPool(ctx context.Context) (crawler.TDLibClient, string, error) {
	poolMu.Lock()
	defer poolMu.Unlock()

	if connectionPool == nil {
		return nil, "", fmt.Errorf("connection pool not initialized")
	}

	return connectionPool.GetConnection(ctx)
}

// ReleaseConnectionToPool returns a previously obtained connection back to the pool.
// This makes the connection available for reuse by other crawling operations.
//
// Parameters:
//   - connID: The unique identifier of the connection to release
//
// The function is thread-safe with mutex protection and safely handles the case
// where the pool might have been closed after the connection was obtained.
// It's critical to release connections to avoid resource exhaustion.
func ReleaseConnectionToPool(connID string) {
	poolMu.Lock()
	defer poolMu.Unlock()

	if connectionPool != nil {
		connectionPool.ReleaseConnection(connID)
	}
}

// CloseConnectionPool gracefully shuts down all connections in the pool and
// releases associated resources. This should be called during application
// shutdown or when the pool is no longer needed.
//
// The function is thread-safe with mutex protection. After calling this function,
// the pool is no longer usable until re-initialized with InitConnectionPool.
func CloseConnectionPool() {
	poolMu.Lock()
	defer poolMu.Unlock()

	if connectionPool != nil {
		connectionPool.Close()
		connectionPool = nil
	}
}

// IsConnectionPoolInitialized checks if the global connection pool has been
// initialized and is available for use.
//
// Returns:
//   - true if the connection pool is initialized
//   - false if the connection pool is nil (not initialized)
//
// This function is thread-safe with mutex protection and can be used to
// determine whether to call InitConnectionPool or use standalone connections.
func IsConnectionPoolInitialized() bool {
	poolMu.Lock()
	defer poolMu.Unlock()

	return connectionPool != nil
}

// GetConnectionPoolStats retrieves statistics about the current state of the
// connection pool, including available connections, connections in use, and
// maximum pool size.
//
// Returns:
//   - A map containing statistics about the connection pool:
//   - "available": Number of connections currently available in the pool
//   - "inUse": Number of connections currently checked out from the pool
//   - "maxSize": Maximum size of the pool
//   - "initialized": 1 if the pool is initialized, 0 otherwise
//
// The function is thread-safe with mutex protection and safely handles the case
// where the pool is not initialized by returning zeroed statistics.
func GetConnectionPoolStats() map[string]int {
	poolMu.Lock()
	defer poolMu.Unlock()

	if connectionPool == nil {
		return map[string]int{
			"available":   0,
			"inUse":       0,
			"maxSize":     0,
			"initialized": 0,
		}
	}

	stats := connectionPool.Stats()
	stats["initialized"] = 1
	return stats
}

// RunForChannelWithPool connects to a Telegram channel and crawls its messages.
// This version uses the connection pool for more efficient client management,
// obtaining a client from the pool, processing the channel, and then returning
// the client to the pool for reuse.
//
// Parameters:
//   - ctx: Context for potential cancellation of the operation
//   - p: The page (channel) to process
//   - storagePrefix: Path prefix for storing TDLib databases and files
//   - sm: State manager interface for storing crawler state
//   - cfg: Configuration for the crawler operation
//
// Returns:
//   - A slice of discovered channel pages that were linked from this channel
//   - An error if any step of the crawling process fails
//
// The function first tries to get a connection from the pool. If the pool is
// exhausted or not initialized, it falls back to creating a new connection.
// After processing, pooled connections are returned to the pool, while
// fallback connections are closed.
func RunForChannelWithPool(ctx context.Context, p *state.Page, storagePrefix string, sm state.StateManagementInterface, cfg common.CrawlerConfig) ([]*state.Page, error) {
	// Get a client from the connection pool
	tdlibClient, connID, err := GetConnectionFromPool(ctx)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to get client from pool for channel %s", p.URL)
		// Fall back to creating a new connection if pool is exhausted or not initialized
		tdlibClient, err = Connect(storagePrefix, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to get client connection: %w", err)
		}
		// Make sure to close this non-pooled connection when done
		defer closeClient(tdlibClient)
	} else {
		// Ensure we return the pooled connection when done
		defer ReleaseConnectionToPool(connID)
	}
	p.ConnectionID = connID
	log.Info().Str("connection_id", p.ConnectionID).Str("channel", p.URL).Msg("random-walk-connection-pool: Started connection")
	// Continue with the regular channel processing
	return RunForChannel(tdlibClient, p, storagePrefix, sm, cfg)
}

// RunForChannel processes a single Telegram channel using the provided TDLib client.
// It retrieves channel information, verifies activity requirements, and processes all
// messages in the channel, storing data and discovering linked channels.
//
// Parameters:
//   - tdlibClient: An initialized TDLib client connection
//   - p: The page (channel) to process
//   - storagePrefix: Path prefix for storing TDLib databases and files
//   - sm: State manager interface for storing crawler state
//   - cfg: Configuration for the crawler operation
//
// Returns:
//   - A slice of discovered channel pages that were linked from this channel
//   - An error if any step of the crawling process fails
//
// The function applies filtering rules based on channel activity, message count,
// and member count to determine whether the channel should be fully processed.
func RunForChannel(tdlibClient crawler.TDLibClient, p *state.Page, storagePrefix string, sm state.StateManagementInterface, cfg common.CrawlerConfig) ([]*state.Page, error) {

	// Get channel information
	channelInfo, messages, err := getChannelInfo(tdlibClient, p, cfg)
	if err != nil {
		return nil, err
	}
	channelData := &model.ChannelData{
		ChannelID:   strconv.FormatInt(channelInfo.chat.Id, 10),
		ChannelName: channelInfo.chat.Title,
		// ChannelDescription unavailable
		ChannelProfileImage: channelInfo.chat.Photo.Extra,
		ChannelEngagementData: model.EngagementData{
			FollowerCount: int(channelInfo.memberCount),
			// FollowingCount unavailable
			// LikeCount unavailable
			PostCount:  int(channelInfo.messageCount),
			ViewsCount: int(channelInfo.totalViews),
			// Comment count unavailable
			// Share count unavailable
		},
		ChannelURLExternal: fmt.Sprintf("https://t.me/%s", p.URL),
		ChannelURL:         fmt.Sprintf("https://t.me/%s", p.URL),
		// CountryCode unavailable
		// PublishedAt unavailable
	}

	validationResult := cfg.NullValidator.ValidateChannelData(channelData)

	if !validationResult.Valid {
		err = fmt.Errorf("Channel %s is missing critical fields: %v", p.URL, validationResult.Errors)
		log.Error().Strs("validation_errors", validationResult.Errors).Msg("Null-RunForChannel: Missing critical fields in youtube channel data. Skipping")
		return nil, err
	}

	// store channel data, as posts are not saved in random-walk
	if cfg.SamplingMethod == "random-walk" {
		err := sm.StoreChannelData(p.URL, channelData)
		if err != nil {
			log.Error().Err(err).Msg("random-walk-channel-info: failed to store channel data")
		}
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

// getLatestMessageTime retrieves the timestamp of the most recent message in a chat.
// This is used to determine if a channel is active within a specified time period.
//
// Parameters:
//   - tdlibClient: An initialized TDLib client connection
//   - chatID: The ID of the chat (channel) to check
//
// Returns:
//   - The timestamp of the most recent message as a time.Time
//   - An error if no messages are found or if the fetch operation fails
//
// The function queries for a single message (the most recent one) and extracts
// its timestamp. This is more efficient than fetching multiple messages when
// only the latest activity time is needed.
func getLatestMessageTime(tdlibClient crawler.TDLibClient, chatID int64) (time.Time, error) {
	// Fetch the most recent message
	// fromMessageID=0 means from the latest message
	// offset=0 means no offset from the chosen message
	// limit=1 means get only one message
	// Use 0 for the fromMessageID parameter to get the most recent message

	// TODO: Replace with client level rate limiting
	sleepMS := 1600 + rand.IntN(900)
	log.Debug().Int("sleep_ms", sleepMS).Str("api_call", "GetChatHistory").Msg("Telegram API Call Sleep")
	time.Sleep(time.Duration(sleepMS) * time.Millisecond)

	getChatHistoryStart := time.Now()
	messages, err := tdlibClient.GetChatHistory(&client.GetChatHistoryRequest{
		ChatId:        chatID,
		FromMessageId: 0, // 0 means get from the latest message
		Offset:        0, // No offset from the starting point
		Limit:         1, // Only need 1 message (the latest one)
		OnlyLocal:     false,
	})
	telegramhelper.DetectCacheOrServer(getChatHistoryStart, "GetChatHistory")

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

// isChannelActiveWithinPeriod determines if a channel has had any activity
// (new messages) after the specified cutoff time.
//
// Parameters:
//   - tdlibClient: An initialized TDLib client connection
//   - chatID: The ID of the chat (channel) to check
//   - cutoffTime: The reference time to compare against; channels with
//     messages newer than this time are considered active
//
// Returns:
//   - true if the channel has activity after the cutoff time
//   - false if the channel has no activity after the cutoff time
//   - an error if retrieving the message timestamp fails
//
// This function is used to filter out inactive or abandoned channels
// that haven't posted within the configured time window. This helps
// focus crawling resources on currently active channels.
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

// channelInfo holds all necessary data about a Telegram channel.
// This struct centralizes all channel-related information collected during crawling,
// including basic chat data, supergroup information if applicable, and key statistics.
type channelInfo struct {
	chat           *client.Chat               // Basic chat information
	chatDetails    *client.Chat               // Detailed chat information
	supergroup     *client.Supergroup         // Supergroup data if the channel is a supergroup
	supergroupInfo *client.SupergroupFullInfo // Detailed supergroup information
	totalViews     int32                      // Total view count across channel messages
	messageCount   int32                      // Total number of messages in the channel
	memberCount    int32                      // Total number of members/subscribers
}

// TotalViewsGetter is a function type for retrieving total view count across
// all messages in a channel. This abstraction enables dependency injection for testing.
//
// Parameters:
//   - client: TDLib client connection
//   - messages: Array of messages to analyze
//   - chatID: ID of the chat/channel
//   - channelUsername: Username of the channel (for logging)
//
// Returns:
//   - Total number of views across all messages
//   - Error if view count calculation fails
type TotalViewsGetter func(client crawler.TDLibClient, messages []*client.Message, chatID int64, channelUsername string) (int, error)

// MessageCountGetter is a function type for retrieving the total message count
// in a channel. This abstraction enables dependency injection for testing.
//
// Parameters:
//   - client: TDLib client connection
//   - messages: Array of messages to analyze
//   - chatID: ID of the chat/channel
//   - channelUsername: Username of the channel (for logging)
//
// Returns:
//   - Total number of messages in the channel
//   - Error if message count retrieval fails
type MessageCountGetter func(client crawler.TDLibClient, messages []*client.Message, chatID int64, channelUsername string) (int, error)

// MemberCountGetter is a function type for retrieving the number of members/subscribers
// in a channel. This abstraction enables dependency injection for testing.
//
// Parameters:
//   - client: TDLib client connection
//   - channelId: ChatID of the channel to to check
//
// Returns:
//   - Number of channel members/subscribers
//   - Error if member count retrieval fails
//

type MemberCountGetter func(client crawler.TDLibClient, channelId int64) (int, error)

// getChannelInfo retrieves comprehensive information about a Telegram channel.
// This is a convenience wrapper around getChannelInfoWithDeps that supplies
// the standard implementations of the dependency functions.
//
// Parameters:
//   - tdlibClient: An initialized TDLib client connection
//   - page: State representation of the channel to fetch
//   - cfg: Configuration settings for the crawler
//
// Returns:
//   - A populated channelInfo struct containing all retrieved channel data
//   - A slice of messages from the channel
//   - An error if critical operations fail
//
// This function uses the standard implementations for retrieving views, message count,
// and member count from the telegramhelper package. For testing or custom implementations,
// use getChannelInfoWithDeps directly.
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

// getChannelInfoWithDeps is the dependency-injected version of getChannelInfo.
// This function retrieves comprehensive information about a Telegram channel,
// including basic details, message content, and various statistics.
//
// Parameters:
//   - tdlibClient: An initialized TDLib client connection
//   - page: State representation of the channel to fetch
//   - getTotalViewsFn: Function to retrieve total view count for the channel
//   - getMessageCountFn: Function to retrieve total message count for the channel
//   - getMemberCountFn: Function to retrieve member count for the channel
//   - cfg: Configuration settings for the crawler
//
// Returns:
//   - A populated channelInfo struct containing all retrieved channel data
//   - A slice of messages from the channel
//   - An error if critical operations fail
//
// The function follows a graceful degradation approach - if non-critical stats (views,
// message count, member count) can't be retrieved, it continues with partial information
// rather than failing completely. This improves crawler resilience while still collecting
// as much data as possible.
//
// The dependency injection pattern allows for easier testing by substituting mock
// implementations of the stat retrieval functions.
func getChannelInfoWithDeps(
	tdlibClient crawler.TDLibClient,
	page *state.Page,
	getTotalViewsFn TotalViewsGetter,
	getMessageCountFn MessageCountGetter,
	getMemberCountFn MemberCountGetter,
	cfg common.CrawlerConfig,
) (*channelInfo, []*client.Message, error) {

	// TODO: Replace with client level rate limiting
	sleepMS := 9600 + rand.IntN(900)
	log.Debug().Int("sleep_ms", sleepMS).Str("api_call", "SearchPublicChat").Msg("Telegram API Call Sleep")
	time.Sleep(time.Duration(sleepMS) * time.Millisecond)

	searchPublicChatStart := time.Now()
	// Search for the channel
	chat, err := tdlibClient.SearchPublicChat(&client.SearchPublicChatRequest{
		Username: page.URL,
	})
	telegramhelper.DetectCacheOrServer(searchPublicChatStart, "SearchPublicChat")
	if err != nil {
		log.Error().Err(err).Stack().Msgf("Failed to find channel: %v", page.URL)
		return nil, nil, err
	}
	// should be cached. not sleeping
	getChatStart := time.Now()
	chatDetails, err := tdlibClient.GetChat(&client.GetChatRequest{
		ChatId: chat.Id,
	})
	telegramhelper.DetectCacheOrServer(getChatStart, "GetChat")
	if err != nil {
		log.Error().Err(err).Stack().Msgf("Failed to get chat details for: %v", page.URL)
		return nil, nil, err
	}

	var mess []*client.Message
	if !cfg.DateBetweenMin.IsZero() && !cfg.DateBetweenMax.IsZero() {
		mess, err = telegramhelper.FetchChannelMessagesWithSampling(tdlibClient, chat.Id, page, cfg.DateBetweenMin, cfg.DateBetweenMax, cfg.MaxPosts, cfg.SampleSize)
	} else {
		mess, err = telegramhelper.FetchChannelMessages(tdlibClient, chat.Id, page, cfg.MinPostDate, cfg.MaxPosts)
	}

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
		// memberCountVal, err := getMemberCountFn(tdlibClient, page.URL)
		memberCountVal, err := getMemberCountFn(tdlibClient, chat.Id)
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

// MessageFetcher defines an interface for retrieving messages from a Telegram chat.
// This abstraction enables dependency injection for testing by allowing
// different implementations to be used in place of the actual TDLib client calls.
type MessageFetcher interface {
	// FetchMessages retrieves messages from a Telegram chat.
	//
	// Parameters:
	//   - tdlibClient: TDLib client connection
	//   - chatID: ID of the chat to fetch messages from
	//   - fromMessageID: Message ID to start fetching from (0 for latest)
	//
	// Returns:
	//   - A slice of fetched messages
	//   - An error if message fetching fails
	FetchMessages(tdlibClient crawler.TDLibClient, chatID int64, fromMessageID int64) ([]*client.Message, error)
}

// DefaultMessageFetcher is the standard implementation of the MessageFetcher interface.
// It uses the TDLib client directly to fetch messages from Telegram.
type DefaultMessageFetcher struct{}

// MessageProcessor defines an interface for processing Telegram messages.
// This abstraction allows for custom processing logic to be injected,
// facilitating testing and specialized processing workflows.
type MessageProcessor interface {
	// ProcessMessage processes a single Telegram message.
	//
	// Parameters:
	//   - tdlibClient: TDLib client connection
	//   - message: The message to process
	//   - messageId: ID of the message
	//   - chatId: ID of the chat containing the message
	//   - info: Channel information
	//   - crawlID: Identifier for the current crawl operation
	//   - channelUsername: Username of the channel being processed
	//   - sm: State management interface for persistent storage
	//   - cfg: Configuration settings for the crawler
	//
	// Returns:
	//   - A slice of outlink URLs discovered in the message
	//   - An error if message processing fails
	ProcessMessage(tdlibClient crawler.TDLibClient, message *client.Message, messageId int64, chatId int64, info *channelInfo, crawlID string, channelUsername string, sm *state.StateManagementInterface, cfg common.CrawlerConfig) ([]string, error)
}

// DefaultMessageProcessor implements the MessageProcessor interface using the default processMessage function.
// It serves as the standard message processor for the crawler, extracting content,
// metadata, and outlinks from Telegram messages.
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

// processAllMessagesWithProcessor processes all messages from a Telegram channel
// using the provided message processor. It coordinates the entire message processing
// workflow, including message status tracking, outlink discovery, and state updates.
//
// Parameters:
//   - tdlibClient: An initialized TDLib client connection
//   - info: Channel information including member count, view count, etc.
//   - messages: Array of retrieved messages from the channel
//   - crawlID: Identifier for the current crawl operation
//   - channelUsername: Username of the channel being processed
//   - sm: State manager for persistent storage of crawl progress
//   - processor: Implementation of MessageProcessor for custom message handling
//   - owner: Page (channel) that owns these messages
//   - cfg: Configuration settings for the crawler
//
// Returns:
//   - A slice of discovered channel pages from outlinks in processed messages
//   - An error if message processing encounters critical failures
//
// The function handles message status tracking (unfetched, fetching, fetched, deleted),
// processing each message with the provided processor, and collecting outlinks to
// other channels for further crawling.
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

	// random-walk structs
	discoveredEdges := make([]*state.EdgeRecord, 0)
	newChannels := make(map[string]bool, 0)
	oldChannels := make(map[string]bool, 0)
	invalidChannels := make(map[string]bool, 0)

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

	log.Info().
		Int("total_messages", len(owner.Messages)).
		Str("page_url", owner.URL).
		Str("page_id", owner.ID).
		Msg("Starting message processing after resampling")

	var processErrors []error
	var fetched, deleted, processed, failed int

	for _, message := range owner.Messages {
		log.Debug().
			Int64("chat_id", message.ChatID).
			Int64("message_id", message.MessageID).
			Str("status", message.Status).
			Str("page_id", message.PageID).
			Msg("Evaluating message for processing")

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
				log.Warn().
					Int64("chat_id", message.ChatID).
					Int64("message_id", message.MessageID).
					Str("page_id", message.PageID).
					Msg("Message not found in latest fetch, marking as deleted")
				sm.UpdateMessage(owner.ID, message.ChatID, message.MessageID, "deleted")
				deleted++
				continue
			}

			processed++
			log.Debug().
				Int64("chat_id", message.ChatID).
				Int64("message_id", message.MessageID).
				Str("status", message.Status).
				Str("page_id", message.PageID).
				Msg("Processing message")

			// Try to process the message, but continue even if it fails
			outlinks, err := processor.ProcessMessage(tdlibClient, discMessage, message.MessageID, message.ChatID, info, crawlID, channelUsername, &sm, cfg)

			if err != nil {
				log.Error().Err(err).
					Int64("chat_id", message.ChatID).
					Int64("message_id", message.MessageID).
					Str("page_id", message.PageID).
					Msg("Error processing message")
				processErrors = append(processErrors, err)
				sm.UpdateMessage(owner.ID, message.MessageID, message.ChatID, "failed")
				failed++
			} else {
				sm.UpdateMessage(owner.ID, message.MessageID, message.ChatID, "fetched")
				fetched++

				if outlinks != nil {
					// Get the current channel URL from the owner's URL
					currentChannelURL := owner.URL

					for _, o := range outlinks {
						// Skip if the outlink is the same as the current channel
						if o == currentChannelURL {
							log.Debug().Msgf("Skipping self-reference outlink: %s", o)
							continue
						}

						if cfg.SamplingMethod != "random-walk" {
							page := &state.Page{
								URL:      o,
								Status:   "unfetched",
								ParentID: owner.ID,
								ID:       uuid.New().String(),
								Depth:    owner.Depth + 1,
							}
							discoveredChannels = append(discoveredChannels, page)
						} else {
							// check if channel already found as new, old, or invalid and skip
							if _, ok := invalidChannels[o]; ok {
								log.Info().Str("channel", o).Msg("random-walk-filter: Channel already identified as invalid. Skipping")
								continue
							} else if _, ok := newChannels[o]; ok {
								log.Info().Str("channel", o).Msg("random-walk-filter: Channel already found as part of new channels. Skipping")
								continue
							} else if _, ok := oldChannels[o]; ok {
								log.Info().Str("channel", o).Msg("random-walk-filter: Channel already found as part of old channels. Skipping")
								continue
							}
							// determine if a previously discovered channel and act accordingly
							if sm.IsDiscoveredChannel(o) {
								oldChannels[o] = true
							} else {
								log.Info().Str("channel", o).Str("source_channel", owner.URL).Msg("random-walk-channel: Sleeping for 2 seconds then checking if valid public channel.")
								time.Sleep(2000 * time.Millisecond)
								chat, err := tdlibClient.SearchPublicChat(&client.SearchPublicChatRequest{
									Username: o,
								})
								if err != nil {
									log.Info().Err(err).Str("channel", o).Stack().Msg("random-walk-channel: Failed to find channel. Skipping")
									invalidChannels[o] = true
									continue
								}
								chatType := string(chat.Type.ChatTypeType())

								if chatType != "chatTypeSupergroup" {
									log.Info().Str("chat_type", chatType).Str("chat", o).Msg("random-walk-channel: Not a valid chat type. Skipping")
									invalidChannels[o] = true
									continue
								}
								log.Info().Str("channel", o).Str("source_channel", owner.URL).Msg("random-walk-channel: Adding channel to discovered channels")
								sm.AddDiscoveredChannel(o)
								newChannels[o] = true
							}
						}
					}
				}
			}
		}
	}

	// Log processing summary
	log.Info().
		Int("messages_processed", processed).
		Int("messages_fetched", fetched).
		Int("messages_deleted", deleted).
		Int("messages_failed", failed).
		Int("discovered_channels", len(discoveredChannels)).
		Str("page_url", owner.URL).
		Str("page_id", owner.ID).
		Msg("Message processing summary")

	// handle choosing a page for the next layer
	if cfg.SamplingMethod == "random-walk" {
		page := &state.Page{
			Status:   "unfetched",
			ParentID: owner.ID,
			ID:       uuid.New().String(),
			Depth:    owner.Depth + 1,
		}
		linkToFollow := &state.EdgeRecord{
			DiscoveryTime: time.Now(),
			SourceChannel: owner.URL,
			Skipped:       false,
		}

		walkback := false
		rndNum := -1
		newChannelCount := len(newChannels)
		if newChannelCount == 0 {
			walkback = true
		} else {
			rndNum = rand.IntN(100) + 1
		}

		log.Info().Int("walkback_rate", cfg.WalkbackRate).Int("random_num", rndNum).Bool("walkback", walkback).
			Int("new_channels", newChannelCount).Str("source_channel", owner.URL).Msg("random-walk-walkback: Walkback decision data")
		if walkback || cfg.WalkbackRate >= rndNum {
			linkToFollow.Walkback = true
			// get walkback channel, skipping new channels discovered on this page
			var walkbackURL string
			for {
				var randomErr error
				walkbackURL, randomErr = sm.GetRandomDiscoveredChannel()
				log.Info().Str("walkback_url", walkbackURL).Str("source_channel", owner.URL).Msg("random-walk-walkback: Random Walkback channel")
				if randomErr != nil {
					return nil, fmt.Errorf("random-walk-walkback: Unable to get url for walkback while processing channel %s. Skipping fetch", owner.URL)
				}
				if _, ok := newChannels[walkbackURL]; ok {
					log.Info().Str("channel", walkbackURL).Msg("random-walk-walkback: Invalid walkback. Part of new channels. Pulling another channel")
				} else if walkbackURL == owner.URL {
					log.Info().Str("channel", walkbackURL).Msg("random-walk-walkback: Invalid walkback. Same as source channel. Pulling another channel")
				} else {
					break
				}
			}
			page.URL = walkbackURL
		} else {
			linkToFollow.Walkback = false

			newChannelSlice := make([]string, 0, len(newChannels))
			for channel := range newChannels {
				newChannelSlice = append(newChannelSlice, channel)
			}
			page.URL = newChannelSlice[rand.IntN(len(newChannelSlice))]
			delete(newChannels, page.URL) // remaining items in map will be used to create skipped edges
		}
		linkToFollow.DestinationChannel = page.URL
		log.Info().Str("destination_channel", linkToFollow.DestinationChannel).Time("discovery_time", linkToFollow.DiscoveryTime).
			Bool("skipped", linkToFollow.Skipped).Str("source_channel", linkToFollow.SourceChannel).Bool("walkback", linkToFollow.Walkback).
			Msg("random-walk-edge: Adding edge to follow in next layer")
		discoveredEdges = append(discoveredEdges, linkToFollow)

		err := sm.AddPageToLayerBuffer(page)
		if err != nil {
			log.Error().Err(err).Msg("random-walk-layer: failed to load page to layer buffer")
		}
		// discoveredChannels = append(discoveredChannels, page)

		if len(newChannels) > 0 {
			log.Info().Int("new_channels", len(newChannels)).Msg("random-walk-edge: New channels found that will be skipped. Adding edge records")
			for channel := range newChannels {
				skippedLink := &state.EdgeRecord{
					DestinationChannel: channel,
					DiscoveryTime:      time.Now(),
					Skipped:            true,
					SourceChannel:      owner.URL,
					Walkback:           false,
				}
				log.Info().Str("destination_channel", skippedLink.DestinationChannel).Time("discovery_time", skippedLink.DiscoveryTime).
					Bool("skipped", skippedLink.Skipped).Str("source_channel", skippedLink.SourceChannel).Bool("walkback", skippedLink.Walkback).
					Msg("random-walk-edge: Adding skipped edge")
				discoveredEdges = append(discoveredEdges, skippedLink)
			}
		}
		log.Info().Str("source_channel", owner.URL).Int("discovered_edges_count", len(discoveredEdges)).Msg("random-walk-edge: Saving discovered edges")
		err = sm.SaveEdgeRecords(discoveredEdges)
		if err != nil {
			log.Err(err).Str("source_channel", owner.URL).Msg("random-walk-edge: Error saving discovered edges")
			return nil, err
		}
	}

	owner.Status = "fetched"
	err = sm.UpdatePage(*owner)
	if err != nil {
		return nil, err
	}

	return discoveredChannels, nil
}

// resampleMarker updates message statuses by comparing existing messages with
// newly discovered messages from the current crawl. This function helps identify
// which messages need reprocessing (still exist) and which ones are no longer
// available in the channel (deleted).
//
// Parameters:
//   - messages: The existing messages from previous crawls
//   - discoveredMessages: The messages discovered in the current crawl
//
// Returns:
//   - The updated slice of messages with statuses set to either "resample" or "deleted"
//
// The function creates a map of discovered messages for efficient lookup, then
// iterates through existing messages, marking them for resampling if they still
// exist in the current crawl, or as deleted if they're no longer present.
// This mechanism allows the crawler to track message lifecycle and avoid
// unnecessary processing of already processed or deleted messages.
//
// IMPORTANT: Messages that are already marked as "fetched" will NOT be marked for
// resampling. This prevents unnecessary reprocessing of messages when resuming a crawl.
func resampleMarker(messages []state.Message, discoveredMessages []state.Message) []state.Message {
	log.Debug().
		Int("existing_message_count", len(messages)).
		Int("discovered_message_count", len(discoveredMessages)).
		Msg("Starting message resample marking")

	// Build lookup map for efficient checking
	discoveredMap := make(map[string]bool)
	for _, msg := range discoveredMessages {
		key := fmt.Sprintf("%d_%d", msg.ChatID, msg.MessageID)
		discoveredMap[key] = true
	}

	var keptFetched, markedResample, markedDeleted int

	// Process each message in the original messages slice
	for i := range messages {
		key := fmt.Sprintf("%d_%d", messages[i].ChatID, messages[i].MessageID)
		originalStatus := messages[i].Status

		// Skip messages that are already marked as "fetched" - don't reprocess them
		if messages[i].Status == "fetched" {
			keptFetched++
			log.Debug().
				Int64("chat_id", messages[i].ChatID).
				Int64("message_id", messages[i].MessageID).
				Str("page_id", messages[i].PageID).
				Msg("Keeping message marked as fetched")
			continue
		}

		// If message exists in discoveredMessages, mark as unfetched for re-processing
		if discoveredMap[key] {
			messages[i].Status = "resample"
			markedResample++
			log.Debug().
				Int64("chat_id", messages[i].ChatID).
				Int64("message_id", messages[i].MessageID).
				Str("page_id", messages[i].PageID).
				Str("old_status", originalStatus).
				Msg("Marking message for resampling")
		} else {
			// If message doesn't exist in discoveredMessages, mark as deleted
			messages[i].Status = "deleted"
			markedDeleted++
			log.Debug().
				Int64("chat_id", messages[i].ChatID).
				Int64("message_id", messages[i].MessageID).
				Str("page_id", messages[i].PageID).
				Str("old_status", originalStatus).
				Msg("Marking message as deleted")
		}
	}

	log.Debug().
		Int("kept_fetched", keptFetched).
		Int("marked_resample", markedResample).
		Int("marked_deleted", markedDeleted).
		Msg("Message resample marking completed")

	return messages
}

// addNewMessages identifies and returns newly discovered messages that don't exist
// in the channel's current message collection. This ensures that only new messages
// are added to the processing queue, avoiding duplicate processing of messages.
//
// Parameters:
//   - discoveredMessages: A slice of newly discovered messages from the current crawl
//   - owner: The page (channel) that owns these messages
//
// Returns:
//   - A slice containing only the messages that don't already exist in owner.Messages
//
// The function creates an efficient lookup map of existing messages using a composite
// key of ChatID and MessageID, then filters the discovered messages to include only
// those not already present in the map. This optimizes message processing by avoiding
// redundant work on messages that have already been processed in previous crawls.
func addNewMessages(discoveredMessages []state.Message, owner *state.Page) []state.Message {
	log.Debug().
		Int("discovered_message_count", len(discoveredMessages)).
		Int("existing_message_count", len(owner.Messages)).
		Str("page_url", owner.URL).
		Str("page_id", owner.ID).
		Msg("Adding new messages to page")

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
			// Add the message to the new messages list
			newMessages = append(newMessages, discoveredMessages[i])
			log.Debug().
				Int64("chat_id", msg.ChatID).
				Int64("message_id", msg.MessageID).
				Str("page_id", owner.ID).
				Msg("Adding new message to collection")
		} else {
			log.Debug().
				Int64("chat_id", msg.ChatID).
				Int64("message_id", msg.MessageID).
				Str("page_id", owner.ID).
				Msg("Skipping already existing message")
		}
	}

	log.Debug().
		Int("new_message_count", len(newMessages)).
		Str("page_url", owner.URL).
		Msg("New messages identified for page")

	return newMessages
}

// processMessage processes a single Telegram message, extracting its content,
// metadata, and outlinks to other channels. It includes panic recovery to ensure
// that failures in processing individual messages don't disrupt the entire crawl.
//
// Parameters:
//   - tdlibClient: An initialized TDLib client connection
//   - message: The Telegram message to process
//   - messageId: ID of the message
//   - chatId: ID of the chat containing the message
//   - info: Channel information including member count, view count, etc.
//   - crawlID: Identifier for the current crawl operation
//   - channelUsername: Username of the channel being processed
//   - sm: State manager for persistent storage
//   - cfg: Configuration settings for the crawler
//
// Returns:
//   - A slice of outlink URLs discovered in the message
//   - An error if message processing fails
//
// The function handles message parsing, media download (if applicable),
// and outlink extraction for discovering more channels to crawl.
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

	// Get message link - handle this error specifically CACHED_CALL
	var messageLink *client.MessageLink
	getMessageLinkStart := time.Now()
	messageLink, err = tdlibClient.GetMessageLink(&client.GetMessageLinkRequest{
		ChatId:    chatId,
		MessageId: messageId,
	})
	cacheHit := telegramhelper.DetectCacheOrServer(getMessageLinkStart, "GetMessageLink")

	if !cacheHit {
		sleepMS := 600 + rand.IntN(900)
		log.Info().Int("sleep_ms", sleepMS).Str("api_call", "GetMessageLink").Msg("Retroactive Telegram API Call Sleep")
		time.Sleep(time.Duration(sleepMS) * time.Millisecond)
	}

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

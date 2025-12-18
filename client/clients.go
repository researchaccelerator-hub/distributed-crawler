package client

import (
	"context"
	"fmt"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/researchaccelerator-hub/telegram-scraper/telegramhelper"
	"github.com/rs/zerolog/log"
	"github.com/zelenin/go-tdlib/client"
)

// TelegramClient implements Client for Telegram
type TelegramClient struct {
	tdlibClient crawler.TDLibClient
	pool        *telegramhelper.ConnectionPool
	config      map[string]interface{}
}

// NewTelegramClient creates a new Telegram client
func NewTelegramClient(config map[string]interface{}) (*TelegramClient, error) {
	log.Info().Msg("Creating Telegram client")

	// Create a client without immediate initialization
	// The actual connection will be established in Connect()
	return &TelegramClient{
		config: config,
	}, nil
}

// Connect implements Client
func (t *TelegramClient) Connect(ctx context.Context) error {
	log.Info().Msg("Connecting to Telegram")

	// Extract configuration values
	tdlibDatabaseURLs, _ := t.config["tdlib_database_urls"].([]string)
	storageRoot, _ := t.config["storage_root"].(string)
	verbosityLevel, _ := t.config["tdlib_verbosity"].(int)

	// Log connection parameters
	log.Info().
		Strs("tdlib_database_urls", tdlibDatabaseURLs).
		Str("storage_root", storageRoot).
		Int("verbosity_level", verbosityLevel).
		Msg("Telegram connection parameters")

	// Initialize connection pool if database URLs are provided
	if len(tdlibDatabaseURLs) > 0 {
		poolSize := len(tdlibDatabaseURLs)
		if poolSize > 5 {
			poolSize = 5 // Limit pool size to a reasonable number
		}

		// Create connection pool configuration
		poolConfig := telegramhelper.ConnectionPoolConfig{
			PoolSize:          poolSize,
			TDLibDatabaseURLs: tdlibDatabaseURLs,
			Verbosity:         verbosityLevel,
			StorageRoot:       storageRoot,
		}

		log.Info().
			Int("pool_size", poolSize).
			Int("url_count", len(tdlibDatabaseURLs)).
			Msg("Initializing Telegram connection pool")

		// Initialize connection pool
		pool, err := telegramhelper.NewConnectionPool(poolConfig)
		if err != nil {
			log.Error().Err(err).Msg("Failed to initialize Telegram connection pool")
			return fmt.Errorf("failed to initialize Telegram connection pool: %w", err)
		}

		t.pool = pool
		log.Info().Msg("Telegram connection pool initialized successfully")
		return nil
	}

	// If no connection pool, create a single client
	log.Info().Msg("No database URLs provided, creating single Telegram client")

	// Create a TDLib client with the configuration
	telegramService := &telegramhelper.RealTelegramService{}
	cfg := common.CrawlerConfig{
		TDLibVerbosity: verbosityLevel,
	}

	// Initialize the client with the service
	client, err := telegramService.InitializeClientWithConfig(storageRoot, cfg)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create Telegram client")
		return fmt.Errorf("failed to create Telegram client: %w", err)
	}

	t.tdlibClient = client
	log.Info().Msg("Telegram client created successfully")
	return nil
}

// Disconnect implements Client
func (t *TelegramClient) Disconnect(ctx context.Context) error {
	log.Info().Msg("Disconnecting from Telegram")

	// Close connection pool if it exists
	if t.pool != nil {
		t.pool.Close()
		log.Info().Msg("Telegram connection pool closed")
	}

	// Close individual client if it exists
	if t.tdlibClient != nil {
		_, err := t.tdlibClient.Close()
		if err != nil {
			log.Error().Err(err).Msg("Error closing Telegram client")
			return fmt.Errorf("error closing Telegram client: %w", err)
		}
		log.Info().Msg("Telegram client closed")
	}

	return nil
}

// GetChannelInfo implements Client
func (t *TelegramClient) GetChannelInfo(ctx context.Context, channelID string) (Channel, error) {
	log.Info().Str("channel_id", channelID).Msg("Getting Telegram channel info")

	// Use connection pool if available
	if t.pool != nil {
		log.Debug().Msg("Using connection pool to get channel info")

		// Acquire connection from pool
		conn, connID, err := t.pool.GetConnection(ctx)
		if err != nil {
			log.Error().Err(err).Msg("Failed to acquire connection from pool")
			return nil, fmt.Errorf("failed to acquire connection from pool: %w", err)
		}
		defer t.pool.ReleaseConnection(connID)

		// Search for the chat using the public username
		searchReq := &client.SearchPublicChatRequest{
			Username: channelID,
		}

		chat, err := conn.SearchPublicChat(searchReq)
		if err != nil {
			log.Error().Err(err).Str("channel_id", channelID).Msg("Failed to search for public chat")
			return nil, fmt.Errorf("failed to search for public chat: %w", err)
		}

		// Get supergroup info
		sgReq := &client.GetSupergroupRequest{
			SupergroupId: chat.Type.(*client.ChatTypeSupergroup).SupergroupId,
		}

		supergroup, err := conn.GetSupergroup(sgReq)
		if err != nil {
			log.Error().Err(err).Str("channel_id", channelID).Msg("Failed to get supergroup info")
			return nil, fmt.Errorf("failed to get supergroup info: %w", err)
		}

		// Get full supergroup info
		sgFullReq := &client.GetSupergroupFullInfoRequest{
			SupergroupId: chat.Type.(*client.ChatTypeSupergroup).SupergroupId,
		}

		fullInfo, err := conn.GetSupergroupFullInfo(sgFullReq)
		if err != nil {
			log.Error().Err(err).Str("channel_id", channelID).Msg("Failed to get supergroup full info")
			return nil, fmt.Errorf("failed to get supergroup full info: %w", err)
		}

		// Convert to our interface type
		return &TelegramChannel{
			ID:          channelID,
			Name:        chat.Title,
			Description: fullInfo.Description,
			MemberCount: int64(supergroup.MemberCount),
		}, nil
	} else if t.tdlibClient != nil {
		log.Debug().Msg("Using direct client to get channel info")

		// Search for the chat using the public username
		searchReq := &client.SearchPublicChatRequest{
			Username: channelID,
		}

		chat, err := t.tdlibClient.SearchPublicChat(searchReq)
		if err != nil {
			log.Error().Err(err).Str("channel_id", channelID).Msg("Failed to search for public chat")
			return nil, fmt.Errorf("failed to search for public chat: %w", err)
		}

		// Get supergroup info
		sgReq := &client.GetSupergroupRequest{
			SupergroupId: chat.Type.(*client.ChatTypeSupergroup).SupergroupId,
		}

		supergroup, err := t.tdlibClient.GetSupergroup(sgReq)
		if err != nil {
			log.Error().Err(err).Str("channel_id", channelID).Msg("Failed to get supergroup info")
			return nil, fmt.Errorf("failed to get supergroup info: %w", err)
		}

		// Get full supergroup info
		sgFullReq := &client.GetSupergroupFullInfoRequest{
			SupergroupId: chat.Type.(*client.ChatTypeSupergroup).SupergroupId,
		}

		fullInfo, err := t.tdlibClient.GetSupergroupFullInfo(sgFullReq)
		if err != nil {
			log.Error().Err(err).Str("channel_id", channelID).Msg("Failed to get supergroup full info")
			return nil, fmt.Errorf("failed to get supergroup full info: %w", err)
		}

		// Convert to our interface type
		return &TelegramChannel{
			ID:          channelID,
			Name:        chat.Title,
			Description: fullInfo.Description,
			MemberCount: int64(supergroup.MemberCount),
		}, nil
	} else {
		return nil, fmt.Errorf("no Telegram client or connection pool available")
	}
}

// GetMessages implements Client
func (t *TelegramClient) GetMessages(ctx context.Context, channelID string, fromTime, toTime time.Time, limit int) ([]Message, error) {
	log.Info().
		Str("channel_id", channelID).
		Time("from_time", fromTime).
		Time("to_time", toTime).
		Int("limit", limit).
		Msg("Getting Telegram messages")

	// Use connection pool if available
	if t.pool != nil {
		log.Debug().Msg("Using connection pool to get messages")

		// Acquire connection from pool
		conn, connID, err := t.pool.GetConnection(ctx)
		if err != nil {
			log.Error().Err(err).Msg("Failed to acquire connection from pool")
			return nil, fmt.Errorf("failed to acquire connection from pool: %w", err)
		}
		defer t.pool.ReleaseConnection(connID)

		// Get messages
		return t.getMessagesWithClient(conn, channelID, fromTime, toTime, limit)
	} else if t.tdlibClient != nil {
		log.Debug().Msg("Using direct client to get messages")

		// Get messages using the direct client
		return t.getMessagesWithClient(t.tdlibClient, channelID, fromTime, toTime, limit)
	} else {
		return nil, fmt.Errorf("no Telegram client or connection pool available")
	}
}

// getMessagesWithClient retrieves messages using a TDLib client
func (t *TelegramClient) getMessagesWithClient(tdlibClient crawler.TDLibClient, channelID string, fromTime, toTime time.Time, limit int) ([]Message, error) {
	// Find the chat by username
	searchReq := &client.SearchPublicChatRequest{
		Username: channelID,
	}

	chat, err := tdlibClient.SearchPublicChat(searchReq)
	if err != nil {
		log.Error().Err(err).Str("channel_id", channelID).Msg("Failed to search for public chat")
		return nil, fmt.Errorf("failed to search for public chat: %w", err)
	}

	// Get chat history
	historyReq := &client.GetChatHistoryRequest{
		ChatId:        chat.Id,
		Limit:         int32(limit),
		OnlyLocal:     false,
		FromMessageId: 0, // Will get the most recent messages
	}

	messages, err := tdlibClient.GetChatHistory(historyReq)
	if err != nil {
		log.Error().Err(err).Str("channel_id", channelID).Msg("Failed to get chat history")
		return nil, fmt.Errorf("failed to get chat history: %w", err)
	}

	// Convert messages to our format
	result := make([]Message, 0, len(messages.Messages))
	for _, msg := range messages.Messages {
		// Check if message is within the time range
		msgTime := time.Unix(int64(msg.Date), 0)
		if (msgTime.After(fromTime) || msgTime.Equal(fromTime)) &&
			(msgTime.Before(toTime) || msgTime.Equal(toTime)) {

			// Extract text content
			text := ""
			if textMsg, ok := msg.Content.(*client.MessageText); ok && textMsg != nil && textMsg.Text != nil {
				text = textMsg.Text.Text
			}

			// Extract view count
			views := int64(0)
			if msg.InteractionInfo != nil {
				views = int64(msg.InteractionInfo.ViewCount)
			}

			// Extract reactions
			reactions := make(map[string]int64)
			if msg.InteractionInfo != nil && msg.InteractionInfo.Reactions != nil {
				for _, reaction := range msg.InteractionInfo.Reactions.Reactions {
					if reaction.Type != nil {
						if emojiReaction, ok := reaction.Type.(*client.ReactionTypeEmoji); ok && emojiReaction != nil {
							reactions[emojiReaction.Emoji] = int64(reaction.TotalCount)
						}
					}
				}
			}

			// Create message object
			telegramMsg := &TelegramMessage{
				ID:         fmt.Sprintf("%d", msg.Id),
				ChannelID:  channelID,
				SenderID:   "0", // We don't have sender info in this context
				SenderName: "",  // We don't have sender info in this context
				Text:       text,
				Timestamp:  msgTime,
				Views:      views,
				Reactions:  reactions,
			}

			result = append(result, telegramMsg)
		}
	}

	log.Info().Int("message_count", len(result)).Msg("Retrieved Telegram messages")
	return result, nil
}

// Helper function to convert reactions
func convertReactions(reactions map[string]int) map[string]int64 {
	result := make(map[string]int64)
	for k, v := range reactions {
		result[k] = int64(v)
	}
	return result
}

// GetChannelType implements Client
func (t *TelegramClient) GetChannelType() string {
	return "telegram"
}

// YouTubeClient implements Client for YouTube
type YouTubeClient struct {
	// YouTube API client would go here
	apiKey string
}

// NewYouTubeClient creates a new YouTube client
func NewYouTubeClient(config map[string]interface{}) (*YouTubeClient, error) {
	// Extract API key from config
	apiKey, ok := config["api_key"].(string)
	if !ok || apiKey == "" {
		return nil, fmt.Errorf("YouTube API key not provided in configuration")
	}

	log.Info().Msg("Creating YouTube client")
	return &YouTubeClient{
		apiKey: apiKey,
	}, nil
}

// Connect implements Client
func (y *YouTubeClient) Connect(ctx context.Context) error {
	log.Info().Msg("Connecting to YouTube API")
	// Initialize YouTube API client if needed
	// This would typically validate the API key
	return nil
}

// Disconnect implements Client
func (y *YouTubeClient) Disconnect(ctx context.Context) error {
	log.Info().Msg("Disconnecting from YouTube API")
	// Cleanup YouTube API resources if needed
	return nil
}

// GetChannelInfo implements Client
func (y *YouTubeClient) GetChannelInfo(ctx context.Context, channelID string) (Channel, error) {
	log.Info().Str("channel_id", channelID).Msg("Getting YouTube channel info")

	// In a real implementation, this would call the YouTube API
	// to fetch channel details using the channels.list endpoint

	// Create a placeholder channel for demo purposes
	return &YouTubeChannel{
		ID:          channelID,
		Name:        fmt.Sprintf("YouTube Channel %s", channelID),
		Description: "This is a placeholder for YouTube channel data",
		MemberCount: 0,
	}, nil
}

// GetMessages implements Client
// For YouTube, this would get video comments or community posts
func (y *YouTubeClient) GetMessages(ctx context.Context, channelID string, fromTime, toTime time.Time, limit int) ([]Message, error) {
	log.Info().
		Str("channel_id", channelID).
		Time("from_time", fromTime).
		Time("to_time", toTime).
		Int("limit", limit).
		Msg("Getting YouTube comments")

	// In a real implementation, this would:
	// 1. Get videos for the channel using search.list with type=video and channelId parameter
	// 2. For each video, get comments using commentThreads.list
	// 3. Filter by date and return up to the specified limit

	// Return empty slice for demo purposes
	return []Message{}, nil
}

// GetChannelType implements Client
func (y *YouTubeClient) GetChannelType() string {
	return "youtube"
}

package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	innertubego "github.com/nezbut/innertube-go"
	youtubemodel "github.com/researchaccelerator-hub/telegram-scraper/model/youtube"
	"github.com/rs/zerolog/log"
)

// YouTubeInnerTubeClient implements the YouTubeClient interface using the InnerTube API
// This provides an alternative to the YouTube Data API that doesn't require API keys
// or have quota limitations, but requires more data parsing.
type YouTubeInnerTubeClient struct {
	client *innertubego.InnerTube

	// Caching system similar to YouTubeDataClient
	channelCache         map[string]*youtubemodel.YouTubeChannel
	uploadsPlaylistCache map[string]string
	videoStatsCache      map[string]*youtubemodel.YouTubeVideo
	cacheMutex           sync.RWMutex

	// Configuration
	clientType    string // "WEB", "ANDROID", "IOS", etc.
	clientVersion string
}

// InnerTubeConfig contains configuration for the InnerTube client
type InnerTubeConfig struct {
	ClientType    string // Default: "WEB"
	ClientVersion string // Default: "2.20230728.00.00"
}

// NewYouTubeInnerTubeClient creates a new YouTube client using the InnerTube API
func NewYouTubeInnerTubeClient(config *InnerTubeConfig) (*YouTubeInnerTubeClient, error) {
	// Set defaults
	if config == nil {
		config = &InnerTubeConfig{
			ClientType:    "WEB",
			ClientVersion: "2.20230728.00.00",
		}
	}

	if config.ClientType == "" {
		config.ClientType = "WEB"
	}

	if config.ClientVersion == "" {
		config.ClientVersion = "2.20230728.00.00"
	}

	log.Info().
		Str("client_type", config.ClientType).
		Str("client_version", config.ClientVersion).
		Msg("Creating YouTube InnerTube client")

	return &YouTubeInnerTubeClient{
		clientType:           config.ClientType,
		clientVersion:        config.ClientVersion,
		channelCache:         make(map[string]*youtubemodel.YouTubeChannel),
		uploadsPlaylistCache: make(map[string]string),
		videoStatsCache:      make(map[string]*youtubemodel.YouTubeVideo),
	}, nil
}

// Connect establishes a connection to the InnerTube API
func (c *YouTubeInnerTubeClient) Connect(ctx context.Context) error {
	log.Info().Msg("Connecting to YouTube InnerTube API")

	// Create InnerTube client
	// Parameters: config, clientType, clientVersion, apiKey, accessToken, refreshToken, httpClient, debug
	client, err := innertubego.NewInnerTube(
		nil,           // config (will use defaults)
		c.clientType,  // clientType
		c.clientVersion, // clientVersion
		"",            // apiKey (not needed for unauthenticated access)
		"",            // accessToken (not implemented yet)
		"",            // refreshToken (not implemented yet)
		nil,           // httpClient (will use default)
		true,          // debug mode
	)

	if err != nil {
		log.Error().Err(err).Msg("Failed to create InnerTube client")
		return fmt.Errorf("failed to create InnerTube client: %w", err)
	}

	c.client = client
	log.Info().Msg("Successfully connected to YouTube InnerTube API")
	return nil
}

// Disconnect closes the connection to the InnerTube API
func (c *YouTubeInnerTubeClient) Disconnect(ctx context.Context) error {
	log.Info().Msg("Disconnecting from YouTube InnerTube API")
	c.client = nil
	return nil
}

// GetChannelInfo retrieves information about a YouTube channel using InnerTube
func (c *YouTubeInnerTubeClient) GetChannelInfo(ctx context.Context, channelID string) (*youtubemodel.YouTubeChannel, error) {
	if c.client == nil {
		return nil, fmt.Errorf("InnerTube client not connected")
	}

	// Check cache first
	c.cacheMutex.RLock()
	cachedChannel, exists := c.channelCache[channelID]
	c.cacheMutex.RUnlock()

	if exists {
		log.Debug().
			Str("channel_id", channelID).
			Str("title", cachedChannel.Title).
			Msg("Using cached channel info")
		return cachedChannel, nil
	}

	log.Info().Str("channel_id", channelID).Msg("Fetching YouTube channel info via InnerTube")

	// Use the browse endpoint to get channel information
	// InnerTube API requires a browse ID which is typically the channel ID
	browseID := channelID

	// Call the browse endpoint
	// Parameters: context, browseID, params, continuation
	data, err := c.client.Browse(ctx, &browseID, nil, nil)
	if err != nil {
		log.Error().Err(err).Str("channel_id", channelID).Msg("Failed to browse channel")
		return nil, fmt.Errorf("failed to browse channel: %w", err)
	}

	// Parse the response to extract channel information
	// Note: InnerTube responses are complex nested structures that need careful parsing
	channel, err := c.parseChannelFromBrowse(data, channelID)
	if err != nil {
		log.Error().Err(err).Str("channel_id", channelID).Msg("Failed to parse channel data")
		return nil, fmt.Errorf("failed to parse channel data: %w", err)
	}

	// Cache the result
	c.cacheMutex.Lock()
	c.channelCache[channelID] = channel

	// Cache under both input ID and actual ID if different
	if channel.ID != channelID {
		c.channelCache[channel.ID] = channel
		log.Debug().
			Str("input_channel_id", channelID).
			Str("actual_channel_id", channel.ID).
			Msg("Cached channel under both IDs")
	}
	c.cacheMutex.Unlock()

	log.Info().
		Str("channel_id", channel.ID).
		Str("title", channel.Title).
		Int64("subscribers", channel.SubscriberCount).
		Int64("video_count", channel.VideoCount).
		Msg("YouTube channel info retrieved via InnerTube")

	return channel, nil
}

// GetVideos retrieves videos from a YouTube channel using InnerTube
func (c *YouTubeInnerTubeClient) GetVideos(ctx context.Context, channelID string, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	return c.GetVideosFromChannel(ctx, channelID, fromTime, toTime, limit)
}

// GetVideosFromChannel retrieves videos from a specific YouTube channel using InnerTube
func (c *YouTubeInnerTubeClient) GetVideosFromChannel(ctx context.Context, channelID string, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	if c.client == nil {
		return nil, fmt.Errorf("InnerTube client not connected")
	}

	log.Info().
		Str("channel_id", channelID).
		Time("from_time", fromTime).
		Time("to_time", toTime).
		Int("limit", limit).
		Msg("Fetching videos from YouTube channel via InnerTube")

	// Use current time as default if toTime is zero
	effectiveToTime := toTime
	if effectiveToTime.IsZero() {
		effectiveToTime = time.Now()
	}

	// Handle negative or zero limit as "unlimited"
	effectiveLimit := limit
	if effectiveLimit <= 0 {
		effectiveLimit = 1000000
	}

	// Browse the channel to get videos
	browseID := channelID
	// Parameters: context, browseID, params, continuation
	data, err := c.client.Browse(ctx, &browseID, nil, nil)
	if err != nil {
		log.Error().Err(err).Str("channel_id", channelID).Msg("Failed to browse channel for videos")
		return nil, fmt.Errorf("failed to browse channel: %w", err)
	}

	// Parse videos from browse response
	videos, err := c.parseVideosFromBrowse(data, channelID, fromTime, effectiveToTime, effectiveLimit)
	if err != nil {
		log.Error().Err(err).Str("channel_id", channelID).Msg("Failed to parse videos from browse data")
		return nil, fmt.Errorf("failed to parse videos: %w", err)
	}

	log.Info().
		Str("channel_id", channelID).
		Int("video_count", len(videos)).
		Msg("Retrieved videos from YouTube channel via InnerTube")

	return videos, nil
}

// parseChannelFromBrowse extracts channel information from InnerTube browse response
func (c *YouTubeInnerTubeClient) parseChannelFromBrowse(data interface{}, channelID string) (*youtubemodel.YouTubeChannel, error) {
	// InnerTube returns complex nested JSON structures
	// This is a simplified parser - in production, you'd need comprehensive parsing logic

	log.Warn().Msg("InnerTube channel parsing is simplified - may not extract all data")

	// Create a basic channel object
	// In a full implementation, you would navigate the InnerTube response structure
	// to extract: title, description, subscriber count, view count, video count, etc.

	channel := &youtubemodel.YouTubeChannel{
		ID:              channelID,
		Title:           "Channel (InnerTube)", // Would be parsed from response
		Description:     "",
		SubscriberCount: 0,
		ViewCount:       0,
		VideoCount:      0,
		PublishedAt:     time.Now(),
		Thumbnails:      make(map[string]string),
		Country:         "",
	}

	// TODO: Parse actual data from the InnerTube response
	// The response structure is complex and varies by endpoint
	// Example structure navigation would look like:
	// dataMap := data.(map[string]interface{})
	// header := dataMap["header"].(map[string]interface{})
	// metadata := header["c4TabbedHeaderRenderer"].(map[string]interface{})
	// title := metadata["title"].(string)

	log.Debug().
		Str("channel_id", channelID).
		Msg("Parsed channel from InnerTube (simplified parser)")

	return channel, nil
}

// parseVideosFromBrowse extracts video information from InnerTube browse response
func (c *YouTubeInnerTubeClient) parseVideosFromBrowse(data interface{}, channelID string, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	log.Warn().Msg("InnerTube video parsing is simplified - may not extract all data")

	// This is a placeholder implementation
	// In production, you would need to:
	// 1. Navigate the InnerTube response structure
	// 2. Extract video renderers from tabs/sections
	// 3. Parse each video's metadata (title, description, views, likes, etc.)
	// 4. Handle pagination using continuation tokens
	// 5. Filter by time range

	videos := make([]*youtubemodel.YouTubeVideo, 0)

	// TODO: Implement actual parsing logic
	// Example structure navigation:
	// dataMap := data.(map[string]interface{})
	// contents := dataMap["contents"].(map[string]interface{})
	// tabs := contents["twoColumnBrowseResultsRenderer"].(map[string]interface{})["tabs"].([]interface{})
	// for each tab, extract gridRenderer or richItemRenderer
	// for each item, parse videoRenderer

	log.Debug().
		Str("channel_id", channelID).
		Int("videos_parsed", len(videos)).
		Msg("Parsed videos from InnerTube (simplified parser)")

	return videos, nil
}

// getCachedVideoStats checks if video statistics are already cached
func (c *YouTubeInnerTubeClient) getCachedVideoStats(videoID string) (*youtubemodel.YouTubeVideo, bool) {
	c.cacheMutex.RLock()
	defer c.cacheMutex.RUnlock()
	video, exists := c.videoStatsCache[videoID]
	return video, exists
}

// cacheVideoStats stores video statistics in cache
func (c *YouTubeInnerTubeClient) cacheVideoStats(video *youtubemodel.YouTubeVideo) {
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()
	c.videoStatsCache[video.ID] = video
}

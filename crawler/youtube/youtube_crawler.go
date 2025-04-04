// Package youtube implements the YouTube-specific crawler functionality
package youtube

import (
	"context"
	"fmt"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/researchaccelerator-hub/telegram-scraper/model"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/rs/zerolog/log"
)

// YouTubeClient defines the methods needed for YouTube API operations
type YouTubeClient interface {
	// Connect establishes a connection to the YouTube API
	Connect(ctx context.Context) error
	
	// Disconnect closes the connection to the YouTube API
	Disconnect(ctx context.Context) error
	
	// GetChannelInfo retrieves information about a YouTube channel
	GetChannelInfo(ctx context.Context, channelID string) (*YouTubeChannel, error)
	
	// GetVideos retrieves videos from a YouTube channel
	GetVideos(ctx context.Context, channelID string, fromTime, toTime time.Time, limit int) ([]*YouTubeVideo, error)
}

// YouTubeChannel represents a YouTube channel
type YouTubeChannel struct {
	ID           string
	Title        string
	Description  string
	SubscriberCount int64
	ViewCount    int64
	VideoCount   int64
	PublishedAt  time.Time
	Thumbnails   map[string]string
}

// YouTubeVideo represents a YouTube video
type YouTubeVideo struct {
	ID          string
	ChannelID   string
	Title       string
	Description string
	PublishedAt time.Time
	ViewCount   int64
	LikeCount   int64
	CommentCount int64
	Duration    string
	Thumbnails  map[string]string
	Tags        []string
}

// YouTubeCrawler implements the crawler.Crawler interface for YouTube
type YouTubeCrawler struct {
	client       YouTubeClient
	stateManager state.StateManager
	initialized  bool
}

// NewYouTubeCrawler creates a new YouTube crawler
func NewYouTubeCrawler() crawler.Crawler {
	return &YouTubeCrawler{
		initialized: false,
	}
}

// Initialize sets up the YouTube crawler
func (c *YouTubeCrawler) Initialize(ctx context.Context, config map[string]interface{}) error {
	if c.initialized {
		return nil
	}
	
	// Extract client and state manager from config
	clientObj, ok := config["client"]
	if !ok {
		return fmt.Errorf("client not provided in config")
	}
	
	youtubeClient, ok := clientObj.(YouTubeClient)
	if !ok {
		return fmt.Errorf("provided client is not a valid YouTube client")
	}
	
	stateManagerObj, ok := config["state_manager"]
	if !ok {
		return fmt.Errorf("state_manager not provided in config")
	}
	
	stateManager, ok := stateManagerObj.(state.StateManager)
	if !ok {
		return fmt.Errorf("provided state_manager is not valid")
	}
	
	c.client = youtubeClient
	c.stateManager = stateManager
	c.initialized = true
	
	return nil
}

// ValidateTarget checks if a target is valid for YouTube
func (c *YouTubeCrawler) ValidateTarget(target crawler.CrawlTarget) error {
	if target.Type != crawler.PlatformYouTube {
		return fmt.Errorf("invalid target type: %s, expected: youtube", target.Type)
	}
	
	if target.ID == "" {
		return fmt.Errorf("target ID cannot be empty")
	}
	
	return nil
}

// GetChannelInfo retrieves information about a YouTube channel
func (c *YouTubeCrawler) GetChannelInfo(ctx context.Context, target crawler.CrawlTarget) (*model.ChannelData, error) {
	if err := c.ValidateTarget(target); err != nil {
		return nil, err
	}
	
	if !c.initialized {
		return nil, fmt.Errorf("crawler not initialized")
	}
	
	log.Info().Str("channel_id", target.ID).Msg("Fetching YouTube channel info")
	
	// Fetch channel info from the YouTube API
	channel, err := c.client.GetChannelInfo(ctx, target.ID)
	if err != nil {
		log.Error().Err(err).Str("channel_id", target.ID).Msg("Failed to get YouTube channel info")
		return nil, fmt.Errorf("failed to get YouTube channel info: %w", err)
	}
	
	// Construct channel URL based on ID format
	channelURL := fmt.Sprintf("https://www.youtube.com/channel/%s", target.ID)
	if len(target.ID) > 0 && target.ID[0] == '@' {
		// Handle username format (@username)
		channelURL = fmt.Sprintf("https://www.youtube.com/%s", target.ID)
	}
	
	// Convert YouTube-specific channel data to common model
	channelData := &model.ChannelData{
		ChannelID:   0, // YouTube doesn't use numeric IDs like Telegram
		ChannelName: channel.Title,
		ChannelURL:  channelURL,
		ChannelURLExternal: channelURL,
		ChannelProfileImage: channel.Thumbnails["default"],
		ChannelEngagementData: model.EngagementData{
			FollowerCount: int(channel.SubscriberCount),
			ViewsCount:    int(channel.ViewCount),
			PostCount:     int(channel.VideoCount),
		},
	}
	
	log.Info().
		Str("channel_id", target.ID).
		Str("channel_name", channel.Title).
		Int64("subscriber_count", channel.SubscriberCount).
		Msg("Successfully retrieved YouTube channel info")
	
	return channelData, nil
}

// FetchMessages retrieves videos from YouTube
func (c *YouTubeCrawler) FetchMessages(ctx context.Context, job crawler.CrawlJob) (crawler.CrawlResult, error) {
	if err := c.ValidateTarget(job.Target); err != nil {
		return crawler.CrawlResult{}, err
	}
	
	if !c.initialized {
		return crawler.CrawlResult{}, fmt.Errorf("crawler not initialized")
	}
	
	log.Info().
		Str("channel_id", job.Target.ID).
		Time("from_time", job.FromTime).
		Time("to_time", job.ToTime).
		Int("limit", job.Limit).
		Msg("Starting YouTube crawl")
	
	// Fetch videos from the YouTube client
	videos, err := c.client.GetVideos(ctx, job.Target.ID, job.FromTime, job.ToTime, job.Limit)
	if err != nil {
		log.Error().Err(err).Str("channel_id", job.Target.ID).Msg("Failed to get videos from YouTube")
		return crawler.CrawlResult{}, err
	}
	
	log.Info().
		Str("channel_id", job.Target.ID).
		Int("video_count", len(videos)).
		Msg("Retrieved videos from YouTube")
	
	// Convert videos to posts
	posts := make([]model.Post, 0, len(videos))
	for _, video := range videos {
		post := c.convertVideoToPost(video)
		
		// Save the post to the state manager
		if err := c.stateManager.SavePost(ctx, post); err != nil {
			log.Error().Err(err).Str("video_id", video.ID).Msg("Failed to save video post")
			// Continue with next video
		}
		
		posts = append(posts, post)
	}
	
	return crawler.CrawlResult{
		Posts:  posts,
		Errors: nil,
	}, nil
}

// GetPlatformType returns the type of platform this crawler supports
func (c *YouTubeCrawler) GetPlatformType() crawler.PlatformType {
	return crawler.PlatformYouTube
}

// Close cleans up resources
func (c *YouTubeCrawler) Close() error {
	if c.client != nil {
		if err := c.client.Disconnect(context.Background()); err != nil {
			log.Error().Err(err).Msg("Error disconnecting YouTube client")
			return err
		}
	}
	return nil
}

// convertVideoToPost converts a YouTubeVideo to model.Post
func (c *YouTubeCrawler) convertVideoToPost(video *YouTubeVideo) model.Post {
	// Get channel info for consistent naming
	var channelName string
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	channel, err := c.client.GetChannelInfo(ctx, video.ChannelID)
	if err != nil {
		log.Warn().Err(err).Str("channel_id", video.ChannelID).Msg("Failed to get channel info for video conversion")
		channelName = video.ChannelID
	} else {
		channelName = channel.Title
	}
	
	// Calculate video engagement (likes + comments + views)
	engagement := int(video.LikeCount + video.CommentCount + (video.ViewCount / 100))
	
	// Prepare title pointer
	title := video.Title
	
	// Convert thumbnail URL map to best available thumbnail
	thumbURL := ""
	if url, ok := video.Thumbnails["maxres"]; ok && url != "" {
		thumbURL = url
	} else if url, ok := video.Thumbnails["high"]; ok && url != "" {
		thumbURL = url
	} else if url, ok := video.Thumbnails["medium"]; ok && url != "" {
		thumbURL = url
	} else if url, ok := video.Thumbnails["default"]; ok && url != "" {
		thumbURL = url
	}
	
	// Create video URL
	videoURL := fmt.Sprintf("https://www.youtube.com/watch?v=%s", video.ID)
	
	// Create the post object
	post := model.Post{
		PostUID:       video.ID,
		ChannelName:   channelName,
		ChannelID:     0, // YouTube uses string IDs, not numeric
		URL:           videoURL,
		PublishedAt:   video.PublishedAt,
		CreatedAt:     time.Now(),
		Engagement:    engagement,
		PostTitle:     &title,
		Description:   video.Description,
		ViewsCount:    int(video.ViewCount),
		LikesCount:    int(video.LikeCount),
		CommentsCount: int(video.CommentCount),
		ViewCount:     int(video.ViewCount),
		LikeCount:     int(video.LikeCount),
		CommentCount:  int(video.CommentCount),
		PlatformName:  "youtube",
		SearchableText: video.Title + " " + video.Description,
		AllText:       video.Title + " " + video.Description,
		PostType:      []string{"video"},
		CaptureTime:   time.Now(),
		ThumbURL:      thumbURL,
		MediaURL:      videoURL,
		Handle:        video.ChannelID,
		PostLink:      videoURL,
		// Add reactions as a map
		Reactions:     map[string]int{"like": int(video.LikeCount)},
	}
	
	// Get channel data if available
	if channel != nil {
		// Construct channel URL based on ID format
		channelURL := fmt.Sprintf("https://www.youtube.com/channel/%s", video.ChannelID)
		if len(video.ChannelID) > 0 && video.ChannelID[0] == '@' {
			// Handle username format (@username)
			channelURL = fmt.Sprintf("https://www.youtube.com/%s", video.ChannelID)
		}
		
		post.ChannelData = model.ChannelData{
			ChannelName: channel.Title,
			ChannelURL:  channelURL,
			ChannelURLExternal: channelURL,
			ChannelProfileImage: channel.Thumbnails["default"],
			ChannelEngagementData: model.EngagementData{
				FollowerCount: int(channel.SubscriberCount),
				ViewsCount:    int(channel.ViewCount),
			},
		}
	}
	
	return post
}
// Package youtube implements the YouTube-specific crawler functionality
package youtube

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/researchaccelerator-hub/telegram-scraper/model"
	youtubemodel "github.com/researchaccelerator-hub/telegram-scraper/model/youtube"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/rs/zerolog/log"
)

// SamplingMethod defines the method used for sampling YouTube videos
type SamplingMethod string

const (
	// SamplingMethodChannel retrieves videos directly from a specific channel
	SamplingMethodChannel SamplingMethod = "channel"
	
	// SamplingMethodRandom uses random sampling with prefix generator
	SamplingMethodRandom SamplingMethod = "random"
	
	// SamplingMethodSnowball uses snowball sampling starting from seed channels
	SamplingMethodSnowball SamplingMethod = "snowball"
)

// YouTubeCrawlerConfig holds the configuration for the YouTube crawler
type YouTubeCrawlerConfig struct {
	// SamplingMethod specifies how to sample videos (channel, random, snowball)
	SamplingMethod SamplingMethod `json:"sampling_method"`
	
	// SeedChannels provides starting points for snowball sampling
	SeedChannels []string `json:"seed_channels,omitempty"`
	
	// MinChannelVideos specifies the minimum number of videos a channel must have
	MinChannelVideos int64 `json:"min_channel_videos"`
}

// YouTubeCrawler implements the crawler.Crawler interface for YouTube
type YouTubeCrawler struct {
	client          youtubemodel.YouTubeClient
	stateManager    state.StateManagementInterface
	initialized     bool
	config          YouTubeCrawlerConfig
	defaultConfig   YouTubeCrawlerConfig
}

// NewYouTubeCrawler creates a new YouTube crawler
func NewYouTubeCrawler() crawler.Crawler {
	// Set default configuration
	defaultConfig := YouTubeCrawlerConfig{
		SamplingMethod:   SamplingMethodChannel, // Default to channel-based sampling
		MinChannelVideos: 10,                   // Default to 10 minimum videos
	}
	
	return &YouTubeCrawler{
		initialized:   false,
		defaultConfig: defaultConfig,
		config:        defaultConfig, // Start with default config
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

	youtubeClient, ok := clientObj.(youtubemodel.YouTubeClient)
	if !ok {
		return fmt.Errorf("provided client is not a valid YouTube client")
	}

	stateManagerObj, ok := config["state_manager"]
	if !ok {
		return fmt.Errorf("state_manager not provided in config")
	}

	stateManager, ok := stateManagerObj.(state.StateManagementInterface)
	if !ok {
		return fmt.Errorf("provided state_manager is not a valid StateManagementInterface")
	}

	// Start with default configuration
	crawlerConfig := c.defaultConfig
	
	// Process crawler-specific configuration if provided
	if crawlerConfigObj, ok := config["crawler_config"]; ok {
		if crawlerConfigMap, ok := crawlerConfigObj.(map[string]interface{}); ok {
			// Extract sampling method
			if methodObj, ok := crawlerConfigMap["sampling_method"]; ok {
				if methodStr, ok := methodObj.(string); ok {
					crawlerConfig.SamplingMethod = SamplingMethod(methodStr)
					log.Info().Str("sampling_method", methodStr).Msg("Using configured sampling method")
				}
			}
			
			// Extract seed channels for snowball sampling
			if seedChannelsObj, ok := crawlerConfigMap["seed_channels"]; ok {
				if seedChannelsSlice, ok := seedChannelsObj.([]interface{}); ok {
					seedChannels := make([]string, 0, len(seedChannelsSlice))
					for _, item := range seedChannelsSlice {
						if channelID, ok := item.(string); ok {
							seedChannels = append(seedChannels, channelID)
						}
					}
					crawlerConfig.SeedChannels = seedChannels
					log.Info().Strs("seed_channels", seedChannels).Msg("Using configured seed channels")
				}
			}
			
			// Extract minimum channel videos threshold
			if minVideosObj, ok := crawlerConfigMap["min_channel_videos"]; ok {
				switch v := minVideosObj.(type) {
				case int:
					crawlerConfig.MinChannelVideos = int64(v)
				case int64:
					crawlerConfig.MinChannelVideos = v
				case float64:
					crawlerConfig.MinChannelVideos = int64(v)
				}
				log.Info().Int64("min_channel_videos", crawlerConfig.MinChannelVideos).Msg("Using configured minimum channel videos")
			}
		}
	}
	
	// Validate configuration
	if crawlerConfig.SamplingMethod == SamplingMethodSnowball && len(crawlerConfig.SeedChannels) == 0 {
		log.Warn().Msg("Snowball sampling method selected but no seed channels provided, using default channel sampling")
		crawlerConfig.SamplingMethod = SamplingMethodChannel
	}
	
	// Set the client, state manager, and configuration
	c.client = youtubeClient
	c.stateManager = stateManager
	c.config = crawlerConfig
	c.initialized = true
	
	log.Info().
		Str("sampling_method", string(c.config.SamplingMethod)).
		Int64("min_channel_videos", c.config.MinChannelVideos).
		Int("seed_channels_count", len(c.config.SeedChannels)).
		Msg("YouTube crawler initialized")

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
		ChannelID:           0, // YouTube doesn't use numeric IDs like Telegram
		ChannelName:         channel.Title,
		ChannelURL:          channelURL,
		ChannelURLExternal:  channelURL,
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
		Str("sampling_method", string(c.config.SamplingMethod)).
		Msg("Starting YouTube crawl")

	// Fetch videos using the configured sampling method
	var videos []*youtubemodel.YouTubeVideo
	var err error
	
	switch c.config.SamplingMethod {
	case SamplingMethodChannel:
		// Traditional channel-based sampling
		videos, err = c.client.GetVideosFromChannel(ctx, job.Target.ID, job.FromTime, job.ToTime, job.Limit)
		if err != nil {
			log.Error().Err(err).Str("channel_id", job.Target.ID).Msg("Failed to get videos from YouTube channel")
			return crawler.CrawlResult{}, err
		}
		
		log.Info().
			Str("channel_id", job.Target.ID).
			Int("video_count", len(videos)).
			Msg("Retrieved videos from specific YouTube channel")
			
	case SamplingMethodRandom:
		// Random sampling using prefix generator
		videos, err = c.client.GetRandomVideos(ctx, job.FromTime, job.ToTime, job.Limit)
		if err != nil {
			log.Error().Err(err).Msg("Failed to get videos using random sampling")
			return crawler.CrawlResult{}, err
		}
		
		log.Info().
			Int("video_count", len(videos)).
			Msg("Retrieved videos using random sampling")
			
	case SamplingMethodSnowball:
		// Snowball sampling based on seed channels
		// If target ID is provided and not in seed channels, add it
		seedChannels := c.config.SeedChannels
		if job.Target.ID != "" {
			// Check if the target ID is already in seed channels
			found := false
			for _, id := range seedChannels {
				if id == job.Target.ID {
					found = true
					break
				}
			}
			
			// If not found, add it to the beginning of seed channels
			if !found {
				seedChannels = append([]string{job.Target.ID}, seedChannels...)
				log.Info().Str("target_id", job.Target.ID).Msg("Added target ID to seed channels for snowball sampling")
			}
		}
		
		if len(seedChannels) == 0 {
			return crawler.CrawlResult{}, fmt.Errorf("no seed channels available for snowball sampling")
		}
		
		videos, err = c.client.GetSnowballVideos(ctx, seedChannels, job.FromTime, job.ToTime, job.Limit)
		if err != nil {
			log.Error().Err(err).Strs("seed_channels", seedChannels).Msg("Failed to get videos using snowball sampling")
			return crawler.CrawlResult{}, err
		}
		
		log.Info().
			Int("video_count", len(videos)).
			Int("seed_channels_count", len(seedChannels)).
			Msg("Retrieved videos using snowball sampling")
			
	default:
		return crawler.CrawlResult{}, fmt.Errorf("unknown sampling method: %s", c.config.SamplingMethod)
	}

	// Convert videos to posts
	posts := make([]model.Post, 0, len(videos))
	for _, video := range videos {
		post := c.convertVideoToPost(video)

		// Save the post to the state manager's storage
		if err := c.stateManager.StorePost(video.ChannelID, post); err != nil {
			log.Error().Err(err).Str("video_id", video.ID).Msg("Failed to save video post")
			// Continue with next video
		}

		posts = append(posts, post)
	}

	log.Info().
		Int("post_count", len(posts)).
		Str("sampling_method", string(c.config.SamplingMethod)).
		Msg("YouTube crawl completed successfully")

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

// Private cache of channel names to avoid repeated GetChannelInfo API calls within the same crawler session
var channelNameCache = make(map[string]string)

// parseISO8601Duration parses YouTube's ISO 8601 duration format to seconds
// Example: PT1H2M3S = 1 hour, 2 minutes, 3 seconds = 3723 seconds
func parseISO8601Duration(duration string) (int, error) {
	if duration == "" {
		return 0, fmt.Errorf("empty duration string")
	}

	// Remove PT prefix
	if len(duration) < 2 || duration[:2] != "PT" {
		return 0, fmt.Errorf("invalid duration format: %s, expected 'PT' prefix", duration)
	}
	duration = duration[2:]

	var hours, minutes, seconds int
	var currentValue string

	// Parse each component
	for _, c := range duration {
		if c >= '0' && c <= '9' {
			currentValue += string(c)
		} else {
			value := 0
			if currentValue != "" {
				var err error
				value, err = strconv.Atoi(currentValue)
				if err != nil {
					return 0, fmt.Errorf("invalid duration value: %s", currentValue)
				}
				currentValue = ""
			}

			switch c {
			case 'H':
				hours = value
			case 'M':
				minutes = value
			case 'S':
				seconds = value
			default:
				return 0, fmt.Errorf("invalid duration component: %c", c)
			}
		}
	}

	// Calculate total seconds
	totalSeconds := hours*3600 + minutes*60 + seconds
	return totalSeconds, nil
}

// extractURLs extracts URLs from a string using a simple regex
func extractURLs(text string) []string {
	// Define a regex pattern to find URLs
	// This is a simplified pattern - for production, consider a more robust solution
	pattern := regexp.MustCompile(`(https?://\S+)`)

	// Find all matches
	matches := pattern.FindAllString(text, -1)

	// Remove trailing punctuation that may have been captured
	for i, url := range matches {
		// Remove trailing punctuation characters
		matches[i] = strings.TrimRight(url, ",.;:!?()'\"")
	}

	// Deduplicate URLs
	uniqueURLs := make(map[string]bool)
	for _, url := range matches {
		uniqueURLs[url] = true
	}

	// Convert map to slice
	result := make([]string, 0, len(uniqueURLs))
	for url := range uniqueURLs {
		result = append(result, url)
	}

	return result
}

// sanitizeFilename removes special characters from a filename
func sanitizeFilename(filename string) string {
	// Replace any non-alphanumeric characters (except underscore and hyphen) with underscore
	reg := regexp.MustCompile(`[^\w\-.]`)
	sanitized := reg.ReplaceAllString(filename, "_")

	// Limit length
	if len(sanitized) > 50 {
		sanitized = sanitized[:50]
	}

	return sanitized
}

// convertVideoToPost converts a YouTubeVideo to model.Post
func (c *YouTubeCrawler) convertVideoToPost(video *youtubemodel.YouTubeVideo) model.Post {
	// Get channel info for consistent naming - check cache first
	var channelName string
	var ok bool

	// For channel data
	var channel *youtubemodel.YouTubeChannel

	// Check if we already have this channel name in our cache
	if channelName, ok = channelNameCache[video.ChannelID]; ok {
		log.Debug().
			Str("channel_id", video.ChannelID).
			Str("channel_name", channelName).
			Msg("Using cached channel name for video conversion")
	} else {
		// Need to fetch from API
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error
		channel, err = c.client.GetChannelInfo(ctx, video.ChannelID)
		if err != nil {
			log.Warn().Err(err).Str("channel_id", video.ChannelID).Msg("Failed to get channel info for video conversion")
			channelName = video.ChannelID
		} else {
			channelName = channel.Title
			// Cache the result for future use
			channelNameCache[video.ChannelID] = channelName
			log.Debug().
				Str("channel_id", video.ChannelID).
				Str("channel_name", channelName).
				Msg("Cached channel name for future video conversions")
		}
	}

	// Calculate video engagement (likes + comments + views)
	engagement := int(video.LikeCount + video.CommentCount + (video.ViewCount / 100))

	// Make sure title is appropriate length for a title
	// If it's too long, it might be a mismapped description
	title := video.Title
	//if len(title) > 300 {
	//	// Try to find a shorter version by taking the first sentence or line
	//	shorterTitle := title
	//
	//	// First try to find the first sentence
	//	if idx := strings.Index(shorterTitle, "."); idx > 0 && idx < 100 {
	//		shorterTitle = shorterTitle[:idx+1]
	//	} else if idx := strings.Index(shorterTitle, "\n"); idx > 0 && idx < 100 {
	//		// If no sentence, try to find the first line
	//		shorterTitle = shorterTitle[:idx]
	//	} else if len(shorterTitle) > 100 {
	//		// If all else fails, truncate to a reasonable length
	//		shorterTitle = shorterTitle[:97] + "..."
	//	}
	//
	//	log.Info().
	//		Str("video_id", video.ID).
	//		Str("original_title", title).
	//		Str("shortened_title", shorterTitle).
	//		Int("original_length", len(title)).
	//		Int("shortened_length", len(shorterTitle)).
	//		Msg("Shortened unusually long title")
	//
	//	title = shorterTitle
	//}

	// Log title and description for debugging
	log.Debug().
		Str("video_id", video.ID).
		Str("title", video.Title).
		Str("first_50_chars_of_title", func() string {
			if len(video.Title) > 50 {
				return video.Title[:50] + "..."
			}
			return video.Title
		}()).
		Str("first_50_chars_of_description", func() string {
			if len(video.Description) > 50 {
				return video.Description[:50] + "..."
			}
			return video.Description
		}()).
		Bool("title_contains_description", strings.Contains(video.Title, video.Description) && len(video.Description) > 50).
		Bool("description_contains_title", strings.Contains(video.Description, video.Title) && len(video.Title) > 0).
		Int("title_length", len(video.Title)).
		Int("description_length", len(video.Description)).
		Msg("Video title and description analysis")

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

	// Parse duration string to seconds
	var videoLengthSeconds int
	if durationStr := video.Duration; durationStr != "" {
		duration, err := parseISO8601Duration(durationStr)
		if err == nil {
			videoLengthSeconds = duration
			log.Debug().Int("video_length_seconds", videoLengthSeconds).Msg("Parsed video duration")
		} else {
			log.Warn().Err(err).Str("duration", durationStr).Msg("Failed to parse video duration")
		}
	}

	// Set HasEmbedMedia flag
	hasEmbedMedia := true

	// Setup performance scores
	likesCount := int(video.LikeCount)
	commentsCount := int(video.CommentCount)
	viewsCount := float64(video.ViewCount)

	// Note: YouTube API may report zero comments even when comments exist
	// This can happen for several reasons:
	// 1. Comments are disabled for the video
	// 2. Comments are hidden by the channel owner
	// 3. API delay in updating comment counts
	// 4. API quota limits preventing full comment data retrieval

	// Extract URLs from description for outlinks
	outlinks := extractURLs(video.Description)

	// Create OCR data with thumbnail information
	// YouTube doesn't provide OCR text, but we can include thumbnails as image metadata
	var ocrData []model.OCRData
	// Add all available thumbnails to OCR data for completeness
	for quality, url := range video.Thumbnails {
		if url != "" {
			ocrData = append(ocrData, model.OCRData{
				ThumbURL: url,
				OCRText:  fmt.Sprintf("YouTube thumbnail: %s quality", quality),
			})
		}
	}

	// Extra debug log specifically for post title
	log.Info().
		Str("video_id", video.ID).
		Str("title_from_api", video.Title).
		Str("first_50_chars_title", func() string {
			if len(video.Title) > 50 {
				return video.Title[:50] + "..."
			}
			return video.Title
		}()).
		Str("first_50_chars_description", func() string {
			if len(video.Description) > 50 {
				return video.Description[:50] + "..."
			}
			return video.Description
		}()).
		Msg("Title right before post creation")

	// Create the post object
	post := model.Post{
		PostUID:        video.ID,
		ChannelName:    channelName,
		ChannelID:      0, // YouTube uses string IDs, not numeric
		URL:            videoURL,
		PublishedAt:    video.PublishedAt,
		CreatedAt:      time.Now(),
		Engagement:     engagement,
		PostTitle:      &title,
		Description:    video.Description,
		ViewsCount:     int(video.ViewCount),
		LikesCount:     int(video.LikeCount),
		CommentsCount:  int(video.CommentCount),
		ViewCount:      int(video.ViewCount),
		LikeCount:      int(video.LikeCount),
		CommentCount:   int(video.CommentCount),
		PlatformName:   "youtube",
		SearchableText: video.Title + " " + video.Description,
		AllText:        video.Title + " " + video.Description,
		PostType:       []string{"video"},
		CaptureTime:    time.Now(),
		ThumbURL:       thumbURL,
		MediaURL:       videoURL,
		Handle:         video.ChannelID,
		PostLink:       videoURL,
		// Additional fields
		VideoLength:   &videoLengthSeconds,
		HasEmbedMedia: &hasEmbedMedia,
		PerformanceScores: model.PerformanceScores{
			Likes:    &likesCount,
			Comments: &commentsCount,
			Views:    viewsCount,
		},
		// Add the outlinks
		Outlinks: outlinks,
		// Add the OCR data with thumbnail information
		OCRData: ocrData,
		// Create media data
		MediaData: model.MediaData{
			DocumentName: fmt.Sprintf("%s-%s.mp4", video.ID, sanitizeFilename(video.Title)),
		},
		// Add reactions as a map
		Reactions: map[string]int{"like": int(video.LikeCount)},
	}

	// Construct channel URL based on ID format
	channelURL := fmt.Sprintf("https://www.youtube.com/channel/%s", video.ChannelID)
	if len(video.ChannelID) > 0 && video.ChannelID[0] == '@' {
		// Handle username format (@username)
		channelURL = fmt.Sprintf("https://www.youtube.com/%s", video.ChannelID)
	}

	// Always fetch channel data to get engagement metrics if not already fetched
	if ok && channel == nil {
		// We have the channel name in cache, but need to fetch engagement data
		// Make an API call to get the full channel data
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error
		log.Debug().
			Str("channel_id", video.ChannelID).
			Msg("Fetching channel data to get engagement metrics")

		channel, err = c.client.GetChannelInfo(ctx, video.ChannelID)
		if err != nil {
			log.Warn().Err(err).Str("channel_id", video.ChannelID).Msg("Failed to get channel engagement data")
			// Continue with limited data
		}
	}

	// Use full channel data if available
	if channel != nil {
		// We have channel data, so use all the details
		post.ChannelData = model.ChannelData{
			ChannelName:         channel.Title,
			ChannelURL:          channelURL,
			ChannelURLExternal:  channelURL,
			ChannelProfileImage: channel.Thumbnails["default"],
			ChannelEngagementData: model.EngagementData{
				FollowerCount: int(channel.SubscriberCount),
				ViewsCount:    int(channel.ViewCount),
				PostCount:     int(channel.VideoCount),
			},
		}
	} else {
		// Fallback to basic data if channel info isn't available
		post.ChannelData = model.ChannelData{
			ChannelName:        channelName,
			ChannelURL:         channelURL,
			ChannelURLExternal: channelURL,
		}

		log.Warn().
			Str("channel_id", video.ChannelID).
			Str("channel_name", channelName).
			Msg("Using limited channel data without engagement metrics")
	}

	return post
}

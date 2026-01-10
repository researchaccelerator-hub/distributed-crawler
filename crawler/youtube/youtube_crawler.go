// Package youtube implements the YouTube-specific crawler functionality
package youtube

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"sync"
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
	client        youtubemodel.YouTubeClient
	stateManager  state.StateManagementInterface
	initialized   bool
	config        YouTubeCrawlerConfig
	defaultConfig YouTubeCrawlerConfig
	crawlLabel    string // Label for the crawl operation
}

// NewYouTubeCrawler creates a new YouTube crawler
func NewYouTubeCrawler() crawler.Crawler {
	// Set default configuration
	defaultConfig := YouTubeCrawlerConfig{
		SamplingMethod:   SamplingMethodChannel, // Default to channel-based sampling
		MinChannelVideos: 10,                    // Default to 10 minimum videos
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
					log.Debug().Str("sampling_method", methodStr).Msg("Using configured sampling method")
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
				log.Debug().Int64("min_channel_videos", crawlerConfig.MinChannelVideos).Msg("Using configured minimum channel videos")
			}
		}
	}

	// Validate configuration
	if crawlerConfig.SamplingMethod == SamplingMethodSnowball && len(crawlerConfig.SeedChannels) == 0 {
		log.Warn().Msg("Snowball sampling method selected but no seed channels provided, using default channel sampling")
		crawlerConfig.SamplingMethod = SamplingMethodChannel
	}

	// Check for crawl label in config
	if crawlLabelObj, ok := config["crawl_label"]; ok {
		if crawlLabel, ok := crawlLabelObj.(string); ok {
			c.crawlLabel = crawlLabel
			log.Info().Str("crawl_label", crawlLabel).Msg("Using configured crawl label")
		}
	}

	// Set the client, state manager, and configuration
	c.client = youtubeClient
	c.stateManager = stateManager
	c.config = crawlerConfig
	c.initialized = true

	log.Debug().
		Str("sampling_method", string(c.config.SamplingMethod)).
		Int64("min_channel_videos", c.config.MinChannelVideos).
		Int("seed_channels_count", len(c.config.SeedChannels)).
		Str("crawl_label", c.crawlLabel).
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
		ChannelID:           target.ID, // Set the proper YouTube channel ID
		ChannelName:         channel.Title,
		ChannelDescription:  channel.Description, // Add channel description
		ChannelURL:          channelURL,
		ChannelURLExternal:  channelURL,
		ChannelProfileImage: channel.Thumbnails["default"],
		CountryCode:         channel.Country,     // Add country code from YouTube data
		PublishedAt:         channel.PublishedAt, // Add channel creation date
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
		Int64("view_count", channel.ViewCount).
		Int64("video_count", channel.VideoCount).
		Str("country", channel.Country).
		Msg("Successfully retrieved YouTube channel info")

	return channelData, nil
}

// FetchMessages retrieves videos from YouTube
func (c *YouTubeCrawler) FetchMessages(ctx context.Context, job crawler.CrawlJob) (result crawler.CrawlResult, err error) {
	// Add panic recovery to prevent crawler crashes
	defer func() {
		if r := recover(); r != nil {
			log.Error().
				Interface("panic", r).
				Str("channel_id", job.Target.ID).
				Str("sampling_method", string(c.config.SamplingMethod)).
				Msg("Panic recovered in YouTube FetchMessages - marking channel as error")

			// Mark the channel as having an error and return error instead of crashing
			err = fmt.Errorf("panic in YouTube FetchMessages for channel %s: %v", job.Target.ID, r)
			result = crawler.CrawlResult{
				Posts:  []model.Post{},
				Errors: []error{err},
			}
		}
	}()

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
		Msg("Starting YouTube crawl with parallel processing")

	// Fetch videos using the configured sampling method
	var videos []*youtubemodel.YouTubeVideo

	// Create a context with timeout for the entire crawl
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	switch c.config.SamplingMethod {
	case SamplingMethodChannel:
		// Traditional channel-based sampling
		videos, err = c.client.GetVideosFromChannel(ctxWithTimeout, job.Target.ID, job.FromTime, job.ToTime, job.Limit)
		if err != nil {
			log.Error().Err(err).Str("channel_id", job.Target.ID).Msg("Failed to get videos from YouTube channel")
			return crawler.CrawlResult{}, err
		}

		log.Debug().
			Str("channel_id", job.Target.ID).
			Int("video_count", len(videos)).
			Msg("Retrieved videos from specific YouTube channel")

	case SamplingMethodRandom:
		// Random sampling using prefix generator with parallel processing
		sampleTargetSize := min(50, job.SamplesRemaining) //rough limit so all video id prefix matches are processed
		videos, err = c.client.GetRandomVideos(ctxWithTimeout, job.FromTime, job.ToTime, sampleTargetSize)
		if err != nil {
			log.Error().Err(err).Msg("Failed to get videos using random sampling")
			return crawler.CrawlResult{}, err
		}
		log.Info().
			Int("video_count", len(videos)).
			Msg("Retrieved batch of random videos using parallel random sampling")

	case SamplingMethodSnowball:
		// Snowball sampling based on seed channels with parallel processing
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

		videos, err = c.client.GetSnowballVideos(ctxWithTimeout, seedChannels, job.FromTime, job.ToTime, job.Limit)
		if err != nil {
			log.Error().Err(err).Strs("seed_channels", seedChannels).Msg("Failed to get videos using snowball sampling")
			return crawler.CrawlResult{}, err
		}

		log.Info().
			Int("video_count", len(videos)).
			Int("seed_channels_count", len(seedChannels)).
			Msg("Retrieved videos using parallel snowball sampling")

	default:
		return crawler.CrawlResult{}, fmt.Errorf("unknown sampling method: %s", c.config.SamplingMethod)
	}

	// Process videos in parallel to convert them to posts
	const maxPostWorkers = 10 // Maximum workers for post conversion

	// Use a WaitGroup to track when all post conversions are done
	var wg sync.WaitGroup

	// Create a mutex for protecting the posts slice
	var mu sync.Mutex
	posts := make([]model.Post, 0, len(videos))

	// Create a channel for videos to process
	videoCh := make(chan *youtubemodel.YouTubeVideo, len(videos))

	// Fill the channel with videos
	for _, video := range videos {
		videoCh <- video
	}
	// Close the channel to signal no more videos are coming
	close(videoCh)

	// Log the start of parallel processing
	log.Debug().
		Int("total_videos", len(videos)).
		Int("max_workers", maxPostWorkers).
		Msg("Starting parallel post conversion")

	// Create workers to process videos
	for i := 0; i < maxPostWorkers; i++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()

			for video := range videoCh {
				// Convert the video to a post
				post := c.convertVideoToPost(video)

				result := job.NullValidator.ValidatePost(&post)
				if !result.Valid {
					log.Error().Strs("errors", result.Errors).Msg("Missing critical fields in youtube post data")
				}

				// Store the post in state manager
				if err := c.stateManager.StorePost(video.ChannelID, post); err != nil {
					log.Error().
						Err(err).
						Str("video_id", video.ID).
						Int("worker_id", workerID).
						Msg("Failed to save video post")
					// Continue with next video
				}

				// Add the post to the result array with mutex protection
				mu.Lock()
				posts = append(posts, post)
				mu.Unlock()

				// Log progress periodically
				if workerID == 0 && len(posts)%10 == 0 {
					log.Debug().
						Int("processed_posts", len(posts)).
						Int("total_videos", len(videos)).
						Int("worker_id", workerID).
						Msg("Post conversion progress")
				}
			}

			log.Debug().
				Int("worker_id", workerID).
				Msg("Post conversion worker completed")
		}(i)
	}

	// Wait for all workers to finish
	wg.Wait()

	log.Info().
		Int("post_count", len(posts)).
		Str("sampling_method", string(c.config.SamplingMethod)).
		Msg("YouTube crawl with parallel processing completed successfully")

	// Apply random sampling if requested
	if job.SampleSize > 0 {
		posts = applySampling(posts, job.SampleSize)
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
	// Get channel info for consistent naming and engagement data - check cache first
	var channelName string
	var ok bool

	// For channel data
	var channel *youtubemodel.YouTubeChannel

	// Check if we already have this channel's full data in the client's cache
	channel, err := c.client.GetChannelInfo(context.Background(), video.ChannelID)
	if err == nil {
		ok = true
	} else {
		ok = false
	}

	if ok {
		channelName = channel.Title
		log.Debug().
			Str("channel_id", video.ChannelID).
			Str("channel_name", channelName).
			Int64("subscriber_count", channel.SubscriberCount).
			Int64("video_count", channel.VideoCount).
			Msg("Using cached channel data for video conversion")
	} else {
		log.Warn().Err(err).Str("channel_id", video.ChannelID).Msg("Failed to get channel info for video conversion")
		channelName = video.ChannelID
		channel = nil // Set to nil so we use fallback channel data
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
	var videoLengthPtr *int
	if durationStr := video.Duration; durationStr != "" {
		duration, err := parseISO8601Duration(durationStr)
		if err == nil {
			videoLengthPtr = &duration
			log.Debug().Int("video_length_seconds", duration).Msg("Parsed video duration")
		} else {
			log.Warn().Err(err).Str("duration", durationStr).Str("log_tag", "FOCUS").Msg("Failed to parse video duration")
		}
	} else {
		log.Warn().Str("video_id", video.ID).Str("log_tag", "FOCUS").Msg("Duration is empty")
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
	log.Debug().
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
		PostLink:     videoURL,
		ChannelID:    video.ChannelID, // Set the proper YouTube channel ID
		PostUID:      video.ID,
		URL:          videoURL,
		PublishedAt:  video.PublishedAt,
		CreatedAt:    time.Now(),
		LanguageCode: video.Language,
		Engagement:   engagement,
		ViewCount:    int(video.ViewCount),
		LikeCount:    int(video.LikeCount),
		// ShareCount unavailable
		CommentCount: int(video.CommentCount),
		CrawlLabel:   c.crawlLabel, // Add the crawl label to identify crawl source
		// ListIDs
		ChannelName: channelName,
		// SearchTerms unavailable
		// SearchTermIDs unavailable
		// ProjectIDs unavailable
		// ExerciseIDs unavailable
		// LabelData unavailable
		// LabelsMetadata unavailable
		// ProjectLabeledPostIDs unavailable
		// LabelerIDs unavailable
		// AllLabels unavailable
		// LabelIDs unavailable
		// IsAd unavailable
		// TranscriptText unavailable
		// ImageText unavailable
		VideoLength: videoLengthPtr,
		// IsVerified unavailable
		// ChannelData added below
		PlatformName: "youtube",
		// SharedID unavailable
		// QuotedID unavailable
		// RepliedID unavailable
		// AILabel unavailable
		// RootPostID unavailable
		// EngagementStepsCount unavailable
		OCRData: ocrData, // Add the OCR data with thumbnail information
		PerformanceScores: model.PerformanceScores{
			Likes:    &likesCount,
			Comments: &commentsCount,
			Views:    viewsCount,
		},
		HasEmbedMedia: &hasEmbedMedia,
		Description:   video.Description,
		// RepostChannelData unavailable
		PostType: []string{"video"},
		// InnerLink unavailable
		PostTitle: &title,
		// Create media data
		MediaData: model.MediaData{
			DocumentName: fmt.Sprintf("%s-%s.mp4", video.ID, sanitizeFilename(video.Title)),
		},
		// IsReply unavailable
		// AdFields unavailable
		LikesCount: int(video.LikeCount),
		// SharesCount unavailable
		CommentsCount:  int(video.CommentCount),
		ViewsCount:     int(video.ViewCount),
		SearchableText: video.Title + " " + video.Description,
		AllText:        video.Title + " " + video.Description,
		// ContrastAgentProjectIDs unavailable
		// AgentIDs unavailable
		// SegmentIDs unavailable
		ThumbURL: thumbURL,
		MediaURL: videoURL,
		// Comments not available yet
		Reactions:   map[string]int{"like": int(video.LikeCount)}, // Add reactions as a map
		Outlinks:    outlinks,                                     // Add the outlinks
		CaptureTime: time.Now(),
		Handle:      video.ChannelID,
	}

	// Construct channel URL based on ID format
	channelURL := fmt.Sprintf("https://www.youtube.com/channel/%s", video.ChannelID)
	if len(video.ChannelID) > 0 && video.ChannelID[0] == '@' {
		// Handle username format (@username)
		channelURL = fmt.Sprintf("https://www.youtube.com/%s", video.ChannelID)
	}

	// With our improved cache, we no longer need to fetch channel data separately
	// as we now cache the complete channel object including engagement metrics

	// Use full channel data if available
	if channel != nil {
		// We have channel data, so use all the details
		post.ChannelData = model.ChannelData{
			ChannelID:           video.ChannelID, // Set the proper YouTube channel ID
			ChannelName:         channel.Title,
			ChannelDescription:  channel.Description, // Add channel description
			ChannelProfileImage: channel.Thumbnails["default"],
			ChannelEngagementData: model.EngagementData{
				FollowerCount: int(channel.SubscriberCount),
				// FollowingCount unavailable
				// LikeCount unavailable
				PostCount:  int(channel.VideoCount),
				ViewsCount: int(channel.ViewCount),
				// CommentCount unavailable
				// ShareCount unavailable
			},
			ChannelURLExternal: channelURL,
			ChannelURL:         channelURL,
			CountryCode:        channel.Country,     // Add country code from YouTube data
			PublishedAt:        channel.PublishedAt, // Add channel creation date
		}
	} else {
		// Fallback to basic data if channel info isn't available
		// But still include any engagement data we have from the video itself
		post.ChannelData = model.ChannelData{
			ChannelID:          video.ChannelID, // Set the proper YouTube channel ID
			ChannelName:        channelName,
			ChannelDescription: "", // Empty description when not available
			// ChannelProfileImage unavailable
			ChannelEngagementData: model.EngagementData{
				// Use the video's data to provide some engagement metrics
				ViewsCount:   int(video.ViewCount),
				LikeCount:    int(video.LikeCount),
				CommentCount: int(video.CommentCount),
				// These will remain zero as we don't have this information
				FollowerCount:  0,
				FollowingCount: 0,
				PostCount:      0,
				ShareCount:     0,
			},
			ChannelURLExternal: channelURL,
			ChannelURL:         channelURL,
			CountryCode:        "",                // No country data available when channel info is missing
			PublishedAt:        video.PublishedAt, // Use video's publish date as fallback
		}

		log.Warn().
			Str("channel_id", video.ChannelID).
			Str("channel_name", channelName).
			Msg("Using limited channel data without engagement metrics")
	}
	return post
}

// applySampling applies random sampling to posts if sampleSize is specified
func applySampling(posts []model.Post, sampleSize int) []model.Post {
	if sampleSize <= 0 || len(posts) <= sampleSize {
		return posts
	}

	log.Info().
		Int("total_posts", len(posts)).
		Int("sample_size", sampleSize).
		Msg("Applying random sampling to YouTube posts")

	// Create a random seed based on current time
	rand.Seed(time.Now().UnixNano())

	// Create a copy of the slice to avoid modifying the original
	postsCopy := make([]model.Post, len(posts))
	copy(postsCopy, posts)

	// Shuffle the posts using Fisher-Yates algorithm
	for i := len(postsCopy) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		postsCopy[i], postsCopy[j] = postsCopy[j], postsCopy[i]
	}

	// Take only the first sampleSize posts
	sampledPosts := postsCopy[:sampleSize]

	log.Info().
		Int("original_count", len(posts)).
		Int("sampled_count", len(sampledPosts)).
		Msg("Random sampling completed for YouTube posts")

	return sampledPosts
}

package client

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"sync"
	"time"

	youtubemodel "github.com/researchaccelerator-hub/telegram-scraper/model/youtube"
	"github.com/rs/zerolog/log"
	"google.golang.org/api/option"
	ytapi "google.golang.org/api/youtube/v3"
)

// BatchConfig holds dynamic batching configuration
type BatchConfig struct {
	MinBatchSize     int
	MaxBatchSize     int
	OptimalBatchSize int
}

// YouTubeDataClient implements the youtubemodel.YouTubeClient interface for accessing YouTube Data API
type YouTubeDataClient struct {
	service *ytapi.Service
	apiKey  string

	// Unified caching system
	channelCache         map[string]*youtubemodel.YouTubeChannel
	uploadsPlaylistCache map[string]string                     // Maps channelID to uploadsPlaylistID
	videoStatsCache      map[string]*youtubemodel.YouTubeVideo // Cache for video statistics
	cacheMutex           sync.RWMutex

	// Batch configuration
	batchConfig BatchConfig

	// RNG instance for generating random values
	rng   *rand.Rand
	rngMu sync.Mutex
}

type RandomPrefix struct {
	Prefix           string
	ValidVideoIDs    []string
	InvalidVideoIDs  []string
	ResultCount      int
	TotalResultCount int64
}

// NewYouTubeDataClient creates a new YouTube data client
func NewYouTubeDataClient(apiKey string) (*YouTubeDataClient, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("YouTube API key is required")
	}

	// Create a new random number generator seeded with current time
	source := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(source)

	// Initialize batch configuration
	batchConfig := BatchConfig{
		MinBatchSize:     10,
		MaxBatchSize:     50,
		OptimalBatchSize: 50,
	}

	return &YouTubeDataClient{
		apiKey:               apiKey,
		channelCache:         make(map[string]*youtubemodel.YouTubeChannel),
		uploadsPlaylistCache: make(map[string]string),
		videoStatsCache:      make(map[string]*youtubemodel.YouTubeVideo),
		batchConfig:          batchConfig,
		rng:                  rng,
	}, nil
}

// Connect establishes a connection to the YouTube API
func (c *YouTubeDataClient) Connect(ctx context.Context) error {
	log.Info().Msg("Connecting to YouTube API")

	//// Create a new HTTP client with default timeout
	//httpClient := &http.Client{
	//	Timeout: 30 * time.Second,
	//}

	// Create YouTube service
	log.Debug().Str("api_key_length", fmt.Sprintf("%d chars", len(c.apiKey))).Msg("Creating YouTube service with API key")
	if c.apiKey == "" {
		log.Error().Msg("YouTube API key is empty! This will cause authentication errors")
	}

	service, err := ytapi.NewService(ctx, option.WithAPIKey(c.apiKey))
	if err != nil {
		log.Error().Err(err).Msg("Failed to create YouTube service")
		return fmt.Errorf("failed to create YouTube service: %w", err)
	}

	c.service = service
	log.Info().Msg("Connected to YouTube API successfully")
	return nil
}

// Disconnect closes the connection to the YouTube API
func (c *YouTubeDataClient) Disconnect(ctx context.Context) error {
	// No explicit disconnect needed for the YouTube API client
	c.service = nil
	return nil
}

// GetChannelInfo retrieves information about a YouTube channel
func (c *YouTubeDataClient) GetChannelInfo(ctx context.Context, channelID string) (*youtubemodel.YouTubeChannel, error) {
	log.Info().Msg("GetChannelInfo-YoutubeDataClient: Getting Youtube channel info")

	if c.service == nil {
		return nil, fmt.Errorf("YouTube client not connected")
	}

	// Check cache first with read lock
	c.cacheMutex.RLock()
	cachedChannel, exists := c.channelCache[channelID]
	c.cacheMutex.RUnlock()

	if exists {
		log.Debug().
			Str("channel_id", channelID).
			Str("title", cachedChannel.Title).
			Msg("Using cached channel info instead of API call")
		return cachedChannel, nil
	}

	log.Info().Str("channel_id", channelID).Str("api_key_length", fmt.Sprintf("%d chars", len(c.apiKey))).Msg("Fetching YouTube channel info")

	// Check if API key is present
	if c.apiKey == "" {
		log.Error().Msg("Making YouTube API call with empty API key - this will fail")
	}

	// Check if the ID is a username (starts with @) or a channel ID
	var part = []string{"snippet", "statistics", "contentDetails"}
	var call *ytapi.ChannelsListCall

	if len(channelID) > 0 && channelID[0] == '@' {
		// Handle username format (@username)
		call = c.service.Channels.List(part).ForUsername(channelID[1:])
	} else if len(channelID) > 2 && channelID[0:2] == "UC" {
		// Handle channel ID format (UCxxx...)
		call = c.service.Channels.List(part).Id(channelID)
	} else {
		// Try as username without @ symbol
		call = c.service.Channels.List(part).ForUsername(channelID)
	}

	response, err := call.MaxResults(1).Context(ctx).Do()
	if err != nil {
		log.Error().Err(err).Str("channel_id", channelID).Msg("Failed to get channel from YouTube API")
		return nil, fmt.Errorf("failed to get channel from YouTube API: %w", err)
	}

	if len(response.Items) == 0 {
		log.Error().Str("channel_id", channelID).Msg("Channel not found on YouTube")
		return nil, fmt.Errorf("channel not found on YouTube: %s", channelID)
	}

	// Get the first (and should be only) result
	item := response.Items[0]

	// Convert subscriber count
	subscriberCount := int64(item.Statistics.SubscriberCount)
	viewCount := int64(item.Statistics.ViewCount)
	videoCount := int64(item.Statistics.VideoCount)

	// Parse published date
	publishedAt, _ := time.Parse(time.RFC3339, item.Snippet.PublishedAt)

	// Extract thumbnails
	thumbnails := make(map[string]string)
	if item.Snippet.Thumbnails != nil {
		if item.Snippet.Thumbnails.Default != nil {
			thumbnails["default"] = item.Snippet.Thumbnails.Default.Url
		}
		if item.Snippet.Thumbnails.Medium != nil {
			thumbnails["medium"] = item.Snippet.Thumbnails.Medium.Url
		}
		if item.Snippet.Thumbnails.High != nil {
			thumbnails["high"] = item.Snippet.Thumbnails.High.Url
		}
		if item.Snippet.Thumbnails.Standard != nil {
			thumbnails["standard"] = item.Snippet.Thumbnails.Standard.Url
		}
		if item.Snippet.Thumbnails.Maxres != nil {
			thumbnails["maxres"] = item.Snippet.Thumbnails.Maxres.Url
		}
	}

	// Create the YouTube channel object
	channel := &youtubemodel.YouTubeChannel{
		ID:              item.Id,
		Title:           item.Snippet.Title,
		Description:     item.Snippet.Description,
		Thumbnails:      thumbnails,
		SubscriberCount: subscriberCount,
		VideoCount:      videoCount,
		ViewCount:       viewCount,
		Country:         item.Snippet.Country, // Add country information
		PublishedAt:     publishedAt,
	}

	// Store in cache with write lock
	c.cacheMutex.Lock()
	c.channelCache[channelID] = channel

	// If the actual channelID from the API is different from the input channelID,
	// cache it under both keys to ensure future lookups hit the cache
	if item.Id != channelID {
		c.channelCache[item.Id] = channel
		log.Debug().
			Str("input_channel_id", channelID).
			Str("actual_channel_id", item.Id).
			Msg("Cached channel under both input ID and actual ID")
	}
	c.cacheMutex.Unlock()

	log.Info().
		Str("channel_id", channel.ID).
		Str("title", channel.Title).
		Int64("subscribers", channel.SubscriberCount).
		Int64("view_count", channel.ViewCount).
		Int64("video_count", channel.VideoCount).
		Str("country", channel.Country).
		// Msg("YouTube channel info retrieved - all engagement data populated")
		Msg("GetChannelInfo-YoutubeDataClient: YouTube channel info retrieved - all engagement data populated")

	return channel, nil
}

// GetVideos retrieves videos from a YouTube channel using the default method
func (c *YouTubeDataClient) GetVideos(ctx context.Context, channelID string, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	return c.GetVideosFromChannel(ctx, channelID, fromTime, toTime, limit)
}

// GetVideosFromChannel retrieves videos from a specific YouTube channel
func (c *YouTubeDataClient) GetVideosFromChannel(ctx context.Context, channelID string, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	if c.service == nil {
		return nil, fmt.Errorf("YouTube client not connected")
	}

	// Use current time as default if toTime is zero
	effectiveToTime := toTime
	if effectiveToTime.IsZero() {
		effectiveToTime = time.Now()
		log.Debug().
			Time("original_to_time", toTime).
			Time("effective_to_time", effectiveToTime).
			Msg("Using current time as default toTime value")
	}

	// Handle negative or zero limit as "no limit"
	effectiveLimit := limit
	if effectiveLimit <= 0 {
		effectiveLimit = 1000000 // Use a large number as "unlimited"
		log.Debug().
			Int("original_limit", limit).
			Int("effective_limit", effectiveLimit).
			Msg("Negative or zero limit detected - treating as unlimited")
	}

	log.Info().
		Str("channel_id", channelID).
		Time("from_time", fromTime).
		Time("to_time", effectiveToTime).
		Int("original_limit", limit).
		Int("effective_limit", effectiveLimit).
		Msg("Fetching videos from YouTube channel")

	// First, check cache for the uploads playlist ID
	var uploadsPlaylistID string
	var exists bool

	c.cacheMutex.RLock()
	uploadsPlaylistID, exists = c.uploadsPlaylistCache[channelID]
	c.cacheMutex.RUnlock()

	if exists {
		log.Debug().
			Str("channel_id", channelID).
			Str("uploads_playlist_id", uploadsPlaylistID).
			Msg("Using cached uploads playlist ID instead of API call")
	} else {
		// Need to make an API call to get the uploads playlist ID
		var part = []string{"contentDetails"}
		var call *ytapi.ChannelsListCall

		// Detailed debug logging for channel ID format detection
		log.Debug().
			Str("raw_channel_id", channelID).
			Bool("starts_with_@", len(channelID) > 0 && channelID[0] == '@').
			Bool("appears_to_be_channel_id", len(channelID) > 2 && channelID[0:2] == "UC").
			Msg("Determining channel ID format for uploads playlist retrieval")

		if len(channelID) > 0 && channelID[0] == '@' {
			// Handle username format (@username)
			username := channelID[1:]
			log.Debug().Str("username", username).Msg("Using ForUsername API call with username (without @)")
			call = c.service.Channels.List(part).ForUsername(username)
		} else if len(channelID) > 2 && channelID[0:2] == "UC" {
			// Handle channel ID format (UCxxx...)
			log.Debug().Str("channel_id", channelID).Msg("Using Id API call with channel ID")
			call = c.service.Channels.List(part).Id(channelID)
		} else {
			// Try as username without @ symbol
			log.Debug().Str("possible_username", channelID).Msg("Trying as username without @ symbol")
			call = c.service.Channels.List(part).ForUsername(channelID)
		}

		response, err := call.MaxResults(1).Context(ctx).Do()
		if err != nil {
			log.Error().Err(err).Str("channel_id", channelID).Msg("Failed to get channel from YouTube API")
			return nil, fmt.Errorf("failed to get channel from YouTube API: %w", err)
		}

		if len(response.Items) == 0 {
			log.Error().Str("channel_id", channelID).Msg("Channel not found on YouTube")
			return nil, fmt.Errorf("channel not found on YouTube: %s", channelID)
		}

		// Get the uploads playlist ID and log it
		uploadsPlaylistID = response.Items[0].ContentDetails.RelatedPlaylists.Uploads

		if uploadsPlaylistID == "" {
			log.Error().Str("channel_id", channelID).Msg("Channel found but no uploads playlist available")
			return nil, fmt.Errorf("no uploads playlist available for channel: %s", channelID)
		}

		// Store in cache with write lock
		c.cacheMutex.Lock()
		c.uploadsPlaylistCache[channelID] = uploadsPlaylistID

		// If the API returned a channel ID different from what was requested, cache it under both
		if response.Items[0].Id != channelID {
			c.uploadsPlaylistCache[response.Items[0].Id] = uploadsPlaylistID
			log.Debug().
				Str("input_channel_id", channelID).
				Str("actual_channel_id", response.Items[0].Id).
				Str("uploads_playlist_id", uploadsPlaylistID).
				Msg("Cached uploads playlist ID under both input ID and actual ID")
		}
		c.cacheMutex.Unlock()

		log.Debug().
			Str("channel_id", channelID).
			Str("uploads_playlist_id", uploadsPlaylistID).
			Msg("Retrieved and cached uploads playlist ID for channel")
	}

	// Now get videos from this playlist with optimized batching
	videos := make([]*youtubemodel.YouTubeVideo, 0)
	var nextPageToken string
	pageCounter := 0

	// Optimization: Batch video IDs for more efficient Videos.List calls
	const maxPlaylistBatchSize = 50 // Maximum videos per playlist call
	const maxVideosBatchSize = 50   // Maximum videos per Videos.List call

	// Accumulate video IDs and metadata across multiple playlist pages
	batchVideoIDs := make([]string, 0, maxVideosBatchSize)
	batchVideoMap := make(map[string]*youtubemodel.YouTubeVideo)

	log.Debug().
		Str("channel_id", channelID).
		Str("uploads_playlist_id", uploadsPlaylistID).
		Int("original_limit", limit).
		Int("effective_limit", effectiveLimit).
		Msg("Starting optimized video retrieval from uploads playlist")

	// Early termination flags for time-based optimization
	consecutiveOldVideos := 0
	const maxConsecutiveOldVideos = 5 // Stop if we see too many old videos in a row

	for len(videos) < effectiveLimit {
		pageCounter++
		maxResultsPerPage := maxPlaylistBatchSize // Always use maximum for efficiency

		log.Debug().
			Str("playlist_id", uploadsPlaylistID).
			Int("page_number", pageCounter).
			Int("max_results_per_page", maxResultsPerPage).
			Str("page_token", nextPageToken).
			Msg("Fetching playlist items page")

		// Fetch playlist items (videos)
		playlistCall := c.service.PlaylistItems.List([]string{"snippet", "contentDetails"}).
			PlaylistId(uploadsPlaylistID).
			MaxResults(int64(maxResultsPerPage)).
			Context(ctx)

		if nextPageToken != "" {
			playlistCall = playlistCall.PageToken(nextPageToken)
		}

		playlistResponse, err := playlistCall.Do()
		if err != nil {
			log.Error().Err(err).Str("playlist_id", uploadsPlaylistID).Msg("Failed to get videos from playlist")
			return nil, fmt.Errorf("failed to get videos from playlist: %w", err)
		}

		// If no items returned, break the loop
		if len(playlistResponse.Items) == 0 {
			log.Warn().
				Str("playlist_id", uploadsPlaylistID).
				Int("page_number", pageCounter).
				Msg("No items returned for this page of results")
			break
		}

		log.Debug().
			Str("playlist_id", uploadsPlaylistID).
			Int("items_count", len(playlistResponse.Items)).
			Str("next_page_token", playlistResponse.NextPageToken).
			Msg("Retrieved playlist items page")

		// Process each video in the playlist
		itemsSkippedBeforeFromTime := 0
		itemsSkippedAfterToTime := 0
		itemsSkippedParseError := 0
		itemsAccepted := 0
		oldVideosInThisPage := 0

		for i, item := range playlistResponse.Items {
			// Basic item info for debugging
			videoID := item.ContentDetails.VideoId
			log.Debug().
				Int("item_index", i).
				Str("video_id", videoID).
				Str("published_at_raw", item.Snippet.PublishedAt).
				Str("title", item.Snippet.Title).
				Msg("Processing playlist item")

			// Parse published date
			publishedAt, err := time.Parse(time.RFC3339, item.Snippet.PublishedAt)
			if err != nil {
				log.Warn().Err(err).Str("date", item.Snippet.PublishedAt).Msg("Failed to parse video published date")
				itemsSkippedParseError++
				continue
			}

			// Early termination optimization: if videos are too old, stop processing
			if !fromTime.IsZero() && publishedAt.Before(fromTime) {
				log.Debug().
					Time("published_at", publishedAt).
					Time("from_time", fromTime).
					Str("video_id", videoID).
					Msg("Skipping video: published before fromTime")
				itemsSkippedBeforeFromTime++
				oldVideosInThisPage++
				consecutiveOldVideos++
				continue
			} else {
				consecutiveOldVideos = 0 // Reset counter when we find a newer video
			}

			if publishedAt.After(effectiveToTime) {
				log.Debug().
					Time("published_at", publishedAt).
					Time("to_time", effectiveToTime).
					Str("video_id", videoID).
					Msg("Skipping video: published after toTime")
				itemsSkippedAfterToTime++
				continue
			}

			log.Debug().
				Time("published_at", publishedAt).
				Time("from_time", fromTime).
				Time("to_time", effectiveToTime).
				Str("video_id", videoID).
				Msg("Video is within the requested time range")

			// Extract thumbnails
			thumbnails := make(map[string]string)
			if item.Snippet.Thumbnails != nil {
				if item.Snippet.Thumbnails.Default != nil {
					thumbnails["default"] = item.Snippet.Thumbnails.Default.Url
				}
				if item.Snippet.Thumbnails.Medium != nil {
					thumbnails["medium"] = item.Snippet.Thumbnails.Medium.Url
				}
				if item.Snippet.Thumbnails.High != nil {
					thumbnails["high"] = item.Snippet.Thumbnails.High.Url
				}
				if item.Snippet.Thumbnails.Standard != nil {
					thumbnails["standard"] = item.Snippet.Thumbnails.Standard.Url
				}
				if item.Snippet.Thumbnails.Maxres != nil {
					thumbnails["maxres"] = item.Snippet.Thumbnails.Maxres.Url
				}
			}

			// Log snippet data for debugging
			log.Debug().
				Str("video_id", videoID).
				Str("raw_title", item.Snippet.Title).
				Str("raw_description", item.Snippet.Description).
				Int("title_length", len(item.Snippet.Title)).
				Int("description_length", len(item.Snippet.Description)).
				Msg("Raw video data from YouTube API")

			description := item.Snippet.Description

			// Ensure title is actually the title, not a description
			title := item.Snippet.Title
			// If title contains newlines or is very long, it might be misplaced description
			if strings.Contains(title, "\n") || len(title) > 500 {
				log.Warn().
					Str("video_id", videoID).
					Int("title_length", len(title)).
					Bool("contains_newlines", strings.Contains(title, "\n")).
					Msg("Found unusually long or formatted title, might be description")
			}

			// Create basic video object (without statistics yet)
			video := &youtubemodel.YouTubeVideo{
				ID:          videoID,
				ChannelID:   channelID,
				Title:       title,
				Description: description,
				PublishedAt: publishedAt,
				Thumbnails:  thumbnails,
				Language:    "", // Will be populated later from videos API
			}

			// Add to batch for processing
			batchVideoIDs = append(batchVideoIDs, videoID)
			batchVideoMap[videoID] = video
			itemsAccepted++
		}

		log.Info().
			Int("total_items", len(playlistResponse.Items)).
			Int("items_accepted", itemsAccepted).
			Int("items_skipped_before_from_time", itemsSkippedBeforeFromTime).
			Int("items_skipped_after_to_time", itemsSkippedAfterToTime).
			Int("items_skipped_parse_error", itemsSkippedParseError).
			Int("consecutive_old_videos", consecutiveOldVideos).
			Int("batch_size", len(batchVideoIDs)).
			Msg("Processed playlist items")

		consecutiveOldVideosExceeded := consecutiveOldVideos >= maxConsecutiveOldVideos

		// Process batch when it reaches optimal size or we're at the end
		shouldProcessBatch := len(batchVideoIDs) >= c.batchConfig.OptimalBatchSize ||
			playlistResponse.NextPageToken == "" ||
			len(videos)+len(batchVideoIDs) >= effectiveLimit ||
			consecutiveOldVideosExceeded

		if shouldProcessBatch && len(batchVideoIDs) > 0 {
			// Check cache first for any videos we already have stats for
			cachedVideos := make(map[string]*youtubemodel.YouTubeVideo)
			uncachedVideoIDs := make([]string, 0, len(batchVideoIDs))

			for _, videoID := range batchVideoIDs {
				if cachedVideo, exists := c.getCachedVideoStats(videoID); exists {
					cachedVideos[videoID] = cachedVideo
					log.Debug().Str("video_id", videoID).Msg("Using cached video statistics")
				} else {
					uncachedVideoIDs = append(uncachedVideoIDs, videoID)
				}
			}

			log.Debug().
				Int("total_videos", len(batchVideoIDs)).
				Int("cached_videos", len(cachedVideos)).
				Int("uncached_videos", len(uncachedVideoIDs)).
				Strs("uncached_video_ids", uncachedVideoIDs).
				Msg("Processing batch with cache optimization")

			// Add cached videos to results first
			for videoID, cachedVideo := range cachedVideos {
				if video, ok := batchVideoMap[videoID]; ok && len(videos) < effectiveLimit {
					// Update with cached statistics
					video.ViewCount = cachedVideo.ViewCount
					video.LikeCount = cachedVideo.LikeCount
					video.CommentCount = cachedVideo.CommentCount
					video.Duration = cachedVideo.Duration
					video.Language = cachedVideo.Language
					videos = append(videos, video)
				}
			}

			// Only make API call for uncached videos
			if len(uncachedVideoIDs) > 0 {
				// Get statistics for uncached videos only
				videosCall := c.service.Videos.List([]string{"snippet", "statistics", "contentDetails"}).
					Id(uncachedVideoIDs...).
					Context(ctx)

				videosResponse, err := videosCall.Do()
				if err != nil {
					log.Error().Err(err).Strs("video_ids", uncachedVideoIDs).Msg("Failed to get video statistics for uncached batch")
					// Continue with basic information only

					// Even without statistics, add the videos to the results to avoid losing them
					for _, video := range batchVideoMap {
						if len(videos) < effectiveLimit {
							videos = append(videos, video)
						}
					}

					log.Warn().
						Int("videos_added_without_stats", len(batchVideoMap)).
						Msg("Added batch videos without statistics due to API error")
				} else {
					log.Debug().
						Int("stats_response_items", len(videosResponse.Items)).
						Int("expected_items", len(batchVideoIDs)).
						Msg("Retrieved video statistics for batch")

					// Track stats retrieval success rate
					statsFound := 0

					// Update videos with statistics
					for _, videoItem := range videosResponse.Items {
						if video, ok := batchVideoMap[videoItem.Id]; ok && len(videos) < effectiveLimit {
							// Parse statistics
							viewCount := int64(videoItem.Statistics.ViewCount)
							likeCount := int64(videoItem.Statistics.LikeCount)
							commentCount := int64(videoItem.Statistics.CommentCount)

							video.ViewCount = viewCount
							video.LikeCount = likeCount
							video.CommentCount = commentCount
							video.Duration = videoItem.ContentDetails.Duration

							// Get language from snippet if available
							if videoItem.Snippet != nil {
								if videoItem.Snippet.DefaultLanguage != "" {
									video.Language = videoItem.Snippet.DefaultLanguage
								} else if videoItem.Snippet.DefaultAudioLanguage != "" {
									// Fall back to default audio language if default language is not set
									video.Language = videoItem.Snippet.DefaultAudioLanguage
								}
							}

							// Cache the video statistics for future use
							c.cacheVideoStats(video)

							// Add to results
							videos = append(videos, video)
							statsFound++

							log.Debug().
								Str("video_id", video.ID).
								Str("title", video.Title).
								Int64("view_count", viewCount).
								Int64("like_count", likeCount).
								Int64("comment_count", commentCount).
								Bool("comments_disabled", videoItem.Statistics.CommentCount == 0 && viewCount > 1000). // Heuristic for detecting disabled comments
								Bool("api_reports_comment_count", videoItem.Statistics.CommentCount > 0).
								Str("duration", video.Duration).
								Str("language", video.Language).
								Msg("Added video with statistics")
						} else {
							log.Warn().
								Str("video_id", videoItem.Id).
								Bool("in_map", ok).
								Int("videos_count", len(videos)).
								Int("effective_limit", effectiveLimit).
								Msg("Received statistics for video not in our map or limit reached")
						}
					}

					// Count videos with zero comments for debugging purposes
					videosWithZeroComments := 0
					for _, videoItem := range videosResponse.Items {
						if videoItem.Statistics.CommentCount == 0 {
							videosWithZeroComments++
						}
					}

					log.Info().
						Int("total_videos_processed", len(batchVideoMap)).
						Int("stats_found", statsFound).
						Int("stats_missing", len(batchVideoMap)-statsFound).
						Int("videos_with_zero_comments", videosWithZeroComments).
						Float64("zero_comments_percentage", float64(videosWithZeroComments)/float64(len(videosResponse.Items))*100).
						Msg("Processed video statistics for batch")

					// Check for videos that didn't get statistics
					if statsFound < len(batchVideoMap) {
						missingStats := []string{}
						for videoID, video := range batchVideoMap {
							// Check if this video was not processed in the stats response
							found := false
							for _, processedVideo := range videos {
								if processedVideo.ID == videoID {
									found = true
									break
								}
							}

							if !found && len(videos) < effectiveLimit {
								// This video didn't get stats, add it anyway
								videos = append(videos, video)
								missingStats = append(missingStats, videoID)

								log.Debug().
									Str("video_id", video.ID).
									Str("title", video.Title).
									Msg("Added video without statistics")
							}
						}

						if len(missingStats) > 0 {
							log.Warn().
								Strs("video_ids_missing_stats", missingStats).
								Msg("Some videos were added without statistics")
						}
					}
				} // End of uncached videos API call
			} // End of uncached videos check

			// Clear batch for next iteration
			batchVideoIDs = batchVideoIDs[:0]
			batchVideoMap = make(map[string]*youtubemodel.YouTubeVideo)

			log.Debug().
				Int("videos_collected", len(videos)).
				Int("effective_limit", effectiveLimit).
				Msg("Completed batch processing")
		}

		// Check if we've reached the limit or no more pages
		if len(videos) >= effectiveLimit || playlistResponse.NextPageToken == "" || consecutiveOldVideosExceeded {
			log.Debug().
				Int("current_videos_count", len(videos)).
				Int("original_limit", limit).
				Int("effective_limit", effectiveLimit).
				Bool("has_next_page", playlistResponse.NextPageToken != "").
				Msg("Evaluating whether to fetch next page")

			if len(videos) >= effectiveLimit {
				log.Debug().Msg("Reached effective video limit, stopping pagination")
			}
			if playlistResponse.NextPageToken == "" {
				log.Debug().Msg("No more pages available, stopping pagination")
			}
			if consecutiveOldVideosExceeded {
				log.Info().
					Int("consecutive_old_videos", consecutiveOldVideos).
					Int("max_allowed", maxConsecutiveOldVideos).
					Msg("Stopping pagination due to too many consecutive old videos")
			}
			break
		}

		// Set up for next page
		nextPageToken = playlistResponse.NextPageToken
		log.Debug().
			Str("next_page_token", nextPageToken).
			Msg("Continuing to next page of results")
	}

	// Log detailed summary of the entire operation
	log.Info().
		Str("channel_id", channelID).
		Int("video_count", len(videos)).
		Time("from_time", fromTime).
		Time("to_time", effectiveToTime).
		Int("original_limit", limit).
		Int("effective_limit", effectiveLimit).
		Bool("limit_reached", len(videos) >= effectiveLimit).
		Msg("Retrieved videos from YouTube channel")

	if len(videos) == 0 {
		log.Warn().
			Str("channel_id", channelID).
			Time("from_time", fromTime).
			Time("to_time", effectiveToTime).
			Msg("No videos found within the specified time range")
	} else {
		// Log a few sample videos to help with debugging
		sampleSize := min(3, len(videos))
		for i := 0; i < sampleSize; i++ {
			video := videos[i]
			log.Debug().
				Str("sample_video_id", video.ID).
				Str("sample_video_title", video.Title).
				Time("sample_video_published_at", video.PublishedAt).
				Int64("sample_video_views", video.ViewCount).
				Msg(fmt.Sprintf("Sample video %d/%d", i+1, sampleSize))
		}
	}

	return videos, nil
}

// Helper function to get the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// generateRandomPrefix generates a random prefix for YouTube search queries
// Similar to the Python example: watch?v=<random_chars> except removes the trailing - which was effectively increasing prefix length by 1
func (c *YouTubeDataClient) generateRandomPrefix(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789_-"
	watchPrefix := "watch?v="

	c.rngMu.Lock()
	defer c.rngMu.Unlock()

	b := make([]byte, length)
	for i := range b {
		b[i] = charset[c.rng.Intn(len(charset))]
	}

	return watchPrefix + string(b)
}

// YouTubeClientAdapter adapts YouTubeDataClient to the Client interface
type YouTubeClientAdapter struct {
	client *YouTubeDataClient
}

// NewYouTubeClientAdapter creates a new YouTube client adapter
func NewYouTubeClientAdapter(apiKey string) (Client, error) {
	client, err := NewYouTubeDataClient(apiKey)
	if err != nil {
		return nil, err
	}

	return &YouTubeClientAdapter{
		client: client,
	}, nil
}

// Connect establishes a connection to the YouTube API
func (a *YouTubeClientAdapter) Connect(ctx context.Context) error {
	return a.client.Connect(ctx)
}

// Disconnect closes the connection to the YouTube API
func (a *YouTubeClientAdapter) Disconnect(ctx context.Context) error {
	return a.client.Disconnect(ctx)
}

// GetChannelInfo retrieves information about a YouTube channel
func (a *YouTubeClientAdapter) GetChannelInfo(ctx context.Context, channelID string) (Channel, error) {
	log.Info().Str("channel_id", channelID).Msg("GetChannelInfo-YoutubeClientAdaptor: Getting YouTube channel info")
	channelInfo, err := a.client.GetChannelInfo(ctx, channelID)
	if err != nil {
		return nil, err
	}

	// Convert YouTube channel to the common Channel interface
	return &YouTubeChannel{
		ID:          channelInfo.ID,
		Name:        channelInfo.Title,
		Description: channelInfo.Description,
		Thumbnails:  channelInfo.Thumbnails,
		MemberCount: channelInfo.SubscriberCount,
		VideoCount:  channelInfo.VideoCount,
		ViewCount:   channelInfo.ViewCount,
		Country:     channelInfo.Country,
		PublishedAt: channelInfo.PublishedAt,
	}, nil
}

// GetMessages retrieves videos from a YouTube channel (adapting to the Message interface)
func (a *YouTubeClientAdapter) GetMessages(ctx context.Context, channelID string, fromTime, toTime time.Time, limit int) ([]Message, error) {
	videos, err := a.client.GetVideos(ctx, channelID, fromTime, toTime, limit)
	if err != nil {
		return nil, err
	}

	// Convert YouTube videos to the common Message interface
	messages := make([]Message, 0, len(videos))
	for _, video := range videos {
		// Convert reactions (likes) to expected format
		reactions := map[string]int64{
			"like": video.LikeCount,
		}

		message := &YouTubeMessage{
			ID:           video.ID,
			ChannelID:    video.ChannelID,
			SenderID:     video.ChannelID,
			SenderName:   "YouTube Channel",                        // This would ideally be populated with the actual channel name
			Text:         video.Title + "\n\n" + video.Description, // Keep for backward compatibility
			Title:        video.Title,
			Description:  video.Description,
			Timestamp:    video.PublishedAt,
			Views:        video.ViewCount,
			Reactions:    reactions,
			Thumbnails:   video.Thumbnails,
			CommentCount: video.CommentCount,
			Language:     video.Language,
		}

		messages = append(messages, message)
	}

	return messages, nil
}

func (c *YouTubeDataClient) GetVideosByIDs(ctx context.Context, videoIDs []string) ([]*youtubemodel.YouTubeVideo, error) {
	if c.service == nil {
		return nil, fmt.Errorf("YouTube client not connected")
	} else if len(videoIDs) == 0 {
		return nil, fmt.Errorf("No video ids provided")
	}
	videos := make([]*youtubemodel.YouTubeVideo, 0)

	videosCall := c.service.Videos.List([]string{"snippet", "statistics", "contentDetails"}).
		Id(videoIDs...).
		Context(ctx)

	videosResponse, err := videosCall.Do()

	if err != nil {
		log.Error().Err(err).Strs("video_ids", videoIDs).Msg("Failed to get video statistics for video ids")
	} else {
		log.Debug().
			Int("stats_response_items", len(videosResponse.Items)).
			Int("expected_items", len(videoIDs)).
			Msg("Retrieved video statistics for batch")
		for _, videoItem := range videosResponse.Items {
			// Parse published date
			publishedAt, err := time.Parse(time.RFC3339, videoItem.Snippet.PublishedAt)
			if err != nil {
				log.Warn().Err(err).Str("date", videoItem.Snippet.PublishedAt).Msg("Failed to parse video published date")
				continue
			}

			// Extract thumbnails
			thumbnails := make(map[string]string)
			if videoItem.Snippet.Thumbnails != nil {
				if videoItem.Snippet.Thumbnails.Default != nil {
					thumbnails["default"] = videoItem.Snippet.Thumbnails.Default.Url
				}
				if videoItem.Snippet.Thumbnails.Medium != nil {
					thumbnails["medium"] = videoItem.Snippet.Thumbnails.Medium.Url
				}
				if videoItem.Snippet.Thumbnails.High != nil {
					thumbnails["high"] = videoItem.Snippet.Thumbnails.High.Url
				}
				if videoItem.Snippet.Thumbnails.Standard != nil {
					thumbnails["standard"] = videoItem.Snippet.Thumbnails.Standard.Url
				}
				if videoItem.Snippet.Thumbnails.Maxres != nil {
					thumbnails["maxres"] = videoItem.Snippet.Thumbnails.Maxres.Url
				}
			}

			video := &youtubemodel.YouTubeVideo{
				ID:           videoItem.Id,
				ChannelID:    videoItem.Snippet.ChannelId,
				Title:        videoItem.Snippet.Title,
				Description:  videoItem.Snippet.Description,
				PublishedAt:  publishedAt,
				Thumbnails:   thumbnails,
				Language:     "", // Will be populated later from videos API
				ViewCount:    int64(videoItem.Statistics.ViewCount),
				LikeCount:    int64(videoItem.Statistics.LikeCount),
				CommentCount: int64(videoItem.Statistics.CommentCount),
				Duration:     videoItem.ContentDetails.Duration,
			}

			// Get language from snippet if available
			if videoItem.Snippet != nil {
				if videoItem.Snippet.DefaultLanguage != "" {
					video.Language = videoItem.Snippet.DefaultLanguage
				} else if videoItem.Snippet.DefaultAudioLanguage != "" {
					// Fall back to default audio language if default language is not set
					video.Language = videoItem.Snippet.DefaultAudioLanguage
				}
			}
			videos = append(videos, video)
		}
	}
	return videos, nil
}

func ShouldProcessRandomBatch(unverifiedRandomVideosCount int, queuedRandomVideosCount int, videosCount int, effectiveLimit int) bool {
	// 50 max size of API request
	const maxBatchSize = 50
	batchLimitReached := queuedRandomVideosCount >= maxBatchSize
	lastBatchReached := queuedRandomVideosCount+videosCount >= effectiveLimit && queuedRandomVideosCount > 0 &&
		unverifiedRandomVideosCount == 0
	shouldProcessBatch := lastBatchReached || batchLimitReached
	if shouldProcessBatch || rand.Intn(10) == 0 {
		log.Debug().Bool("should_process", shouldProcessBatch).Bool("lastBatchReached", lastBatchReached).
			Bool("batch_limit_reached", batchLimitReached).Int("queued_random_count", queuedRandomVideosCount).
			Int("video_count", videosCount).Int("unverified_random_video_count", unverifiedRandomVideosCount).
			Msg("Batch Process Check")
	}
	return shouldProcessBatch
}

func (c *YouTubeDataClient) ProcessRandomBatch(ctxWithCancel context.Context, videosToQuery []string, resultsChan chan<- []*youtubemodel.YouTubeVideo) {
	retrievedVideos, err := c.GetVideosByIDs(ctxWithCancel, videosToQuery)
	if err != nil {
		log.Warn().
			Err(err).
			Msg("Failed to get random videos")
		log.Warn().Msgf("Failed to query following video ids %v\n", videosToQuery)
		return
	}
	// Send results to collector
	resultsChan <- retrievedVideos
	log.Debug().
		Int("videos_found", len(retrievedVideos)).
		Msg("Sent random videos to result collector")
}

// GetRandomVideos retrieves videos using random sampling with the prefix generator
// Uses parallel processing to handle multiple channels concurrently
func (c *YouTubeDataClient) GetRandomVideos(ctx context.Context, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	if c.service == nil {
		return nil, fmt.Errorf("YouTube client not connected")
	}

	// Use current time as default if toTime is zero
	effectiveToTime := toTime
	if effectiveToTime.IsZero() {
		effectiveToTime = time.Now()
	}

	// Handle negative or zero limit as "no limit"
	effectiveLimit := limit
	if effectiveLimit <= 0 {
		effectiveLimit = 1000
	}
	log.Info().
		Time("from_time", fromTime).
		Time("to_time", toTime).
		Int("limit", limit).
		Int("effective_limit", effectiveLimit).
		Msg("Starting parallel random YouTube video sampling")

	// Define concurrency limits
	const maxWorkers = 5            // Maximum number of parallel workers
	const maxAttempts = 50          // Maximum number of search attempts with random prefixes
	const maxChannelsPerSearch = 10 // Maximum channels to process from each search

	// Shared data with mutex protection
	var mu sync.Mutex
	videos := make([]*youtubemodel.YouTubeVideo, 0, effectiveLimit*2)

	// TODO: package these into a random crawl struct
	processedChannels := make(map[string]bool)
	channelsWithMinVideos := make(map[string]bool)
	unverifiedRandomVideosMap := make(map[string][]string)
	// TODO: replace with queue structure instead if possible
	queuedRandomVideos := make([]string, 0, effectiveLimit*2)
	skipChannels := make(map[string]bool)
	// var prefixMU sync.Mutex
	var prefixMismatchCount int
	var prefixMatchCount int

	prefixMap := make(map[string]RandomPrefix)

	// var verifiedRandomVideos int

	// Channel for discovered channel IDs to process
	channelQueue := make(chan string, 1000) // Buffer to prevent blocking

	// Create a context that can be canceled
	ctxWithCancel, cancel := context.WithCancel(ctx)
	defer cancel()

	// Create control channel to signal workers to stop
	done := make(chan struct{})
	defer close(done)

	// Create a channel for collecting video results
	resultsChan := make(chan []*youtubemodel.YouTubeVideo, maxWorkers)

	// Create WaitGroup for tracking workers
	var wg sync.WaitGroup

	// Create semaphore for limiting concurrent API calls
	sem := make(chan struct{}, maxWorkers)

	// Create a separate goroutine for prefix searching
	go func() {
		defer close(channelQueue) // Close channel when done searching

		// Generate random prefixes and search for videos
		var attempt int
		var stopPrefixSearch bool
		for {
			// Check if context is done
			select {
			case <-ctxWithCancel.Done():
				log.Debug().Msg("Context cancelled, stopping prefix search")
				return
			default:
				// Continue with search
			}

			if stopPrefixSearch {
				for {
					mu.Lock()
					queuedRandomVideosCount := len(queuedRandomVideos)
					unverifiedRandomVideosCount := len(unverifiedRandomVideosMap)
					mu.Unlock()
					if (queuedRandomVideosCount + unverifiedRandomVideosCount) > 0 {
						log.Info().Int("queued_random_videos", queuedRandomVideosCount).Int("unverified_random_videos", unverifiedRandomVideosCount).
							Msg("Waiting for prefix matches to finish processing")
						time.Sleep(100 * time.Millisecond)
					} else {
						log.Info().Int("queued_random_videos", queuedRandomVideosCount).Int("unverified_random_videos", unverifiedRandomVideosCount).
							Msg("Prefix matches completed processing")
						break
					}
				}
				log.Info().Int("total_attempts", attempt).Msg("Completed all random prefix searches")
				return
			}
			// Get unique prefixes using mutex-protected rng
			prefix := c.generateRandomPrefix(5)
			// TODO: get crawl level map for this. only works for same sample run
			if _, exists := prefixMap[prefix]; exists {
				log.Error().Str("duplicated_prefix", prefix).Msg("Duplicate prefix found. Skipping")
				continue
			}

			prefixData := RandomPrefix{
				Prefix:           prefix,
				ValidVideoIDs:    make([]string, 0, 50),
				InvalidVideoIDs:  make([]string, 0, 50),
				ResultCount:      0,
				TotalResultCount: 0,
			}
			// watch?v=id7nj ->id7nj
			videoIdPrefix := strings.ToLower(prefix[8:])
			attempt++
			// Search for videos with this prefix
			searchCall := c.service.Search.List([]string{"id", "snippet"}).
				Q(prefix).
				MaxResults(50). // Max allowed by API
				Context(ctxWithCancel).
				Type("video").
				Order("relevance")
				// Order("date") // Sort by date

			// Add time restrictions if provided
			if !fromTime.IsZero() {
				searchCall = searchCall.PublishedAfter(fromTime.Format(time.RFC3339))
			}
			if !effectiveToTime.IsZero() {
				searchCall = searchCall.PublishedBefore(effectiveToTime.Format(time.RFC3339))
			}

			searchResponse, err := searchCall.Do()
			if err != nil {
				log.Error().Err(err).Str("prefix", prefix).Msg("Failed to search videos with prefix")
				prefixMap[prefix] = prefixData
				continue // Try another prefix
			}
			prefixData.ResultCount = len(searchResponse.Items)
			prefixData.TotalResultCount = searchResponse.PageInfo.TotalResults

			// Process search results - collect unique channel IDs
			channelIDs := make(map[string]bool)
			channelsAdded := 0
			// invalidVideoIDs := make([]string, 0, 50)

			for _, item := range searchResponse.Items {
				channelID := item.Snippet.ChannelId
				videoID := item.Id.VideoId
				if strings.ToLower(videoID[0:5]) != videoIdPrefix {
					prefixMismatchCount++
					prefixData.InvalidVideoIDs = append(prefixData.InvalidVideoIDs, videoID)
					continue
				}
				prefixMatchCount++
				prefixData.ValidVideoIDs = append(prefixData.ValidVideoIDs, videoID)
				mu.Lock()
				alreadyProcessed := processedChannels[channelID]
				if !alreadyProcessed {
					processedChannels[channelID] = true
					channelIDs[channelID] = true
				}

				if channelsWithMinVideos[channelID] {
					log.Info().Str("video_id", videoID).Msg("Adding video id to queuedRandomVideos")
					queuedRandomVideos = append(queuedRandomVideos, videoID)
				} else if skipChannels[channelID] {
					log.Info().Str("video_id", videoID).Msg("Video_id for channel below minumum total video threshold")
				} else {
					log.Info().Str("video_id", videoID).Msg("Adding video id to unverifiedRandomVideosMap")
					unverifiedRandomVideosMap[channelID] = append(unverifiedRandomVideosMap[channelID], videoID)
				}
				mu.Unlock()

				if len(channelIDs) >= maxChannelsPerSearch {
					log.Info().Int("max_channels_per_search", maxChannelsPerSearch).Int("channel_id_count", len(channelIDs)).Msg("Reached maximum number of channel results allowed per search")
					break // Limit channels per search
				}
			}

			// Queue discovered channels for processing
			for channelID := range channelIDs {
				// Check if context is done before each send
				select {
				case <-ctxWithCancel.Done():
					return
				case channelQueue <- channelID:
					channelsAdded++
					log.Debug().
						Str("channel_id", channelID).
						Str("prefix", prefix).
						Msg("Queued channel for processing")
				default:
					// Channel queue is full, wait briefly and try again
					time.Sleep(100 * time.Millisecond)
					select {
					case <-ctxWithCancel.Done():
						return
					case channelQueue <- channelID:
						channelsAdded++
					default:
						log.Warn().
							Str("channel_id", channelID).
							Msg("Channel queue is full, dropping channel")
					}
				}
			}

			log.Debug().Str("prefix", prefix).Int("result_count", prefixData.ResultCount).
				Int64("total_result_count", prefixData.TotalResultCount).Int("prefix_matches", len(prefixData.ValidVideoIDs)).
				Int("prefix_misses", len(prefixData.InvalidVideoIDs)).Msg("Prefix Results")

			if len(prefixData.ValidVideoIDs) > 0 {
				log.Info().Str("prefix", prefix).Strs("valid_video_ids", prefixData.ValidVideoIDs).
					Msg("Valid IDs")
			}
			if len(prefixData.InvalidVideoIDs) > 0 {
				log.Warn().Str("prefix", prefix).Strs("invalid_video_ids", prefixData.InvalidVideoIDs).
					Msg("Invalid IDs")
			}
			if attempt%20 == 0 {
				log.Info().Int("total_attempts", attempt).Int("prefix_matches", prefixMatchCount).
					Int("prefix_misses", prefixMismatchCount).
					Msg("Prefix Update")
			}
			// Check if we should continue searching
			mu.Lock()
			prefixMap[prefix] = prefixData
			videoCount := len(videos)
			queuedRandomVideosCount := len(queuedRandomVideos)
			mu.Unlock()
			validPrefixesIdentified := videoCount + queuedRandomVideosCount

			if validPrefixesIdentified >= effectiveLimit {
				log.Debug().
					Int("videos_processed", videoCount).
					Int("queued_random_videos", queuedRandomVideosCount).
					Int("valid_prefixes_identified", validPrefixesIdentified).
					Int("limit", effectiveLimit).
					Msg("Reached video limit, stopping prefix search")
				stopPrefixSearch = true
			}
		}
	}()

	// Worker function for processing channels
	channelWorker := func(workerID int) {
		defer wg.Done()

		for {
			select {
			case <-ctxWithCancel.Done():
				log.Debug().Int("worker_id", workerID).Msg("Worker stopping due to context cancellation")
				return
			case <-done:
				log.Debug().Int("worker_id", workerID).Msg("Worker stopping due to done signal")
				return
			case channelID, ok := <-channelQueue:
				if !ok {
					log.Debug().Int("worker_id", workerID).Msg("Channel queue closed, worker stopping")
					return
				}

				// Acquire semaphore slot
				sem <- struct{}{}

				log.Debug().Int("worker_id", workerID).Str("channel_id", channelID).
					Msg("Worker processing channel")

				// Check if this channel has more than minimum required videos
				channel, err := c.GetChannelInfo(ctxWithCancel, channelID)
				if err != nil {
					log.Warn().Err(err).Int("worker_id", workerID).Str("channel_id", channelID).
						Msg("Failed to get channel info")
					mu.Lock()
					skipChannels[channelID] = true
					delete(unverifiedRandomVideosMap, channelID)
					mu.Unlock()
					<-sem // Release semaphore
					continue
				}

				// Only include channels with > minimum videos
				mu.Lock()
				if channel.VideoCount > 10 {
					log.Info().Str("channel_id", channelID).Msg("Adding channel id to queuedRandomVideos")
					channelsWithMinVideos[channelID] = true
					// move items from unverified to queued
					queuedRandomVideos = append(queuedRandomVideos, unverifiedRandomVideosMap[channelID]...)
					delete(unverifiedRandomVideosMap, channelID)
				} else {
					skipChannels[channelID] = true
					delete(unverifiedRandomVideosMap, channelID)
					log.Debug().
						Int("worker_id", workerID).
						Str("channel_id", channelID).
						Msg("Skipping channel. Does not meet minimum required video count")
				}
				mu.Unlock()
				// Release semaphore
				<-sem
			}
		}
	}

	// Start worker pool
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go channelWorker(i)
	}

	// Collector goroutine to process results
	go func() {
		for randomVideos := range resultsChan {
			mu.Lock()

			// Check if we've reached the limit
			if len(videos) >= effectiveLimit && (len(queuedRandomVideos)+len(unverifiedRandomVideosMap)) == 0 {
				mu.Unlock()
				continue // Skip but keep collecting to prevent blocking
			}

			toAdd := len(randomVideos)
			videos = append(videos, randomVideos...)

			log.Debug().Int("videos_added", toAdd).Int("total_videos", len(videos)).
				Int("effective_limit", effectiveLimit).Msg("Added videos from channel result")

			mu.Unlock()
		}
	}()

	// Monitor the system and check for completion
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	// Main control loop
	for {
		select {
		case <-ctxWithCancel.Done():
			log.Debug().Msg("Context cancelled, stopping all processing")
			goto Wait
		case <-ticker.C:
			// Check if we've reached the limit
			mu.Lock()
			videosCount := len(videos)
			channelsCount := len(processedChannels)
			qualifiedChannelsCount := len(channelsWithMinVideos)
			queuedRandomVideosCount := len(queuedRandomVideos)
			unverifiedRandomVideosCount := len(unverifiedRandomVideosMap)
			mu.Unlock()
			if ShouldProcessRandomBatch(unverifiedRandomVideosCount, queuedRandomVideosCount, videosCount, effectiveLimit) {
				mu.Lock()
				toAdd := min(50, len(queuedRandomVideos))
				videosToQuery := queuedRandomVideos[:toAdd]
				queuedRandomVideos = queuedRandomVideos[toAdd:]
				newQueuedCount := len(queuedRandomVideos)
				mu.Unlock()
				log.Info().Int("num_videos_to_process", toAdd).Int("updated_queued_random_videos", newQueuedCount).Msg("Processing batch")
				c.ProcessRandomBatch(ctxWithCancel, videosToQuery, resultsChan)
			} else if videosCount >= effectiveLimit && (queuedRandomVideosCount+unverifiedRandomVideosCount) == 0 {
				log.Debug().Int("videos_count", videosCount).Msg("Video limit reached in main loop, stopping processing")
				cancel() // Cancel context to signal workers to stop
				goto Wait
			}

			// Check if channel queue is closed and empty, and all workers are idle (semaphore empty)
			if channelQueue == nil || (len(channelQueue) == 0 && len(sem) == 0) {
				select {
				case _, ok := <-channelQueue:
					if !ok {
						log.Debug().Msg("Channel queue is closed and empty, stopping")
						goto Wait
					}
					// Put it back if we took one
					if ok {
						channelQueue <- ""
					}
				default:
					// Queue is not closed but might be empty
					// Continue and check again on next tick
				}
			}

			log.Debug().
				Int("videos_collected", videosCount).
				Int("videos_queued", len(queuedRandomVideos)).
				Int("channels_processed", channelsCount).
				Int("qualified_channels", qualifiedChannelsCount).
				Int("channel_queue_size", len(channelQueue)).
				Int("busy_workers", len(sem)).
				Msg("Progress update")
		}
	}

Wait:
	// Wait for all workers to finish
	wg.Wait()
	close(resultsChan)

	for _, prefixObj := range prefixMap {
		log.Info().Str("prefix", prefixObj.Prefix).Strs("invalid_video_ids", prefixObj.InvalidVideoIDs).
			Strs("valid_video_ids", prefixObj.ValidVideoIDs).Int("result_count", prefixObj.ResultCount).
			Int64("total_result_count", prefixObj.TotalResultCount).Msg("SavePrefix")
	}

	mu.Lock()
	defer mu.Unlock()

	log.Info().
		Int("prefix_attempts", len(prefixMap)).
		Int("final_video_count", len(videos)).
		Int("channels_processed", len(processedChannels)).
		Int("qualified_channels", len(channelsWithMinVideos)).
		Msg("Completed parallel random YouTube video sampling")

	return videos, nil
}

// GetSnowballVideos retrieves videos using snowball sampling from channels with > 10 videos
// Uses parallel processing to handle multiple channels concurrently
func (c *YouTubeDataClient) GetSnowballVideos(ctx context.Context, seedChannelIDs []string, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	if c.service == nil {
		return nil, fmt.Errorf("YouTube client not connected")
	}

	if len(seedChannelIDs) == 0 {
		return nil, fmt.Errorf("at least one seed channel ID is required for snowball sampling")
	}

	log.Info().
		Strs("seed_channels", seedChannelIDs).
		Time("from_time", fromTime).
		Time("to_time", toTime).
		Int("limit", limit).
		Msg("Starting parallel snowball YouTube sampling")

	// Use current time as default if toTime is zero
	effectiveToTime := toTime
	if effectiveToTime.IsZero() {
		effectiveToTime = time.Now()
	}

	// Handle negative or zero limit as "no limit"
	effectiveLimit := limit
	if effectiveLimit <= 0 {
		effectiveLimit = 1000
	}

	// Define concurrency limit - adjust based on API quota
	const maxWorkers = 5 // Maximum number of parallel workers

	// Shared data with mutex protection
	var mu sync.Mutex
	videos := make([]*youtubemodel.YouTubeVideo, 0, effectiveLimit)
	processedChannels := make(map[string]bool)
	channelsWithMinVideos := make(map[string]bool)

	// Channel for new channel IDs discovered during processing
	newChannelsQueue := make(chan string, 1000) // Buffer to prevent blocking

	// Initialize with seed channels
	for _, channelID := range seedChannelIDs {
		if !processedChannels[channelID] {
			newChannelsQueue <- channelID
			processedChannels[channelID] = true // Mark as queued
		}
	}

	// Create a WaitGroup to track active workers
	var wg sync.WaitGroup

	// Create a context that can be canceled
	ctxWithCancel, cancel := context.WithCancel(ctx)
	defer cancel()

	// Create control channel to signal workers to stop
	done := make(chan struct{})
	defer close(done)

	// Create a channel for collecting results
	resultsChan := make(chan []*youtubemodel.YouTubeVideo, maxWorkers)

	// Launch workers
	activeWorkers := 0

	// Track active channels being processed
	activeChannels := make(map[string]bool)
	var activeChannelsMu sync.Mutex

	// Create semaphore for limiting concurrent API calls
	sem := make(chan struct{}, maxWorkers)

	log.Info().Int("max_workers", maxWorkers).Msg("Starting worker pool for channel processing")

	// Worker function
	worker := func(workerID int) {
		defer wg.Done()

		for {
			select {
			case <-ctxWithCancel.Done():
				log.Debug().Int("worker_id", workerID).Msg("Worker stopping due to context cancellation")
				return
			case <-done:
				log.Debug().Int("worker_id", workerID).Msg("Worker stopping due to done signal")
				return
			case channelID, ok := <-newChannelsQueue:
				if !ok {
					log.Debug().Int("worker_id", workerID).Msg("Channel queue closed, worker stopping")
					return
				}

				// Mark this channel as being processed
				activeChannelsMu.Lock()
				activeChannels[channelID] = true
				activeChannelsMu.Unlock()

				// Acquire semaphore slot
				sem <- struct{}{}

				log.Debug().
					Int("worker_id", workerID).
					Str("channel_id", channelID).
					Msg("Worker processing channel")

				// Check if this channel has more than minimum required videos
				channel, err := c.GetChannelInfo(ctxWithCancel, channelID)
				if err != nil {
					log.Warn().
						Err(err).
						Int("worker_id", workerID).
						Str("channel_id", channelID).
						Msg("Failed to get channel info")

					// Release semaphore and mark as done
					<-sem
					activeChannelsMu.Lock()
					delete(activeChannels, channelID)
					activeChannelsMu.Unlock()
					continue
				}

				// Only include channels with > minimum videos
				if channel.VideoCount > 10 {
					mu.Lock()
					channelsWithMinVideos[channelID] = true
					mu.Unlock()

					log.Debug().
						Int("worker_id", workerID).
						Str("channel_id", channelID).
						Str("channel_title", channel.Title).
						Int64("video_count", channel.VideoCount).
						Msg("Processing channel with sufficient videos")

					// Get videos from this channel
					channelVideos, err := c.GetVideosFromChannel(ctxWithCancel, channelID, fromTime, effectiveToTime, 50)
					if err != nil {
						log.Warn().
							Err(err).
							Int("worker_id", workerID).
							Str("channel_id", channelID).
							Msg("Failed to get videos from channel")

						// Release semaphore and mark as done
						<-sem
						activeChannelsMu.Lock()
						delete(activeChannels, channelID)
						activeChannelsMu.Unlock()
						continue
					}

					// Send channel videos to result channel
					resultsChan <- channelVideos

					// Find related channels from video descriptions
					var newChannels []string
					for _, video := range channelVideos {
						// Extract mentioned channel IDs from the description
						mentionedChannels := extractChannelIDsFromText(video.Description)

						mu.Lock()
						for _, mentionedChannelID := range mentionedChannels {
							// Check if already processed or queued
							if !processedChannels[mentionedChannelID] {
								processedChannels[mentionedChannelID] = true // Mark as queued
								newChannels = append(newChannels, mentionedChannelID)

								log.Debug().
									Int("worker_id", workerID).
									Str("source_channel", channelID).
									Str("mentioned_channel", mentionedChannelID).
									Msg("Found new channel to process")
							}
						}
						mu.Unlock()
					}

					// Queue the newly discovered channels
					for _, newChannel := range newChannels {
						select {
						case newChannelsQueue <- newChannel:
							// Successfully queued
						default:
							// Queue is full, try once more and log if still full
							select {
							case newChannelsQueue <- newChannel:
								// Successfully queued on retry
							default:
								log.Warn().
									Int("worker_id", workerID).
									Str("channel_id", newChannel).
									Msg("Channel queue is full, dropping channel")
							}
						}
					}
				}

				// Release semaphore and mark as done with this channel
				<-sem
				activeChannelsMu.Lock()
				delete(activeChannels, channelID)
				activeChannelsMu.Unlock()
			}
		}
	}

	// Start initial worker pool
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		activeWorkers++
		go worker(i)
	}

	// Collector goroutine to process results
	go func() {
		for result := range resultsChan {
			mu.Lock()

			// Check if we've reached the limit
			if len(videos) >= effectiveLimit {
				mu.Unlock()
				continue // Skip but keep collecting to prevent blocking
			}

			// Add videos up to the limit
			remaining := effectiveLimit - len(videos)
			if remaining > 0 {
				toAdd := min(remaining, len(result))
				videos = append(videos, result[:toAdd]...)

				log.Debug().
					Int("videos_added", toAdd).
					Int("total_videos", len(videos)).
					Int("effective_limit", effectiveLimit).
					Msg("Added videos from channel result")

				// If we've reached the limit, cancel context to signal workers to stop
				if len(videos) >= effectiveLimit {
					log.Info().Int("total_videos", len(videos)).Msg("Reached video limit, cancelling remaining work")
					cancel()
				}
			}
			mu.Unlock()
		}
	}()

	// Monitor the system and check for completion
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	// Main control loop
	for {
		select {
		case <-ctxWithCancel.Done():
			log.Debug().Msg("Context cancelled, stopping all processing")
			close(newChannelsQueue) // Signal workers to stop
			goto Wait
		case <-ticker.C:
			// Check if we've reached the limit
			mu.Lock()
			videosCount := len(videos)
			mu.Unlock()

			if videosCount >= effectiveLimit {
				log.Debug().Int("videos_count", videosCount).Msg("Video limit reached in main loop, stopping processing")
				cancel() // Cancel context to signal workers to stop
				goto Wait
			}

			// Check if there's more work to do
			activeChannelsMu.Lock()
			activeChannelCount := len(activeChannels)
			activeChannelsMu.Unlock()

			// Check if channel queue is empty and no active processing
			if len(newChannelsQueue) == 0 && activeChannelCount == 0 {
				log.Debug().Msg("No more channels to process and no active processing, stopping")
				goto Wait
			}
		}
	}

Wait:
	// Wait for all workers to finish
	wg.Wait()
	close(resultsChan)

	mu.Lock()
	defer mu.Unlock()

	// Ensure we don't exceed the limit
	if len(videos) > effectiveLimit {
		videos = videos[:effectiveLimit]
	}

	log.Info().
		Int("final_video_count", len(videos)).
		Int("channels_processed", len(processedChannels)).
		Int("qualified_channels", len(channelsWithMinVideos)).
		Msg("Completed parallel snowball YouTube sampling")

	return videos, nil
}

// extractChannelIDsFromText extracts potential YouTube channel IDs from text
// This is a simplified implementation that looks for patterns like:
// - "youtube.com/channel/UC..."
// - "youtube.com/@..."
func extractChannelIDsFromText(text string) []string {
	channelIDs := make([]string, 0)

	// Look for standard channel IDs (UCxxxx)
	ucPattern := regexp.MustCompile(`youtube\.com/channel/([a-zA-Z0-9_-]+)`)
	ucMatches := ucPattern.FindAllStringSubmatch(text, -1)
	for _, match := range ucMatches {
		if len(match) > 1 {
			channelIDs = append(channelIDs, match[1])
		}
	}

	// Look for handle-based channels (@xxxx)
	handlePattern := regexp.MustCompile(`youtube\.com/@([a-zA-Z0-9_.-]+)`)
	handleMatches := handlePattern.FindAllStringSubmatch(text, -1)
	for _, match := range handleMatches {
		if len(match) > 1 {
			// Add @ prefix to indicate it's a handle
			channelIDs = append(channelIDs, "@"+match[1])
		}
	}

	return channelIDs
}

// getDynamicBatchSize returns optimal batch size based on current progress
func (c *YouTubeDataClient) getDynamicBatchSize(currentVideos, targetLimit int) int {
	remaining := targetLimit - currentVideos

	// If we're close to the limit, use smaller batches
	if remaining <= c.batchConfig.MinBatchSize {
		return c.batchConfig.MinBatchSize
	}

	// Use optimal batch size for normal operation
	if remaining >= c.batchConfig.OptimalBatchSize {
		return c.batchConfig.OptimalBatchSize
	}

	// Use remaining count if it's between min and optimal
	return remaining
}

// getCachedVideoStats checks if video statistics are already cached
func (c *YouTubeDataClient) getCachedVideoStats(videoID string) (*youtubemodel.YouTubeVideo, bool) {
	c.cacheMutex.RLock()
	defer c.cacheMutex.RUnlock()
	video, exists := c.videoStatsCache[videoID]
	return video, exists
}

// cacheVideoStats stores video statistics in cache
func (c *YouTubeDataClient) cacheVideoStats(video *youtubemodel.YouTubeVideo) {
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()
	c.videoStatsCache[video.ID] = video
}

// GetChannelType returns "youtube" as the channel type
func (a *YouTubeClientAdapter) GetChannelType() string {
	return "youtube"
}

// GetRandomVideos retrieves videos using random sampling - delegates to the wrapped YouTubeDataClient
func (a *YouTubeClientAdapter) GetRandomVideos(ctx context.Context, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	return a.client.GetRandomVideos(ctx, fromTime, toTime, limit)
}

// GetVideosByIDs retrieves videos by videoIDs - delegates to the wrapped YouTubeDataClient
func (a *YouTubeClientAdapter) GetVideosByIDs(ctx context.Context, videoIDs []string) ([]*youtubemodel.YouTubeVideo, error) {
	return a.client.GetVideosByIDs(ctx, videoIDs)
}

// GetSnowballVideos retrieves videos using snowball sampling - delegates to the wrapped YouTubeDataClient
func (a *YouTubeClientAdapter) GetSnowballVideos(ctx context.Context, seedChannelIDs []string, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	return a.client.GetSnowballVideos(ctx, seedChannelIDs, fromTime, toTime, limit)
}

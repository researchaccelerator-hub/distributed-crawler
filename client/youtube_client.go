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

// YouTubeDataClient implements the youtubemodel.YouTubeClient interface for accessing YouTube Data API
type YouTubeDataClient struct {
	service *ytapi.Service
	apiKey  string

	// Caches to minimize API calls
	channelCache         map[string]*youtubemodel.YouTubeChannel
	uploadsPlaylistCache map[string]string // Maps channelID to uploadsPlaylistID
	
	// RNG instance for generating random values
	rng *rand.Rand
	rngMu sync.Mutex
}

// NewYouTubeDataClient creates a new YouTube data client
func NewYouTubeDataClient(apiKey string) (*YouTubeDataClient, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("YouTube API key is required")
	}

	// Create a new random number generator seeded with current time
	source := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(source)

	return &YouTubeDataClient{
		apiKey:               apiKey,
		channelCache:         make(map[string]*youtubemodel.YouTubeChannel),
		uploadsPlaylistCache: make(map[string]string),
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
	if c.service == nil {
		return nil, fmt.Errorf("YouTube client not connected")
	}

	// Check cache first
	if cachedChannel, exists := c.channelCache[channelID]; exists {
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
		SubscriberCount: subscriberCount,
		ViewCount:       viewCount,
		VideoCount:      videoCount,
		PublishedAt:     publishedAt,
		Thumbnails:      thumbnails,
		Country:         item.Snippet.Country, // Add country information
	}

	// Store in cache
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

	log.Info().
		Str("channel_id", channel.ID).
		Str("title", channel.Title).
		Int64("subscribers", channel.SubscriberCount).
		Int64("view_count", channel.ViewCount).
		Int64("video_count", channel.VideoCount).
		Str("country", channel.Country).
		Msg("YouTube channel info retrieved - all engagement data populated")

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

	if uploadsPlaylistID, exists = c.uploadsPlaylistCache[channelID]; exists {
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

		// Store in cache
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

		log.Debug().
			Str("channel_id", channelID).
			Str("uploads_playlist_id", uploadsPlaylistID).
			Msg("Retrieved and cached uploads playlist ID for channel")
	}

	// Now get videos from this playlist
	videos := make([]*youtubemodel.YouTubeVideo, 0)
	var nextPageToken string
	pageCounter := 0

	log.Debug().
		Str("channel_id", channelID).
		Str("uploads_playlist_id", uploadsPlaylistID).
		Int("original_limit", limit).
		Int("effective_limit", effectiveLimit).
		Msg("Starting video retrieval from uploads playlist")

	for len(videos) < effectiveLimit {
		pageCounter++
		maxResultsPerPage := min(50, effectiveLimit-len(videos))

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
		videoIDs := make([]string, 0, len(playlistResponse.Items))
		videoMap := make(map[string]*youtubemodel.YouTubeVideo)

		log.Debug().
			Int("processing_items_count", len(playlistResponse.Items)).
			Str("playlist_id", uploadsPlaylistID).
			Msg("Processing playlist items")

		itemsSkippedBeforeFromTime := 0
		itemsSkippedAfterToTime := 0
		itemsSkippedParseError := 0
		itemsAccepted := 0

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

			// Skip videos outside of the time range
			if !fromTime.IsZero() && publishedAt.Before(fromTime) {
				log.Debug().
					Time("published_at", publishedAt).
					Time("from_time", fromTime).
					Str("video_id", videoID).
					Msg("Skipping video: published before fromTime")
				itemsSkippedBeforeFromTime++
				continue
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

			// Extract video ID
			videoIDs = append(videoIDs, videoID)

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

			videoMap[videoID] = video
			itemsAccepted++
		}

		log.Info().
			Int("total_items", len(playlistResponse.Items)).
			Int("items_accepted", itemsAccepted).
			Int("items_skipped_before_from_time", itemsSkippedBeforeFromTime).
			Int("items_skipped_after_to_time", itemsSkippedAfterToTime).
			Int("items_skipped_parse_error", itemsSkippedParseError).
			Msg("Processed playlist items")

		// If we have videos to process, fetch their statistics in batches
		if len(videoIDs) > 0 {
			log.Debug().
				Int("video_ids_count", len(videoIDs)).
				Strs("video_ids", videoIDs).
				Msg("Fetching statistics for videos")

			// Get statistics for these videos in a single call
			videosCall := c.service.Videos.List([]string{"snippet", "statistics", "contentDetails"}).
				Id(videoIDs...).
				Context(ctx)

			videosResponse, err := videosCall.Do()
			if err != nil {
				log.Error().Err(err).Strs("video_ids", videoIDs).Msg("Failed to get video statistics")
				// Continue with basic information only

				// Even without statistics, add the videos to the results to avoid losing them
				for _, video := range videoMap {
					videos = append(videos, video)
				}

				log.Warn().
					Int("videos_added_without_stats", len(videoMap)).
					Msg("Added videos without statistics due to API error")
			} else {
				log.Debug().
					Int("stats_response_items", len(videosResponse.Items)).
					Int("expected_items", len(videoIDs)).
					Msg("Retrieved video statistics")

				// Track stats retrieval success rate
				statsFound := 0

				// Update videos with statistics
				for _, videoItem := range videosResponse.Items {
					if video, ok := videoMap[videoItem.Id]; ok {
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
							Msg("Received statistics for video not in our map")
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
					Int("total_videos_processed", len(videoMap)).
					Int("stats_found", statsFound).
					Int("stats_missing", len(videoMap)-statsFound).
					Int("videos_with_zero_comments", videosWithZeroComments).
					Float64("zero_comments_percentage", float64(videosWithZeroComments)/float64(len(videosResponse.Items))*100).
					Msg("Processed video statistics")

				// Check for videos that didn't get statistics
				if statsFound < len(videoMap) {
					missingStats := []string{}
					for videoID, video := range videoMap {
						// Check if this video was not processed in the stats response
						found := false
						for _, processedVideo := range videos {
							if processedVideo.ID == videoID {
								found = true
								break
							}
						}

						if !found {
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
			}
		} else {
			log.Debug().Msg("No videos to process after time filtering")
		}

		// Check if we've reached the limit or no more pages
		if len(videos) >= effectiveLimit || playlistResponse.NextPageToken == "" {
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
// Similar to the Python example: watch?v=<random_chars>-
func (c *YouTubeDataClient) generateRandomPrefix(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	watchPrefix := "watch?v="
	
	c.rngMu.Lock()
	defer c.rngMu.Unlock()
	
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[c.rng.Intn(len(charset))]
	}
	
	return watchPrefix + string(b) + "-"
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
	channelInfo, err := a.client.GetChannelInfo(ctx, channelID)
	if err != nil {
		return nil, err
	}

	// Convert YouTube channel to the common Channel interface
	return &YouTubeChannel{
		ID:          channelInfo.ID,
		Name:        channelInfo.Title,
		Description: channelInfo.Description,
		MemberCount: channelInfo.SubscriberCount,
		Country:     channelInfo.Country,
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
			SenderName:   "YouTube Channel", // This would ideally be populated with the actual channel name
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

// GetRandomVideos retrieves videos using random sampling with the prefix generator
// Uses parallel processing to handle multiple channels concurrently
func (c *YouTubeDataClient) GetRandomVideos(ctx context.Context, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	if c.service == nil {
		return nil, fmt.Errorf("YouTube client not connected")
	}

	log.Info().
		Time("from_time", fromTime).
		Time("to_time", toTime).
		Int("limit", limit).
		Msg("Starting parallel random YouTube video sampling")

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

	// Define concurrency limits
	const maxWorkers = 5 // Maximum number of parallel workers
	const maxAttempts = 50 // Maximum number of search attempts with random prefixes
	const maxChannelsPerSearch = 10 // Maximum channels to process from each search

	// Shared data with mutex protection
	var mu sync.Mutex
	videos := make([]*youtubemodel.YouTubeVideo, 0, effectiveLimit)
	processedChannels := make(map[string]bool)
	channelsWithMinVideos := make(map[string]bool)

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
		for attempt := 0; attempt < maxAttempts; attempt++ {
			// Check if context is done
			select {
			case <-ctxWithCancel.Done():
				log.Debug().Msg("Context cancelled, stopping prefix search")
				return
			default:
				// Continue with search
			}

			// Get unique prefixes using mutex-protected rng
			prefix := c.generateRandomPrefix(5)

			log.Debug().
				Str("prefix", prefix).
				Int("attempt", attempt+1).
				Int("max_attempts", maxAttempts).
				Msg("Searching with random prefix")

			// Search for videos with this prefix
			searchCall := c.service.Search.List([]string{"id", "snippet"}).
				Q(prefix).
				MaxResults(50). // Max allowed by API
				Context(ctxWithCancel).
				Type("video").
				Order("date") // Sort by date

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
				continue // Try another prefix
			}

			log.Debug().
				Str("prefix", prefix).
				Int("results", len(searchResponse.Items)).
				Msg("Search results received")

			// Process search results - collect unique channel IDs
			channelIDs := make(map[string]bool)
			channelsAdded := 0

			for _, item := range searchResponse.Items {
				channelID := item.Snippet.ChannelId

				mu.Lock()
				alreadyProcessed := processedChannels[channelID]
				if !alreadyProcessed {
					processedChannels[channelID] = true
					channelIDs[channelID] = true
				}
				mu.Unlock()

				if len(channelIDs) >= maxChannelsPerSearch {
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

			log.Info().
				Str("prefix", prefix).
				Int("channels_added", channelsAdded).
				Msg("Completed processing search results")

			// Check if we should continue searching
			mu.Lock()
			videoCount := len(videos)
			mu.Unlock()

			if videoCount >= effectiveLimit {
				log.Debug().
					Int("videos_collected", videoCount).
					Int("limit", effectiveLimit).
					Msg("Reached video limit, stopping prefix search")
				return
			}
		}

		log.Info().Int("total_attempts", maxAttempts).Msg("Completed all random prefix searches")
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
					<-sem // Release semaphore
					continue
				}

				// Only include channels with > minimum videos
				if channel.VideoCount > 10 {
					mu.Lock()
					channelsWithMinVideos[channelID] = true
					mu.Unlock()

					// Get videos from this channel
					channelVideos, err := c.GetVideosFromChannel(ctxWithCancel, channelID, fromTime, effectiveToTime, 50)
					if err != nil {
						log.Warn().
							Err(err).
							Int("worker_id", workerID).
							Str("channel_id", channelID).
							Msg("Failed to get videos from channel")
						<-sem // Release semaphore
						continue
					}

					// Send results to collector
					resultsChan <- channelVideos

					log.Debug().
						Int("worker_id", workerID).
						Str("channel_id", channelID).
						Str("channel_title", channel.Title).
						Int("videos_found", len(channelVideos)).
						Msg("Sent channel videos to result collector")
				}

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
		for channelVideos := range resultsChan {
			mu.Lock()

			// Check if we've reached the limit
			if len(videos) >= effectiveLimit {
				mu.Unlock()
				continue // Skip but keep collecting to prevent blocking
			}

			// Add videos up to the limit
			remaining := effectiveLimit - len(videos)
			if remaining > 0 {
				toAdd := min(remaining, len(channelVideos))
				videos = append(videos, channelVideos[:toAdd]...)

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
			goto Wait
		case <-ticker.C:
			// Check if we've reached the limit
			mu.Lock()
			videosCount := len(videos)
			channelsMu := len(processedChannels)
			qualifiedMu := len(channelsWithMinVideos)
			mu.Unlock()

			if videosCount >= effectiveLimit {
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
				Int("channels_processed", channelsMu).
				Int("qualified_channels", qualifiedMu).
				Int("channel_queue_size", len(channelQueue)).
				Int("busy_workers", len(sem)).
				Msg("Progress update")
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

// GetChannelType returns "youtube" as the channel type
func (a *YouTubeClientAdapter) GetChannelType() string {
	return "youtube"
}

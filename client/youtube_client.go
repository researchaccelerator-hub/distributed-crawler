package client

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/crawler/youtube"
	"github.com/rs/zerolog/log"
	"google.golang.org/api/option"
	ytapi "google.golang.org/api/youtube/v3"
)

// YouTubeDataClient implements the youtube.YouTubeClient interface for accessing YouTube Data API
type YouTubeDataClient struct {
	service *ytapi.Service
	apiKey  string
}

// NewYouTubeDataClient creates a new YouTube data client
func NewYouTubeDataClient(apiKey string) (*YouTubeDataClient, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("YouTube API key is required")
	}

	return &YouTubeDataClient{
		apiKey: apiKey,
	}, nil
}

// Connect establishes a connection to the YouTube API
func (c *YouTubeDataClient) Connect(ctx context.Context) error {
	log.Info().Msg("Connecting to YouTube API")

	// Create a new HTTP client with default timeout
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Create YouTube service
	service, err := ytapi.NewService(ctx, option.WithAPIKey(c.apiKey), option.WithHTTPClient(httpClient))
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
func (c *YouTubeDataClient) GetChannelInfo(ctx context.Context, channelID string) (*youtube.YouTubeChannel, error) {
	if c.service == nil {
		return nil, fmt.Errorf("YouTube client not connected")
	}

	log.Info().Str("channel_id", channelID).Msg("Fetching YouTube channel info")

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

	// Create and return the YouTube channel object
	channel := &youtube.YouTubeChannel{
		ID:              item.Id,
		Title:           item.Snippet.Title,
		Description:     item.Snippet.Description,
		SubscriberCount: subscriberCount,
		ViewCount:       viewCount,
		VideoCount:      videoCount,
		PublishedAt:     publishedAt,
		Thumbnails:      thumbnails,
	}

	log.Info().
		Str("channel_id", channel.ID).
		Str("title", channel.Title).
		Int64("subscribers", channel.SubscriberCount).
		Int64("view_count", channel.ViewCount).
		Int64("video_count", channel.VideoCount).
		Msg("YouTube channel info retrieved")

	return channel, nil
}

// GetVideos retrieves videos from a YouTube channel
func (c *YouTubeDataClient) GetVideos(ctx context.Context, channelID string, fromTime, toTime time.Time, limit int) ([]*youtube.YouTubeVideo, error) {
	if c.service == nil {
		return nil, fmt.Errorf("YouTube client not connected")
	}

	log.Info().
		Str("channel_id", channelID).
		Time("from_time", fromTime).
		Time("to_time", toTime).
		Int("limit", limit).
		Msg("Fetching videos from YouTube channel")

	// First, get the uploads playlist ID for this channel
	var part = []string{"contentDetails"}
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

	// Get the uploads playlist ID
	uploadsPlaylistID := response.Items[0].ContentDetails.RelatedPlaylists.Uploads

	// Now get videos from this playlist
	videos := make([]*youtube.YouTubeVideo, 0)
	var nextPageToken string
	for len(videos) < limit {
		// Fetch playlist items (videos)
		playlistCall := c.service.PlaylistItems.List([]string{"snippet", "contentDetails"}).
			PlaylistId(uploadsPlaylistID).
			MaxResults(int64(min(50, limit-len(videos)))).
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
			break
		}

		// Process each video in the playlist
		videoIDs := make([]string, 0, len(playlistResponse.Items))
		videoMap := make(map[string]*youtube.YouTubeVideo)

		for _, item := range playlistResponse.Items {
			// Parse published date
			publishedAt, err := time.Parse(time.RFC3339, item.Snippet.PublishedAt)
			if err != nil {
				log.Warn().Err(err).Str("date", item.Snippet.PublishedAt).Msg("Failed to parse video published date")
				continue
			}

			// Skip videos outside of the time range
			if !fromTime.IsZero() && publishedAt.Before(fromTime) {
				continue
			}

			if !toTime.IsZero() && publishedAt.After(toTime) {
				continue
			}

			// Extract video ID
			videoID := item.ContentDetails.VideoId
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

			// Create basic video object (without statistics yet)
			video := &youtube.YouTubeVideo{
				ID:          videoID,
				ChannelID:   channelID,
				Title:       item.Snippet.Title,
				Description: item.Snippet.Description,
				PublishedAt: publishedAt,
				Thumbnails:  thumbnails,
			}

			videoMap[videoID] = video
		}

		// If we have videos to process, fetch their statistics in batches
		if len(videoIDs) > 0 {
			// Get statistics for these videos in a single call
			videosCall := c.service.Videos.List([]string{"statistics", "contentDetails"}).
				Id(videoIDs...).
				Context(ctx)

			videosResponse, err := videosCall.Do()
			if err != nil {
				log.Error().Err(err).Strs("video_ids", videoIDs).Msg("Failed to get video statistics")
				// Continue with basic information only
			} else {
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

						// Add to results
						videos = append(videos, video)
					}
				}
			}
		}

		// Check if we've reached the limit or no more pages
		if len(videos) >= limit || playlistResponse.NextPageToken == "" {
			break
		}

		// Set up for next page
		nextPageToken = playlistResponse.NextPageToken
	}

	log.Info().
		Str("channel_id", channelID).
		Int("video_count", len(videos)).
		Msg("Retrieved videos from YouTube channel")

	return videos, nil
}

// Helper function to get the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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
			ID:         video.ID,
			ChannelID:  video.ChannelID,
			SenderID:   video.ChannelID,
			SenderName: "YouTube Channel", // This would ideally be populated with the actual channel name
			Text:       video.Title + "\n\n" + video.Description,
			Timestamp:  video.PublishedAt,
			Views:      video.ViewCount,
			Reactions:  reactions,
		}
		
		messages = append(messages, message)
	}
	
	return messages, nil
}

// GetChannelType returns "youtube" as the channel type
func (a *YouTubeClientAdapter) GetChannelType() string {
	return "youtube"
}
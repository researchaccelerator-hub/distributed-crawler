// Package youtube provides adapter types for YouTube clients
package youtube

import (
	"context"
	"fmt"
	"time"

	clientpkg "github.com/researchaccelerator-hub/telegram-scraper/client"
	youtubemodel "github.com/researchaccelerator-hub/telegram-scraper/model/youtube"
)

// ClientAdapter adapts a client.Client to the YouTubeClient interface
type ClientAdapter struct {
	client clientpkg.Client
}

// NewClientAdapter creates a new adapter for the provided client
func NewClientAdapter(client clientpkg.Client) (*ClientAdapter, error) {
	if client == nil {
		return nil, fmt.Errorf("client cannot be nil")
	}

	if client.GetChannelType() != "youtube" {
		return nil, fmt.Errorf("client is not a YouTube client")
	}

	adapter := &ClientAdapter{
		client: client,
	}

	// Verify adapter implements YouTubeClient interface
	var _ youtubemodel.YouTubeClient = adapter

	return adapter, nil
}

// Connect establishes a connection to the YouTube API
func (a *ClientAdapter) Connect(ctx context.Context) error {
	return a.client.Connect(ctx)
}

// Disconnect closes the connection to the YouTube API
func (a *ClientAdapter) Disconnect(ctx context.Context) error {
	return a.client.Disconnect(ctx)
}

// GetChannelInfo retrieves information about a YouTube channel
func (a *ClientAdapter) GetChannelInfo(ctx context.Context, channelID string) (*youtubemodel.YouTubeChannel, error) {
	// Get channel info from the client
	channel, err := a.client.GetChannelInfo(ctx, channelID)
	if err != nil {
		return nil, err
	}
	// Convert client.Channel to YouTubeChannel
	ytChannel := &youtubemodel.YouTubeChannel{
		ID:              channelID,
		Title:           channel.GetName(),
		Description:     channel.GetDescription(),
		Thumbnails:      channel.GetThumbnails(),
		SubscriberCount: channel.GetMemberCount(),
		ViewCount:       channel.GetViewCount(),
		VideoCount:      channel.GetPostCount(),
		Country:         channel.GetCountry(),
		PublishedAt:     channel.GetPublishedAt(),
	}

	return ytChannel, nil
}

// GetVideos retrieves videos from a YouTube channel
func (a *ClientAdapter) GetVideos(ctx context.Context, channelID string, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	// Get messages (videos) from the client
	messages, err := a.client.GetMessages(ctx, channelID, fromTime, toTime, limit)
	if err != nil {
		return nil, err
	}

	// Convert messages to YouTube videos
	videos := make([]*youtubemodel.YouTubeVideo, 0, len(messages))
	for _, msg := range messages {
		// Use the new getter methods directly
		video := &youtubemodel.YouTubeVideo{
			ID:           msg.GetID(),
			ChannelID:    channelID,
			Title:        msg.GetTitle(),
			Description:  msg.GetDescription(),
			PublishedAt:  msg.GetTimestamp(),
			ViewCount:    msg.GetViews(),
			LikeCount:    0,
			CommentCount: msg.GetCommentCount(),
			Thumbnails:   msg.GetThumbnails(),
			Language:     msg.GetLanguage(),
			Duration:     msg.GetDuration(),
		}

		// Extract like count from reactions if available
		if reactions := msg.GetReactions(); reactions != nil {
			if likeCount, ok := reactions["like"]; ok {
				video.LikeCount = likeCount
			}
		}

		videos = append(videos, video)
	}
	return videos, nil
}

// GetVideosFromChannel retrieves videos from a specific YouTube channel
func (a *ClientAdapter) GetVideosFromChannel(ctx context.Context, channelID string, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	// Reuse the GetVideos implementation since they do the same thing
	return a.GetVideos(ctx, channelID, fromTime, toTime, limit)
}

func (a *ClientAdapter) GetVideosByIDs(ctx context.Context, videoIDs []string) ([]*youtubemodel.YouTubeVideo, error) {

	// Check if the underlying client is a YouTubeClientAdapter that wraps a YouTubeDataClient
	if ytClientAdapter, ok := a.client.(*clientpkg.YouTubeClientAdapter); ok {
		// Access the wrapped YouTubeDataClient which has the actual GetVideosByIDs implementation
		return ytClientAdapter.GetVideosByIDs(ctx, videoIDs)
	}
	// Fallback for clients that don't support GetVideosByIDs
	return []*youtubemodel.YouTubeVideo{}, fmt.Errorf("GetVideosByIDs not supported by underlying client type %T", a.client)
}

// GetRandomVideos retrieves videos using random sampling
func (a *ClientAdapter) GetRandomVideos(ctx context.Context, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	// Check if the underlying client is a YouTubeClientAdapter that wraps a YouTubeDataClient
	if ytClientAdapter, ok := a.client.(*clientpkg.YouTubeClientAdapter); ok {
		// Access the wrapped YouTubeDataClient which has the actual random sampling implementation
		return ytClientAdapter.GetRandomVideos(ctx, fromTime, toTime, limit)
	}

	// Fallback for clients that don't support random sampling
	return []*youtubemodel.YouTubeVideo{}, fmt.Errorf("random sampling not supported by underlying client type %T", a.client)
}

// GetSnowballVideos retrieves videos using snowball sampling
func (a *ClientAdapter) GetSnowballVideos(ctx context.Context, seedChannelIDs []string, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	// Check if the underlying client is a YouTubeClientAdapter that wraps a YouTubeDataClient
	if ytClientAdapter, ok := a.client.(*clientpkg.YouTubeClientAdapter); ok {
		// Access the wrapped YouTubeDataClient which has the actual snowball sampling implementation
		return ytClientAdapter.GetSnowballVideos(ctx, seedChannelIDs, fromTime, toTime, limit)
	}

	// Fallback for clients that don't support snowball sampling
	return []*youtubemodel.YouTubeVideo{}, fmt.Errorf("snowball sampling not supported by underlying client type %T", a.client)
}

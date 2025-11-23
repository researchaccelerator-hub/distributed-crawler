package client

import (
	"context"
	"time"
)

// YouTubeInnerTubeClientAdapter adapts YouTubeInnerTubeClient to the Client interface
type YouTubeInnerTubeClientAdapter struct {
	client *YouTubeInnerTubeClient
}

// Connect establishes a connection to the InnerTube API
func (a *YouTubeInnerTubeClientAdapter) Connect(ctx context.Context) error {
	return a.client.Connect(ctx)
}

// Disconnect closes the connection to the InnerTube API
func (a *YouTubeInnerTubeClientAdapter) Disconnect(ctx context.Context) error {
	return a.client.Disconnect(ctx)
}

// GetChannelInfo retrieves information about a YouTube channel
func (a *YouTubeInnerTubeClientAdapter) GetChannelInfo(ctx context.Context, channelID string) (Channel, error) {
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
func (a *YouTubeInnerTubeClientAdapter) GetMessages(ctx context.Context, channelID string, fromTime, toTime time.Time, limit int) ([]Message, error) {
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
			SenderName:   "YouTube Channel",
			Text:         video.Title + "\n\n" + video.Description,
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

// GetChannelType returns "youtube" as the channel type
func (a *YouTubeInnerTubeClientAdapter) GetChannelType() string {
	return "youtube"
}

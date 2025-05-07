// Package youtube contains YouTube-specific data models
package youtube

import (
	"context"
	"time"
)

// YouTubeChannel represents a YouTube channel
type YouTubeChannel struct {
	ID              string
	Title           string
	Description     string
	SubscriberCount int64
	ViewCount       int64
	VideoCount      int64
	PublishedAt     time.Time
	Thumbnails      map[string]string
	Country         string            // Country code of the channel
}

// YouTubeVideo represents a YouTube video
type YouTubeVideo struct {
	ID           string
	ChannelID    string
	Title        string
	Description  string
	PublishedAt  time.Time
	ViewCount    int64
	LikeCount    int64
	CommentCount int64
	Duration     string
	Thumbnails   map[string]string
	Tags         []string
	Language     string           // Default language of the video
}

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
	
	// GetVideosFromChannel retrieves videos from a specific YouTube channel
	GetVideosFromChannel(ctx context.Context, channelID string, fromTime, toTime time.Time, limit int) ([]*YouTubeVideo, error)
	
	// GetRandomVideos retrieves videos using random sampling with the prefix generator
	GetRandomVideos(ctx context.Context, fromTime, toTime time.Time, limit int) ([]*YouTubeVideo, error)
	
	// GetSnowballVideos retrieves videos using snowball sampling from channels with > 10 videos
	GetSnowballVideos(ctx context.Context, seedChannelIDs []string, fromTime, toTime time.Time, limit int) ([]*YouTubeVideo, error)
}
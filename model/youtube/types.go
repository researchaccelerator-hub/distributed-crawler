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
}
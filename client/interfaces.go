package client

import (
	"context"
	"time"
)

// Client represents a generic client for any platform
type Client interface {
	// Connect establishes a connection to the service
	Connect(ctx context.Context) error

	// Disconnect closes the connection to the service
	Disconnect(ctx context.Context) error

	// GetChannelInfo retrieves information about a channel
	GetChannelInfo(ctx context.Context, channelID string) (Channel, error)

	// GetMessages retrieves messages from a channel
	GetMessages(ctx context.Context, channelID string, fromTime, toTime time.Time, limit int) ([]Message, error)

	// GetChannelType returns the type of channel ("telegram", "youtube")
	GetChannelType() string
}

// Channel represents a generic channel across platforms
type Channel interface {
	// GetID returns the channel ID
	GetID() string

	// GetName returns the channel name
	GetName() string

	// GetDescription returns the channel description
	GetDescription() string

	// GetMemberCount returns the number of members/subscribers
	GetMemberCount() int64

	// GetCountry returns the country code of the channel (if available)
	GetCountry() string

	// GetType returns the platform type ("telegram", "youtube")
	GetType() string

	GetViewCount() int64

	GetPostCount() int64

	GetPublishedAt() time.Time

	GetThumbnails() map[string]string
}

// Message represents a generic message across platforms
type Message interface {
	// GetID returns the message ID
	GetID() string

	// GetChannelID returns the channel ID
	GetChannelID() string

	// GetSenderID returns the sender ID
	GetSenderID() string

	// GetSenderName returns the sender name
	GetSenderName() string

	// GetText returns the message text
	GetText() string

	// GetTitle returns the message title (if applicable, otherwise empty)
	GetTitle() string

	// GetDescription returns the message description (if applicable, otherwise empty)
	GetDescription() string

	// GetTimestamp returns the message timestamp
	GetTimestamp() time.Time

	// GetViews returns the view count
	GetViews() int64

	// GetReactions returns the reaction count
	GetReactions() map[string]int64

	// GetCommentCount returns the comment count
	GetCommentCount() int64

	// GetThumbnails returns the thumbnails map
	GetThumbnails() map[string]string

	// GetLanguage returns the content language
	GetLanguage() string

	// GetType returns the platform type ("telegram", "youtube")
	GetType() string
}

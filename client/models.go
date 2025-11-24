package client

import (
	"time"
)

// Channel models
// *********************************************

// TelegramChannel implements Channel for Telegram
type TelegramChannel struct {
	ID          string
	Name        string
	Description string
	MemberCount int64
}

// GetID implements Channel
func (c *TelegramChannel) GetID() string {
	return c.ID
}

// GetName implements Channel
func (c *TelegramChannel) GetName() string {
	return c.Name
}

// GetDescription implements Channel
func (c *TelegramChannel) GetDescription() string {
	return c.Description
}

// GetMemberCount implements Channel
func (c *TelegramChannel) GetMemberCount() int64 {
	return c.MemberCount
}

// GetCountry implements Channel
func (c *TelegramChannel) GetCountry() string {
	return "" // Not typically available for Telegram
}

// GetType implements Channel
func (c *TelegramChannel) GetType() string {
	return "telegram"
}

// YouTubeChannel implements Channel for YouTube
type YouTubeChannel struct {
	ID          string
	Name        string
	Description string
	MemberCount int64
	Country     string // Country code of the channel
}

// GetID implements Channel
func (c *YouTubeChannel) GetID() string {
	return c.ID
}

// GetName implements Channel
func (c *YouTubeChannel) GetName() string {
	return c.Name
}

// GetDescription implements Channel
func (c *YouTubeChannel) GetDescription() string {
	return c.Description
}

// GetMemberCount implements Channel
func (c *YouTubeChannel) GetMemberCount() int64 {
	return c.MemberCount
}

// GetCountry implements Channel
func (c *YouTubeChannel) GetCountry() string {
	return c.Country
}

// GetType implements Channel
func (c *YouTubeChannel) GetType() string {
	return "youtube"
}

// Message models
// *********************************************

// TelegramMessage implements Message for Telegram
type TelegramMessage struct {
	ID          string
	ChannelID   string
	SenderID    string
	SenderName  string
	Text        string
	Title       string // Usually empty for Telegram
	Description string // Usually empty for Telegram
	Timestamp   time.Time
	Views       int64
	Reactions   map[string]int64
	Thumbnails  map[string]string // Usually empty for Telegram
	Comments    int64             // Usually not available for Telegram
	Language    string            // Usually not available for Telegram
}

// GetID implements Message
func (m *TelegramMessage) GetID() string {
	return m.ID
}

// GetChannelID implements Message
func (m *TelegramMessage) GetChannelID() string {
	return m.ChannelID
}

// GetSenderID implements Message
func (m *TelegramMessage) GetSenderID() string {
	return m.SenderID
}

// GetSenderName implements Message
func (m *TelegramMessage) GetSenderName() string {
	return m.SenderName
}

// GetText implements Message
func (m *TelegramMessage) GetText() string {
	return m.Text
}

// GetTimestamp implements Message
func (m *TelegramMessage) GetTimestamp() time.Time {
	return m.Timestamp
}

// GetViews implements Message
func (m *TelegramMessage) GetViews() int64 {
	return m.Views
}

// GetReactions implements Message
func (m *TelegramMessage) GetReactions() map[string]int64 {
	return m.Reactions
}

// GetTitle implements Message
func (m *TelegramMessage) GetTitle() string {
	return m.Title
}

// GetDescription implements Message
func (m *TelegramMessage) GetDescription() string {
	return m.Description
}

// GetCommentCount implements Message
func (m *TelegramMessage) GetCommentCount() int64 {
	return m.Comments
}

// GetThumbnails implements Message
func (m *TelegramMessage) GetThumbnails() map[string]string {
	return m.Thumbnails
}

// GetLanguage implements Message
func (m *TelegramMessage) GetLanguage() string {
	return m.Language
}

// GetType implements Message
func (m *TelegramMessage) GetType() string {
	return "telegram"
}

// YouTubeMessage implements Message for YouTube
type YouTubeMessage struct {
	ID           string
	ChannelID    string
	SenderID     string
	SenderName   string
	Text         string
	Title        string             // Video title
	Description  string             // Video description
	Timestamp    time.Time
	Views        int64
	Reactions    map[string]int64
	Thumbnails   map[string]string  // Video thumbnails
	CommentCount int64              // Video comment count
	Language     string             // Video language
}

// GetID implements Message
func (m *YouTubeMessage) GetID() string {
	return m.ID
}

// GetChannelID implements Message
func (m *YouTubeMessage) GetChannelID() string {
	return m.ChannelID
}

// GetSenderID implements Message
func (m *YouTubeMessage) GetSenderID() string {
	return m.SenderID
}

// GetSenderName implements Message
func (m *YouTubeMessage) GetSenderName() string {
	return m.SenderName
}

// GetText implements Message
func (m *YouTubeMessage) GetText() string {
	return m.Text
}

// GetTimestamp implements Message
func (m *YouTubeMessage) GetTimestamp() time.Time {
	return m.Timestamp
}

// GetViews implements Message
func (m *YouTubeMessage) GetViews() int64 {
	return m.Views
}

// GetReactions implements Message
func (m *YouTubeMessage) GetReactions() map[string]int64 {
	return m.Reactions
}

// GetTitle implements Message
func (m *YouTubeMessage) GetTitle() string {
	return m.Title
}

// GetDescription implements Message
func (m *YouTubeMessage) GetDescription() string {
	return m.Description
}

// GetCommentCount implements Message
func (m *YouTubeMessage) GetCommentCount() int64 {
	return m.CommentCount
}

// GetThumbnails implements Message
func (m *YouTubeMessage) GetThumbnails() map[string]string {
	return m.Thumbnails
}

// GetLanguage implements Message
func (m *YouTubeMessage) GetLanguage() string {
	return m.Language
}

// GetType implements Message
func (m *YouTubeMessage) GetType() string {
	return "youtube"
}

// BlueskyChannel implements Channel for Bluesky
type BlueskyChannel struct {
	ID          string
	Name        string
	Description string
	MemberCount int64
}

// GetID implements Channel
func (c *BlueskyChannel) GetID() string {
	return c.ID
}

// GetName implements Channel
func (c *BlueskyChannel) GetName() string {
	return c.Name
}

// GetDescription implements Channel
func (c *BlueskyChannel) GetDescription() string {
	return c.Description
}

// GetMemberCount implements Channel
func (c *BlueskyChannel) GetMemberCount() int64 {
	return c.MemberCount
}

// GetCountry implements Channel
func (c *BlueskyChannel) GetCountry() string {
	return "" // Not available for Bluesky
}

// GetType implements Channel
func (c *BlueskyChannel) GetType() string {
	return "bluesky"
}

// BlueskyMessage implements Message for Bluesky
type BlueskyMessage struct {
	ID           string
	ChannelID    string
	SenderID     string
	SenderName   string
	Text         string
	Title        string
	Description  string
	Timestamp    time.Time
	Views        int64
	Reactions    map[string]int64
	Thumbnails   map[string]string
	CommentCount int64
	Language     string
	Type         string
}

// GetID implements Message
func (m *BlueskyMessage) GetID() string {
	return m.ID
}

// GetChannelID implements Message
func (m *BlueskyMessage) GetChannelID() string {
	return m.ChannelID
}

// GetSenderID implements Message
func (m *BlueskyMessage) GetSenderID() string {
	return m.SenderID
}

// GetSenderName implements Message
func (m *BlueskyMessage) GetSenderName() string {
	return m.SenderName
}

// GetText implements Message
func (m *BlueskyMessage) GetText() string {
	return m.Text
}

// GetTimestamp implements Message
func (m *BlueskyMessage) GetTimestamp() time.Time {
	return m.Timestamp
}

// GetViews implements Message
func (m *BlueskyMessage) GetViews() int64 {
	return m.Views
}

// GetReactions implements Message
func (m *BlueskyMessage) GetReactions() map[string]int64 {
	return m.Reactions
}

// GetTitle implements Message
func (m *BlueskyMessage) GetTitle() string {
	return m.Title
}

// GetDescription implements Message
func (m *BlueskyMessage) GetDescription() string {
	return m.Description
}

// GetCommentCount implements Message
func (m *BlueskyMessage) GetCommentCount() int64 {
	return m.CommentCount
}

// GetThumbnails implements Message
func (m *BlueskyMessage) GetThumbnails() map[string]string {
	return m.Thumbnails
}

// GetLanguage implements Message
func (m *BlueskyMessage) GetLanguage() string {
	return m.Language
}

// GetType implements Message
func (m *BlueskyMessage) GetType() string {
	if m.Type != "" {
		return m.Type
	}
	return "bluesky"
}
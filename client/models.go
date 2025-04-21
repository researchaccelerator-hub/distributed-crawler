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

// GetType implements Message
func (m *YouTubeMessage) GetType() string {
	return "youtube"
}
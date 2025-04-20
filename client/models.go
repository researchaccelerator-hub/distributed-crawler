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
	ID         string
	ChannelID  string
	SenderID   string
	SenderName string
	Text       string
	Timestamp  time.Time
	Views      int64
	Reactions  map[string]int64
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

// GetType implements Message
func (m *TelegramMessage) GetType() string {
	return "telegram"
}

// YouTubeMessage implements Message for YouTube
type YouTubeMessage struct {
	ID         string
	ChannelID  string
	SenderID   string
	SenderName string
	Text       string
	Timestamp  time.Time
	Views      int64
	Reactions  map[string]int64
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

// GetType implements Message
func (m *YouTubeMessage) GetType() string {
	return "youtube"
}
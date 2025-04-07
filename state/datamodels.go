package state

import (
	"sync"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/model"
)

// StateManager extends StateManagementInterface with additional methods
// needed for the new crawler architecture
type StateManager interface {
	StateManagementInterface

	// SavePost saves a post to the storage
	SavePost(ctx interface{}, post model.Post) error

	// GetChannelInfo retrieves information about a channel
	GetChannelInfo(channelID string) (*ChannelInfo, error)

	// SaveChannelInfo saves channel information to the storage
	SaveChannelInfo(info *ChannelInfo) error
}

// ChannelInfo represents information about a channel/source
type ChannelInfo struct {
	ChannelID   string                 `json:"channelId"`   // Channel identifier
	Name        string                 `json:"name"`        // Channel name
	Description string                 `json:"description"` // Channel description
	MemberCount int64                  `json:"memberCount"` // Number of members/subscribers
	URL         string                 `json:"url"`         // External URL to the channel
	Platform    string                 `json:"platform"`    // Platform type ("telegram", "youtube")
	LastCrawled time.Time              `json:"lastCrawled"` // Last time this channel was crawled
	Metadata    map[string]interface{} `json:"metadata"`    // Additional platform-specific metadata
}

// Page represents a URL/page being crawled
type Page struct {
	// Core page information
	ID        string    `json:"id"`
	URL       string    `json:"url"`
	Depth     int       `json:"depth"`
	Status    string    `json:"status"` // "unfetched", "fetching", "fetched", "error"
	Error     string    `json:"error,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	Platform  string    `json:"platform,omitempty"` // Added for multi-platform support

	// Relationships
	ParentID string    `json:"parentId,omitempty"`
	Messages []Message `json:"messages,omitempty"`
}

// Message represents a message associated with a page
type Message struct {
	ChatID    int64  `json:"chatId"`
	MessageID int64  `json:"messageId"`
	Status    string `json:"status"`
	PageID    string `json:"pageId"`
	Platform  string `json:"platform,omitempty"` // Added for multi-platform support
}

// Layer represents a collection of pages at the same depth level
type Layer struct {
	Depth int    `json:"depth"`
	Pages []Page `json:"pages"`
	mutex sync.RWMutex
}

// CrawlMetadata stores information about a crawl operation
type CrawlMetadata struct {
	CrawlID         string    `json:"crawlId"`
	ExecutionID     string    `json:"executionId"`
	StartTime       time.Time `json:"startTime"`
	EndTime         time.Time `json:"endTime,omitempty"`
	Status          string    `json:"status"` // "running", "completed", "failed"
	PreviousCrawlID []string  `json:"previousCrawlId,omitempty"`
	Platform        string    `json:"platform,omitempty"` // Added for multi-platform support
	TargetChannels  []string  `json:"targetChannels,omitempty"` // Target channels for this crawl
	MessagesCount   int       `json:"messagesCount,omitempty"` // Number of messages retrieved
	ErrorsCount     int       `json:"errorsCount,omitempty"` // Number of errors encountered
}

// MediaCacheItem represents an item in the media cache
type MediaCacheItem struct {
	ID        string    `json:"id"`
	FirstSeen time.Time `json:"firstSeen"`
	Metadata  string    `json:"metadata,omitempty"`
	Platform  string    `json:"platform,omitempty"` // Added for multi-platform support
}

// MediaCache represents a sharded cache for processed media items
// This structure helps to avoid Dapr size limits by partitioning the cache
type MediaCache struct {
	Items      map[string]MediaCacheItem `json:"items"`      // Media cache items in this shard
	UpdateTime time.Time                 `json:"updateTime"` // Last time this shard was updated
	CacheID    string                    `json:"cacheId"`    // Unique ID for this cache shard
}

// MediaCacheIndex keeps track of which shard contains which media IDs
// This is much smaller than the full cache and allows quick lookups
type MediaCacheIndex struct {
	Shards     []string          `json:"shards"`     // List of shard IDs
	MediaIndex map[string]string `json:"mediaIndex"` // Maps media ID to shard ID
	UpdateTime time.Time         `json:"updateTime"` // Last time the index was updated
}

// State represents the complete state of a crawl operation
type State struct {
	Layers      []*Layer      `json:"layers"`
	Metadata    CrawlMetadata `json:"metadata"`
	LastUpdated time.Time     `json:"lastUpdated"`
}

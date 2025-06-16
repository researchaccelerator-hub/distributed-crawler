// Package crawler defines the core interfaces and types for the multi-platform crawler system.
package crawler

import (
	"context"
	"fmt"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/model"
	tdlibclient "github.com/zelenin/go-tdlib/client"
)

// PlatformType represents the type of platform to crawl
type PlatformType string

const (
	// PlatformTelegram is for Telegram crawling
	PlatformTelegram PlatformType = "telegram"
	// PlatformYouTube is for YouTube crawling
	PlatformYouTube PlatformType = "youtube"
)

// CrawlTarget represents a specific source to crawl
type CrawlTarget struct {
	ID       string            // Universal identifier (channel name, YouTube channel ID, etc.)
	Type     PlatformType      // Platform type
	Metadata map[string]string // Additional source-specific metadata
}

// CrawlResult represents unified crawl results
type CrawlResult struct {
	Posts  []model.Post
	Errors []error
}

// CrawlJob represents a job to crawl a specific target
type CrawlJob struct {
	Target     CrawlTarget
	FromTime   time.Time
	ToTime     time.Time
	Limit      int
	SampleSize int // Number of posts to randomly sample (0 means no sampling)
}

// Crawler defines the interface that all crawler implementations must satisfy
type Crawler interface {
	// Initialize sets up the crawler with necessary configuration
	Initialize(ctx context.Context, config map[string]interface{}) error

	// ValidateTarget checks if a target is valid for this crawler
	ValidateTarget(target CrawlTarget) error

	// GetChannelInfo retrieves information about a channel
	GetChannelInfo(ctx context.Context, target CrawlTarget) (*model.ChannelData, error)

	// FetchMessages retrieves messages from the specified target
	FetchMessages(ctx context.Context, job CrawlJob) (CrawlResult, error)

	// GetPlatformType returns the type of platform this crawler supports
	GetPlatformType() PlatformType

	// Close cleans up resources
	Close() error
}

// CrawlerFactory creates crawlers of various types
type CrawlerFactory interface {
	// GetCrawler returns a crawler for the specified type
	GetCrawler(platformType PlatformType) (Crawler, error)

	// RegisterCrawler registers a crawler implementation for a platform type
	RegisterCrawler(platformType PlatformType, creator func() Crawler) error
}

// DefaultCrawlerFactory implements the CrawlerFactory interface
type DefaultCrawlerFactory struct {
	creators map[PlatformType]func() Crawler
}

// NewCrawlerFactory creates a new crawler factory
func NewCrawlerFactory() *DefaultCrawlerFactory {
	return &DefaultCrawlerFactory{
		creators: make(map[PlatformType]func() Crawler),
	}
}

// RegisterCrawler registers a crawler implementation for a platform type
func (f *DefaultCrawlerFactory) RegisterCrawler(platformType PlatformType, creator func() Crawler) error {
	if _, exists := f.creators[platformType]; exists {
		return fmt.Errorf("crawler for platform %s already registered", platformType)
	}
	f.creators[platformType] = creator
	return nil
}

// GetCrawler returns a crawler for the specified type
func (f *DefaultCrawlerFactory) GetCrawler(platformType PlatformType) (Crawler, error) {
	creator, exists := f.creators[platformType]
	if !exists {
		return nil, fmt.Errorf("no crawler registered for platform %s", platformType)
	}
	return creator(), nil
}

// TDLibClient interface defines the methods we need from the TDLib client
type TDLibClient interface {
	GetMessage(req *tdlibclient.GetMessageRequest) (*tdlibclient.Message, error)
	GetMessageLink(req *tdlibclient.GetMessageLinkRequest) (*tdlibclient.MessageLink, error)
	GetMessageThreadHistory(req *tdlibclient.GetMessageThreadHistoryRequest) (*tdlibclient.Messages, error)
	GetMessageThread(req *tdlibclient.GetMessageThreadRequest) (*tdlibclient.MessageThreadInfo, error)
	GetRemoteFile(req *tdlibclient.GetRemoteFileRequest) (*tdlibclient.File, error)
	DownloadFile(req *tdlibclient.DownloadFileRequest) (*tdlibclient.File, error)
	GetChatHistory(req *tdlibclient.GetChatHistoryRequest) (*tdlibclient.Messages, error)
	SearchPublicChat(req *tdlibclient.SearchPublicChatRequest) (*tdlibclient.Chat, error)
	GetChat(req *tdlibclient.GetChatRequest) (*tdlibclient.Chat, error)
	GetSupergroup(req *tdlibclient.GetSupergroupRequest) (*tdlibclient.Supergroup, error)
	GetSupergroupFullInfo(req *tdlibclient.GetSupergroupFullInfoRequest) (*tdlibclient.SupergroupFullInfo, error)
	Close() (*tdlibclient.Ok, error)
	GetMe() (*tdlibclient.User, error)
	GetBasicGroupFullInfo(req *tdlibclient.GetBasicGroupFullInfoRequest) (*tdlibclient.BasicGroupFullInfo, error)
	GetUser(*tdlibclient.GetUserRequest) (*tdlibclient.User, error)
	DeleteFile(req *tdlibclient.DeleteFileRequest) (*tdlibclient.Ok, error)
}

// CrawlerOptions holds configuration options for a crawler
type CrawlerOptions struct {
	StoragePrefix string
	Config        interface{}
	// Add other options as needed
}

// Option is a function that sets an option on a crawler
type Option func(*CrawlerOptions)

// WithStoragePrefix sets the storage prefix for the crawler
func WithStoragePrefix(prefix string) Option {
	return func(o *CrawlerOptions) {
		o.StoragePrefix = prefix
	}
}

// WithConfig sets the configuration for the crawler
func WithConfig(config interface{}) Option {
	return func(o *CrawlerOptions) {
		o.Config = config
	}
}

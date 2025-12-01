// Package state provides interfaces and implementations for managing crawler state,
// including pages, layers, and metadata across different storage backends.
//
// It supports multiple storage providers such as local filesystem, Azure Blob Storage,
// and DAPR state management, allowing for flexible deployment configurations.
package state

import (
	"github.com/researchaccelerator-hub/telegram-scraper/model"
)

// StateManagementInterface defines the core functionality for state management
// regardless of the underlying storage implementation (filesystem, cloud storage, etc.)
type StateManagementInterface interface {
	// Initialize sets up the state store with seed data
	// It checks for existing state or initializes a new one with the given URLs
	Initialize(seedURLs []string) error

	// Core page and message operations
	// GetPage retrieves a page by its ID from the state store
	GetPage(id string) (Page, error)

	// UpdatePage updates or creates a page in the state store
	UpdatePage(page Page) error

	// UpdateMessage sets the status of a specific message within a page
	// This is used to track processing status of individual messages
	UpdateMessage(pageID string, chatID int64, messageID int64, status string) error

	// Layer management
	// AddLayer adds a new layer of pages at a specific depth level
	// Used when discovering new pages during crawling
	AddLayer(pages []Page) error

	// GetLayerByDepth retrieves all pages at a specified depth
	GetLayerByDepth(depth int) ([]Page, error)

	// GetMaxDepth returns the highest depth value present in the state
	GetMaxDepth() (int, error)

	// State persistence
	// SaveState persists the current state to the storage backend
	SaveState() error

	// ExportPagesToBinding exports pages to an external storage binding
	// Useful for transferring data to other systems
	ExportPagesToBinding(crawlID string) error

	// Data storage
	// StorePost saves a parsed Telegram post to persistent storage
	StorePost(channelID string, post model.Post) error

	// StoreFile saves a media file to persistent storage and returns its new path
	StoreFile(channelID string, sourceFilePath string, fileName string) (string, string, error)

	// Crawl management
	// GetPreviousCrawls returns a list of previous crawl IDs
	GetPreviousCrawls() ([]string, error)

	// UpdateCrawlMetadata updates metadata for a specific crawl
	// Used to track crawl progress, status, and statistics
	UpdateCrawlMetadata(crawlID string, metadata map[string]interface{}) error

	// FindIncompleteCrawl checks if there's an incomplete crawl with the given ID
	// Returns executionID, exists flag, and any error
	FindIncompleteCrawl(crawlID string) (string, bool, error)

	// Media cache
	// HasProcessedMedia checks if a media item has already been processed
	// Used to avoid duplicate downloads and storage
	HasProcessedMedia(mediaID string) (bool, error)

	// MarkMediaAsProcessed marks a media item as processed in the cache
	MarkMediaAsProcessed(mediaID string) error

	// Used for random-walk sampling

	// Initializes discovered channels from database
	// Only implemented for dapr currently
	InitializeDiscoveredChannels() error
	// Initializes initial layer from randomly chosen discovered channels
	InitializeRandomWalkLayer() error
	GetRandomDiscoveredChannel() (string, error)
	IsDiscoveredChannel(channelID string) bool
	AddDiscoveredChannel(channelID string) error
	StoreChannelData(channelID string, channelData *model.ChannelData) error
	// random-walk database
	SaveEdgeRecords(edges []*EdgeRecord) error
	GetPagesFromLayerBuffer() ([]Page, error)
	WipeLayerBuffer(includeCurrentCrawl bool) error
	ExecuteDatabaseOperation(sqlQuery string, params []any) error
	AddPageToLayerBuffer(page *Page) error

	// Combined files
	UploadCombinedFile(filename string) error

	// Cleanup
	// Close performs cleanup operations when shutting down
	Close() error
}

// StateManagerFactory defines a factory interface for creating appropriate
// state manager implementations based on configuration.
type StateManagerFactory interface {
	// Create instantiates and configures a state manager implementation
	// based on the provided configuration. It returns a fully initialized
	// state manager ready for use and any error encountered during creation.
	//
	// The factory will examine the config to determine which state manager
	// implementation to use (local, Azure, DAPR, etc.) and set it up accordingly.
	Create(config Config) (StateManagementInterface, error)
}

// MaxPagesConfig configures the maximum number of pages the crawler will process.
// This prevents unbounded crawling and provides control over resource usage.
type MaxPagesConfig struct {
	// MaxPages sets the upper limit on the number of pages to be crawled
	// If this limit is reached, the crawler will implement a replacement strategy
	// to handle new pages (default: 108000)
	MaxPages int
}

// Config contains common configuration for all state manager implementations.
// It holds both core settings and provider-specific configuration options.
type Config struct {
	// StorageRoot is the base storage location, which could be:
	// - A filesystem path for local storage
	// - A container name for cloud storage
	// - A component name for DAPR storage
	StorageRoot string

	// Crawl identifiers
	// CrawlID identifies a logical crawl operation across runs
	CrawlID string

	// CrawlExecutionID identifies a specific execution instance
	// This allows for multiple executions of the same logical crawl
	CrawlExecutionID string

	// Platform identifies which platform we're crawling
	// This affects storage binding selection
	// Values can be "telegram", "youtube", etc.
	Platform string

	// SamplingMethod identifies which sampling method is being used
	// This affects certain behavior involving duplicate pages and random-walk related structs
	// Values can be "channel", "random", "snowball", "random-walk"
	SamplingMethod string

	// SeedSize indentifies the number of discovered channels to pull at random
	SeedSize int

	// Specific configuration options for different backends
	// Only one of these should typically be set, based on the
	// storage backend being used
	AzureConfig    *AzureConfig
	DaprConfig     *DaprConfig
	LocalConfig    *LocalConfig
	MaxPagesConfig *MaxPagesConfig

	// Boolean for whether or not to combine crawl data before upload
	CombineFiles bool
	// Directory to write crawl data to for combining
	CombineWatchDir string
}

// AzureConfig contains Azure Blob Storage-specific configuration options
// for storing crawler state and processed data in Azure cloud storage.
type AzureConfig struct {
	// ContainerName is the name of the Azure Blob Storage container to use
	ContainerName string

	// BlobNameRoot is the base path prefix for all blobs in the container
	BlobNameRoot string

	// AccountURL is the full URL to the Azure Storage account
	// Format: https://<account-name>.blob.core.windows.net
	AccountURL string
}

// DaprConfig contains configuration for using the Distributed Application Runtime (DAPR)
// as a storage backend for the crawler state and processed data.
type DaprConfig struct {
	// StateStoreName is the name of the DAPR state store component to use
	// This refers to a configured state store in the DAPR runtime
	StateStoreName string

	// ComponentName is the name of the DAPR binding component for file storage
	// This refers to a configured binding component in the DAPR runtime
	ComponentName string

	// DatabaseComponentName is the name of the Dapr binding component for database storage
	// This is used exclusively in random-walk sampling
	DatabaseComponentName string
}

// LocalConfig contains configuration for storing crawler state and
// processed data on the local filesystem.
type LocalConfig struct {
	// BasePath specifies the root directory where all crawl data will be stored
	// This should be an absolute path to a directory where the process has write access
	BasePath string
}

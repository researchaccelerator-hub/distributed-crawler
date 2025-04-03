package state

import (
	"github.com/researchaccelerator-hub/telegram-scraper/model"
)

// StateManagementInterface defines the core functionality for state management
// regardless of the underlying storage implementation
type StateManagementInterface interface {
	// Initialize the state store with seed data
	Initialize(seedURLs []string) error

	// Core page and message operations
	GetPage(id string) (Page, error)
	UpdatePage(page Page) error
	UpdateMessage(pageID string, chatID int64, messageID int64, status string) error

	// Layer management
	AddLayer(pages []Page) error
	GetLayerByDepth(depth int) ([]Page, error)
	GetMaxDepth() (int, error)

	// State persistence
	SaveState() error
	ExportPagesToBinding(crawlID string) error

	// Data storage
	StorePost(channelID string, post model.Post) error
	StoreFile(channelID string, sourceFilePath string, fileName string) (string, error)

	// Crawl management
	GetPreviousCrawls() ([]string, error)
	UpdateCrawlMetadata(crawlID string, metadata map[string]interface{}) error
	FindIncompleteCrawl(crawlID string) (string, bool, error) // Returns executionID, exists, error

	// Media cache
	HasProcessedMedia(mediaID string) (bool, error)
	MarkMediaAsProcessed(mediaID string) error

	// Cleanup
	Close() error
}

// StateManagerFactory creates the appropriate state manager implementation
type StateManagerFactory interface {
	// Create returns a state manager implementation based on the given configuration
	Create(config Config) (StateManagementInterface, error)
}

// Config contains common configuration for all state manager implementations
type Config struct {
	// Base storage location (filesystem path, container name, etc.)
	StorageRoot string

	// Crawl identifiers
	CrawlID          string
	CrawlExecutionID string

	// Specific configuration options for different backends
	AzureConfig *AzureConfig
	DaprConfig  *DaprConfig
	LocalConfig *LocalConfig
}

// AzureConfig contains Azure-specific configuration
type AzureConfig struct {
	ContainerName string
	BlobNameRoot  string
	AccountURL    string
}

// DaprConfig contains Dapr-specific configuration
type DaprConfig struct {
	StateStoreName string
	ComponentName  string
}

// LocalConfig contains local filesystem-specific configuration
type LocalConfig struct {
	BasePath string
}

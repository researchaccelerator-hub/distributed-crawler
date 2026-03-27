// Package state provides interfaces and implementations for managing crawler state,
// including pages, layers, and metadata across different storage backends.
//
// It supports multiple storage providers such as local filesystem, Azure Blob Storage,
// and DAPR state management, allowing for flexible deployment configurations.
package state

import (
	"time"

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

	// LoadSeedChannels loads seed_channels table rows into the in-memory
	// DiscoveredChannels set and chat ID cache.  Only implemented for DAPR.
	LoadSeedChannels() error
	// UpsertSeedChannelChatID caches the TDLib chat ID for a username in both
	// memory and the seed_channels DB table.
	UpsertSeedChannelChatID(username string, chatID int64) error
	// InsertSeedChannelIfNew inserts the username into seed_channels if it does not
	// already exist, without touching chat_id or other columns on an existing row.
	InsertSeedChannelIfNew(username string) error
	// GetCachedChatID returns the cached TDLib chat ID for username, if known.
	GetCachedChatID(username string) (int64, bool)
	// IsSeedChannel reports whether username was loaded from seed_channels at startup.
	IsSeedChannel(username string) bool

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
	GetPagesFromPageBuffer(limit int) ([]Page, error)
	// ClaimPages atomically claims up to limit unclaimed pages from the page
	// buffer for this pod using UPDATE ... FOR UPDATE SKIP LOCKED ... RETURNING.
	ClaimPages(limit int) ([]Page, error)
	// UnclaimPages releases previously claimed pages back to the queue.
	UnclaimPages(pageIDs []string) error
	// RefreshPageClaim updates claimed_at to NOW() for an actively-processed
	// page so that RecoverStalePageClaims does not reclaim it.
	RefreshPageClaim(pageID string) error
	// RecoverStalePageClaims resets pages claimed longer than staleThreshold
	// ago, making them available for re-processing by any pod.
	RecoverStalePageClaims(staleThreshold time.Duration) (int, error)
	ExecuteDatabaseOperation(sqlQuery string, params []any) error
	AddPageToPageBuffer(page *Page) error
	// DeletePageBufferPages removes specific pages by ID from the page buffer.
	// Used in tandem mode to avoid wiping pages the validator wrote after the read.
	DeletePageBufferPages(pageIDs []string, pageURLs []string) error

	// GetChannelLastCrawled returns the last_crawled_at timestamp from seed_channels
	// for the given username. Returns zero time if the channel has never been crawled.
	GetChannelLastCrawled(username string) (time.Time, error)
	// MarkChannelCrawled upserts the channel into seed_channels, recording the
	// current time as last_crawled_at, the most recent message date, and caching
	// the resolved chatID.
	MarkChannelCrawled(username string, chatID int64, lastMessageDate time.Time) error
	// MarkSeedChannelInvalid sets invalidated_at = NOW() on the seed_channels row
	// for the given username, if the row exists.  No-ops for channels not in the
	// seed table.  The 30-day TTL is enforced in LoadSeedChannels and
	// GetRandomSeedChannel by filtering out rows where invalidated_at is recent.
	MarkSeedChannelInvalid(username string) error

	// LoadInvalidChannels populates the in-memory invalid channel cache from the
	// invalid_channels table (rows within the 30-day TTL window only).
	LoadInvalidChannels() error
	// IsInvalidChannel returns true if the channel is cached as invalid and the
	// 30-day TTL has not yet expired.
	IsInvalidChannel(username string) bool
	// MarkChannelInvalid persists the channel to the invalid_channels table and
	// adds it to the in-memory cache.  reason should be "not_found" or "not_supergroup".
	MarkChannelInvalid(username string, reason string) error

	// Combined files
	UploadCombinedFile(filename string) error

	// Validator / tandem-crawl methods

	// CreatePendingBatch inserts a new batch row with status='open'.
	// Called by the crawler when the first potential edge is found for a source channel.
	CreatePendingBatch(batch *PendingEdgeBatch) error

	// InsertPendingEdge writes a single pending edge row. Called as each edge is
	// discovered (streaming — does not wait for the channel to finish).
	InsertPendingEdge(edge *PendingEdge) error

	// ClosePendingBatch sets status='closed' and closed_at=NOW(), signalling that
	// the crawler has finished extracting edges from this source channel.
	ClosePendingBatch(batchID string) error

	// ClaimPendingEdges atomically claims up to limit edges with
	// validation_status='pending', ordered by discovery_time.  Uses
	// UPDATE ... WHERE pending_id IN (SELECT ... FOR UPDATE SKIP LOCKED)
	// to allow concurrent validators.
	ClaimPendingEdges(limit int) ([]*PendingEdge, error)

	// UpdatePendingEdge sets validation_status, validation_reason, and
	// validated_at for one edge.
	UpdatePendingEdge(update PendingEdgeUpdate) error

	// ClaimWalkbackBatch finds the oldest batch where status='closed' and no
	// pending_edges have validation_status='pending'. Atomically sets
	// status='processing'.  Returns (nil, nil, nil) if no batch is ready.
	ClaimWalkbackBatch() (*PendingEdgeBatch, []*PendingEdge, error)

	// CompletePendingBatch sets status='completed' and completed_at=NOW().
	CompletePendingBatch(batchID string) error

	// RecoverStaleBatchClaims resets pending_edge_batches stuck in 'processing'
	// for longer than staleThreshold back to 'closed', incrementing attempt_count.
	// Batches that have reached maxBatchAttempts are logged and left in place.
	RecoverStaleBatchClaims(staleThreshold time.Duration) (int, error)

	// RecoverStaleValidatingEdges resets pending_edges stuck in 'validating'
	// for longer than staleThreshold back to 'pending'. This recovers edges
	// orphaned when a validator pod dies mid-validation.
	RecoverStaleValidatingEdges(staleThreshold time.Duration) (int, error)

	// FlushBatchStats upserts source_type_stats for the batch, then DELETEs
	// all pending_edges rows for that batch.
	FlushBatchStats(batchID, crawlID string, edges []*PendingEdge) error

	// GetRandomSeedChannel returns a random channel_username from seed_channels.
	GetRandomSeedChannel() (string, error)

	// ClaimDiscoveredChannel atomically claims first-discovery of a channel.
	// Returns true if this call inserted (first claim), false if already claimed
	// by any crawl.  crawlID is stored for audit only.
	ClaimDiscoveredChannel(username, crawlID string) (bool, error)

	// IsChannelDiscovered checks whether a channel has been discovered by any
	// crawl in the crawler's history, without inserting. Used by validators to
	// skip HTTP calls.
	IsChannelDiscovered(username string) (bool, error)

	// CountIncompleteBatches returns the count of pending_edge_batches with
	// status != 'completed' for the given crawl_id.
	CountIncompleteBatches(crawlID string) (int, error)

	// InsertAccessEvent appends a row to access_events recording that the
	// validator detected an IP-level block.  An external process polls this
	// table to trigger IP rotation; the validator has no knowledge of the
	// rotation mechanism.
	InsertAccessEvent(reason string) error

	// Edge record repair (400-replacement)

	// GetEdgeRecord returns the edge_records row matching the current crawl,
	// sequence_id, and destination_channel. Returns nil, nil if not found.
	GetEdgeRecord(sequenceID, destinationChannel string) (*EdgeRecord, error)

	// DeleteEdgeRecord removes the edge_records row matching the current crawl,
	// sequence_id, and destination_channel.
	DeleteEdgeRecord(sequenceID, destinationChannel string) error

	// GetRandomSkippedEdge returns a randomly chosen edge_records row where
	// crawl_id matches, skipped=true, sequence_id=sequenceID, and
	// source_channel=sourceChannel. Returns nil, nil if none exist.
	GetRandomSkippedEdge(sequenceID, sourceChannel string) (*EdgeRecord, error)

	// PromoteEdge sets skipped=false on the edge_records row matching the
	// current crawl, sequence_id, and destination_channel.
	PromoteEdge(sequenceID, destinationChannel string) error

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

	// // User-defined label for the crawl (e.g., "youtube-snowball")
	CrawlLabel string

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
	// Directory to move crawl data to for combining
	CombineWatchDir string
	// Directory to write crawl data to (reduces number of file events)
	CombineTempDir string

	// PodName identifies this pod in a multi-pod deployment (from POD_NAME env var).
	// Used as the claimed_by value when claiming pages from the shared page_buffer.
	PodName string
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

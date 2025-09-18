package state

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	daprc "github.com/dapr/go-sdk/client"
	"github.com/researchaccelerator-hub/telegram-scraper/model"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

const (
	// Default component names
	defaultStateStoreName  = "statestore"
	telegramStorageBinding = "telegramcrawlstorage"
	youtubeStorageBinding  = "youtubecrawlstorage"
	databaseStorageBinding = "postgres"
)

// DaprStateManager implements StateManagementInterface using Dapr
type DaprStateManager struct {
	*BaseStateManager
	client *daprc.Client
	// databaseClient  *daprc.Client
	stateStoreName  string
	storageBinding  string
	databaseBinding string

	// For compatibility with old code during transition
	mediaCache      map[string]MediaCacheItem
	mediaCacheMutex sync.RWMutex // Separate mutex for media cache to reduce contention

	// New sharded media cache implementation
	mediaCacheIndex      MediaCacheIndex        // Index of which shard contains which media ID
	activeMediaCache     MediaCache             // Current active shard being written to
	mediaCacheShards     map[string]*MediaCache // All loaded shards
	mediaCacheIndexMutex sync.RWMutex           // Mutex for media cache index operations

	// URL cache
	urlCache      map[string]string // Maps URL -> "crawlID:pageID" for all known URLs
	urlCacheMutex sync.RWMutex      // Separate mutex for URL cache to reduce contention

	// Cache configuration
	maxCacheItemsPerShard int // Maximum number of items per shard
	cacheExpirationDays   int // Number of days after which cache entries are considered stale
}

func GetEnvValue(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

// NewDaprStateManager creates a new Dapr-backed state manager for storing and retrieving crawler state.
//
// This function initializes a state manager that uses Dapr's state store and binding components
// to persist crawler state, including:
// - Page information for each Telegram channel being crawled
// - Layer structure representing the depth of crawling
// - Metadata about the crawl execution
// - Media processing cache to avoid duplicate downloads
// - URL deduplication cache to prevent processing the same URLs repeatedly
//
// The Dapr state manager enables distributed and resilient state management by leveraging
// Dapr's state store capabilities, which can be backed by various databases (Redis, MongoDB, etc.)
// based on the Dapr component configuration.
//
// Parameters:
//   - config: Configuration containing storage paths, crawl identifiers, and Dapr-specific settings
//
// Returns:
//   - A fully initialized DaprStateManager ready to manage crawler state
//   - An error if initialization fails (e.g., Dapr client creation fails)
func NewDaprStateManager(config Config) (*DaprStateManager, error) {
	base := NewBaseStateManager(config)

	// Create Dapr client with custom message size
	maxMessageSize := 200 // 200 MB as configured
	headerBuffer := 1     // 1 MB buffer for headers
	maxSizeInBytes := (maxMessageSize + headerBuffer) * 1024 * 1024

	// Create gRPC call options for both send and receive
	var callOpts []grpc.CallOption
	callOpts = append(callOpts,
		grpc.MaxCallRecvMsgSize(maxSizeInBytes),
		grpc.MaxCallSendMsgSize(maxSizeInBytes),
	)

	// Get Dapr gRPC port from environment
	daprPort := GetEnvValue("DAPR_GRPC_PORT", "50001")

	// Create connection with custom options
	conn, err := grpc.Dial(
		net.JoinHostPort("127.0.0.1", daprPort),
		grpc.WithDefaultCallOptions(callOpts...),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection: %w", err)
	}

	// Create Dapr client with custom connection
	client := daprc.NewClientWithConnection(conn)

	//err = client.Wait(context.Background(), 300)
	//if err != nil {
	//	log.Panic().Err(err).Msg("DAPR FAILED TO START WITHIN THE TIMEOUT")
	//}
	// Get configuration values
	stateStoreName := defaultStateStoreName

	// Determine storage binding based on platform
	var storageBinding string

	// If component name is explicitly set in config, use that
	if config.DaprConfig != nil && config.DaprConfig.ComponentName != "" {
		storageBinding = config.DaprConfig.ComponentName
	} else {
		// Otherwise select based on platform
		switch config.Platform {
		case "youtube":
			storageBinding = youtubeStorageBinding
			log.Info().Str("platform", config.Platform).Str("storage_binding", storageBinding).Msg("Using YouTube storage binding")
		default:
			// Default to telegram storage binding for telegram platform or any unknown platform
			storageBinding = telegramStorageBinding
			log.Info().Str("platform", config.Platform).Str("storage_binding", storageBinding).Msg("Using Telegram storage binding")
		}
	}

	// Set state store name if configured
	if config.DaprConfig != nil && config.DaprConfig.StateStoreName != "" {
		stateStoreName = config.DaprConfig.StateStoreName
	}

	// Generate a new cache ID for the active cache
	newCacheID := uuid.New().String()

	// Default cache configuration
	maxCacheItemsPerShard := 5000 // Limit each shard to 5000 items
	cacheExpirationDays := 30     // Expire items after 30 days

	dsm := &DaprStateManager{
		BaseStateManager: base,
		client:           &client,
		stateStoreName:   stateStoreName,
		storageBinding:   storageBinding,
		mediaCache:       make(map[string]MediaCacheItem), // For backward compatibility
		urlCache:         make(map[string]string),

		// Initialize new sharded cache structure
		mediaCacheShards: make(map[string]*MediaCache),
		activeMediaCache: MediaCache{
			Items:      make(map[string]MediaCacheItem),
			UpdateTime: time.Now(),
			CacheID:    newCacheID,
		},
		mediaCacheIndex: MediaCacheIndex{
			Shards:     []string{newCacheID},
			MediaIndex: make(map[string]string),
			UpdateTime: time.Now(),
		},

		// Default configuration
		maxCacheItemsPerShard: maxCacheItemsPerShard,
		cacheExpirationDays:   cacheExpirationDays,
	}

	// Load the media cache index as part of initialization
	err = dsm.loadMediaCacheIndex()
	if err != nil {
		log.Warn().Err(err).Msg("Failed to load media cache index, starting with a fresh index")
		// Continue with the fresh index initialized above
	}

	return dsm, nil
}
func (dsm *DaprStateManager) GetPage(id string) (Page, error) {
	// First try memory
	page, err := dsm.BaseStateManager.GetPage(id)
	if err == nil {
		return page, nil
	}

	// If not in memory, try DAPR
	pageKey := fmt.Sprintf("%s/page/%s", dsm.config.CrawlExecutionID, id)
	response, err := (*dsm.client).GetState(
		context.Background(),
		dsm.stateStoreName,
		pageKey,
		nil,
	)

	if err != nil {
		return Page{}, fmt.Errorf("failed to get page from DAPR: %w", err)
	}

	if response.Value == nil {
		return Page{}, fmt.Errorf("page with ID %s not found", id)
	}

	var daprPage Page
	err = json.Unmarshal(response.Value, &daprPage)
	if err != nil {
		return Page{}, fmt.Errorf("failed to parse page data: %w", err)
	}

	// Cache the page in memory
	dsm.mutex.Lock()
	dsm.pageMap[id] = daprPage
	dsm.mutex.Unlock()

	return daprPage, nil
}

func (dsm *DaprStateManager) GetPreviousCrawls() ([]string, error) {
	// First try getting from the base state manager
	prevCrawls, err := dsm.BaseStateManager.GetPreviousCrawls()
	if err == nil && len(prevCrawls) > 0 {
		return prevCrawls, nil
	}

	// If not available, try reading from DAPR
	metadataKey := dsm.config.CrawlID // The key where metadata is stored
	response, err := (*dsm.client).GetState(
		context.Background(),
		dsm.stateStoreName,
		metadataKey,
		nil,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get metadata from DAPR: %w", err)
	}

	if response.Value == nil {
		return nil, nil // No metadata exists yet
	}

	var metadata CrawlMetadata
	err = json.Unmarshal(response.Value, &metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to parse metadata: %w", err)
	}

	return metadata.PreviousCrawlID, nil
}

// Initialize sets up the state in Dapr
func (dsm *DaprStateManager) Initialize(seedURLs []string) error {
	log.Info().Str("crawlID", dsm.config.CrawlID).Str("executionID", dsm.config.CrawlExecutionID).
		Msg("Initializing state manager")

	// First, check if we're resuming a specific execution ID
	// If so, try to load the layer map for this execution ID directly
	if dsm.config.CrawlExecutionID != "" && dsm.config.CrawlExecutionID != dsm.config.CrawlID {
		log.Info().Str("executionID", dsm.config.CrawlExecutionID).
			Msg("Resuming with specific execution ID - trying to load state")

		// First try to load the layer map for the specific execution ID
		// This is the most direct method when we know the execution ID
		layerMapKey := fmt.Sprintf("%s/layer_map", dsm.config.CrawlExecutionID)
		layerMapResponse, err := (*dsm.client).GetState(
			context.Background(),
			dsm.stateStoreName,
			layerMapKey,
			nil,
		)

		if err == nil && layerMapResponse.Value != nil && len(layerMapResponse.Value) > 0 {
			// Found layer map for this execution ID, load it
			err = json.Unmarshal(layerMapResponse.Value, &dsm.layerMap)
			if err != nil {
				log.Warn().Err(err).Str("executionID", dsm.config.CrawlExecutionID).
					Msg("Failed to parse layer map for execution ID, but continuing")
			} else {
				log.Info().Str("executionID", dsm.config.CrawlExecutionID).
					Int("layerCount", len(dsm.layerMap)).
					Msg("Successfully loaded layer map for execution ID")

				// Also load the URL cache
				err = dsm.loadURLsForCrawl(dsm.config.CrawlExecutionID)
				if err != nil {
					log.Warn().Err(err).Str("executionID", dsm.config.CrawlExecutionID).
						Msg("Failed to load URLs for execution ID, but continuing")
				}

				// Load all the pages from this layer map into memory with "unfetched" status
				err = dsm.loadPagesIntoMemory(dsm.config.CrawlExecutionID, false)
				if err != nil {
					log.Warn().Err(err).Str("executionID", dsm.config.CrawlExecutionID).
						Msg("Failed to load pages into memory for execution ID, but continuing")
				}

				// Try to load metadata too so we have complete state
				metadataKey := fmt.Sprintf("%s/metadata", dsm.config.CrawlID)
				metadataResponse, err := (*dsm.client).GetState(
					context.Background(),
					dsm.stateStoreName,
					metadataKey,
					nil,
				)

				if err == nil && metadataResponse.Value != nil {
					err = json.Unmarshal(metadataResponse.Value, &dsm.metadata)
					if err != nil {
						log.Warn().Err(err).Msg("Failed to parse metadata, but continuing")
					}
				}

				// After loading state, check if we have any pages at layer 0
				dsm.mutex.RLock()
				layerZeroPages, hasLayerZero := dsm.layerMap[0]
				dsm.mutex.RUnlock()

				if !hasLayerZero || len(layerZeroPages) == 0 {
					log.Warn().Msg("No layer 0 pages found in loaded state, will create from seed URLs")
				} else {
					log.Info().Int("pageCount", len(layerZeroPages)).Msg("Loaded layer 0 pages from previous execution")
					return nil
				}
			}
		} else {
			log.Warn().Str("executionID", dsm.config.CrawlExecutionID).
				Msg("Could not find layer map for execution ID directly, trying general initialization")
		}
	}

	// Regular initialization path - try to load metadata and layer structure
	// using crawl ID rather than execution ID
	metadataKey := fmt.Sprintf("%s/metadata", dsm.config.CrawlID)
	metadataResponse, err := (*dsm.client).GetState(
		context.Background(),
		dsm.stateStoreName,
		metadataKey,
		nil,
	)

	if err == nil && metadataResponse.Value != nil {
		// Metadata exists, load it
		err = json.Unmarshal(metadataResponse.Value, &dsm.metadata)
		if err != nil {
			return fmt.Errorf("failed to parse metadata: %w", err)
		}

		// If we're resuming a specific execution, prioritize that one
		var crawlIDsToTry []string
		if dsm.config.CrawlExecutionID != "" && dsm.config.CrawlExecutionID != dsm.config.CrawlID {
			// Put the execution ID first in the list to try
			crawlIDsToTry = append(crawlIDsToTry, dsm.config.CrawlExecutionID)
		}

		// Add all previous crawl IDs in reverse order (newest first)
		for i := len(dsm.metadata.PreviousCrawlID) - 1; i >= 0; i-- {
			crawlIDsToTry = append(crawlIDsToTry, dsm.metadata.PreviousCrawlID[i])
		}

		// Try each crawl ID until we find one with a valid layer map
		var loadedLayerMap bool = false
		for _, crawlID := range crawlIDsToTry {
			// Skip if this is a duplicate in our list
			if crawlID == dsm.config.CrawlExecutionID && crawlIDsToTry[0] == dsm.config.CrawlExecutionID {
				continue
			}

			// Load layer structure
			layerMapKey := fmt.Sprintf("%s/layer_map", crawlID)
			layerMapResponse, err := (*dsm.client).GetState(
				context.Background(),
				dsm.stateStoreName,
				layerMapKey,
				nil,
			)

			if err == nil && layerMapResponse.Value != nil && len(layerMapResponse.Value) > 0 {
				// Layer map exists and is not empty, load it
				err = json.Unmarshal(layerMapResponse.Value, &dsm.layerMap)
				if err != nil {
					log.Warn().Err(err).Str("crawlID", crawlID).Msg("Failed to parse layer map for crawl ID")
					continue // Try next crawl ID
				}

				// Load URL cache for this crawl
				err = dsm.loadURLsForCrawl(crawlID)
				if err != nil {
					log.Warn().Err(err).Str("crawlID", crawlID).Msg("Failed to load URLs for crawl")
					continue // Try next crawl ID
				}

				// Load all pages from this layer map into memory with reset status
				err = dsm.loadPagesIntoMemory(crawlID, false)
				if err != nil {
					log.Warn().Err(err).Str("crawlID", crawlID).Msg("Failed to load pages into memory")
					continue // Try next crawl ID
				}

				log.Info().Str("crawlID", crawlID).Msg("Successfully loaded existing state structure from DAPR")
				loadedLayerMap = true
				break // Successfully loaded a layer map, stop searching
			}
		}

		// No hardcoded fallback needed

		if !loadedLayerMap {
			log.Warn().Msg("Could not find any valid layer maps in previous crawls")
		} else {
			// Check if we have any layer 0 pages before returning
			dsm.mutex.RLock()
			layerZeroPages, hasLayerZero := dsm.layerMap[0]
			dsm.mutex.RUnlock()

			if !hasLayerZero || len(layerZeroPages) == 0 {
				log.Warn().Msg("No layer 0 pages found in loaded state, will create from seed URLs")
			} else {
				log.Info().Int("pageCount", len(layerZeroPages)).Msg("Successfully loaded layer 0 pages")
				return nil
			}
		}
	}

	// If we reached here, either:
	// 1. No previous state was found, or
	// 2. We found previous state but there are no layer 0 pages
	// So we need to initialize with seed URLs

	log.Info().Msg("Initializing with new seed URLs")

	// Load URLs from all previous crawls for deduplication
	err = dsm.initializeURLCache()
	if err != nil {
		log.Warn().Err(err).Msg("Failed to initialize URL cache from previous crawls")
	}

	// Filter out seed URLs that we've already seen in previous crawls
	dsm.urlCacheMutex.RLock()
	uniqueSeedURLs := make([]string, 0)
	for _, url := range seedURLs {
		// random-walk stuff
		// TODO: figure out best way to get crawl type in here
		if dsm.BaseStateManager.config.SamplingMethod == "random-walk" {
			log.Info().Str("url", url).Msg("random-walk: Adding seed url in Dapr Initialize")
			dsm.BaseStateManager.AddDiscoveredChannel(url)
		}
		if _, exists := dsm.urlCache[url]; !exists {
			uniqueSeedURLs = append(uniqueSeedURLs, url)
		} else {
			log.Debug().Str("url", url).Msg("Skipping seed URL already processed in previous crawl")
		}
	}
	dsm.urlCacheMutex.RUnlock()

	log.Info().Int("originalCount", len(seedURLs)).Int("uniqueCount", len(uniqueSeedURLs)).
		Msg("Filtered seed URLs against previous crawls")

	// If we have no unique seed URLs, it's probably a problem
	if len(uniqueSeedURLs) == 0 && len(seedURLs) > 0 {
		log.Warn().Msg("All seed URLs were filtered as duplicates - this may be unintended")
		// Use the original seed URLs anyway
		uniqueSeedURLs = seedURLs
	}

	// Create pages from the unique seed URLs
	pages := make([]Page, 0, len(uniqueSeedURLs))
	for _, url := range uniqueSeedURLs {
		pages = append(pages, Page{
			ID:        uuid.New().String(),
			URL:       url,
			Depth:     0,
			Status:    "unfetched",
			Timestamp: time.Now(),
		})
	}

	// In case it's a random-walk without seed urls
	if len(pages) == 0 && dsm.BaseStateManager.config.SamplingMethod == "random-walk" {
		log.Info().Msg("No seed urls provided for url. Creating layer from discovered channels")
		return nil
	}

	// In case we don't have any seed URLs, don't try to create an empty layer
	if len(pages) == 0 {
		log.Warn().Msg("No seed URLs available - cannot initialize crawler")
		return fmt.Errorf("cannot initialize crawler with zero seed URLs")
	}

	// Add initial pages
	if err := dsm.AddLayer(pages); err != nil {
		return fmt.Errorf("failed to add seed pages: %w", err)
	}

	// Save the state
	return dsm.SaveState()
}

func (dsm *DaprStateManager) initializeURLCache() error {
	// Get the list of previous crawl IDs
	previousCrawlIDs, err := dsm.GetPreviousCrawls()
	if err != nil {
		return fmt.Errorf("failed to get previous crawl IDs: %w", err)
	}

	// Load URLs from each previous crawl
	for _, crawlID := range previousCrawlIDs {
		err = dsm.loadURLsForCrawl(crawlID)
		if err != nil {
			log.Warn().Err(err).Str("crawlID", crawlID).Msg("Failed to load URLs for crawl")
		}
	}

	// Also load URLs from the current crawl if any exist
	err = dsm.loadURLsForCrawl(dsm.config.CrawlExecutionID)
	if err != nil {
		log.Warn().Err(err).Str("crawlID", dsm.config.CrawlID).Msg("Failed to load URLs for current crawl")
	}

	log.Info().Int("urlCount", len(dsm.urlCache)).Msg("URL cache initialized from previous crawls")
	return nil
}

func (dsm *DaprStateManager) loadURLsForCrawl(crawlID string) error {
	// Load the layer map for this crawl
	layerMapKey := fmt.Sprintf("%s/layer_map", crawlID)
	layerMapResponse, err := (*dsm.client).GetState(
		context.Background(),
		dsm.stateStoreName,
		layerMapKey,
		nil,
	)

	if err != nil || layerMapResponse.Value == nil {
		return fmt.Errorf("failed to load layer map for crawl %s: %w", crawlID, err)
	}

	var layerMap map[int][]string
	err = json.Unmarshal(layerMapResponse.Value, &layerMap)
	if err != nil {
		return fmt.Errorf("failed to parse layer map for crawl %s: %w", crawlID, err)
	}

	// Track how many URLs we add
	addedCount := 0

	// For each layer, get all page IDs
	for _, pageIDs := range layerMap {
		for _, pageID := range pageIDs {
			// Fetch the page from Dapr
			pageKey := fmt.Sprintf("%s/page/%s", crawlID, pageID)
			pageResponse, err := (*dsm.client).GetState(
				context.Background(),
				dsm.stateStoreName,
				pageKey,
				nil,
			)

			if err != nil || pageResponse.Value == nil {
				log.Warn().Err(err).Str("pageID", pageID).Str("crawlID", crawlID).Msg("Failed to fetch page")
				continue
			}

			var page Page
			err = json.Unmarshal(pageResponse.Value, &page)
			if err != nil {
				log.Warn().Err(err).Str("pageID", pageID).Str("crawlID", crawlID).Msg("Failed to parse page data")
				continue
			}

			// Add URL to cache
			dsm.urlCacheMutex.Lock()
			dsm.urlCache[page.URL] = fmt.Sprintf("%s:%s", crawlID, page.ID)
			dsm.urlCacheMutex.Unlock()

			addedCount++
		}
	}

	log.Debug().Str("crawlID", crawlID).Int("urlCount", addedCount).Msg("Loaded URLs for crawl")
	return nil
}

func (dsm *DaprStateManager) AddLayer(pages []Page) error {
	if len(pages) == 0 {
		return nil
	}

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Determine the depth
	depth := pages[0].Depth

	// First pass: filter out duplicate URLs and prepare pages to add
	// This minimizes the time we need to hold locks
	var pagesToAdd []Page
	duplicateCount := 0

	for i := range pages {
		if dsm.BaseStateManager.config.SamplingMethod != "random-walk" {
			// Check for duplicates using read lock on URL cache
			dsm.urlCacheMutex.RLock()
			existingID, exists := dsm.urlCache[pages[i].URL]
			dsm.urlCacheMutex.RUnlock()

			if exists {
				log.Debug().Msgf("Skipping duplicate URL: %s (already exists with ID: %s)", pages[i].URL, existingID)
				duplicateCount++
				continue
			}
		} else {
			log.Info().Msgf("random-walk: Sampling Method %s allows for duplicate visits to a URL. SKipping de-duplication process", dsm.BaseStateManager.config.SamplingMethod)
		}

		// Ensure the page has an ID
		if pages[i].ID == "" {
			pages[i].ID = uuid.New().String()
		}

		// Set timestamp if not already set
		if pages[i].Timestamp.IsZero() {
			pages[i].Timestamp = time.Now()
		}

		// Add to our list of pages to process
		pagesToAdd = append(pagesToAdd, pages[i])
	}

	// If all pages were duplicates, return early
	if len(pagesToAdd) == 0 {
		log.Debug().Msgf("All %d pages were duplicates, skipping layer addition", len(pages))
		return nil
	}

	// Create a wait group to track parallel page saves
	var eg errgroup.Group
	var addedIDs []string

	// Take a write lock to update in-memory structures
	dsm.mutex.Lock()

	// Initialize the layer if it doesn't exist
	if _, exists := dsm.layerMap[depth]; !exists {
		dsm.layerMap[depth] = make([]string, 0)
	}

	// Update in-memory structures for each page
	for i := range pagesToAdd {
		page := pagesToAdd[i]

		// Store the page in memory
		dsm.pageMap[page.ID] = page

		// Add to layer map
		dsm.layerMap[depth] = append(dsm.layerMap[depth], page.ID)

		// Track this ID as successfully added
		addedIDs = append(addedIDs, page.ID)

		// Capture the page for the closure
		pageCopy := page

		// Launch a goroutine to save this page to Dapr
		eg.Go(func() error {
			// Marshal page data for Dapr
			pageData, err := json.Marshal(pageCopy)
			if err != nil {
				return fmt.Errorf("failed to marshal page %s: %w", pageCopy.ID, err)
			}

			// Save page to Dapr
			pageKey := fmt.Sprintf("%s/page/%s", dsm.config.CrawlExecutionID, pageCopy.ID)
			err = (*dsm.client).SaveState(
				ctx,
				dsm.stateStoreName,
				pageKey,
				pageData,
				nil,
			)
			if err != nil {
				return fmt.Errorf("failed to save page %s to Dapr: %w", pageCopy.ID, err)
			}

			return nil
		})
	}

	// Update URL cache after processing - we do this separately to minimize lock contention
	dsm.urlCacheMutex.Lock()
	for _, page := range pagesToAdd {
		dsm.urlCache[page.URL] = fmt.Sprintf("%s:%s", dsm.config.CrawlExecutionID, page.ID)
	}
	dsm.urlCacheMutex.Unlock()

	// Release main lock
	dsm.mutex.Unlock()

	// Wait for all page saves to complete
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("error saving pages to Dapr: %w", err)
	}

	log.Debug().Msgf("Added %d unique pages to depth %d (filtered out %d duplicates from current and previous crawls)",
		len(addedIDs), depth, duplicateCount)

	// Save the updated layer structure
	return dsm.SaveState()
}

// UpdateCrawlMetadata updates the crawl metadata with the provided values
// and saves it to the Dapr state store in a non-blocking way
func (dsm *DaprStateManager) UpdateCrawlMetadata(crawlID string, metadata map[string]interface{}) error {
	// Create a context with timeout to prevent hanging
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// First update in-memory metadata using the base implementation
	// BaseStateManager already handles locking appropriately
	if err := dsm.BaseStateManager.UpdateCrawlMetadata(crawlID, metadata); err != nil {
		return err
	}

	// Make a clean copy of the metadata to work with
	var metadataCopy CrawlMetadata

	// Use an immediately-invoked function to scope the lock
	func() {
		// Use a read lock to copy the metadata
		dsm.mutex.RLock()
		defer dsm.mutex.RUnlock()

		// Deep copy the metadata
		metadataCopy = dsm.metadata
	}()

	// Add any missing fields outside the lock
	if metadataCopy.ExecutionID == "" {
		metadataCopy.ExecutionID = dsm.config.CrawlExecutionID
	}

	// If explicitly setting to completed, record end time if not set
	if status, ok := metadata["status"]; ok && status == "completed" && metadataCopy.EndTime.IsZero() {
		metadataCopy.EndTime = time.Now()
	}

	// Marshal metadata to JSON
	metadataData, err := json.Marshal(metadataCopy)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Use errgroup to perform save operations concurrently
	var eg errgroup.Group

	// 1. Save with formatted key
	metadataKey := fmt.Sprintf("%s/metadata", dsm.config.CrawlID)
	eg.Go(func() error {
		err := (*dsm.client).SaveState(ctx, dsm.stateStoreName, metadataKey, metadataData, nil)
		if err != nil {
			log.Error().Err(err).Str("key", metadataKey).Msg("Failed to save metadata with formatted key")
			return err
		}
		return nil
	})

	// 2. Save with direct crawl ID key
	eg.Go(func() error {
		err := (*dsm.client).SaveState(ctx, dsm.stateStoreName, dsm.config.CrawlID, metadataData, nil)
		if err != nil {
			log.Warn().Err(err).Str("key", dsm.config.CrawlID).Msg("Failed to save metadata with direct key")
		}
		// Continue even if this fails as it's redundant
		return nil
	})

	// 3. Save with execution ID key
	eg.Go(func() error {
		err := (*dsm.client).SaveState(ctx, dsm.stateStoreName, dsm.config.CrawlExecutionID, metadataData, nil)
		if err != nil {
			log.Warn().Err(err).Str("key", dsm.config.CrawlExecutionID).Msg("Failed to save metadata with execution ID")
		}
		// Continue even if this fails as it's redundant
		return nil
	})

	// Wait for all operations to complete, fail only if the primary one fails
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("failed to update metadata: %w", err)
	}

	log.Info().
		Str("crawlID", crawlID).
		Str("executionID", dsm.config.CrawlExecutionID).
		Str("status", metadataCopy.Status).
		Msg("Crawl metadata updated in DAPR")

	return nil
}

// SaveState persists the current state to Dapr
// This implementation minimizes lock contention by using goroutines for async writes
func (dsm *DaprStateManager) SaveState() error {
	// Use a context with timeout for all DAPR operations
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Copy all data we need to save under a short-lived lock
	var metadataCopy CrawlMetadata
	var layerMapCopy map[int][]string

	// Copy metadata with a read-write lock
	func() {
		dsm.mutex.Lock()
		defer dsm.mutex.Unlock()

		// Ensure execution ID is set
		if dsm.metadata.ExecutionID == "" {
			dsm.metadata.ExecutionID = dsm.config.CrawlExecutionID
		}

		// Ensure status is set - default to "running" unless explicitly completed
		if dsm.metadata.Status == "" {
			dsm.metadata.Status = "running"
		}

		// Ensure CrawlID is set
		if dsm.metadata.CrawlID == "" {
			dsm.metadata.CrawlID = dsm.config.CrawlID
		}

		// Create a deep copy of metadata
		metadataCopy = dsm.metadata

		// Create a deep copy of the layer map
		layerMapCopy = make(map[int][]string, len(dsm.layerMap))
		for depth, ids := range dsm.layerMap {
			// Make a copy of the slice
			idsCopy := make([]string, len(ids))
			copy(idsCopy, ids)
			layerMapCopy[depth] = idsCopy
		}
	}()

	// Now that we have copies of all data, release the lock and process asynchronously

	// Marshal metadata and layer map
	metadataData, err := json.Marshal(metadataCopy)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	layerMapData, err := json.Marshal(layerMapCopy)
	if err != nil {
		return fmt.Errorf("failed to marshal layer map: %w", err)
	}

	// Use errgroup to manage all concurrent DAPR operations
	var eg errgroup.Group

	// 1. Save metadata with different key formats
	metadataKey := fmt.Sprintf("%s/metadata", dsm.config.CrawlID)
	eg.Go(func() error {
		err := (*dsm.client).SaveState(ctx, dsm.stateStoreName, metadataKey, metadataData, nil)
		if err != nil {
			log.Error().Err(err).Str("key", metadataKey).Msg("Failed to save metadata with formatted key")
			return err
		}
		return nil
	})

	// Also save with direct crawl ID
	eg.Go(func() error {
		err := (*dsm.client).SaveState(ctx, dsm.stateStoreName, dsm.config.CrawlID, metadataData, nil)
		if err != nil {
			log.Warn().Err(err).Str("key", dsm.config.CrawlID).Msg("Failed to save metadata with direct key")
		}
		return nil // Non-critical, continue even if it fails
	})

	// Also save with execution ID
	eg.Go(func() error {
		err := (*dsm.client).SaveState(ctx, dsm.stateStoreName, dsm.config.CrawlExecutionID, metadataData, nil)
		if err != nil {
			log.Warn().Err(err).Str("key", dsm.config.CrawlExecutionID).Msg("Failed to save metadata with execution ID")
		}
		return nil // Non-critical, continue even if it fails
	})

	// 2. Save layer map data
	layerMapKey := fmt.Sprintf("%s/layer_map", dsm.config.CrawlExecutionID)
	eg.Go(func() error {
		err := (*dsm.client).SaveState(ctx, dsm.stateStoreName, layerMapKey, layerMapData, nil)
		if err != nil {
			log.Error().Err(err).Str("key", layerMapKey).Msg("Failed to save layer map")
			return err
		}
		return nil
	})

	// Save with alternate key format as well
	alternateLayerMapKey := fmt.Sprintf("%s/layer_map/%s", dsm.config.CrawlID, dsm.config.CrawlExecutionID)
	eg.Go(func() error {
		err := (*dsm.client).SaveState(ctx, dsm.stateStoreName, alternateLayerMapKey, layerMapData, nil)
		if err != nil {
			log.Warn().Err(err).Str("key", alternateLayerMapKey).Msg("Failed to save layer map with alternate key")
		}
		return nil // Non-critical, continue even if it fails
	})

	// 3. Save active crawl indicator
	activeKey := fmt.Sprintf("active_crawl/%s", dsm.config.CrawlID)
	activeData := []byte(fmt.Sprintf(`{"crawl_id":"%s","execution_id":"%s","timestamp":"%s"}`,
		dsm.config.CrawlID, dsm.config.CrawlExecutionID, time.Now().Format(time.RFC3339)))

	eg.Go(func() error {
		err := (*dsm.client).SaveState(ctx, dsm.stateStoreName, activeKey, activeData, nil)
		if err != nil {
			log.Warn().Err(err).Str("key", activeKey).Msg("Failed to save active crawl indicator")
		}
		return nil // Non-critical, continue even if it fails
	})

	// Wait for all operations to complete
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("failed to save state: %w", err)
	}

	log.Debug().Str("crawlID", dsm.config.CrawlID).Str("executionID", dsm.config.CrawlExecutionID).
		Msg("Successfully saved state to DAPR")
	return nil
}

// StorePost stores a post in Dapr
func (dsm *DaprStateManager) StorePost(channelID string, post model.Post) error {
	postData, err := json.Marshal(post)
	if err != nil {
		return fmt.Errorf("failed to marshal post: %w", err)
	}

	// Append newline for JSONL format
	postData = append(postData, '\n')

	// Create storage path
	storagePath, err := dsm.generateCrawlExecutableStoragePath(
		channelID,
		fmt.Sprintf("posts/%s.jsonl", post.PostUID),
	)
	if err != nil {
		return err
	}

	// Encode data for Dapr binding
	encodedData := base64.StdEncoding.EncodeToString(postData)
	key, err := fetchFileNamingComponent(*dsm.client, dsm.storageBinding)
	if err != nil {
		return err
	}
	// Prepare metadata
	metadata := map[string]string{
		key:         storagePath,
		"operation": "append",
	}
	// Send to Dapr binding
	req := daprc.InvokeBindingRequest{
		Name:      dsm.storageBinding,
		Operation: "create",
		Data:      []byte(encodedData),
		Metadata:  metadata,
	}

	log.Info().Msgf("Writing file to: %s", storagePath)
	_, err = (*dsm.client).InvokeBinding(context.Background(), &req)
	if err != nil {
		return fmt.Errorf("failed to store post via Dapr: %w", err)
	}

	log.Debug().Str("channel", channelID).Str("postUID", post.PostUID).Msg("Post stored")
	return nil
}

// StoreFile stores a file via Dapr
func (dsm *DaprStateManager) StoreFile(crawlId string, sourceFilePath string, fileName string) (string, string, error) {
	// Check if the file exists
	if _, err := os.Stat(sourceFilePath); os.IsNotExist(err) {
		return "", sourceFilePath, fmt.Errorf("source file does not exist: %w", err)
	}

	// Read file content
	fileContent, err := os.ReadFile(sourceFilePath)
	if err != nil {
		return "", sourceFilePath, fmt.Errorf("failed to read source file: %w", err)
	}

	if fileName == "" {
		fileName = filepath.Base(sourceFilePath)
	} else {
		// Get the extension from the source file and add it to the fileName if it doesn't already have it
		ext := filepath.Ext(sourceFilePath)
		if ext != "" && !strings.HasSuffix(fileName, ext) {
			fileName = fileName + ext
		}
	}

	// Create storage path for media
	storagePath, err := dsm.generateCrawlLevelStoragePath(fmt.Sprintf("media/%s", fileName))
	if err != nil {
		return storagePath, storagePath, err
	}

	// Encode data for Dapr binding
	encodedData := base64.StdEncoding.EncodeToString(fileContent)
	key, err := fetchFileNamingComponent(*dsm.client, dsm.storageBinding)
	if err != nil {
		return storagePath, storagePath, err
	}
	// Prepare metadata
	metadata := map[string]string{
		key:         storagePath,
		"operation": "create",
	}

	// Send to Dapr binding
	req := daprc.InvokeBindingRequest{
		Name:      dsm.storageBinding,
		Operation: "create",
		Data:      []byte(encodedData),
		Metadata:  metadata,
	}

	log.Info().Msgf("Writing file to: %s", storagePath)
	_, err = (*dsm.client).InvokeBinding(context.Background(), &req)
	if err != nil {
		return storagePath, storagePath, fmt.Errorf("failed to store file via Dapr: %w", err)
	}

	// Delete original file after successful upload
	err = os.Remove(sourceFilePath)
	if err != nil {
		log.Warn().Err(err).Str("path", sourceFilePath).Msg("Failed to delete source file after upload")
	}

	return storagePath, storagePath, nil
}

// HasProcessedMedia checks if media has been processed before using the sharded cache architecture
func (dsm *DaprStateManager) HasProcessedMedia(mediaID string) (bool, error) {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// For backward compatibility - check the old cache format first
	dsm.mediaCacheMutex.RLock()
	if _, exists := dsm.mediaCache[mediaID]; exists {
		dsm.mediaCacheMutex.RUnlock()
		return true, nil
	}
	dsm.mediaCacheMutex.RUnlock()

	// Check the index to find which shard has the media ID
	dsm.mediaCacheIndexMutex.RLock()
	shardID, exists := dsm.mediaCacheIndex.MediaIndex[mediaID]
	dsm.mediaCacheIndexMutex.RUnlock()

	// If found in the index, look for it in the corresponding shard
	if exists {
		// Check if the shard is already loaded in memory
		dsm.mediaCacheIndexMutex.RLock()
		shard, shardLoaded := dsm.mediaCacheShards[shardID]
		dsm.mediaCacheIndexMutex.RUnlock()

		// If shard is not in memory, load it from Dapr
		if !shardLoaded {
			err := dsm.loadMediaCacheShard(shardID)
			if err != nil {
				log.Warn().Err(err).Str("shardID", shardID).Str("mediaID", mediaID).
					Msg("Failed to load media cache shard, checking other shards")
			} else {
				dsm.mediaCacheIndexMutex.RLock()
				shard = dsm.mediaCacheShards[shardID]
				dsm.mediaCacheIndexMutex.RUnlock()
			}
		}

		// If we have the shard, check if the media ID exists in it
		if shard != nil {
			_, itemExists := shard.Items[mediaID]
			return itemExists, nil
		}
	}

	// If not found in the index or shard, check if it's in the active cache
	dsm.mediaCacheIndexMutex.RLock()
	_, activeExists := dsm.activeMediaCache.Items[mediaID]
	dsm.mediaCacheIndexMutex.RUnlock()

	if activeExists {
		return true, nil
	}

	// As a last resort, check all loaded shards - this handles cases where the index might be out of sync
	var found bool
	dsm.mediaCacheIndexMutex.RLock()
	for _, shard := range dsm.mediaCacheShards {
		if _, exists := shard.Items[mediaID]; exists {
			found = true
			break
		}
	}
	dsm.mediaCacheIndexMutex.RUnlock()

	// If still not found, try loading from the old format in Dapr
	if !found {
		cacheKey := dsm.getMediaCacheKey()
		response, err := (*dsm.client).GetState(
			ctx,
			dsm.stateStoreName,
			cacheKey,
			nil,
		)

		if err != nil {
			return false, fmt.Errorf("failed to get legacy media cache from Dapr: %w", err)
		}

		if response.Value == nil {
			// No legacy cache exists
			return false, nil
		}

		// Parse the legacy cache
		var cacheItems map[string]MediaCacheItem
		err = json.Unmarshal(response.Value, &cacheItems)
		if err != nil {
			return false, fmt.Errorf("failed to parse legacy media cache: %w", err)
		}

		// Check if the media ID exists in the legacy cache
		if _, exists := cacheItems[mediaID]; exists {
			// Found in legacy cache, migrate this entry to the active shard
			dsm.migrateMediaCacheItem(mediaID, cacheItems[mediaID])
			return true, nil
		}
	}

	return found, nil
}

// migrateMediaCacheItem moves an item from the old cache format to the new sharded format
func (dsm *DaprStateManager) migrateMediaCacheItem(mediaID string, item MediaCacheItem) {
	dsm.mediaCacheIndexMutex.Lock()
	defer dsm.mediaCacheIndexMutex.Unlock()

	// Add to active shard
	dsm.activeMediaCache.Items[mediaID] = item
	dsm.activeMediaCache.UpdateTime = time.Now()

	// Update the index
	dsm.mediaCacheIndex.MediaIndex[mediaID] = dsm.activeMediaCache.CacheID
	dsm.mediaCacheIndex.UpdateTime = time.Now()

	log.Debug().Str("mediaID", mediaID).Msg("Migrated media cache item from legacy format to sharded cache")
}

func (dsm *DaprStateManager) UpdatePage(page Page) error {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Make a copy of the page first
	pageCopy := page

	// First update in memory
	err := dsm.BaseStateManager.UpdatePage(pageCopy)
	if err != nil {
		return err
	}

	// Marshal page data outside any locks
	pageData, err := json.Marshal(pageCopy)
	if err != nil {
		return fmt.Errorf("failed to marshal page: %w", err)
	}

	// Save to DAPR asynchronously
	pageKey := fmt.Sprintf("%s/page/%s", dsm.config.CrawlExecutionID, pageCopy.ID)

	// Save the page to the state store
	err = (*dsm.client).SaveState(
		ctx,
		dsm.stateStoreName,
		pageKey,
		pageData,
		nil,
	)

	if err != nil {
		return fmt.Errorf("failed to save page to DAPR: %w", err)
	}

	return nil
}

// MarkMediaAsProcessed marks media as processed using the sharded cache architecture
func (dsm *DaprStateManager) MarkMediaAsProcessed(mediaID string) error {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// For backward compatibility - also add to the old cache format
	dsm.mediaCacheMutex.Lock()
	dsm.mediaCache[mediaID] = MediaCacheItem{
		ID:        mediaID,
		FirstSeen: time.Now(),
	}
	dsm.mediaCacheMutex.Unlock()

	// Create the new cache item
	item := MediaCacheItem{
		ID:        mediaID,
		FirstSeen: time.Now(),
	}

	// Add to the sharded cache system
	return dsm.addMediaToCacheWithSharding(ctx, mediaID, item)
}

// addMediaToCacheWithSharding handles adding a media item to the sharded cache system
func (dsm *DaprStateManager) addMediaToCacheWithSharding(ctx context.Context, mediaID string, item MediaCacheItem) error {
	dsm.mediaCacheIndexMutex.Lock()

	// Check if we need to create a new shard (current shard is at or near capacity)
	if len(dsm.activeMediaCache.Items) >= dsm.maxCacheItemsPerShard {
		// Create a deep copy of the active cache before saving
		activeCacheCopy := MediaCache{
			Items:      make(map[string]MediaCacheItem, len(dsm.activeMediaCache.Items)),
			UpdateTime: dsm.activeMediaCache.UpdateTime,
			CacheID:    dsm.activeMediaCache.CacheID,
		}

		// Copy all items to the new map
		for k, v := range dsm.activeMediaCache.Items {
			activeCacheCopy.Items[k] = v
		}

		// Create a new shard with a fresh UUID for the active cache
		newShardID := uuid.New().String()
		dsm.activeMediaCache = MediaCache{
			Items:      make(map[string]MediaCacheItem),
			UpdateTime: time.Now(),
			CacheID:    newShardID,
		}

		// Add new shard to the index
		dsm.mediaCacheIndex.Shards = append(dsm.mediaCacheIndex.Shards, newShardID)

		// Log the shard creation
		log.Info().
			Str("newShardID", newShardID).
			Int("totalShards", len(dsm.mediaCacheIndex.Shards)).
			Msg("Created new media cache shard after reaching capacity limit")

		// Unlock before IO operations
		dsm.mediaCacheIndexMutex.Unlock()

		// Save the copy outside the lock
		err := dsm.saveMediaCacheShard(&activeCacheCopy)
		if err != nil {
			log.Warn().Err(err).Msg("Failed to save full media cache shard")
			// Continue despite error - we'll retry later
		}

		// Reacquire the lock
		dsm.mediaCacheIndexMutex.Lock()
	}

	// Add the item to the active shard
	dsm.activeMediaCache.Items[mediaID] = item
	dsm.activeMediaCache.UpdateTime = time.Now()

	// Update the index to point to the correct shard
	dsm.mediaCacheIndex.MediaIndex[mediaID] = dsm.activeMediaCache.CacheID
	dsm.mediaCacheIndex.UpdateTime = time.Now()

	// Periodically save active shard (every 20 items instead of 100 to reduce potential data loss)
	shouldSave := len(dsm.activeMediaCache.Items)%20 == 0

	// Periodically clean up stale cache entries (every 500 items)
	shouldCleanup := len(dsm.activeMediaCache.Items)%500 == 0

	// Make a deep copy of active shard if we need to save it
	var activeCacheCopy MediaCache
	if shouldSave {
		activeCacheCopy = MediaCache{
			Items:      make(map[string]MediaCacheItem, len(dsm.activeMediaCache.Items)),
			UpdateTime: dsm.activeMediaCache.UpdateTime,
			CacheID:    dsm.activeMediaCache.CacheID,
		}

		// Copy all items to the new map
		for k, v := range dsm.activeMediaCache.Items {
			activeCacheCopy.Items[k] = v
		}
	}

	// Release the lock before any IO operations
	dsm.mediaCacheIndexMutex.Unlock()

	// Execute these operations outside the lock to improve concurrency
	if shouldSave {
		// Save the copy we created inside the lock
		err := dsm.saveMediaCacheShard(&activeCacheCopy)
		if err != nil {
			log.Warn().Err(err).Msg("Failed to save active media cache shard")
		}

		// Also save the index
		err = dsm.saveMediaCacheIndex()
		if err != nil {
			log.Warn().Err(err).Msg("Failed to save media cache index")
		}
	}

	// Perform cache cleanup if needed
	if shouldCleanup {
		go dsm.cleanupStaleMediaCacheEntries(ctx)
	}

	return nil
}

// cleanupStaleMediaCacheEntries removes old entries from the cache
func (dsm *DaprStateManager) cleanupStaleMediaCacheEntries(ctx context.Context) {
	// Define the expiration cutoff time
	cutoffTime := time.Now().AddDate(0, 0, -dsm.cacheExpirationDays)
	log.Debug().Time("cutoffTime", cutoffTime).Msg("Starting stale media cache cleanup")

	// Keep track of shards that were modified and need to be saved
	modifiedShards := make(map[string]bool)

	// Take a write lock on the index
	dsm.mediaCacheIndexMutex.Lock()

	// First clean up the active shard
	initialActiveCount := len(dsm.activeMediaCache.Items)
	for mediaID, item := range dsm.activeMediaCache.Items {
		if item.FirstSeen.Before(cutoffTime) {
			delete(dsm.activeMediaCache.Items, mediaID)
			delete(dsm.mediaCacheIndex.MediaIndex, mediaID)
		}
	}
	finalActiveCount := len(dsm.activeMediaCache.Items)

	if initialActiveCount > finalActiveCount {
		modifiedShards[dsm.activeMediaCache.CacheID] = true
		log.Debug().
			Int("initialCount", initialActiveCount).
			Int("finalCount", finalActiveCount).
			Str("shardID", dsm.activeMediaCache.CacheID).
			Msg("Cleaned up expired entries from active shard")
	}

	// Clean up loaded shards
	for shardID, shard := range dsm.mediaCacheShards {
		initialCount := len(shard.Items)
		for mediaID, item := range shard.Items {
			if item.FirstSeen.Before(cutoffTime) {
				delete(shard.Items, mediaID)
				delete(dsm.mediaCacheIndex.MediaIndex, mediaID)
			}
		}

		finalCount := len(shard.Items)
		if initialCount > finalCount {
			modifiedShards[shardID] = true
			log.Debug().
				Int("initialCount", initialCount).
				Int("finalCount", finalCount).
				Str("shardID", shardID).
				Msg("Cleaned up expired entries from shard")
		}
	}

	// Check if any shards are now empty and can be removed
	var nonEmptyShards []string
	for _, shardID := range dsm.mediaCacheIndex.Shards {
		// Skip the active shard - never remove it
		if shardID == dsm.activeMediaCache.CacheID {
			nonEmptyShards = append(nonEmptyShards, shardID)
			continue
		}

		// Check if shard is in memory
		shard, exists := dsm.mediaCacheShards[shardID]
		if exists {
			// Keep non-empty shards
			if len(shard.Items) > 0 {
				nonEmptyShards = append(nonEmptyShards, shardID)
			} else {
				// Remove empty shard from memory
				delete(dsm.mediaCacheShards, shardID)
				log.Debug().Str("shardID", shardID).Msg("Removing empty shard from memory")

				// Add to modified list so we delete from Dapr
				modifiedShards[shardID] = true
			}
		} else {
			// If not in memory, keep it in the list for now
			// We could fetch to check if empty, but that's expensive
			nonEmptyShards = append(nonEmptyShards, shardID)
		}
	}

	// Update the shards list if changed
	if len(nonEmptyShards) < len(dsm.mediaCacheIndex.Shards) {
		dsm.mediaCacheIndex.Shards = nonEmptyShards
		dsm.mediaCacheIndex.UpdateTime = time.Now()
	}

	// Make a copy of modified shards and the index for saving after releasing lock
	var shardsToSave []*MediaCache
	if len(modifiedShards) > 0 {
		for shardID := range modifiedShards {
			if shardID == dsm.activeMediaCache.CacheID {
				// Make a copy of active shard
				activeCopy := dsm.activeMediaCache
				shardsToSave = append(shardsToSave, &activeCopy)
			} else if shard, exists := dsm.mediaCacheShards[shardID]; exists {
				shardsToSave = append(shardsToSave, shard)
			}
		}
	}

	// Make index copy
	indexCopy := dsm.mediaCacheIndex

	// Release the lock before any IO operations
	dsm.mediaCacheIndexMutex.Unlock()

	// Save modified shards and index
	for _, shard := range shardsToSave {
		// If shard is empty, delete it from Dapr
		if len(shard.Items) == 0 {
			shardKey := dsm.getShardKey(shard.CacheID)
			err := (*dsm.client).DeleteState(ctx, dsm.stateStoreName, shardKey, nil)
			if err != nil {
				log.Warn().Err(err).Str("shardID", shard.CacheID).Msg("Failed to delete empty shard from Dapr")
			} else {
				log.Debug().Str("shardID", shard.CacheID).Msg("Deleted empty shard from Dapr")
			}
		} else {
			// Otherwise save the updated shard
			err := dsm.saveMediaCacheShard(shard)
			if err != nil {
				log.Warn().Err(err).Str("shardID", shard.CacheID).Msg("Failed to save updated shard during cleanup")
			}
		}
	}

	// Save the updated index if it was modified
	if len(modifiedShards) > 0 {
		err := dsm.saveMediaCacheIndex()
		if err != nil {
			log.Warn().Err(err).Msg("Failed to save media cache index during cleanup")
		}
	}

	log.Info().
		Int("modifiedShards", len(modifiedShards)).
		Int("currentShardCount", len(indexCopy.Shards)).
		Msg("Completed media cache cleanup")
}

// Close releases resources and ensures all data is persisted
func (dsm *DaprStateManager) Close() error {
	// Save the current media cache state before closing
	// This ensures any unsaved cache data is persisted

	log.Info().Msg("Performing final media cache save during shutdown")

	// Save the active media cache shard
	activeCacheCopy := dsm.activeMediaCache
	if err := dsm.saveMediaCacheShard(&activeCacheCopy); err != nil {
		log.Warn().Err(err).Msg("Failed to save active media cache shard during shutdown")
	}

	// Save the media cache index
	if err := dsm.saveMediaCacheIndex(); err != nil {
		log.Warn().Err(err).Msg("Failed to save media cache index during shutdown")
	}

	log.Info().Msg("Media cache successfully saved during shutdown")
	return nil
}

// FindIncompleteCrawl looks for an incomplete crawl with the given crawl ID
// in the Dapr state store and returns its execution ID if found
func (dsm *DaprStateManager) FindIncompleteCrawl(crawlID string) (string, bool, error) {
	// Skip the base implementation check which only works for in-memory state
	// Instead, directly check the persisted state in Dapr

	log.Info().Str("crawlID", crawlID).Msg("Checking for incomplete crawl in Dapr state store")

	// Try all possible variations of metadata keys
	// First try the direct key format
	metadataKey := fmt.Sprintf("%s/metadata", crawlID)
	log.Debug().Str("lookingForKey", metadataKey).Msg("Checking metadata key")

	// Also look for the crawl ID directly
	response, err := (*dsm.client).GetState(
		context.Background(),
		dsm.stateStoreName,
		crawlID,
		nil,
	)

	// If direct crawl ID has data, check that first
	if err == nil && response.Value != nil {
		log.Info().Str("crawlID", crawlID).Msg("Found direct crawl ID data")
		var metadata CrawlMetadata
		err = json.Unmarshal(response.Value, &metadata)
		if err == nil && metadata.ExecutionID != "" {
			// Check if the crawl is completed - never return completed crawls
			if metadata.Status == "completed" {
				log.Info().
					Str("crawlID", crawlID).
					Str("executionID", metadata.ExecutionID).
					Msg("Skipping completed crawl from direct key")
				return "", false, nil
			} else {
				// Only consider incomplete crawls
				log.Info().
					Str("crawlID", crawlID).
					Str("executionID", metadata.ExecutionID).
					Str("status", metadata.Status).
					Msg("Found incomplete crawl with direct key")

				// Verify that the execution ID has a layer map before returning it
				layerMapKey := fmt.Sprintf("%s/layer_map", metadata.ExecutionID)
				layerMapResponse, lmErr := (*dsm.client).GetState(
					context.Background(),
					dsm.stateStoreName,
					layerMapKey,
					nil,
				)

				if lmErr == nil && layerMapResponse.Value != nil && len(layerMapResponse.Value) > 0 {
					log.Info().
						Str("executionID", metadata.ExecutionID).
						Msg("Verified layer map exists for execution ID")
					return metadata.ExecutionID, true, nil
				} else {
					log.Warn().
						Str("executionID", metadata.ExecutionID).
						Err(lmErr).
						Msg("Found execution ID but it has no layer map, continuing search")
				}
			}
		}
	}

	// Now check the formatted metadata key
	response, err = (*dsm.client).GetState(
		context.Background(),
		dsm.stateStoreName,
		metadataKey,
		nil,
	)

	// If we got metadata for this crawl ID
	if err == nil && response.Value != nil {
		var metadata CrawlMetadata
		err = json.Unmarshal(response.Value, &metadata)
		if err == nil {
			// Skip if metadata shows completed status
			if metadata.Status == "completed" {
				log.Info().
					Str("crawlID", crawlID).
					Str("executionID", metadata.ExecutionID).
					Msg("Skipping metadata with completed status")
			} else if metadata.ExecutionID != "" {
				// Only check for layer map if status is not completed
				// Verify the layer map exists
				layerMapKey := fmt.Sprintf("%s/layer_map", metadata.ExecutionID)
				layerMapResponse, lmErr := (*dsm.client).GetState(
					context.Background(),
					dsm.stateStoreName,
					layerMapKey,
					nil,
				)

				if lmErr == nil && layerMapResponse.Value != nil && len(layerMapResponse.Value) > 0 {
					log.Info().
						Str("crawlID", crawlID).
						Str("executionID", metadata.ExecutionID).
						Str("status", metadata.Status).
						Msg("Found incomplete crawl with layer map")
					return metadata.ExecutionID, true, nil
				} else {
					log.Warn().
						Str("executionID", metadata.ExecutionID).
						Err(lmErr).
						Msg("Found incomplete execution ID in metadata but it has no layer map")
				}
			}

			// Check each of the previous executions in reverse order (newest first)
			// to find the most recent incomplete one

			// First check the newest execution's metadata to see if it's completed
			// If the newest execution is completed, we should stop searching entirely
			if len(metadata.PreviousCrawlID) > 0 {
				newestExecID := metadata.PreviousCrawlID[len(metadata.PreviousCrawlID)-1]
				newestMetadataKey := fmt.Sprintf("%s/metadata", newestExecID)
				newestMetadataResponse, err := (*dsm.client).GetState(
					context.Background(),
					dsm.stateStoreName,
					newestMetadataKey,
					nil,
				)

				// Check if the newest execution is completed
				if err == nil && newestMetadataResponse.Value != nil {
					var newestMetadata CrawlMetadata
					if jsonErr := json.Unmarshal(newestMetadataResponse.Value, &newestMetadata); jsonErr == nil {
						if newestMetadata.Status == "completed" {
							log.Info().
								Str("crawlID", crawlID).
								Str("newestExecutionID", newestExecID).
								Msg("Latest execution is marked as completed, not checking older executions")
							return "", false, nil
						}
					}
				}
			}

			// If we get here, the newest execution is not marked as completed
			// or we couldn't determine its status, so check all executions
			for i := len(metadata.PreviousCrawlID) - 1; i >= 0; i-- {
				prevExecID := metadata.PreviousCrawlID[i]

				// See if this execution has pages that can be processed
				layerMapKey := fmt.Sprintf("%s/layer_map", prevExecID)
				layerMapResponse, err := (*dsm.client).GetState(
					context.Background(),
					dsm.stateStoreName,
					layerMapKey,
					nil,
				)

				if err != nil || layerMapResponse.Value == nil || len(layerMapResponse.Value) == 0 {
					log.Warn().Err(err).Str("executionID", prevExecID).Msg("Failed to load layer map for execution")
					continue
				}

				// Verify the layer map has at least one page
				var layerMap map[int][]string
				err = json.Unmarshal(layerMapResponse.Value, &layerMap)
				if err != nil {
					log.Warn().Err(err).Str("executionID", prevExecID).Msg("Failed to parse layer map")
					continue
				}

				// Make sure there's at least one layer with pages
				hasPages := false
				for _, pageIDs := range layerMap {
					if len(pageIDs) > 0 {
						hasPages = true
						break
					}
				}

				if !hasPages {
					log.Warn().Str("executionID", prevExecID).Msg("Layer map exists but has no pages")
					continue
				}

				// Get this execution's specific metadata
				execMetadataKey := fmt.Sprintf("%s/metadata", prevExecID)
				execMetadataResponse, err := (*dsm.client).GetState(
					context.Background(),
					dsm.stateStoreName,
					execMetadataKey,
					nil,
				)

				// If we can find metadata, check its status
				// Completed crawls should NEVER be returned
				var isCompleted bool = false
				if err == nil && execMetadataResponse.Value != nil {
					var execMetadata CrawlMetadata
					err = json.Unmarshal(execMetadataResponse.Value, &execMetadata)
					if err == nil {
						// Check if the execution is marked as completed
						if execMetadata.Status == "completed" {
							log.Info().
								Str("crawlID", crawlID).
								Str("executionID", prevExecID).
								Msg("Skipping execution marked as completed")
							isCompleted = true
						} else {
							// If this execution isn't marked as completed, use it
							log.Info().
								Str("crawlID", crawlID).
								Str("executionID", prevExecID).
								Str("status", execMetadata.Status).
								Msg("Found incomplete previous execution with valid layer map")
							return prevExecID, true, nil
						}
					}
				}

				// Skip completed executions entirely, regardless of page status
				if isCompleted {
					log.Debug().
						Str("executionID", prevExecID).
						Msg("Skipping completed execution, even if it has incomplete pages")
					continue
				}

				// If we couldn't find metadata or status wasn't "completed",
				// check if there are any incomplete pages
				hasIncompletePages, err := dsm.checkForIncompletePages(prevExecID)
				if err != nil {
					log.Warn().Err(err).Str("executionID", prevExecID).Msg("Error checking for incomplete pages")
					continue
				}

				if hasIncompletePages {
					log.Info().
						Str("crawlID", crawlID).
						Str("executionID", prevExecID).
						Msg("Found execution with incomplete pages and valid layer map")
					return prevExecID, true, nil
				}

				// If we get here, the execution has a complete layer map with pages,
				// but all pages are processed or the execution is marked as completed.
				log.Info().
					Str("crawlID", crawlID).
					Str("executionID", prevExecID).
					Msg("Found previous execution with valid layer map but all pages are completed")

				// Do NOT use completed crawls as fallbacks
				// Skip this execution and continue looking for truly incomplete ones
			}
		} else {
			log.Warn().Err(err).Str("crawlID", crawlID).Msg("Failed to parse crawl metadata")
		}
	}

	// If we reach here, we need to check for any executions related to this crawl ID
	// by looking for active crawl indicators and direct layer maps

	// Check for active crawl indicator
	activeKey := fmt.Sprintf("active_crawl/%s", crawlID)
	activeResponse, err := (*dsm.client).GetState(
		context.Background(),
		dsm.stateStoreName,
		activeKey,
		nil,
	)

	if err == nil && activeResponse.Value != nil {
		log.Info().Str("crawlID", crawlID).Msg("Found active crawl indicator")
		// Parse the active crawl data to get the execution ID
		var activeData struct {
			CrawlID     string `json:"crawl_id"`
			ExecutionID string `json:"execution_id"`
			Timestamp   string `json:"timestamp"`
		}

		if err := json.Unmarshal(activeResponse.Value, &activeData); err == nil && activeData.ExecutionID != "" {
			// Check if this active execution is already marked as completed
			// by looking up its metadata
			execMetadataKey := fmt.Sprintf("%s/metadata", activeData.ExecutionID)
			execMetadataResponse, metaErr := (*dsm.client).GetState(
				context.Background(),
				dsm.stateStoreName,
				execMetadataKey,
				nil,
			)

			var isCompleted bool = false
			if metaErr == nil && execMetadataResponse.Value != nil {
				var execMetadata CrawlMetadata
				if jsonErr := json.Unmarshal(execMetadataResponse.Value, &execMetadata); jsonErr == nil {
					if execMetadata.Status == "completed" {
						isCompleted = true
						log.Info().
							Str("crawlID", crawlID).
							Str("executionID", activeData.ExecutionID).
							Msg("Skipping active crawl marker that points to a completed execution")
					}
				}
			}

			// Only proceed with non-completed executions
			if !isCompleted {
				// Verify it has a layer map
				layerMapKey := fmt.Sprintf("%s/layer_map", activeData.ExecutionID)
				layerMapResponse, lmErr := (*dsm.client).GetState(
					context.Background(),
					dsm.stateStoreName,
					layerMapKey,
					nil,
				)

				if lmErr == nil && layerMapResponse.Value != nil && len(layerMapResponse.Value) > 0 {
					log.Info().
						Str("crawlID", crawlID).
						Str("executionID", activeData.ExecutionID).
						Str("timestamp", activeData.Timestamp).
						Msg("Found active crawl with execution ID and valid layer map")
					return activeData.ExecutionID, true, nil
				} else {
					log.Warn().
						Str("executionID", activeData.ExecutionID).
						Err(lmErr).
						Msg("Found active crawl but it has no layer map")
				}
			}
		}

		// Check if crawl ID itself is marked as completed
		crawlMetadataKey := fmt.Sprintf("%s/metadata", crawlID)
		crawlMetadataResponse, metaErr := (*dsm.client).GetState(
			context.Background(),
			dsm.stateStoreName,
			crawlMetadataKey,
			nil,
		)

		if metaErr == nil && crawlMetadataResponse.Value != nil {
			var crawlMetadata CrawlMetadata
			if json.Unmarshal(crawlMetadataResponse.Value, &crawlMetadata) == nil {
				if crawlMetadata.Status == "completed" {
					log.Info().
						Str("crawlID", crawlID).
						Msg("Skipping crawl ID itself as it is marked as completed")
					return "", false, nil
				}
			}
		}

		// If we can't parse it or there's no layer map but the active crawl exists and isn't completed,
		// use the crawl ID as the execution ID as a fallback
		log.Info().
			Str("crawlID", crawlID).
			Msg("Using crawl ID as execution ID (fallback) after verifying it's not completed")
		return crawlID, true, nil
	}

	// Check for alternate layer map formats with the direct crawl ID
	layerMapKey := fmt.Sprintf("%s/layer_map", crawlID)
	layerMapResponse, err := (*dsm.client).GetState(
		context.Background(),
		dsm.stateStoreName,
		layerMapKey,
		nil,
	)

	if err == nil && layerMapResponse.Value != nil && len(layerMapResponse.Value) > 0 {
		// Verify it has pages
		var layerMap map[int][]string
		err = json.Unmarshal(layerMapResponse.Value, &layerMap)
		if err == nil {
			// Check if there are any pages in the layer map
			hasPages := false
			for _, pageIDs := range layerMap {
				if len(pageIDs) > 0 {
					hasPages = true
					break
				}
			}

			if hasPages {
				// Before returning, check if the crawl is marked as completed
				crawlMetadataKey := fmt.Sprintf("%s/metadata", crawlID)
				metadataResponse, metaErr := (*dsm.client).GetState(
					context.Background(),
					dsm.stateStoreName,
					crawlMetadataKey,
					nil,
				)

				// Check for completed status
				if metaErr == nil && metadataResponse.Value != nil {
					var metadata CrawlMetadata
					if jsonErr := json.Unmarshal(metadataResponse.Value, &metadata); jsonErr == nil {
						if metadata.Status == "completed" {
							log.Info().
								Str("crawlID", crawlID).
								Msg("Found layer map with pages, but crawl is marked as completed")
							return "", false, nil
						}
					}
				}

				log.Info().Str("crawlID", crawlID).Msg("Found layer map with pages for crawl ID")
				// If we have a layer map with pages but no execution ID, use the crawl ID itself
				return crawlID, true, nil
			} else {
				log.Warn().Str("crawlID", crawlID).Msg("Found layer map but it has no pages")
			}
		}
	}

	// Finally check any generic state key format
	stateKey := fmt.Sprintf("%s/state", crawlID)
	stateResponse, err := (*dsm.client).GetState(
		context.Background(),
		dsm.stateStoreName,
		stateKey,
		nil,
	)

	if err == nil && stateResponse.Value != nil {
		log.Info().Str("crawlID", crawlID).Msg("Found state data for crawl ID")
		// One more attempt - check if this crawl ID itself has a layer map
		layerMapKey := fmt.Sprintf("%s/layer_map", crawlID)
		layerMapResponse, lmErr := (*dsm.client).GetState(
			context.Background(),
			dsm.stateStoreName,
			layerMapKey,
			nil,
		)

		if lmErr == nil && layerMapResponse.Value != nil && len(layerMapResponse.Value) > 0 {
			log.Info().Str("crawlID", crawlID).Msg("Found state and layer map using crawl ID")
			return crawlID, true, nil
		}

		// If we have state but no layer map, not usable
		log.Warn().Str("crawlID", crawlID).
			Msg("Found state data but no layer map for crawl ID")
	}

	// Last resort - check one more common format
	crawlExecKey := fmt.Sprintf("crawl/%s", crawlID)
	crawlExecResponse, err := (*dsm.client).GetState(
		context.Background(),
		dsm.stateStoreName,
		crawlExecKey,
		nil,
	)

	if err == nil && crawlExecResponse.Value != nil {
		// Try to extract an execution ID
		var execData struct {
			ExecutionID string `json:"execution_id"`
		}

		if err := json.Unmarshal(crawlExecResponse.Value, &execData); err == nil && execData.ExecutionID != "" {
			// Verify it has a layer map
			layerMapKey := fmt.Sprintf("%s/layer_map", execData.ExecutionID)
			layerMapResponse, lmErr := (*dsm.client).GetState(
				context.Background(),
				dsm.stateStoreName,
				layerMapKey,
				nil,
			)

			if lmErr == nil && layerMapResponse.Value != nil && len(layerMapResponse.Value) > 0 {
				log.Info().
					Str("crawlID", crawlID).
					Str("executionID", execData.ExecutionID).
					Msg("Found execution ID with layer map in crawl key format")
				return execData.ExecutionID, true, nil
			}
		}

		// Last attempt - check if crawl ID itself has a layer map
		layerMapKey := fmt.Sprintf("%s/layer_map", crawlID)
		layerMapResponse, lmErr := (*dsm.client).GetState(
			context.Background(),
			dsm.stateStoreName,
			layerMapKey,
			nil,
		)

		if lmErr == nil && layerMapResponse.Value != nil && len(layerMapResponse.Value) > 0 {
			log.Info().Str("crawlID", crawlID).Msg("Found layer map using crawl ID as fallback")
			return crawlID, true, nil
		}
	}

	// Exhausted all possibilities, no incomplete crawl found
	log.Info().Str("crawlID", crawlID).Msg("No incomplete crawl found after exhaustive search")
	return "", false, nil
}

// checkForIncompletePages checks if an execution has any pages that weren't successfully processed
func (dsm *DaprStateManager) checkForIncompletePages(executionID string) (bool, error) {
	// Fetch the layer map for this execution
	layerMapKey := fmt.Sprintf("%s/layer_map", executionID)
	layerMapResponse, err := (*dsm.client).GetState(
		context.Background(),
		dsm.stateStoreName,
		layerMapKey,
		nil,
	)

	if err != nil || layerMapResponse.Value == nil {
		return false, fmt.Errorf("failed to load layer map: %w", err)
	}

	// Parse the layer map
	var layerMap map[string][]string
	err = json.Unmarshal(layerMapResponse.Value, &layerMap)
	if err != nil {
		return false, fmt.Errorf("failed to parse layer map: %w", err)
	}

	// Check each layer for pages
	for depth, pageIDs := range layerMap {
		for _, pageID := range pageIDs {
			// Get the page data
			pageKey := fmt.Sprintf("%s/page/%s", executionID, pageID)
			pageResponse, err := (*dsm.client).GetState(
				context.Background(),
				dsm.stateStoreName,
				pageKey,
				nil,
			)

			if err != nil || pageResponse.Value == nil {
				log.Warn().Err(err).Str("pageID", pageID).Str("depth", depth).Msg("Failed to load page data")
				continue
			}

			var page Page
			err = json.Unmarshal(pageResponse.Value, &page)
			if err != nil {
				log.Warn().Err(err).Str("pageID", pageID).Msg("Failed to parse page data")
				continue
			}

			// If the page isn't marked as fetched, this execution has incomplete pages
			if page.Status != "fetched" {
				log.Debug().
					Str("executionID", executionID).
					Str("pageID", pageID).
					Str("status", page.Status).
					Msg("Found incomplete page")
				return true, nil
			}
		}
	}

	return false, nil
}

// Helper methods

// loadStateForCrawl loads the state for a specific crawl ID
func (dsm *DaprStateManager) loadStateForCrawl(crawlID string) (State, error) {
	stateKey := dsm.getStateKey(crawlID)

	response, err := (*dsm.client).GetState(
		context.Background(),
		dsm.stateStoreName,
		stateKey,
		nil,
	)

	if err != nil {
		return State{}, fmt.Errorf("failed to get state from Dapr: %w", err)
	}

	if response.Value == nil {
		return State{}, errors.New("no state found for crawl ID")
	}

	var state State
	err = json.Unmarshal(response.Value, &state)
	if err != nil {
		return State{}, fmt.Errorf("failed to unmarshal state: %w", err)
	}

	return state, nil
}

// getStateKey generates a key for the state in Dapr
func (dsm *DaprStateManager) getStateKey(crawlID string) string {
	return fmt.Sprintf("state/%s", crawlID)
}

// getMediaCacheKey generates a key for the media cache in Dapr
func (dsm *DaprStateManager) getMediaCacheKey() string {
	return fmt.Sprintf("%s/media-cache", dsm.config.CrawlID)
}

// getMediaCacheIndexKey generates a key for the media cache index in Dapr
func (dsm *DaprStateManager) getMediaCacheIndexKey() string {
	return fmt.Sprintf("%s/media-cache-index", dsm.config.CrawlID)
}

// getShardKey generates a key for a specific media cache shard in Dapr
func (dsm *DaprStateManager) getShardKey(shardID string) string {
	return fmt.Sprintf("%s/media-cache-shard/%s", dsm.config.CrawlID, shardID)
}

// loadMediaCacheIndex loads the media cache index from Dapr
func (dsm *DaprStateManager) loadMediaCacheIndex() error {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Get the index key
	indexKey := dsm.getMediaCacheIndexKey()

	// Load the index from Dapr
	response, err := (*dsm.client).GetState(
		ctx,
		dsm.stateStoreName,
		indexKey,
		nil,
	)

	if err != nil {
		return fmt.Errorf("failed to get media cache index from Dapr: %w", err)
	}

	// If no index exists yet, check if we need to migrate from old format
	if response.Value == nil {
		log.Debug().Msg("No media cache index found in Dapr, checking for legacy format")

		// Check if old format exists and migrate if needed
		migrated, err := dsm.migrateFromLegacyCache(ctx)
		if err != nil {
			log.Warn().Err(err).Msg("Failed to check for legacy cache format")
		} else if migrated {
			log.Info().Msg("Successfully migrated from legacy cache format")
			return nil
		}

		log.Debug().Msg("No legacy format found, using new empty index")
		return nil
	}

	// Parse the index
	var index MediaCacheIndex
	err = json.Unmarshal(response.Value, &index)
	if err != nil {
		return fmt.Errorf("failed to parse media cache index: %w", err)
	}

	// Update our instance with the loaded index
	dsm.mediaCacheIndexMutex.Lock()
	defer dsm.mediaCacheIndexMutex.Unlock()

	dsm.mediaCacheIndex = index

	// Load the active shard (the last one in the list)
	if len(index.Shards) > 0 {
		activeCacheID := index.Shards[len(index.Shards)-1]
		err = dsm.loadMediaCacheShard(activeCacheID)
		if err != nil {
			log.Warn().Err(err).Str("shardID", activeCacheID).Msg("Failed to load active media cache shard")
		} else {
			// Set the loaded shard as active if available
			if shard, exists := dsm.mediaCacheShards[activeCacheID]; exists {
				dsm.activeMediaCache = *shard
			}
		}
	}

	log.Info().
		Int("indexSize", len(index.MediaIndex)).
		Int("shardCount", len(index.Shards)).
		Msg("Media cache index loaded from Dapr")

	return nil
}

// migrateFromLegacyCache migrates data from the old monolithic cache to the new sharded format
func (dsm *DaprStateManager) migrateFromLegacyCache(ctx context.Context) (bool, error) {
	cacheKey := dsm.getMediaCacheKey()
	response, err := (*dsm.client).GetState(
		ctx,
		dsm.stateStoreName,
		cacheKey,
		nil,
	)

	if err != nil {
		return false, fmt.Errorf("failed to check for legacy cache: %w", err)
	}

	if response.Value == nil {
		// No legacy cache exists
		return false, nil
	}

	// Parse the legacy cache
	var legacyCache map[string]MediaCacheItem
	err = json.Unmarshal(response.Value, &legacyCache)
	if err != nil {
		return false, fmt.Errorf("failed to parse legacy cache: %w", err)
	}

	// If no items, nothing to migrate
	if len(legacyCache) == 0 {
		return false, nil
	}

	log.Info().Int("itemCount", len(legacyCache)).Msg("Found legacy media cache, starting migration to sharded format")

	// Initialize the migration
	dsm.mediaCacheIndexMutex.Lock()

	// Create a new batch counter to track when to save
	batchCount := 0
	batchSize := 500 // Save every 500 items

	// For each item in the legacy cache
	for mediaID, item := range legacyCache {
		// Add to the active shard
		dsm.activeMediaCache.Items[mediaID] = item

		// Update the index
		dsm.mediaCacheIndex.MediaIndex[mediaID] = dsm.activeMediaCache.CacheID

		// Increment batch counter
		batchCount++

		// If we've reached the shard capacity, save and create a new shard
		if len(dsm.activeMediaCache.Items) >= dsm.maxCacheItemsPerShard {
			// Update timestamp
			dsm.activeMediaCache.UpdateTime = time.Now()

			// Make a copy to save
			shardToSave := dsm.activeMediaCache

			// Create a new shard
			newShardID := uuid.New().String()
			dsm.activeMediaCache = MediaCache{
				Items:      make(map[string]MediaCacheItem),
				UpdateTime: time.Now(),
				CacheID:    newShardID,
			}

			// Add new shard to the index
			dsm.mediaCacheIndex.Shards = append(dsm.mediaCacheIndex.Shards, newShardID)

			// Release lock temporarily for IO
			dsm.mediaCacheIndexMutex.Unlock()

			// Save the full shard
			err = dsm.saveMediaCacheShard(&shardToSave)
			if err != nil {
				log.Warn().Err(err).Str("shardID", shardToSave.CacheID).Msg("Failed to save shard during migration")
			}

			// Save the index periodically
			if batchCount >= batchSize {
				err = dsm.saveMediaCacheIndex()
				if err != nil {
					log.Warn().Err(err).Msg("Failed to save index during migration")
				}
				batchCount = 0
			}

			// Re-acquire lock
			dsm.mediaCacheIndexMutex.Lock()

			log.Info().
				Str("oldShardID", shardToSave.CacheID).
				Str("newShardID", newShardID).
				Int("itemCount", len(shardToSave.Items)).
				Msg("Created new shard during migration after reaching capacity")
		}
	}

	// Update timestamps
	dsm.activeMediaCache.UpdateTime = time.Now()
	dsm.mediaCacheIndex.UpdateTime = time.Now()

	// Release lock for final save operations
	dsm.mediaCacheIndexMutex.Unlock()

	// Save the final active shard
	err = dsm.saveMediaCacheShard(&dsm.activeMediaCache)
	if err != nil {
		log.Warn().Err(err).Str("shardID", dsm.activeMediaCache.CacheID).Msg("Failed to save final active shard during migration")
	}

	// Save the final index
	err = dsm.saveMediaCacheIndex()
	if err != nil {
		log.Warn().Err(err).Msg("Failed to save final index during migration")
	}

	// Optionally: rename the old cache to avoid reprocessing if this runs again
	backupKey := fmt.Sprintf("%s-backup-%s", cacheKey, time.Now().Format("20060102-150405"))
	backupErr := (*dsm.client).SaveState(
		ctx,
		dsm.stateStoreName,
		backupKey,
		response.Value,
		nil,
	)

	if backupErr != nil {
		log.Warn().Err(backupErr).Msg("Failed to backup legacy cache")
	} else {
		// Delete the old cache now that we've backed it up
		deleteErr := (*dsm.client).DeleteState(ctx, dsm.stateStoreName, cacheKey, nil)
		if deleteErr != nil {
			log.Warn().Err(deleteErr).Msg("Failed to delete legacy cache after migration")
		}
	}

	log.Info().
		Int("totalItems", len(legacyCache)).
		Int("shards", len(dsm.mediaCacheIndex.Shards)).
		Msg("Completed migration from legacy cache to sharded format")

	return true, nil
}

// loadMediaCacheShard loads a specific media cache shard from Dapr
func (dsm *DaprStateManager) loadMediaCacheShard(shardID string) error {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Get the shard key
	shardKey := dsm.getShardKey(shardID)

	// Load the shard from Dapr
	response, err := (*dsm.client).GetState(
		ctx,
		dsm.stateStoreName,
		shardKey,
		nil,
	)

	if err != nil {
		return fmt.Errorf("failed to get media cache shard from Dapr: %w", err)
	}

	if response.Value == nil {
		return fmt.Errorf("shard %s not found in Dapr", shardID)
	}

	// Parse the shard
	var shard MediaCache
	err = json.Unmarshal(response.Value, &shard)
	if err != nil {
		return fmt.Errorf("failed to parse media cache shard: %w", err)
	}

	// Add to our cache
	dsm.mediaCacheShards[shardID] = &shard

	log.Debug().
		Str("shardID", shardID).
		Int("itemCount", len(shard.Items)).
		Time("updateTime", shard.UpdateTime).
		Msg("Media cache shard loaded from Dapr")

	return nil
}

// saveMediaCacheIndex saves the media cache index to Dapr
func (dsm *DaprStateManager) saveMediaCacheIndex() error {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create a deep copy of the media cache index to avoid concurrent access issues
	dsm.mediaCacheIndexMutex.RLock()
	indexCopy := MediaCacheIndex{
		Shards:     make([]string, len(dsm.mediaCacheIndex.Shards)),
		MediaIndex: make(map[string]string, len(dsm.mediaCacheIndex.MediaIndex)),
		UpdateTime: time.Now(), // Set current time for the update
	}

	// Copy the slices and maps
	copy(indexCopy.Shards, dsm.mediaCacheIndex.Shards)
	for k, v := range dsm.mediaCacheIndex.MediaIndex {
		indexCopy.MediaIndex[k] = v
	}

	// Get counts for logging
	indexSize := len(indexCopy.MediaIndex)
	shardCount := len(indexCopy.Shards)
	dsm.mediaCacheIndexMutex.RUnlock()

	// Marshal the index copy (safe since it's our own copy)
	indexData, err := json.Marshal(indexCopy)
	if err != nil {
		return fmt.Errorf("failed to marshal media cache index: %w", err)
	}

	// Save to Dapr
	indexKey := dsm.getMediaCacheIndexKey()
	err = (*dsm.client).SaveState(
		ctx,
		dsm.stateStoreName,
		indexKey,
		indexData,
		nil,
	)

	if err != nil {
		return fmt.Errorf("failed to save media cache index to Dapr: %w", err)
	}

	log.Debug().
		Int("indexSize", indexSize).
		Int("shardCount", shardCount).
		Msg("Media cache index saved to Dapr")

	return nil
}

// saveMediaCacheShard saves a specific media cache shard to Dapr
// Important: This function expects that the MediaCache passed is a copy that won't be modified
// while this function executes. The caller must ensure proper synchronization.
func (dsm *DaprStateManager) saveMediaCacheShard(shard *MediaCache) error {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Update timestamp (safe since we're working with a copy)
	shard.UpdateTime = time.Now()

	// Get item count for logging before marshaling
	itemCount := len(shard.Items)
	shardID := shard.CacheID

	// Marshal the shard - this is safe because we're working with an independent copy
	shardData, err := json.Marshal(shard)
	if err != nil {
		return fmt.Errorf("failed to marshal media cache shard: %w", err)
	}

	// Save to Dapr
	shardKey := dsm.getShardKey(shardID)
	err = (*dsm.client).SaveState(
		ctx,
		dsm.stateStoreName,
		shardKey,
		shardData,
		nil,
	)

	if err != nil {
		return fmt.Errorf("failed to save media cache shard to Dapr: %w", err)
	}

	log.Debug().
		Str("shardID", shardID).
		Int("itemCount", itemCount).
		Msg("Media cache shard saved to Dapr")

	return nil
}

// generateStoragePath creates a standardized storage path
func (dsm *DaprStateManager) generateStoragePath(channelID, subPath string) (string, error) {
	return fmt.Sprintf(
		"%s/%s/%s/%s",
		dsm.config.StorageRoot,
		dsm.config.CrawlID,
		channelID,
		subPath,
	), nil
}

func (dsm *DaprStateManager) generateCrawlLevelStoragePath(subPath string) (string, error) {
	return fmt.Sprintf(
		"%s/%s/%s",
		dsm.config.StorageRoot,
		dsm.config.CrawlID,
		subPath,
	), nil
}

// generateStoragePath creates a standardized storage path
func (dsm *DaprStateManager) generateCrawlExecutableStoragePath(channelID, subPath string) (string, error) {
	return fmt.Sprintf(
		"%s/%s/%s/%s/%s",
		dsm.config.StorageRoot,
		dsm.config.CrawlID,
		dsm.config.CrawlExecutionID,
		channelID,
		subPath,
	), nil
}

func (dsm *DaprStateManager) loadURLsFromPreviousCrawls() (map[string]string, error) {
	urlMap := make(map[string]string)

	// Get the list of previous crawl IDs
	previousCrawlIDs, err := dsm.GetPreviousCrawls()
	if err != nil {
		return urlMap, fmt.Errorf("failed to get previous crawl IDs: %w", err)
	}

	// Include the current crawl ID
	crawlIDs := append(previousCrawlIDs, dsm.config.CrawlExecutionID)

	for _, crawlID := range crawlIDs {
		// For each crawl ID, load its layer map
		layerMapKey := fmt.Sprintf("%s/layer_map", crawlID)
		layerMapResponse, err := (*dsm.client).GetState(
			context.Background(),
			dsm.stateStoreName,
			layerMapKey,
			nil,
		)

		if err != nil || layerMapResponse.Value == nil {
			log.Warn().Err(err).Str("crawlID", crawlID).Msg("Failed to load layer map for previous crawl")
			continue
		}

		var layerMap map[int][]string
		err = json.Unmarshal(layerMapResponse.Value, &layerMap)
		if err != nil {
			log.Warn().Err(err).Str("crawlID", crawlID).Msg("Failed to parse layer map for previous crawl")
			continue
		}

		// For each layer, get all page IDs
		for _, pageIDs := range layerMap {
			for _, pageID := range pageIDs {
				// Fetch the page from Dapr
				pageKey := fmt.Sprintf("%s/page/%s", crawlID, pageID)
				pageResponse, err := (*dsm.client).GetState(
					context.Background(),
					dsm.stateStoreName,
					pageKey,
					nil,
				)

				if err != nil || pageResponse.Value == nil {
					log.Warn().Err(err).Str("pageID", pageID).Str("crawlID", crawlID).Msg("Failed to fetch page from previous crawl")
					continue
				}

				var page Page
				err = json.Unmarshal(pageResponse.Value, &page)
				if err != nil {
					log.Warn().Err(err).Str("pageID", pageID).Str("crawlID", crawlID).Msg("Failed to parse page data from previous crawl")
					continue
				}

				// Add URL to deduplication map
				urlMap[page.URL] = fmt.Sprintf("%s:%s", crawlID, page.ID) // Store crawlID:pageID to know where it came from
			}
		}
	}

	log.Info().Int("urlCount", len(urlMap)).Msg("Loaded URLs from previous crawls for deduplication")
	return urlMap, nil
}

func (dsm *DaprStateManager) loadPagesIntoMemory(crawlID string, cont bool) error {
	loadedCount := 0

	// Check if we're resuming the same crawl execution
	// We consider it the same execution if we're using the exact same executionID
	isResumingSameCrawlExecution := dsm.config.CrawlExecutionID == crawlID
	log.Info().
		Bool("isResumingSameCrawlExecution", isResumingSameCrawlExecution).
		Str("configExecutionID", dsm.config.CrawlExecutionID).
		Str("loadingCrawlID", crawlID).
		Msg("Loading pages with execution context")

	// For each layer in the layer map
	for _, pageIDs := range dsm.layerMap {
		// For each page ID in the layer
		for _, pageID := range pageIDs {
			// Check if we already have this page in memory
			if _, exists := dsm.pageMap[pageID]; exists {
				continue
			}

			// Fetch the page from Dapr
			pageKey := fmt.Sprintf("%s/page/%s", crawlID, pageID)
			pageResponse, err := (*dsm.client).GetState(
				context.Background(),
				dsm.stateStoreName,
				pageKey,
				nil,
			)

			if err != nil || pageResponse.Value == nil {
				log.Warn().Err(err).Str("pageID", pageID).Str("crawlID", crawlID).Msg("Failed to fetch page")
				continue
			}

			var page Page
			err = json.Unmarshal(pageResponse.Value, &page)
			if err != nil {
				log.Warn().Err(err).Str("pageID", pageID).Str("crawlID", crawlID).Msg("Failed to parse page data")
				continue
			}

			// Add page to in-memory page map
			if !cont {
				if !isResumingSameCrawlExecution {
					// When not resuming the same execution, mark ALL pages as unfetched
					// to ensure they get reprocessed with the new execution ID
					oldStatus := page.Status
					page.Status = "unfetched"
					log.Debug().
						Str("pageID", pageID).
						Str("url", page.URL).
						Str("old_status", oldStatus).
						Bool("resuming_same_execution", isResumingSameCrawlExecution).
						Msg("New execution: Marking page as unfetched regardless of previous status")
				} else {
					// When resuming same execution, preserve fetched status when loading from DAPR
					if page.Status != "fetched" {
						page.Status = "unfetched"
						log.Debug().
							Str("pageID", pageID).
							Str("url", page.URL).
							Str("old_status", page.Status).
							Bool("resuming_same_execution", isResumingSameCrawlExecution).
							Msg("Resetting non-fetched page status to unfetched in same execution")
					} else {
						log.Debug().
							Str("pageID", pageID).
							Str("url", page.URL).
							Bool("resuming_same_execution", isResumingSameCrawlExecution).
							Msg("Preserving fetched status from Dapr storage in same execution")
					}
				}
				page.Timestamp = time.Now()
			}

			// Clear any previous messages or processing data
			page.Messages = []Message{}

			// Update the timestamp to current time
			dsm.pageMap[pageID] = page
			loadedCount++
		}
	}

	log.Info().
		Str("crawlID", crawlID).
		Int("pageCount", loadedCount).
		Bool("resuming_same_execution", isResumingSameCrawlExecution).
		Msg("Loaded pages into memory from DAPR")
	return nil
}

func (dsm *DaprStateManager) ExportPagesToBinding(crawlID string) error {
	exportedCount := 0

	// Get timestamp for filename
	timestamp := time.Now().Format("20060102-150405")
	filename := fmt.Sprintf("channel-pages-%s-%s.jsonl", crawlID, timestamp)

	// Create storage path
	storagePath, err := dsm.generateStoragePath(
		"analysis", // Using "analysis" instead of channel ID
		fmt.Sprintf("channels/%s", filename),
	)
	if err != nil {
		return fmt.Errorf("failed to generate storage path: %w", err)
	}

	// Prepare for collecting all data as JSONL
	var allPagesData []byte

	// For each layer in the layer map
	for _, pageIDs := range dsm.layerMap {
		// For each page ID in the layer
		for _, pageID := range pageIDs {
			// Fetch the page from Dapr or memory
			var page Page

			// First try from memory
			dsm.mutex.RLock()
			existingPage, exists := dsm.pageMap[pageID]
			dsm.mutex.RUnlock()

			if exists {
				page = existingPage
			} else {
				// If not in memory, fetch from Dapr
				pageKey := fmt.Sprintf("%s/page/%s", crawlID, pageID)
				pageResponse, err := (*dsm.client).GetState(
					context.Background(),
					dsm.stateStoreName,
					pageKey,
					nil,
				)

				if err != nil || pageResponse.Value == nil {
					log.Warn().Err(err).Str("pageID", pageID).Str("crawlID", crawlID).Msg("Failed to fetch page")
					continue
				}

				err = json.Unmarshal(pageResponse.Value, &page)
				if err != nil {
					log.Warn().Err(err).Str("pageID", pageID).Str("crawlID", crawlID).Msg("Failed to parse page data")
					continue
				}
			}

			// Extract channel ID - customize this based on your page structure
			channelID := page.URL
			if channelID == "" {
				channelID = "unknown" // Default for pages without channel ID
			}

			// Marshal page to JSON and append to our data buffer with newline
			pageData, err := json.Marshal(page)
			if err != nil {
				log.Warn().Err(err).Str("pageID", pageID).Msg("Failed to marshal page data")
				continue
			}

			// Append newline for JSONL format
			pageData = append(pageData, '\n')
			allPagesData = append(allPagesData, pageData...)

			exportedCount++
		}
	}

	// If we didn't export any pages, return early
	if exportedCount == 0 {
		return fmt.Errorf("no pages found to export for crawl ID: %s", crawlID)
	}

	// Encode data for Dapr binding
	encodedData := base64.StdEncoding.EncodeToString(allPagesData)
	key, err := fetchFileNamingComponent(*dsm.client, dsm.storageBinding)
	if err != nil {
		return err
	}
	// Prepare metadata for the binding
	metadata := map[string]string{
		key:         storagePath,
		"operation": "create",
	}

	log.Info().Msgf("Writing file to: %s", storagePath)
	// Send to Dapr binding
	req := daprc.InvokeBindingRequest{
		Name:      dsm.storageBinding,
		Operation: "create",
		Data:      []byte(encodedData),
		Metadata:  metadata,
	}

	_, err = (*dsm.client).InvokeBinding(context.Background(), &req)
	if err != nil {
		return fmt.Errorf("failed to store pages via Dapr binding: %w", err)
	}

	return nil
}

func (dsm *DaprStateManager) GetLayerByDepth(depth int) ([]Page, error) {
	// First, try to get pages from memory using the base implementation
	pages, err := dsm.BaseStateManager.GetLayerByDepth(depth)

	// If we got pages, return them
	if err == nil && len(pages) > 0 {
		return pages, nil
	}

	// If not successful, let's check if layer exists in Dapr
	dsm.mutex.RLock()
	ids, exists := dsm.layerMap[depth]
	dsm.mutex.RUnlock()

	if !exists {
		return []Page{}, nil // Layer doesn't exist
	}

	// Layer exists but pages might not be in memory, fetch them from Dapr
	pages = make([]Page, 0, len(ids))
	for _, id := range ids {
		// Try to get page from Dapr
		page, err := dsm.GetPage(id)
		if err != nil {
			log.Warn().Err(err).Str("pageID", id).Int("depth", depth).Msg("Failed to fetch page for layer")
			continue
		}

		// Preserve the "fetched" status from Dapr storage
		// Only reset non-fetched pages to "unfetched" status
		if page.Status != "unfetched" && page.Status != "fetched" {
			page.Status = "unfetched"
			page.Messages = []Message{}
			page.Timestamp = time.Now()

			// Save the updated page back to memory
			dsm.mutex.Lock()
			dsm.pageMap[id] = page
			dsm.mutex.Unlock()
		} else if page.Status == "fetched" {
			// When status is "fetched", preserve it in memory
			// This prevents re-processing pages that were already completed
			log.Debug().
				Str("pageID", id).
				Str("url", page.URL).
				Msg("Preserving fetched status from Dapr storage")

			// Still update memory cache with the fetched page
			dsm.mutex.Lock()
			dsm.pageMap[id] = page
			dsm.mutex.Unlock()

			// No need to save to Dapr here as it's a retrieval operation
			// The status will be saved when the page is processed
		}

		pages = append(pages, page)
	}

	return pages, nil
}

func (dsm *DaprStateManager) GetRandomDiscoveredChannel() (string, error) {
	return dsm.BaseStateManager.GetRandomDiscoveredChannel()
}

func (dsm *DaprStateManager) IsDiscoveredChannel(channelID string) bool {
	return dsm.BaseStateManager.IsDiscoveredChannel(channelID)
}

func (dsm *DaprStateManager) AddDiscoveredChannel(channelID string) error {
	return dsm.BaseStateManager.AddDiscoveredChannel(channelID)
}

func (dsm *DaprStateManager) SaveEdgeRecords(edges []*EdgeRecord) error {
	// Create a context with timeout to prevent hanging
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Add Edges in Memory
	baseErr := dsm.BaseStateManager.SaveEdgeRecords(edges)
	if baseErr != nil {
		log.Error().Err(baseErr).Msg("random-walk: Failed to add edge records")
		return baseErr
	}
	log.Info().Int("new_edges", len(edges)).Int("total_edges", len(dsm.BaseStateManager.edgeRecords)).
		Msg("random-walk: Adding new edges")
		// // use mutex to avoid writes to edge records during marshalling
		// dsm.BaseStateManager.mutex.Lock()
		// edgeData := map[string]any{
		// 	"edgeRecords": dsm.BaseStateManager.edgeRecords,
		// }
		// edgeByteData, err := json.MarshalIndent(edgeData, "", "  ")
		// dsm.BaseStateManager.mutex.Unlock()
		// if err != nil {
		// 	log.Error().Err(err).Msg("random-walk: Failed to marshall edge data")
		// 	return err
		// }
		// edgeKey := fmt.Sprintf("%s/discoveredEdges", dsm.config.CrawlID)
		// // log.Info().Str("state_store", dsm.stateStoreName).Str("key", edgeKey).Int("bytes", len(edgeData)).
		// log.Info().Str("state_store", dsm.stateStoreName).Str("key", edgeKey).Int("bytes", len(edgeByteData)).
		// 	Msg("random-walk: Saving edges to dapr now")
		// // err = (*dsm.client).SaveState(ctx, dsm.stateStoreName, edgeKey, edgeData, nil)
		// err = (*dsm.client).SaveState(ctx, dsm.stateStoreName, edgeKey, edgeByteData, nil)
		// if err != nil {
		// 	log.Warn().Err(err).Str("key", edgeKey).Msg("Failed to save edge data")
		// 	return err
		// }

		// var (
		// 	queryBuilder strings.Builder
		// 	values       []interface{}
		// 	placeholder  int
		// )

		// queryBuilder.WriteString(`INSERT INTO edge_records (destination_channel, source_channel, walkback, skipped, discovery_time, crawl_id) VALUES `)

		// // Add a placeholder for each record.
		// for i, record := range edges {
		// 	if i > 0 {
		// 		queryBuilder.WriteString(", ")
		// 	}
		// 	queryBuilder.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d)",
		// 		placeholder+1, placeholder+2, placeholder+3, placeholder+4, placeholder+5, placeholder+6))

		// 	values = append(values, record.DestinationChannel, record.SourceChannel, record.Walkback, record.Skipped, record.DiscoveryTime, dsm.config.CrawlID)
		// 	placeholder += 6
		// }

		// queryBuilder.WriteString(";")

		// // Marshal the slice of interface{} into JSON bytes.
		// jsonData, err := json.Marshal(values)
		// if err != nil {
		// 	return fmt.Errorf("failed to marshal parameters to JSON: %w", err)
		// }

		// log.Info().Str("sql_query", queryBuilder.String()).Msg("random-walk: Adding edges")

		// req := daprc.InvokeBindingRequest{
		// 	Name:      dsm.databaseBinding,
		// 	Operation: "exec", // The operation to execute a SQL query.
		// 	Metadata: map[string]string{
		// 		"sql": queryBuilder.String(),
		// 	},
		// 	Data: jsonData,
		// }

		// sqlQuery := `INSERT INTO edge_records (destination_channel, source_channel, walkback, skipped, discovery_time, crawl_id)
		//              VALUES ($1, $2, $3, $4, $5, $6);`

		// // Create a type to represent a single operation for Dapr's bulk format.
		// type sqlOperation struct {
		// 	SQL    string `json:"sql"`
		// 	Params []any  `json:"params"`
		// }

		// // 1. Build a slice of operations, one for each record.
		// operations := make([]sqlOperation, 0, len(edges))
		// for _, record := range edges {
		// 	operations = append(operations, sqlOperation{
		// 		SQL: sqlQuery,
		// 		Params: []any{
		// 			record.DestinationChannel,
		// 			record.SourceChannel,
		// 			record.Walkback,
		// 			record.Skipped,
		// 			record.DiscoveryTime,
		// 			dsm.config.CrawlID,
		// 		},
		// 	})
		// }

		// jsonData, err := json.Marshal(operations)
		// if err != nil {
		// 	return fmt.Errorf("random-walk: failed to marshal bulk operations to JSON: %w", err)
		// }

		// req := &daprc.InvokeBindingRequest{
		// 	Name:      dsm.databaseBinding,
		// 	Operation: "exec",
		// 	Data:      jsonData,
		// }

		// log.Info().Int("record_count", len(edges)).Msg("random-walk: Adding edges in bulk")

	sqlQuery := `INSERT INTO edge_records (destination_channel, source_channel, walkback, skipped, discovery_time, crawl_id) 
                 VALUES ($1, $2, $3, $4, $5, $6);`

	for _, record := range edges {
		values := []any{
			record.DestinationChannel,
			record.SourceChannel,
			record.Walkback,
			record.Skipped,
			record.DiscoveryTime,
			dsm.config.CrawlID,
		}

		jsonData, err := json.Marshal(values)
		if err != nil {
			return fmt.Errorf("random-walk: failed to marshal record to JSON: %w", err)
		}

		req := &daprc.InvokeBindingRequest{
			Name:      dsm.databaseBinding,
			Operation: "exec",
			Metadata: map[string]string{
				"sql":    sqlQuery,
				"params": string(jsonData),
			},
		}

		log.Info().Str("source_channel", record.SourceChannel).Str("destination_channel", record.DestinationChannel).
			Msg("random-walk: adding edge record")
		if _, err := (*dsm.client).InvokeBinding(ctx, req); err != nil {
			return fmt.Errorf("random-walk: failed to invoke Dapr binding: %w", err)
		}
	}

	return nil
}

func (dsm *DaprStateManager) InitializeDiscoveredChannels() error {
	// TODO: add to config
	dsm.databaseBinding = databaseStorageBinding

	query := "SELECT source_channel FROM edge_records UNION SELECT destination_channel FROM edge_records"
	req := daprc.InvokeBindingRequest{
		Name:      dsm.databaseBinding,
		Operation: "query",
		Data:      nil,
		Metadata: map[string]string{
			"sql": query,
		},
	}
	res, err := (*dsm.client).InvokeBinding(context.Background(), &req)
	if err != nil {
		return fmt.Errorf("random-walk: failed to query discovered channels: %w", err)
	}
	var discoveredChannels []string

	if err := json.Unmarshal(res.Data, &discoveredChannels); err != nil {
		return fmt.Errorf("random-walk: Failed to unmarshal response for discovered channels: %v", err)
	}

	if len(discoveredChannels) > 0 {
		log.Printf("random-walk: Found %d previously discovered channels:\n", len(discoveredChannels))
		for _, channel := range discoveredChannels {
			dsm.BaseStateManager.AddDiscoveredChannel(channel)
		}
	} else {
		log.Info().Msg("random-walk: No discovered channels found")
	}

	return nil
}

func (dsm *DaprStateManager) InitializeRandomWalkLayer() error {
	if dsm.BaseStateManager.config.SeedSize <= 0 {
		return fmt.Errorf("random-walk: must use a seed-size of 1 or larger")
	} else if dsm.BaseStateManager.config.SeedSize > len(dsm.BaseStateManager.discoveredChannels.keys) {
		return fmt.Errorf("random-walk: seed-size exceeds number of discovered channels")
	}

	pages := make([]Page, 0, dsm.BaseStateManager.config.SeedSize)

	seedChannels := make(map[string]bool)
	for {
		url, err := dsm.BaseStateManager.GetRandomDiscoveredChannel()
		if err != nil {
			return fmt.Errorf("random-walk: unable to get random discovered channel during ")
		}
		if _, ok := seedChannels[url]; ok {
			log.Info().Str("seed_url", url).Msg("random-walk: url previously selected. skipping")
			continue
		}
		seedChannels[url] = true
		pages = append(pages, Page{
			ID:        uuid.New().String(),
			URL:       url,
			Depth:     0,
			Status:    "unfetched",
			Timestamp: time.Now(),
		})
		if len(seedChannels) >= dsm.BaseStateManager.config.SeedSize {
			log.Info().Int("seed_size", dsm.BaseStateManager.config.SeedSize).Int("seed_channel_size", len(seedChannels)).Msg("random-walk: finished selecting seed channels")
			break
		}
	}

	if err := dsm.AddLayer(pages); err != nil {
		return fmt.Errorf("random-walk: failed to add seed pages: %w", err)
	}

	return nil
}

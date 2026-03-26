package state

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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

	// Seed channel chat ID cache: username -> TDLib chat ID
	chatIDCache   map[string]int64
	chatIDCacheMu sync.RWMutex

	// seedChannelSet holds every username loaded from seed_channels at startup.
	// Written once during LoadSeedChannels; read-only after that.
	seedChannelSet map[string]struct{}

	// Invalid channel cache: username -> time it was marked invalid (TTL = 30 days)
	invalidChannelCache   map[string]time.Time
	invalidChannelCacheMu sync.RWMutex

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

		chatIDCache:         make(map[string]int64),
		seedChannelSet:      make(map[string]struct{}),
		invalidChannelCache: make(map[string]time.Time),
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

	if dsm.config.SamplingMethod == "random-walk" {
		dsm.databaseBinding = databaseStorageBinding
	}

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
				log.Info().Msg("Loading URLS for crawl")
				err = dsm.loadURLsForCrawl(dsm.config.CrawlExecutionID)
				if err != nil {
					log.Warn().Err(err).Str("executionID", dsm.config.CrawlExecutionID).
						Msg("Failed to load URLs for execution ID, but continuing")
				}
				log.Info().Msg("Loading Pages into memory")
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
					if dsm.BaseStateManager.config.SamplingMethod == "random-walk" {
						log.Info().Msg("random-walk-seed: Adding seed urls for in progress crawl")
						for _, url := range seedURLs {
							dsm.BaseStateManager.AddDiscoveredChannel(url)
						}
					}
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
				if dsm.BaseStateManager.config.SamplingMethod == "random-walk" {
					log.Info().Msg("random-walk-seed: Adding seed urls for in progress crawl")
					for _, url := range seedURLs {
						dsm.BaseStateManager.AddDiscoveredChannel(url)
					}
				}
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
		if dsm.BaseStateManager.config.SamplingMethod == "random-walk" {
			log.Info().Str("url", url).Msg("random-walk-seed: Adding seed url in Dapr Initialize")
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
		page := Page{
			ID:        uuid.New().String(),
			URL:       url,
			Depth:     0,
			Status:    "unfetched",
			Timestamp: time.Now(),
		}
		// In random-walk mode each seed channel starts its own chain
		if dsm.BaseStateManager.config.SamplingMethod == "random-walk" {
			page.SequenceID = uuid.New().String()
		}
		pages = append(pages, page)
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

	log.Info().Str("layer_map_key", layerMapKey).Msg("Getting layer map REMOVE")

	layerMapResponse, err := (*dsm.client).GetState(
		context.Background(),
		dsm.stateStoreName,
		layerMapKey,
		nil,
	)

	log.Info().Str("layer_map_key", layerMapKey).Msg("Layer map received REMOVE")

	if err != nil || layerMapResponse.Value == nil {
		return fmt.Errorf("failed to load layer map for crawl %s: %w", crawlID, err)
	}

	var layerMap map[int][]string
	err = json.Unmarshal(layerMapResponse.Value, &layerMap)
	if err != nil {
		return fmt.Errorf("failed to parse layer map for crawl %s: %w", crawlID, err)
	}

	log.Info().Str("layer_map_key", layerMapKey).Msg("Parsed layer map REMOVE")

	// flatten page ids into list
	var allIDs []string
	for _, ids := range layerMap {
		allIDs = append(allIDs, ids...)
	}

	pages, err := dsm.fetchAllPages(crawlID, allIDs)
	if err != nil {
		return err
	}

	// 4. Update Cache in one pass
	dsm.urlCacheMutex.Lock()
	for _, page := range pages {
		dsm.urlCache[page.URL] = fmt.Sprintf("%s:%s", crawlID, page.ID)
	}
	dsm.urlCacheMutex.Unlock()

	log.Info().Str("crawlID", crawlID).Int("urlCount", len(pages)).Msg("Loaded URLs for crawl")

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

	if dsm.BaseStateManager.config.SamplingMethod == "random-walk" {
		log.Info().Msgf("random-walk-layer: Sampling Method %s allows for duplicate visits to a URL. Skipping de-duplication process", dsm.BaseStateManager.config.SamplingMethod)
	}

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

	// to avoid overwhelming statestore for large seed counts
	const maxConcurrentDaprCalls = 1000
	sem := make(chan struct{}, maxConcurrentDaprCalls)

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

			// acquire slot in semaphore
			sem <- struct{}{}
			defer func() { <-sem }()
			// page level context to avoid timeout for large seeds
			saveCtx, saveCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer saveCancel()

			// Marshal page data for Dapr
			pageData, err := json.Marshal(pageCopy)
			if err != nil {
				return fmt.Errorf("failed to marshal page %s: %w", pageCopy.ID, err)
			}

			// Save page to Dapr
			pageKey := fmt.Sprintf("%s/page/%s", dsm.config.CrawlExecutionID, pageCopy.ID)
			err = (*dsm.client).SaveState(
				saveCtx,
				dsm.stateStoreName,
				pageKey,
				pageData,
				nil,
			)
			if err != nil {
				// TODO: Replace with dapr resiliency: https://docs.dapr.io/operations/resiliency/resiliency-overview/
				sleepMilliseconds := 3000 + rand.IntN(4000)
				log.Info().Str("page_id", pageCopy.ID).Int("sleep_milliseconds", sleepMilliseconds).Err(err).
					Msg("Encountered error saving page. Waiting between 3 and 7 seconds and retrying")
				time.Sleep(time.Duration(sleepMilliseconds) * time.Millisecond)
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

	// // Use errgroup to perform save operations concurrently
	// var eg errgroup.Group

	// // 1. Save with formatted key
	// metadataKey := fmt.Sprintf("%s/metadata", dsm.config.CrawlID)
	// eg.Go(func() error {
	// 	err := (*dsm.client).SaveState(ctx, dsm.stateStoreName, metadataKey, metadataData, nil)
	// 	if err != nil {
	// 		log.Error().Err(err).Str("key", metadataKey).Msg("Failed to save metadata with formatted key")
	// 		return err
	// 	}
	// 	return nil
	// })

	// // 2. Save with direct crawl ID key
	// eg.Go(func() error {
	// 	err := (*dsm.client).SaveState(ctx, dsm.stateStoreName, dsm.config.CrawlID, metadataData, nil)
	// 	if err != nil {
	// 		log.Warn().Err(err).Str("key", dsm.config.CrawlID).Msg("Failed to save metadata with direct key")
	// 	}
	// 	// Continue even if this fails as it's redundant
	// 	return nil
	// })

	// // 3. Save with execution ID key
	// eg.Go(func() error {
	// 	err := (*dsm.client).SaveState(ctx, dsm.stateStoreName, dsm.config.CrawlExecutionID, metadataData, nil)
	// 	if err != nil {
	// 		log.Warn().Err(err).Str("key", dsm.config.CrawlExecutionID).Msg("Failed to save metadata with execution ID")
	// 	}
	// 	// Continue even if this fails as it's redundant
	// 	return nil
	// })

	// // Wait for all operations to complete, fail only if the primary one fails
	// if err := eg.Wait(); err != nil {
	// 	return fmt.Errorf("failed to update metadata: %w", err)
	// }

	keys := []struct {
		key     string
		keyType string
	}{
		{fmt.Sprintf("%s/metadata", dsm.config.CrawlID), "formatted key"},
		{dsm.config.CrawlID, "direct key"},
		{dsm.config.CrawlExecutionID, "execution ID"},
	}

	for i, k := range keys {
		err := (*dsm.client).SaveState(ctx, dsm.stateStoreName, k.key, metadataData, nil)
		if err == nil {
			log.Info().
				Str("crawlID", crawlID).
				Str("executionID", dsm.config.CrawlExecutionID).
				Str("status", metadataCopy.Status).
				Str("key", k.key).
				Str("key_type", k.keyType).
				Msg("Crawl metadata updated in DAPR using")
			return nil
		}
		if i == 0 {
			log.Error().Err(err).Str("key", k.key).Msg("Primary key failed")
		} else {
			log.Warn().Err(err).Str("key", k.key).Str("key_type", k.keyType).Msg("Fallback key failed")
		}
	}

	return fmt.Errorf("failed to save metadata with all attempted keys")

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

	// // Use errgroup to manage all concurrent DAPR operations
	// var eg errgroup.Group

	// // 1. Save metadata with different key formats
	// metadataKey := fmt.Sprintf("%s/metadata", dsm.config.CrawlID)
	// eg.Go(func() error {
	// 	err := (*dsm.client).SaveState(ctx, dsm.stateStoreName, metadataKey, metadataData, nil)
	// 	if err != nil {
	// 		log.Error().Err(err).Str("key", metadataKey).Msg("Failed to save metadata with formatted key")
	// 		return err
	// 	}
	// 	return nil
	// })

	// // Also save with direct crawl ID
	// eg.Go(func() error {
	// 	err := (*dsm.client).SaveState(ctx, dsm.stateStoreName, dsm.config.CrawlID, metadataData, nil)
	// 	if err != nil {
	// 		log.Warn().Err(err).Str("key", dsm.config.CrawlID).Msg("Failed to save metadata with direct key")
	// 	}
	// 	return nil // Non-critical, continue even if it fails
	// })

	// // Also save with execution ID
	// eg.Go(func() error {
	// 	err := (*dsm.client).SaveState(ctx, dsm.stateStoreName, dsm.config.CrawlExecutionID, metadataData, nil)
	// 	if err != nil {
	// 		log.Warn().Err(err).Str("key", dsm.config.CrawlExecutionID).Msg("Failed to save metadata with execution ID")
	// 	}
	// 	return nil // Non-critical, continue even if it fails
	// })

	// // 2. Save layer map data
	// layerMapKey := fmt.Sprintf("%s/layer_map", dsm.config.CrawlExecutionID)
	// eg.Go(func() error {
	// 	err := (*dsm.client).SaveState(ctx, dsm.stateStoreName, layerMapKey, layerMapData, nil)
	// 	if err != nil {
	// 		log.Error().Err(err).Str("key", layerMapKey).Msg("Failed to save layer map")
	// 		return err
	// 	}
	// 	return nil
	// })

	// // Save with alternate key format as well
	// alternateLayerMapKey := fmt.Sprintf("%s/layer_map/%s", dsm.config.CrawlID, dsm.config.CrawlExecutionID)
	// eg.Go(func() error {
	// 	err := (*dsm.client).SaveState(ctx, dsm.stateStoreName, alternateLayerMapKey, layerMapData, nil)
	// 	if err != nil {
	// 		log.Warn().Err(err).Str("key", alternateLayerMapKey).Msg("Failed to save layer map with alternate key")
	// 	}
	// 	return nil // Non-critical, continue even if it fails
	// })

	// // 3. Save active crawl indicator
	// activeKey := fmt.Sprintf("active_crawl/%s", dsm.config.CrawlID)
	// activeData := []byte(fmt.Sprintf(`{"crawl_id":"%s","execution_id":"%s","timestamp":"%s"}`,
	// 	dsm.config.CrawlID, dsm.config.CrawlExecutionID, time.Now().Format(time.RFC3339)))

	// eg.Go(func() error {
	// 	err := (*dsm.client).SaveState(ctx, dsm.stateStoreName, activeKey, activeData, nil)
	// 	if err != nil {
	// 		log.Warn().Err(err).Str("key", activeKey).Msg("Failed to save active crawl indicator")
	// 	}
	// 	return nil // Non-critical, continue even if it fails
	// })

	// // Wait for all operations to complete
	// if err := eg.Wait(); err != nil {
	// 	return fmt.Errorf("failed to save state: %w", err)
	// }

	// Use errgroup to manage the three major grouping operations concurrently
	var eg errgroup.Group

	// 1. Concurrent Block: Save Metadata with Fallback Logic
	eg.Go(func() error {
		metadataKeys := []struct {
			key     string
			keyType string
		}{
			{fmt.Sprintf("%s/metadata", dsm.config.CrawlID), "formatted key"},
			{dsm.config.CrawlID, "direct key"},
			{dsm.config.CrawlExecutionID, "execution ID"},
		}

		for i, k := range metadataKeys {
			err := (*dsm.client).SaveState(ctx, dsm.stateStoreName, k.key, metadataData, nil)
			if err == nil {
				return nil // Success! Exit this goroutine
			}

			// Log the failure
			log.Warn().Err(err).Str("key", k.key).Str("key_type", k.keyType).Msg("Failed metadata save attempt")

			// If this was the last attempt and it failed, return the error to the group
			if i == len(metadataKeys)-1 {
				return fmt.Errorf("all metadata save attempts failed: %w", err)
			}
		}
		return nil
	})

	// 2. Concurrent Block: Save Layer Map with Fallback Logic
	eg.Go(func() error {
		layerKeys := []struct {
			key     string
			keyType string
		}{
			{fmt.Sprintf("%s/layer_map", dsm.config.CrawlExecutionID), "primary key"},
			{fmt.Sprintf("%s/layer_map/%s", dsm.config.CrawlID, dsm.config.CrawlExecutionID), "alternate key"},
		}

		for i, k := range layerKeys {
			err := (*dsm.client).SaveState(ctx, dsm.stateStoreName, k.key, layerMapData, nil)
			if err == nil {
				return nil // Success!
			}

			log.Warn().Err(err).Str("key", k.key).Str("key_type", k.keyType).Msg("Failed layer map save attempt")

			if i == len(layerKeys)-1 {
				return fmt.Errorf("all layer map save attempts failed: %w", err)
			}
		}
		return nil
	})

	// 3. Concurrent Block: Save Active Crawl Indicator
	eg.Go(func() error {
		activeKey := fmt.Sprintf("active_crawl/%s", dsm.config.CrawlID)
		activeData := []byte(fmt.Sprintf(`{"crawl_id":"%s","execution_id":"%s","timestamp":"%s"}`,
			dsm.config.CrawlID, dsm.config.CrawlExecutionID, time.Now().Format(time.RFC3339)))

		err := (*dsm.client).SaveState(ctx, dsm.stateStoreName, activeKey, activeData, nil)
		if err != nil {
			log.Warn().Err(err).Str("key", activeKey).Msg("Failed to save active crawl indicator (non-critical)")
		}
		return nil
	})

	// Wait for all three groups to finish
	if err := eg.Wait(); err != nil {
		return fmt.Errorf("state persistence failed: %w", err)
	}

	log.Debug().
		Str("crawlID", dsm.config.CrawlID).
		Str("executionID", dsm.config.CrawlExecutionID).
		Msg("Successfully completed all concurrent DAPR state operations")

	return nil
}

// StorePost stores a post in Dapr
func (dsm *DaprStateManager) StorePost(channelID string, post model.Post) error {

	// post data is not necessary for random-walk crawls
	if dsm.BaseStateManager.config.SamplingMethod == "random-walk" {
		return nil
	}

	if post.CrawlLabel == "" {
		post.CrawlLabel = dsm.BaseStateManager.config.CrawlLabel
	}

	if dsm.BaseStateManager.config.CombineFiles {
		jsonData, err := json.Marshal(post)
		// append \n for jsonl
		jsonData = append(jsonData, '\n')
		if err != nil {
			return fmt.Errorf("Chunk: Unable to marshall data for writing to file: %w", err)
		}

		tempFilename := fmt.Sprintf("%s/temp_%s_%d.jsonl", dsm.BaseStateManager.config.CombineTempDir, post.PostUID, time.Now().UnixNano())
		watchFilename := fmt.Sprintf("%s/post_%s_%d.jsonl", dsm.BaseStateManager.config.CombineWatchDir, post.PostUID, time.Now().UnixNano())

		err = os.WriteFile(tempFilename, jsonData, 0644)
		if err != nil {
			return fmt.Errorf("Chunk: Unable to write to combined filename %s: %w", tempFilename, err)
		}

		err = os.Rename(tempFilename, watchFilename)
		if err != nil {
			return fmt.Errorf("Chunk: Unable to move %s to %s: %w", tempFilename, watchFilename, err)
		}

		return nil
	}

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
	// loadedCount := 0

	// Check if we're resuming the same crawl execution
	// We consider it the same execution if we're using the exact same executionID
	isResumingSameCrawlExecution := dsm.config.CrawlExecutionID == crawlID
	log.Info().
		Bool("isResumingSameCrawlExecution", isResumingSameCrawlExecution).
		Str("configExecutionID", dsm.config.CrawlExecutionID).
		Str("loadingCrawlID", crawlID).
		Msg("Loading pages with execution context")

	var allIDs []string
	for _, ids := range dsm.layerMap {
		for _, id := range ids {
			if _, exists := dsm.pageMap[id]; !exists {
				allIDs = append(allIDs, id)
			}
		}
	}

	pages, err := dsm.fetchAllPages(crawlID, allIDs)
	if err != nil {
		return err
	}

	for _, page := range pages {
		if !cont {
			if !isResumingSameCrawlExecution {
				page.Status = "unfetched"
				// When not resuming the same execution, mark ALL pages as unfetched
				// to ensure they get reprocessed with the new execution ID
				oldStatus := page.Status
				page.Status = "unfetched"
				log.Debug().
					Str("pageID", page.ID).
					Str("url", page.URL).
					Str("old_status", oldStatus).
					Bool("resuming_same_execution", isResumingSameCrawlExecution).
					Msg("New execution: Marking page as unfetched regardless of previous status")
			} else if page.Status != "fetched" {
				page.Status = "unfetched"
				log.Debug().
					Str("pageID", page.ID).
					Str("url", page.URL).
					Str("old_status", page.Status).
					Bool("resuming_same_execution", isResumingSameCrawlExecution).
					Msg("Resetting non-fetched page status to unfetched in same execution")
			} else {
				// // When resuming same execution, preserve fetched status when loading from DAPR
			}
			page.Timestamp = time.Now()
		}
		page.Messages = []Message{}
		dsm.pageMap[page.ID] = page
	}

	// // For each layer in the layer map
	// for _, pageIDs := range dsm.layerMap {
	// 	// For each page ID in the layer
	// 	for _, pageID := range pageIDs {
	// 		// Check if we already have this page in memory
	// 		if _, exists := dsm.pageMap[pageID]; exists {
	// 			continue
	// 		}

	// 		// Fetch the page from Dapr
	// 		pageKey := fmt.Sprintf("%s/page/%s", crawlID, pageID)
	// 		pageResponse, err := (*dsm.client).GetState(
	// 			context.Background(),
	// 			dsm.stateStoreName,
	// 			pageKey,
	// 			nil,
	// 		)

	// 		if err != nil || pageResponse.Value == nil {
	// 			log.Warn().Err(err).Str("pageID", pageID).Str("crawlID", crawlID).Msg("Failed to fetch page")
	// 			continue
	// 		}

	// 		var page Page
	// 		err = json.Unmarshal(pageResponse.Value, &page)
	// 		if err != nil {
	// 			log.Warn().Err(err).Str("pageID", pageID).Str("crawlID", crawlID).Msg("Failed to parse page data")
	// 			continue
	// 		}

	// 		// Add page to in-memory page map
	// 		if !cont {
	// 			if !isResumingSameCrawlExecution {
	// 				// When not resuming the same execution, mark ALL pages as unfetched
	// 				// to ensure they get reprocessed with the new execution ID
	// 				oldStatus := page.Status
	// 				page.Status = "unfetched"
	// 				log.Debug().
	// 					Str("pageID", pageID).
	// 					Str("url", page.URL).
	// 					Str("old_status", oldStatus).
	// 					Bool("resuming_same_execution", isResumingSameCrawlExecution).
	// 					Msg("New execution: Marking page as unfetched regardless of previous status")
	// 			} else {
	// 				// When resuming same execution, preserve fetched status when loading from DAPR
	// 				if page.Status != "fetched" {
	// 					page.Status = "unfetched"
	// 					log.Debug().
	// 						Str("pageID", pageID).
	// 						Str("url", page.URL).
	// 						Str("old_status", page.Status).
	// 						Bool("resuming_same_execution", isResumingSameCrawlExecution).
	// 						Msg("Resetting non-fetched page status to unfetched in same execution")
	// 				} else {
	// 					log.Debug().
	// 						Str("pageID", pageID).
	// 						Str("url", page.URL).
	// 						Bool("resuming_same_execution", isResumingSameCrawlExecution).
	// 						Msg("Preserving fetched status from Dapr storage in same execution")
	// 				}
	// 			}
	// 			page.Timestamp = time.Now()
	// 		}

	// 		// Clear any previous messages or processing data
	// 		page.Messages = []Message{}

	// 		// Update the timestamp to current time
	// 		dsm.pageMap[pageID] = page
	// 		loadedCount++
	// 	}
	// }

	log.Info().
		Str("crawlID", crawlID).
		Int("pageCount", len(pages)).
		Bool("resuming_same_execution", isResumingSameCrawlExecution).
		Msg("Loaded pages into memory from DAPR")
	return nil
}

// exportChunkSizeBytes is the maximum raw JSONL size per binding call (~100 MB).
// After base64 encoding (~33% overhead) this stays well under the 200 MB gRPC limit.
// Declared as a var so tests can override it.
var exportChunkSizeBytes = 100 * 1024 * 1024

func (dsm *DaprStateManager) ExportPagesToBinding(crawlID string) error {
	exportedCount := 0
	chunkIndex := 0

	timestamp := time.Now().Format("20060102-150405")

	key, err := fetchFileNamingComponent(*dsm.client, dsm.storageBinding)
	if err != nil {
		return err
	}

	flushChunk := func(data []byte) error {
		filename := fmt.Sprintf("channel-pages-%s-%s-part%d.jsonl", crawlID, timestamp, chunkIndex)
		storagePath, err := dsm.generateStoragePath(
			"analysis",
			fmt.Sprintf("channels/%s", filename),
		)
		if err != nil {
			return fmt.Errorf("failed to generate storage path: %w", err)
		}

		encodedData := base64.StdEncoding.EncodeToString(data)
		req := daprc.InvokeBindingRequest{
			Name:      dsm.storageBinding,
			Operation: "create",
			Data:      []byte(encodedData),
			Metadata: map[string]string{
				key:         storagePath,
				"operation": "create",
			},
		}

		log.Info().Str("path", storagePath).Int("raw_bytes", len(data)).Msg("Writing export chunk")
		_, err = (*dsm.client).InvokeBinding(context.Background(), &req)
		if err != nil {
			return fmt.Errorf("failed to store pages via Dapr binding: %w", err)
		}
		chunkIndex++
		return nil
	}

	var buf []byte

	for _, pageIDs := range dsm.layerMap {
		for _, pageID := range pageIDs {
			var page Page

			dsm.mutex.RLock()
			existingPage, exists := dsm.pageMap[pageID]
			dsm.mutex.RUnlock()

			if exists {
				page = existingPage
			} else {
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
				if err = json.Unmarshal(pageResponse.Value, &page); err != nil {
					log.Warn().Err(err).Str("pageID", pageID).Str("crawlID", crawlID).Msg("Failed to parse page data")
					continue
				}
			}

			pageData, err := json.Marshal(page)
			if err != nil {
				log.Warn().Err(err).Str("pageID", pageID).Msg("Failed to marshal page data")
				continue
			}
			pageData = append(pageData, '\n')
			buf = append(buf, pageData...)
			exportedCount++

			if len(buf) >= exportChunkSizeBytes {
				if err := flushChunk(buf); err != nil {
					return err
				}
				buf = buf[:0]
			}
		}
	}

	if exportedCount == 0 {
		return fmt.Errorf("no pages found to export for crawl ID: %s", crawlID)
	}

	if len(buf) > 0 {
		if err := flushChunk(buf); err != nil {
			return err
		}
	}

	log.Info().Int("pages", exportedCount).Int("chunks", chunkIndex).Msg("Export to binding complete")
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

func (dsm *DaprStateManager) StoreChannelData(channelID string, channelData *model.ChannelData) error {
	channelJSON, err := json.Marshal(channelData)
	if err != nil {
		return fmt.Errorf("random-walk-channel-info: failed to marshall channel data: %w", err)
	}

	channelJSON = append(channelJSON, '\n')

	if dsm.BaseStateManager.config.CombineFiles {
		tempFilename := fmt.Sprintf("%s/temp_channel_%s_%d.jsonl", dsm.BaseStateManager.config.CombineTempDir, channelID, time.Now().UnixNano())
		watchFilename := fmt.Sprintf("%s/channel_%s_%d.jsonl", dsm.BaseStateManager.config.CombineWatchDir, channelID, time.Now().UnixNano())

		err = os.WriteFile(tempFilename, channelJSON, 0644)
		if err != nil {
			return fmt.Errorf("random-walk-channel-info: unable to write to temp file %s: %w", tempFilename, err)
		}

		err = os.Rename(tempFilename, watchFilename)
		if err != nil {
			return fmt.Errorf("random-walk-channel-info: unable to move %s to %s: %w", tempFilename, watchFilename, err)
		}

		return nil
	}

	storagePath, err := dsm.generateCrawlExecutableStoragePath(
		channelID,
		"channel_data.jsonl",
	)

	if err != nil {
		return err
	}

	encodedData := base64.StdEncoding.EncodeToString(channelJSON)
	key, err := fetchFileNamingComponent(*dsm.client, dsm.storageBinding)
	if err != nil {
		return err
	}

	// Prepare metadata
	metadata := map[string]string{
		key:         storagePath,
		"operation": "append",
	}

	req := daprc.InvokeBindingRequest{
		Name:      dsm.storageBinding,
		Operation: "create",
		Data:      []byte(encodedData),
		Metadata:  metadata,
	}
	log.Info().Str("channel", channelID).Msgf("random-walk-channel-info: Writing channel info to: %s", storagePath)
	_, err = (*dsm.client).InvokeBinding(context.Background(), &req)
	if err != nil {
		return fmt.Errorf("random-walk-channel-info: failed to store post via Dapr: %w", err)
	}

	log.Debug().Str("channel", channelID).Msg("Post stored")
	return nil
}

func (dsm *DaprStateManager) SaveEdgeRecords(edges []*EdgeRecord) error {
	// Create a context with timeout to prevent hanging
	// ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	// defer cancel()

	log.Info().Str("log_tag", "rw_edge").Msg("Copying records before saving")
	edgesCopy := make([]*EdgeRecord, len(edges))
	copy(edgesCopy, edges)

	// Add Edges in Memory
	baseErr := dsm.BaseStateManager.SaveEdgeRecords(edgesCopy)
	if baseErr != nil {
		log.Error().Err(baseErr).Str("log_tag", "rw_edge").Msg("Failed to add edge records")
		return baseErr
	}
	log.Info().Int("new_edges", len(edges)).Int("total_edges", len(dsm.BaseStateManager.edgeRecords)).Str("source_channel", edges[0].SourceChannel).
		Str("log_tag", "rw_edge").Msg("Adding new edges")

	sqlQuery := `INSERT INTO edge_records (destination_channel, source_channel, walkback, skipped, discovery_time, crawl_id, sequence_id) VALUES ($1, $2, $3, $4, $5, $6, $7);`

	for i, record := range edgesCopy {

		log.Debug().Int("index", i).Str("log_tag", "rw_edge").Msg("Processing record")

		// Check if this specific record is nil
		if record == nil {
			log.Error().Int("index", i).Str("log_tag", "rw_edge").Msg("Skipping nil record")
			continue
		}

		crawlID := dsm.config.CrawlID
		if record.CrawlID != "" {
			crawlID = record.CrawlID
		}
		values := []any{
			record.DestinationChannel,
			record.SourceChannel,
			record.Walkback,
			record.Skipped,
			record.DiscoveryTime,
			crawlID,
			record.SequenceID,
		}

		err := dsm.ExecuteDatabaseOperation(sqlQuery, values)

		if err != nil {
			return err
		}

		log.Debug().Int("index", i).Str("log_tag", "rw_edge").Msg("Finished processing record")
	}

	return nil
}

// GetEdgeRecord returns the edge_records row for the current crawl matching
// sequence_id and destination_channel. Returns nil, nil if not found.
func (dsm *DaprStateManager) GetEdgeRecord(sequenceID, destinationChannel string) (*EdgeRecord, error) {
	sqlQuery := `SELECT source_channel, walkback, skipped FROM edge_records WHERE crawl_id=$1 AND sequence_id=$2 AND destination_channel=$3 LIMIT 1;`
	rows, err := dsm.queryDatabase(sqlQuery, dsm.config.CrawlID, sequenceID, destinationChannel)
	if err != nil {
		return nil, fmt.Errorf("random-walk-400: GetEdgeRecord query failed: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	row := rows[0]
	if len(row) < 3 {
		return nil, fmt.Errorf("random-walk-400: GetEdgeRecord: unexpected row length %d", len(row))
	}
	sourceChannel, _ := row[0].(string)
	walkback, _ := row[1].(bool)
	skipped, _ := row[2].(bool)
	return &EdgeRecord{
		DestinationChannel: destinationChannel,
		SourceChannel:      sourceChannel,
		Walkback:           walkback,
		Skipped:            skipped,
		SequenceID:         sequenceID,
	}, nil
}

// DeleteEdgeRecord removes the edge_records row for the current crawl matching
// sequence_id and destination_channel.
func (dsm *DaprStateManager) DeleteEdgeRecord(sequenceID, destinationChannel string) error {
	sqlQuery := `DELETE FROM edge_records WHERE crawl_id=$1 AND sequence_id=$2 AND destination_channel=$3;`
	if err := dsm.ExecuteDatabaseOperation(sqlQuery, []any{dsm.config.CrawlID, sequenceID, destinationChannel}); err != nil {
		return fmt.Errorf("random-walk-400: DeleteEdgeRecord failed: %w", err)
	}
	return nil
}

// GetRandomSkippedEdge returns a randomly chosen skipped edge_records row for
// the current crawl with the given sequence_id and source_channel.
// Returns nil, nil if no skipped edges exist.
func (dsm *DaprStateManager) GetRandomSkippedEdge(sequenceID, sourceChannel string) (*EdgeRecord, error) {
	sqlQuery := `SELECT destination_channel, source_channel, sequence_id FROM edge_records WHERE crawl_id=$1 AND sequence_id=$2 AND source_channel=$3 AND skipped=true ORDER BY RANDOM() LIMIT 1;`
	rows, err := dsm.queryDatabase(sqlQuery, dsm.config.CrawlID, sequenceID, sourceChannel)
	if err != nil {
		return nil, fmt.Errorf("random-walk-400: GetRandomSkippedEdge query failed: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	row := rows[0]
	if len(row) < 3 {
		return nil, fmt.Errorf("random-walk-400: GetRandomSkippedEdge: unexpected row length %d", len(row))
	}
	destChannel, _ := row[0].(string)
	srcChannel, _ := row[1].(string)
	seqID, _ := row[2].(string)
	return &EdgeRecord{
		DestinationChannel: destChannel,
		SourceChannel:      srcChannel,
		SequenceID:         seqID,
		Skipped:            true,
	}, nil
}

// PromoteEdge sets skipped=false on the edge_records row for the current crawl
// matching sequence_id and destination_channel.
func (dsm *DaprStateManager) PromoteEdge(sequenceID, destinationChannel string) error {
	sqlQuery := `UPDATE edge_records SET skipped=false WHERE crawl_id=$1 AND sequence_id=$2 AND destination_channel=$3;`
	if err := dsm.ExecuteDatabaseOperation(sqlQuery, []any{dsm.config.CrawlID, sequenceID, destinationChannel}); err != nil {
		return fmt.Errorf("random-walk-400: PromoteEdge failed: %w", err)
	}
	return nil
}

func (dsm *DaprStateManager) InitializeDiscoveredChannels() error {
	log.Info().Str("log_tag", "rw_init").Msg("Initializing discovered channels")
	// TODO: add to config
	dsm.databaseBinding = databaseStorageBinding

	query := "SELECT source_channel FROM edge_records UNION SELECT destination_channel FROM edge_records;"
	req := &daprc.InvokeBindingRequest{
		Name:      dsm.databaseBinding,
		Operation: "query",
		Data:      nil,
		Metadata: map[string]string{
			"sql": query,
		},
	}
	res, err := (*dsm.client).InvokeBinding(context.Background(), req)
	if err != nil {
		return fmt.Errorf("random-walk-init: failed to query discovered channels: %w", err)
	}

	var discoveredChannels [][]string

	if err := json.Unmarshal(res.Data, &discoveredChannels); err != nil {
		log.Error().Str("result_data", string(res.Data)).Str("log_tag", "rw_init").Msg("Failed to unmarshal discovered channels response")
		return fmt.Errorf("random-walk-init: Failed to unmarshal response for discovered channels: %v", err)
	}

	if len(discoveredChannels) > 0 {
		log.Info().Int("count", len(discoveredChannels)).Str("log_tag", "rw_init").Msg("Found previously discovered channels")
		for _, row := range discoveredChannels {
			for _, channel := range row {
				if err := dsm.BaseStateManager.AddDiscoveredChannel(channel); err != nil && !errors.Is(err, ErrChannelExists) {
					log.Warn().Err(err).Str("channel", channel).Str("log_tag", "rw_init").Msg("Error adding discovered channel")
				}
			}
		}
	} else {
		log.Info().Str("log_tag", "rw_init").Msg("No discovered channels found")
	}

	return nil
}

// LoadSeedChannels queries the seed_channels table, adds all usernames to the
// in-memory DiscoveredChannels set, and populates the chatID cache for any rows
// that have a resolved chat_id.  Call this during initialization before
// InitializeDiscoveredChannels so seed channels are immediately treated as known.
func (dsm *DaprStateManager) LoadSeedChannels() error {
	log.Info().Str("log_tag", "rw_seed").Msg("Loading seed channels")
	dsm.databaseBinding = databaseStorageBinding

	query := "SELECT channel_username, chat_id, invalidated_at FROM seed_channels;"
	req := &daprc.InvokeBindingRequest{
		Name:      dsm.databaseBinding,
		Operation: "query",
		Data:      nil,
		Metadata: map[string]string{
			"sql": query,
		},
	}
	res, err := (*dsm.client).InvokeBinding(context.Background(), req)
	if err != nil {
		return fmt.Errorf("random-walk-seed: failed to query seed_channels: %w", err)
	}

	var rows [][]interface{}
	if err := json.Unmarshal(res.Data, &rows); err != nil {
		log.Error().Str("result_data", string(res.Data)).Str("log_tag", "rw_seed").Msg("Failed to unmarshal seed_channels response")
		return fmt.Errorf("random-walk-seed: failed to unmarshal seed_channels response: %w", err)
	}

	dsm.chatIDCacheMu.Lock()
	defer dsm.chatIDCacheMu.Unlock()

	loaded := 0
	skipped := 0
	for _, row := range rows {
		if len(row) < 3 {
			continue
		}
		username, ok := row[0].(string)
		if !ok || username == "" {
			continue
		}
		// Skip channels whose seed-table invalidation is still within the 30-day TTL.
		if row[2] != nil {
			if invalidatedStr, ok := row[2].(string); ok {
				if t, parseErr := time.Parse(time.RFC3339, invalidatedStr); parseErr == nil {
					if time.Since(t) < invalidChannelTTL {
						skipped++
						continue
					}
				}
			}
		}
		dsm.seedChannelSet[username] = struct{}{}
		if addErr := dsm.BaseStateManager.AddDiscoveredChannel(username); addErr != nil {
			// Already in the set — not an error worth logging at warn level
			log.Debug().Str("channel", username).Str("log_tag", "rw_seed").Msg("Channel already in discovered set")
		}
		if row[1] != nil {
			if chatIDFloat, ok := row[1].(float64); ok {
				dsm.chatIDCache[username] = int64(chatIDFloat)
			}
		}
		loaded++
	}

	log.Info().Int("loaded", loaded).Int("skipped_invalid", skipped).Str("log_tag", "rw_seed").Msg("Finished loading seed channels")
	return nil
}

// UpsertSeedChannelChatID stores (or updates) the TDLib chat ID for a channel
// username in both the in-memory cache and the seed_channels table.  Call this
// after a successful SearchPublicChat so future crawl runs can skip the RPC.
func (dsm *DaprStateManager) UpsertSeedChannelChatID(username string, chatID int64) error {
	dsm.chatIDCacheMu.Lock()
	dsm.chatIDCache[username] = chatID
	dsm.chatIDCacheMu.Unlock()

	sqlQuery := `INSERT INTO seed_channels (channel_username, chat_id) VALUES ($1, $2) ON CONFLICT (channel_username) DO UPDATE SET chat_id = EXCLUDED.chat_id;`
	return dsm.ExecuteDatabaseOperation(sqlQuery, []any{username, chatID})
}

// GetCachedChatID returns the TDLib chat ID for username if it is present in
// the in-memory cache, along with a boolean indicating whether a value was found.
func (dsm *DaprStateManager) GetCachedChatID(username string) (int64, bool) {
	dsm.chatIDCacheMu.RLock()
	defer dsm.chatIDCacheMu.RUnlock()
	id, ok := dsm.chatIDCache[username]
	return id, ok
}

// IsSeedChannel reports whether username was loaded from seed_channels at startup.
func (dsm *DaprStateManager) IsSeedChannel(username string) bool {
	_, ok := dsm.seedChannelSet[username]
	return ok
}

// GetChannelLastCrawled returns the last_crawled_at timestamp from seed_channels
// for the given username. Returns zero time if the channel has no recorded crawl.
func (dsm *DaprStateManager) GetChannelLastCrawled(username string) (time.Time, error) {
	dsm.databaseBinding = databaseStorageBinding

	query := "SELECT last_crawled_at FROM seed_channels WHERE channel_username = $1;"
	paramsJSON, err := json.Marshal([]any{username})
	if err != nil {
		return time.Time{}, fmt.Errorf("random-walk-seed: failed to marshal params: %w", err)
	}
	req := &daprc.InvokeBindingRequest{
		Name:      dsm.databaseBinding,
		Operation: "query",
		Data:      nil,
		Metadata: map[string]string{
			"sql":    query,
			"params": string(paramsJSON),
		},
	}
	res, err := (*dsm.client).InvokeBinding(context.Background(), req)
	if err != nil {
		return time.Time{}, fmt.Errorf("random-walk-seed: failed to query last_crawled_at for %s: %w", username, err)
	}

	var rows [][]interface{}
	if err := json.Unmarshal(res.Data, &rows); err != nil {
		return time.Time{}, fmt.Errorf("random-walk-seed: failed to unmarshal last_crawled_at response: %w", err)
	}

	if len(rows) == 0 || len(rows[0]) == 0 || rows[0][0] == nil {
		return time.Time{}, nil
	}

	switch v := rows[0][0].(type) {
	case string:
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			// Try common Postgres format
			t, err = time.Parse("2006-01-02T15:04:05Z", v)
			if err != nil {
				return time.Time{}, fmt.Errorf("random-walk-seed: failed to parse last_crawled_at %q: %w", v, err)
			}
		}
		return t, nil
	case float64:
		return time.Unix(int64(v), 0), nil
	default:
		return time.Time{}, fmt.Errorf("random-walk-seed: unexpected type %T for last_crawled_at", v)
	}
}

// MarkChannelCrawled upserts the channel into seed_channels, setting last_crawled_at
// to NOW() and caching the resolved chatID for future SearchPublicChat avoidance.
func (dsm *DaprStateManager) MarkChannelCrawled(username string, chatID int64) error {
	dsm.chatIDCacheMu.Lock()
	dsm.chatIDCache[username] = chatID
	dsm.chatIDCacheMu.Unlock()

	sqlQuery := `INSERT INTO seed_channels (channel_username, chat_id, last_crawled_at) VALUES ($1, $2, NOW()) ON CONFLICT (channel_username) DO UPDATE SET chat_id = EXCLUDED.chat_id, last_crawled_at = NOW();`
	return dsm.ExecuteDatabaseOperation(sqlQuery, []any{username, chatID})
}

// MarkSeedChannelInvalid sets invalidated_at = NOW() on the seed_channels row
// for the given username, if one exists.  Channels not in the seed table are
// silently ignored — this is intentional (only seed channels need the flag).
func (dsm *DaprStateManager) MarkSeedChannelInvalid(username string) error {
	sqlQuery := `UPDATE seed_channels SET invalidated_at = NOW() WHERE channel_username = $1;`
	return dsm.ExecuteDatabaseOperation(sqlQuery, []any{username})
}

const invalidChannelTTL = 30 * 24 * time.Hour

// LoadInvalidChannels queries the invalid_channels table for rows within the
// 30-day TTL window and populates the in-memory cache.  Call this during
// initialization so all pods share the same set of known-invalid channels.
func (dsm *DaprStateManager) LoadInvalidChannels() error {
	log.Info().Str("log_tag", "rw_invalid").Msg("Loading invalid channels")
	dsm.databaseBinding = databaseStorageBinding

	query := "SELECT channel_username, invalidated_at FROM invalid_channels WHERE invalidated_at > NOW() - INTERVAL '30 days';"
	req := &daprc.InvokeBindingRequest{
		Name:      dsm.databaseBinding,
		Operation: "query",
		Data:      nil,
		Metadata: map[string]string{
			"sql": query,
		},
	}
	res, err := (*dsm.client).InvokeBinding(context.Background(), req)
	if err != nil {
		return fmt.Errorf("random-walk-invalid: failed to query invalid_channels: %w", err)
	}

	var rows [][]interface{}
	if err := json.Unmarshal(res.Data, &rows); err != nil {
		return fmt.Errorf("random-walk-invalid: failed to unmarshal invalid_channels response: %w", err)
	}

	dsm.invalidChannelCacheMu.Lock()
	defer dsm.invalidChannelCacheMu.Unlock()

	loaded := 0
	for _, row := range rows {
		if len(row) < 2 {
			continue
		}
		username, ok := row[0].(string)
		if !ok || username == "" {
			continue
		}
		var invalidatedAt time.Time
		switch v := row[1].(type) {
		case string:
			t, parseErr := time.Parse(time.RFC3339, v)
			if parseErr != nil {
				t, parseErr = time.Parse("2006-01-02T15:04:05Z", v)
				if parseErr != nil {
					log.Warn().Str("channel", username).Str("value", v).Str("log_tag", "rw_invalid").Msg("Failed to parse invalidated_at, skipping")
					continue
				}
			}
			invalidatedAt = t
		case float64:
			invalidatedAt = time.Unix(int64(v), 0)
		default:
			continue
		}
		dsm.invalidChannelCache[username] = invalidatedAt
		loaded++
	}

	log.Info().Int("loaded", loaded).Str("log_tag", "rw_invalid").Msg("Finished loading invalid channels")
	return nil
}

// IsInvalidChannel returns true if the channel is present in the in-memory
// invalid cache and its 30-day TTL has not expired.
func (dsm *DaprStateManager) IsInvalidChannel(username string) bool {
	dsm.invalidChannelCacheMu.RLock()
	t, ok := dsm.invalidChannelCache[username]
	dsm.invalidChannelCacheMu.RUnlock()
	if !ok {
		return false
	}
	return time.Since(t) < invalidChannelTTL
}

// MarkChannelInvalid persists the channel to the invalid_channels table and
// adds it to the in-memory cache so subsequent calls to IsInvalidChannel return
// true without a DB round-trip.
func (dsm *DaprStateManager) MarkChannelInvalid(username string, reason string) error {
	now := time.Now()

	dsm.invalidChannelCacheMu.Lock()
	dsm.invalidChannelCache[username] = now
	dsm.invalidChannelCacheMu.Unlock()

	sqlQuery := `INSERT INTO invalid_channels (channel_username, reason, invalidated_at) VALUES ($1, $2, NOW()) ON CONFLICT (channel_username) DO UPDATE SET reason = EXCLUDED.reason, invalidated_at = NOW();`
	return dsm.ExecuteDatabaseOperation(sqlQuery, []any{username, reason})
}

func (dsm *DaprStateManager) InitializeRandomWalkLayer() error {
	if dsm.BaseStateManager.config.SeedSize <= 0 {
		return fmt.Errorf("random-walk-init: must use a seed-size of 1 or larger")
	} else if dsm.BaseStateManager.config.SeedSize > len(dsm.BaseStateManager.discoveredChannels.keys) {
		return fmt.Errorf("random-walk-init: seed-size exceeds number of discovered channels")
	}

	seedChannels := make(map[string]bool)
	for {
		url, err := dsm.BaseStateManager.GetRandomDiscoveredChannel()
		if err != nil {
			return fmt.Errorf("random-walk-init: unable to get random discovered channel during seeding")
		}
		if _, ok := seedChannels[url]; ok {
			log.Info().Str("seed_url", url).Str("log_tag", "rw_init").Msg("URL previously selected, skipping")
			continue
		}
		seedChannels[url] = true
		page := &Page{
			ID:         uuid.New().String(),
			URL:        url,
			Depth:      0,
			Status:     "unfetched",
			Timestamp:  time.Now(),
			SequenceID: uuid.New().String(),
		}
		if err := dsm.AddPageToPageBuffer(page); err != nil {
			return fmt.Errorf("random-walk-init: failed to add page to page buffer: %w", err)
		}
		if len(seedChannels) >= dsm.BaseStateManager.config.SeedSize {
			log.Info().Int("seed_size", dsm.BaseStateManager.config.SeedSize).Int("seed_channel_size", len(seedChannels)).Str("log_tag", "rw_init").Msg("Finished selecting seed channels")
			break
		}
	}

	return nil
}

// TODO: generalize the process of inserting records to function that takes in query and params
func (dsm *DaprStateManager) AddPageToPageBuffer(page *Page) error {
	sqlQuery := `INSERT INTO page_buffer (page_id, parent_id, depth, url, crawl_id, sequence_id) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (crawl_id, url) DO NOTHING;`

	crawlID := page.CrawlID
	if crawlID == "" {
		crawlID = dsm.config.CrawlID
	}

	values := []any{
		page.ID,
		page.ParentID,
		page.Depth,
		page.URL,
		crawlID,
		page.SequenceID,
	}

	if err := dsm.ExecuteDatabaseOperation(sqlQuery, values); err != nil {
		return fmt.Errorf("random-walk-layer: failed to add page to page buffer: %w", err)
	}

	return nil
}

func (dsm *DaprStateManager) ExecuteDatabaseOperation(sqlQuery string, params []any) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	jsonData, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("random-walk-insert: failed to marshal params to JSON: %w", err)
	}

	req := &daprc.InvokeBindingRequest{
		Name:      dsm.databaseBinding,
		Operation: "exec",
		Metadata: map[string]string{
			"sql":    sqlQuery,
			"params": string(jsonData),
		},
	}

	if resp, err := (*dsm.client).InvokeBinding(ctx, req); err != nil {
		if strings.Contains(err.Error(), "invalid header field value") {
			if resp != nil && resp.Metadata != nil {
				if val, ok := resp.Metadata["sql"]; ok {
					log.Error().Str("metadata_sql", val).Str("log_tag", "rw_db").Msg("Invalid header field value error with metadata")
				} else {
					log.Error().Str("log_tag", "rw_db").Msg("Invalid header field value error, no sql metadata key")
				}
			}
			log.Error().Err(err).Str("log_tag", "rw_db").Msg("Ignoring invalid header field value error")
		} else {
			return fmt.Errorf("random-walk-init: failed to invoke Dapr binding: %w", err)
		}
	}
	return nil

}

// DeletePageBufferPages removes specific pages by ID in a single query.
func (dsm *DaprStateManager) DeletePageBufferPages(pageIDs []string, pageURLs []string) error {
	if len(pageIDs) == 0 {
		return nil
	}
	// Build IN clause with escaped values: ('id1','id2','id3')
	escaped := make([]string, len(pageIDs))
	for i, pid := range pageIDs {
		escaped[i] = "'" + strings.ReplaceAll(pid, "'", "''") + "'"
	}
	sqlQuery := fmt.Sprintf("DELETE FROM page_buffer WHERE crawl_id = '%s' AND page_id IN (%s);",
		strings.ReplaceAll(dsm.config.CrawlID, "'", "''"),
		strings.Join(escaped, ","))

	if err := dsm.ExecuteDatabaseOperation(sqlQuery, nil); err != nil {
		return fmt.Errorf("random-walk-layer: failed to delete %d pages from page buffer: %w", len(pageIDs), err)
	}
	log.Info().Int("count", len(pageIDs)).Strs("urls", pageURLs).Str("log_tag", "rw_page_buffer").Msg("Deleted specific pages from page buffer")
	return nil
}

func (dsm *DaprStateManager) GetPagesFromPageBuffer(limit int) ([]Page, error) {
	log.Debug().Str("log_tag", "rw_page_buffer").Msg("Getting pages from page buffer")
	pages := make([]Page, 0)

	query := fmt.Sprintf("SELECT page_id, parent_id, depth, url, crawl_id, sequence_id FROM page_buffer WHERE crawl_id = $1 AND claimed_by = '' LIMIT %d;", limit)
	pageResults, err := dsm.queryDatabase(query, dsm.config.CrawlID)
	if err != nil {
		return pages, fmt.Errorf("random-walk-layer: failed to pull page buffer pages: %w", err)
	}

	if len(pageResults) > 0 {
		log.Debug().Int("count", len(pageResults)).Str("log_tag", "rw_page_buffer").Msg("Found pages in page buffer")
		for _, page := range pageResults {
			seqID := ""
			if len(page) > 5 && page[5] != nil {
				seqID, _ = page[5].(string)
			}
			pages = append(pages, Page{
				ID:         page[0].(string),
				ParentID:   page[1].(string),
				Depth:      int(page[2].(float64)),
				URL:        page[3].(string),
				Status:     "unfetched",
				Timestamp:  time.Now(),
				SequenceID: seqID,
			})
		}
	} else {
		log.Error().Str("log_tag", "rw_page_buffer").Msg("No pages found in page buffer")
	}
	return pages, nil
}

// ClaimPages atomically claims up to `limit` unclaimed pages from the page
// buffer for this pod.  Uses UPDATE ... FOR UPDATE SKIP LOCKED ... RETURNING
// so that concurrent pods (or workers) never receive the same page.
func (dsm *DaprStateManager) ClaimPages(limit int) ([]Page, error) {
	podName := dsm.config.PodName
	if podName == "" {
		podName = "unknown"
	}
	safePodName := strings.ReplaceAll(podName, "'", "''")
	safeCrawlID := strings.ReplaceAll(dsm.config.CrawlID, "'", "''")

	sqlQuery := fmt.Sprintf(
		`UPDATE page_buffer SET claimed_by = '%s', claimed_at = NOW() WHERE page_id IN ( SELECT page_id FROM page_buffer WHERE crawl_id = '%s' AND claimed_by = '' ORDER BY depth, page_id LIMIT %d FOR UPDATE SKIP LOCKED ) RETURNING page_id, parent_id, depth, url, crawl_id, sequence_id;`,
		safePodName, safeCrawlID, limit)

	rows, err := dsm.queryDatabase(sqlQuery)
	if err != nil {
		return nil, fmt.Errorf("claim-pages: failed to claim pages: %w", err)
	}

	pages := make([]Page, 0, len(rows))
	for _, row := range rows {
		if len(row) < 6 {
			continue
		}
		seqID := ""
		if row[5] != nil {
			seqID, _ = row[5].(string)
		}
		pages = append(pages, Page{
			ID:         row[0].(string),
			ParentID:   row[1].(string),
			Depth:      int(row[2].(float64)),
			URL:        row[3].(string),
			Status:     "claimed",
			Timestamp:  time.Now(),
			SequenceID: seqID,
		})
	}

	if len(pages) > 0 {
		log.Debug().Int("count", len(pages)).Str("pod", podName).
			Str("log_tag", "rw_page_buffer").Msg("Claimed pages from page buffer")
	}
	return pages, nil
}

// UnclaimPages releases previously claimed pages back to the queue so they
// can be picked up by any pod.  Used when processing fails with a retryable
// error (e.g. walkback exhausted, flood-wait retire).
func (dsm *DaprStateManager) UnclaimPages(pageIDs []string) error {
	if len(pageIDs) == 0 {
		return nil
	}
	escaped := make([]string, len(pageIDs))
	for i, pid := range pageIDs {
		escaped[i] = "'" + strings.ReplaceAll(pid, "'", "''") + "'"
	}
	safeCrawlID := strings.ReplaceAll(dsm.config.CrawlID, "'", "''")
	sqlQuery := fmt.Sprintf(
		"UPDATE page_buffer SET claimed_by = '', claimed_at = NULL WHERE crawl_id = '%s' AND page_id IN (%s);",
		safeCrawlID, strings.Join(escaped, ","))
	return dsm.ExecuteDatabaseOperation(sqlQuery, nil)
}

// RefreshPageClaim bumps claimed_at to NOW() for a page that is still being
// actively processed, preventing RecoverStalePageClaims from reclaiming it.
func (dsm *DaprStateManager) RefreshPageClaim(pageID string) error {
	safeCrawlID := strings.ReplaceAll(dsm.config.CrawlID, "'", "''")
	safePageID := strings.ReplaceAll(pageID, "'", "''")
	sqlQuery := fmt.Sprintf(
		"UPDATE page_buffer SET claimed_at = NOW() WHERE crawl_id = '%s' AND page_id = '%s' AND claimed_by != '';",
		safeCrawlID, safePageID)
	return dsm.ExecuteDatabaseOperation(sqlQuery, nil)
}

// RecoverStalePageClaims resets pages that have been claimed longer than
// staleThreshold, making them available for re-processing.  Returns the
// number of recovered pages.
func (dsm *DaprStateManager) RecoverStalePageClaims(staleThreshold time.Duration) (int, error) {
	minutes := int(staleThreshold.Minutes())
	if minutes < 1 {
		minutes = 1
	}
	safeCrawlID := strings.ReplaceAll(dsm.config.CrawlID, "'", "''")

	sqlQuery := fmt.Sprintf(
		`UPDATE page_buffer SET claimed_by = '', claimed_at = NULL WHERE crawl_id = '%s' AND claimed_by != '' AND claimed_at < NOW() - INTERVAL '%d minutes' RETURNING page_id;`,
		safeCrawlID, minutes)

	rows, err := dsm.queryDatabase(sqlQuery)
	if err != nil {
		return 0, fmt.Errorf("recover-stale-claims: %w", err)
	}
	count := len(rows)
	if count > 0 {
		log.Warn().Int("recovered", count).Int("stale_minutes", minutes).
			Str("log_tag", "rw_page_buffer").Msg("Recovered stale page claims")
	}
	return count, nil
}

// TODO: alot of shared code between StorePost. Generalize a solution that both functions can use
func (dsm *DaprStateManager) UploadCombinedFile(filename string) error {

	postData, err := os.ReadFile(filename)

	if err != nil {
		return fmt.Errorf("Chunk: error reading combined file %s: %w", filename, err)
	}

	// Create storage path
	storagePath, err := dsm.generateCrawlExecutableStoragePath(
		"combined-posts",
		path.Base(filename),
	)

	if err != nil {
		return fmt.Errorf("Chunk: error creating storage path %s: %w", filename, err)
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
	return nil
}

func (dsm *DaprStateManager) fetchAllPages(crawlID string, pageIDs []string) ([]Page, error) {

	// TODO: figure out correct batch size here
	const (
		batchSize   = 100
		maxFailures = 3 // Exit entirely if 3 batches fail
	)

	pages := make([]Page, 0, len(pageIDs))
	var mu sync.Mutex
	var failureCount int32

	// errgroup.WithContext handles the "Stop everything else" logic automatically
	g, ctx := errgroup.WithContext(context.Background())

	for i := 0; i < len(pageIDs); i += batchSize {
		i := i
		end := i + batchSize
		if end > len(pageIDs) {
			end = len(pageIDs)
		}

		g.Go(func() error {
			// 1. Check if another goroutine already triggered a shutdown
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			keys := make([]string, 0, end-i)
			for j := i; j < end; j++ {
				keys = append(keys, fmt.Sprintf("%s/page/%s", crawlID, pageIDs[j]))
			}

			// 2. Attempt the Dapr call
			resp, err := (*dsm.client).GetBulkState(ctx, dsm.stateStoreName, keys, nil, 10)

			if err != nil {
				newFailures := atomic.AddInt32(&failureCount, 1)
				log.Error().Err(err).Int32("failure_count", newFailures).Msg("Batch fetch failed")

				// 3. If we hit the limit, return the error to trigger errgroup cancellation
				if newFailures >= maxFailures {
					return fmt.Errorf("halted: reached %d batch failures", maxFailures)
				}
				// Otherwise, just return nil to let other batches try to finish
				// (or return err if you want to stop on the very first failure)
				return nil
			}

			// 4. Process successful results
			localPages := make([]Page, 0, len(resp))
			for _, item := range resp {
				if len(item.Value) == 0 {
					continue
				}
				var p Page
				if err := json.Unmarshal(item.Value, &p); err == nil {
					localPages = append(localPages, p)
				}
			}

			mu.Lock()
			pages = append(pages, localPages...)
			mu.Unlock()
			return nil
		})
	}

	// Wait returns the first error returned by any goroutine
	if err := g.Wait(); err != nil {
		return nil, err
	}

	return pages, nil
}

// --- Validator / tandem-crawl methods ---

// queryDatabase is a helper for SELECT queries that return rows.  It invokes
// the DAPR postgres binding in "query" mode and unmarshals the JSON response
// into a [][]any (one sub-slice per row).
func (dsm *DaprStateManager) queryDatabase(sqlQuery string, params ...any) ([][]any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// gRPC metadata values may not contain newlines or tabs; collapse all
	// whitespace so multi-line SQL literals pass through without error.
	normalizedSQL := strings.Join(strings.Fields(sqlQuery), " ")
	metadata := map[string]string{"sql": normalizedSQL}
	if len(params) > 0 {
		jsonData, err := json.Marshal(params)
		if err != nil {
			return nil, fmt.Errorf("queryDatabase: failed to marshal params: %w", err)
		}
		metadata["params"] = string(jsonData)
	}

	req := &daprc.InvokeBindingRequest{
		Name:      dsm.databaseBinding,
		Operation: "query",
		Metadata:  metadata,
	}
	res, err := (*dsm.client).InvokeBinding(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("queryDatabase: failed to invoke binding: %w", err)
	}

	var rows [][]any
	if err := json.Unmarshal(res.Data, &rows); err != nil {
		return nil, fmt.Errorf("queryDatabase: failed to unmarshal response: %w", err)
	}
	return rows, nil
}

// CreatePendingBatch inserts a new batch row with status='open'.
func (dsm *DaprStateManager) CreatePendingBatch(batch *PendingEdgeBatch) error {
	sqlQuery := `INSERT INTO pending_edge_batches (batch_id, crawl_id, source_channel, source_page_id, source_depth, sequence_id, status) VALUES ($1, $2, $3, $4, $5, $6, $7);`
	values := []any{
		batch.BatchID,
		batch.CrawlID,
		batch.SourceChannel,
		batch.SourcePageID,
		batch.SourceDepth,
		batch.SequenceID,
		"open",
	}
	if err := dsm.ExecuteDatabaseOperation(sqlQuery, values); err != nil {
		return fmt.Errorf("validator-db: failed to create pending batch: %w", err)
	}
	return nil
}

// InsertPendingEdge writes a single pending edge row.
func (dsm *DaprStateManager) InsertPendingEdge(edge *PendingEdge) error {
	sqlQuery := `INSERT INTO pending_edges (batch_id, crawl_id, destination_channel, source_channel, sequence_id, discovery_time, source_type, validation_status) VALUES ($1, $2, $3, $4, $5, $6, $7, $8);`
	values := []any{
		edge.BatchID,
		edge.CrawlID,
		edge.DestinationChannel,
		edge.SourceChannel,
		edge.SequenceID,
		edge.DiscoveryTime,
		edge.SourceType,
		"pending",
	}
	if err := dsm.ExecuteDatabaseOperation(sqlQuery, values); err != nil {
		return fmt.Errorf("validator-db: failed to insert pending edge: %w", err)
	}
	return nil
}

// ClosePendingBatch sets status='closed' and closed_at=NOW().
func (dsm *DaprStateManager) ClosePendingBatch(batchID string) error {
	sqlQuery := `UPDATE pending_edge_batches SET status = 'closed', closed_at = NOW() WHERE batch_id = $1;`
	if err := dsm.ExecuteDatabaseOperation(sqlQuery, []any{batchID}); err != nil {
		return fmt.Errorf("validator-db: failed to close pending batch: %w", err)
	}
	return nil
}

// ClaimPendingEdges atomically claims up to `limit` edges with
// validation_status='pending' by setting them to 'validating'.
// Uses a CTE with FOR UPDATE SKIP LOCKED for concurrent validator safety.
func (dsm *DaprStateManager) ClaimPendingEdges(limit int) ([]*PendingEdge, error) {
	// DAPR postgres binding supports parameterised queries for "exec" but not
	// easily for "query".  We use a single UPDATE ... RETURNING which is a write
	// that also returns rows — DAPR handles this via "exec" but we need to use
	// "query" to get the RETURNING data back.  The DAPR postgres binding treats
	// UPDATE ... RETURNING as a query that returns rows.
	sqlQuery := fmt.Sprintf(`UPDATE pending_edges SET validation_status = 'validating', validated_at = NOW() WHERE pending_id IN ( SELECT pending_id FROM pending_edges WHERE validation_status = 'pending' ORDER BY discovery_time LIMIT %d FOR UPDATE SKIP LOCKED ) RETURNING pending_id, batch_id, crawl_id, destination_channel, source_channel, sequence_id, discovery_time, source_type;`, limit)

	rows, err := dsm.queryDatabase(sqlQuery)
	if err != nil {
		return nil, fmt.Errorf("validator-db: failed to claim pending edges: %w", err)
	}

	edges := make([]*PendingEdge, 0, len(rows))
	for _, row := range rows {
		if len(row) < 8 {
			continue
		}
		pendingID := 0
		switch v := row[0].(type) {
		case float64:
			pendingID = int(v)
		}

		var discoveryTime time.Time
		if ts, ok := row[6].(string); ok {
			discoveryTime, _ = time.Parse(time.RFC3339, ts)
			if discoveryTime.IsZero() {
				discoveryTime, _ = time.Parse("2006-01-02T15:04:05Z", ts)
			}
		}

		batchID, _ := row[1].(string)
		crawlID, _ := row[2].(string)
		destChan, _ := row[3].(string)
		srcChan, _ := row[4].(string)
		seqID, _ := row[5].(string)
		srcType, _ := row[7].(string)

		edges = append(edges, &PendingEdge{
			PendingID:          pendingID,
			BatchID:            batchID,
			CrawlID:            crawlID,
			DestinationChannel: destChan,
			SourceChannel:      srcChan,
			SequenceID:         seqID,
			DiscoveryTime:      discoveryTime,
			SourceType:         srcType,
			ValidationStatus:   "validating",
		})
	}
	return edges, nil
}

// UpdatePendingEdge sets validation_status, validation_reason, and validated_at.
func (dsm *DaprStateManager) UpdatePendingEdge(update PendingEdgeUpdate) error {
	sqlQuery := `UPDATE pending_edges SET validation_status = $1, validation_reason = $2, validated_at = NOW() WHERE pending_id = $3;`
	values := []any{update.ValidationStatus, update.ValidationReason, update.PendingID}
	if err := dsm.ExecuteDatabaseOperation(sqlQuery, values); err != nil {
		return fmt.Errorf("validator-db: failed to update pending edge %d: %w", update.PendingID, err)
	}
	return nil
}

// maxBatchAttempts is the maximum number of times a batch may be re-claimed
// after stale recovery before it is treated as poison and left in place.
const maxBatchAttempts = 3

// ClaimWalkbackBatch finds the oldest batch where status='closed' and all
// pending_edges are validated.  Atomically sets status='processing',
// records claimed_at=NOW(), and increments attempt_count.
// Batches that have already reached maxBatchAttempts are skipped.
func (dsm *DaprStateManager) ClaimWalkbackBatch() (*PendingEdgeBatch, []*PendingEdge, error) {
	// Step 1: Find and claim a closed batch that has no remaining 'pending' or 'validating' edges.
	claimSQL := fmt.Sprintf(`UPDATE pending_edge_batches
		SET status = 'processing', claimed_at = NOW(), attempt_count = attempt_count + 1
		WHERE batch_id = (
			SELECT b.batch_id FROM pending_edge_batches b
			WHERE b.status = 'closed'
			  AND b.attempt_count < %d
			  AND NOT EXISTS (
				SELECT 1 FROM pending_edges e
				WHERE e.batch_id = b.batch_id
				  AND e.validation_status IN ('pending', 'validating')
			  )
			ORDER BY b.created_at
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING batch_id, crawl_id, source_channel, source_page_id, source_depth, sequence_id, attempt_count;`, maxBatchAttempts)

	batchRows, err := dsm.queryDatabase(claimSQL)
	if err != nil {
		return nil, nil, fmt.Errorf("validator-db: failed to claim walkback batch: %w", err)
	}
	if len(batchRows) == 0 {
		return nil, nil, nil
	}

	row := batchRows[0]
	if len(row) < 7 {
		return nil, nil, fmt.Errorf("validator-db: unexpected row length %d for walkback batch", len(row))
	}

	batchID, _ := row[0].(string)
	crawlID, _ := row[1].(string)
	srcChan, _ := row[2].(string)
	srcPageID, _ := row[3].(string)
	srcDepth := 0
	if v, ok := row[4].(float64); ok {
		srcDepth = int(v)
	}
	seqID, _ := row[5].(string)
	attemptCount := 0
	if v, ok := row[6].(float64); ok {
		attemptCount = int(v)
	}

	batch := &PendingEdgeBatch{
		BatchID:       batchID,
		CrawlID:       crawlID,
		SourceChannel: srcChan,
		SourcePageID:  srcPageID,
		SourceDepth:   srcDepth,
		SequenceID:    seqID,
		Status:        "processing",
		AttemptCount:  attemptCount,
	}

	// Step 2: Fetch all edges for this batch.
	edgeSQL := `SELECT pending_id, batch_id, crawl_id, destination_channel, source_channel, sequence_id, discovery_time, source_type, validation_status, validation_reason FROM pending_edges WHERE batch_id = $1;`

	edgeRows, err := dsm.queryDatabase(edgeSQL, batchID)
	if err != nil {
		return batch, nil, fmt.Errorf("validator-db: failed to fetch edges for batch %s: %w", batchID, err)
	}

	edges := make([]*PendingEdge, 0, len(edgeRows))
	for _, erow := range edgeRows {
		if len(erow) < 10 {
			continue
		}
		pendingID := 0
		if v, ok := erow[0].(float64); ok {
			pendingID = int(v)
		}
		var discoveryTime time.Time
		if ts, ok := erow[6].(string); ok {
			discoveryTime, _ = time.Parse(time.RFC3339, ts)
			if discoveryTime.IsZero() {
				discoveryTime, _ = time.Parse("2006-01-02T15:04:05Z", ts)
			}
		}
		eBatchID, _ := erow[1].(string)
		eCrawlID, _ := erow[2].(string)
		destChan, _ := erow[3].(string)
		eSrcChan, _ := erow[4].(string)
		eSeqID, _ := erow[5].(string)
		eSrcType, _ := erow[7].(string)
		valStatus, _ := erow[8].(string)
		valReason, _ := erow[9].(string)

		edges = append(edges, &PendingEdge{
			PendingID:          pendingID,
			BatchID:            eBatchID,
			CrawlID:            eCrawlID,
			DestinationChannel: destChan,
			SourceChannel:      eSrcChan,
			SequenceID:         eSeqID,
			DiscoveryTime:      discoveryTime,
			SourceType:         eSrcType,
			ValidationStatus:   valStatus,
			ValidationReason:   valReason,
		})
	}

	return batch, edges, nil
}

// CompletePendingBatch sets status='completed' and completed_at=NOW().
func (dsm *DaprStateManager) CompletePendingBatch(batchID string) error {
	sqlQuery := `UPDATE pending_edge_batches SET status = 'completed', completed_at = NOW() WHERE batch_id = $1;`
	if err := dsm.ExecuteDatabaseOperation(sqlQuery, []any{batchID}); err != nil {
		return fmt.Errorf("validator-db: failed to complete pending batch: %w", err)
	}
	return nil
}

// FlushBatchStats upserts aggregated source_type counts into source_type_stats
// for each distinct source_type in the batch, then DELETEs all pending_edges
// rows for that batch.
func (dsm *DaprStateManager) FlushBatchStats(batchID, crawlID string, edges []*PendingEdge) error {
	// Aggregate counts by source_type
	type counts struct {
		total, valid, notChannel, invalid, alreadyDiscovered int
	}
	agg := make(map[string]*counts)
	for _, e := range edges {
		st := e.SourceType
		if st == "" {
			st = "unknown"
		}
		c, ok := agg[st]
		if !ok {
			c = &counts{}
			agg[st] = c
		}
		c.total++
		switch e.ValidationStatus {
		case "valid":
			c.valid++
		case "not_channel":
			c.notChannel++
		case "invalid":
			c.invalid++
		case "duplicate":
			c.alreadyDiscovered++
		}
	}

	// Upsert each source_type
	upsertSQL := `INSERT INTO source_type_stats (crawl_id, source_type, total, valid, not_channel, invalid, duplicate) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (crawl_id, source_type) DO UPDATE SET total = source_type_stats.total + EXCLUDED.total, valid = source_type_stats.valid + EXCLUDED.valid, not_channel = source_type_stats.not_channel + EXCLUDED.not_channel, invalid = source_type_stats.invalid + EXCLUDED.invalid, duplicate = source_type_stats.duplicate + EXCLUDED.duplicate;`

	for st, c := range agg {
		if err := dsm.ExecuteDatabaseOperation(upsertSQL, []any{crawlID, st, c.total, c.valid, c.notChannel, c.invalid, c.alreadyDiscovered}); err != nil {
			log.Warn().Err(err).Str("source_type", st).Msg("validator-db: failed to upsert source_type_stats")
		}
	}

	// Delete pending_edges for this batch
	deleteSQL := `DELETE FROM pending_edges WHERE batch_id = $1;`
	if err := dsm.ExecuteDatabaseOperation(deleteSQL, []any{batchID}); err != nil {
		return fmt.Errorf("validator-db: failed to delete pending edges for batch %s: %w", batchID, err)
	}
	return nil
}

// GetRandomSeedChannel returns a random channel_username from seed_channels.
func (dsm *DaprStateManager) GetRandomSeedChannel() (string, error) {
	sqlQuery := `SELECT channel_username FROM seed_channels WHERE (invalidated_at IS NULL OR invalidated_at < NOW() - INTERVAL '30 days') ORDER BY RANDOM() LIMIT 1;`
	rows, err := dsm.queryDatabase(sqlQuery)
	if err != nil {
		return "", fmt.Errorf("validator-db: failed to get random seed channel: %w", err)
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return "", fmt.Errorf("validator-db: no seed channels found")
	}
	username, ok := rows[0][0].(string)
	if !ok {
		return "", fmt.Errorf("validator-db: unexpected type for channel_username")
	}
	return username, nil
}

// ClaimDiscoveredChannel atomically claims first-discovery of a channel across
// all crawls.  Returns true if this call inserted (first claim), false if
// already claimed by any previous crawl.  crawlID is stored for audit only.
//
// Uses a CTE so the answer comes back in a single round-trip:
//
//	WITH ins AS (INSERT ... ON CONFLICT DO NOTHING RETURNING 1)
//	SELECT COUNT(*) FROM ins;
//
// COUNT = 1 → we inserted (first claim).  COUNT = 0 → conflict (already claimed).
func (dsm *DaprStateManager) ClaimDiscoveredChannel(username, crawlID string) (bool, error) {
	sqlQuery := `WITH ins AS ( INSERT INTO discovered_channels (channel_username, crawl_id) VALUES ($1, $2) ON CONFLICT (channel_username) DO NOTHING RETURNING 1 ) SELECT COUNT(*) FROM ins;`

	rows, err := dsm.queryDatabase(sqlQuery, username, crawlID)
	if err != nil {
		return false, fmt.Errorf("validator-db: failed to claim discovered channel: %w", err)
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return false, nil
	}
	switch v := rows[0][0].(type) {
	case float64:
		return v > 0, nil
	case string:
		return v == "1", nil
	}
	return false, nil
}

// IsChannelDiscovered checks whether a channel has been discovered by any crawl
// in the crawler's history, without inserting.
func (dsm *DaprStateManager) IsChannelDiscovered(username string) (bool, error) {
	sqlQuery := `SELECT 1 FROM discovered_channels WHERE channel_username = $1 LIMIT 1;`
	rows, err := dsm.queryDatabase(sqlQuery, username)
	if err != nil {
		return false, fmt.Errorf("validator-db: failed to check discovered channel: %w", err)
	}
	return len(rows) > 0, nil
}

// CountIncompleteBatches returns the count of pending_edge_batches with
// status != 'completed' for the given crawl_id.
func (dsm *DaprStateManager) CountIncompleteBatches(crawlID string) (int, error) {
	sqlQuery := `SELECT COUNT(*) FROM pending_edge_batches WHERE crawl_id = $1 AND status <> 'completed';`
	rows, err := dsm.queryDatabase(sqlQuery, crawlID)
	if err != nil {
		return 0, fmt.Errorf("validator-db: failed to count incomplete batches: %w", err)
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return 0, nil
	}
	switch v := rows[0][0].(type) {
	case float64:
		return int(v), nil
	case string:
		// Some postgres drivers return count as string
		var count int
		fmt.Sscanf(v, "%d", &count)
		return count, nil
	}
	return 0, nil
}

// RecoverStaleEdgeClaims resets pending_edges stuck in 'validating' for longer
// than the given threshold back to 'pending'.  This recovers from validator
// crashes that leave edges in limbo.  Call once at validator startup.
func (dsm *DaprStateManager) RecoverStaleEdgeClaims(staleThreshold time.Duration) (int, error) {
	minutes := int(staleThreshold.Minutes())
	if minutes < 1 {
		minutes = 1
	}
	sqlQuery := fmt.Sprintf(
		`UPDATE pending_edges SET validation_status = 'pending', validated_at = NULL WHERE validation_status = 'validating' AND validated_at < NOW() - INTERVAL '%d minutes';`, minutes)

	// We can't easily get affected row count from DAPR exec, so run a count query first.
	countSQL := fmt.Sprintf(
		`SELECT COUNT(*) FROM pending_edges WHERE validation_status = 'validating' AND validated_at < NOW() - INTERVAL '%d minutes';`, minutes)
	rows, err := dsm.queryDatabase(countSQL)
	if err != nil {
		return 0, fmt.Errorf("validator-db: failed to count stale edge claims: %w", err)
	}
	staleCount := 0
	if len(rows) > 0 && len(rows[0]) > 0 {
		if v, ok := rows[0][0].(float64); ok {
			staleCount = int(v)
		}
	}

	if staleCount > 0 {
		if err := dsm.ExecuteDatabaseOperation(sqlQuery, nil); err != nil {
			return 0, fmt.Errorf("validator-db: failed to recover stale edge claims: %w", err)
		}
		log.Info().Int("recovered", staleCount).Int("threshold_minutes", minutes).
			Msg("validator-db: recovered stale edge claims")
	}
	return staleCount, nil
}

// RecoverStaleBatchClaims resets pending_edge_batches stuck in 'processing' for
// longer than staleThreshold back to 'closed', incrementing attempt_count.
// Batches that have already reached maxBatchAttempts are logged and skipped —
// they are left in 'processing' so they don't block the claim query indefinitely.
func (dsm *DaprStateManager) RecoverStaleBatchClaims(staleThreshold time.Duration) (int, error) {
	minutes := int(staleThreshold.Minutes())
	if minutes < 1 {
		minutes = 1
	}

	// Log poison batches (attempt_count >= maxBatchAttempts) so operators are aware.
	poisonSQL := fmt.Sprintf(
		`SELECT batch_id, source_channel, attempt_count FROM pending_edge_batches WHERE status = 'processing' AND attempt_count >= %d AND claimed_at < NOW() - INTERVAL '%d minutes';`,
		maxBatchAttempts, minutes)
	poisonRows, err := dsm.queryDatabase(poisonSQL)
	if err != nil {
		return 0, fmt.Errorf("validator-db: failed to query poison batches: %w", err)
	}
	for _, row := range poisonRows {
		if len(row) < 3 {
			continue
		}
		batchID, _ := row[0].(string)
		srcChan, _ := row[1].(string)
		attempts := 0
		if v, ok := row[2].(float64); ok {
			attempts = int(v)
		}
		log.Error().Str("batch_id", batchID).Str("source_channel", srcChan).Int("attempt_count", attempts).
			Msg("validator-db: poison batch detected — stuck in processing after max attempts, manual intervention required")
	}

	// Recover stale batches that haven't yet reached the attempt limit.
	countSQL := fmt.Sprintf(
		`SELECT COUNT(*) FROM pending_edge_batches WHERE status = 'processing' AND attempt_count < %d AND claimed_at < NOW() - INTERVAL '%d minutes';`,
		maxBatchAttempts, minutes)
	rows, err := dsm.queryDatabase(countSQL)
	if err != nil {
		return 0, fmt.Errorf("validator-db: failed to count stale batch claims: %w", err)
	}
	staleCount := 0
	if len(rows) > 0 && len(rows[0]) > 0 {
		if v, ok := rows[0][0].(float64); ok {
			staleCount = int(v)
		}
	}

	if staleCount > 0 {
		recoverSQL := fmt.Sprintf(
			`UPDATE pending_edge_batches SET status = 'closed' WHERE status = 'processing' AND attempt_count < %d AND claimed_at < NOW() - INTERVAL '%d minutes';`,
			maxBatchAttempts, minutes)
		if err := dsm.ExecuteDatabaseOperation(recoverSQL, nil); err != nil {
			return 0, fmt.Errorf("validator-db: failed to recover stale batch claims: %w", err)
		}
		log.Info().Int("recovered", staleCount).Int("threshold_minutes", minutes).
			Msg("validator-db: recovered stale batch claims")
	}
	return staleCount, nil
}

// RecoverStaleValidatingEdges resets pending_edges stuck in 'validating' for
// longer than staleThreshold back to 'pending'. This recovers edges orphaned
// when a validator pod dies mid-validation.
func (dsm *DaprStateManager) RecoverStaleValidatingEdges(staleThreshold time.Duration) (int, error) {
	minutes := int(staleThreshold.Minutes())
	if minutes < 1 {
		minutes = 1
	}

	countSQL := fmt.Sprintf(
		`SELECT COUNT(*) FROM pending_edges WHERE validation_status = 'validating' AND validated_at < NOW() - INTERVAL '%d minutes';`,
		minutes)
	rows, err := dsm.queryDatabase(countSQL)
	if err != nil {
		return 0, fmt.Errorf("validator-db: failed to count stale validating edges: %w", err)
	}
	staleCount := 0
	if len(rows) > 0 && len(rows[0]) > 0 {
		if v, ok := rows[0][0].(float64); ok {
			staleCount = int(v)
		}
	}

	if staleCount > 0 {
		recoverSQL := fmt.Sprintf(
			`UPDATE pending_edges SET validation_status = 'pending', validated_at = NULL WHERE validation_status = 'validating' AND validated_at < NOW() - INTERVAL '%d minutes';`,
			minutes)
		if err := dsm.ExecuteDatabaseOperation(recoverSQL, nil); err != nil {
			return 0, fmt.Errorf("validator-db: failed to recover stale validating edges: %w", err)
		}
		log.Info().Int("recovered", staleCount).Int("threshold_minutes", minutes).
			Msg("validator-db: recovered stale validating edges")
	}
	return staleCount, nil
}

// RecoverOrphanEdges deletes pending_edges that belong to completed batches.
// These arise when a validator crashes after CompletePendingBatch but before
// FlushBatchStats finishes deleting the edges.  Safe to run at any time since
// edges on completed batches will never be reprocessed.  Call once at startup.
func (dsm *DaprStateManager) RecoverOrphanEdges() (int, error) {
	countSQL := `SELECT COUNT(*) FROM pending_edges WHERE batch_id IN (SELECT batch_id FROM pending_edge_batches WHERE status = 'completed');`
	rows, err := dsm.queryDatabase(countSQL)
	if err != nil {
		return 0, fmt.Errorf("validator-db: failed to count orphan edges: %w", err)
	}
	orphanCount := 0
	if len(rows) > 0 && len(rows[0]) > 0 {
		if v, ok := rows[0][0].(float64); ok {
			orphanCount = int(v)
		}
	}
	if orphanCount > 0 {
		deleteSQL := `DELETE FROM pending_edges WHERE batch_id IN (SELECT batch_id FROM pending_edge_batches WHERE status = 'completed');`
		if err := dsm.ExecuteDatabaseOperation(deleteSQL, nil); err != nil {
			return 0, fmt.Errorf("validator-db: failed to delete orphan edges: %w", err)
		}
		log.Info().Int("deleted", orphanCount).Msg("validator-db: deleted orphan edges from completed batches")
	}
	return orphanCount, nil
}

// InsertAccessEvent appends a row to access_events. Called by the validator
// when it enters blocked state so that an external process can trigger IP
// rotation.
func (dsm *DaprStateManager) InsertAccessEvent(reason string) error {
	sqlQuery := `INSERT INTO access_events (reason, occurred_at) VALUES ($1, NOW());`
	if err := dsm.ExecuteDatabaseOperation(sqlQuery, []any{reason}); err != nil {
		return fmt.Errorf("validator-db: failed to insert access event: %w", err)
	}
	return nil
}

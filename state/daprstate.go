package state

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	daprc "github.com/dapr/go-sdk/client"
	"github.com/researchaccelerator-hub/telegram-scraper/model"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

const (
	// Default component names
	defaultStateStoreName = "statestore"
	defaultStorageBinding = "telegramcrawlstorage"
)

// DaprStateManager implements StateManagementInterface using Dapr
type DaprStateManager struct {
	*BaseStateManager
	client          *daprc.Client
	stateStoreName  string
	storageBinding  string
	mediaCache      map[string]MediaCacheItem
	mediaCacheMutex sync.RWMutex      // Separate mutex for media cache to reduce contention
	urlCache        map[string]string // Maps URL -> "crawlID:pageID" for all known URLs
	urlCacheMutex   sync.RWMutex      // Separate mutex for URL cache to reduce contention
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

	client, err := daprc.NewClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create Dapr client: %w", err)
	}

	stateStoreName := defaultStateStoreName
	storageBinding := defaultStorageBinding

	if config.DaprConfig != nil {
		if config.DaprConfig.StateStoreName != "" {
			stateStoreName = config.DaprConfig.StateStoreName
		}
		if config.DaprConfig.ComponentName != "" {
			storageBinding = config.DaprConfig.ComponentName
		}
	}

	return &DaprStateManager{
		BaseStateManager: base,
		client:           &client,
		stateStoreName:   stateStoreName,
		storageBinding:   storageBinding,
		mediaCache:       make(map[string]MediaCacheItem),
		urlCache:         make(map[string]string),
	}, nil
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

	// Check for duplicates using read lock on URL cache
	for i := range pages {
		dsm.urlCacheMutex.RLock()
		existingID, exists := dsm.urlCache[pages[i].URL]
		dsm.urlCacheMutex.RUnlock()

		if exists {
			log.Debug().Msgf("Skipping duplicate URL: %s (already exists with ID: %s)", pages[i].URL, existingID)
			duplicateCount++
			continue
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
func (dsm *DaprStateManager) StoreFile(crawlId string, sourceFilePath string, fileName string) (string, error) {
	// Check if the file exists
	if _, err := os.Stat(sourceFilePath); os.IsNotExist(err) {
		return "", fmt.Errorf("source file does not exist: %w", err)
	}

	// Read file content
	fileContent, err := os.ReadFile(sourceFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to read source file: %w", err)
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
		return "", err
	}

	// Encode data for Dapr binding
	encodedData := base64.StdEncoding.EncodeToString(fileContent)
	key, err := fetchFileNamingComponent(*dsm.client, dsm.storageBinding)
	if err != nil {
		return "", err
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
		return "", fmt.Errorf("failed to store file via Dapr: %w", err)
	}

	// Delete original file after successful upload
	err = os.Remove(sourceFilePath)
	if err != nil {
		log.Warn().Err(err).Str("path", sourceFilePath).Msg("Failed to delete source file after upload")
	}

	return storagePath, nil
}

// HasProcessedMedia checks if media has been processed before
func (dsm *DaprStateManager) HasProcessedMedia(mediaID string) (bool, error) {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Check in memory cache first with read lock
	dsm.mediaCacheMutex.RLock()
	if _, exists := dsm.mediaCache[mediaID]; exists {
		dsm.mediaCacheMutex.RUnlock()
		return true, nil
	}
	dsm.mediaCacheMutex.RUnlock()

	// If not in memory, check Dapr state store
	cacheKey := dsm.getMediaCacheKey()
	response, err := (*dsm.client).GetState(
		ctx,
		dsm.stateStoreName,
		cacheKey,
		nil,
	)

	if err != nil {
		return false, fmt.Errorf("failed to get media cache from Dapr: %w", err)
	}

	if response.Value == nil {
		// No cache exists yet
		return false, nil
	}

	// Parse the cache
	var cacheItems map[string]MediaCacheItem
	err = json.Unmarshal(response.Value, &cacheItems)
	if err != nil {
		return false, fmt.Errorf("failed to parse media cache: %w", err)
	}

	// Update memory cache with the write lock
	dsm.mediaCacheMutex.Lock()
	// Merge with existing cache instead of replacing
	if dsm.mediaCache == nil {
		dsm.mediaCache = cacheItems
	} else {
		for k, v := range cacheItems {
			dsm.mediaCache[k] = v
		}
	}

	// Check if mediaID exists while we still have the lock
	_, exists := dsm.mediaCache[mediaID]
	dsm.mediaCacheMutex.Unlock()

	return exists, nil
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

// MarkMediaAsProcessed marks media as processed
func (dsm *DaprStateManager) MarkMediaAsProcessed(mediaID string) error {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Add to memory cache with the dedicated media cache mutex
	dsm.mediaCacheMutex.Lock()
	dsm.mediaCache[mediaID] = MediaCacheItem{
		ID:        mediaID,
		FirstSeen: time.Now(),
	}

	// Create a copy of the entire cache to avoid holding the lock during serialization and IO
	mediaCacheCopy := make(map[string]MediaCacheItem, len(dsm.mediaCache))
	for k, v := range dsm.mediaCache {
		mediaCacheCopy[k] = v
	}
	dsm.mediaCacheMutex.Unlock()

	// Save cache to Dapr - working with the copy instead of the original
	cacheData, err := json.Marshal(mediaCacheCopy)
	if err != nil {
		return fmt.Errorf("failed to marshal media cache: %w", err)
	}

	cacheKey := dsm.getMediaCacheKey()
	err = (*dsm.client).SaveState(
		ctx,
		dsm.stateStoreName,
		cacheKey,
		cacheData,
		nil,
	)

	if err != nil {
		return fmt.Errorf("failed to save media cache to Dapr: %w", err)
	}

	return nil
}

// Close releases resources
func (dsm *DaprStateManager) Close() error {
	// No special cleanup needed
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
			// Check if the crawl is incomplete
			if metadata.Status != "completed" {
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
			// Check if the most recent execution is incomplete
			if metadata.Status != "completed" && metadata.ExecutionID != "" {
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
				if err == nil && execMetadataResponse.Value != nil {
					var execMetadata CrawlMetadata
					err = json.Unmarshal(execMetadataResponse.Value, &execMetadata)
					if err == nil {
						// If this execution isn't marked as completed, use it
						if execMetadata.Status != "completed" {
							log.Info().
								Str("crawlID", crawlID).
								Str("executionID", prevExecID).
								Str("status", execMetadata.Status).
								Msg("Found incomplete previous execution with valid layer map")
							return prevExecID, true, nil
						}
					}
				}

				// Even if the execution is marked as completed or we couldn't find metadata,
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
				// but all pages are processed. We'll keep it as a fallback option
				// but continue checking other executions first
				log.Info().
					Str("crawlID", crawlID).
					Str("executionID", prevExecID).
					Msg("Found previous execution with valid layer map but all pages are completed")

				// If this is the last execution we'll check and we have no better options,
				// use this one even though all its pages are complete
				if i == 0 {
					log.Info().
						Str("crawlID", crawlID).
						Str("executionID", prevExecID).
						Msg("Using completed execution with valid layer map as a last resort")
					return prevExecID, true, nil
				}
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

		// If we can't parse it or there's no layer map but the active crawl exists,
		// use the crawl ID as the execution ID as a fallback
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
				page.Status = "unfetched"
				page.Timestamp = time.Now()
			}

			// Clear any previous messages or processing data
			page.Messages = []Message{}

			// Update the timestamp to current time
			dsm.pageMap[pageID] = page
			loadedCount++
		}
	}

	log.Info().Str("crawlID", crawlID).Int("pageCount", loadedCount).Msg("Loaded pages into memory from DAPR")
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

		// Reset status when fetching for a new crawl
		if page.Status != "unfetched" {
			page.Status = "unfetched"
			page.Messages = []Message{}
			page.Timestamp = time.Now()

			// Save the updated page back to memory and Dapr
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

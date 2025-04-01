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
)

const (
	// Default component names
	defaultStateStoreName = "statestore"
	defaultStorageBinding = "telegramcrawlstorage"
)

// DaprStateManager implements StateManagementInterface using Dapr
type DaprStateManager struct {
	*BaseStateManager
	client         *daprc.Client
	stateStoreName string
	storageBinding string
	mediaCache     map[string]MediaCacheItem
	urlCache       map[string]string // Maps URL -> "crawlID:pageID" for all known URLs
	urlCacheMutex  sync.RWMutex      // Separate mutex for URL cache to reduce contention
}

// NewDaprStateManager creates a new DaprStateManager
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
	// Try to load metadata and layer structure
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

		// Try each crawl ID in reverse order until we find one with a valid layer map
		var loadedLayerMap bool = false
		for i := len(dsm.metadata.PreviousCrawlID) - 1; i >= 0; i-- {
			crawlID := dsm.metadata.PreviousCrawlID[i]

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

				// NEW CODE: Load all pages from this layer map into memory
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

		if !loadedLayerMap {
			crawlID := "20250327185808"

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

				}

				// Load URL cache for this crawl
				err = dsm.loadURLsForCrawl(crawlID)
				if err != nil {
					log.Warn().Err(err).Str("crawlID", crawlID).Msg("Failed to load URLs for crawl")

				}

				// NEW CODE: Load all pages from this layer map into memory
				err = dsm.loadPagesIntoMemory(crawlID, true)
				if err != nil {
					log.Warn().Err(err).Str("crawlID", crawlID).Msg("Failed to load pages into memory")

				}

				log.Info().Str("crawlID", crawlID).Msg("Successfully loaded existing state structure from DAPR")
				loadedLayerMap = true

			}
		}

		if !loadedLayerMap {
			log.Warn().Msg("Could not find any valid layer maps in previous crawls")
		}

		return nil
	}

	// Load URLs from all previous crawls
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

	log.Info().Int("originalCount", len(seedURLs)).Int("uniqueCount", len(uniqueSeedURLs)).Msg("Filtered seed URLs against previous crawls")

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

	dsm.mutex.Lock()
	defer dsm.mutex.Unlock()

	// Determine the depth
	depth := pages[0].Depth

	// Initialize the layer if it doesn't exist
	if _, exists := dsm.layerMap[depth]; !exists {
		dsm.layerMap[depth] = make([]string, 0)
	}

	// Track the IDs of pages we're actually adding (after deduplication)
	addedIDs := make([]string, 0)
	duplicateCount := 0

	// Save each page individually
	for i := range pages {
		// Check if URL already exists in any crawl using our cached map
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

		// Store the page in memory
		dsm.pageMap[pages[i].ID] = pages[i]

		// Add URL to our URL cache for future deduplication
		dsm.urlCacheMutex.Lock()
		dsm.urlCache[pages[i].URL] = fmt.Sprintf("%s:%s", dsm.config.CrawlExecutionID, pages[i].ID)
		dsm.urlCacheMutex.Unlock()

		// Add to layer map
		dsm.layerMap[depth] = append(dsm.layerMap[depth], pages[i].ID)

		// Marshal page data for Dapr
		pageData, err := json.Marshal(pages[i])
		if err != nil {
			return fmt.Errorf("failed to marshal page: %w", err)
		}

		// Save page to Dapr
		pageKey := fmt.Sprintf("%s/page/%s", dsm.config.CrawlExecutionID, pages[i].ID)
		err = (*dsm.client).SaveState(
			context.Background(),
			dsm.stateStoreName,
			pageKey,
			pageData,
			nil,
		)
		if err != nil {
			return fmt.Errorf("failed to save page to Dapr: %w", err)
		}

		// Track this ID as successfully added
		addedIDs = append(addedIDs, pages[i].ID)
	}

	log.Debug().Msgf("Added %d unique pages to depth %d (filtered out %d duplicates from current and previous crawls)",
		len(addedIDs), depth, duplicateCount)

	// Save the updated layer structure
	return dsm.SaveState()
}

// In DaprStateManager
func (dsm *DaprStateManager) UpdateCrawlMetadata(crawlID string, metadata map[string]interface{}) error {
	// First update in-memory metadata using the base implementation
	if err := dsm.BaseStateManager.UpdateCrawlMetadata(crawlID, metadata); err != nil {
		return err
	}

	// Get the current metadata from the base state manager
	dsm.mutex.RLock()
	metadataCopy := dsm.metadata // Use the direct metadata field instead of state.Metadata
	dsm.mutex.RUnlock()

	// Marshal metadata to JSON
	metadataData, err := json.Marshal(metadataCopy)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Store metadata in DAPR using appropriate key
	metadataKey := fmt.Sprintf("%s/metadata", dsm.config.CrawlID)
	err = (*dsm.client).SaveState(
		context.Background(),
		dsm.stateStoreName,
		metadataKey,
		metadataData,
		nil,
	)

	if err != nil {
		return fmt.Errorf("failed to save metadata to DAPR: %w", err)
	}

	log.Debug().Str("crawlID", crawlID).Msg("Crawl metadata updated in DAPR")
	return nil
}

// SaveState persists the current state to Dapr
func (dsm *DaprStateManager) SaveState() error {
	// Save metadata
	metadataData, err := json.Marshal(dsm.metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	metadataKey := fmt.Sprintf("%s/metadata", dsm.config.CrawlID)
	err = (*dsm.client).SaveState(
		context.Background(),
		dsm.stateStoreName,
		metadataKey,
		metadataData,
		nil,
	)

	if err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	// Save layer structure
	layerMapData, err := json.Marshal(dsm.layerMap)
	if err != nil {
		return fmt.Errorf("failed to marshal layer map: %w", err)
	}

	layerMapKey := fmt.Sprintf("%s/layer_map", dsm.config.CrawlExecutionID)
	err = (*dsm.client).SaveState(
		context.Background(),
		dsm.stateStoreName,
		layerMapKey,
		layerMapData,
		nil,
	)

	if err != nil {
		return fmt.Errorf("failed to save layer map: %w", err)
	}

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
	// Check in memory cache first
	if _, exists := dsm.mediaCache[mediaID]; exists {
		return true, nil
	}

	// If not in memory, check Dapr state store
	cacheKey := dsm.getMediaCacheKey()
	response, err := (*dsm.client).GetState(
		context.Background(),
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

	// Update memory cache
	dsm.mediaCache = cacheItems

	// Check if mediaID exists
	_, exists := cacheItems[mediaID]
	return exists, nil
}

func (dsm *DaprStateManager) UpdatePage(page Page) error {
	// First update in memory
	err := dsm.BaseStateManager.UpdatePage(page)
	if err != nil {
		return err
	}

	// Then save to DAPR
	pageData, err := json.Marshal(page)
	if err != nil {
		return fmt.Errorf("failed to marshal page: %w", err)
	}

	pageKey := fmt.Sprintf("%s/page/%s", dsm.config.CrawlExecutionID, page.ID)
	err = (*dsm.client).SaveState(
		context.Background(),
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
	// Add to memory cache
	dsm.mediaCache[mediaID] = MediaCacheItem{
		ID:        mediaID,
		FirstSeen: time.Now(),
	}

	// Save cache to Dapr
	cacheData, err := json.Marshal(dsm.mediaCache)
	if err != nil {
		return fmt.Errorf("failed to marshal media cache: %w", err)
	}

	cacheKey := dsm.getMediaCacheKey()
	err = (*dsm.client).SaveState(
		context.Background(),
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
	// First check in-memory data using the base implementation
	execID, exists, err := dsm.BaseStateManager.FindIncompleteCrawl(crawlID)
	if err != nil || exists {
		return execID, exists, err
	}
	
	// Try to get metadata for the crawl ID
	metadataKey := fmt.Sprintf("%s/metadata", crawlID)
	response, err := (*dsm.client).GetState(
		context.Background(),
		dsm.stateStoreName,
		metadataKey,
		nil,
	)
	
	if err != nil {
		return "", false, fmt.Errorf("failed to get metadata from Dapr: %w", err)
	}
	
	if response.Value == nil {
		return "", false, nil // No metadata exists for this crawl ID
	}
	
	var metadata CrawlMetadata
	err = json.Unmarshal(response.Value, &metadata)
	if err != nil {
		return "", false, fmt.Errorf("failed to parse metadata: %w", err)
	}
	
	// Check if the crawl is incomplete (not marked as completed)
	if metadata.Status != "completed" {
		log.Info().
			Str("crawlID", crawlID).
			Str("executionID", metadata.ExecutionID).
			Str("status", metadata.Status).
			Msg("Found incomplete crawl")
		return metadata.ExecutionID, true, nil
	}
	
	// If we have previous execution IDs, check them too
	for _, prevExecID := range metadata.PreviousCrawlID {
		// For each previous execution ID, check if it's incomplete
		layerMapKey := fmt.Sprintf("%s/layer_map", prevExecID)
		layerMapResponse, err := (*dsm.client).GetState(
			context.Background(),
			dsm.stateStoreName,
			layerMapKey,
			nil,
		)
		
		if err != nil || layerMapResponse.Value == nil {
			log.Warn().Err(err).Str("executionID", prevExecID).Msg("Failed to load layer map for execution")
			continue
		}
		
		// Get the execution's metadata to check its status
		execMetadataKey := fmt.Sprintf("%s/metadata", prevExecID)
		execMetadataResponse, err := (*dsm.client).GetState(
			context.Background(),
			dsm.stateStoreName,
			execMetadataKey,
			nil,
		)
		
		if err != nil || execMetadataResponse.Value == nil {
			continue
		}
		
		var execMetadata CrawlMetadata
		err = json.Unmarshal(execMetadataResponse.Value, &execMetadata)
		if err != nil {
			continue
		}
		
		// If the execution is not completed, return it
		if execMetadata.Status != "completed" {
			log.Info().
				Str("crawlID", crawlID).
				Str("executionID", prevExecID).
				Str("status", execMetadata.Status).
				Msg("Found incomplete previous execution")
			return prevExecID, true, nil
		}
	}
	
	// No incomplete crawl found
	return "", false, nil
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
	return fmt.Sprintf("%s/state/%s", dsm.config.JobID, crawlID)
}

// getMediaCacheKey generates a key for the media cache in Dapr
func (dsm *DaprStateManager) getMediaCacheKey() string {
	return fmt.Sprintf("%s/media-cache", dsm.config.CrawlID)
}

// generateStoragePath creates a standardized storage path
func (dsm *DaprStateManager) generateStoragePath(channelID, subPath string) (string, error) {
	return fmt.Sprintf(
		"%s/%s/%s/%s/%s",
		dsm.config.StorageRoot,
		dsm.config.JobID,
		dsm.config.CrawlID,
		channelID,
		subPath,
	), nil
}

func (dsm *DaprStateManager) generateCrawlLevelStoragePath(subPath string) (string, error) {
	return fmt.Sprintf(
		"%s/%s/%s/%s",
		dsm.config.StorageRoot,
		dsm.config.JobID,
		dsm.config.CrawlID,
		subPath,
	), nil
}

// generateStoragePath creates a standardized storage path
func (dsm *DaprStateManager) generateCrawlExecutableStoragePath(channelID, subPath string) (string, error) {
	return fmt.Sprintf(
		"%s/%s/%s/%s/%s/%s",
		dsm.config.StorageRoot,
		dsm.config.JobID,
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

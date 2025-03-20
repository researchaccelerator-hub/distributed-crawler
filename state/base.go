package state

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

// BaseStateManager provides common functionality for all state manager implementations
type BaseStateManager struct {
	config Config
	//state  State
	mutex sync.RWMutex

	metadata    CrawlMetadata
	lastUpdated time.Time

	// Map of depth -> page IDs (to track layer structure)
	layerMap map[int][]string

	// Map of page ID -> Page (to store all pages)
	pageMap map[string]Page
}

// NewBaseStateManager creates a new BaseStateManager
func NewBaseStateManager(config Config) *BaseStateManager {
	return &BaseStateManager{
		config: config,
		metadata: CrawlMetadata{
			CrawlID:     config.CrawlID,
			ExecutionID: config.CrawlExecutionID,
			StartTime:   time.Now(),
			Status:      "running",
		},
		lastUpdated: time.Now(),
		layerMap:    make(map[int][]string),
		pageMap:     make(map[string]Page),
	}
}

// Initialize sets up the state with seed URLs
func (bsm *BaseStateManager) Initialize(seedURLs []string) error {
	bsm.mutex.Lock()
	defer bsm.mutex.Unlock()

	// Create initial layer at depth 0
	bsm.layerMap[0] = make([]string, 0, len(seedURLs))

	// Create pages from seed URLs
	for _, url := range seedURLs {
		page := Page{
			ID:        uuid.New().String(),
			URL:       url,
			Depth:     0,
			Status:    "unfetched",
			Timestamp: time.Now(),
		}

		// Store page in page map
		bsm.pageMap[page.ID] = page

		// Add page ID to layer 0
		bsm.layerMap[0] = append(bsm.layerMap[0], page.ID)
	}

	log.Info().Msgf("Initialized state with %d seed URLs", len(seedURLs))
	return nil
}

// GetPage retrieves a page by ID
func (bsm *BaseStateManager) GetPage(id string) (Page, error) {
	bsm.mutex.RLock()
	defer bsm.mutex.RUnlock()

	page, exists := bsm.pageMap[id]
	if !exists {
		return Page{}, fmt.Errorf("page with ID %s not found", id)
	}

	return page, nil
}

// UpdatePage updates a page's information
func (bsm *BaseStateManager) UpdatePage(page Page) error {
	bsm.mutex.Lock()
	defer bsm.mutex.Unlock()

	// Store or update the page
	bsm.pageMap[page.ID] = page

	// Update layer map if needed
	found := false
	for depth, ids := range bsm.layerMap {
		if depth == page.Depth {
			for _, id := range ids {
				if id == page.ID {
					found = true
					break
				}
			}

			if !found {
				bsm.layerMap[depth] = append(bsm.layerMap[depth], page.ID)
			}
			break
		}
	}

	return nil
}

// UpdateMessage updates or adds a message to a page
func (bsm *BaseStateManager) UpdateMessage(pageID string, chatID int64, messageID int64, status string) error {
	bsm.mutex.Lock()
	defer bsm.mutex.Unlock()

	page, exists := bsm.pageMap[pageID]
	if !exists {
		return fmt.Errorf("page with ID %s not found", pageID)
	}

	// Check if message already exists
	found := false
	for i := range page.Messages {
		if page.Messages[i].ChatID == chatID && page.Messages[i].MessageID == messageID {
			// Update existing message
			page.Messages[i].Status = status
			found = true
			break
		}
	}

	// Add new message if not found
	if !found {
		page.Messages = append(page.Messages, Message{
			ChatID:    chatID,
			MessageID: messageID,
			Status:    status,
			PageID:    pageID,
		})
	}

	// Update the page in the page map
	bsm.pageMap[pageID] = page
	return nil
}

// AddLayer adds a new layer of pages, ensuring URLs are unique across all layers
func (bsm *BaseStateManager) AddLayer(pages []Page) error {
	if len(pages) == 0 {
		return nil
	}

	bsm.mutex.Lock()
	defer bsm.mutex.Unlock()

	// Create URL to existing page ID map for deduplication
	existingURLs := make(map[string]string)
	for id, page := range bsm.pageMap {
		existingURLs[page.URL] = id
	}

	// Determine the depth
	depth := pages[0].Depth

	// Initialize the layer if it doesn't exist
	if _, exists := bsm.layerMap[depth]; !exists {
		bsm.layerMap[depth] = make([]string, 0)
	}

	// Track the IDs of pages we're actually adding (after deduplication)
	addedIDs := make([]string, 0)

	// Process each page
	for i := range pages {
		// Check if URL already exists in any layer
		if existingID, exists := existingURLs[pages[i].URL]; exists {
			log.Debug().Msgf("Skipping duplicate URL: %s (already exists with ID: %s)", pages[i].URL, existingID)
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

		// Store the page
		bsm.pageMap[pages[i].ID] = pages[i]

		// Add URL to our tracking map for future deduplication
		existingURLs[pages[i].URL] = pages[i].ID

		// Add to layer map
		bsm.layerMap[depth] = append(bsm.layerMap[depth], pages[i].ID)

		// Track this ID as successfully added
		addedIDs = append(addedIDs, pages[i].ID)
	}

	log.Debug().Msgf("Added %d unique pages to depth %d (filtered out %d duplicates)",
		len(addedIDs), depth, len(pages)-len(addedIDs))

	return nil
}

// GetLayerByDepth retrieves all pages at a specific depth
func (bsm *BaseStateManager) GetLayerByDepth(depth int) ([]Page, error) {
	bsm.mutex.RLock()
	defer bsm.mutex.RUnlock()

	ids, exists := bsm.layerMap[depth]
	if !exists {
		return []Page{}, nil
	}

	pages := make([]Page, 0, len(ids))
	for _, id := range ids {
		if page, exists := bsm.pageMap[id]; exists {
			pages = append(pages, page)
		}
	}

	return pages, nil
}

// GetState returns a copy of the current state
func (bsm *BaseStateManager) GetState() State {
	bsm.mutex.RLock()
	defer bsm.mutex.RUnlock()

	state := State{
		Metadata:    bsm.metadata,
		LastUpdated: bsm.lastUpdated,
		Layers:      make([]*Layer, 0),
	}

	// Convert layer map and page map to layers
	for depth, ids := range bsm.layerMap {
		pages := make([]Page, 0, len(ids))
		for _, id := range ids {
			if page, exists := bsm.pageMap[id]; exists {
				pages = append(pages, page)
			}
		}

		layer := &Layer{
			Depth: depth,
			Pages: pages,
		}
		state.Layers = append(state.Layers, layer)
	}

	return state
}

// SetState updates the entire state
func (bsm *BaseStateManager) SetState(state State) {
	bsm.mutex.Lock()
	defer bsm.mutex.Unlock()

	// Update metadata and timestamp
	bsm.metadata = state.Metadata
	bsm.lastUpdated = time.Now()

	// Clear existing maps
	bsm.layerMap = make(map[int][]string)
	bsm.pageMap = make(map[string]Page)

	// Convert layers to layerMap and pageMap
	for _, layer := range state.Layers {
		depth := layer.Depth
		bsm.layerMap[depth] = make([]string, 0, len(layer.Pages))

		for _, page := range layer.Pages {
			bsm.pageMap[page.ID] = page
			bsm.layerMap[depth] = append(bsm.layerMap[depth], page.ID)
		}
	}
}

// GetPreviousCrawls returns a list of previous crawl IDs
func (bsm *BaseStateManager) GetPreviousCrawls() ([]string, error) {
	bsm.mutex.RLock()
	defer bsm.mutex.RUnlock()

	return bsm.metadata.PreviousCrawlID, nil
}

// UpdateCrawlMetadata updates the crawl metadata
func (bsm *BaseStateManager) UpdateCrawlMetadata(crawlID string, metadata map[string]interface{}) error {
	bsm.mutex.Lock()
	defer bsm.mutex.Unlock()

	// Only update if it's the current crawl
	if bsm.metadata.CrawlID != crawlID {
		return errors.New("cannot update metadata for a different crawl ID")
	}

	// Update metadata fields based on the provided map
	for key, value := range metadata {
		switch key {
		case "status":
			if status, ok := value.(string); ok {
				bsm.metadata.Status = status
			}
		case "endTime":
			if endTime, ok := value.(time.Time); ok {
				bsm.metadata.EndTime = endTime
			}
		case "previousCrawlID":
			if prevID, ok := value.(string); ok {
				// Append the new ID to the existing array
				bsm.metadata.PreviousCrawlID = append(bsm.metadata.PreviousCrawlID, prevID)
			} else if prevIDs, ok := value.([]string); ok {
				// For backward compatibility, also handle array input
				bsm.metadata.PreviousCrawlID = append(bsm.metadata.PreviousCrawlID, prevIDs...)
			}
		default:
			// Ignore unknown fields
		}
	}

	bsm.lastUpdated = time.Now()
	return nil
}

// GetMaxDepth returns the highest depth value among all layers
func (bsm *BaseStateManager) GetMaxDepth() (int, error) {
	bsm.mutex.RLock()
	defer bsm.mutex.RUnlock()

	maxDepth := -1
	for depth := range bsm.layerMap {
		if depth > maxDepth {
			maxDepth = depth
		}
	}

	if maxDepth == -1 {
		return -1, errors.New("no layers found")
	}

	return maxDepth, nil
}

// StorePost and StoreFile are left to specific implementations
// HasProcessedMedia and MarkMediaAsProcessed are left to specific implementations
// SaveState is left to specific implementations
// Close is left to specific implementations

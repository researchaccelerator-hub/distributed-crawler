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
	state  State
	mutex  sync.RWMutex
}

// NewBaseStateManager creates a new BaseStateManager
func NewBaseStateManager(config Config) *BaseStateManager {
	return &BaseStateManager{
		config: config,
		state: State{
			Layers:      []*Layer{},
			LastUpdated: time.Now(),
			Metadata: CrawlMetadata{
				CrawlID:     config.CrawlID,
				ExecutionID: config.CrawlExecutionID,
				StartTime:   time.Now(),
				Status:      "running",
			},
		},
	}
}

// Initialize sets up the state with seed URLs
func (bsm *BaseStateManager) Initialize(seedURLs []string) error {
	bsm.mutex.Lock()
	defer bsm.mutex.Unlock()

	// Create pages from seed URLs
	pages := make([]Page, 0, len(seedURLs))
	for _, url := range seedURLs {
		pages = append(pages, Page{
			ID:        uuid.New().String(),
			URL:       url,
			Depth:     0,
			Status:    "unfetched",
			Timestamp: time.Now(),
		})
	}

	// Create initial layer
	layer := &Layer{
		Depth: 0,
		Pages: pages,
	}

	bsm.state.Layers = []*Layer{layer}

	log.Info().Msgf("Initialized state with %d seed URLs", len(seedURLs))
	return nil
}

// GetPage retrieves a page by ID
func (bsm *BaseStateManager) GetPage(id string) (Page, error) {
	bsm.mutex.RLock()
	defer bsm.mutex.RUnlock()

	for _, layer := range bsm.state.Layers {
		layer.mutex.RLock()
		for _, page := range layer.Pages {
			if page.ID == id {
				layer.mutex.RUnlock()
				return page, nil
			}
		}
		layer.mutex.RUnlock()
	}

	return Page{}, fmt.Errorf("page with ID %s not found", id)
}

// UpdatePage updates a page's information
func (bsm *BaseStateManager) UpdatePage(page Page) error {
	bsm.mutex.RLock()
	defer bsm.mutex.RUnlock()

	for _, layer := range bsm.state.Layers {
		layer.mutex.Lock()
		for i := range layer.Pages {
			if layer.Pages[i].ID == page.ID {
				// Update page fields but preserve Messages
				messages := layer.Pages[i].Messages
				layer.Pages[i] = page
				layer.Pages[i].Messages = messages
				layer.mutex.Unlock()
				return nil
			}
		}
		layer.mutex.Unlock()
	}

	return fmt.Errorf("page with ID %s not found", page.ID)
}

// UpdateMessage updates or adds a message to a page
func (bsm *BaseStateManager) UpdateMessage(pageID string, chatID int64, messageID int64, status string) error {
	bsm.mutex.RLock()
	defer bsm.mutex.RUnlock()

	for _, layer := range bsm.state.Layers {
		layer.mutex.Lock()
		for i := range layer.Pages {
			if layer.Pages[i].ID == pageID {
				// Check if message already exists
				found := false
				for j := range layer.Pages[i].Messages {
					if layer.Pages[i].Messages[j].ChatID == chatID &&
						layer.Pages[i].Messages[j].MessageID == messageID {
						// Update existing message
						layer.Pages[i].Messages[j].Status = status
						found = true
						break
					}
				}

				// Add new message if not found
				if !found {
					layer.Pages[i].Messages = append(layer.Pages[i].Messages, Message{
						ChatID:    chatID,
						MessageID: messageID,
						Status:    status,
						PageID:    pageID,
					})
				}

				layer.mutex.Unlock()
				return nil
			}
		}
		layer.mutex.Unlock()
	}

	return fmt.Errorf("page with ID %s not found", pageID)
}

// AddLayer adds a new layer of pages
func (bsm *BaseStateManager) AddLayer(pages []Page) error {
	if len(pages) == 0 {
		return nil // Nothing to add
	}

	bsm.mutex.Lock()
	defer bsm.mutex.Unlock()

	// Determine the depth of the new layer
	depth := 0
	if len(pages) > 0 && pages[0].ParentID != "" {
		// Find parent page to determine depth
		for _, layer := range bsm.state.Layers {
			for _, page := range layer.Pages {
				if page.ID == pages[0].ParentID {
					depth = layer.Depth + 1
					break
				}
			}
		}
	}

	// Check if a layer at this depth already exists
	var targetLayer *Layer
	for _, layer := range bsm.state.Layers {
		if layer.Depth == depth {
			targetLayer = layer
			break
		}
	}

	// Create a new layer if needed
	if targetLayer == nil {
		targetLayer = &Layer{
			Depth: depth,
			Pages: []Page{},
		}
		bsm.state.Layers = append(bsm.state.Layers, targetLayer)
	}

	// Add pages to the layer without duplicates
	targetLayer.mutex.Lock()
	defer targetLayer.mutex.Unlock()

	// Build a map of existing URLs for quick lookup
	existingURLs := make(map[string]bool)
	for _, layer := range bsm.state.Layers {
		for _, page := range layer.Pages {
			existingURLs[page.URL] = true
		}
	}

	// Add new pages that don't already exist
	addedCount := 0
	for _, page := range pages {
		if !existingURLs[page.URL] {
			// Ensure the page has a timestamp and ID
			if page.Timestamp.IsZero() {
				page.Timestamp = time.Now()
			}
			if page.ID == "" {
				page.ID = uuid.New().String()
			}

			targetLayer.Pages = append(targetLayer.Pages, page)
			existingURLs[page.URL] = true
			addedCount++
		}
	}

	log.Info().Msgf("Added %d new pages to layer at depth %d", addedCount, depth)
	return nil
}

// GetLayerByDepth retrieves all pages at a specific depth
func (bsm *BaseStateManager) GetLayerByDepth(depth int) ([]Page, error) {
	bsm.mutex.RLock()
	defer bsm.mutex.RUnlock()

	for _, layer := range bsm.state.Layers {
		if layer.Depth == depth {
			layer.mutex.RLock()
			// Create a copy of the pages slice to avoid concurrent access issues
			pagesCopy := make([]Page, len(layer.Pages))
			copy(pagesCopy, layer.Pages)
			layer.mutex.RUnlock()
			return pagesCopy, nil
		}
	}

	return nil, fmt.Errorf("no layer found at depth %d", depth)
}

// GetState returns a copy of the current state
func (bsm *BaseStateManager) GetState() State {
	bsm.mutex.RLock()
	defer bsm.mutex.RUnlock()

	// Create a deep copy to avoid concurrent access issues
	stateCopy := State{
		Layers:      make([]*Layer, len(bsm.state.Layers)),
		Metadata:    bsm.state.Metadata,
		LastUpdated: bsm.state.LastUpdated,
	}

	for i, layer := range bsm.state.Layers {
		layer.mutex.RLock()
		layerCopy := &Layer{
			Depth: layer.Depth,
			Pages: make([]Page, len(layer.Pages)),
		}
		copy(layerCopy.Pages, layer.Pages)
		layer.mutex.RUnlock()
		stateCopy.Layers[i] = layerCopy
	}

	return stateCopy
}

// SetState updates the entire state
func (bsm *BaseStateManager) SetState(state State) {
	bsm.mutex.Lock()
	defer bsm.mutex.Unlock()

	bsm.state = state
	bsm.state.LastUpdated = time.Now()
}

// GetPreviousCrawls returns a list of previous crawl IDs
func (bsm *BaseStateManager) GetPreviousCrawls() ([]string, error) {
	bsm.mutex.RLock()
	defer bsm.mutex.RUnlock()

	return bsm.state.Metadata.PreviousCrawlID, nil
}

// UpdateCrawlMetadata updates the crawl metadata
func (bsm *BaseStateManager) UpdateCrawlMetadata(crawlID string, metadata map[string]interface{}) error {
	bsm.mutex.Lock()
	defer bsm.mutex.Unlock()

	// Only update if it's the current crawl
	if bsm.state.Metadata.CrawlID != crawlID {
		return errors.New("cannot update metadata for a different crawl ID")
	}

	// Update metadata fields based on the provided map
	for key, value := range metadata {
		switch key {
		case "status":
			if status, ok := value.(string); ok {
				bsm.state.Metadata.Status = status
			}
		case "endTime":
			if endTime, ok := value.(time.Time); ok {
				bsm.state.Metadata.EndTime = endTime
			}
		case "previousCrawlID":
			if prevID, ok := value.(string); ok {
				// Append the new ID to the existing array
				bsm.state.Metadata.PreviousCrawlID = append(bsm.state.Metadata.PreviousCrawlID, prevID)
			} else if prevIDs, ok := value.([]string); ok {
				// For backward compatibility, also handle array input
				bsm.state.Metadata.PreviousCrawlID = append(bsm.state.Metadata.PreviousCrawlID, prevIDs...)
			}
		default:
			// Ignore unknown fields
		}
	}

	bsm.state.LastUpdated = time.Now()
	return nil
}

// GetMaxDepth returns the highest depth value among all layers
func (bsm *BaseStateManager) GetMaxDepth() (int, error) {
	bsm.mutex.RLock()
	defer bsm.mutex.RUnlock()

	if len(bsm.state.Layers) == 0 {
		return -1, errors.New("no layers found")
	}

	maxDepth := -1
	for _, layer := range bsm.state.Layers {
		if layer.Depth > maxDepth {
			maxDepth = layer.Depth
		}
	}

	return maxDepth, nil
}

// StorePost and StoreFile are left to specific implementations
// HasProcessedMedia and MarkMediaAsProcessed are left to specific implementations
// SaveState is left to specific implementations
// Close is left to specific implementations

package state

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	daprc "github.com/dapr/go-sdk/client"
	"github.com/researchaccelerator-hub/telegram-scraper/model"
	"github.com/rs/zerolog/log"
)

const (
	// Default component names
	defaultStateStoreName = "statestore"
	defaultStorageBinding = "crawlstorage"
)

// DaprStateManager implements StateManagementInterface using Dapr
type DaprStateManager struct {
	*BaseStateManager
	client         *daprc.Client
	stateStoreName string
	storageBinding string
	mediaCache     map[string]MediaCacheItem
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
	}, nil
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
	// First check if we have state from a previous crawl
	prevCrawls, err := dsm.GetPreviousCrawls()
	if err != nil {
		log.Warn().Err(err).Msg("Failed to get previous crawls, starting fresh")
	}

	startMetadata := map[string]interface{}{
		"status":          "running",
		"startTime":       time.Now(),
		"previousCrawlID": prevCrawls,
	}

	if err := dsm.UpdateCrawlMetadata(dsm.config.CrawlID, startMetadata); err != nil {
		log.Warn().Err(err).Msg("Failed to update crawl start metadata")
	}
	if len(prevCrawls) > 0 {
		// Try to load state from the most recent previous crawl
		for i := len(prevCrawls) - 1; i >= 0; i-- {
			prevID := prevCrawls[i]
			state, err := dsm.loadStateForCrawl(prevID)
			if err == nil && len(state.Layers) > 0 {
				// Found previous state, use it
				dsm.SetState(state)

				// Reset all pages to unfetched status
				for _, layer := range dsm.state.Layers {
					layer.mutex.Lock()
					for i := range layer.Pages {
						layer.Pages[i].Status = "unfetched"
						layer.Pages[i].Error = ""
					}
					layer.mutex.Unlock()
				}

				log.Info().Msgf("Loaded state from previous crawl %s with %d layers",
					prevID, len(dsm.state.Layers))

				// Update metadata to include this crawl
				dsm.state.Metadata.PreviousCrawlID = append(dsm.state.Metadata.PreviousCrawlID, dsm.config.CrawlExecutionID)

				// Save the updated state
				return dsm.SaveState()
			}
		}
	}

	// No previous state found or loadable, initialize with seed URLs
	err = dsm.BaseStateManager.Initialize(seedURLs)
	if err != nil {
		return err
	}

	// Save the initialized state
	return dsm.SaveState()
}

// In DaprStateManager
func (dsm *DaprStateManager) UpdateCrawlMetadata(crawlID string, metadata map[string]interface{}) error {
	// First update in-memory metadata using the base implementation
	if err := dsm.BaseStateManager.UpdateCrawlMetadata(crawlID, metadata); err != nil {
		return err
	}

	// Get the current metadata from the base state
	dsm.mutex.RLock()
	metadataCopy := dsm.state.Metadata
	dsm.mutex.RUnlock()

	// Marshal metadata to JSON
	metadataData, err := json.Marshal(metadataCopy)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Store metadata in DAPR using appropriate key
	metadataKey := dsm.config.CrawlID
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
	state := dsm.GetState()
	state.LastUpdated = time.Now()

	stateData, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	stateKey := dsm.getStateKey(dsm.config.CrawlExecutionID)
	err = (*dsm.client).SaveState(
		context.Background(),
		dsm.stateStoreName,
		stateKey,
		stateData,
		nil,
	)

	if err != nil {
		return fmt.Errorf("failed to save state to Dapr: %w", err)
	}

	log.Debug().Str("key", stateKey).Msg("State saved to Dapr")
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
	storagePath, err := dsm.generateStoragePath(
		channelID,
		fmt.Sprintf("posts/%s.jsonl", post.PostUID),
	)
	if err != nil {
		return err
	}

	// Encode data for Dapr binding
	encodedData := base64.StdEncoding.EncodeToString(postData)

	// Prepare metadata
	metadata := map[string]string{
		"path":      storagePath,
		"operation": "append",
	}

	// Send to Dapr binding
	req := daprc.InvokeBindingRequest{
		Name:      dsm.storageBinding,
		Operation: "create",
		Data:      []byte(encodedData),
		Metadata:  metadata,
	}

	_, err = (*dsm.client).InvokeBinding(context.Background(), &req)
	if err != nil {
		return fmt.Errorf("failed to store post via Dapr: %w", err)
	}

	log.Debug().Str("channel", channelID).Str("postUID", post.PostUID).Msg("Post stored")
	return nil
}

// StoreFile stores a file via Dapr
func (dsm *DaprStateManager) StoreFile(channelID string, sourceFilePath string, fileName string) (string, error) {
	// Check if the file exists
	if _, err := os.Stat(sourceFilePath); os.IsNotExist(err) {
		return "", fmt.Errorf("source file does not exist: %w", err)
	}

	// Read file content
	fileContent, err := os.ReadFile(sourceFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to read source file: %w", err)
	}

	// If fileName is empty, use the base name of the source file
	if fileName == "" {
		fileName = filepath.Base(sourceFilePath)
	}

	// Create storage path for media
	storagePath, err := dsm.generateStoragePath(channelID, fmt.Sprintf("media/%s", fileName))
	if err != nil {
		return "", err
	}

	// Encode data for Dapr binding
	encodedData := base64.StdEncoding.EncodeToString(fileContent)

	// Prepare metadata
	metadata := map[string]string{
		"path":      storagePath,
		"operation": "create",
	}

	// Send to Dapr binding
	req := daprc.InvokeBindingRequest{
		Name:      dsm.storageBinding,
		Operation: "create",
		Data:      []byte(encodedData),
		Metadata:  metadata,
	}

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
	return fmt.Sprintf("%s/media-cache", dsm.config.JobID)
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

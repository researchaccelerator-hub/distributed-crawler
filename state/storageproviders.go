package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/model"
	"github.com/rs/zerolog/log"
)

// LocalStorageProvider implements file system operations
type LocalStorageProvider struct {
	BasePath string
}

func NewLocalStorageProvider(basePath string) *LocalStorageProvider {
	return &LocalStorageProvider{
		BasePath: basePath,
	}
}

func (p *LocalStorageProvider) ReadFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func (p *LocalStorageProvider) WriteFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func (p *LocalStorageProvider) AppendToFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return err
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(data)
	return err
}

func (p *LocalStorageProvider) CreateDir(path string) error {
	return os.MkdirAll(path, os.ModePerm)
}

func (p *LocalStorageProvider) FileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (p *LocalStorageProvider) DeleteFile(path string) error {
	return os.Remove(path)
}

// LocalStateManager implements the StateManagementInterface using local filesystem
type LocalStateManager struct {
	*BaseStateManager
	storageProvider *LocalStorageProvider
	basePath        string
	mediaCache      map[string]MediaCacheItem
	mediaCacheMutex sync.RWMutex
}

// NewLocalStateManager creates a new local filesystem-backed state manager
func NewLocalStateManager(config Config) (*LocalStateManager, error) {
	base := NewBaseStateManager(config)

	// Ensure a valid base path is provided
	if config.LocalConfig == nil || config.LocalConfig.BasePath == "" {
		return nil, fmt.Errorf("local state manager requires a valid base path")
	}

	// Create the storage provider
	storageProvider := NewLocalStorageProvider(config.LocalConfig.BasePath)

	// Ensure the base directory exists
	storageDir := filepath.Join(config.LocalConfig.BasePath, config.CrawlID)
	if err := storageProvider.CreateDir(storageDir); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}

	lsm := &LocalStateManager{
		BaseStateManager: base,
		storageProvider:  storageProvider,
		basePath:         config.LocalConfig.BasePath,
		mediaCache:       make(map[string]MediaCacheItem),
	}

	// Load existing state if available
	if err := lsm.loadState(); err != nil {
		log.Warn().Err(err).Msg("Failed to load existing state, starting fresh")
	}

	return lsm, nil
}

// Initialize sets up the state with seed URLs
func (lsm *LocalStateManager) Initialize(seedURLs []string) error {
	// First check if we have existing state
	stateFile := lsm.getStateFilePath()
	exists, err := lsm.storageProvider.FileExists(stateFile)
	if err != nil {
		return fmt.Errorf("error checking for existing state: %w", err)
	}

	if exists {
		// We have existing state, load it instead of initializing
		if err := lsm.loadState(); err != nil {
			log.Warn().Err(err).Msg("Failed to load existing state, will initialize with seed URLs")
		} else {
			log.Info().Msg("Loaded existing state from disk")
			return nil
		}
	}

	// Initialize with seed URLs using the base implementation
	if err := lsm.BaseStateManager.Initialize(seedURLs); err != nil {
		return err
	}

	// Save the initial state
	return lsm.SaveState()
}

func (lsm *LocalStateManager) InitializeDiscoveredChannels() error {
	return fmt.Errorf("InitializeDiscoveredChannels not implemented for local state manager")
}

func (lsm *LocalStateManager) InitializeRandomWalkLayer() error {
	return fmt.Errorf("InitializeRandomWalkLayer not implemented for local state manager")
}

func (lsm *LocalStateManager) GetPagesFromLayerBuffer() ([]Page, error) {
	return []Page{}, fmt.Errorf("GetPagesFromLayerBuffer not implemented for local state manager")
}

func (lsm *LocalStateManager) WipeLayerBuffer(includeCurrentCrawl bool) error {
	return fmt.Errorf("WipeLayerBuffer not implemented for local state manager")
}

func (lsm *LocalStateManager) ExecuteDatabaseOperation(sqlQuery string, params []any) error {
	return fmt.Errorf("ExecuteDatabaseOperation not implemented for local state manager")
}

func (lsm *LocalStateManager) AddPageToLayerBuffer(page *Page) error {
	return fmt.Errorf(" not implemented for local state manager")
}

// SaveState persists the state to the filesystem
func (lsm *LocalStateManager) SaveState() error {
	state := lsm.GetState()
	stateData, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	// Save state to file
	stateFile := lsm.getStateFilePath()
	if err := lsm.storageProvider.WriteFile(stateFile, stateData); err != nil {
		return fmt.Errorf("failed to write state file: %w", err)
	}

	// Also save metadata separately for easier access
	metadataFile := lsm.getMetadataFilePath()
	metadataData, err := json.Marshal(lsm.metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if err := lsm.storageProvider.WriteFile(metadataFile, metadataData); err != nil {
		return fmt.Errorf("failed to write metadata file: %w", err)
	}

	log.Debug().Msgf("State saved to %s", stateFile)
	return nil
}

// StorePost saves a post to the filesystem
func (lsm *LocalStateManager) StorePost(channelID string, post model.Post) error {
	postData, err := json.Marshal(post)
	if err != nil {
		return fmt.Errorf("failed to marshal post: %w", err)
	}

	// Append newline for JSONL format
	postData = append(postData, '\n')

	// Create directory path
	postsDir := filepath.Join(lsm.basePath, lsm.config.CrawlID, channelID, "posts")
	if err := lsm.storageProvider.CreateDir(postsDir); err != nil {
		return fmt.Errorf("failed to create posts directory: %w", err)
	}

	// Append to JSONL file
	postsFile := filepath.Join(postsDir, "posts.jsonl")
	if err := lsm.storageProvider.AppendToFile(postsFile, postData); err != nil {
		return fmt.Errorf("failed to append post to file: %w", err)
	}

	log.Debug().Str("channel", channelID).Str("postID", post.PostUID).Msg("Post stored")
	return nil
}

// StoreFile stores a file in the filesystem
func (lsm *LocalStateManager) StoreFile(channelID string, sourceFilePath string, fileName string) (string, string, error) {
	// Check if source file exists
	if _, err := os.Stat(sourceFilePath); os.IsNotExist(err) {
		return "", "", fmt.Errorf("source file does not exist: %w", err)
	}

	// Read file data
	fileData, err := os.ReadFile(sourceFilePath)
	if err != nil {
		return "", "", fmt.Errorf("failed to read source file: %w", err)
	}

	// Generate filename if not provided
	if fileName == "" {
		fileName = filepath.Base(sourceFilePath)
	} else {
		// Add extension from source file if needed
		ext := filepath.Ext(sourceFilePath)
		if ext != "" && !strings.HasSuffix(fileName, ext) {
			fileName = fileName + ext
		}
	}

	// Create media directory
	mediaDir := filepath.Join(lsm.basePath, lsm.config.CrawlID, "media", channelID)
	if err := lsm.storageProvider.CreateDir(mediaDir); err != nil {
		return "", "", fmt.Errorf("failed to create media directory: %w", err)
	}

	// Write file
	destPath := filepath.Join(mediaDir, fileName)
	if err := lsm.storageProvider.WriteFile(destPath, fileData); err != nil {
		return "", "", fmt.Errorf("failed to write file: %w", err)
	}

	// Delete original file after copying
	if err := os.Remove(sourceFilePath); err != nil {
		log.Warn().Err(err).Str("path", sourceFilePath).Msg("Failed to delete source file")
	}

	// Return the relative path and the filename
	relPath := filepath.Join(lsm.config.CrawlID, "media", channelID, fileName)
	return relPath, fileName, nil
}

// HasProcessedMedia checks if media has been processed before
func (lsm *LocalStateManager) HasProcessedMedia(mediaID string) (bool, error) {
	// First check memory cache
	lsm.mediaCacheMutex.RLock()
	if _, exists := lsm.mediaCache[mediaID]; exists {
		lsm.mediaCacheMutex.RUnlock()
		return true, nil
	}
	lsm.mediaCacheMutex.RUnlock()

	// If not in memory, check filesystem cache
	mediaCacheFile := lsm.getMediaCacheFilePath()
	exists, err := lsm.storageProvider.FileExists(mediaCacheFile)
	if err != nil {
		return false, fmt.Errorf("error checking media cache file: %w", err)
	}

	if !exists {
		return false, nil
	}

	// Read and parse the cache file
	cacheData, err := lsm.storageProvider.ReadFile(mediaCacheFile)
	if err != nil {
		return false, fmt.Errorf("failed to read media cache file: %w", err)
	}

	var cache map[string]MediaCacheItem
	if err := json.Unmarshal(cacheData, &cache); err != nil {
		return false, fmt.Errorf("failed to parse media cache: %w", err)
	}

	// Update in-memory cache and check for the media ID
	lsm.mediaCacheMutex.Lock()
	defer lsm.mediaCacheMutex.Unlock()

	// Merge with existing cache
	for k, v := range cache {
		lsm.mediaCache[k] = v
	}

	_, exists = lsm.mediaCache[mediaID]
	return exists, nil
}

// MarkMediaAsProcessed marks media as processed
func (lsm *LocalStateManager) MarkMediaAsProcessed(mediaID string) error {
	// Add to memory cache
	lsm.mediaCacheMutex.Lock()
	lsm.mediaCache[mediaID] = MediaCacheItem{
		ID:        mediaID,
		FirstSeen: time.Now(),
	}

	// Create a copy of the cache for saving
	cacheCopy := make(map[string]MediaCacheItem)
	for k, v := range lsm.mediaCache {
		cacheCopy[k] = v
	}
	lsm.mediaCacheMutex.Unlock()

	// Save to file
	cacheData, err := json.Marshal(cacheCopy)
	if err != nil {
		return fmt.Errorf("failed to marshal media cache: %w", err)
	}

	mediaCacheFile := lsm.getMediaCacheFilePath()
	if err := lsm.storageProvider.WriteFile(mediaCacheFile, cacheData); err != nil {
		return fmt.Errorf("failed to write media cache file: %w", err)
	}

	return nil
}

// Close performs cleanup
func (lsm *LocalStateManager) Close() error {
	// Save state one last time
	if err := lsm.SaveState(); err != nil {
		log.Warn().Err(err).Msg("Failed to save state during close")
	}
	return nil
}

// UpdateMessage updates a message's status
func (lsm *LocalStateManager) UpdateMessage(pageID string, chatID int64, messageID int64, status string) error {
	// Use the base implementation
	err := lsm.BaseStateManager.UpdateMessage(pageID, chatID, messageID, status)
	if err != nil {
		return err
	}

	// Save state after update
	return lsm.SaveState()
}

// AddLayer adds a new layer of pages
func (lsm *LocalStateManager) AddLayer(pages []Page) error {
	// Use the base implementation
	err := lsm.BaseStateManager.AddLayer(pages)
	if err != nil {
		return err
	}

	// Save state after adding layer
	return lsm.SaveState()
}

// UpdatePage updates a page
func (lsm *LocalStateManager) UpdatePage(page Page) error {
	// Use the base implementation
	err := lsm.BaseStateManager.UpdatePage(page)
	if err != nil {
		return err
	}

	// Save state after update
	return lsm.SaveState()
}

// UpdateCrawlMetadata updates metadata
func (lsm *LocalStateManager) UpdateCrawlMetadata(crawlID string, metadata map[string]interface{}) error {
	// Use the base implementation
	err := lsm.BaseStateManager.UpdateCrawlMetadata(crawlID, metadata)
	if err != nil {
		return err
	}

	// Save metadata immediately
	metadataFile := lsm.getMetadataFilePath()
	metadataData, err := json.Marshal(lsm.metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if err := lsm.storageProvider.WriteFile(metadataFile, metadataData); err != nil {
		return fmt.Errorf("failed to write metadata file: %w", err)
	}

	return nil
}

// FindIncompleteCrawl checks for incomplete crawls
func (lsm *LocalStateManager) FindIncompleteCrawl(crawlID string) (string, bool, error) {
	// First check in memory
	execID, exists, err := lsm.BaseStateManager.FindIncompleteCrawl(crawlID)
	if err == nil && exists {
		return execID, true, nil
	}

	// Check if metadata file exists
	metadataFile := lsm.getMetadataFilePath()
	exists, err = lsm.storageProvider.FileExists(metadataFile)
	if err != nil {
		return "", false, fmt.Errorf("error checking metadata file: %w", err)
	}

	if !exists {
		return "", false, nil
	}

	// Read and parse the metadata file
	metadataData, err := lsm.storageProvider.ReadFile(metadataFile)
	if err != nil {
		return "", false, fmt.Errorf("failed to read metadata file: %w", err)
	}

	var metadata CrawlMetadata
	if err := json.Unmarshal(metadataData, &metadata); err != nil {
		return "", false, fmt.Errorf("failed to parse metadata: %w", err)
	}

	// Check if this crawl is incomplete
	if metadata.Status != "completed" && metadata.ExecutionID != "" {
		return metadata.ExecutionID, true, nil
	}

	// Look through any previous crawl IDs
	for _, prevID := range metadata.PreviousCrawlID {
		// Check if this previous crawl has incomplete state
		prevMetadataFile := filepath.Join(lsm.basePath, prevID, "metadata.json")
		exists, err := lsm.storageProvider.FileExists(prevMetadataFile)
		if err != nil || !exists {
			continue
		}

		prevMetadataData, err := lsm.storageProvider.ReadFile(prevMetadataFile)
		if err != nil {
			continue
		}

		var prevMetadata CrawlMetadata
		if err := json.Unmarshal(prevMetadataData, &prevMetadata); err != nil {
			continue
		}

		if prevMetadata.Status != "completed" && prevMetadata.ExecutionID != "" {
			return prevMetadata.ExecutionID, true, nil
		}
	}

	return "", false, nil
}

// ExportPagesToBinding exports pages to a file
func (lsm *LocalStateManager) ExportPagesToBinding(crawlID string) error {
	// Get all pages from memory
	lsm.mutex.RLock()
	pageMap := make(map[string]Page)
	for id, page := range lsm.pageMap {
		pageMap[id] = page
	}
	lsm.mutex.RUnlock()

	// Format as JSONL
	var pagesData []byte
	for _, page := range pageMap {
		pageJSON, err := json.Marshal(page)
		if err != nil {
			log.Warn().Err(err).Str("pageID", page.ID).Msg("Failed to marshal page")
			continue
		}
		pageJSON = append(pageJSON, '\n')
		pagesData = append(pagesData, pageJSON...)
	}

	// Create export directory
	timestamp := time.Now().Format("20060102-150405")
	exportDir := filepath.Join(lsm.basePath, crawlID, "exports")
	if err := lsm.storageProvider.CreateDir(exportDir); err != nil {
		return fmt.Errorf("failed to create export directory: %w", err)
	}

	// Write export file
	exportFile := filepath.Join(exportDir, fmt.Sprintf("pages-export-%s.jsonl", timestamp))
	if err := lsm.storageProvider.WriteFile(exportFile, pagesData); err != nil {
		return fmt.Errorf("failed to write export file: %w", err)
	}

	log.Info().Str("file", exportFile).Int("pageCount", len(pageMap)).Msg("Pages exported successfully")
	return nil
}

// Helper methods

// loadState loads the state from disk
func (lsm *LocalStateManager) loadState() error {
	stateFile := lsm.getStateFilePath()
	exists, err := lsm.storageProvider.FileExists(stateFile)
	if err != nil {
		return fmt.Errorf("error checking state file: %w", err)
	}

	if !exists {
		return fmt.Errorf("state file does not exist")
	}

	stateData, err := lsm.storageProvider.ReadFile(stateFile)
	if err != nil {
		return fmt.Errorf("failed to read state file: %w", err)
	}

	var state State
	if err := json.Unmarshal(stateData, &state); err != nil {
		return fmt.Errorf("failed to parse state: %w", err)
	}

	// Set the state in the base manager
	lsm.SetState(state)

	// Also load media cache
	mediaCacheFile := lsm.getMediaCacheFilePath()
	exists, err = lsm.storageProvider.FileExists(mediaCacheFile)
	if err == nil && exists {
		cacheData, err := lsm.storageProvider.ReadFile(mediaCacheFile)
		if err == nil {
			var cache map[string]MediaCacheItem
			if err := json.Unmarshal(cacheData, &cache); err == nil {
				lsm.mediaCacheMutex.Lock()
				lsm.mediaCache = cache
				lsm.mediaCacheMutex.Unlock()
			}
		}
	}

	return nil
}

// getStateFilePath returns the path to the state file
func (lsm *LocalStateManager) getStateFilePath() string {
	return filepath.Join(lsm.basePath, lsm.config.CrawlID, "state.json")
}

// getMetadataFilePath returns the path to the metadata file
func (lsm *LocalStateManager) getMetadataFilePath() string {
	return filepath.Join(lsm.basePath, lsm.config.CrawlID, "metadata.json")
}

// getMediaCacheFilePath returns the path to the media cache file
func (lsm *LocalStateManager) getMediaCacheFilePath() string {
	return filepath.Join(lsm.basePath, lsm.config.CrawlID, "media-cache.json")
}

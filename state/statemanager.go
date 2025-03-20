package state

import (
	"time"
)

// import (
//
//	"bufio"
//	"bytes"
//	"context"
//	"encoding/base64"
//	"encoding/json"
//	"fmt"
//	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
//	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
//	daprc "github.com/dapr/go-sdk/client"
//	"github.com/google/uuid"
//	"github.com/researchaccelerator-hub/telegram-scraper/model"
//	"github.com/rs/zerolog/log"
//	"io"
//	"net/url"
//	"os"
//	"path/filepath"
//	"strings"
//	"sync"
//	"time"
//
// )
//
//	type StateManagementInterface interface {
//		// Core methods used by the processor
//		UpdateStatePage(Page)
//		UpdateStateMessage(messageId int64, chatId int64, owner *Page, status string)
//		StoreState()
//		AppendLayerAndPersist(pages []*Page)
//
//		// Additional methods that might be called
//		StateSetup(seedlist []string) (DaprStateStore, error)
//		StoreLayers(layers []*Layer) error
//		StoreData(channelname string, post model.Post) error
//		UploadBlobFileAndDelete(channelid, rawURL, filePath, newname string) error
//		UploadStateToStorage(channelid string) error
//		UpdateCrawlManagement(management CrawlManagement) error
//		GetLayers(ids []string) ([]*Layer, error)
//		GetLastPreviousCrawlId() ([]string, error)
//		UpdateMediaCache(cache MediaCache) error
//		CheckItemExists(remoteId string) (bool, error)
//		GetMediaCache() (MediaCache, error)
//		AddToMediaCache(remoteId string) error
//	}
//type Message struct {
//	ChatId    int64
//	MessageId int64
//	Status    string
//	PageId    string
//}
//
//type Page struct {
//	URL       string
//	Depth     int
//	Timestamp time.Time
//	Status    string
//	Error     error
//	ID        string
//	ParentID  string
//	Messages  []Message
//}

// Layer represents a collection of pages at the same depth
//type Layer struct {
//	Depth int
//	Pages []Page
//	mutex sync.RWMutex
//}
//
//// Config holds the configuration for StateManager
//type Config struct {
//	StorageRoot      string
//	ContainerName    string
//	BlobNameRoot     string
//	JobID            string
//	CrawlID          string
//	CrawlExecutionID string
//	DAPREnabled      bool
//	MaxLayers        int
//}

//	type MediaCacheItem struct {
//		FirstSeen time.Time `json:"first_seen"`
//	}
//
//	type MediaCache struct {
//		Items map[string]*MediaCacheItem `json:"items"`
//	}
//
// // StateManager encapsulates state management with a configurable storage root prefix.
//
//	type StateManager struct {
//		config      Config
//		azureClient *azblob.Client
//		daprClient  *daprc.Client
//		StateStore  DaprStateStore
//		listFile    string
//	}
type DaprStateStore struct {
	Layers           []*Layer  `json:"layerList"`
	JobExecutionTime time.Time `json:"jobExecutionTime"`
}

//
//type CrawlManagement struct {
//	PreviousCrawlID []string  `json:"previousCrawlId"`
//	LastTriggerTime time.Time `json:"lastTriggerTime"`
//}
//
//// NewStateManager initializes a new StateManager with the given storage root prefix.
//func NewStateManager(config Config) (*StateManager, error) {
//	sm := &StateManager{
//		config:   config,
//		listFile: filepath.Join(config.StorageRoot, "list.txt"),
//	}
//	accountURL := os.Getenv("AZURE_STORAGE_ACCOUNT_URL")
//
//	// Initialize Azure client if we have the credentials
//	if config.ContainerName != "" && config.BlobNameRoot != "" && accountURL != "" {
//		client, err := createAzureClient(accountURL)
//		if err != nil {
//			return nil, fmt.Errorf("failed to create Azure client: %w", err)
//		}
//		sm.azureClient = client
//
//	} else if config.DAPREnabled {
//		client, err := daprc.NewClient()
//		if err != nil {
//			return nil, err
//		}
//		sm.daprClient = &client
//	}
//
//	return sm, nil
//}
//
//// createAzureClient creates a new Azure Blob Storage client
//func createAzureClient(accountURL string) (*azblob.Client, error) {
//	cred, err := azidentity.NewDefaultAzureCredential(nil)
//	if err != nil {
//		return nil, fmt.Errorf("failed to create Azure credential: %w", err)
//	}
//
//	client, err := azblob.NewClient(accountURL, cred, nil)
//	if err != nil {
//		return nil, fmt.Errorf("failed to create Azure Blob Storage client: %w", err)
//	}
//
//	return client, nil
//}
//
//type readSeekCloserWrapper struct {
//	*bytes.Reader
//}
//
//func (r readSeekCloserWrapper) Close() error {
//	return nil
//}
//
//func (sm *StateManager) listToLayer(list []string) []*Layer {
//	layers := make([]*Layer, 0)
//	pages := make([]Page, 0)
//	for _, l := range list {
//		page := Page{
//			URL:    l,
//			Depth:  0,
//			Status: "unfetched",
//			ID:     uuid.New().String(),
//		}
//
//		pages = append(pages, page)
//	}
//
//	layer := Layer{
//		Depth: 0,
//		Pages: pages,
//		mutex: sync.RWMutex{},
//	}
//	layers = append(layers, &layer)
//
//	return layers
//
//}
//
//func setPagesToUnfetched(layers []*Layer) {
//	for _, layer := range layers {
//		layer.mutex.Lock() // Add lock to prevent race conditions
//		for i := range layer.Pages {
//			// Use index to access and modify the actual element in the slice
//			layer.Pages[i].Status = "unfetched"
//		}
//		layer.mutex.Unlock() // Release the lock
//	}
//}
//
//// SeedSetup initializes the list file with the provided seed list if it does not exist,
//// and then loads the list from the file.
//func (sm *StateManager) StateSetup(seedlist []string) (DaprStateStore, error) {
//	prev, err := sm.GetLastPreviousCrawlId()
//	if err != nil {
//		return DaprStateStore{}, err
//	}
//
//	management := CrawlManagement{
//		PreviousCrawlID: append(prev, sm.config.CrawlExecutionID),
//		LastTriggerTime: time.Time{},
//	}
//
//	useAzure := sm.shouldUseAzure()
//	useDAPR := sm.shouldUseDapr()
//	layerzero := sm.listToLayer(seedlist)
//	if useAzure {
//		panic("not implemented")
//	} else if useDAPR {
//
//		//Does a previous run exist
//		prevlayers, err := sm.GetLayers(prev)
//		if err != nil {
//			return DaprStateStore{}, fmt.Errorf("failed to check if list blob exists: %w", err)
//		}
//		//If so don't seed
//		if prevlayers != nil {
//			sm.StateStore.Layers = prevlayers
//			setPagesToUnfetched(sm.StateStore.Layers)
//			err = sm.UpdateCrawlManagement(management)
//			if err != nil {
//				return DaprStateStore{}, err
//			}
//			return sm.StateStore, nil
//		}
//
//		// Need to seed the list
//		if err := sm.layersToState(layerzero); err != nil {
//			return DaprStateStore{}, fmt.Errorf("failed to seed list to Azure: %w", err)
//		}
//		sm.StateStore.Layers = layerzero
//		err = sm.UpdateCrawlManagement(management)
//		if err != nil {
//			return DaprStateStore{}, err
//		}
//		return sm.loadListFromDapr()
//	} else {
//		//// Check if list exists locally
//		//if _, err := os.Stat(sm.listFile); os.IsNotExist(err) {
//		//	if err := sm.seedList(layerzero); err != nil {
//		//		return nil, fmt.Errorf("failed to seed list locally: %w", err)
//		//	}
//		//}
//		//
//		//// Load list from local file
//		//return sm.loadList()
//		panic("not implemented")
//	}
//}
//
//func (sm *StateManager) StoreLayers(layers []*Layer) error {
//	if sm.shouldUseAzure() {
//		err := sm.layersToBlob(layers)
//		if err != nil {
//			return err
//		}
//	} else if sm.shouldUseDapr() {
//		err := sm.layersToState(layers)
//		if err != nil {
//			return err
//		}
//	} else {
//		panic("no filestore layers yet")
//	}
//	return nil
//}
//
//const stateStoreComponentName = "statestore"
//
//func (sm *StateManager) storageExists() (bool, error) {
//	client := *sm.daprClient
//	res, err := client.GetState(context.Background(), stateStoreComponentName, sm.config.ContainerName, nil)
//	if err != nil {
//		return false, err
//	}
//	if res.Value == nil {
//		return false, nil
//	}
//	return true, nil
//}
//
//func (sm *StateManager) generateStorageKey(contname, crawlexecutionid string) string {
//	return contname + "/" + crawlexecutionid
//}
//
//func (sm *StateManager) layersToState(seedlist []*Layer) error {
//	state := DaprStateStore{Layers: seedlist}
//	err := sm.saveDaprState(state)
//	return err
//}
//
//func (sm *StateManager) loadListFromDapr() (DaprStateStore, error) {
//	res, err := sm.loadDaprState()
//	return res, err
//}
//
//// loadListFromBlob downloads a list from an Azure Blob Storage container and returns it as a slice of strings.
//func (sm *StateManager) loadListFromBlob() (DaprStateStore, error) {
//	if sm.azureClient == nil {
//		return DaprStateStore{}, fmt.Errorf("Azure client not initialized")
//	}
//
//	// Create temporary file to download the blob
//	tmpFile, err := os.CreateTemp("", "list-*.txt")
//	if err != nil {
//		return DaprStateStore{}, fmt.Errorf("failed to create temporary file: %w", err)
//	}
//	defer os.Remove(tmpFile.Name()) // Clean up after reading
//	defer tmpFile.Close()
//
//	// Download blob directly to temp file
//	_, err = sm.azureClient.DownloadFile(
//		context.TODO(),
//		sm.config.ContainerName,
//		sm.getListBlobPath(),
//		tmpFile,
//		nil,
//	)
//
//	if err != nil {
//		if os.IsNotExist(err) {
//			return DaprStateStore{}, nil // Blob not found, return empty list
//		}
//		return DaprStateStore{}, fmt.Errorf("failed to download list from Azure: %w", err)
//	}
//
//	// Read and parse the downloaded file
//	if _, err := tmpFile.Seek(0, 0); err != nil {
//		return DaprStateStore{}, fmt.Errorf("failed to reset file pointer: %w", err)
//	}
//
//	var list DaprStateStore
//	scanner := bufio.NewScanner(tmpFile)
//	for scanner.Scan() {
//		line := scanner.Text()
//
//		// Create a Layer object from the line
//		_, err := ParseLayer(line)
//		if err != nil {
//			return DaprStateStore{}, fmt.Errorf("failed to parse layer from line: %w", err)
//		}
//		panic("not implemented")
//		//list = append(list, layer)
//	}
//
//	if err := scanner.Err(); err != nil {
//		return DaprStateStore{}, fmt.Errorf("failed to read downloaded list: %w", err)
//	}
//
//	log.Info().Msgf("Loaded %d layers from Azure Blob Storage", len(list.Layers))
//	return list, nil
//
//}
//
//func ParseLayer(line string) (*Layer, error) {
//	var layer Layer
//	err := json.Unmarshal([]byte(line), &layer)
//	if err != nil {
//		return nil, fmt.Errorf("failed to unmarshal layer: %w", err)
//	}
//	return &layer, nil
//}
//
//// seedList writes a list of items to a file, creating the file if it does not exist.
//func (sm *StateManager) seedList(items []*Layer) error {
//	// Ensure directory exists
//	if err := os.MkdirAll(filepath.Dir(sm.listFile), os.ModePerm); err != nil {
//		return fmt.Errorf("failed to create directory: %w", err)
//	}
//
//	// Marshal the entire slice of Layer objects to JSON
//	layersJSON, err := json.Marshal(items)
//	if err != nil {
//		return fmt.Errorf("failed to marshal layers: %w", err)
//	}
//
//	// Write the JSON to file in a single operation
//	if err := os.WriteFile(sm.listFile, layersJSON, 0644); err != nil {
//		return fmt.Errorf("failed to write to list file: %w", err)
//	}
//
//	log.Info().Msg("List seeded successfully.")
//	return nil
//}
//
//func (sm *StateManager) layersToBlob(items []*Layer) error {
//	if sm.azureClient == nil {
//		return fmt.Errorf("Azure client not initialized")
//	}
//
//	// Marshal the entire slice of Layer objects to JSON
//	layersJSON, err := json.Marshal(items)
//	if err != nil {
//		return fmt.Errorf("failed to marshal layers: %w", err)
//	}
//
//	// Upload to Azure Blob Storage
//	reader := bytes.NewReader(layersJSON)
//	_, err = sm.azureClient.UploadStream(
//		context.TODO(),
//		sm.config.ContainerName,
//		sm.getListBlobPath(),
//		reader,
//		nil,
//	)
//
//	if err != nil {
//		return fmt.Errorf("failed to upload list to Azure: %w", err)
//	}
//
//	log.Info().Msgf("Seed list uploaded to Azure: %s/%s", sm.config.ContainerName, sm.getListBlobPath())
//	return nil
//}
//
//// loadList reads the list of items from the list file and returns them as a slice of strings.
//func (sm *StateManager) loadList() ([]*Layer, error) {
//	data, err := os.ReadFile(sm.listFile)
//	if err != nil {
//		return nil, fmt.Errorf("failed to read list file: %w", err)
//	}
//
//	var layers []*Layer
//	if err := json.Unmarshal(data, &layers); err != nil {
//		return nil, fmt.Errorf("failed to unmarshal layers: %w", err)
//	}
//
//	log.Info().Msgf("Loaded %d layers from file", len(layers))
//	return layers, nil
//}
//
//func (sm *StateManager) loadDaprState() (DaprStateStore, error) {
//	client := *sm.daprClient
//	res, err := client.GetState(context.Background(), stateStoreComponentName, sm.generateStorageKey(sm.config.ContainerName, sm.config.CrawlExecutionID), nil)
//	if err != nil {
//		return DaprStateStore{}, err
//	}
//	var result DaprStateStore
//	err = json.Unmarshal(res.Value, &result)
//	return result, err
//}
//
//func (sm *StateManager) saveDaprState(store DaprStateStore) error {
//	sbytes, err := json.Marshal(store)
//	if err != nil {
//		return err
//	}
//	err = (*sm.daprClient).SaveState(context.Background(), stateStoreComponentName, sm.generateStorageKey(sm.config.ContainerName, sm.config.CrawlExecutionID), sbytes, nil)
//	if err != nil {
//		return err
//	}
//	return nil
//}
//
//func (sm *StateManager) saveDaprStateToFile(store DaprStateStore, channelid string) error {
//	sbytes, err := json.Marshal(store)
//	if err != nil {
//		return err
//	}
//
//	client := *sm.daprClient
//
//	data := base64.StdEncoding.EncodeToString(sbytes)
//	metadata := make(map[string]string)
//	byteArray := []byte(data)
//	fn, err := fetchFileNamingComponent(client, "crawlstorage")
//	if err != nil {
//		return err
//	}
//	fgen, err := generateStandardStorageLocation(sm.config.StorageRoot, sm.config.CrawlID, sm.config.CrawlExecutionID, channelid, "state.json", false)
//	if err != nil {
//		return err
//	}
//	metadata[fn] = fgen
//	req := daprc.InvokeBindingRequest{
//		Name:      "crawlstorage",
//		Operation: "create",
//		Data:      byteArray,
//		Metadata:  metadata,
//	}
//	_, err = client.InvokeBinding(context.Background(), &req)
//	if err != nil {
//		return err
//	}
//	return nil
//}
//
//func (sm *StateManager) savePageToFile(store Page, channelid string) error {
//	sbytes, err := json.Marshal(store)
//	if err != nil {
//		return err
//	}
//
//	client := *sm.daprClient
//
//	data := base64.StdEncoding.EncodeToString(sbytes)
//	metadata := make(map[string]string)
//	byteArray := []byte(data)
//	fn, err := fetchFileNamingComponent(client, "crawlstorage")
//	if err != nil {
//		return err
//	}
//	fgen, err := generateStandardStorageLocation(sm.config.StorageRoot, sm.config.CrawlID, sm.config.CrawlExecutionID, channelid, "state.json", false)
//	if err != nil {
//		return err
//	}
//	metadata[fn] = fgen
//	req := daprc.InvokeBindingRequest{
//		Name:      "crawlstorage",
//		Operation: "create",
//		Data:      byteArray,
//		Metadata:  metadata,
//	}
//	_, err = client.InvokeBinding(context.Background(), &req)
//	if err != nil {
//		return err
//	}
//	return nil
//}
//
//// StoreData saves a model.Post to a JSONL file under the channel's directory.
//func (sm *StateManager) StoreData(channelname string, post model.Post) error {
//	postData, err := json.Marshal(post)
//	if err != nil {
//		return fmt.Errorf("failed to marshal post: %w", err)
//	}
//
//	postData = append(postData, '\n')
//
//	if sm.shouldUseAzure() {
//		// Azure Blob Storage logic
//		panic("not yet implemented")
//	} else if sm.shouldUseDapr() {
//		client := *sm.daprClient
//
//		data := base64.StdEncoding.EncodeToString(postData)
//		metadata := make(map[string]string)
//		byteArray := []byte(data)
//		fn, err := fetchFileNamingComponent(client, "crawlstorage")
//		if err != nil {
//			return err
//		}
//		fgen, err := generateStandardStorageLocation(sm.config.StorageRoot, sm.config.CrawlID, sm.config.CrawlExecutionID, channelname, post.PostUID, false)
//		if err != nil {
//			return err
//		}
//		metadata[fn] = fgen
//		req := daprc.InvokeBindingRequest{
//			Name:      "crawlstorage",
//			Operation: "create",
//			Data:      byteArray,
//			Metadata:  metadata,
//		}
//		_, err = client.InvokeBinding(context.Background(), &req)
//		if err != nil {
//			return err
//		}
//		return nil
//	}
//
//	// Local Storage logic
//	channelDir, err := generateStandardStorageLocation(sm.config.StorageRoot, sm.config.CrawlID, sm.config.CrawlExecutionID, channelname, post.PostUID, true)
//
//	jsonlFile := filepath.Join(channelDir, "data.jsonl")
//	file, err := os.OpenFile(jsonlFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
//	if err != nil {
//		return fmt.Errorf("failed to open file %s: %w", jsonlFile, err)
//	}
//	defer file.Close()
//
//	if _, err := file.Write(postData); err != nil {
//		return fmt.Errorf("failed to write post to file %s: %w", jsonlFile, err)
//	}
//
//	log.Info().Msgf("Post successfully stored locally for channel %s", channelname)
//	return nil
//}
//
//// UploadBlobFileAndDelete uploads a local file to Azure Blob Storage and deletes it locally upon successful upload.
//func (sm *StateManager) UploadBlobFileAndDelete(channelid, rawURL, filePath, newname string) error {
//	// Check if the file exists
//	if _, err := os.Stat(filePath); os.IsNotExist(err) {
//		return fmt.Errorf("file does not exist: %w", err)
//	}
//
//	// Open the file for reading
//	file, err := os.OpenFile(filePath, os.O_RDONLY, 0)
//	if err != nil {
//		return fmt.Errorf("failed to open file: %w", err)
//	}
//	defer file.Close()
//
//	// Read file content into memory (we need this for both local and remote storage)
//	fileContent, err := io.ReadAll(file)
//	if err != nil {
//		return fmt.Errorf("failed to read file content: %w", err)
//	}
//
//	// Reset file pointer to beginning for potential reuse
//	if _, err := file.Seek(0, 0); err != nil {
//		return fmt.Errorf("failed to reset file pointer: %w", err)
//	}
//
//	filename := filepath.Base(filePath)
//
//	// Try Azure upload if it should be used
//	if sm.shouldUseAzure() && sm.azureClient != nil {
//		panic("not yet implemented")
//	} else if sm.shouldUseDapr() {
//		client := *sm.daprClient
//
//		data := base64.StdEncoding.EncodeToString(fileContent)
//		metadata := make(map[string]string)
//		byteArray := []byte(data)
//
//		fn, err := fetchFileNamingComponent(client, "crawlstorage")
//		if err != nil {
//			return err
//		}
//		if newname != "" {
//			ext := filepath.Ext(filename)
//
//			filename = newname + ext
//		}
//
//		fgen, err := generateStandardSharedStorageLocation(sm.config.StorageRoot, sm.config.CrawlID, "media", filename, false)
//		if err != nil {
//			return err
//		}
//		metadata[fn] = fgen
//		req := daprc.InvokeBindingRequest{
//			Name:      "crawlstorage",
//			Operation: "create",
//			Data:      byteArray,
//			Metadata:  metadata,
//		}
//		r, err := client.InvokeBinding(context.Background(), &req)
//		if err != nil {
//			return err
//		}
//		log.Info().Msgf("%v", r)
//	} else {
//
//		panic("not yet implemented")
//	}
//
//	// Remove the original file after successful processing
//	if err := os.Remove(filePath); err != nil {
//		return fmt.Errorf("failed to delete original file after upload: %w", err)
//	}
//
//	log.Info().Msg("Original file deleted successfully.")
//	return nil
//}
//
//// sanitizeURLForFilename creates a safe filename from a URL
//func sanitizeURLForFilename(url string) string {
//	// Remove protocol and domain parts
//	parts := strings.Split(url, "/")
//	relevantParts := parts[len(parts)-2:]
//
//	// Replace unsafe characters with underscores
//	sanitized := strings.Join(relevantParts, "_")
//	sanitized = strings.Map(func(r rune) rune {
//		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
//			return r
//		}
//		return '_'
//	}, sanitized)
//
//	// Limit filename length
//	if len(sanitized) > 50 {
//		sanitized = sanitized[:50]
//	}
//
//	return sanitized
//}
//
//// urlToBlobPath converts a raw URL string into a blob path
//func (sm *StateManager) urlToBlobPath(rawURL string) (string, error) {
//	// Parse the URL
//	parsedURL, err := url.Parse(rawURL)
//	if err != nil {
//		return "", fmt.Errorf("failed to parse URL: %w", err)
//	}
//
//	// Get the path and remove the leading slash
//	path := strings.TrimPrefix(parsedURL.Path, "/")
//
//	return path, nil
//}
//
//// blobExists checks if a blob exists in Azure
//func (sm *StateManager) blobExists(containerName, blobName string) (bool, error) {
//	if sm.azureClient == nil {
//		return false, fmt.Errorf("Azure client not initialized")
//	}
//
//	blobClient := sm.azureClient.ServiceClient().NewContainerClient(containerName).NewBlobClient(blobName)
//	_, err := blobClient.GetProperties(context.Background(), nil)
//
//	if err == nil {
//		return true, nil
//	}
//
//	if strings.Contains(err.Error(), "404") {
//		return false, nil
//	}
//
//	return false, err
//}
//
//// createAppendBlob creates a new append blob in Azure
//func (sm *StateManager) createAppendBlob(containerName, blobName string) error {
//	if sm.azureClient == nil {
//		return fmt.Errorf("Azure client not initialized")
//	}
//
//	appendBlobClient := sm.azureClient.ServiceClient().NewContainerClient(containerName).NewAppendBlobClient(blobName)
//	_, err := appendBlobClient.Create(context.Background(), nil)
//	return err
//}
//
//// appendToBlob appends data to an existing append blob in Azure
//func (sm *StateManager) appendToBlob(containerName, blobName string, data []byte) error {
//	if sm.azureClient == nil {
//		return fmt.Errorf("Azure client not initialized")
//	}
//
//	appendBlobClient := sm.azureClient.ServiceClient().NewContainerClient(containerName).NewAppendBlobClient(blobName)
//	reader := bytes.NewReader(data)
//	readSeekCloser := readSeekCloserWrapper{reader}
//
//	_, err := appendBlobClient.AppendBlock(context.Background(), readSeekCloser, nil)
//	return err
//}
//
//// Helper methods for blob paths
//func (sm *StateManager) getListBlobPath() string {
//	return filepath.Join(sm.config.BlobNameRoot, sm.config.JobID, "list.txt")
//}
//
//func (sm *StateManager) getProgressBlobPath() string {
//	// Incorporate crawlID into the progress file path for per-crawl tracking
//	return filepath.Join(
//		sm.config.BlobNameRoot,
//		sm.config.JobID,
//		"progress",
//		fmt.Sprintf("%s.txt", sm.config.CrawlID),
//	)
//}
//
//func (sm *StateManager) getChannelDataBlobPath(channelname string) string {
//	return filepath.Join(sm.config.BlobNameRoot, sm.config.JobID, sm.config.CrawlID, channelname+".jsonl")
//}
//
//// Helper method to determine if Azure storage should be used
//func (sm *StateManager) shouldUseAzure() bool {
//	return sm.config.ContainerName != "" && sm.config.BlobNameRoot != "" && sm.azureClient != nil
//}
//
//func (sm *StateManager) shouldUseDapr() bool {
//	return sm.config.ContainerName != "" && sm.daprClient != nil
//}
//
//func (sm *StateManager) UpdateStatePage(state Page) {
//	for i := range sm.StateStore.Layers {
//		layer := sm.StateStore.Layers[i]
//
//		// Lock the layer for reading and writing
//		layer.mutex.Lock()
//
//		// Search for the page with matching ID in this layer
//		for j := range layer.Pages {
//			if layer.Pages[j].ID == state.ID {
//				// Found the page, update its state
//				layer.Pages[j].URL = state.URL
//				layer.Pages[j].Depth = state.Depth
//				layer.Pages[j].Timestamp = state.Timestamp
//				layer.Pages[j].Status = state.Status
//				layer.Pages[j].Error = state.Error
//				layer.Pages[j].ParentID = state.ParentID
//				layer.Pages[j].Messages = state.Messages
//
//				// Unlock and return after update
//				layer.mutex.Unlock()
//				sm.StoreState()
//				return
//			}
//		}
//
//		// Unlock if page not found in this layer
//		layer.mutex.Unlock()
//	}
//
//}
//
//func (sm *StateManager) UpdateStateMessage(mId int64, chatId int64, owner *Page, status string) {
//	// First, find the owner page in the layers
//	if owner == nil {
//		// Handle nil owner case - optional depending on your requirements
//		return
//	}
//
//	for i := range sm.StateStore.Layers {
//		layer := sm.StateStore.Layers[i]
//
//		// Lock the layer
//		layer.mutex.Lock()
//
//		// Search for the page with matching ID
//		for j := range layer.Pages {
//			if layer.Pages[j].ID == owner.ID {
//				// Found the owner page, now find the message by mId and chatId
//				found := false
//				for k := range layer.Pages[j].Messages {
//					if layer.Pages[j].Messages[k].MessageId == mId &&
//						layer.Pages[j].Messages[k].ChatId == chatId {
//						// Update the message status
//						layer.Pages[j].Messages[k].Status = status
//						found = true
//						break
//					}
//				}
//
//				// If message not found, append a new one
//				if !found {
//					newMessage := Message{
//						ChatId:    chatId,
//						MessageId: mId,
//						Status:    status,
//						PageId:    owner.ID,
//					}
//					layer.Pages[j].Messages = append(layer.Pages[j].Messages, newMessage)
//				}
//
//				// Unlock and return after update
//				layer.mutex.Unlock()
//				sm.StoreState()
//				return
//			}
//		}
//
//		// Unlock if page not found in this layer
//		layer.mutex.Unlock()
//	}
//
//	// If we get here, the owner page wasn't found in any layer
//	// Depending on your requirements, you might want to:
//	// 1. Log an error
//	// 2. Create a new page with this message
//	// 3. Or other error handling
//}
//
//func (sm *StateManager) AppendLayerAndPersist(pages []*Page) {
//	if len(pages) == 0 {
//		return // Nothing to add
//	}
//
//	// Get the depth of the new layer (assuming all pages have the same parent depth)
//	parentDepth := 0
//	if pages[0] != nil && pages[0].ParentID != "" {
//		// Find the parent's depth
//		for _, layer := range sm.StateStore.Layers {
//			for _, page := range layer.Pages {
//				if page.ID == pages[0].ParentID {
//					parentDepth = page.Depth
//					break
//				}
//			}
//			if parentDepth != -1 {
//				break
//			}
//		}
//	}
//
//	// Calculate new depth (one level deeper than parent)
//	newDepth := parentDepth + 1
//
//	// Check if the layer for this depth already exists
//	var targetLayer *Layer
//	for i := range sm.StateStore.Layers {
//		if sm.StateStore.Layers[i].Depth == newDepth {
//			targetLayer = sm.StateStore.Layers[i]
//			break
//		}
//	}
//
//	// If layer doesn't exist, create it
//	if targetLayer == nil {
//		newLayer := Layer{
//			Depth: newDepth,
//			Pages: []Page{},
//		}
//		sm.StateStore.Layers = append(sm.StateStore.Layers, &newLayer)
//		targetLayer = sm.StateStore.Layers[len(sm.StateStore.Layers)-1]
//	}
//
//	addPagesWithoutDuplicates(sm, pages, targetLayer, newDepth)
//	sm.StoreState()
//
//}
//
//func addPagesWithoutDuplicates(sm *StateManager, pages []*Page, targetLayer *Layer, newDepth int) {
//	// Create a map for faster lookups of existing URLs
//	existingURLs := make(map[string]bool)
//
//	// First collect all existing URLs with a single lock per layer
//	for i := range sm.StateStore.Layers {
//		layer := sm.StateStore.Layers[i]
//
//		// Lock the layer for reading
//		layer.mutex.RLock()
//
//		// Add all URLs from this layer to our map
//		for _, existingPage := range layer.Pages {
//			existingURLs[existingPage.URL] = true
//		}
//
//		layer.mutex.RUnlock()
//	}
//
//	// Now we can process new pages without needing to lock/unlock repeatedly
//	pagesToAdd := []Page{}
//	newPageURLs := make(map[string]bool) // Track URLs of new pages we're adding
//
//	for _, newPage := range pages {
//		if newPage == nil {
//			continue
//		}
//
//		// Check if this URL already exists in existing pages or in our new pages list
//		if !existingURLs[newPage.URL] && !newPageURLs[newPage.URL] {
//			// Convert *Page to Page
//			pageToAdd := Page{
//				URL:       newPage.URL,
//				Depth:     newDepth,
//				Timestamp: time.Now(),
//				Status:    "unfetched",
//				Error:     nil,
//				ID:        newPage.ID,
//				ParentID:  newPage.ParentID,
//				Messages:  newPage.Messages,
//			}
//
//			pagesToAdd = append(pagesToAdd, pageToAdd)
//			newPageURLs[newPage.URL] = true // Mark this URL as being added
//		}
//	}
//
//	// Finally, add all new pages at once
//	if len(pagesToAdd) > 0 {
//		targetLayer.mutex.Lock()
//		targetLayer.Pages = append(targetLayer.Pages, pagesToAdd...)
//		targetLayer.mutex.Unlock()
//	}
//}
//func (sm *StateManager) StoreState() {
//	if sm.shouldUseDapr() {
//		err := sm.saveDaprState(sm.StateStore)
//		if err != nil {
//			return
//		}
//	} else {
//		panic("not implemented")
//	}
//}
//func GetPageFromLayers(layers []*Layer, pageID string) (Page, error) {
//	// Iterate through all layers
//	for _, layer := range layers {
//		// Iterate through all pages in the current layer
//		for i := range layer.Pages {
//			// Check if this is the page we're looking for
//			if layer.Pages[i].URL == pageID {
//				return layer.Pages[i], nil
//			}
//		}
//	}
//
//	// If we get here, the page wasn't found in any layer
//	return Page{}, fmt.Errorf("page with ID %s not found in any layer", pageID)
//}
//func (sm *StateManager) UploadStateToStorage(channelid string) error {
//	state := sm.StateStore
//	p, err := GetPageFromLayers(state.Layers, channelid)
//	if err != nil {
//		return err
//	}
//	err = sm.savePageToFile(p, channelid)
//	if err != nil {
//		return err
//	}
//	return nil
//}
//func (sm *StateManager) UpdateCrawlManagement(management CrawlManagement) error {
//
//	if sm.shouldUseDapr() {
//		sbytes, err := json.Marshal(management)
//		if err != nil {
//			return err
//		}
//		err = (*sm.daprClient).SaveState(context.Background(), stateStoreComponentName, sm.config.ContainerName, sbytes, nil)
//		if err != nil {
//			return err
//		}
//		return nil
//	} else {
//		panic("not implemented")
//	}
//}
//
//func (sm *StateManager) GetLayers(ids []string) ([]*Layer, error) {
//	// Iterate through ids starting from the most recent (last) one
//	for i := len(ids) - 1; i >= 0; i-- {
//		id := ids[i]
//		resp, err := (*sm.daprClient).GetState(context.Background(), stateStoreComponentName, sm.generateStorageKey(sm.config.ContainerName, id), nil)
//		if err != nil {
//			// If there's an error, continue to the next id rather than failing immediately
//			continue
//		}
//
//		if resp.Value != nil {
//			var result DaprStateStore
//			err = json.Unmarshal(resp.Value, &result)
//			if err != nil {
//				// If we can't unmarshal, try the next id
//				continue
//			}
//
//			// If we found layers, return them
//			if result.Layers != nil && len(result.Layers) > 0 {
//				return result.Layers, nil
//			}
//		}
//	}
//
//	// If we went through all ids and found nothing, return nil
//	return nil, nil
//}
//
//func (sm *StateManager) GetLastPreviousCrawlId() ([]string, error) {
//	resp, err := (*sm.daprClient).GetState(context.Background(), stateStoreComponentName, sm.config.ContainerName, nil)
//	if err != nil {
//		return nil, err
//	}
//	if resp.Value != nil {
//		var result CrawlManagement
//		err = json.Unmarshal(resp.Value, &result)
//		return result.PreviousCrawlID, err
//	}
//	return nil, nil
//}
//
//func (sm *StateManager) AddToMediaCache(remoteId string) error {
//	cache, err := sm.GetMediaCache()
//	if err != nil {
//		return err
//	}
//
//	if cache.Items == nil {
//		cache = MediaCache{
//			Items: make(map[string]*MediaCacheItem),
//		}
//	}
//	cache.Items[remoteId] = &MediaCacheItem{
//		FirstSeen: time.Now(),
//	}
//
//	return sm.UpdateMediaCache(cache)
//
//}
//
//func (sm *StateManager) GetMediaCache() (MediaCache, error) {
//	resp, err := (*sm.daprClient).GetState(context.Background(), stateStoreComponentName, sm.config.ContainerName+"_MediaCache", nil)
//	if err != nil {
//		return MediaCache{}, err
//	}
//	if resp.Value != nil {
//		var result MediaCache
//		err = json.Unmarshal(resp.Value, &result)
//		return result, err
//	}
//	return MediaCache{}, nil
//}
//
//func (sm *StateManager) CheckItemExists(remoteId string) (bool, error) {
//	cache, err := sm.GetMediaCache()
//	if err != nil {
//		return false, err
//	}
//
//	_, exists := cache.Items[remoteId]
//
//	return exists, nil
//}
//
//func (sm *StateManager) UpdateMediaCache(cache MediaCache) error {
//	b, err := json.Marshal(cache)
//	if err != nil {
//		return err
//	}
//	err = (*sm.daprClient).SaveState(context.Background(), stateStoreComponentName, sm.config.ContainerName+"_MediaCache", b, nil)
//	if err != nil {
//		return err
//	}
//	return nil
//}

package state

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/rs/zerolog/log"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"tdlib-scraper/model"
)

// StateManager encapsulates state management with a configurable storage root prefix.
type StateManager struct {
	storageRoot  string
	listFile     string
	progressFile string
	crawlid      string
}

// NewStateManager initializes a new StateManager with the given storage root prefix.
func NewStateManager(storageRoot string, crawlid string) *StateManager {
	return &StateManager{
		storageRoot:  storageRoot,
		listFile:     storageRoot + "/list.txt",
		progressFile: storageRoot + "/progress.txt",
		crawlid:      crawlid,
	}
}

type readSeekCloserWrapper struct {
	*bytes.Reader
}

func (r readSeekCloserWrapper) Close() error {
	return nil
}

// SeedSetup initializes the list file with the provided seed list if it does not exist,
// and then loads the list from the file.
//
// Parameters:
//   - seedlist: A slice of strings representing the initial items to seed the list file.
//
// Returns:
//   - A slice of strings containing the loaded list from the file.
//   - An error if there is a failure in loading the list.
func (sm *StateManager) SeedSetup(seedlist []string) ([]string, error) {
	containerName := os.Getenv("CONTAINER_NAME")
	blobName := os.Getenv("BLOB_NAME")
	accountUrl := os.Getenv("AZURE_STORAGE_ACCOUNT_URL")
	// Check if list needs to be seeded
	if _, err := os.Stat(sm.listFile); os.IsNotExist(err) {
		if containerName != "" && blobName != "" && accountUrl != "" {
			err := sm.seedListToBlob(seedlist)
			if err != nil {
				log.Fatal().Err(err).Msg("Failed to seed list to Azure Blob Storage")
			}
			return sm.loadListFromBlob() // Read directly from blob after seeding
		} else {
			sm.seedList(seedlist) // Seed to local file
		}
	}

	// Load list from either local file or Azure Blob Storage
	if containerName != "" && blobName != "" && accountUrl != "" {
		return sm.loadListFromBlob()
	}

	return sm.loadList() // Fallback to local file loading
}

func (sm *StateManager) loadListFromBlob() ([]string, error) {
	client, err := sm.createAZClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure Blob client: %w", err)
	}

	containerName := os.Getenv("CONTAINER_NAME")
	jobid := os.Getenv("JOB_ID")
	blobName := os.Getenv("BLOB_NAME") + "/" + jobid + "/list.txt"

	// Create temporary file to download the blob
	tmpFile, err := os.CreateTemp("", "progress-*.txt")
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer os.Remove(tmpFile.Name()) // Clean up after reading
	defer tmpFile.Close()

	// Download blob directly to temp file
	_, err = client.DownloadFile(context.TODO(), containerName, blobName, tmpFile, nil)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // Blob not found, return empty list
		}
		return nil, fmt.Errorf("failed to download list from Azure Blob: %w", err)
	}

	// Read and parse the downloaded file
	data, err := os.ReadFile(tmpFile.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to read temporary file: %w", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	log.Info().Msgf("Loaded %d items from Azure Blob Storage", len(lines))
	return lines, nil
}

// seedList writes a list of items to a file, creating the file if it does not exist.
//
// Parameters:
//   - items: A slice of strings representing the items to be written to the list file.
//
// This function logs a fatal error and terminates the program if it fails to create the file
// or write any item to the file. On successful completion, it logs an informational message.
func (sm *StateManager) seedList(items []string) {
	file, err := os.Create(sm.listFile)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create list file")
	}
	defer file.Close()

	for _, item := range items {
		_, err := file.WriteString(item + "\n")
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to write to list file")
		}
	}
	log.Info().Msg("List seeded successfully.")
}

func (sm *StateManager) seedListToBlob(items []string) error {
	client, err := sm.createAZClient()
	if err != nil {
		return fmt.Errorf("failed to create Azure Blob client: %w", err)
	}

	containerName := os.Getenv("CONTAINER_NAME")
	jobid := os.Getenv("JOB_ID")

	blobName := os.Getenv("BLOB_NAME") + "/" + jobid + "/list.txt"

	// Create temporary file to store seed list
	tmpFile, err := os.CreateTemp("", "seedlist-*.txt")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer os.Remove(tmpFile.Name()) // Cleanup after upload
	defer tmpFile.Close()

	// Write seed list to the temporary file
	for _, item := range items {
		_, err := tmpFile.WriteString(item + "\n")
		if err != nil {
			return fmt.Errorf("failed to write to temporary file: %w", err)
		}
	}

	// Reset file pointer to the start before upload
	_, err = tmpFile.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("failed to reset file pointer: %w", err)
	}

	// Upload the temporary file to Azure Blob Storage
	_, err = client.UploadFile(context.TODO(), containerName, blobName, tmpFile, nil)
	if err != nil {
		return fmt.Errorf("failed to upload list to Azure Blob Storage: %w", err)
	}

	log.Info().Msgf("Seed list uploaded to Azure Blob Storage: %s/%s", containerName, blobName)
	return nil
}

// loadList reads the list of items from the list file and returns them as a slice of strings.
//
// Returns:
//   - A slice of strings containing the items from the list file.
//   - An error if there is a failure in opening the file or reading its contents.
func (sm *StateManager) loadList() ([]string, error) {
	file, err := os.Open(sm.listFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var list []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		list = append(list, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return list, nil
}

// LoadProgress retrieves the current progress index from the progress file.
//
// Returns:
//   - An integer representing the progress index.
//   - An error if there is a failure in reading the file or converting its contents to an integer.
//
// If the progress file does not exist, it returns 0 and no error, indicating to start from the beginning.
func (sm *StateManager) LoadProgress() (int, error) {
	containerName := os.Getenv("CONTAINER_NAME")
	jobid := os.Getenv("JOB_ID")

	blobName := os.Getenv("BLOB_NAME") + "/" + jobid + "/progress.txt"

	// If Azure environment variables are set, load from Azure Blob Storage
	if containerName != "" && blobName != "" {
		return sm.loadProgressFromBlob()
	}

	// Fallback to local file loading
	if _, err := os.Stat(sm.progressFile); os.IsNotExist(err) {
		return 0, nil // Start from the beginning if no progress file
	}

	data, err := os.ReadFile(sm.progressFile)
	if err != nil {
		return 0, err
	}

	progress, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, err
	}

	return progress, nil
}

// SaveProgress writes the given progress index to the progress file.
//
// Parameters:
//   - index: An integer representing the progress index to be saved.
//
// Returns:
//   - An error if there is a failure in creating the file or writing the index to it.
func (sm *StateManager) SaveProgress(index int) error {
	containerName := os.Getenv("CONTAINER_NAME")
	jobid := os.Getenv("JOB_ID")

	blobName := os.Getenv("BLOB_NAME") + "/" + jobid + "/progress.txt"

	// If Azure environment variables are set, save to Azure Blob Storage
	if containerName != "" && blobName != "" {
		return sm.saveProgressToBlob(index)
	}

	// Fallback to local file saving
	file, err := os.Create(sm.progressFile)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(strconv.Itoa(index) + "\n")
	return err
}

func (sm *StateManager) loadProgressFromBlob() (int, error) {
	client, err := sm.createAZClient()
	if err != nil {
		return 0, fmt.Errorf("failed to create Azure Blob client: %w", err)
	}

	containerName := os.Getenv("CONTAINER_NAME")
	jobid := os.Getenv("JOB_ID")

	blobName := os.Getenv("BLOB_NAME") + "/" + jobid + "/progress.txt"

	// Create a temporary file to download the blob
	tmpFile, err := os.CreateTemp("", "progress-*.txt")
	if err != nil {
		return 0, fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer os.Remove(tmpFile.Name()) // Clean up after reading
	defer tmpFile.Close()

	// Download blob to file
	_, err = client.DownloadFile(context.TODO(), containerName, blobName, tmpFile, nil)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil // Blob not found, start from zero
		}
		return 0, fmt.Errorf("failed to download progress file: %w", err)
	}

	// Read and parse the downloaded file
	data, err := os.ReadFile(tmpFile.Name())
	if err != nil {
		return 0, fmt.Errorf("failed to read progress data: %w", err)
	}

	progress, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, fmt.Errorf("invalid progress format: %w", err)
	}

	return progress, nil
}

func (sm *StateManager) saveProgressToBlob(index int) error {
	client, err := sm.createAZClient()
	if err != nil {
		return fmt.Errorf("failed to create Azure Blob client: %w", err)
	}

	containerName := os.Getenv("CONTAINER_NAME")
	jobid := os.Getenv("JOB_ID")

	blobName := os.Getenv("BLOB_NAME") + "/" + jobid + "/progress.txt"

	// Write progress to an in-memory buffer
	data := []byte(strconv.Itoa(index) + "\n")
	reader := bytes.NewReader(data)

	_, err = client.UploadStream(context.TODO(), containerName, blobName, reader, nil)
	if err != nil {
		return fmt.Errorf("failed to upload progress to Azure Blob Storage: %w", err)
	}

	log.Info().Msgf("Progress saved to Azure Blob Storage: %d", index)
	return nil
}
func (sm *StateManager) createAZClient() (*azblob.Client, error) {
	// Check if environment variables for Azure Blob Storage are set
	containerName := os.Getenv("CONTAINER_NAME")
	blobName := os.Getenv("BLOB_NAME")
	accountUrl := os.Getenv("AZURE_STORAGE_ACCOUNT_URL")

	if containerName != "" && blobName != "" {
		// Azure Blob Storage logic
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure credential: %w", err)
		}
		client, err := azblob.NewClient(accountUrl, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure Blob Storage client: %w", err)
		}
		return client, nil
	}
	return nil, nil
}

// StoreData saves a model.Post to a JSONL file under the channel's directory in storageRoot/crawls/crawlid.
//
// Parameters:
//   - channelname: The name of the channel associated with the post.
//   - post: The model.Post object to be saved.
//
// Returns:
//   - An error if there is a failure in creating the file or writing the post to it.
func (sm *StateManager) StoreData(crawlid, channelname string, post model.Post) error {
	postData, err := json.Marshal(post)
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal post to JSON")
		return fmt.Errorf("failed to marshal post: %w", err)
	}

	// Check if environment variables for Azure Blob Storage are set
	containerName := os.Getenv("CONTAINER_NAME")
	blobName := os.Getenv("BLOB_NAME")
	jobid := os.Getenv("JOB_ID")

	if containerName != "" && blobName != "" {
		// Azure Blob Storage logic
		client, err := sm.createAZClient()
		// Use a function to append the data to an existing blob or create a new one if not present
		blobPath := filepath.Join(blobName, jobid, crawlid, channelname+".jsonl")
		err = sm.appendToBlob(client, containerName, blobPath, postData)
		if err != nil {
			return fmt.Errorf("failed to upload post to Azure Blob Storage: %w", err)
		}

		log.Info().Msgf("Post successfully uploaded to Azure Blob Storage for channel %s", channelname)
		return nil
	}

	// Local Storage logic
	channelDir := filepath.Join(sm.storageRoot, "crawls", sm.crawlid, channelname)
	if err := os.MkdirAll(channelDir, os.ModePerm); err != nil {
		log.Error().Err(err).Msg("Failed to create channel directory")
		return fmt.Errorf("failed to create directory for channel %s: %w", channelname, err)
	}

	jsonlFile := filepath.Join(channelDir, "data.jsonl")
	file, err := os.OpenFile(jsonlFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Error().Err(err).Msg("Failed to open JSONL file")
		return fmt.Errorf("failed to open file %s: %w", jsonlFile, err)
	}
	defer file.Close()

	_, err = file.WriteString(string(postData) + "\n")
	if err != nil {
		log.Error().Err(err).Msg("Failed to write to JSONL file")
		return fmt.Errorf("failed to write post to file %s: %w", jsonlFile, err)
	}

	log.Info().Msgf("Post successfully stored locally for channel %s", channelname)
	return nil
}

// blobExists checks if a blob exists in Azure Blob Storage.
func (sm *StateManager) blobExists(client *azblob.Client, containerName, blobName string) (bool, error) {
	// Get blob client
	blobClient := client.ServiceClient().NewContainerClient(containerName).NewBlobClient(blobName)

	// Get blob properties
	_, err := blobClient.GetProperties(context.Background(), nil)

	// If err is nil, blob exists
	if err == nil {
		return true, nil
	}

	// Return any other error
	return false, fmt.Errorf("error checking blob existence: %w", err)
}

// uploadDataToBlob uploads or overwrites data to an Azure Blob.
func (sm *StateManager) uploadDataToBlob(client *azblob.Client, containerName, blobName, data string) error {
	dataReader := strings.NewReader(data)
	_, err := client.UploadStream(context.TODO(), containerName, blobName, dataReader, nil)
	return err
}

func (sm *StateManager) appendToBlob(client *azblob.Client, containerName, blobName string, data []byte) error {
	// Get container client
	containerClient := client.ServiceClient().NewContainerClient(containerName)

	// Get append blob client
	appendBlobClient := containerClient.NewAppendBlobClient(blobName)

	// Check if blob exists, if not create it
	_, err := appendBlobClient.GetProperties(context.Background(), nil)
	if err != nil {
		// Check if it's a 404 error
		if strings.Contains(err.Error(), "404") {
			// Create the append blob
			_, err = appendBlobClient.Create(context.Background(), nil)
			if err != nil {
				return fmt.Errorf("failed to create append blob: %w", err)
			}
		} else {
			return fmt.Errorf("error checking blob existence: %w", err)
		}
	}

	// Append the data
	reader := bytes.NewReader(data)
	readSeekCloser := readSeekCloserWrapper{reader}

	// Append the data
	_, err = appendBlobClient.AppendBlock(context.Background(), readSeekCloser, nil)
	if err != nil {
		return fmt.Errorf("failed to append block: %w", err)
	}

	return nil
}

// Helper function to append a string
func (sm *StateManager) appendStringToBlob(client *azblob.Client, containerName, blobName, content string) error {
	return sm.appendToBlob(client, containerName, blobName, []byte(content))
}

// Helper function to append from a reader
func (sm *StateManager) appendReaderToBlob(client *azblob.Client, containerName, blobName string, reader io.Reader) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read data: %w", err)
	}
	return sm.appendToBlob(client, containerName, blobName, data)
}

func (sm *StateManager) UploadBlobFileAndDelete(crawlid, channelid, rawURL, filePath string) error {
	// Open the file for reading
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()
	client, err := sm.createAZClient()
	if err != nil {
		log.Error().Stack().Err(err).Msg("Failed to create Azure Blob Storage client")
	}

	containerName := os.Getenv("CONTAINER_NAME")
	fp, _ := sm.urlToBlobPath(rawURL)
	filename := filepath.Base(filePath)
	jobid := os.Getenv("JOB_ID")

	fp = fp + "_" + filename
	blobName := os.Getenv("BLOB_NAME") + "/" + jobid + "/" + crawlid + "/media/" + channelid + "/" + fp
	// Upload the file to the specified container with the specified blob name
	_, err = client.UploadFile(context.TODO(), containerName, blobName, file, nil)
	if err != nil {
		log.Error().Stack().Err(err).Msg("Failed to upload file to Azure Blob Storage")
		return fmt.Errorf("failed to upload file to Azure Blob Storage: %w", err)
	}

	// Remove the local file upon successful upload
	err = os.Remove(filePath)
	if err != nil {
		log.Error().Stack().Err(err).Msg("Failed to remove file from local Storage")
		return fmt.Errorf("failed to delete local file after upload: %w", err)
	}

	log.Info().Msg("File uploaded and deleted successfully.")
	return nil
}
func (sm *StateManager) urlToBlobPath(rawURL string) (string, error) {
	// Parse the URL
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse URL: %w", err)
	}

	// Get the path and remove the leading slash
	path := strings.TrimPrefix(parsedURL.Path, "/")

	// Replace slashes with a desired separator, like underscores or keep them
	blobPath := strings.ReplaceAll(path, "/", "/") // Keeps slashes for folder structure

	return blobPath, nil
}

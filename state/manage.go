package state

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/rs/zerolog/log"
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
	// Seed list if needed
	if _, err := os.Stat(sm.listFile); os.IsNotExist(err) {
		sm.seedList(seedlist)
	}

	// Load list
	list, err := sm.loadList()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load list")
	}

	return list, nil
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
	file, err := os.Create(sm.progressFile)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(strconv.Itoa(index) + "\n")
	return err
}

// StoreData saves a model.Post to a JSONL file under the channel's directory in storageRoot/crawls/crawlid.
//
// Parameters:
//   - channelname: The name of the channel associated with the post.
//   - post: The model.Post object to be saved.
//
// Returns:
//   - An error if there is a failure in creating the file or writing the post to it.
func (sm *StateManager) StoreData(channelname string, post model.Post) error {
	// Construct the file path
	channelDir := filepath.Join(sm.storageRoot, "crawls", sm.crawlid, channelname)
	if err := os.MkdirAll(channelDir, os.ModePerm); err != nil {
		log.Error().Err(err).Msg("Failed to create channel directory")
		return fmt.Errorf("failed to create directory for channel %s: %w", channelname, err)
	}

	// Define the JSONL file path
	jsonlFile := filepath.Join(channelDir, "data.jsonl")

	// Open the file in append mode, create it if it does not exist
	file, err := os.OpenFile(jsonlFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Error().Err(err).Msg("Failed to open JSONL file")
		return fmt.Errorf("failed to open file %s: %w", jsonlFile, err)
	}
	defer file.Close()

	// Marshal the post to JSON
	postData, err := json.Marshal(post)
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal post to JSON")
		return fmt.Errorf("failed to marshal post: %w", err)
	}

	// Write the JSON data to the file followed by a newline
	_, err = file.WriteString(string(postData) + "\n")
	if err != nil {
		log.Error().Err(err).Msg("Failed to write to JSONL file")
		return fmt.Errorf("failed to write post to file %s: %w", jsonlFile, err)
	}

	log.Info().Msgf("Post successfully stored for channel %s", channelname)
	return nil
}

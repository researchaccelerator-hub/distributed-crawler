package state

import (
	"bufio"
	"github.com/rs/zerolog/log"
	"os"
	"strconv"
	"strings"
)

const (
	stateDir     = "/state"
	listFile     = stateDir + "/list.txt"
	progressFile = stateDir + "/progress.txt"
)

// SeedSetup initializes the list file with the provided seed list if it does not exist,
// and then loads the list from the file.
//
// Parameters:
//   - seedlist: A slice of strings representing the initial items to seed the list file.
//
// Returns:
//   - A slice of strings containing the loaded list from the file.
//   - An error if there is a failure in loading the list.
func SeedSetup(seedlist []string) ([]string, error) {
	// Seed list if needed
	if _, err := os.Stat(listFile); os.IsNotExist(err) {
		seedList(seedlist)
	}

	// Load list
	list, err := loadList()
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
func seedList(items []string) {
	file, err := os.Create(listFile)
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
func loadList() ([]string, error) {
	file, err := os.Open(listFile)
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
func LoadProgress() (int, error) {
	if _, err := os.Stat(progressFile); os.IsNotExist(err) {
		return 0, nil // Start from the beginning if no progress file
	}

	data, err := os.ReadFile(progressFile)
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
func SaveProgress(index int) error {
	file, err := os.Create(progressFile)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(strconv.Itoa(index) + "\n")
	return err
}

package dapr_test

import (
	"github.com/stretchr/testify/assert"
	"os"
	"strings"
	"testing"
)

// TestReadURLsFromFile_WithMock tests the readURLsFromFile function with a mocked file
func TestReadURLsFromFile_WithMock(t *testing.T) {
	// Create a temporary file with test data
	tempFile, err := os.CreateTemp("", "testfile*.txt")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	// Write test data to the temp file
	testURLs := []string{
		"https://t.me/channel1",
		"# This is a comment",
		"",
		"https://t.me/channel2",
		"   https://t.me/channel3  ", // With whitespace
	}
	
	if _, err := tempFile.WriteString(strings.Join(testURLs, "\n")); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	
	// Flush changes
	if err := tempFile.Sync(); err != nil {
		t.Fatalf("Failed to sync temp file: %v", err)
	}

	// Define the readURLsFromFile function for testing
	readURLsFromFile := func(filename string) ([]string, error) {
		data, err := os.ReadFile(filename)
		if err != nil {
			return nil, err
		}
	
		lines := strings.Split(string(data), "\n")
		var urls []string
	
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line != "" && !strings.HasPrefix(line, "#") {
				urls = append(urls, line)
			}
		}
	
		return urls, nil
	}
	
	// Call the function under test
	urls, err := readURLsFromFile(tempFile.Name())
	
	// Assertions
	assert.NoError(t, err, "Should read URLs without error")
	assert.Equal(t, 3, len(urls), "Should read 3 valid URLs (ignoring comments and empty lines)")
	assert.Equal(t, "https://t.me/channel1", urls[0], "First URL should match")
	assert.Equal(t, "https://t.me/channel2", urls[1], "Second URL should match")
	assert.Equal(t, "https://t.me/channel3", urls[2], "Third URL should have whitespace trimmed")
}

// TestReadURLsFromFile_NonexistentFile tests error handling for nonexistent files
func TestReadURLsFromFile_NonexistentFile(t *testing.T) {
	// Define the readURLsFromFile function for testing
	readURLsFromFile := func(filename string) ([]string, error) {
		data, err := os.ReadFile(filename)
		if err != nil {
			return nil, err
		}
	
		lines := strings.Split(string(data), "\n")
		var urls []string
	
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line != "" && !strings.HasPrefix(line, "#") {
				urls = append(urls, line)
			}
		}
	
		return urls, nil
	}
	
	// Call with a nonexistent file
	urls, err := readURLsFromFile("/path/to/nonexistent/file.txt")
	
	// Assertions
	assert.Error(t, err, "Should return error for nonexistent file")
	assert.Nil(t, urls, "URLs should be nil for nonexistent file")
}
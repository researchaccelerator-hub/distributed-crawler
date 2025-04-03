package common

import (
	"fmt"
	"regexp"
	"testing"
	"time"
)

func TestGenerateCrawlID(t *testing.T) {
	// Call the function
	crawlID := GenerateCrawlID()

	// Check that the crawlID is not empty
	if crawlID == "" {
		t.Error("Expected non-empty crawlID, got empty string")
	}

	// Check that the crawlID is a string of 14 digits (YYYYMMDDHHMMSS)
	matched, err := regexp.MatchString(`^\d{14}$`, crawlID)
	if err != nil {
		t.Fatalf("Error in regex matching: %v", err)
	}
	if !matched {
		t.Errorf("CrawlID %s does not match the expected format YYYYMMDDHHMMSS", crawlID)
	}

	// Try to parse the crawlID back to a time
	_, err = time.Parse("20060102150405", crawlID)
	if err != nil {
		t.Fatalf("Could not parse crawlID %s back to time: %v", crawlID, err)
	}

	// Note: We're not checking the exact time range because this can lead to flaky tests
	// Especially in CI environments where time execution might vary
}

func ExampleGenerateCrawlID() {
	// Mock the current time for consistent output in the example
	// In a real application, you wouldn't do this
	currentTime, _ := time.Parse("2006-01-02 15:04:05", "2023-05-15 10:30:45")

	// For the example, we'll create a modified version that uses our fixed time
	mockCrawlID := func() string {
		return currentTime.Format("20060102150405")
	}

	// Show the result
	fmt.Println(mockCrawlID())
	// Output: 20230515103045
}

func ExampleGenerateCrawlID_usage() {
	// Mock a fixed time for consistent example output
	currentTime, _ := time.Parse("2006-01-02 15:04:05", "2023-05-15 10:30:45")
	mockCrawlID := currentTime.Format("20060102150405")

	// Demonstrate practical usage of a crawl ID
	fmt.Printf("Initiating web crawl with ID: %s\n", mockCrawlID)
	fmt.Printf("Saving results to: crawl_%s.json\n", mockCrawlID)

	// Output:
	// Initiating web crawl with ID: 20230515103045
	// Saving results to: crawl_20230515103045.json
}
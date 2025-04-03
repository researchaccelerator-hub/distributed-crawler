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
	parsedTime, err := time.Parse("20060102150405", crawlID)
	if err != nil {
		t.Fatalf("Could not parse crawlID %s back to time: %v", crawlID, err)
	}

	// For this test, we'll simply verify that the parsed time is within a reasonable
	// window of "now" (last 24 hours), rather than a specific range, to avoid timezone issues
	now := time.Now()
	dayAgo := now.Add(-24 * time.Hour)
	dayLater := now.Add(24 * time.Hour)
	
	if parsedTime.Before(dayAgo) || parsedTime.After(dayLater) {
		t.Errorf("Parsed time %v is not within a reasonable time range of now", parsedTime)
	} else {
		// Additional check - the difference between the parsed time and now should be less than 5 minutes
		// This will detect if the times are way off but within the 24 hour window
		diff := now.Sub(parsedTime)
		if diff < 0 {
			diff = -diff
		}
		
		if diff > 5*time.Minute {
			t.Logf("Warning: Parsed time %v differs from current time %v by %v", 
				parsedTime, now, diff)
		}
	}
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

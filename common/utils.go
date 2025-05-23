package common

import (
	"time"
)

// Configuration structure
type CrawlerConfig struct {
	DaprMode          bool
	DaprPort          int
	Concurrency       int
	Timeout           int
	UserAgent         string
	OutputFormat      string
	StorageRoot       string
	TDLibDatabaseURL  string         // Single database URL (for backward compatibility)
	TDLibDatabaseURLs []string       // Multiple database URLs for connection pooling
	MinPostDate       time.Time
	PostRecency       time.Time
	DaprJobMode       bool
	MinUsers          int
	CrawlID           string
	MaxComments       int
	MaxPosts          int
	MaxDepth          int
	MaxPages          int            // Maximum number of pages to crawl (default: 108000)
	TDLibVerbosity    int            // TDLib verbosity level for logging (default: 1)
	SkipMediaDownload bool           // Skip downloading media files (only process metadata)
}

// GenerateCrawlID generates a unique identifier based on the current timestamp.
// The identifier is formatted as a string in the "YYYYMMDDHHMMSS" format.
func GenerateCrawlID() string {
	// Get the current timestamp
	currentTime := time.Now()

	// Format the timestamp to a string (e.g., "20060102150405" for YYYYMMDDHHMMSS)
	crawlID := currentTime.Format("20060102150405")

	return crawlID
}

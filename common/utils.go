package common

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
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
	TDLibDatabaseURL  string   // Single database URL (for backward compatibility)
	TDLibDatabaseURLs []string // Multiple database URLs for connection pooling
	MinPostDate       time.Time
	PostRecency       time.Time
	DateBetweenMin    time.Time // Start date for date-between range
	DateBetweenMax    time.Time // End date for date-between range
	SampleSize        int       // Number of posts to randomly sample when using date-between
	DaprJobMode       bool
	MinUsers          int
	CrawlID           string
	CrawlLabel        string // User-defined label for the crawl (e.g., "youtube-snowball")
	MaxComments       int
	MaxPosts          int
	MaxDepth          int
	MaxPages          int    // Maximum number of pages to crawl (default: 108000)
	TDLibVerbosity    int    // TDLib verbosity level for logging (default: 1)
	SkipMediaDownload bool   // Skip downloading media files (only process metadata)
	Platform          string // Platform to crawl: "telegram", "youtube", etc.
	YouTubeAPIKey     string // API key for YouTube Data API
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

// DownloadURLFile downloads a file from a URL and saves it to a temporary location.
// Returns the path to the downloaded file and any error encountered.
func DownloadURLFile(url string) (string, error) {
	log.Info().Str("url", url).Msg("Downloading URL file")

	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Create request
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	// Set a user agent
	req.Header.Set("User-Agent", "Mozilla/5.0 Telegram-Scraper/1.0")

	// Make the request
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to download file: %w", err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("bad status code: %d", resp.StatusCode)
	}

	// Create a temporary directory to store the downloaded file
	tempDir := os.TempDir()
	filename := filepath.Join(tempDir, fmt.Sprintf("seed_urls_%s.txt", GenerateCrawlID()))

	// Create the file
	out, err := os.Create(filename)
	if err != nil {
		return "", fmt.Errorf("failed to create output file: %w", err)
	}
	defer out.Close()

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to write to file: %w", err)
	}

	log.Info().Str("file", filename).Msg("URL file downloaded successfully")
	return filename, nil
}

// ReadURLsFromFile reads URLs from a file, one per line.
// It ignores empty lines and lines starting with a '#' character (comments).
func ReadURLsFromFile(filename string) ([]string, error) {
	log.Debug().Str("filename", filename).Msg("Reading URLs from file")

	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	lines := strings.Split(string(data), "\n")
	var urls []string

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			urls = append(urls, line)
		}
	}

	log.Debug().Int("url_count", len(urls)).Msg("URLs read from file")
	return urls, nil
}

// PlatformType defines the supported platform types for crawling
type PlatformType string

const (
	// PlatformTelegram represents the Telegram platform
	PlatformTelegram PlatformType = "telegram"

	// PlatformYouTube represents the YouTube platform
	PlatformYouTube PlatformType = "youtube"
)

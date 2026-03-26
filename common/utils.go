package common

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/null_handler"
	"github.com/rs/zerolog/log"
)

// TelegramRateLimitConfig configures per-connection rate limiting for Telegram API calls.
// Rates are in calls per minute; jitter values add a random delay after each rate-limited
// call to reduce fingerprinting.
type TelegramRateLimitConfig struct {
	GetChatHistoryRate        float64 // max calls per minute (default: 30)
	SearchPublicChatRate      float64 // max calls per minute (default: 6)
	GetSupergroupInfoRate     float64 // max calls per minute (default: 20)
	GetChatHistoryJitterMs    int     // max jitter in milliseconds (default: 500)
	SearchPublicChatJitterMs  int     // max jitter in milliseconds (default: 1500)
	GetSupergroupInfoJitterMs int     // max jitter in milliseconds (default: 800)

	// GetMessage is handled reactively: a token is only consumed when the call
	// misses TDLib's local cache and hits the Telegram server. Cache hits are free.
	GetMessageServerHitRate      float64 // max server-hitting GetMessage calls per minute (default: 60)
	GetMessageServerHitJitterMs  int     // max jitter added after a server-hit throttle (default: 300)
}

// DefaultTelegramRateLimitConfig returns conservative defaults that approximate the
// historical sleep-based behaviour.
func DefaultTelegramRateLimitConfig() TelegramRateLimitConfig {
	return TelegramRateLimitConfig{
		GetChatHistoryRate:           30,
		SearchPublicChatRate:         6,
		GetSupergroupInfoRate:        20,
		GetChatHistoryJitterMs:       500,
		SearchPublicChatJitterMs:     1500,
		GetSupergroupInfoJitterMs:    800,
		GetMessageServerHitRate:      60,
		GetMessageServerHitJitterMs:  300,
	}
}

// Configuration structure
type CrawlerConfig struct {
	DaprMode           bool
	DaprPort           int
	Concurrency        int
	Timeout            int
	UserAgent          string
	OutputFormat       string
	StorageRoot        string
	TDLibDatabaseURL   string   // Single database URL (for backward compatibility)
	TDLibDatabaseURLs  []string // Multiple database URLs for connection pooling
	MinPostDate        time.Time
	PostRecency        time.Time
	DateBetweenMin     time.Time // Start date for date-between range
	DateBetweenMax     time.Time // End date for date-between range
	SampleSize         int       // Number of posts to randomly sample when using date-between
	DaprJobMode        bool
	MinUsers           int
	CrawlID            string
	CrawlLabel         string // User-defined label for the crawl (e.g., "youtube-snowball")
	MaxComments        int
	MaxPosts           int
	MaxDepth           int
	MaxPages           int                    // Maximum number of pages to crawl (default: 108000)
	TDLibVerbosity     int                    // TDLib verbosity level for logging (default: 1)
	SkipMediaDownload  bool                   // Skip downloading media files (only process metadata)
	Platform           string                 // Platform to crawl: "telegram", "youtube", etc.
	YouTubeAPIKey      string                 // API key for YouTube Data API
	SamplingMethod     string                 // Sampling method: "channel", "random", "snowball", "random-walk"
	SeedSize           int                    // Number of discovered channels to use as seed channels in random-walk crawl
	WalkbackRate       int                    // Rate to walkback using random-walk sampling method
	MinChannelVideos   int64                  // Minimum videos per channel for inclusion
	CombineFiles       bool                   // Flag to turn on combining files before upload
	CombineTempDir     string                 // Location to write temp files to before moving on completion. Reduces file events removing some pressure on the kernel buffer
	CombineWatchDir    string                 // Location to write crawl data to and watch for combining files once they reach the trigger size
	CombineWriteDir    string                 // Location to write combined files to before upload
	CombineTriggerSize int64                  // Total file size hreshold for creating a new combined file
	CombineHardCap     int64                  // File size cap that can not be exceeded in creation of combined file
	NullConfig         string                 // JSON string for user defined changes to handling of null crawl data
	NullValidator      null_handler.Validator // Null Validator object
	ExitOnComplete     bool                   // Exit with code 0 after a successful crawl (useful for Kubernetes cron jobs)
	MaxCrawlDuration   time.Duration          // Maximum wall-clock duration for a random-walk crawl (0 = unlimited)
	RateLimitConfig    TelegramRateLimitConfig // Per-connection Telegram API rate limits

	// Validator / tandem-crawl mode
	TandemCrawl              bool    // Crawler writes to pending_edges; no SearchPublicChat
	ValidateOnly             bool    // Pod runs RunValidationLoop; no crawl loop
	ValidatorRequestRate     float64 // HTTP calls per minute for validator (default: 120)
	ValidatorRequestJitterMs int     // Max jitter in ms for validator requests (default: 200)
	ValidatorClaimBatchSize  int     // Edges to claim per DB round-trip (default: 10)
	ValidatorTimeout         time.Duration // Abort crawl if blocked waiting for validator for this long (0 = disabled)

	// SOCKS5 proxy — all empty by default (direct connection)
	ProxyAddrs   []string // Ordered list of proxy addresses (ip:port), one per pod ordinal
	ProxyUser    string   // SOCKS5 auth username (from env PROXY_USER)
	ProxyPass    string   // SOCKS5 auth password (from env PROXY_PASS)
	ProxyOrdinal int      // Override ordinal for local dev (-1 = derive from POD_NAME)
	ProxyAddr    string   // Resolved proxy address for this pod (set at startup)

	// Managed ACI proxy lifecycle — mutually exclusive with ProxyAddrs
	ManagedProxies     bool    // Create/destroy ACI SOCKS5 proxy containers for this crawl
	ProxyResourceGroup string  // Azure resource group for ACI containers
	ProxyImage         string  // Container image for microsocks (e.g. "myregistry.azurecr.io/microsocks:latest")
	ProxySubnetID      string  // Azure subnet resource ID for private VNet injection (optional)
	ProxyLocation      string  // Azure region (e.g. "eastus2")
	ProxyCPU           float64 // CPU cores per proxy container (default: 0.5)
	ProxyMemoryGB      float64 // Memory in GB per proxy container (default: 0.5)
	ProxyPort           int     // SOCKS5 listen port inside the container (default: 1080)
	ProxyCount          int     // Number of proxy ACIs to create (default: 1)
	ProxySubscriptionID string  // Azure subscription ID for ACI management

	// Pod identity for multi-pod page claiming (from POD_NAME env var)
	PodName string
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

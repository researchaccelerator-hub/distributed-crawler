// Package dapr provides Dapr-related functionality
package dapr

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/researchaccelerator-hub/telegram-scraper/chunk"
	clientpkg "github.com/researchaccelerator-hub/telegram-scraper/client"
	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/crawl"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	crawlercommon "github.com/researchaccelerator-hub/telegram-scraper/crawler/common"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler/youtube"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/researchaccelerator-hub/telegram-scraper/telegramhelper"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello, World!")
}

// waitForDaprReady polls the Dapr sidecar health endpoint until it responds with 204 (ready)
// or the context is cancelled. The Dapr HTTP port is read from DAPR_HTTP_PORT (default: 3500).
func waitForDaprReady(ctx context.Context) error {
	daprHTTPPort := os.Getenv("DAPR_HTTP_PORT")
	if daprHTTPPort == "" {
		daprHTTPPort = "3500"
	}

	healthURL := fmt.Sprintf("http://localhost:%s/v1.0/healthz", daprHTTPPort)
	log.Info().Str("url", healthURL).Msg("Waiting for Dapr sidecar to become ready...")

	client := &http.Client{Timeout: 2 * time.Second}
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for Dapr sidecar: %w", ctx.Err())
		case <-ticker.C:
			resp, err := client.Get(healthURL)
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == http.StatusNoContent {
					log.Info().Msg("Dapr sidecar is ready")
					return nil
				}
				log.Debug().Int("status", resp.StatusCode).Msg("Dapr sidecar not yet ready, retrying...")
			} else {
				log.Debug().Err(err).Msg("Dapr sidecar not yet reachable, retrying...")
			}
		}
	}
}

// shutdownDaprSidecar sends a POST to the Dapr sidecar shutdown endpoint so the sidecar
// (and therefore the Kubernetes pod) terminates cleanly alongside the app container.
func shutdownDaprSidecar() {
	daprHTTPPort := os.Getenv("DAPR_HTTP_PORT")
	if daprHTTPPort == "" {
		daprHTTPPort = "3500"
	}
	shutdownURL := fmt.Sprintf("http://localhost:%s/v1.0/shutdown", daprHTTPPort)
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post(shutdownURL, "application/json", nil)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to send shutdown to Dapr sidecar")
		return
	}
	resp.Body.Close()
	log.Info().Int("status", resp.StatusCode).Msg("Dapr sidecar shutdown requested")
}

// StartDaprStandaloneMode initializes and starts the crawler in standalone mode with Dapr integration.
//
// This function handles the end-to-end process of starting a Telegram crawling operation:
// 1. Sets up a basic HTTP server (port 6481) for health checks and monitoring
// 2. Collects URLs to crawl from either a direct list or a file
// 3. Validates the URLs and ensures there's at least one URL to process
// 4. Optionally runs code generation if specified
// 5. Initializes a connection pool for Telegram clients based on concurrency settings
// 6. Launches the crawling process to extract data from the specified channels
// 7. Maintains the crawler in a blocking state after completion
//
// The function uses Dapr conventions for state management and data storage, allowing
// the crawler to integrate with distributed Dapr components even when running
// in standalone mode. This enables features like resumable crawls, distributed
// storage, and integration with other Dapr-aware services.
//
// Parameters:
//   - urlList: A list of URLs to crawl directly provided as strings
//   - urlFile: A file path containing URLs to crawl (one per line, comments with #)
//   - crawlerCfg: Configuration settings for the crawler including connection settings
//   - generateCode: A flag indicating whether to run code generation for Telegram API
//
// The function will log fatal errors if no URLs are provided or if essential
// initialization steps fail. It will block indefinitely after starting the crawler.
func StartDaprStandaloneMode(urlList []string, urlFile string, crawlerCfg common.CrawlerConfig, generateCode bool) {
	log.Info().Msg("Starting crawler in standalone mode")

	http.HandleFunc("/", handler)
	go func() {
		if err := http.ListenAndServe(":6481", nil); err != nil {
			// Log the error and exit the application if the server fails to start
			fmt.Printf("Failed to start server: %v\n", err)
			return
		}
	}()

	readyCtx, readyCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer readyCancel()
	if err := waitForDaprReady(readyCtx); err != nil {
		log.Fatal().Err(err).Msg("Dapr sidecar did not become ready")
	}

	// Create a file cleaner that targets the same location as where connections are unzipped
	// to ensure proper cleanup of temporary files

	// Collect URLs from command line arguments or file
	var urls []string

	if len(urlList) > 0 {
		urls = append(urls, urlList...)
	}

	if urlFile != "" {
		fileURLs, err := common.ReadURLsFromFile(urlFile)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to read URLs from file")
		}
		urls = append(urls, fileURLs...)
	}

	// For random sampling or validate-only, URLs are not required
	noURLsRequired := crawlerCfg.ValidateOnly ||
		(crawlerCfg.Platform == "youtube" && crawlerCfg.SamplingMethod == "random") ||
		crawlerCfg.SamplingMethod == "random-walk"
	if len(urls) == 0 && !noURLsRequired {
		log.Fatal().Msg("No URLs provided. Use --urls or --url-file to specify URLs to crawl")
	}

	log.Info().Msgf("Starting crawl of %d URLs with concurrency %d", len(urls), crawlerCfg.Concurrency)

	if generateCode {
		log.Info().Msg("Running code generation...")
		svc := &telegramhelper.RealTelegramService{}
		telegramhelper.GenCode(svc, crawlerCfg.StorageRoot)
		os.Exit(0)
	}

	// Platform-specific initialization
	if crawlerCfg.Platform == "youtube" {
		// For YouTube platform, we need to validate the API key
		if crawlerCfg.YouTubeAPIKey == "" {
			log.Error().Msg("YouTube API key is required for YouTube platform. Please provide it with --youtube-api-key flag")
			return
		}

		log.Info().Msg("Using YouTube platform with the provided API key")
	} else {
		// Pre-flight: verify proxy is reachable before spending 30s in TDLib init
		if crawlerCfg.ProxyAddr != "" {
			if err := common.CheckProxyTCP(crawlerCfg.ProxyAddr, 5*time.Second); err != nil {
				log.Fatal().Err(err).Msg("Proxy is not reachable — aborting before TDLib init")
			}
			if err := common.VerifyOutboundIP(crawlerCfg.ProxyAddr, crawlerCfg.ProxyUser, crawlerCfg.ProxyPass); err != nil {
				log.Fatal().Err(err).Msg("Proxy IP verification failed — aborting before TDLib init")
			}
		}

		baseDir := filepath.Join(crawlerCfg.StorageRoot, "state") // Same base path where connection folders are created
		cleaner := telegramhelper.NewFileCleaner(
			baseDir, // Base directory where conn_* folders are located (matches InitializeClientWithConfig)
			[]string{
				".tdlib/files/videos",    // Videos directory to clean up
				".tdlib/files/photos",    // Database directory to clean up
				".tdlib/files/documents", // General files directory
			},
			5,  // cleanup interval minutes
			15, // file age threshold minutes
		)

		if err := cleaner.Start(); err != nil {
			log.Fatal().Err(err).Msg("Failed to start file cleaner")
		}
		// Default Telegram platform initialization
		// Initialize connection pool with an appropriate size
		poolSize := crawlerCfg.Concurrency
		if poolSize < 1 {
			poolSize = 1
		}

		// If we have database URLs, use those to determine pool size
		if len(crawlerCfg.TDLibDatabaseURLs) > 0 {
			log.Info().Msgf("Found %d TDLib database URLs for connection pooling", len(crawlerCfg.TDLibDatabaseURLs))
			// Use the smaller of concurrency or number of database URLs
			if len(crawlerCfg.TDLibDatabaseURLs) < poolSize {
				poolSize = len(crawlerCfg.TDLibDatabaseURLs)
				log.Info().Msgf("Adjusting pool size to %d to match available database URLs", poolSize)
			}
		}

		// Initialize the connection pool
		crawl.InitConnectionPool(poolSize, crawlerCfg.StorageRoot, crawlerCfg)
		defer crawl.CloseConnectionPool()
	}

	// TODO: use completely different launch for random sampling
	launch(urls, crawlerCfg)

	log.Info().Msg("Crawling completed")
	if crawlerCfg.ExitOnComplete {
		log.Info().Msg("Crawl complete, exiting with code 0 (--exit-on-complete)")
		shutdownDaprSidecar()
		os.Exit(0)
	}
	select {}
}

// Note: readURLsFromFile function removed as we're now using the common implementation

// launch initializes and runs the scraping process for a given list of strings using the specified crawler configuration.
//
// It generates a unique crawl ID, sets up the state manager, and seeds the list. The function then loads the progress
// and processes each item in the list from the last saved progress point. Errors during processing are logged, and the
// progress is saved after each item is processed. The function ensures that all items are processed successfully, and
// handles any panics that occur during item processing.
//
// Parameters:
//   - stringList: A slice of strings representing the items to be processed.
//   - crawlCfg: A CrawlerConfig struct containing configuration settings for the crawler.
func launch(stringList []string, crawlCfg common.CrawlerConfig) {

	// Initialize state manager factory
	log.Info().Msgf("Starting scraper for crawl ID: %s", crawlCfg.CrawlID)
	smfact := state.DefaultStateManagerFactory{}

	// Create a temporary state manager to check for incomplete crawls
	tempSM, _, err := CreateStateManager(&smfact, crawlCfg, "")

	// Check for crawl id or create new one
	crawlexecid, isResumingSameCrawlExecution := DetermineCrawlID(tempSM, crawlCfg)

	sm, cfg, err := CreateStateManager(&smfact, crawlCfg, crawlexecid)
	if err != nil {
		return
	}

	var chunker *chunk.Chunker

	// Turn on chunking if necessary
	if crawlCfg.CombineFiles {
		chunker = chunk.NewChunker(
			context.Background(),
			sm,
			crawlCfg.CombineTempDir,
			crawlCfg.CombineWatchDir,
			crawlCfg.CombineWriteDir,
			crawlCfg.CombineTriggerSize*1024*1024,
			crawlCfg.CombineHardCap*1024*1024,
		)

		if err := chunker.Start(); err != nil {
			log.Fatal().Err(err).Msg("Failed to start file combiner")
		}
		defer chunker.Shutdown()
	}

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	// Validator-only mode: run the validation loop and exit.
	if crawlCfg.ValidateOnly {
		log.Info().Msg("validator-mode: starting validation-only loop")
		// Load shared caches used by the validator (same order as random-walk init).
		if seedErr := sm.LoadSeedChannels(); seedErr != nil {
			log.Warn().Err(seedErr).Msg("validator-mode: failed to load seed channels (continuing)")
		}
		if invalidErr := sm.LoadInvalidChannels(); invalidErr != nil {
			log.Warn().Err(invalidErr).Msg("validator-mode: failed to load invalid channels (continuing)")
		}
		if discErr := sm.InitializeDiscoveredChannels(); discErr != nil {
			log.Fatal().Err(discErr).Msg("validator-mode: failed to initialize discovered channels")
		}
		// Recover edges/batches stuck in intermediate states from prior crashes.
		const staleThreshold = 10 * time.Minute
		if dsm, ok := sm.(*state.DaprStateManager); ok {
			if n, recErr := dsm.RecoverStaleEdgeClaims(staleThreshold); recErr != nil {
				log.Warn().Err(recErr).Msg("validator-mode: failed to recover stale edge claims")
			} else if n > 0 {
				log.Info().Int("recovered", n).Msg("validator-mode: recovered stale edge claims")
			}
			if n, recErr := sm.RecoverStaleBatchClaims(staleThreshold); recErr != nil {
				log.Warn().Err(recErr).Msg("validator-mode: failed to recover stale batch claims")
			} else if n > 0 {
				log.Info().Int("recovered", n).Msg("validator-mode: recovered stale batch claims")
			}
			if n, recErr := sm.RecoverStaleValidatingEdges(staleThreshold); recErr != nil {
				log.Warn().Err(recErr).Msg("validator-mode: failed to recover stale validating edges")
			} else if n > 0 {
				log.Info().Int("recovered", n).Msg("validator-mode: recovered stale validating edges")
			}
			if n, recErr := dsm.RecoverOrphanEdges(); recErr != nil {
				log.Warn().Err(recErr).Msg("validator-mode: failed to recover orphan edges")
			} else if n > 0 {
				log.Info().Int("deleted", n).Msg("validator-mode: recovered orphan edges")
			}
		}
		if err := crawl.RunValidationLoop(context.Background(), sm, crawlCfg); err != nil {
			log.Error().Err(err).Msg("validator-mode: validation loop exited with error")
		}
		if closeErr := sm.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msg("validator-mode: failed to close state manager")
		}
		return
	}

	if crawlCfg.SamplingMethod == "random" && crawlCfg.Platform == "youtube" {
		RunRandomYoutubeSample(context.Background(), sm, crawlCfg)
	} else if crawlCfg.SamplingMethod == "random-walk" && crawlCfg.Platform == "telegram" {
		// Layerless random-walk path: pages live exclusively in the page_buffer table.
		// No depth layers are created; RunForChannel writes the next hop directly back
		// into page_buffer after processing each channel.

		// Normalize seed URLs: strip any t.me prefix and lowercase.
		for i, u := range stringList {
			u = strings.TrimPrefix(u, "https://t.me/")
			u = strings.TrimPrefix(u, "http://t.me/")
			u = strings.TrimPrefix(u, "t.me/")
			u = strings.TrimPrefix(u, "@")
			stringList[i] = strings.ToLower(u)
		}

		// Initialize with an empty seed list so the state manager sets up its DB
		// binding without creating a layer-map entry for the seed URLs.
		if err = sm.Initialize([]string{}); err != nil {
			log.Error().Err(err).Msg("random-walk-init: failed to initialize state manager")
			return
		}
		if seedErr := sm.LoadSeedChannels(); seedErr != nil {
			log.Warn().Err(seedErr).Msg("random-walk-init: failed to load seed channels (continuing)")
		}
		if invalidErr := sm.LoadInvalidChannels(); invalidErr != nil {
			log.Warn().Err(invalidErr).Msg("random-walk-init: failed to load invalid channels (continuing)")
		}
		if err = sm.InitializeDiscoveredChannels(); err != nil {
			log.Fatal().Err(err).Msg("random-walk-init: failed to pull discovered channels")
		}

		// Seed the page_buffer only on a fresh start (empty buffer = not resuming).
		existingPages, _ := sm.GetPagesFromPageBuffer(1)
		if len(existingPages) == 0 {
			if len(stringList) > 0 {
				log.Info().Int("count", len(stringList)).Msg("random-walk-init: seeding page buffer from URL list")
				for _, url := range stringList {
					page := &state.Page{
						ID:         uuid.New().String(),
						URL:        url,
						Depth:      0,
						Status:     "unfetched",
						Timestamp:  time.Now(),
						SequenceID: uuid.New().String(),
					}
					if bufErr := sm.AddPageToPageBuffer(page); bufErr != nil {
						log.Error().Err(bufErr).Str("url", url).Msg("random-walk-init: failed to seed URL into page buffer")
					}
				}
			} else {
				if initErr := sm.InitializeRandomWalkLayer(); initErr != nil {
					log.Fatal().Err(initErr).Msg("random-walk-init: failed to initialize page buffer from discovered channels")
				}
			}
		} else {
			log.Info().Int("count", len(existingPages)).Msg("random-walk-init: resuming from existing page buffer")
		}

		if err := RunRandomWalkLayerless(sm, crawlCfg); err != nil {
			log.Error().Err(err).Msg("random-walk-layerless: crawl aborted")
		}
	} else {
		// All other modes (snowball, channel, youtube-channel, etc.)
		err = sm.Initialize(stringList)
		if err != nil {
			log.Error().Err(err).Msg("Failed to set up seed URLs")
			return
		}

		ProcessLayersIteratively(sm, crawlCfg, isResumingSameCrawlExecution)
	}

	// Explicitly save any pending media cache data before completing the crawl
	log.Info().Msg("Saving final state before marking crawl as completed")
	if closeErr := sm.Close(); closeErr != nil {
		log.Warn().Err(closeErr).Msg("Error during final state save, but will continue with crawl completion")
	}

	completionMetadata := map[string]interface{}{
		"status":          "completed",
		"endTime":         time.Now(),
		"previousCrawlID": cfg.CrawlExecutionID,
	}

	if err := sm.UpdateCrawlMetadata(cfg.CrawlID, completionMetadata); err != nil {
		log.Panic().Err(err).Msg("Failed to update crawl completion metadata")
	}

	err = sm.ExportPagesToBinding(cfg.CrawlID)
	if err != nil {
		log.Error().Err(err).Msg("Error exporting pages to binding")
		return
	}
	log.Info().Msg("All items processed successfully.")

}

// processLayerInParallel processes all pages in a layer with a maximum of maxWorkers concurrent goroutines.
// It uses a semaphore pattern to limit concurrency and ensures all pages are processed before returning.
// This version uses the connection pool for efficient client management.
func processLayerInParallel(layer *state.Layer, maxWorkers int, sm state.StateManagementInterface, crawlCfg common.CrawlerConfig, shouldStop *atomic.Bool, crawlStart time.Time) {
	// In dapr mode it's harder to accurately detect this, so we'll simplify the approach
	// to prevent reprocessing of fetched pages, always skip them
	isResumingSameCrawlExecution := true
	log.Info().Bool("is_resuming_same_execution", isResumingSameCrawlExecution).Msg("Dapr always skips fetched pages")
	// Map to collect all discovered channels
	allDiscoveredChannels := make([]*state.Page, 0)

	// Use a mutex to protect the shared allDiscoveredChannels slice
	var mu sync.Mutex

	// Use a wait group to track when all pages are processed
	var wg sync.WaitGroup

	// Semaphore to limit concurrent processing
	semaphore := make(chan struct{}, maxWorkers)

	// Create a context that can be cancelled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a map to track unique pages by URL to avoid processing duplicates
	uniquePages := make(map[string]bool)

	// Log the original count of pages
	originalCount := len(layer.Pages)

	// create pool of youtube clients
	var ytPool chan *ytWorker
	if crawlCfg.Platform == "youtube" {
		ytPool = make(chan *ytWorker, maxWorkers)
		for i := 0; i < maxWorkers; i++ {
			w, _ := createFreshWorker(sm, crawlCfg)
			ytPool <- w
		}
	}

	// Process each page in the current layer, ensuring each URL is processed only once
	for pageIndex := 0; pageIndex < len(layer.Pages); pageIndex++ {
		pageToProcess := layer.Pages[pageIndex]

		// Skip if this URL has already been seen in this layer
		if uniquePages[pageToProcess.URL] {
			log.Debug().Str("url", pageToProcess.URL).Msg("Skipping duplicate page URL in current layer")
			continue
		}
		uniquePages[pageToProcess.URL] = true

		// Add debug log for unique pages
		log.Debug().Str("url", pageToProcess.URL).Msg("Processing unique URL from current layer")

		// Print debug information about each page discovered during crawl restart
		log.Debug().
			Str("url", pageToProcess.URL).
			Str("status", pageToProcess.Status).
			Str("id", pageToProcess.ID).
			Int("message_count", len(pageToProcess.Messages)).
			Bool("resuming_execution", isResumingSameCrawlExecution).
			Msg("Page discovered during crawl restart in Dapr mode")

		// Skip already processed or errored pages
		if pageToProcess.Status == "fetched" || pageToProcess.Status == "error" {
			if isResumingSameCrawlExecution {
				// When resuming with the same crawlexecutionid, skip already fetched/errored pages
				// regardless of message status - this prevents reprocessing
				if pageToProcess.Status == "error" {
					log.Debug().Msgf("Skipping previously errored page during same execution resume: %s", pageToProcess.URL)
				} else {
					log.Debug().Msgf("Skipping already fetched page during same execution resume: %s", pageToProcess.URL)
				}
				continue
			} else {
				// When starting a new crawlexecutionid, we'll process fetched pages
				// and retry errored pages
				if pageToProcess.Status == "error" {
					log.Debug().Msgf("Retrying previously errored page in new execution: %s", pageToProcess.URL)
				} else {
					log.Debug().Msgf("Processing fetched page in new execution, will use resample flag: %s", pageToProcess.URL)
				}
				// Continue processing this page
			}
		}

		// Stop dispatching new channels if the crawl duration has been exceeded
		if shouldStop != nil && shouldStop.Load() {
			log.Info().Str("url", pageToProcess.URL).Msg("Max crawl duration reached, skipping remaining channels in layer")
			break
		}

		// Acquire semaphore slot (block if we're at max workers)
		semaphore <- struct{}{}
		wg.Add(1)

		go func(page state.Page) {
			defer func() {
				// Release semaphore slot
				<-semaphore
				wg.Done()

				// Recover from panics to ensure we don't hang the wait group
				if r := recover(); r != nil {
					log.Error().Msgf("Recovered from panic while processing item: %s, error: %v", page.URL, r)

					// Update the page status to error
					page.Status = "error"
					page.Error = fmt.Sprintf("Panic: %v", r)

					// Update the page in the state manager
					if err := sm.UpdatePage(page); err != nil {
						log.Error().Err(err).Msg("Failed to update page status after panic")
					}

					// Save state after recovery
					if err := sm.SaveState(); err != nil {
						log.Error().Err(err).Msg("Failed to save state after panic recovery")
					}
				}
			}()

			// Set the timestamp
			page.Timestamp = time.Now()

			// Platform-specific processing
			var discoveredChannels []*state.Page
			var err error

			if crawlCfg.Platform == "youtube" {
				// YouTube platform processing
				log.Info().Msgf("Processing YouTube channel: %s", page.URL)
				ytCrawler := <-ytPool
				discoveredChannels, err = FetchYoutubeChannelInfoAndVideos(ytCrawler.crawler, crawlCfg, page, ctx)
				ytCrawler.usage++

				// Rotate if
				// TODO: move pool logic to another function/package
				if ytCrawler.usage >= ytCrawler.retireAt {
					log.Info().Int("usage", ytCrawler.usage).Str("log_tag", "FOCUS").Int("channels_crawled", ytCrawler.usage).Msg("Youtube Crawler retirement triggered")

					// Cleanup old
					if ytCrawler.client != nil {
						ytCrawler.client.Disconnect(ytCrawler.ctx)
					}
					if ytCrawler.cancel != nil {
						ytCrawler.cancel() // Vital for OOM prevention
					}

					ytCrawler.client = nil
					ytCrawler.crawler = nil
					ytCrawler.ctx = nil

					// Replace
					newCrawler, err := createFreshWorker(sm, crawlCfg)
					if err == nil {
						ytCrawler = newCrawler
					} else {
						log.Error().Err(err).Str("log_tag", "FOCUS").Msg("Failed to rotate youtube crawler")
						ytCrawler.usage = 0 // Fallback: reset counter if init fails
					}
				}

				ytPool <- ytCrawler
			} else {
				// Telegram platform processing (default)
				// Use the pooled channel processing
				log.Info().Msgf("Starting run for Telegram channel: %s", page.URL)
				discoveredChannels, err = crawl.RunForChannelWithPool(ctx, &page, crawlCfg.StorageRoot, sm, crawlCfg)
			}

			log.Info().Str("page_url", page.URL).Str("connection_id", page.ConnectionID).Msg("Page processed")

			if err != nil {
				log.Error().Stack().Err(err).Msgf("Error processing item %s", page.URL)
				page.Status = "error"
				page.Error = err.Error()

				// Update the page in the state manager
				if updateErr := sm.UpdatePage(page); updateErr != nil {
					log.Error().Err(updateErr).Msg("Failed to update page status after error")
				}

				// Save the state to ensure error status is persisted
				if err := sm.SaveState(); err != nil {
					log.Error().Err(err).Msgf("Error saving state after marking channel %s as error", page.URL)
				}
			} else {
				page.Status = "fetched"
				if updateErr := sm.UpdatePage(page); updateErr != nil {
					log.Error().Err(updateErr).Msg("Failed to update page status after successful processing")
				}

				// Save the entire state
				if err := sm.SaveState(); err != nil {
					log.Error().Err(err).Msgf("Error saving state after processing channel %s", page.URL)
				}

				// Collect discovered channels with mutex protection
				if len(discoveredChannels) > 0 {
					mu.Lock()
					allDiscoveredChannels = append(allDiscoveredChannels, discoveredChannels...)
					mu.Unlock()
				}
			}

		}(pageToProcess)
	}

	// Wait for all pages to be processed
	wg.Wait()

	if crawlCfg.Platform == "youtube" {
		close(ytPool)
		for ytCrawler := range ytPool {
			if disconnectErr := ytCrawler.client.Disconnect(ytCrawler.ctx); disconnectErr != nil {
				log.Warn().Err(disconnectErr).Msg("Error disconnecting YouTube client")
			}
		}
	}

	// Log summary of unique pages processed
	uniqueCount := len(uniquePages)
	duplicateCount := originalCount - uniqueCount
	log.Info().
		Int("original_page_count", originalCount).
		Int("unique_page_count", uniqueCount).
		Int("duplicate_page_count", duplicateCount).
		Msgf("Processed %d unique pages (skipped %d duplicates) in layer at depth %d",
			uniqueCount, duplicateCount, layer.Depth)

	// After all pages in the layer are processed, build the next layer from
	// discovered channels (non-random-walk modes only).
	if len(allDiscoveredChannels) > 0 {
		currentDepth := layer.Depth
		newPages := make([]state.Page, 0, len(allDiscoveredChannels))
		newLayerUniqueURLs := make(map[string]bool)
		uniqueDiscovered := 0

		for _, channel := range allDiscoveredChannels {
			if newLayerUniqueURLs[channel.URL] {
				log.Debug().Str("url", channel.URL).Msg("Skipping duplicate discovered URL for next layer")
				continue
			}
			newLayerUniqueURLs[channel.URL] = true
			uniqueDiscovered++

			parentID := ""
			if channel.ParentID != "" {
				parentID = channel.ParentID
			}
			newPages = append(newPages, state.Page{
				URL:       channel.URL,
				Depth:     currentDepth + 1,
				Status:    "unfetched",
				Timestamp: time.Now(),
				ParentID:  parentID,
			})
		}

		log.Info().
			Int("total_discovered", len(allDiscoveredChannels)).
			Int("unique_discovered", uniqueDiscovered).
			Int("duplicate_discovered", len(allDiscoveredChannels)-uniqueDiscovered).
			Msgf("Deduplicated discovered channels for next layer at depth %d", currentDepth+1)

		if err := sm.AddLayer(newPages); err != nil {
			log.Error().Err(err).Msg("Failed to add discovered channels as new layer")
		} else {
			log.Info().Int("count", len(newPages)).Msg("Added new channels to be processed")
			if err := sm.SaveState(); err != nil {
				log.Error().Err(err).Msg("Failed to save state after adding new layer")
			}
		}
	}
}

func CreateStateManager(smfact state.StateManagerFactory, crawlCfg common.CrawlerConfig, crawlexecid string) (state.StateManagementInterface, state.Config, error) {
	var cfg state.Config
	if crawlexecid == "" {
		// Create a temporary state manager to check for incomplete crawls
		cfg = state.Config{
			StorageRoot:     crawlCfg.StorageRoot,
			CrawlID:         crawlCfg.CrawlID,
			CrawlLabel:      crawlCfg.CrawlLabel,
			Platform:        crawlCfg.Platform, // Pass the platform information
			SamplingMethod:  crawlCfg.SamplingMethod,
			SeedSize:        crawlCfg.SeedSize,
			CombineFiles:    crawlCfg.CombineFiles,
			CombineTempDir:  crawlCfg.CombineTempDir,
			CombineWatchDir: crawlCfg.CombineWatchDir,
		}
	} else {
		cfg = state.Config{
			StorageRoot:      crawlCfg.StorageRoot,
			CrawlID:          crawlCfg.CrawlID,
			CrawlLabel:       crawlCfg.CrawlLabel,
			CrawlExecutionID: crawlexecid,
			Platform:         crawlCfg.Platform, // Pass the platform information
			SamplingMethod:   crawlCfg.SamplingMethod,
			SeedSize:         crawlCfg.SeedSize,
			CombineFiles:     crawlCfg.CombineFiles,
			CombineTempDir:   crawlCfg.CombineTempDir,
			CombineWatchDir:  crawlCfg.CombineWatchDir,
			// Add the MaxPages config
			MaxPagesConfig: &state.MaxPagesConfig{
				MaxPages: crawlCfg.MaxPages,
			},
		}
	}
	sm, err := smfact.Create(cfg)
	if err != nil {
		if crawlexecid == "" {
			log.Error().Err(err).Msg("Failed to create temporary state manager")
			return nil, state.Config{}, err
		} else {
			log.Error().Err(err).Msg("Failed to initialize state manager")
			return nil, state.Config{}, err
		}
	}
	return sm, cfg, nil
}

func DetermineCrawlID(tempSM state.StateManagementInterface, crawlCfg common.CrawlerConfig) (string, bool) {
	var crawlexecid string
	if tempSM != nil {
		existingExecID, exists, err := tempSM.FindIncompleteCrawl(crawlCfg.CrawlID)
		if err != nil {
			log.Warn().Err(err).Msg("Error checking for existing crawls, starting fresh")
		} else if exists {
			// Use existing execution ID
			crawlexecid = existingExecID
			log.Info().Msgf("Resuming existing crawl: %s (execution: %s)",
				crawlCfg.CrawlID, crawlexecid)
		}

		// Close the temporary state manager to free up resources
		_ = tempSM.Close()
	}

	// If no existing crawl was found, generate a new execution ID
	if crawlexecid == "" {
		crawlexecid = common.GenerateCrawlID()
		log.Info().Msgf("Starting new crawl execution: %s", crawlexecid)
	}

	// Track whether we're resuming the same execution ID or starting a new one
	var isResumingSameCrawlExecution bool
	if tempSM != nil {
		existingExecID, exists, _ := tempSM.FindIncompleteCrawl(crawlCfg.CrawlID)
		isResumingSameCrawlExecution = exists && existingExecID != "" && crawlexecid == existingExecID
	}
	log.Info().Bool("is_resuming_same_execution", isResumingSameCrawlExecution).Msg("Crawl execution mode")
	return crawlexecid, isResumingSameCrawlExecution
}

// layerlessPollInterval is the sleep between page-buffer polls in RunRandomWalkLayerless.
// Exposed as a var so tests can set it to a short value without sleeping.
var layerlessPollInterval = 5 * time.Second

// RunRandomWalkLayerless runs a random-walk Telegram crawl without layer bookkeeping.
//
// Instead of accumulating discovered channels into depth layers, this function
// continuously pops pages from the page_buffer table, dispatches them to
// RunForChannelWithPool (which writes the next hop back into page_buffer), and
// deletes each page immediately after it is processed.
//
// Concurrency model: a semaphore of size maxWorkers limits concurrent goroutines.
// The producer loop acquires a slot before spawning each worker and releases it
// on completion, so a free worker picks up the next available page without
// waiting for the slowest worker in a batch (no convoy effect).
//
// Benefits over ProcessLayersIteratively for random-walk:
//   - No depth counter or layer-map state to maintain
//   - Crash-restart is safe: unprocessed pages remain in the buffer
//   - No convoy effect: workers operate independently, not in lock-step batches
//   - Tandem mode works naturally: the validator writes to page_buffer and this
//     loop picks up the results without any special batching logic
func RunRandomWalkLayerless(sm state.StateManagementInterface, crawlCfg common.CrawlerConfig) error {
	ctx := context.Background()
	crawlStart := time.Now()
	var shouldStop atomic.Bool

	maxWorkers := crawlCfg.Concurrency
	if maxWorkers < 1 {
		maxWorkers = 1
	}

	// sem limits the number of concurrently running workers.
	sem := make(chan struct{}, maxWorkers)

	var (
		wg                 sync.WaitGroup
		inFlight           sync.Map
		inFlightCount      atomic.Int32
		validatorWaitSince time.Time // non-zero when crawler is fully blocked waiting for validator
	)

	for !shouldStop.Load() {
		if crawlCfg.MaxCrawlDuration > 0 && time.Since(crawlStart) >= crawlCfg.MaxCrawlDuration {
			log.Info().
				Dur("elapsed", time.Since(crawlStart)).
				Dur("max_crawl_duration", crawlCfg.MaxCrawlDuration).
				Msg("random-walk-layerless: max crawl duration reached, stopping")
			break
		}

		// Don't poll the DB when all worker slots are occupied — any pages
		// returned would be deduplicated by inFlight and wasted round-trips.
		// Wait until at least one worker finishes and frees capacity.
		if int(inFlightCount.Load()) >= maxWorkers {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		pages, err := sm.GetPagesFromPageBuffer(maxWorkers)
		if err != nil {
			log.Error().Err(err).Msg("random-walk-layerless: failed to get pages from page buffer")
			time.Sleep(layerlessPollInterval)
			continue
		}

		if len(pages) == 0 {
			if crawlCfg.TandemCrawl {
				// Only declare completion when no workers are in-flight either —
				// a running worker may be about to write the next page.
				if inFlightCount.Load() == 0 {
					pending, countErr := sm.CountIncompleteBatches(crawlCfg.CrawlID)
					if countErr != nil {
						log.Warn().Err(countErr).Msg("random-walk-layerless: tandem: could not check incomplete batches")
						validatorWaitSince = time.Time{} // reset on error — don't time out on DB errors
					} else if pending == 0 {
						log.Info().Str("crawl_id", crawlCfg.CrawlID).
							Msg("random-walk-layerless: tandem: buffer empty and no pending batches, crawl complete")
						break
					} else {
						// Crawler is fully blocked: no pages, no in-flight workers, validator has outstanding batches.
						if validatorWaitSince.IsZero() {
							validatorWaitSince = time.Now()
						}
						if crawlCfg.ValidatorTimeout > 0 {
							if waited := time.Since(validatorWaitSince); waited >= crawlCfg.ValidatorTimeout {
								return fmt.Errorf("random-walk-layerless: validator circuit breaker: no progress from validator after %s (%d incomplete batches) — validator pod may have crashed",
									waited.Round(time.Second), pending)
							}
						}
						log.Info().Int("incomplete_batches", pending).
							Dur("waiting_for", time.Since(validatorWaitSince).Round(time.Second)).
							Msg("random-walk-layerless: tandem: buffer empty, waiting for validator")
					}
				} else {
					// Workers still in-flight — not fully blocked yet, reset the timer.
					validatorWaitSince = time.Time{}
				}
			} else {
				log.Debug().Msg("random-walk-layerless: buffer empty, waiting")
			}
			time.Sleep(layerlessPollInterval)
			continue
		}

		validatorWaitSince = time.Time{} // pages found — no longer waiting

		dispatched := 0
		for _, page := range pages {
			// Skip pages already being processed by another goroutine.
			if _, loaded := inFlight.LoadOrStore(page.ID, true); loaded {
				continue
			}

			// Acquire a worker slot.  Blocks until a running worker finishes
			// and releases its slot, so the producer naturally back-pressures
			// when all maxWorkers slots are taken.
			sem <- struct{}{}

			inFlightCount.Add(1)
			wg.Add(1)
			dispatched++

			go func(p state.Page) {
				defer wg.Done()
				defer func() {
					inFlight.Delete(p.ID)
					inFlightCount.Add(-1)
					<-sem
				}()

				_, procErr := crawl.RunForChannelWithPool(ctx, &p, crawlCfg.StorageRoot, sm, crawlCfg)
				if errors.Is(procErr, crawl.ErrWalkbackExhausted) {
					// Leave page in buffer — will be re-processed on restart.
					log.Error().Err(procErr).Str("url", p.URL).Msg("random-walk-layerless: walkback exhausted, page left in buffer for restart")
				} else if errors.Is(procErr, crawl.ErrFloodWaitRetire) {
					// Connection was retired; leave page in buffer for retry by a remaining connection.
					log.Warn().Str("url", p.URL).Msg("random-walk-layerless: connection retired due to FLOOD_WAIT, page left in buffer")
					if crawl.ConnectionPoolTotalSize() == 0 {
						log.Error().Msg("random-walk-layerless: all connections retired due to FLOOD_WAIT, aborting crawl")
						shouldStop.Store(true)
					}
				} else if errors.Is(procErr, crawl.ErrTDLib400) {
					// Channel is permanently invalid — find a replacement edge, then
					// remove the failed page from the buffer.
					log.Error().Err(procErr).Str("url", p.URL).Msg("random-walk-layerless: TDLib 400, finding replacement edge")
					if replErr := crawl.Handle400Replacement(sm, &p, crawlCfg); replErr != nil {
						log.Error().Err(replErr).Str("url", p.URL).Msg("random-walk-layerless: failed to find 400 replacement")
					}
					if delErr := sm.DeletePageBufferPages([]string{p.ID}, []string{p.URL}); delErr != nil {
						log.Error().Err(delErr).Str("url", p.URL).Msg("random-walk-layerless: failed to delete 400 page from buffer")
					}
				} else {
					if procErr != nil {
						log.Error().Err(procErr).Str("url", p.URL).Msg("random-walk-layerless: error processing channel")
					}
					if delErr := sm.DeletePageBufferPages([]string{p.ID}, []string{p.URL}); delErr != nil {
						log.Error().Err(delErr).Str("url", p.URL).Msg("random-walk-layerless: failed to delete page from buffer")
					}
				}

				if crawlCfg.MaxCrawlDuration > 0 && time.Since(crawlStart) >= crawlCfg.MaxCrawlDuration {
					shouldStop.Store(true)
				}
			}(page)
		}

		if dispatched == 0 {
			// All fetched pages are already in-flight; wait briefly before re-polling.
			time.Sleep(500 * time.Millisecond)
		}
	}

	// Drain any workers still running before returning.
	wg.Wait()
	return nil
}

func ProcessLayersIteratively(sm state.StateManagementInterface, crawlCfg common.CrawlerConfig, isResumingSameCrawlExecution bool) {
	depth := 0
	crawlStart := time.Now()
	var shouldStop atomic.Bool
	for {
		log.Info().Msgf("Starting loop for depth: %v", depth)
		// Check current maximum depth at the beginning of each iteration
		maxDepth, err := sm.GetMaxDepth()
		if err != nil {
			log.Error().Err(err).Msg("Failed to get maximum depth, using 0")
			maxDepth = 0
		}

		// If we've processed all layers up to the current maximum depth, we're done
		if depth > maxDepth {
			log.Info().Msgf("Processed all layers up to maximum depth %d", maxDepth)
			break
		}

		// Get the current layer to process
		pages, err := sm.GetLayerByDepth(depth)
		if err != nil {
			log.Error().Err(err).Msgf("Failed to get layer at depth %d", depth)
			depth++
			continue
		}

		// Print all page statuses before processing
		log.Info().Int("page_count", len(pages)).Int("depth", depth).Msg("Page status summary before processing")
		pageStatusCount := make(map[string]int)
		for _, page := range pages {
			pageStatusCount[page.Status]++
			log.Debug().
				Str("url", page.URL).
				Str("status", page.Status).
				Str("id", page.ID).
				Int("message_count", len(page.Messages)).
				Time("timestamp", page.Timestamp).
				Bool("resuming_execution", isResumingSameCrawlExecution).
				Msg("Page status before processing in Dapr mode")
		}
		// Log the counts of pages by status
		for status, count := range pageStatusCount {
			log.Info().Str("status", status).Int("count", count).Int("depth", depth).Msg("Page status count")
		}

		// Skip if there are no pages at this depth
		if len(pages) == 0 {
			log.Info().Msgf("No pages found at depth %d, skipping", depth)
			depth++
			continue
		}

		if crawlCfg.MaxDepth >= 0 && depth > crawlCfg.MaxDepth {
			log.Info().Msgf("Processed all layers up to max depth %d", maxDepth)
			break
		}
		log.Info().Msgf("Processing layer at depth %d with %d pages", depth, len(pages))

		// Create a Layer object
		layer := &state.Layer{
			Depth: depth,
			Pages: pages,
		}

		// Process pages in current layer in parallel
		processLayerInParallel(layer, crawlCfg.Concurrency, sm, crawlCfg, &shouldStop, crawlStart)

		// Log progress after completing a layer
		log.Info().Msgf("Completed layer at depth %d", depth)

		// Move to the next depth
		depth++
	}
}

func InitializeYoutubeCrawlerComponents(ctx context.Context, sm state.StateManagementInterface, crawlCfg common.CrawlerConfig) (crawler.Crawler, clientpkg.Client, context.Context, context.CancelFunc, error) {
	// Initialize YouTube components
	var err error
	clientCtx, clientCtxCancel := context.WithCancel(ctx)
	clientFactory := clientpkg.NewDefaultClientFactory()

	// Debug API key passing
	if crawlCfg.YouTubeAPIKey == "" {
		log.Error().Msg("YouTube API key is empty - make sure you provided it with --youtube-api-key")
	} else {
		log.Debug().Str("api_key_length", fmt.Sprintf("%d chars", len(crawlCfg.YouTubeAPIKey))).Msg("Using YouTube API key in DAPR mode")
	}

	config := map[string]interface{}{
		"api_key": crawlCfg.YouTubeAPIKey,
	}

	// Create YouTube client
	ytClient, ytErr := clientFactory.CreateClient(clientCtx, "youtube", config)
	if ytErr != nil {
		err = fmt.Errorf("failed to create YouTube client: %w", ytErr)
		log.Error().Err(err).Msg("YouTube client creation failed")
		return nil, nil, clientCtx, clientCtxCancel, err
	}
	// Connect to YouTube API
	if ytErr = ytClient.Connect(clientCtx); ytErr != nil {
		err = fmt.Errorf("failed to connect to YouTube API: %w", ytErr)
		log.Error().Err(err).Msg("YouTube API connection failed")
		return nil, ytClient, clientCtx, clientCtxCancel, err
	}
	// Create crawler factory and register crawlers
	factory := crawler.NewCrawlerFactory()
	if ytErr = crawlercommon.RegisterAllCrawlers(factory); ytErr != nil {
		err = fmt.Errorf("failed to register crawlers: %w", ytErr)
		log.Error().Err(err).Msg("Failed to register YouTube crawler")
		return nil, ytClient, clientCtx, clientCtxCancel, err
	}
	// Create YouTube crawler
	ytCrawler, ytErr := factory.GetCrawler(crawler.PlatformYouTube)
	if ytErr != nil {
		err = fmt.Errorf("failed to create YouTube crawler: %w", ytErr)
		log.Error().Err(err).Msg("Failed to create YouTube crawler")
		return nil, ytClient, clientCtx, clientCtxCancel, err
	}
	// Create YouTubeClient adapter
	ytAdapter, adapterErr := youtube.NewClientAdapter(ytClient)
	if adapterErr != nil {
		err = fmt.Errorf("failed to create YouTube client adapter: %w", adapterErr)
		log.Error().Err(err).Msg("YouTube client adapter creation failed")
		return nil, ytClient, clientCtx, clientCtxCancel, err
	}
	// Initialize YouTube crawler with the adapter
	crawlerConfig := map[string]interface{}{
		"client":        ytAdapter,
		"state_manager": sm,
		"crawler_config": map[string]interface{}{
			"sampling_method":    crawlCfg.SamplingMethod,
			"min_channel_videos": crawlCfg.MinChannelVideos,
		},
	}
	if ytErr = ytCrawler.Initialize(clientCtx, crawlerConfig); ytErr != nil {
		err = fmt.Errorf("failed to initialize YouTube crawler: %w", ytErr)
		log.Error().Err(err).Msg("Failed to initialize YouTube crawler")
		return ytCrawler, ytClient, clientCtx, clientCtxCancel, err
	}
	return ytCrawler, ytClient, clientCtx, clientCtxCancel, nil
}

func CalculateDateFilters(crawlCfg common.CrawlerConfig) (time.Time, time.Time) {
	// Execute the crawl job with date filtering
	var fromTime, toTime time.Time
	if !crawlCfg.DateBetweenMin.IsZero() && !crawlCfg.DateBetweenMax.IsZero() {
		// Use date-between range
		fromTime = crawlCfg.DateBetweenMin
		toTime = crawlCfg.DateBetweenMax
		log.Info().
			Time("date_between_min", fromTime).
			Time("date_between_max", toTime).
			Msg("Using date-between filter for YouTube crawl in DAPR mode")
	} else if !crawlCfg.PostRecency.IsZero() {
		// Use PostRecency var to generate time between
		fromTime = crawlCfg.PostRecency
		toTime = time.Now()
		log.Debug().
			Time("date_between_min", fromTime).
			Time("date_between_max", toTime).
			Msg("Using time-ago filter for YouTube crawl in DAPR mode")
	} else {
		// Use traditional min post date with current time as upper bound
		fromTime = crawlCfg.MinPostDate
		toTime = time.Now()
	}
	return fromTime, toTime
}

func FetchYoutubeChannelInfoAndVideos(ytCrawler crawler.Crawler, crawlCfg common.CrawlerConfig, page state.Page, ctx context.Context) ([]*state.Page, error) {
	var err error
	var discoveredChannels []*state.Page

	// Create a crawl target for the YouTube channel
	target := crawler.CrawlTarget{
		Type: crawler.PlatformYouTube,
		ID:   page.URL, // YouTube channel ID/handle
	}

	// Fetch channel info and videos
	channelData, ytErr := ytCrawler.GetChannelInfo(ctx, target)
	if ytErr != nil {
		err = fmt.Errorf("failed to get YouTube channel info: %w", ytErr)
		log.Error().Err(err).Msg("Failed to get YouTube channel info")
		return []*state.Page{}, err
	}
	validationResult := crawlCfg.NullValidator.ValidateChannelData(channelData)

	if !validationResult.Valid {
		err = fmt.Errorf("Missing critical fields: %v", validationResult.Errors)
		log.Error().Strs("validation_errors", validationResult.Errors).Msg("Missing critical fields in youtube channel data. Skipping")
		return []*state.Page{}, err
	}

	fromTime, toTime := CalculateDateFilters(crawlCfg)
	job := crawler.CrawlJob{
		Target:        target,
		FromTime:      fromTime,
		ToTime:        toTime,
		Limit:         crawlCfg.MaxPosts,
		SampleSize:    crawlCfg.SampleSize,
		NullValidator: crawlCfg.NullValidator,
	}

	// Log job details
	log.Debug().Time("from_time", fromTime).Time("to_time", toTime).Int("limit", job.Limit).
		Msg("YouTube crawl job configured in DAPR mode")

	result, ytErr := ytCrawler.FetchMessages(ctx, job)
	if ytErr != nil {
		err = fmt.Errorf("failed to fetch YouTube videos: %w", ytErr)
		log.Error().Err(err).Msg("Failed to fetch YouTube videos")
		return []*state.Page{}, err
	} else {
		log.Debug().
			Int("video_count", len(result.Posts)).
			Str("channel", page.URL).
			Msg("Successfully crawled YouTube channel")

		// For now, we don't discover channels from YouTube
		discoveredChannels = []*state.Page{}
	}
	return discoveredChannels, nil
}

func RunRandomYoutubeSample(ctx context.Context, sm state.StateManagementInterface, crawlCfg common.CrawlerConfig) {
	// Guard against a no-op run with no target sample count
	if crawlCfg.SampleSize <= 0 {
		log.Warn().Msg("YouTube random sampling requires SampleSize > 0; nothing to do")
		return
	}

	fromTime, toTime := CalculateDateFilters(crawlCfg)
	target := crawler.CrawlTarget{
		Type: crawler.PlatformYouTube,
		ID:   crawlCfg.CrawlID,
	}
	job := crawler.CrawlJob{
		Target:           target,
		FromTime:         fromTime,
		ToTime:           toTime,
		Limit:            crawlCfg.MaxPosts,
		SampleSize:       crawlCfg.SampleSize,
		SamplesRemaining: crawlCfg.SampleSize,
		NullValidator:    crawlCfg.NullValidator,
	}

	ytCrawler, ytClient, clientCtx, clientCtxCancel, ytInitErr := InitializeYoutubeCrawlerComponents(ctx, sm, crawlCfg)
	if ytInitErr != nil {
		return
	}
	defer clientCtxCancel()

	maxIter := crawlCfg.SampleSize*100 + 100
	for iter := 0; iter < maxIter; iter++ {
		var result crawler.CrawlResult
		var ytErr error
		backoff := time.Second
		for attempt := 0; attempt < 3; attempt++ {
			result, ytErr = ytCrawler.FetchMessages(ctx, job)
			if ytErr == nil {
				break
			}
			log.Warn().Err(ytErr).Int("attempt", attempt+1).Msg("FetchMessages failed, retrying")
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff *= 2
		}
		if ytErr != nil {
			log.Error().Err(ytErr).Msg("Failed to fetch messages after retries")
			break
		}
		resultsCount := len(result.Posts)
		job.SamplesRemaining = job.SamplesRemaining - resultsCount
		log.Info().Int("new_videos_processed", resultsCount).Int("samples_left", job.SamplesRemaining).
			Msg("YouTube random sampling progress")
		if job.SamplesRemaining <= 0 {
			log.Info().Int("samples_left", job.SamplesRemaining).Msg("Finished fetching random samples")
			break
		}
		if iter == maxIter-1 {
			log.Warn().Int("max_iterations", maxIter).Msg("Hit max iterations without reaching sample target")
		}
	}
	// Disconnect YouTube client
	if ytClient != nil {
		if disconnectErr := ytClient.Disconnect(clientCtx); disconnectErr != nil {
			log.Warn().Err(disconnectErr).Msg("Error disconnecting YouTube client")
		}
	}
}

type ytWorker struct {
	crawler  crawler.Crawler
	client   clientpkg.Client
	ctx      context.Context
	cancel   context.CancelFunc // Now we have something to call!
	usage    int
	retireAt int
}

func createFreshWorker(sm state.StateManagementInterface, crawlCfg common.CrawlerConfig) (*ytWorker, error) {
	c, cl, cCtx, cCtxCancel, err := InitializeYoutubeCrawlerComponents(context.Background(), sm, crawlCfg)
	if err != nil {
		return nil, err
	}

	// Define a base life and add ±20% jitter
	base := 50
	jitter := rand.Intn(21) - 10 // range -10 to +10

	return &ytWorker{
		crawler:  c,
		client:   cl,
		ctx:      cCtx,
		cancel:   cCtxCancel, // Call this if you have it!
		usage:    0,
		retireAt: base + jitter,
	}, nil
}

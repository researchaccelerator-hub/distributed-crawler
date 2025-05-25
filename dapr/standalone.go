// Package dapr provides Dapr-related functionality
package dapr

import (
	"context"
	"fmt"
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
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello, World!")
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

	log.Info().Msg("Waiting 300 seconds for Dapr sidecar to initialize...")
	time.Sleep(300 * time.Second)
	log.Info().Msg("Dapr sidecar initialization wait complete")

	http.HandleFunc("/", handler)
	go func() {
		if err := http.ListenAndServe(":6481", nil); err != nil {
			// Log the error and exit the application if the server fails to start
			fmt.Printf("Failed to start server: %v\n", err)
			return
		}
	}()

	// Create a file cleaner that targets the same location as where connections are unzipped
	// to ensure proper cleanup of temporary files
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

	if len(urls) == 0 {
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

	launch(urls, crawlerCfg)

	log.Info().Msg("Crawling completed")
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
	// Create a global map to track all URLs we've seen across all layers
	seenURLs := make(map[string]bool)

	// Initialize seenURLs with the seed URLs
	for _, url := range stringList {
		seenURLs[url] = true
	}

	// Initialize state manager factory
	log.Info().Msgf("Starting scraper for crawl ID: %s", crawlCfg.CrawlID)
	smfact := state.DefaultStateManagerFactory{}

	// Create a temporary state manager to check for incomplete crawls
	tempCfg := state.Config{
		StorageRoot: crawlCfg.StorageRoot,
		CrawlID:     crawlCfg.CrawlID,
		Platform:    crawlCfg.Platform, // Pass the platform information
	}

	tempSM, err := smfact.Create(tempCfg)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create temporary state manager")
	}

	// Check for an existing incomplete crawl
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

	// Create the actual state manager with the determined execution ID
	cfg := state.Config{
		StorageRoot:      crawlCfg.StorageRoot,
		CrawlID:          crawlCfg.CrawlID,
		CrawlExecutionID: crawlexecid,
		Platform:         crawlCfg.Platform, // Pass the platform information

		// Add the MaxPages config
		MaxPagesConfig: &state.MaxPagesConfig{
			MaxPages: crawlCfg.MaxPages,
		},
	}

	sm, err := smfact.Create(cfg)
	if err != nil {
		log.Error().Err(err).Msg("Failed to initialize state manager")
		return
	}

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	// Get the existing layers or seed a new crawl
	err = sm.Initialize(stringList)
	if err != nil {
		log.Error().Err(err).Msg("Failed to set up seed URLs")
		return
	}

	// Process layers iteratively, with potential for new layers to be added during execution
	depth := 0
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

		if depth > crawlCfg.MaxDepth {
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
		processLayerInParallel(layer, crawlCfg.Concurrency, sm, crawlCfg)

		// Log progress after completing a layer
		log.Info().Msgf("Completed layer at depth %d", depth)

		// Move to the next depth
		depth++
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
		return
	}
	log.Info().Msg("All items processed successfully.")
}

// processLayerInParallel processes all pages in a layer with a maximum of maxWorkers concurrent goroutines.
// It uses a semaphore pattern to limit concurrency and ensures all pages are processed before returning.
// This version uses the connection pool for efficient client management.
func processLayerInParallel(layer *state.Layer, maxWorkers int, sm state.StateManagementInterface, crawlCfg common.CrawlerConfig) {
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

				// Initialize YouTube components
				clientCtx := context.Background()
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
				} else {
					// Connect to YouTube API
					if ytErr = ytClient.Connect(clientCtx); ytErr != nil {
						err = fmt.Errorf("failed to connect to YouTube API: %w", ytErr)
						log.Error().Err(err).Msg("YouTube API connection failed")
					} else {
						// Create crawler factory and register crawlers
						factory := crawler.NewCrawlerFactory()
						if ytErr = crawlercommon.RegisterAllCrawlers(factory); ytErr != nil {
							err = fmt.Errorf("failed to register crawlers: %w", ytErr)
							log.Error().Err(err).Msg("Failed to register YouTube crawler")
						} else {
							// Create YouTube crawler
							ytCrawler, ytErr := factory.GetCrawler(crawler.PlatformYouTube)
							if ytErr != nil {
								err = fmt.Errorf("failed to create YouTube crawler: %w", ytErr)
								log.Error().Err(err).Msg("Failed to create YouTube crawler")
							} else {
								// Create YouTubeClient adapter
								ytAdapter, adapterErr := youtube.NewClientAdapter(ytClient)
								if adapterErr != nil {
									err = fmt.Errorf("failed to create YouTube client adapter: %w", adapterErr)
									log.Error().Err(err).Msg("YouTube client adapter creation failed")
								} else {
									// Initialize YouTube crawler with the adapter
									crawlerConfig := map[string]interface{}{
										"client":        ytAdapter,
										"state_manager": sm,
									}

									if ytErr = ytCrawler.Initialize(clientCtx, crawlerConfig); ytErr != nil {
										err = fmt.Errorf("failed to initialize YouTube crawler: %w", ytErr)
										log.Error().Err(err).Msg("Failed to initialize YouTube crawler")
									} else {
										// Create a crawl target for the YouTube channel
										target := crawler.CrawlTarget{
											Type: crawler.PlatformYouTube,
											ID:   page.URL, // YouTube channel ID/handle
										}

										// Fetch channel info and videos
										_, ytErr := ytCrawler.GetChannelInfo(ctx, target)
										if ytErr != nil {
											err = fmt.Errorf("failed to get YouTube channel info: %w", ytErr)
											log.Error().Err(err).Msg("Failed to get YouTube channel info")
										} else {
											// Execute the crawl job
											job := crawler.CrawlJob{
												Target:   target,
												FromTime: crawlCfg.MinPostDate,
												ToTime:   time.Now(), // Use current time as the upper bound
												Limit:    crawlCfg.MaxPosts,
											}

											// Log job details
											log.Debug().
												Time("from_time", job.FromTime).
												Time("to_time", job.ToTime).
												Int("limit", job.Limit).
												Msg("YouTube crawl job configured in DAPR mode")

											result, ytErr := ytCrawler.FetchMessages(ctx, job)
											if ytErr != nil {
												err = fmt.Errorf("failed to fetch YouTube videos: %w", ytErr)
												log.Error().Err(err).Msg("Failed to fetch YouTube videos")
											} else {
												log.Info().
													Int("video_count", len(result.Posts)).
													Str("channel", page.URL).
													Msg("Successfully crawled YouTube channel")

												// For now, we don't discover channels from YouTube
												discoveredChannels = []*state.Page{}
											}
										}

										// Cleanup YouTube crawler resources
										if closeErr := ytCrawler.Close(); closeErr != nil {
											log.Warn().Err(closeErr).Msg("Error closing YouTube crawler")
										}
									}
								}
							}
						}
					}

					// Disconnect YouTube client
					if disconnectErr := ytClient.Disconnect(clientCtx); disconnectErr != nil {
						log.Warn().Err(disconnectErr).Msg("Error disconnecting YouTube client")
					}
				}
			} else {
				// Telegram platform processing (default)
				// Use the pooled channel processing
				log.Info().Msgf("Starting run for Telegram channel: %s", page.URL)
				discoveredChannels, err = crawl.RunForChannelWithPool(ctx, &page, crawlCfg.StorageRoot, sm, crawlCfg)
			}

			log.Info().Msgf("Page processed for %s", page.URL)

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

	// Log summary of unique pages processed
	uniqueCount := len(uniquePages)
	duplicateCount := originalCount - uniqueCount
	log.Info().
		Int("original_page_count", originalCount).
		Int("unique_page_count", uniqueCount).
		Int("duplicate_page_count", duplicateCount).
		Msgf("Processed %d unique pages (skipped %d duplicates) in layer at depth %d",
			uniqueCount, duplicateCount, layer.Depth)

	// After all pages in the layer are processed, append the new layer with all discovered channels
	if len(allDiscoveredChannels) > 0 {
		currentDepth := layer.Depth
		newPages := make([]state.Page, 0, len(allDiscoveredChannels))

		// Track unique URLs in the new layer
		newLayerUniqueURLs := make(map[string]bool)

		// Count of total and unique pages for logging
		totalDiscovered := len(allDiscoveredChannels)
		uniqueDiscovered := 0

		for _, channel := range allDiscoveredChannels {
			// Skip if this URL has already been seen in the new layer
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
			// Create a new Page for each discovered channel
			page := state.Page{
				URL:       channel.URL,      // Assuming channel has a URL field
				Depth:     currentDepth + 1, // Set depth one level deeper than current
				Status:    "unfetched",
				Timestamp: time.Now(),
				ParentID:  parentID, // Set parent ID to the current page being processed
				// Any other fields you need to set
			}
			newPages = append(newPages, page)
		}

		// Log the deduplication results for the new layer
		log.Info().
			Int("total_discovered", totalDiscovered).
			Int("unique_discovered", uniqueDiscovered).
			Int("duplicate_discovered", totalDiscovered-uniqueDiscovered).
			Msgf("Deduplicated discovered channels for next layer at depth %d", currentDepth+1)

		if err := sm.AddLayer(newPages); err != nil {
			log.Error().Err(err).Msg("Failed to add discovered channels as new layer")
		} else {
			log.Info().Int("count", len(newPages)).Msg("Added new channels to be processed")

			// Save state after adding new pages
			if err := sm.SaveState(); err != nil {
				log.Error().Err(err).Msg("Failed to save state after adding new layer")
			}
		}
	}
}

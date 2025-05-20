package standalone

import (
	"context"
	"encoding/json"
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
	"github.com/zelenin/go-tdlib/client"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

// StartStandaloneMode initializes and starts the crawler in standalone mode. It collects URLs from the provided list or file,
// configures the crawler using the specified configuration, and optionally runs code generation. If no URLs are provided,
// the function logs a fatal error. The function logs the start and completion of the crawling process.
// Parameters:
//   - urlList: A list of URLs to crawl.
//   - urlFile: A file containing URLs to crawl.
//   - crawlerCfg: Configuration settings for the crawler.
//   - generateCode: A flag indicating whether to run code generation.
func StartStandaloneMode(urlList []string, urlFile string, crawlerCfg common.CrawlerConfig, generateCode bool) {
	log.Info().Msg("Starting crawler in standalone mode")

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

	if !generateCode && len(urls) == 0 {
		log.Fatal().Msg("No URLs provided. Use --urls or --url-file to specify URLs to crawl")
	}

	log.Info().Msgf("Starting crawl of %d URLs with concurrency %d", len(urls), crawlerCfg.Concurrency)

	if generateCode {
		log.Info().Msg("Running code generation...")
		//svc := &telegramhelper.RealTelegramService{}
		//telegramhelper.GenCode(svc, crawlerCfg.StorageRoot)
		generatePCode()
		os.Exit(0)
	}

	launch(urls, crawlerCfg)

	log.Info().Msg("Crawling completed")
}

func generatePCode() {
	// This function doesn't receive crawler config, so we use a default verbosity level
	var (
		apiIdRaw    = os.Getenv("TG_API_ID")
		apiHash     = os.Getenv("TG_API_HASH")
		phoneNumber = os.Getenv("TG_PHONE_NUMBER")
		phoneCode   = os.Getenv("TG_PHONE_CODE")
	)

	apiId64, err := strconv.ParseInt(apiIdRaw, 10, 32)
	if err != nil {
		log.Fatal().Msgf("strconv.Atoi error: %s", err)
	}

	// Ensure .tdlib directory exists
	tdlibDir := ".tdlib"
	if _, err := os.Stat(tdlibDir); os.IsNotExist(err) {
		err = os.Mkdir(tdlibDir, 0755)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to create .tdlib directory")
		}
	}

	authorizer := client.ClientAuthorizer()
	authorizer.TdlibParameters <- &client.SetTdlibParametersRequest{
		UseTestDc:           false,
		DatabaseDirectory:   filepath.Join(tdlibDir, "database"),
		FilesDirectory:      filepath.Join(tdlibDir, "files"),
		UseFileDatabase:     true,
		UseChatInfoDatabase: true,
		UseMessageDatabase:  true,
		UseSecretChats:      false,
		ApiId:               int32(apiId64),
		ApiHash:             apiHash,
		SystemLanguageCode:  "en",
		DeviceModel:         "Server",
		SystemVersion:       "1.0.0",
		ApplicationVersion:  "1.0.0",
	}

	// Set up authentication environment variables 
	telegramhelper.SetupAuth(phoneNumber, phoneCode)
	
	// Use the default CLI interactor
	go client.CliInteractor(authorizer)

	// Get verbosity level from configuration (default is 1, will use 0 if not specified)
	verbosityLevel := 1 // Default
	
	log.Debug().Int("verbosity_level", verbosityLevel).Msg("Setting TDLib verbosity level for code generation")
	_, err = client.SetLogVerbosityLevel(&client.SetLogVerbosityLevelRequest{
		NewVerbosityLevel: int32(verbosityLevel),
	})
	if err != nil {
		log.Fatal().Msgf("SetLogVerbosityLevel error: %s", err)
	}

	tdlibClient, err := client.NewClient(authorizer)
	if err != nil {
		log.Fatal().Msgf("NewClient error: %s", err)
	}

	versionOption, err := client.GetOption(&client.GetOptionRequest{
		Name: "version",
	})
	if err != nil {
		log.Fatal().Msgf("GetOption error: %s", err)
	}

	commitOption, err := client.GetOption(&client.GetOptionRequest{
		Name: "commit_hash",
	})
	if err != nil {
		log.Fatal().Msgf("GetOption error: %s", err)
	}

	log.Printf("TDLib version: %s (commit: %s)", versionOption.(*client.OptionValueString).Value, commitOption.(*client.OptionValueString).Value)

	me, err := tdlibClient.GetMe()
	if err != nil {
		log.Fatal().Msgf("GetMe error: %s", err)
	}

	log.Printf("Me: %s %s", me.FirstName, me.LastName)

	// Import the Credentials type from telegramhelper
	// Create the credentials object
	creds := telegramhelper.Credentials{
		APIId:       apiIdRaw,
		APIHash:     apiHash,
		PhoneNumber: phoneNumber,
		PhoneCode:   phoneCode,
	}

	// Convert to JSON
	credsJson, err := json.MarshalIndent(creds, "", "  ")
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal credentials to JSON")
	} else {
		// Write to file in .tdlib directory
		credsPath := filepath.Join(tdlibDir, "credentials.json")
		err = os.WriteFile(credsPath, credsJson, 0600) // Use restrictive permissions for sensitive data
		if err != nil {
			log.Error().Err(err).Msg("Failed to write credentials to file")
		} else {
			log.Info().Msgf("Credentials saved to %s", credsPath)
		}
	}

	// Wait for signal to exit
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	tdlibClient.Close()
	os.Exit(1)
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
	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	
	// Store the state manager in a package-level variable so signal handler can access it
	var shutdownSM state.StateManagementInterface
	
	// Start a goroutine to handle shutdown signals
	go func() {
		sig := <-sigChan
		log.Warn().Str("signal", sig.String()).Msg("Received shutdown signal, performing graceful shutdown")
		
		// If we have a state manager, close it to save any pending data
		if shutdownSM != nil {
			log.Info().Msg("Saving media cache and state during signal-triggered shutdown")
			if err := shutdownSM.Close(); err != nil {
				log.Error().Err(err).Msg("Error saving state during shutdown")
			} else {
				log.Info().Msg("Successfully saved state during shutdown")
			}
		}
		
		log.Info().Msg("Shutdown complete, exiting")
		os.Exit(0)
	}()

	// Initialize state manager factory
	log.Info().Str("platform", crawlCfg.Platform).Msgf("Starting %s scraper for crawl ID: %s", crawlCfg.Platform, crawlCfg.CrawlID)
	smfact := state.NewStateManagerFactory()

	// Create a state manager configuration specifically for checking incomplete crawls
	// Include all necessary configuration to ensure proper state loading
	tempCfg := state.Config{
		StorageRoot: crawlCfg.StorageRoot,
		CrawlID:     crawlCfg.CrawlID,
		Platform:    crawlCfg.Platform, // Pass the platform information
		
		// Configure DAPR if we're using it (this ensures proper state lookup)
		DaprConfig: &state.DaprConfig{
			StateStoreName: "statestore", // Default DAPR state store name
			ComponentName:  "statestore", // Default component name
		},
	}

	// Create a temporary state manager to look for incomplete crawls
	log.Info().Msgf("Checking for incomplete crawls with ID: %s", crawlCfg.CrawlID)
	tempSM, err := smfact.Create(tempCfg)
	if err != nil {
		log.Error().Err(err).Msg("Failed to create temporary state manager, will start fresh")
	}

	// Check for an existing incomplete crawl
	var crawlexecid string
	if tempSM != nil {
		// Look for an incomplete crawl with this ID
		existingExecID, exists, err := tempSM.FindIncompleteCrawl(crawlCfg.CrawlID)
		if err != nil {
			log.Warn().Err(err).Msg("Error checking for existing crawls, starting fresh")
		} else if exists && existingExecID != "" {
			// Found an incomplete crawl to resume
			crawlexecid = existingExecID
			log.Info().Msgf("Resuming existing crawl: %s (execution: %s)",
				crawlCfg.CrawlID, crawlexecid)
		} else {
			log.Debug().Msg("No incomplete crawl found, will start a new execution")
		}

		// Close the temporary state manager to free up resources
		if err := tempSM.Close(); err != nil {
			log.Warn().Err(err).Msg("Error closing temporary state manager")
		}
	}

	// If no existing crawl was found or there was an error, generate a new execution ID
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
		
		// Add the DAPR config here too to ensure proper state storage
		DaprConfig: &state.DaprConfig{
			StateStoreName: "statestore",
			ComponentName:  "statestore",
		},
		
		// Add the MaxPages config
		MaxPagesConfig: &state.MaxPagesConfig{
			MaxPages: crawlCfg.MaxPages,
		},
	}

	sm, err := smfact.Create(cfg)
	if err != nil {
		log.Error().Err(err).Msg("Failed to load progress")
		return
	}
	
	// Store reference to state manager for signal handler
	shutdownSM = sm
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	// Initialize with seed URLs if this is a new crawl
	// If resuming an existing crawl, this is a no-op 
	// as the state manager already has the data
	err = sm.Initialize(stringList)
	if err != nil {
		log.Error().Err(err).Msg("Failed to initialize state")
		return
	}

	// Initialize connection pool with an appropriate size
	poolSize := crawlCfg.Concurrency
	if poolSize < 1 {
		poolSize = 1
	}

	// If we have database URLs, use those to determine pool size
	if len(crawlCfg.TDLibDatabaseURLs) > 0 {
		log.Info().Msgf("Found %d TDLib database URLs for connection pooling", len(crawlCfg.TDLibDatabaseURLs))
		// Use the smaller of concurrency or number of database URLs
		if len(crawlCfg.TDLibDatabaseURLs) < poolSize {
			poolSize = len(crawlCfg.TDLibDatabaseURLs)
			log.Info().Msgf("Adjusting pool size to %d to match available database URLs", poolSize)
		}
	}

	// Platform-specific initialization
	var connect crawler.TDLibClient
	var ytClient clientpkg.Client
	var ytCrawler crawler.Crawler
	
	// Setup cleanup for YouTube resources if needed
	defer func() {
		if ytCrawler != nil {
			log.Info().Msg("Cleaning up YouTube crawler resources")
			if err := ytCrawler.Close(); err != nil {
				log.Warn().Err(err).Msg("Error while closing YouTube crawler")
			}
		}
	}()
	
	if crawlCfg.Platform == "youtube" {
		// YouTube platform initialization
		log.Info().Msg("Initializing YouTube crawler components")
		
		// Validate YouTube API key
		if crawlCfg.YouTubeAPIKey == "" {
			log.Error().Msg("YouTube API key is required for YouTube platform. Please provide it with --youtube-api-key flag")
			return
		}
		
		// Create YouTube client using factory
		clientCtx := context.Background()
		clientFactory := clientpkg.NewDefaultClientFactory()
		
		// Debug API key passing
		if crawlCfg.YouTubeAPIKey == "" {
			log.Error().Msg("YouTube API key is empty - make sure you provided it with --youtube-api-key")
		} else {
			log.Debug().Str("api_key_length", fmt.Sprintf("%d chars", len(crawlCfg.YouTubeAPIKey))).Msg("Using YouTube API key")
		}
		
		config := map[string]interface{}{
			"api_key": crawlCfg.YouTubeAPIKey,
		}
		
		var err error
		ytClient, err = clientFactory.CreateClient(clientCtx, "youtube", config)
		if err != nil {
			log.Error().Err(err).Msg("Failed to create YouTube client")
			return
		}
		
		// Connect to YouTube API
		err = ytClient.Connect(clientCtx)
		if err != nil {
			log.Error().Err(err).Msg("Failed to connect to YouTube API")
			return
		}
		
		// Create and initialize YouTube crawler factory
		factory := crawler.NewCrawlerFactory()
		if err = crawlercommon.RegisterAllCrawlers(factory); err != nil {
			log.Error().Err(err).Msg("Failed to register crawlers")
			return
		}
		
		// Create YouTube crawler
		ytCrawler, err = factory.GetCrawler(crawler.PlatformYouTube)
		if err != nil {
			log.Error().Err(err).Msg("Failed to create YouTube crawler")
			return
		}
		
		// Initialize the YouTube crawler
		// Create YouTubeClient adapter
		ytAdapter, adapterErr := youtube.NewClientAdapter(ytClient)
		if adapterErr != nil {
			err = fmt.Errorf("failed to create YouTube client adapter: %w", adapterErr)
			log.Error().Err(err).Msg("YouTube client adapter creation failed")
			return
		}
		
		crawlerConfig := map[string]interface{}{
			"client": ytAdapter,
			"state_manager": sm,
			"crawl_label": crawlCfg.CrawlLabel, // Pass the crawl label to be added to posts
		}
		
		err = ytCrawler.Initialize(clientCtx, crawlerConfig)
		if err != nil {
			log.Error().Err(err).Msg("Failed to initialize YouTube crawler")
			return
		}
		
		log.Info().Msg("YouTube crawler components initialized successfully")
	} else {
		// Telegram platform initialization (default)
		// Initialize the connection pool
		crawl.InitConnectionPool(poolSize, crawlCfg.StorageRoot, crawlCfg)
		defer crawl.CloseConnectionPool()
	
		// Create a single non-pooled connection for backward compatibility
		var connectErr error
		connect, connectErr = crawl.Connect(crawlCfg.StorageRoot, crawlCfg)
		if connectErr != nil {
			log.Error().Err(connectErr).Msg("Failed to create Telegram connection")
			return
		}
	}
	
	// Process layers sequentially starting from depth 0
	// and continuing until max depth is reached or no more layers exist
	currentDepth := 0
	maxDepthConfig := crawlCfg.MaxDepth 
	
	// If maxDepth is -1 or 0, we'll just continue until no more layers are found
	if maxDepthConfig <= 0 {
		maxDepthConfig = 1000 // Set a very high number as default
	}
	
	log.Info().Int("maxDepth", maxDepthConfig).Msg("Starting multi-layer crawl")
	
	// Track overall statistics
	var totalPagesProcessed, totalPagesSkipped, totalPagesSuccess, totalPagesError int
	
	for currentDepth <= maxDepthConfig {
		// Fetch the current layer of pages
		currentLayer, err := sm.GetLayerByDepth(currentDepth)
		if err != nil {
			log.Error().Err(err).Int("depth", currentDepth).Msg("Failed to get layer pages")
			break
		}
		
		// If no pages at this depth, we've reached the end of the crawl
		if len(currentLayer) == 0 {
			log.Info().Int("depth", currentDepth).Msg("No pages found at this depth, crawl complete")
			break
		}
		
		log.Info().Msgf("Processing layer at depth %d with %d pages", currentDepth, len(currentLayer))
		
		// Print all page statuses before processing
		log.Info().Int("page_count", len(currentLayer)).Int("depth", currentDepth).Msg("Page status summary before processing")
		pageStatusCount := make(map[string]int)
		for _, page := range currentLayer {
			pageStatusCount[page.Status]++
			log.Debug().
				Str("url", page.URL).
				Str("status", page.Status).
				Str("id", page.ID).
				Int("message_count", len(page.Messages)).
				Time("timestamp", page.Timestamp).
				Bool("resuming_execution", isResumingSameCrawlExecution).
				Msg("Page status before processing in standalone mode")
		}
		// Log the counts of pages by status
		for status, count := range pageStatusCount {
			log.Info().Str("status", status).Int("count", count).Int("depth", currentDepth).Msg("Page status count")
		}
		
		// Track statistics for this layer
		var layerPages, layerSkipped, layerSuccess, layerError int
		layerPages = len(currentLayer)
		totalPagesProcessed += layerPages
		
		// Process each page in the current layer
		// Find the first page in the layer that needs processing
		startIndex := 0
		for i, la := range currentLayer {
			// Print debug information about each page discovered during crawl restart
			log.Debug().
				Str("url", la.URL).
				Str("status", la.Status).
				Str("id", la.ID).
				Int("message_count", len(la.Messages)).
				Bool("resuming_execution", isResumingSameCrawlExecution).
				Msg("Page discovered during crawl restart")
				
			// Check the page status to determine if we need to process it
			if la.Status == "fetched" {
				if isResumingSameCrawlExecution {
					// When resuming with the same crawlexecutionid, skip already fetched pages
					// regardless of message status - this prevents reprocessing
					log.Debug().Str("url", la.URL).Msg("Skipping already fetched page during same execution resume")
					layerSkipped++
					totalPagesSkipped++
					startIndex = i + 1
					continue
				} else {
					// When starting a new crawlexecutionid, we'll process fetched pages
					// and rely on the resample flag for message level decisions
					log.Debug().Str("url", la.URL).Msg("Processing fetched page in new execution, will use resample flag")
					break
				}
			}
			
			// If we found a page that's not fetched, this is where we want to start
			break
		}
		
		// Process from the first page that needs processing
		log.Info().Int("starting_index", startIndex).Int("total_pages", len(currentLayer)).Msg("Starting processing from index")
		
		for i := startIndex; i < len(currentLayer); i++ {
			la := currentLayer[i]
			
			// Double-check status since we're now using an index-based approach
			if la.Status == "fetched" {
				if isResumingSameCrawlExecution {
					// When resuming with the same crawlexecutionid, skip already fetched pages
					// regardless of message status - this prevents reprocessing
					log.Debug().Str("url", la.URL).Msg("Skipping already fetched page during same execution resume")
					layerSkipped++
					totalPagesSkipped++
					continue
				} else {
					// For new execution IDs, process the page and rely on message status checks
					log.Debug().Str("url", la.URL).Msg("Processing fetched page in new execution, will use resample flag")
					// Continue processing this page
				}
			}
			
			if la.Status == "processing" {
				log.Info().Str("url", la.URL).Msg("Found page in 'processing' state - will retry")
				// Continue to process it
			}
			
			// Process this page in a self-contained function to handle panics
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Error().Msgf("Recovered from panic while processing item: %s, error: %v", la.URL, r)
						la.Status = "error" // Mark as error so we can retry later
						layerError++
						totalPagesError++
						
						// Make sure we save the state even after a panic
						saveErr := sm.SaveState()
						if saveErr != nil {
							log.Error().Err(saveErr).Msg("Failed to save state after panic")
						}
					}
				}()

				// Update page status and timestamp before processing
				la.Timestamp = time.Now()
				la.Status = "processing" // Mark as in-progress
				
				// Save state before processing to record that we're working on this page
				saveErr := sm.SaveState()
				if saveErr != nil {
					log.Warn().Err(saveErr).Str("url", la.URL).Msg("Failed to save state before processing page")
				}
				
				// Try to use the connection pool
				var discoveredChannels []*state.Page
				var runErr error

				log.Info().Msgf("Processing page: %s", la.URL)

				// Create context for operations
				ctx := context.Background()
				
				// Process based on selected platform
				if crawlCfg.Platform == "youtube" {
					log.Info().Str("url", la.URL).Msg("Processing YouTube channel")
					
					// Create a crawl target for the YouTube channel
					target := crawler.CrawlTarget{
						Type: crawler.PlatformYouTube,
						ID:   la.URL, // YouTube channel ID/handle
					}
					
					// Fetch channel information first
					channelInfo, err := ytCrawler.GetChannelInfo(ctx, target)
					if err != nil {
						log.Error().Err(err).Str("channel", la.URL).Msg("Failed to get YouTube channel info")
						runErr = err
					} else {
						log.Info().
							Str("channel_name", channelInfo.ChannelName).
							Int("subscribers", channelInfo.ChannelEngagementData.FollowerCount).
							Msg("Retrieved YouTube channel info")
							
						// Construct crawl job with appropriate time filters
						// Construct crawl job with appropriate time filters
						job := crawler.CrawlJob{
							Target:   target,
							FromTime: crawlCfg.MinPostDate,
							ToTime:   time.Now(), // Use current time as the upper bound
							Limit:    crawlCfg.MaxPosts,
						}
						
						log.Debug().
							Time("from_time", crawlCfg.MinPostDate).
							Time("to_time", job.ToTime).
							Int("limit", job.Limit).
							Msg("YouTube crawl job configured")
						
						// Execute the crawl
						result, err := ytCrawler.FetchMessages(ctx, job)
						if err != nil {
							log.Error().Err(err).Str("channel", la.URL).Msg("Failed to fetch YouTube videos")
							runErr = err
						} else {
							log.Info().
								Int("video_count", len(result.Posts)).
								Str("channel", la.URL).
								Msg("Successfully crawled YouTube channel")
								
							// For now, we don't handle outlinks from YouTube channels
							discoveredChannels = []*state.Page{}
						}
					}
				} else {
					// Telegram platform processing (default)
					// Try to get the connection pool stats
					poolStats := crawl.GetConnectionPoolStats()
					log.Info().Interface("poolStats", poolStats).Msg("Connection pool status")
					
					// Use the connection pool if it's initialized
					if crawl.IsConnectionPoolInitialized() {
						log.Info().Msg("Using connection pool for channel processing")
						discoveredChannels, runErr = crawl.RunForChannelWithPool(ctx, &la, crawlCfg.StorageRoot, sm, crawlCfg)
					} else {
						log.Info().Msg("No connection pool available, using single connection")
						discoveredChannels, runErr = crawl.RunForChannel(connect, &la, crawlCfg.StorageRoot, sm, crawlCfg)
					}
				}

				if runErr != nil {
					log.Error().Stack().Err(runErr).Msgf("Error processing item %s", la.URL)
					la.Status = "error"
					layerError++
					totalPagesError++
				} else {
					la.Status = "fetched"
					log.Info().Msgf("Successfully processed page: %s", la.URL)
					layerSuccess++
					totalPagesSuccess++

					// Handle any discovered channels from this page
					if len(discoveredChannels) > 0 {
						log.Info().Msgf("Discovered %d new channels from %s", len(discoveredChannels), la.URL)
						
						// Convert to Page structs needed for AddLayer
						newPages := make([]state.Page, 0, len(discoveredChannels))
						for _, channel := range discoveredChannels {
							// Use the existing Page struct directly
							newPages = append(newPages, *channel)
						}
						
						// Add the new channels as a layer
						if err := sm.AddLayer(newPages); err != nil {
							log.Error().Err(err).Msg("Failed to add discovered channels as new layer")
						} else {
							log.Info().Int("count", len(newPages)).Msg("Added new channels to be processed in next layer")
						}
					}
				}

				// Save state after processing
				saveErr = sm.SaveState()
				if saveErr != nil {
					log.Error().Stack().Err(saveErr).Msg("Failed to save state after processing page")
				}
			}()
		}
		
		// Log statistics about the layer processing
		log.Info().
			Int("depth", currentDepth).
			Int("totalPages", layerPages).
			Int("skippedPages", layerSkipped).
			Int("successPages", layerSuccess).
			Int("errorPages", layerError).
			Msg("Layer processing statistics")
		
		// Move to the next depth
		currentDepth++
	}
	
	// Log overall statistics
	log.Info().
		Int("totalPagesProcessed", totalPagesProcessed).
		Int("totalPagesSkipped", totalPagesSkipped).
		Int("totalPagesSuccess", totalPagesSuccess).
		Int("totalPagesError", totalPagesError).
		Int("maxDepthReached", currentDepth-1).
		Msg("Overall crawl statistics")
			
	// Update crawl metadata to mark as completed if all pages were processed successfully
	if totalPagesError == 0 {
		// Explicitly call Close() to save any unsaved cache data
		// This ensures media cache is fully persisted before marking the crawl as completed
		log.Info().Msg("Saving final state before marking crawl as completed")
		if closeErr := sm.Close(); closeErr != nil {
			log.Warn().Err(closeErr).Msg("Error during final state save, but will continue with crawl completion")
		}
		
		metadata := map[string]interface{}{
			"status":  "completed",
			"endTime": time.Now(),
		}
		err := sm.UpdateCrawlMetadata(crawlCfg.CrawlID, metadata)
		if err != nil {
			log.Error().Err(err).Msg("Failed to update crawl metadata")
		} else {
			log.Info().Msg("Crawl marked as completed successfully")
		}
	} else {
		log.Info().Int("errorPages", totalPagesError).Msg("Crawl completed with errors - can be resumed later")
		// Still save any unsaved cache data even if there were errors
		if closeErr := sm.Close(); closeErr != nil {
			log.Warn().Err(closeErr).Msg("Error during final state save")
		}
	}

	log.Info().Msg("All layers processed successfully.")
}
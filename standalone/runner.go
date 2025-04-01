package standalone

import (
	"context"
	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/crawl"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/zelenin/go-tdlib/client"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
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
		fileURLs, err := readURLsFromFile(urlFile)
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
	var (
		apiIdRaw = os.Getenv("TG_API_ID")
		apiHash  = os.Getenv("TG_API_HASH")
	)

	apiId64, err := strconv.ParseInt(apiIdRaw, 10, 32)
	if err != nil {
		log.Fatal().Msgf("strconv.Atoi error: %s", err)
	}

	authorizer := client.ClientAuthorizer()
	authorizer.TdlibParameters <- &client.SetTdlibParametersRequest{
		UseTestDc:           false,
		DatabaseDirectory:   filepath.Join(".tdlib", "database"),
		FilesDirectory:      filepath.Join(".tdlib", "files"),
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

	go client.CliInteractor(authorizer)

	_, err = client.SetLogVerbosityLevel(&client.SetLogVerbosityLevelRequest{
		NewVerbosityLevel: 0,
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

	ch := make(chan os.Signal, 2)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	tdlibClient.Close()
	os.Exit(1)
}

// readURLsFromFile reads a file specified by the filename and returns a slice of URLs.
// It ignores empty lines and lines starting with a '#' character, which are considered comments.
// Returns an error if the file cannot be read.
func readURLsFromFile(filename string) ([]string, error) {
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
	tempCfg := state.Config{
		StorageRoot: crawlCfg.StorageRoot,
		JobID:       "",
		CrawlID:     crawlCfg.CrawlID,
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
	
	// Create the actual state manager with the determined execution ID
	cfg := state.Config{
		StorageRoot:      crawlCfg.StorageRoot,
		JobID:            "",
		CrawlID:          crawlCfg.CrawlID,
		CrawlExecutionID: crawlexecid,
	}
	
	sm, err := smfact.Create(cfg)
	if err != nil {
		log.Error().Err(err).Msg("Failed to load progress")
	}
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	//Layer Zero Loaded
	err = sm.Initialize(stringList)

	if err != nil {
		log.Error().Err(err).Msg("Failed to initialize state")
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
	
	// Initialize the connection pool
	crawl.InitConnectionPool(poolSize, crawlCfg.StorageRoot, crawlCfg)
	defer crawl.CloseConnectionPool()
	
	// Create a context for the connections
	ctx := context.Background()
	
	// Create a single non-pooled connection for backward compatibility
	connect, _ := crawl.Connect(crawlCfg.StorageRoot, crawlCfg)
	layerzero, err := sm.GetLayerByDepth(0)
	for _, la := range layerzero {
		if la.Status != "fetched" {
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Error().Msgf("Recovered from panic while processing item: %s, error: %v", la.URL, r)
						// Continue to the next item
					}
				}()

				la.Timestamp = time.Now()
				// Try to use the connection pool
				var discoveredChannels []*state.Page
				var err error
				
				log.Info().Msgf("Processing page: %s", la.URL)
				
				// Use pool if available, fall back to direct connection
				if crawl.GetConnectionPool() != nil {
					discoveredChannels, err = crawl.RunForChannelWithPool(ctx, &la, crawlCfg.StorageRoot, sm, crawlCfg)
				} else {
					discoveredChannels, err = crawl.RunForChannel(connect, &la, crawlCfg.StorageRoot, sm, crawlCfg)
				}
				
				if err != nil {
					log.Error().Stack().Err(err).Msgf("Error processing item %s", la.URL)
					la.Status = "error"
				} else {
					la.Status = "fetched"
					log.Info().Msgf("Successfully processed page: %s", la.URL)
					
					// Handle any discovered channels from this page
					if len(discoveredChannels) > 0 {
						log.Info().Msgf("Discovered %d new channels from %s", len(discoveredChannels), la.URL)
					}
				}

				//err := sm.StoreLayers(list)
				if err != nil {
					log.Error().Stack().Err(err).Msg("Failed to store layers")
				}
			}()
		}
	}

	log.Info().Msg("All items processed successfully.")

}

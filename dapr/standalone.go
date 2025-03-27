package dapr

import (
	"fmt"
	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/crawl"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/researchaccelerator-hub/telegram-scraper/telegramhelper"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello, World!")
}

// StartStandaloneMode initializes and starts the crawler in standalone mode. It collects URLs from the provided list or file,
// configures the crawler using the specified configuration, and optionally runs code generation. If no URLs are provided,
// the function logs a fatal error. The function logs the start and completion of the crawling process.
// Parameters:
//   - urlList: A list of URLs to crawl.
//   - urlFile: A file containing URLs to crawl.
//   - crawlerCfg: Configuration settings for the crawler.
//   - generateCode: A flag indicating whether to run code generation.
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

	launch(urls, crawlerCfg)

	log.Info().Msg("Crawling completed")
	select {}
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
	seenURLs := make(map[string]bool)

	// Initialize seenURLs with the seed URLs
	for _, url := range stringList {
		seenURLs[url] = true
	}

	crawlexecid := common.GenerateCrawlID()
	log.Info().Msgf("Starting scraper for crawl: %s", crawlCfg.CrawlID)

	cfg := state.Config{
		StorageRoot:      crawlCfg.StorageRoot,
		JobID:            "",
		CrawlID:          crawlCfg.CrawlID,
		CrawlExecutionID: crawlexecid,
	}

	smfact := state.DefaultStateManagerFactory{}
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
func processLayerInParallel(layer *state.Layer, maxWorkers int, sm state.StateManagementInterface, crawlCfg common.CrawlerConfig) {

	// Create a mutex to protect shared state
	var mutex sync.Mutex

	// Map to collect all discovered channels
	allDiscoveredChannels := make([]*state.Page, 0)

	// Process each page in the current layer
	for pageIndex := 0; pageIndex < len(layer.Pages); pageIndex++ {
		pageToProcess := layer.Pages[pageIndex]

		// Skip already processed pages
		if pageToProcess.Status == "fetched" {
			log.Debug().Msgf("Skipping already processed page: %s", pageToProcess.URL)
			continue
		}

		defer func() {
			if r := recover(); r != nil {
				log.Error().Msgf("Recovered from panic while processing item: %s, error: %v", pageToProcess.URL, r)

				// Update the page status to error
				pageToProcess.Status = "error"
				pageToProcess.Error = fmt.Sprintf("Panic: %v", r)

				// Update the page in the state manager
				if err := sm.UpdatePage(pageToProcess); err != nil {
					log.Error().Err(err).Msg("Failed to update page status after panic")
				}

				// Save state after recovery
				mutex.Lock()
				if err := sm.SaveState(); err != nil {
					log.Panic().Stack().Err(err).Msg("Failed to save state after panic recovery")
				}
				mutex.Unlock()
			}
		}()

		pageToProcess.Timestamp = time.Now()

		connect, err := crawl.Connect(crawlCfg.StorageRoot, crawlCfg)
		if err != nil {
			log.Error().Err(err).Msg("Failed to connect to tdlib")
			return
		}
		log.Info().Msgf("Starting run for channel: %s", pageToProcess.URL)
		// Run the crawler for this page
		discoveredChannels, err := crawl.RunForChannel(connect, &pageToProcess, crawlCfg.StorageRoot, sm, crawlCfg)
		log.Info().Msgf("Page processed for %s", pageToProcess.URL)
		ok, err := connect.Close()
		if err != nil {
			log.Error().Err(err).Msg("Failed to close connection")
			return
		}
		if ok == nil {
			log.Error().Err(err).Msg("No OK signal on close connection")
		}
		if err != nil {
			log.Error().Stack().Err(err).Msgf("Error processing item %s", pageToProcess.URL)
			pageToProcess.Status = "error"
			pageToProcess.Error = err.Error()

			// Update the page in the state manager
			if updateErr := sm.UpdatePage(pageToProcess); updateErr != nil {
				log.Error().Err(updateErr).Msg("Failed to update page status after error")
			}
		} else {
			mutex.Lock()
			defer mutex.Unlock()

			pageToProcess.Status = "fetched"
			if updateErr := sm.UpdatePage(pageToProcess); updateErr != nil {
				log.Error().Err(updateErr).Msg("Failed to update page status after successful processing")
			}

			// Save the entire state
			if err := sm.SaveState(); err != nil {
				log.Panic().Stack().Err(err).Msgf("Error saving state after processing channel %s", pageToProcess.URL)
			}

			// Collect discovered channels
			allDiscoveredChannels = append(allDiscoveredChannels, discoveredChannels...)
		}
	}

	// After all pages in the layer are processed, append the new layer with all discovered channels
	if len(allDiscoveredChannels) > 0 {
		currentDepth := layer.Depth
		newPages := make([]state.Page, 0, len(allDiscoveredChannels))
		for _, channel := range allDiscoveredChannels {
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
		if err := sm.AddLayer(newPages); err != nil {
			log.Error().Err(err).Msg("Failed to add discovered channels as new layer")
		} else {
			log.Info().Int("count", len(newPages)).Msg("Added new channels to be processed")

			// Save state after adding new pages
			if err := sm.SaveState(); err != nil {
				log.Panic().Err(err).Msg("Failed to save state after adding new layer")
			}
		}
	}
}

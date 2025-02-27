package standalone

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"strings"
	"tdlib-scraper/common"
	"tdlib-scraper/crawl"
	"tdlib-scraper/state"
	"tdlib-scraper/telegramhelper"
)

// Start the crawler in standalone mode
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

	if len(urls) == 0 {
		log.Fatal().Msg("No URLs provided. Use --urls or --url-file to specify URLs to crawl")
	}

	log.Info().Msgf("Starting crawl of %d URLs with concurrency %d", len(urls), crawlerCfg.Concurrency)

	if generateCode {
		log.Info().Msg("Running code generation...")
		telegramhelper.GenCode()
		os.Exit(0)
	}

	launch(urls, crawlerCfg)

	log.Info().Msg("Crawling completed")
}

// Helper function to read URLs from a file
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

func launch(stringList []string, crawlCfg common.CrawlerConfig) {

	crawlid := common.GenerateCrawlID()
	log.Info().Msgf("Starting scraper for crawl: %s", crawlid)
	sm := state.NewStateManager(crawlCfg.StorageRoot, crawlid)
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	list, err := sm.SeedSetup(stringList)
	// Load progress
	progress, err := sm.LoadProgress()
	if err != nil {
		log.Error().Err(err).Msg("Failed to load progress")
	}
	// Process remaining items
	for i := progress; i < len(list); i++ {
		item := list[i]
		log.Info().Msgf("Processing item: %s", item)

		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Error().Msgf("Recovered from panic while processing item: %s, error: %v", item, r)
					// Continue to the next item
				}
			}()

			if err = crawl.Run(crawlid, item, crawlCfg.StorageRoot, *sm); err != nil {
				log.Error().Stack().Err(err).Msgf("Error processing item %s", item)
			}
		}()

		// Update progress
		progress = i + 1
		if err = sm.SaveProgress(progress); err != nil {
			log.Fatal().Err(err).Msgf("Failed to save progress: %v", err)
		}
	}

	log.Info().Msg("All items processed successfully.")

}

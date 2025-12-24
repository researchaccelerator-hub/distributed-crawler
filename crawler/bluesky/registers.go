package bluesky

import (
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/rs/zerolog/log"
)

// RegisterBlueskyCrawler registers the Bluesky crawler with the factory.
func RegisterBlueskyCrawler(factory *crawler.DefaultCrawlerFactory) error {
	log.Info().Msg("Registering Bluesky crawler")

	return factory.RegisterCrawler(crawler.PlatformBluesky, func() crawler.Crawler {
		return NewBlueskyCrawler()
	})
}

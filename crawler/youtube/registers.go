package youtube

import (
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
)

// RegisterYouTubeCrawler registers the YouTube crawler with the factory
func RegisterYouTubeCrawler(factory *crawler.DefaultCrawlerFactory) error {
	return factory.RegisterCrawler(crawler.PlatformYouTube, func() crawler.Crawler {
		return NewYouTubeCrawler()
	})
}
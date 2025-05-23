package telegram

import (
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
)

// RegisterTelegramCrawler registers the Telegram crawler with the factory
func RegisterTelegramCrawler(factory *crawler.DefaultCrawlerFactory) error {
	return factory.RegisterCrawler(crawler.PlatformTelegram, func() crawler.Crawler {
		return NewTelegramCrawler()
	})
}
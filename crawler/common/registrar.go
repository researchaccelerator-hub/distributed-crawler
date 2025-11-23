// Package common provides shared functionality for crawlers
package common

import (
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler/bluesky"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler/telegram"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler/youtube"
)

// RegisterAllCrawlers registers all crawler implementations with the factory
func RegisterAllCrawlers(factory *crawler.DefaultCrawlerFactory) error {
	// Register Telegram crawler
	if err := telegram.RegisterTelegramCrawler(factory); err != nil {
		return err
	}

	// Register YouTube crawler
	if err := youtube.RegisterYouTubeCrawler(factory); err != nil {
		return err
	}

	// Register Bluesky crawler
	if err := bluesky.RegisterBlueskyCrawler(factory); err != nil {
		return err
	}

	return nil
}
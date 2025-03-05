// Package crawl provides functionality to crawl Telegram channels and extract data.
package crawl

import (
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/zelenin/go-tdlib/client"
)

//// MessageProcessor defines the interface for message processing during crawling
//type MessageProcessor interface {
//	ProcessMessage(
//		tdlibClient crawler.TDLibClient,
//		message *client.Message,
//		info *channelInfo,
//		crawlID string,
//		channelUsername string,
//		sm *state.StateManager) error
//}
//
//// MessageFetcher defines the interface for fetching messages from a chat
//type MessageFetcher interface {
//	FetchMessages(tdlibClient crawler.TDLibClient, chatID int64, fromMessageID int64) ([]*client.Message, error)
//}

// StandardMessageProcessor is the default implementation of MessageProcessor
type StandardMessageProcessor struct{}

// ProcessMessage processes a message according to the standard implementation
func (p *StandardMessageProcessor) ProcessMessage(
	tdlibClient crawler.TDLibClient,
	message *client.Message,
	info *channelInfo,
	crawlID string,
	channelUsername string,
	sm *state.StateManager) error {

	return processMessage(tdlibClient, message, info, crawlID, channelUsername, *sm)
}

// StandardMessageFetcher is the default implementation of MessageFetcher
type StandardMessageFetcher struct{}

// FetchMessages fetches messages according to the standard implementation
func (f *StandardMessageFetcher) FetchMessages(
	tdlibClient crawler.TDLibClient,
	chatID int64,
	fromMessageID int64) ([]*client.Message, error) {

	return fetchMessages(tdlibClient, chatID, fromMessageID)
}

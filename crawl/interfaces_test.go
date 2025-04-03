// Package crawl provides functionality to crawl Telegram channels and extract data.
package crawl

import (
	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/zelenin/go-tdlib/client"
)

// StandardMessageProcessor is the default implementation of MessageProcessor
type StandardMessageProcessor struct{}

// ProcessMessage processes a message according to the standard implementation
func (p *StandardMessageProcessor) ProcessMessage(
	tdlibClient crawler.TDLibClient,
	message *client.Message,
	messageId int64,
	chatId int64,
	info *channelInfo,
	crawlID string,
	channelUsername string,
	sm *state.StateManagementInterface,
	cfg common.CrawlerConfig) ([]string, error) {

	return processMessage(tdlibClient, message, messageId, chatId, info, crawlID, channelUsername, *sm, cfg)
}

// StandardMessageFetcher is the default implementation of MessageFetcher
type StandardMessageFetcher struct{}

// FetchMessages fetches messages according to the standard implementation
func (f *StandardMessageFetcher) FetchMessages(
	tdlibClient crawler.TDLibClient,
	chatID int64,
	fromMessageID int64) ([]*client.Message, error) {

	// Not directly used anymore, as we're using telegramhelper.FetchChannelMessages
	// But kept for test compatibility
	request := client.GetChatHistoryRequest{
		ChatId:        chatID,
		FromMessageId: fromMessageID,
		Offset:        0,
		Limit:         100,
		OnlyLocal:     false,
	}
	messages, err := tdlibClient.GetChatHistory(&request)
	if err != nil {
		return nil, err
	}
	
	if messages == nil {
		return nil, nil
	}
	
	if messages.Messages == nil {
		return []*client.Message{}, nil
	}
	
	return messages.Messages, nil
}

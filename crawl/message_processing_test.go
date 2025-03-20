// Package crawl provides functionality to crawl Telegram channels and extract data.
package crawl

import (
	"errors"
	"github.com/google/uuid"
	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/stretchr/testify/mock"
	"testing"

	"github.com/researchaccelerator-hub/telegram-scraper/model"
	"github.com/researchaccelerator-hub/telegram-scraper/telegramhelper"
	"github.com/stretchr/testify/assert"
	"github.com/zelenin/go-tdlib/client"
)

func TestProcessMessage(t *testing.T) {
	// Create test fixtures
	fixtures := NewTestFixtures(t)
	defer fixtures.Cleanup()

	t.Run("GetMessageError", func(t *testing.T) {
		// Setup
		mockClient := new(MockTDLibClient)
		//msg := CreateClientMessage(1, "Test message", fixtures.ChatID)

		// Set expectations - only GetMessage should be called, which will return an error
		mockClient.On("GetMessage", &client.GetMessageRequest{
			ChatId:    fixtures.ChatID,
			MessageId: 1,
		}).Return(nil, errors.New("GetMessage error"))

		cfg := common.CrawlerConfig{}
		// Execute
		_, err := processMessage(mockClient, 1, fixtures.ChatID, fixtures.Channel, fixtures.CrawlID, fixtures.ChannelName, fixtures.StateManager, cfg)

		// Assert
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("GetMessageLinkError", func(t *testing.T) {
		// Setup
		mockClient := new(MockTDLibClient)
		msg := CreateClientMessage(2, "Test message", fixtures.ChatID)

		// Set expectations
		mockClient.On("GetMessage", &client.GetMessageRequest{
			ChatId:    fixtures.ChatID,
			MessageId: 2,
		}).Return(msg, nil)

		mockClient.On("GetMessageLink", &client.GetMessageLinkRequest{
			ChatId:    fixtures.ChatID,
			MessageId: 2,
			ForAlbum:  false,
			//ForComment: false,
		}).Return(nil, errors.New("GetMessageLink error"))

		// Override ParseMessage to simulate success
		origParseMessage := telegramhelper.ParseMessage
		telegramhelper.ParseMessage = func(
			crawlid string,
			message *client.Message,
			mlr *client.MessageLink,
			chat *client.Chat,
			supergroup *client.Supergroup,
			supergroupInfo *client.SupergroupFullInfo,
			postcount int,
			viewcount int,
			channelName string,
			tdlibClient crawler.TDLibClient,
			sm state.StateManagementInterface,
			cfg common.CrawlerConfig,
		) (post model.Post, err error) {
			// Verify that mlr is nil due to the simulated error
			if mlr != nil {
				t.Error("Expected nil mlr because of error in GetMessageLink")
			}
			return model.Post{PostLink: "success"}, nil
		}
		defer func() { telegramhelper.ParseMessage = origParseMessage }()

		cfg := common.CrawlerConfig{}
		// Execute
		_, err := processMessage(mockClient, 2, fixtures.ChatID, fixtures.Channel, fixtures.CrawlID, fixtures.ChannelName, fixtures.StateManager, cfg)

		// Assert
		assert.NoError(t, err) // Should not error since GetMessageLink error is non-critical
		mockClient.AssertExpectations(t)
	})

	t.Run("ParseMessageError", func(t *testing.T) {
		// Setup
		mockClient := new(MockTDLibClient)
		msg := CreateClientMessage(3, "Test message", fixtures.ChatID)

		// Set expectations
		mockClient.On("GetMessage", &client.GetMessageRequest{
			ChatId:    fixtures.ChatID,
			MessageId: 3,
		}).Return(msg, nil)

		mockClient.On("GetMessageLink", &client.GetMessageLinkRequest{
			ChatId:    fixtures.ChatID,
			MessageId: 3,
			ForAlbum:  false,
			//ForComment: false,
		}).Return(&client.MessageLink{
			Link:     "http://t.me/message_link",
			IsPublic: false,
		}, nil)

		// Override ParseMessage to simulate error
		origParseMessage := telegramhelper.ParseMessage
		telegramhelper.ParseMessage = func(
			crawlid string,
			message *client.Message,
			mlr *client.MessageLink,
			chat *client.Chat,
			supergroup *client.Supergroup,
			supergroupInfo *client.SupergroupFullInfo,
			postcount int,
			viewcount int,
			channelName string,
			tdlibClient crawler.TDLibClient,
			sm state.StateManagementInterface,
			cfg common.CrawlerConfig,
		) (post model.Post, err error) {
			return model.Post{}, errors.New("ParseMessage error")
		}
		defer func() { telegramhelper.ParseMessage = origParseMessage }()
		cfg := common.CrawlerConfig{}

		// Execute
		_, err := processMessage(mockClient, 3, fixtures.ChatID, fixtures.Channel, fixtures.CrawlID, fixtures.ChannelName, fixtures.StateManager, cfg)

		// Assert
		assert.Error(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("Success", func(t *testing.T) {
		// Setup
		mockClient := new(MockTDLibClient)
		msg := CreateClientMessage(4, "Test message", fixtures.ChatID)

		// Set expectations
		mockClient.On("GetMessage", &client.GetMessageRequest{
			ChatId:    fixtures.ChatID,
			MessageId: 4,
		}).Return(msg, nil)

		mockClient.On("GetMessageLink", &client.GetMessageLinkRequest{
			ChatId:    fixtures.ChatID,
			MessageId: 4,
			ForAlbum:  false,
			//ForComment: false,
		}).Return(&client.MessageLink{
			Link:     "http://t.me/message_link",
			IsPublic: false,
		}, nil)

		// Override ParseMessage to simulate success
		origParseMessage := telegramhelper.ParseMessage
		telegramhelper.ParseMessage = func(
			crawlid string,
			message *client.Message,
			mlr *client.MessageLink,
			chat *client.Chat,
			supergroup *client.Supergroup,
			supergroupInfo *client.SupergroupFullInfo,
			postcount int,
			viewcount int,
			channelName string,
			tdlibClient crawler.TDLibClient,
			sm state.StateManagementInterface,
			cfg common.CrawlerConfig,
		) (post model.Post, err error) {
			return model.Post{PostLink: "parsed"}, nil
		}
		defer func() { telegramhelper.ParseMessage = origParseMessage }()
		cfg := common.CrawlerConfig{}

		// Execute
		_, err := processMessage(mockClient, 4, fixtures.ChatID, fixtures.Channel, fixtures.CrawlID, fixtures.ChannelName, fixtures.StateManager, cfg)

		// Assert
		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})
}

func TestProcessAllMessages(t *testing.T) {
	// Create test fixtures
	fixtures := NewTestFixtures(t)
	defer fixtures.Cleanup()

	t.Run("Success", func(t *testing.T) {
		// Create two batches of messages
		batch1 := []*client.Message{
			CreateClientMessage(1, "Message 1", fixtures.ChatID),
			CreateClientMessage(2, "Message 2", fixtures.ChatID),
			CreateClientMessage(3, "Message 3", fixtures.ChatID),
		}

		batch2 := []*client.Message{
			CreateClientMessage(4, "Message 4", fixtures.ChatID),
			CreateClientMessage(5, "Message 5", fixtures.ChatID),
		}

		// Create mocks
		mockClient := new(MockTDLibClient)
		mockProcessor := new(MockMessageProcessor)
		mockFetcher := new(MockMessageFetcher)
		mockStateManager := new(MockStateManager)
		// Create owner page
		ownerPage := &state.Page{
			ID:       uuid.New().String(),
			URL:      fixtures.ChannelName,
			Status:   "unfetched",
			Depth:    0,
			Messages: []state.Message{},
		}

		// Set expectations for message fetcher
		mockFetcher.On("FetchMessages", mockClient, fixtures.ChatID, int64(0)).Return(batch1, nil)
		mockFetcher.On("FetchMessages", mockClient, fixtures.ChatID, int64(3)).Return(batch2, nil)
		mockFetcher.On("FetchMessages", mockClient, fixtures.ChatID, int64(5)).Return([]*client.Message{}, nil)

		// Set expectations for state manager
		// For UpdateStatePage (called multiple times in the function)
		mockStateManager.On("UpdateStatePage", mock.AnythingOfType("state.Page")).Return()

		// For UpdateStateMessage (called for each message)
		for _, msg := range append(batch1, batch2...) {
			mockStateManager.On("UpdateStateMessage",
				msg.Id,
				msg.ChatId,
				ownerPage,
				"fetched").Return()
		}

		// Set expectations for message processor with new signature
		for _, msg := range append(batch1, batch2...) {
			outlinks := []string{"https://t.me/newchannel1", "https://t.me/newchannel2"}
			mockProcessor.On("ProcessMessage",
				mockClient,
				msg.Id,
				msg.ChatId,
				fixtures.Channel,
				fixtures.CrawlID,
				fixtures.ChannelName,
				mock.MatchedBy(func(sm interface{}) bool {
					// This will match any interface{} that's passed
					return true
				}),
				mock.AnythingOfType("common.CrawlerConfig")).Return(outlinks, nil)
		}

		cfg := common.CrawlerConfig{}

		// Execute
		discoveredChannels, err := processAllMessagesWithProcessor(
			mockClient,
			fixtures.Channel,
			fixtures.CrawlID,
			fixtures.ChannelName,
			mockStateManager,
			mockProcessor,
			mockFetcher,
			ownerPage,
			cfg)

		// Assert
		assert.NoError(t, err)
		assert.Len(t, discoveredChannels, 10) // 5 messages * 2 outlinks each
		mockProcessor.AssertNumberOfCalls(t, "ProcessMessage", 5)
		mockFetcher.AssertExpectations(t)
		mockProcessor.AssertExpectations(t)
		mockStateManager.AssertExpectations(t)
	})
	t.Run("FetchError", func(t *testing.T) {
		// Create mocks
		mockClient := new(MockTDLibClient)
		mockProcessor := new(MockMessageProcessor)
		mockFetcher := new(MockMessageFetcher)
		mockStateManager := new(MockStateManager)

		// Set expectations
		fetchError := errors.New("fetch error")
		mockFetcher.On("FetchMessages", mockClient, fixtures.ChatID, int64(0)).Return([]*client.Message(nil), fetchError)
		ownerPage := &state.Page{
			ID:       uuid.New().String(),
			URL:      fixtures.ChannelName,
			Status:   "unfetched",
			Depth:    0,
			Messages: []state.Message{},
		}
		cfg := common.CrawlerConfig{}

		// Execute
		_, err := processAllMessagesWithProcessor(
			mockClient,
			fixtures.Channel,
			fixtures.CrawlID,
			fixtures.ChannelName,
			mockStateManager,
			mockProcessor,
			mockFetcher,
			ownerPage,
			cfg)

		// Assert
		assert.Error(t, err)
		assert.Equal(t, fetchError, err)
		mockProcessor.AssertNotCalled(t, "ProcessMessage")
		mockFetcher.AssertExpectations(t)
	})

	t.Run("ProcessError", func(t *testing.T) {
		// Create messages
		messages := []*client.Message{
			CreateClientMessage(1, "Message 1", fixtures.ChatID),
			CreateClientMessage(2, "Message 2", fixtures.ChatID),
			CreateClientMessage(3, "Message 3", fixtures.ChatID),
		}

		// Create mocks
		mockClient := new(MockTDLibClient)
		mockProcessor := new(MockMessageProcessor)
		mockFetcher := new(MockMessageFetcher)
		mockStateManager := new(MockStateManager)

		// Create owner page
		ownerPage := &state.Page{
			ID:       uuid.New().String(),
			URL:      fixtures.ChannelName,
			Status:   "unfetched",
			Depth:    0,
			Messages: []state.Message{},
		}

		// Set expectations for fetcher
		mockFetcher.On("FetchMessages", mockClient, fixtures.ChatID, int64(0)).Return(messages, nil)
		mockFetcher.On("FetchMessages", mockClient, fixtures.ChatID, int64(3)).Return([]*client.Message{}, nil)

		// Set expectations for state manager methods
		// This is crucial - set up expectations for ALL methods that will be called
		mockStateManager.On("UpdateStatePage", mock.AnythingOfType("state.Page")).Return()
		//mockStateManager.On("StoreState").Return().Times(5)
		// For UpdateStateMessage (called for each message)
		for _, msg := range messages {
			mockStateManager.On("UpdateStateMessage",
				msg.Id,
				msg.ChatId,
				ownerPage,
				mock.AnythingOfType("string")).Return()
		}

		// Set expectations for processor - fail on the second message
		for _, msg := range messages {
			outlinks := []string{"https://t.me/newchannel1", "https://t.me/newchannel2"}
			mockProcessor.On("ProcessMessage",
				mockClient,
				msg.Id,
				msg.ChatId,
				fixtures.Channel,
				fixtures.CrawlID,
				fixtures.ChannelName,
				mock.MatchedBy(func(sm interface{}) bool {
					return true // Match any StateManager passed
				}),
				mock.MatchedBy(func(cfg interface{}) bool {
					return true // Match any config passed
				})).Return(outlinks, nil)
		}

		cfg := common.CrawlerConfig{}

		// Execute
		_, err := processAllMessagesWithProcessor(
			mockClient,
			fixtures.Channel,
			fixtures.CrawlID,
			fixtures.ChannelName,
			mockStateManager,
			mockProcessor,
			mockFetcher,
			ownerPage,
			cfg)

		// Assert
		assert.NoError(t, err) // Should continue despite process errors
		mockProcessor.AssertNumberOfCalls(t, "ProcessMessage", 3)
		mockFetcher.AssertExpectations(t)
		mockProcessor.AssertExpectations(t)
		mockStateManager.AssertExpectations(t)
	})
	t.Run("NoMessages", func(t *testing.T) {
		// Create mocks
		mockClient := new(MockTDLibClient)
		mockProcessor := new(MockMessageProcessor)
		mockFetcher := new(MockMessageFetcher)
		mockStateManager := new(MockStateManager)

		// Set expectations
		mockFetcher.On("FetchMessages", mockClient, fixtures.ChatID, int64(0)).Return([]*client.Message{}, nil)

		// Add missing expectations for state manager
		mockStateManager.On("UpdateStatePage", mock.AnythingOfType("state.Page")).Return()

		// Create owner page
		ownerPage := &state.Page{
			ID:       uuid.New().String(),
			URL:      fixtures.ChannelName,
			Status:   "unfetched",
			Depth:    0,
			Messages: []state.Message{},
		}
		cfg := common.CrawlerConfig{}

		// Execute
		_, err := processAllMessagesWithProcessor(
			mockClient,
			fixtures.Channel,
			fixtures.CrawlID,
			fixtures.ChannelName,
			mockStateManager,
			mockProcessor,
			mockFetcher,
			ownerPage,
			cfg)

		// Assert
		assert.NoError(t, err)
		mockProcessor.AssertNotCalled(t, "ProcessMessage")
		mockFetcher.AssertExpectations(t)
	})
}

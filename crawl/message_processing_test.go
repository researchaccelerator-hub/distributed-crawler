// Package crawl provides functionality to crawl Telegram channels and extract data.
package crawl

import (
	"errors"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
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
		msg := CreateClientMessage(1, "Test message", fixtures.ChatID)

		// Set expectations - only GetMessage should be called, which will return an error
		mockClient.On("GetMessage", &client.GetMessageRequest{
			ChatId:    fixtures.ChatID,
			MessageId: 1,
		}).Return(nil, errors.New("GetMessage error"))

		// Execute
		err := processMessage(mockClient, msg, fixtures.Channel, fixtures.CrawlID, fixtures.ChannelName, *fixtures.StateManager)

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
			sm state.StateManager,
		) (post model.Post, err error) {
			// Verify that mlr is nil due to the simulated error
			if mlr != nil {
				t.Error("Expected nil mlr because of error in GetMessageLink")
			}
			return model.Post{PostLink: "success"}, nil
		}
		defer func() { telegramhelper.ParseMessage = origParseMessage }()

		// Execute
		err := processMessage(mockClient, msg, fixtures.Channel, fixtures.CrawlID, fixtures.ChannelName, *fixtures.StateManager)

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
			sm state.StateManager,
		) (post model.Post, err error) {
			return model.Post{}, errors.New("ParseMessage error")
		}
		defer func() { telegramhelper.ParseMessage = origParseMessage }()

		// Execute
		err := processMessage(mockClient, msg, fixtures.Channel, fixtures.CrawlID, fixtures.ChannelName, *fixtures.StateManager)

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
			sm state.StateManager,
		) (post model.Post, err error) {
			return model.Post{PostLink: "parsed"}, nil
		}
		defer func() { telegramhelper.ParseMessage = origParseMessage }()

		// Execute
		err := processMessage(mockClient, msg, fixtures.Channel, fixtures.CrawlID, fixtures.ChannelName, *fixtures.StateManager)

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

		// Set expectations for message fetcher
		mockFetcher.On("FetchMessages", mockClient, fixtures.ChatID, int64(0)).Return(batch1, nil)
		mockFetcher.On("FetchMessages", mockClient, fixtures.ChatID, int64(3)).Return(batch2, nil)
		mockFetcher.On("FetchMessages", mockClient, fixtures.ChatID, int64(5)).Return([]*client.Message{}, nil)

		// Set expectations for message processor
		for _, msg := range append(batch1, batch2...) {
			mockProcessor.On("ProcessMessage",
				mockClient,
				msg,
				fixtures.Channel,
				fixtures.CrawlID,
				fixtures.ChannelName,
				fixtures.StateManager).Return(nil)
		}

		// Execute
		stateManagerValue := *fixtures.StateManager
		err := processAllMessagesWithProcessor(
			mockClient,
			fixtures.Channel,
			fixtures.CrawlID,
			fixtures.ChannelName,
			stateManagerValue,
			mockProcessor,
			mockFetcher)

		// Assert
		assert.NoError(t, err)
		mockProcessor.AssertNumberOfCalls(t, "ProcessMessage", 5)
		mockFetcher.AssertExpectations(t)
		mockProcessor.AssertExpectations(t)
	})

	t.Run("FetchError", func(t *testing.T) {
		// Create mocks
		mockClient := new(MockTDLibClient)
		mockProcessor := new(MockMessageProcessor)
		mockFetcher := new(MockMessageFetcher)

		// Set expectations
		fetchError := errors.New("fetch error")
		mockFetcher.On("FetchMessages", mockClient, fixtures.ChatID, int64(0)).Return([]*client.Message(nil), fetchError)

		// Execute
		err := processAllMessagesWithProcessor(
			mockClient,
			fixtures.Channel,
			fixtures.CrawlID,
			fixtures.ChannelName,
			*fixtures.StateManager,
			mockProcessor,
			mockFetcher)

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

		// Set expectations for fetcher
		mockFetcher.On("FetchMessages", mockClient, fixtures.ChatID, int64(0)).Return(messages, nil)
		mockFetcher.On("FetchMessages", mockClient, fixtures.ChatID, int64(3)).Return([]*client.Message{}, nil)

		// Set expectations for processor - fail on the second message
		mockProcessor.On("ProcessMessage", mockClient, messages[0], fixtures.Channel, fixtures.CrawlID, fixtures.ChannelName, fixtures.StateManager).Return(nil)
		mockProcessor.On("ProcessMessage", mockClient, messages[1], fixtures.Channel, fixtures.CrawlID, fixtures.ChannelName, fixtures.StateManager).Return(errors.New("process error"))
		mockProcessor.On("ProcessMessage", mockClient, messages[2], fixtures.Channel, fixtures.CrawlID, fixtures.ChannelName, fixtures.StateManager).Return(nil)

		// Execute
		err := processAllMessagesWithProcessor(
			mockClient,
			fixtures.Channel,
			fixtures.CrawlID,
			fixtures.ChannelName,
			*fixtures.StateManager,
			mockProcessor,
			mockFetcher)

		// Assert
		assert.NoError(t, err) // Should continue despite process errors
		mockProcessor.AssertNumberOfCalls(t, "ProcessMessage", 3)
		mockFetcher.AssertExpectations(t)
		mockProcessor.AssertExpectations(t)
	})

	t.Run("NoMessages", func(t *testing.T) {
		// Create mocks
		mockClient := new(MockTDLibClient)
		mockProcessor := new(MockMessageProcessor)
		mockFetcher := new(MockMessageFetcher)

		// Set expectations
		mockFetcher.On("FetchMessages", mockClient, fixtures.ChatID, int64(0)).Return([]*client.Message{}, nil)

		// Execute
		err := processAllMessagesWithProcessor(
			mockClient,
			fixtures.Channel,
			fixtures.CrawlID,
			fixtures.ChannelName,
			*fixtures.StateManager,
			mockProcessor,
			mockFetcher)

		// Assert
		assert.NoError(t, err)
		mockProcessor.AssertNotCalled(t, "ProcessMessage")
		mockFetcher.AssertExpectations(t)
	})
}

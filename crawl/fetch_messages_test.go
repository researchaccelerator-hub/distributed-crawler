// Package crawl provides functionality to crawl Telegram channels and extract data.
package crawl

import (
	"errors"
	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/zelenin/go-tdlib/client"
)

// Create a DefaultMessageFetcher that implements the MessageFetcher interface
func (f *DefaultMessageFetcher) FetchMessages(tdlibClient crawler.TDLibClient, chatID int64, fromMessageID int64) ([]*client.Message, error) {
	// Default limit
	limit := int32(100)

	// Get chat history
	messages, err := tdlibClient.GetChatHistory(&client.GetChatHistoryRequest{
		ChatId:        chatID,
		FromMessageId: fromMessageID,
		Offset:        0,
		Limit:         limit,
		OnlyLocal:     false,
	})

	if err != nil {
		return nil, err
	}

	// Handle the case where messages is nil
	if messages == nil {
		return nil, nil
	}

	// Handle the case where Messages is nil
	if messages.Messages == nil {
		return []*client.Message{}, nil
	}

	return messages.Messages, nil
}

func TestMessageFetcher(t *testing.T) {
	// Create fixtures
	fixtures := NewTestFixtures(t)
	defer fixtures.Cleanup()

	t.Run("Success", func(t *testing.T) {
		// Setup
		expectedMessages := []*client.Message{
			{
				Id:         1,
				ChatId:     fixtures.ChatID,
				Content:    &client.MessageText{Text: &client.FormattedText{Text: "Message 1"}},
				Date:       int32(time.Now().Unix()),
				IsOutgoing: false,
			},
			{
				Id:         2,
				ChatId:     fixtures.ChatID,
				Content:    &client.MessageText{Text: &client.FormattedText{Text: "Message 2"}},
				Date:       int32(time.Now().Unix()),
				IsOutgoing: true,
			},
		}

		mockClient := new(MockTDLibClient)
		fromMessageID := int64(5)
		limit := int32(100)

		// Set expectations
		mockClient.On("GetChatHistory", &client.GetChatHistoryRequest{
			ChatId:        fixtures.ChatID,
			FromMessageId: fromMessageID,
			Offset:        0,
			Limit:         limit,
			OnlyLocal:     false,
		}).Return(&client.Messages{
			TotalCount: int32(len(expectedMessages)),
			Messages:   expectedMessages,
		}, nil)

		// Create the fetcher
		fetcher := &DefaultMessageFetcher{}

		// Execute
		messages, err := fetcher.FetchMessages(mockClient, fixtures.ChatID, fromMessageID)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, expectedMessages, messages)
		assert.Len(t, messages, 2)
		mockClient.AssertExpectations(t)
	})

	t.Run("EmptyResult", func(t *testing.T) {
		// Setup
		mockClient := new(MockTDLibClient)

		// Set expectations
		mockClient.On("GetChatHistory", &client.GetChatHistoryRequest{
			ChatId:        fixtures.ChatID,
			FromMessageId: 0,
			Offset:        0,
			Limit:         100,
			OnlyLocal:     false,
		}).Return(&client.Messages{
			TotalCount: 0,
			Messages:   []*client.Message{},
		}, nil)

		// Create the fetcher
		fetcher := &DefaultMessageFetcher{}

		// Execute
		messages, err := fetcher.FetchMessages(mockClient, fixtures.ChatID, 0)

		// Assert
		assert.NoError(t, err)
		assert.Empty(t, messages)
		mockClient.AssertExpectations(t)
	})

	t.Run("Error", func(t *testing.T) {
		// Setup
		mockClient := new(MockTDLibClient)
		expectedError := errors.New("API error")

		// Set expectations
		mockClient.On("GetChatHistory", &client.GetChatHistoryRequest{
			ChatId:        fixtures.ChatID,
			FromMessageId: 0,
			Offset:        0,
			Limit:         100,
			OnlyLocal:     false,
		}).Return(nil, expectedError)

		// Create the fetcher
		fetcher := &DefaultMessageFetcher{}

		// Execute
		messages, err := fetcher.FetchMessages(mockClient, fixtures.ChatID, 0)

		// Assert
		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
		assert.Nil(t, messages)
		mockClient.AssertExpectations(t)
	})

	t.Run("NilMessages", func(t *testing.T) {
		// Setup
		mockClient := new(MockTDLibClient)

		// Set expectations
		mockClient.On("GetChatHistory", &client.GetChatHistoryRequest{
			ChatId:        fixtures.ChatID,
			FromMessageId: 0,
			Offset:        0,
			Limit:         100,
			OnlyLocal:     false,
		}).Return(&client.Messages{
			TotalCount: 0,
			Messages:   nil,
		}, nil)

		// Create the fetcher
		fetcher := &DefaultMessageFetcher{}

		// Execute
		messages, err := fetcher.FetchMessages(mockClient, fixtures.ChatID, 0)

		// Assert
		assert.NoError(t, err)
		assert.Empty(t, messages)
		mockClient.AssertExpectations(t)
	})

	t.Run("DifferentMessageTypes", func(t *testing.T) {
		// Setup
		expectedMessages := []*client.Message{
			{
				Id:     1,
				ChatId: fixtures.ChatID,
				Content: &client.MessageText{
					Text: &client.FormattedText{Text: "Text message"},
				},
			},
			{
				Id:     2,
				ChatId: fixtures.ChatID,
				Content: &client.MessagePhoto{
					Photo:   &client.Photo{},
					Caption: &client.FormattedText{Text: "Photo caption"},
				},
			},
			{
				Id:     3,
				ChatId: fixtures.ChatID,
				Content: &client.MessageVideo{
					Video:   &client.Video{},
					Caption: &client.FormattedText{Text: "Video caption"},
				},
			},
		}

		mockClient := new(MockTDLibClient)

		// Set expectations
		mockClient.On("GetChatHistory", &client.GetChatHistoryRequest{
			ChatId:        fixtures.ChatID,
			FromMessageId: 0,
			Offset:        0,
			Limit:         100,
			OnlyLocal:     false,
		}).Return(&client.Messages{
			TotalCount: int32(len(expectedMessages)),
			Messages:   expectedMessages,
		}, nil)

		// Create the fetcher
		fetcher := &DefaultMessageFetcher{}

		// Execute
		messages, err := fetcher.FetchMessages(mockClient, fixtures.ChatID, 0)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, expectedMessages, messages)
		assert.Len(t, messages, 3)
		mockClient.AssertExpectations(t)
	})
}

func TestProcessAllMessagesWithFetcher(t *testing.T) {
	// Create fixtures
	fixtures := NewTestFixtures(t)
	defer fixtures.Cleanup()

	t.Run("Integration", func(t *testing.T) {
		// Create a mock client
		mockClient := new(MockTDLibClient)
		
		// Create a mock state manager
		mockStateManager := new(MockStateManager)
		
		// Note: A message fetcher would normally be used with FetchMessages
		// but in this test we're providing the messages directly to processAllMessagesWithProcessor
		
		// Create test messages
		messages := []*client.Message{
			{
				Id:     1,
				ChatId: fixtures.ChatID,
				Date:   int32(time.Now().Unix()),
				Content: &client.MessageText{
					Text: &client.FormattedText{Text: "Test message with channel @newchannel1"},
				},
			},
		}
		
		// Set up the channel info
		chatInfo := &channelInfo{
			chat: &client.Chat{
				Id:    fixtures.ChatID,
				Title: "Test Channel",
			},
			chatDetails:    &client.Chat{Id: fixtures.ChatID},
			totalViews:     100,
			messageCount:   10,
			memberCount:    500,
		}
		
		// Create a page
		page := &state.Page{
			ID:      "test-page-id",
			URL:     "test-channel",
			Status:  "unfetched",
			Depth:   1,
		}
		
		// Set up expectations for the state manager
		mockStateManager.On("UpdatePage", mock.AnythingOfType("state.Page")).Return(nil)
		mockStateManager.On("UpdateMessage", mock.AnythingOfType("string"), mock.AnythingOfType("int64"), mock.AnythingOfType("int64"), mock.AnythingOfType("string")).Return(nil)
		
		// Note: The mockFetcher is not actually used in processAllMessagesWithProcessor
		// The function processes the messages we pass to it directly
		// So we remove these expectations that would never be called
		
		// Mock getting individual messages
		for _, msg := range messages {
			mockClient.On("GetMessage", &client.GetMessageRequest{
				ChatId:    msg.ChatId,
				MessageId: msg.Id,
			}).Return(msg, nil)
			
			mockClient.On("GetMessageLink", &client.GetMessageLinkRequest{
				ChatId:    msg.ChatId,
				MessageId: msg.Id,
				ForAlbum:  false,
			}).Return(&client.MessageLink{
				Link:     "https://t.me/c/12345/1",
				IsPublic: true,
			}, nil)
		}
		
		// Create a basic config
		cfg := common.CrawlerConfig{
			MaxPages: 108000,
		}
		
		// Create processor that returns outlinks
		processor := &MockMessageProcessor{}
		processor.On("ProcessMessage", 
			mockClient, 
			mock.AnythingOfType("*client.Message"),
			mock.AnythingOfType("int64"), 
			fixtures.ChatID, 
			chatInfo, 
			fixtures.CrawlID, 
			"test-channel", 
			mock.AnythingOfType("*state.StateManagementInterface"),
			mock.AnythingOfType("common.CrawlerConfig")).Return([]string{"newchannel1"}, nil)
		
		// Execute
		discoveredChannels, err := processAllMessagesWithProcessor(
			mockClient,
			chatInfo,
			messages,
			fixtures.CrawlID,
			"test-channel",
			mockStateManager,
			processor,
			page,
			cfg,
		)
		
		// Assert
		assert.NoError(t, err)
		assert.Len(t, discoveredChannels, 1)
		assert.Equal(t, "newchannel1", discoveredChannels[0].URL)
		
		// Verify all mocks were called as expected
		mockStateManager.AssertExpectations(t)
		// We don't check mockFetcher expectations as it's not actually used in processAllMessagesWithProcessor
		processor.AssertExpectations(t)
	})
}
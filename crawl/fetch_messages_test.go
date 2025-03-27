// // Package crawl provides functionality to crawl Telegram channels and extract data.
package crawl

//
//import (
//	"errors"
//	"testing"
//	"time"
//
//	"github.com/stretchr/testify/assert"
//	"github.com/zelenin/go-tdlib/client"
//)
//
//func TestFetchMessages(t *testing.T) {
//	// Create fixtures
//	fixtures := NewTestFixtures(t)
//	defer fixtures.Cleanup()
//
//	t.Run("Success", func(t *testing.T) {
//		// Setup
//		expectedMessages := []*client.Message{
//			{
//				Id:         1,
//				ChatId:     fixtures.ChatID,
//				Content:    &client.MessageText{Text: &client.FormattedText{Text: "Message 1"}},
//				Date:       int32(time.Now().Unix()),
//				IsOutgoing: false,
//			},
//			{
//				Id:         2,
//				ChatId:     fixtures.ChatID,
//				Content:    &client.MessageText{Text: &client.FormattedText{Text: "Message 2"}},
//				Date:       int32(time.Now().Unix()),
//				IsOutgoing: true,
//			},
//		}
//
//		mockClient := new(MockTDLibClient)
//		fromMessageID := int64(5)
//		limit := int32(100)
//
//		// Set expectations
//		mockClient.On("GetChatHistory", &client.GetChatHistoryRequest{
//			ChatId:        fixtures.ChatID,
//			FromMessageId: fromMessageID,
//			Offset:        0,
//			Limit:         limit,
//		}).Return(&client.Messages{
//			TotalCount: int32(len(expectedMessages)),
//			Messages:   expectedMessages,
//		}, nil)
//
//		// Execute
//		messages, err := fetchMessages(mockClient, fixtures.ChatID, fromMessageID)
//
//		// Assert
//		assert.NoError(t, err)
//		assert.Equal(t, expectedMessages, messages)
//		assert.Len(t, messages, 2)
//		mockClient.AssertExpectations(t)
//	})
//
//	t.Run("EmptyResult", func(t *testing.T) {
//		// Setup
//		mockClient := new(MockTDLibClient)
//
//		// Set expectations
//		mockClient.On("GetChatHistory", &client.GetChatHistoryRequest{
//			ChatId:        fixtures.ChatID,
//			FromMessageId: 0,
//			Offset:        0,
//			Limit:         100,
//		}).Return(&client.Messages{
//			TotalCount: 0,
//			Messages:   []*client.Message{},
//		}, nil)
//
//		// Execute
//		messages, err := fetchMessages(mockClient, fixtures.ChatID, 0)
//
//		// Assert
//		assert.NoError(t, err)
//		assert.Empty(t, messages)
//		mockClient.AssertExpectations(t)
//	})
//
//	t.Run("Error", func(t *testing.T) {
//		// Setup
//		mockClient := new(MockTDLibClient)
//		expectedError := errors.New("API error")
//
//		// Set expectations
//		mockClient.On("GetChatHistory", &client.GetChatHistoryRequest{
//			ChatId:        fixtures.ChatID,
//			FromMessageId: 0,
//			Offset:        0,
//			Limit:         100,
//		}).Return(nil, expectedError)
//
//		// Execute
//		messages, err := fetchMessages(mockClient, fixtures.ChatID, 0)
//
//		// Assert
//		assert.Error(t, err)
//		assert.Equal(t, expectedError, err)
//		assert.Nil(t, messages)
//		mockClient.AssertExpectations(t)
//	})
//
//	t.Run("NilMessages", func(t *testing.T) {
//		// Setup
//		mockClient := new(MockTDLibClient)
//
//		// Set expectations
//		mockClient.On("GetChatHistory", &client.GetChatHistoryRequest{
//			ChatId:        fixtures.ChatID,
//			FromMessageId: 0,
//			Offset:        0,
//			Limit:         100,
//		}).Return(&client.Messages{
//			TotalCount: 0,
//			Messages:   nil,
//		}, nil)
//
//		// Execute
//		messages, err := fetchMessages(mockClient, fixtures.ChatID, 0)
//
//		// Assert
//		assert.NoError(t, err)
//		assert.Empty(t, messages)
//		mockClient.AssertExpectations(t)
//	})
//
//	t.Run("VerifyParameters", func(t *testing.T) {
//		// Setup
//		mockClient := new(MockTDLibClient)
//		chatID := int64(123456)
//		fromMessageID := int64(789)
//		limit := int32(100)
//
//		// Set expectations
//		mockClient.On("GetChatHistory", &client.GetChatHistoryRequest{
//			ChatId:        chatID,
//			FromMessageId: fromMessageID,
//			Offset:        0,
//			Limit:         limit,
//		}).Return(&client.Messages{
//			TotalCount: 0,
//			Messages:   []*client.Message{},
//		}, nil)
//
//		// Execute
//		_, err := fetchMessages(mockClient, chatID, fromMessageID)
//
//		// Assert
//		assert.NoError(t, err)
//		mockClient.AssertExpectations(t)
//	})
//
//	t.Run("DifferentMessageTypes", func(t *testing.T) {
//		// Setup
//		expectedMessages := []*client.Message{
//			{
//				Id:     1,
//				ChatId: fixtures.ChatID,
//				Content: &client.MessageText{
//					Text: &client.FormattedText{Text: "Text message"},
//				},
//			},
//			{
//				Id:     2,
//				ChatId: fixtures.ChatID,
//				Content: &client.MessagePhoto{
//					Photo:   &client.Photo{},
//					Caption: &client.FormattedText{Text: "Photo caption"},
//				},
//			},
//			{
//				Id:     3,
//				ChatId: fixtures.ChatID,
//				Content: &client.MessageVideo{
//					Video:   &client.Video{},
//					Caption: &client.FormattedText{Text: "Video caption"},
//				},
//			},
//		}
//
//		mockClient := new(MockTDLibClient)
//
//		// Set expectations
//		mockClient.On("GetChatHistory", &client.GetChatHistoryRequest{
//			ChatId:        fixtures.ChatID,
//			FromMessageId: 0,
//			Offset:        0,
//			Limit:         100,
//		}).Return(&client.Messages{
//			TotalCount: int32(len(expectedMessages)),
//			Messages:   expectedMessages,
//		}, nil)
//
//		// Execute
//		messages, err := fetchMessages(mockClient, fixtures.ChatID, 0)
//
//		// Assert
//		assert.NoError(t, err)
//		assert.Equal(t, expectedMessages, messages)
//		assert.Len(t, messages, 3)
//		mockClient.AssertExpectations(t)
//	})
//}

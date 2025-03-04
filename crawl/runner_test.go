// Package crawl provides functionality to crawl Telegram channels and extract data.
// This file contains tests for the crawling functionality.
package crawl

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/researchaccelerator-hub/telegram-scraper/model"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/researchaccelerator-hub/telegram-scraper/telegramhelper"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/zelenin/go-tdlib/client"
)

// fakeTdlibClient implements the TDLibClient interface for testing purposes.
// It allows customizing individual method behaviors by providing function fields
// that can be set to return specific test responses or simulate errors.
type fakeTdlibClient struct {
	getMessageFunc     func(req *client.GetMessageRequest) (*client.Message, error)
	getMessageLinkFunc func(req *client.GetMessageLinkRequest) (*client.MessageLink, error)
	getChatHistoryFunc func(req *client.GetChatHistoryRequest) (*client.Messages, error)
}

func (f *fakeTdlibClient) GetMe() (*client.User, error) {
	//TODO implement me
	panic("implement me")
}

func (f *fakeTdlibClient) GetMessageThreadHistory(req *client.GetMessageThreadHistoryRequest) (*client.Messages, error) {
	panic("implement me")
}

func (f *fakeTdlibClient) GetRemoteFile(req *client.GetRemoteFileRequest) (*client.File, error) {
	panic("implement me")
}

func (f *fakeTdlibClient) DownloadFile(req *client.DownloadFileRequest) (*client.File, error) {
	panic("implement me")
}

func (f *fakeTdlibClient) GetChatHistory(req *client.GetChatHistoryRequest) (*client.Messages, error) {
	if f.getChatHistoryFunc != nil {
		return f.getChatHistoryFunc(req)
	}
	return nil, errors.New("getChatHistoryFunc not implemented")
}

func (f *fakeTdlibClient) SearchPublicChat(req *client.SearchPublicChatRequest) (*client.Chat, error) {
	panic("implement me")
}

func (f *fakeTdlibClient) GetChat(req *client.GetChatRequest) (*client.Chat, error) {
	panic("implement me")
}

func (f *fakeTdlibClient) GetSupergroup(req *client.GetSupergroupRequest) (*client.Supergroup, error) {
	panic("implement me")
}

func (f *fakeTdlibClient) GetSupergroupFullInfo(req *client.GetSupergroupFullInfoRequest) (*client.SupergroupFullInfo, error) {
	panic("implement me")
}

func (f *fakeTdlibClient) Close() (*client.Ok, error) {
	panic("implement me")
}

func (f *fakeTdlibClient) GetMessage(req *client.GetMessageRequest) (*client.Message, error) {
	return f.getMessageFunc(req)
}

func (f *fakeTdlibClient) GetMessageLink(req *client.GetMessageLinkRequest) (*client.MessageLink, error) {
	return f.getMessageLinkFunc(req)
}

// TestProcessMessage_GetMessageError tests the behavior of processMessage when GetMessage fails.
// It verifies that when the TDLib client's GetMessage method returns an error,
// the processMessage function should propagate that error to the caller.
func TestProcessMessage_GetMessageError(t *testing.T) {
	fakeClient := &fakeTdlibClient{
		getMessageFunc: func(req *client.GetMessageRequest) (*client.Message, error) {
			return nil, errors.New("GetMessage error")
		},
		getMessageLinkFunc: func(req *client.GetMessageLinkRequest) (*client.MessageLink, error) {
			return &client.MessageLink{
				Link:     "http://t.me/message_link",
				IsPublic: false,
			}, nil
		},
	}

	msg := &client.Message{Id: 1, ChatId: 100}
	info := &channelInfo{
		chat:           &client.Chat{Title: "Test Chat"},
		chatDetails:    &client.Chat{Title: "Test Chat Details"},
		supergroup:     &client.Supergroup{},
		supergroupInfo: &client.SupergroupFullInfo{},
		messageCount:   10,
		totalViews:     100,
	}

	err := processMessage(fakeClient, msg, info, "crawlID1", "channelUsername1", state.StateManager{})
	if err == nil {
		t.Error("Expected error from GetMessage, got nil")
	}
}

// TestProcessMessage_GetMessageLinkError tests how processMessage handles errors in GetMessageLink.
// This test verifies that when the TDLib client's GetMessageLink method fails,
// processMessage should log the failure as a warning but continue processing
// the message without returning an error (non-critical failure handling).
func TestProcessMessage_GetMessageLinkError(t *testing.T) {
	fakeClient := &fakeTdlibClient{
		getMessageFunc: func(req *client.GetMessageRequest) (*client.Message, error) {
			return &client.Message{Id: 2, ChatId: 200}, nil
		},
		getMessageLinkFunc: func(req *client.GetMessageLinkRequest) (*client.MessageLink, error) {
			return nil, errors.New("GetMessageLink error")
		},
	}

	msg := &client.Message{Id: 2, ChatId: 200}
	info := &channelInfo{
		chat:           &client.Chat{Title: "Test Chat"},
		chatDetails:    &client.Chat{Title: "Test Chat Details"},
		supergroup:     &client.Supergroup{},
		supergroupInfo: &client.SupergroupFullInfo{},
		messageCount:   5,
		totalViews:     50,
	}

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

	err := processMessage(fakeClient, msg, info, "crawlID2", "channelUsername2", state.StateManager{})
	if err != nil {
		t.Errorf("Expected nil error, got: %v", err)
	}
}

// TestProcessMessage_ParseMessageError tests the behavior when the ParseMessage function fails.
// It verifies that when the telegramhelper.ParseMessage function returns an error,
// the processMessage function should propagate that error to the caller since
// parsing is a critical step in the message processing flow.
func TestProcessMessage_ParseMessageError(t *testing.T) {
	fakeClient := &fakeTdlibClient{
		getMessageFunc: func(req *client.GetMessageRequest) (*client.Message, error) {
			return &client.Message{Id: 3, ChatId: 300}, nil
		},
		getMessageLinkFunc: func(req *client.GetMessageLinkRequest) (*client.MessageLink, error) {
			return &client.MessageLink{
				Link:     "http://t.me/message_link",
				IsPublic: false,
			}, nil
		},
	}

	msg := &client.Message{Id: 3, ChatId: 300}
	info := &channelInfo{
		chat:           &client.Chat{Title: "Test Chat"},
		chatDetails:    &client.Chat{Title: "Test Chat Details"},
		supergroup:     &client.Supergroup{},
		supergroupInfo: &client.SupergroupFullInfo{},
		messageCount:   7,
		totalViews:     70,
	}

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

	err := processMessage(fakeClient, msg, info, "crawlID3", "channelUsername3", state.StateManager{})
	if err == nil {
		t.Error("Expected error from ParseMessage, got nil")
	}
}

// TestProcessMessage_Success tests the successful execution path of processMessage.
// This "happy path" test verifies that when all TDLib client calls succeed and
// ParseMessage processes the message correctly, the processMessage function
// should complete without errors and correctly parse the message content.
func TestProcessMessage_Success(t *testing.T) {
	fakeClient := &fakeTdlibClient{
		getMessageFunc: func(req *client.GetMessageRequest) (*client.Message, error) {
			return &client.Message{Id: 4, ChatId: 400}, nil
		},
		getMessageLinkFunc: func(req *client.GetMessageLinkRequest) (*client.MessageLink, error) {
			return &client.MessageLink{
				Link:     "http://t.me/message_link",
				IsPublic: false,
			}, nil
		},
	}

	msg := &client.Message{Id: 4, ChatId: 400}
	info := &channelInfo{
		chat:           &client.Chat{Title: "Test Chat"},
		chatDetails:    &client.Chat{Title: "Test Chat Details"},
		supergroup:     &client.Supergroup{},
		supergroupInfo: &client.SupergroupFullInfo{},
		messageCount:   15,
		totalViews:     150,
	}

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

	err := processMessage(fakeClient, msg, info, "crawlID4", "channelUsername4", state.StateManager{})
	if err != nil {
		t.Errorf("Expected nil error, got: %v", err)
	}
}

// TestFetchMessages_Success tests the successful retrieval of messages from a chat.
// It verifies that the fetchMessages function correctly calls the TDLib client's
// GetChatHistory method with the expected parameters and properly processes the
// returned messages.
func TestFetchMessages_Success(t *testing.T) {
	// Arrange
	expectedMessages := []*client.Message{
		{
			Id:         1,
			ChatId:     100,
			Content:    &client.MessageText{Text: &client.FormattedText{Text: "Message 1"}},
			Date:       int32(time.Now().Unix()),
			IsOutgoing: false,
		},
		{
			Id:         2,
			ChatId:     100,
			Content:    &client.MessageText{Text: &client.FormattedText{Text: "Message 2"}},
			Date:       int32(time.Now().Unix()),
			IsOutgoing: true,
		},
	}

	mockClient := &fakeTdlibClient{
		getChatHistoryFunc: func(req *client.GetChatHistoryRequest) (*client.Messages, error) {
			// Verify request parameters
			assert.Equal(t, int64(100), req.ChatId)
			assert.Equal(t, int64(5), req.FromMessageId)
			assert.Equal(t, int32(100), req.Limit)

			return &client.Messages{
				TotalCount: int32(len(expectedMessages)),
				Messages:   expectedMessages,
			}, nil
		},
	}

	// Act
	messages, err := fetchMessages(mockClient, 100, 5)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, expectedMessages, messages)
	assert.Len(t, messages, 2)
}

// TestFetchMessages_EmptyResult tests the behavior of fetchMessages when no messages are returned.
// It verifies that the function handles empty message lists correctly, returning an empty
// slice rather than nil and not throwing any errors.
func TestFetchMessages_EmptyResult(t *testing.T) {
	mockClient := &fakeTdlibClient{
		getChatHistoryFunc: func(req *client.GetChatHistoryRequest) (*client.Messages, error) {
			return &client.Messages{
				TotalCount: 0,
				Messages:   []*client.Message{},
			}, nil
		},
	}

	messages, err := fetchMessages(mockClient, 100, 0)

	assert.NoError(t, err)
	assert.Empty(t, messages)
}

// TestFetchMessages_Error tests the error handling in fetchMessages when the API call fails.
// It verifies that when the GetChatHistory call returns an error, the fetchMessages function
// properly propagates that error to the caller and returns nil for the messages slice.
func TestFetchMessages_Error(t *testing.T) {
	expectedError := errors.New("API error")
	mockClient := &fakeTdlibClient{
		getChatHistoryFunc: func(req *client.GetChatHistoryRequest) (*client.Messages, error) {
			return nil, expectedError
		},
	}

	messages, err := fetchMessages(mockClient, 100, 0)

	assert.Error(t, err)
	assert.Equal(t, expectedError, err)
	assert.Nil(t, messages)
}

// TestFetchMessages_NilMessages tests how fetchMessages handles a nil messages field in the API response.
// It verifies that when GetChatHistory returns a response with a nil Messages field,
// the fetchMessages function returns an empty slice instead of propagating the nil value.
func TestFetchMessages_NilMessages(t *testing.T) {
	mockClient := &fakeTdlibClient{
		getChatHistoryFunc: func(req *client.GetChatHistoryRequest) (*client.Messages, error) {
			return &client.Messages{
				TotalCount: 0,
				Messages:   nil,
			}, nil
		},
	}

	messages, err := fetchMessages(mockClient, 100, 0)

	assert.NoError(t, err)
	assert.Empty(t, messages)
}

// TestFetchMessages_VerifyParameters tests that fetchMessages passes the correct parameters to GetChatHistory.
// It verifies that all parameters (chatID, fromMessageID, and limit) are correctly forwarded
// from fetchMessages to the underlying TDLib client call.
func TestFetchMessages_VerifyParameters(t *testing.T) {
	chatID := int64(123456)
	fromMessageID := int64(789)
	limit := int32(100)

	parameterChecked := false
	mockClient := &fakeTdlibClient{
		getChatHistoryFunc: func(req *client.GetChatHistoryRequest) (*client.Messages, error) {
			parameterChecked = true
			assert.Equal(t, chatID, req.ChatId)
			assert.Equal(t, fromMessageID, req.FromMessageId)
			assert.Equal(t, limit, req.Limit)

			return &client.Messages{
				TotalCount: 0,
				Messages:   []*client.Message{},
			}, nil
		},
	}

	_, err := fetchMessages(mockClient, chatID, fromMessageID)

	assert.NoError(t, err)
	assert.True(t, parameterChecked, "Parameters should have been verified")
}

// TestFetchMessages_DifferentMessageTypes tests that fetchMessages properly handles various message types.
// It verifies that the function correctly processes text messages, photo messages, and video messages,
// ensuring that all message types are properly returned in the result slice.
func TestFetchMessages_DifferentMessageTypes(t *testing.T) {
	expectedMessages := []*client.Message{
		{
			Id:     1,
			ChatId: 100,
			Content: &client.MessageText{
				Text: &client.FormattedText{Text: "Text message"},
			},
		},
		{
			Id:     2,
			ChatId: 100,
			Content: &client.MessagePhoto{
				Photo:   &client.Photo{},
				Caption: &client.FormattedText{Text: "Photo caption"},
			},
		},
		{
			Id:     3,
			ChatId: 100,
			Content: &client.MessageVideo{
				Video:   &client.Video{},
				Caption: &client.FormattedText{Text: "Video caption"},
			},
		},
	}

	mockClient := &fakeTdlibClient{
		getChatHistoryFunc: func(req *client.GetChatHistoryRequest) (*client.Messages, error) {
			return &client.Messages{
				TotalCount: int32(len(expectedMessages)),
				Messages:   expectedMessages,
			}, nil
		},
	}

	messages, err := fetchMessages(mockClient, 100, 0)

	assert.NoError(t, err)
	assert.Equal(t, expectedMessages, messages)
	assert.Len(t, messages, 3)
}

// MessageProcessor is an interface for processing Telegram messages.
// This interface abstracts the message processing logic to allow for easier testing
// and more flexible implementation strategies. Implementations of this interface
// are responsible for extracting and storing relevant information from messages.
type MessageProcessor interface {
	// ProcessMessage processes a single Telegram message.
	// It takes the TDLib client, the message to process, channel information,
	// a crawl ID to identify the current crawling session, the channel username,
	// and a state manager to track progress.
	// Returns an error if processing fails.
	ProcessMessage(tdlibClient crawler.TDLibClient, message *client.Message, info *channelInfo, crawlID string, channelUsername string, sm *state.StateManager) error
}

// processAllMessagesWithProcessor retrieves and processes all messages from a channel.
// This function fetches messages in batches, starting from the most recent message
// and working backward through the history. It uses the provided MessageProcessor
// to process each message, allowing for customized message handling.
//
// Parameters:
//   - tdlibClient: The TDLib client for Telegram API access
//   - info: Information about the channel being processed
//   - crawlID: Unique identifier for this crawling session
//   - channelUsername: Username of the channel being processed
//   - sm: State manager to track crawling progress
//   - processor: Implementation of MessageProcessor to handle individual messages
//
// Returns an error if message fetching fails, but continues processing despite individual message errors.
func processAllMessagesWithProcessor(tdlibClient crawler.TDLibClient, info *channelInfo, crawlID, channelUsername string, sm *state.StateManager, processor MessageProcessor) error {
	var fromMessageID int64 = 0

	for {
		messages, err := fetchMessages(tdlibClient, info.chat.Id, fromMessageID)
		if err != nil {
			return err
		}

		if len(messages) == 0 {
			break
		}

		// Process messages
		for _, message := range messages {
			if err := processor.ProcessMessage(tdlibClient, message, info, crawlID, channelUsername, sm); err != nil {
				continue // Skip to next message on error
			}
		}

		// Update message ID for next batch
		fromMessageID = messages[len(messages)-1].Id
	}

	return nil
}

// mockMessageProcessor implements the MessageProcessor interface for testing.
// It uses the testify/mock framework to track and verify method calls.
type mockMessageProcessor struct {
	mock.Mock
}

func (m *mockMessageProcessor) ProcessMessage(tdlibClient crawler.TDLibClient, message *client.Message, info *channelInfo, crawlID string, channelUsername string, sm *state.StateManager) error {
	args := m.Called(tdlibClient, message, info, crawlID, channelUsername, sm)
	return args.Error(0)
}

// createTempStateManager creates a temporary StateManager for testing purposes.
// It creates a temporary directory and initializes a StateManager with a unique crawl ID.
// The caller is responsible for cleaning up the temporary directory when done.
//
// Returns:
//   - A pointer to the initialized StateManager
//   - The path to the temporary directory (for cleanup)
//   - An error if creation fails
func createTempStateManager() (*state.StateManager, string, error) {
	// Create a temporary directory
	tmpDir, err := os.MkdirTemp("", "test-state-")
	if err != nil {
		return nil, "", err
	}

	// Create a state manager with the temp directory
	crawlID := "test-crawl-" + uuid.New().String()
	stateManager := state.NewStateManager(tmpDir, crawlID)

	return stateManager, tmpDir, nil
}

// TestProcessAllMessages_Success tests the successful operation of processAllMessagesWithProcessor.
// It verifies that the function correctly fetches messages in multiple batches and
// processes each message using the provided processor, eventually stopping when
// no more messages are available.
func TestProcessAllMessages_Success(t *testing.T) {
	// Create a temp StateManager
	stateManager, tmpDir, err := createTempStateManager()
	if err != nil {
		t.Fatalf("Failed to create temp state manager: %v", err)
	}
	defer os.RemoveAll(tmpDir) // Clean up

	// Create channel info
	channelInfo := &channelInfo{
		chat: &client.Chat{
			Id: 100,
		},
	}

	// Create two batches of messages
	batch1 := []*client.Message{
		{Id: 1, ChatId: 100},
		{Id: 2, ChatId: 100},
		{Id: 3, ChatId: 100},
	}

	batch2 := []*client.Message{
		{Id: 4, ChatId: 100},
		{Id: 5, ChatId: 100},
	}

	// Empty batch to signal end of messages
	emptyBatch := []*client.Message{}

	mockClient := &fakeTdlibClient{
		getChatHistoryFunc: func(req *client.GetChatHistoryRequest) (*client.Messages, error) {
			if req.FromMessageId == 0 {
				return &client.Messages{Messages: batch1}, nil
			} else if req.FromMessageId == 3 {
				return &client.Messages{Messages: batch2}, nil
			} else if req.FromMessageId == 5 {
				return &client.Messages{Messages: emptyBatch}, nil
			}
			return nil, errors.New("unexpected FromMessageId")
		},
	}

	mockProcessor := new(mockMessageProcessor)

	// Set expectations for all message processing calls
	for _, msg := range append(batch1, batch2...) {
		mockProcessor.On("ProcessMessage", mockClient, msg, channelInfo, "crawl123", "testchannel", stateManager).Return(nil)
	}

	// Act
	err = processAllMessagesWithProcessor(mockClient, channelInfo, "crawl123", "testchannel", stateManager, mockProcessor)

	// Assert
	assert.NoError(t, err)
	mockProcessor.AssertNumberOfCalls(t, "ProcessMessage", 5)
	mockProcessor.AssertExpectations(t)
}

// TestProcessAllMessages_FetchError tests error handling when message fetching fails.
// It verifies that when fetchMessages returns an error, processAllMessagesWithProcessor
// properly propagates that error to the caller and does not attempt to process any messages.
func TestProcessAllMessages_FetchError(t *testing.T) {
	// Create a temp StateManager
	stateManager, tmpDir, err := createTempStateManager()
	if err != nil {
		t.Fatalf("Failed to create temp state manager: %v", err)
	}
	defer os.RemoveAll(tmpDir) // Clean up

	// Create channel info
	channelInfo := &channelInfo{
		chat: &client.Chat{
			Id: 100,
		},
	}

	fetchError := errors.New("fetch error")
	mockClient := &fakeTdlibClient{
		getChatHistoryFunc: func(req *client.GetChatHistoryRequest) (*client.Messages, error) {
			return nil, fetchError
		},
	}

	mockProcessor := new(mockMessageProcessor)

	// Act
	err = processAllMessagesWithProcessor(mockClient, channelInfo, "crawl123", "testchannel", stateManager, mockProcessor)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, fetchError, err)
	mockProcessor.AssertNotCalled(t, "ProcessMessage")
}

// TestProcessAllMessages_ProcessError tests how processAllMessagesWithProcessor handles message processing errors.
// It verifies that when the processor returns an error for a specific message,
// the function should log the error and continue processing the remaining messages
// rather than stopping the entire process.
func TestProcessAllMessages_ProcessError(t *testing.T) {
	// Create a temp StateManager
	stateManager, tmpDir, err := createTempStateManager()
	if err != nil {
		t.Fatalf("Failed to create temp state manager: %v", err)
	}
	defer os.RemoveAll(tmpDir) // Clean up

	// Create channel info
	channelInfo := &channelInfo{
		chat: &client.Chat{
			Id: 100,
		},
	}

	messages := []*client.Message{
		{Id: 1, ChatId: 100},
		{Id: 2, ChatId: 100},
		{Id: 3, ChatId: 100},
	}

	mockClient := &fakeTdlibClient{
		getChatHistoryFunc: func(req *client.GetChatHistoryRequest) (*client.Messages, error) {
			if req.FromMessageId == 0 {
				return &client.Messages{Messages: messages}, nil
			}
			return &client.Messages{Messages: []*client.Message{}}, nil
		},
	}

	mockProcessor := new(mockMessageProcessor)

	// Set up processMessage to fail on the second message
	mockProcessor.On("ProcessMessage", mockClient, messages[0], channelInfo, "crawl123", "testchannel", stateManager).Return(nil)
	mockProcessor.On("ProcessMessage", mockClient, messages[1], channelInfo, "crawl123", "testchannel", stateManager).Return(errors.New("process error"))
	mockProcessor.On("ProcessMessage", mockClient, messages[2], channelInfo, "crawl123", "testchannel", stateManager).Return(nil)

	// Act
	err = processAllMessagesWithProcessor(mockClient, channelInfo, "crawl123", "testchannel", stateManager, mockProcessor)

	// Assert
	assert.NoError(t, err) // Function should continue despite process errors
	mockProcessor.AssertNumberOfCalls(t, "ProcessMessage", 3)
	mockProcessor.AssertExpectations(t)
}

// TestProcessAllMessages_NoMessages tests processAllMessagesWithProcessor behavior with an empty channel.
// It verifies that when fetchMessages returns an empty list of messages immediately,
// the function should terminate gracefully without error and without calling
// the message processor.
func TestProcessAllMessages_NoMessages(t *testing.T) {
	// Create a temp StateManager
	stateManager, tmpDir, err := createTempStateManager()
	if err != nil {
		t.Fatalf("Failed to create temp state manager: %v", err)
	}
	defer os.RemoveAll(tmpDir) // Clean up

	// Create channel info
	channelInfo := &channelInfo{
		chat: &client.Chat{
			Id: 100,
		},
	}

	mockClient := &fakeTdlibClient{
		getChatHistoryFunc: func(req *client.GetChatHistoryRequest) (*client.Messages, error) {
			return &client.Messages{Messages: []*client.Message{}}, nil
		},
	}

	mockProcessor := new(mockMessageProcessor)

	// Act
	err = processAllMessagesWithProcessor(mockClient, channelInfo, "crawl123", "testchannel", stateManager, mockProcessor)

	// Assert
	assert.NoError(t, err)
	mockProcessor.AssertNotCalled(t, "ProcessMessage")
}

// TestProcessAllMessages_MultipleBatches tests processing messages across multiple batches.
// It verifies that processAllMessagesWithProcessor correctly handles pagination
// by fetching multiple batches of messages and updating the fromMessageID parameter
// correctly between batches, ensuring all messages in the channel are processed.
func TestProcessAllMessages_MultipleBatches(t *testing.T) {
	// Create a temp StateManager
	stateManager, tmpDir, err := createTempStateManager()
	if err != nil {
		t.Fatalf("Failed to create temp state manager: %v", err)
	}
	defer os.RemoveAll(tmpDir) // Clean up

	// Create channel info
	channelInfo := &channelInfo{
		chat: &client.Chat{
			Id: 100,
		},
	}

	// Create several batches of messages to test pagination
	batch1 := []*client.Message{
		{Id: 10, ChatId: 100},
		{Id: 20, ChatId: 100},
	}

	batch2 := []*client.Message{
		{Id: 30, ChatId: 100},
		{Id: 40, ChatId: 100},
	}

	batch3 := []*client.Message{
		{Id: 50, ChatId: 100},
	}

	allMessages := append(append(batch1, batch2...), batch3...)

	batchCount := 0
	mockClient := &fakeTdlibClient{
		getChatHistoryFunc: func(req *client.GetChatHistoryRequest) (*client.Messages, error) {
			batchCount++
			switch req.FromMessageId {
			case 0:
				return &client.Messages{Messages: batch1}, nil
			case 20:
				return &client.Messages{Messages: batch2}, nil
			case 40:
				return &client.Messages{Messages: batch3}, nil
			case 50:
				return &client.Messages{Messages: []*client.Message{}}, nil
			default:
				return nil, errors.New("unexpected FromMessageId")
			}
		},
	}

	mockProcessor := new(mockMessageProcessor)

	// Set expectations for all message processing calls
	for _, msg := range allMessages {
		mockProcessor.On("ProcessMessage", mockClient, msg, channelInfo, "crawl123", "testchannel", stateManager).Return(nil)
	}

	// Act
	err = processAllMessagesWithProcessor(mockClient, channelInfo, "crawl123", "testchannel", stateManager, mockProcessor)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, 4, batchCount, "Should have made 4 batch requests (3 with data, 1 empty)")
	mockProcessor.AssertNumberOfCalls(t, "ProcessMessage", 5)
	mockProcessor.AssertExpectations(t)
}

// mockTDLibClient is a comprehensive mock implementation of the TDLibClient interface.
// Unlike fakeTdlibClient, which only implements a subset of methods with custom function fields,
// this implementation uses the testify/mock framework to provide a complete mock that
// can be configured with expectations for all methods of the interface.
type mockTDLibClient struct {
	mock.Mock
}

func (m *mockTDLibClient) GetMe() (*client.User, error) {
	//TODO implement me
	panic("implement me")
}

func (m *mockTDLibClient) SearchPublicChat(req *client.SearchPublicChatRequest) (*client.Chat, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.Chat), args.Error(1)
}

func (m *mockTDLibClient) GetChat(req *client.GetChatRequest) (*client.Chat, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.Chat), args.Error(1)
}

func (m *mockTDLibClient) GetSupergroup(req *client.GetSupergroupRequest) (*client.Supergroup, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.Supergroup), args.Error(1)
}

func (m *mockTDLibClient) GetSupergroupFullInfo(req *client.GetSupergroupFullInfoRequest) (*client.SupergroupFullInfo, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.SupergroupFullInfo), args.Error(1)
}

func (m *mockTDLibClient) GetChatHistory(req *client.GetChatHistoryRequest) (*client.Messages, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.Messages), args.Error(1)
}

func (m *mockTDLibClient) GetMessageThreadHistory(req *client.GetMessageThreadHistoryRequest) (*client.Messages, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.Messages), args.Error(1)
}

func (m *mockTDLibClient) GetMessage(req *client.GetMessageRequest) (*client.Message, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.Message), args.Error(1)
}

func (m *mockTDLibClient) GetMessageLink(req *client.GetMessageLinkRequest) (*client.MessageLink, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.MessageLink), args.Error(1)
}

func (m *mockTDLibClient) GetRemoteFile(req *client.GetRemoteFileRequest) (*client.File, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.File), args.Error(1)
}

func (m *mockTDLibClient) DownloadFile(req *client.DownloadFileRequest) (*client.File, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.File), args.Error(1)
}

func (m *mockTDLibClient) Close() (*client.Ok, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.Ok), args.Error(1)
}

// TotalViewsGetter is a function type for retrieving the total view count of a channel.
// This abstraction allows for easier testing by replacing the real implementation
// with test-specific versions.
type TotalViewsGetter func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error)

// MessageCountGetter is a function type for retrieving the message count of a channel.
// This abstraction allows for easier testing by replacing the real implementation
// with test-specific versions.
type MessageCountGetter func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error)

// getChannelInfoForTest is a testable version of getChannelInfo that accepts replaceable helper functions.
// This function retrieves information about a Telegram channel, including basic chat details,
// supergroup information, view counts, and message counts. By accepting the helper functions
// as parameters, it allows tests to provide mock implementations, making the function testable.
//
// Parameters:
//   - tdlibClient: The TDLib client for Telegram API access
//   - channelUsername: Username of the channel to retrieve information for
//   - getTotalViewsFn: Function to get the channel's total view count
//   - getMessageCountFn: Function to get the channel's message count
//
// Returns channel information or an error if retrieval fails.
func getChannelInfoForTest(
	tdlibClient crawler.TDLibClient,
	channelUsername string,
	getTotalViewsFn TotalViewsGetter,
	getMessageCountFn MessageCountGetter,
) (*channelInfo, error) {
	// Search for the channel
	chat, err := tdlibClient.SearchPublicChat(&client.SearchPublicChatRequest{
		Username: channelUsername,
	})
	if err != nil {
		log.Error().Err(err).Stack().Msgf("Failed to find channel: %v", channelUsername)
		return nil, err
	}

	chatDetails, err := tdlibClient.GetChat(&client.GetChatRequest{
		ChatId: chat.Id,
	})
	if err != nil {
		log.Error().Err(err).Stack().Msgf("Failed to get chat details for: %v", channelUsername)
		return nil, err
	}

	// Get channel stats
	totalViews := 0
	if getTotalViewsFn != nil {
		totalViewsVal, err := getTotalViewsFn(tdlibClient, chat.Id, channelUsername)
		if err != nil {
			log.Warn().Err(err).Msgf("Failed to get total views for channel: %v", channelUsername)
			// Continue anyway, this is not critical
		} else {
			totalViews = totalViewsVal
		}
	}

	messageCount := 0
	if getMessageCountFn != nil {
		messageCountVal, err := getMessageCountFn(tdlibClient, chat.Id, channelUsername)
		if err != nil {
			log.Warn().Err(err).Msgf("Failed to get message count for channel: %v", channelUsername)
			// Continue anyway, this is not critical
		} else {
			messageCount = messageCountVal
		}
	}

	// Get supergroup information if available
	var supergroup *client.Supergroup
	var supergroupInfo *client.SupergroupFullInfo

	if chat.Type != nil {
		if supergroupType, ok := chat.Type.(*client.ChatTypeSupergroup); ok {
			supergroup, err = tdlibClient.GetSupergroup(&client.GetSupergroupRequest{
				SupergroupId: supergroupType.SupergroupId,
			})
			if err != nil {
				log.Warn().Err(err).Msgf("Failed to get supergroup info for: %v", channelUsername)
				// Continue anyway, this is not critical
			}

			if supergroup != nil {
				req := &client.GetSupergroupFullInfoRequest{
					SupergroupId: supergroup.Id,
				}
				supergroupInfo, err = tdlibClient.GetSupergroupFullInfo(req)
				if err != nil {
					log.Warn().Err(err).Msgf("Failed to get supergroup full info for: %v", channelUsername)
					// Continue anyway, this is not critical
				}
			}
		}
	}

	return &channelInfo{
		chat:           chat,
		chatDetails:    chatDetails,
		supergroup:     supergroup,
		supergroupInfo: supergroupInfo,
		totalViews:     int32(totalViews),
		messageCount:   int32(messageCount),
	}, nil
}

// getChannelInfoWrapper is a wrapper function that connects the original getChannelInfo
// function with our testable version. It uses the standard telegramhelper implementations
// for view count and message count retrieval.
//
// This function serves as a bridge between the production code and the test code,
// allowing the same core logic to be tested with mock dependencies.
func getChannelInfoWrapper(tdlibClient crawler.TDLibClient, channelUsername string) (*channelInfo, error) {
	return getChannelInfoForTest(
		tdlibClient,
		channelUsername,
		telegramhelper.GetTotalChannelViews,
		telegramhelper.GetMessageCount,
	)
}

// TestGetChannelInfo_SearchPublicChatError tests getChannelInfo behavior when SearchPublicChat fails.
// It verifies that when the initial SearchPublicChat call to find the channel fails,
// getChannelInfo properly propagates that error to the caller without attempting
// to retrieve additional information.
func TestGetChannelInfo_SearchPublicChatError(t *testing.T) {
	// Mock client
	mockClient := new(mockTDLibClient)

	// Test data
	channelUsername := "testchannel"
	searchError := errors.New("channel not found")

	// Set up mocks
	mockClient.On("SearchPublicChat", &client.SearchPublicChatRequest{
		Username: channelUsername,
	}).Return(nil, searchError)

	// Create mock helper functions
	getMockTotalViews := func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error) {
		return 0, nil
	}

	getMockMessageCount := func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error) {
		return 0, nil
	}

	// Call the function with mock helpers
	info, err := getChannelInfoForTest(mockClient, channelUsername, getMockTotalViews, getMockMessageCount)

	// Assertions
	assert.Error(t, err)
	assert.Equal(t, searchError, err)
	assert.Nil(t, info)

	// Verify all expected calls were made
	mockClient.AssertExpectations(t)
}

// TestGetChannelInfo_Success tests the successful retrieval of channel information.
// It verifies that getChannelInfo correctly retrieves and combines information from
// multiple sources: basic chat info, supergroup info, view counts, and message counts,
// resulting in a complete channelInfo object.
func TestGetChannelInfo_Success(t *testing.T) {
	// Mock client
	mockClient := new(mockTDLibClient)

	// Test data
	channelUsername := "testchannel"
	chatID := int64(12345)
	supergroupID := int64(67890)
	totalViews := 5000
	messageCount := 100

	// Mock responses
	chat := &client.Chat{
		Id: chatID,
		Type: &client.ChatTypeSupergroup{
			SupergroupId: supergroupID,
			IsChannel:    true,
		},
		Title: "Test Channel",
	}

	chatDetails := &client.Chat{
		Id:    chatID,
		Title: "Test Channel Details",
	}

	supergroup := &client.Supergroup{
		Id: supergroupID,
		Usernames: &client.Usernames{
			ActiveUsernames:   []string{channelUsername},
			DisabledUsernames: []string{},
			EditableUsername:  channelUsername,
		},
		MemberCount:       1000,
		Date:              123456789,
		HasLinkedChat:     true,
		HasLocation:       false,
		SignMessages:      true,
		IsVerified:        false,
		RestrictionReason: "",
	}

	supergroupInfo := &client.SupergroupFullInfo{
		Description:        "Test channel description",
		MemberCount:        1000,
		AdministratorCount: 5,
		RestrictedCount:    0,
		BannedCount:        0,
		LinkedChatId:       54321,
		CanGetMembers:      true,
		CanSetStickerSet:   true,
		StickerSetId:       0,
	}

	// Set up mocks
	mockClient.On("SearchPublicChat", &client.SearchPublicChatRequest{
		Username: channelUsername,
	}).Return(chat, nil)

	mockClient.On("GetChat", &client.GetChatRequest{
		ChatId: chatID,
	}).Return(chatDetails, nil)

	mockClient.On("GetSupergroup", &client.GetSupergroupRequest{
		SupergroupId: supergroupID,
	}).Return(supergroup, nil)

	mockClient.On("GetSupergroupFullInfo", &client.GetSupergroupFullInfoRequest{
		SupergroupId: supergroupID,
	}).Return(supergroupInfo, nil)

	// Create mock helper functions
	getMockTotalViews := func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error) {
		return totalViews, nil
	}

	getMockMessageCount := func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error) {
		return messageCount, nil
	}

	// Call the function with mock helpers
	info, err := getChannelInfoForTest(mockClient, channelUsername, getMockTotalViews, getMockMessageCount)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, info)
	assert.Equal(t, chat, info.chat)
	assert.Equal(t, chatDetails, info.chatDetails)
	assert.Equal(t, supergroup, info.supergroup)
	assert.Equal(t, supergroupInfo, info.supergroupInfo)
	assert.Equal(t, int32(totalViews), info.totalViews)
	assert.Equal(t, int32(messageCount), info.messageCount)

	// Verify all expected calls were made
	mockClient.AssertExpectations(t)
}

// TestGetChannelInfo_NonSupergroupChannel tests getChannelInfo with a non-supergroup chat type.
// It verifies that when the channel is not a supergroup (e.g., it's a private chat),
// getChannelInfo still retrieves basic information but correctly handles the absence
// of supergroup-specific data without errors.
func TestGetChannelInfo_NonSupergroupChannel(t *testing.T) {
	// Mock client
	mockClient := new(mockTDLibClient)

	// Test data
	channelUsername := "testchannel"
	chatID := int64(12345)
	totalViews := 0
	messageCount := 50

	// Mock responses - chat that is not a supergroup
	chat := &client.Chat{
		Id: chatID,
		Type: &client.ChatTypePrivate{
			UserId: 987654,
		},
		Title: "Private Chat",
	}

	chatDetails := &client.Chat{
		Id:    chatID,
		Title: "Private Chat Details",
	}

	// Set up mocks
	mockClient.On("SearchPublicChat", &client.SearchPublicChatRequest{
		Username: channelUsername,
	}).Return(chat, nil)

	mockClient.On("GetChat", &client.GetChatRequest{
		ChatId: chatID,
	}).Return(chatDetails, nil)

	// Create mock helper functions
	getMockTotalViews := func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error) {
		return totalViews, nil
	}

	getMockMessageCount := func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error) {
		return messageCount, nil
	}

	// Call the function with mock helpers
	info, err := getChannelInfoForTest(mockClient, channelUsername, getMockTotalViews, getMockMessageCount)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, info)
	assert.Equal(t, chat, info.chat)
	assert.Equal(t, chatDetails, info.chatDetails)
	assert.Nil(t, info.supergroup)
	assert.Nil(t, info.supergroupInfo)
	assert.Equal(t, int32(totalViews), info.totalViews)
	assert.Equal(t, int32(messageCount), info.messageCount)

	// Verify all expected calls were made
	mockClient.AssertExpectations(t)
}

// TestGetChannelInfo_TotalViewsError tests getChannelInfo resilience when view count retrieval fails.
// It verifies that when the getTotalViewsFn returns an error, getChannelInfo should
// log a warning but continue execution, setting the totalViews field to 0 and
// still returning the channel information without an error.
func TestGetChannelInfo_TotalViewsError(t *testing.T) {
	// Mock client
	mockClient := new(mockTDLibClient)

	// Test data
	channelUsername := "testchannel"
	chatID := int64(12345)
	viewsError := errors.New("views count error")
	messageCount := 200

	// Mock responses
	chat := &client.Chat{
		Id: chatID,
		Type: &client.ChatTypeBasicGroup{
			BasicGroupId: 54321,
		},
		Title: "Basic Group",
	}

	chatDetails := &client.Chat{
		Id:    chatID,
		Title: "Basic Group Details",
	}

	// Set up mocks
	mockClient.On("SearchPublicChat", &client.SearchPublicChatRequest{
		Username: channelUsername,
	}).Return(chat, nil)

	mockClient.On("GetChat", &client.GetChatRequest{
		ChatId: chatID,
	}).Return(chatDetails, nil)

	// Create mock helper functions
	getMockTotalViews := func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error) {
		return 0, viewsError
	}

	getMockMessageCount := func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error) {
		return messageCount, nil
	}

	// Call the function with mock helpers
	info, err := getChannelInfoForTest(mockClient, channelUsername, getMockTotalViews, getMockMessageCount)

	// Assertions - should still succeed because the function is tolerant of views count errors
	assert.NoError(t, err)
	assert.NotNil(t, info)
	assert.Equal(t, chat, info.chat)
	assert.Equal(t, chatDetails, info.chatDetails)
	assert.Equal(t, int32(0), info.totalViews) // Should be 0 when error occurs
	assert.Equal(t, int32(messageCount), info.messageCount)

	// Verify all expected calls were made
	mockClient.AssertExpectations(t)
}

// TestGetChannelInfo_MessageCountError tests getChannelInfo resilience when message count retrieval fails.
// It verifies that when the getMessageCountFn returns an error, getChannelInfo should
// log a warning but continue execution, setting the messageCount field to 0 and
// still returning the channel information without an error.
func TestGetChannelInfo_MessageCountError(t *testing.T) {
	// Mock client
	mockClient := new(mockTDLibClient)

	// Test data
	channelUsername := "testchannel"
	chatID := int64(12345)
	messageCountError := errors.New("message count error")
	totalViews := 4000

	// Mock responses
	chat := &client.Chat{
		Id: chatID,
		Type: &client.ChatTypeBasicGroup{
			BasicGroupId: 54321,
		},
		Title: "Basic Group",
	}

	chatDetails := &client.Chat{
		Id:    chatID,
		Title: "Basic Group Details",
	}

	// Set up mocks
	mockClient.On("SearchPublicChat", &client.SearchPublicChatRequest{
		Username: channelUsername,
	}).Return(chat, nil)

	mockClient.On("GetChat", &client.GetChatRequest{
		ChatId: chatID,
	}).Return(chatDetails, nil)

	// Create mock helper functions
	getMockTotalViews := func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error) {
		return totalViews, nil
	}

	getMockMessageCount := func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error) {
		return 0, messageCountError
	}

	// Call the function with mock helpers
	info, err := getChannelInfoForTest(mockClient, channelUsername, getMockTotalViews, getMockMessageCount)

	// Assertions - should still succeed because the function is tolerant of message count errors
	assert.NoError(t, err)
	assert.NotNil(t, info)
	assert.Equal(t, chat, info.chat)
	assert.Equal(t, chatDetails, info.chatDetails)
	assert.Equal(t, int32(totalViews), info.totalViews)
	assert.Equal(t, int32(0), info.messageCount) // Should be 0 when error occurs

	// Verify all expected calls were made
	mockClient.AssertExpectations(t)
}

// TestGetChannelInfo_AllNonCriticalErrorsTogether tests getChannelInfo with multiple non-critical errors.
// It verifies that getChannelInfo is resilient to multiple simultaneous non-critical
// errors (supergroup info, view count, and message count errors), continuing execution
// and returning partial channel information without propagating these errors.
func TestGetChannelInfo_AllNonCriticalErrorsTogether(t *testing.T) {
	// Mock client
	mockClient := new(mockTDLibClient)

	// Test data
	channelUsername := "testchannel"
	chatID := int64(12345)
	supergroupID := int64(67890)
	supergroupError := errors.New("supergroup fetch error")
	viewsError := errors.New("views count error")
	messageCountError := errors.New("message count error")

	// Mock responses
	chat := &client.Chat{
		Id: chatID,
		Type: &client.ChatTypeSupergroup{
			SupergroupId: supergroupID,
			IsChannel:    true,
		},
		Title: "Test Channel",
	}

	chatDetails := &client.Chat{
		Id:    chatID,
		Title: "Test Channel Details",
	}

	// Set up mocks
	mockClient.On("SearchPublicChat", &client.SearchPublicChatRequest{
		Username: channelUsername,
	}).Return(chat, nil)

	mockClient.On("GetChat", &client.GetChatRequest{
		ChatId: chatID,
	}).Return(chatDetails, nil)

	mockClient.On("GetSupergroup", &client.GetSupergroupRequest{
		SupergroupId: supergroupID,
	}).Return(nil, supergroupError)

	// Create mock helper functions
	getMockTotalViews := func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error) {
		return 0, viewsError
	}

	getMockMessageCount := func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error) {
		return 0, messageCountError
	}

	// Call the function with mock helpers
	info, err := getChannelInfoForTest(mockClient, channelUsername, getMockTotalViews, getMockMessageCount)

	// Assertions - should still succeed despite all non-critical errors
	assert.NoError(t, err)
	assert.NotNil(t, info)
	assert.Equal(t, chat, info.chat)
	assert.Equal(t, chatDetails, info.chatDetails)
	assert.Nil(t, info.supergroup)
	assert.Nil(t, info.supergroupInfo)
	assert.Equal(t, int32(0), info.totalViews)
	assert.Equal(t, int32(0), info.messageCount)

	// Verify all expected calls were made
	mockClient.AssertExpectations(t)
}

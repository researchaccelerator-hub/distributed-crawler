// Package crawl provides functionality to crawl Telegram channels and extract data.
package crawl

import (
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/stretchr/testify/mock"
	"github.com/zelenin/go-tdlib/client"
)

// MockTDLibClient is a comprehensive mock implementation of the TDLibClient interface.
type MockTDLibClient struct {
	mock.Mock
}

func (m *MockTDLibClient) GetMe() (*client.User, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.User), args.Error(1)
}

func (m *MockTDLibClient) SearchPublicChat(req *client.SearchPublicChatRequest) (*client.Chat, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.Chat), args.Error(1)
}

func (m *MockTDLibClient) GetChat(req *client.GetChatRequest) (*client.Chat, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.Chat), args.Error(1)
}

func (m *MockTDLibClient) GetSupergroup(req *client.GetSupergroupRequest) (*client.Supergroup, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.Supergroup), args.Error(1)
}

func (m *MockTDLibClient) GetSupergroupFullInfo(req *client.GetSupergroupFullInfoRequest) (*client.SupergroupFullInfo, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.SupergroupFullInfo), args.Error(1)
}

func (m *MockTDLibClient) GetChatHistory(req *client.GetChatHistoryRequest) (*client.Messages, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.Messages), args.Error(1)
}

func (m *MockTDLibClient) GetMessageThreadHistory(req *client.GetMessageThreadHistoryRequest) (*client.Messages, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.Messages), args.Error(1)
}

func (m *MockTDLibClient) GetMessage(req *client.GetMessageRequest) (*client.Message, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.Message), args.Error(1)
}

func (m *MockTDLibClient) GetMessageLink(req *client.GetMessageLinkRequest) (*client.MessageLink, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.MessageLink), args.Error(1)
}

func (m *MockTDLibClient) GetRemoteFile(req *client.GetRemoteFileRequest) (*client.File, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.File), args.Error(1)
}

func (m *MockTDLibClient) DownloadFile(req *client.DownloadFileRequest) (*client.File, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.File), args.Error(1)
}

func (m *MockTDLibClient) Close() (*client.Ok, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.Ok), args.Error(1)
}

// MockMessageProcessor implements the MessageProcessor interface for testing.
type MockMessageProcessor struct {
	mock.Mock
}

func (m *MockMessageProcessor) ProcessMessage(
	tdlibClient crawler.TDLibClient,
	message *client.Message,
	info *channelInfo,
	crawlID string,
	channelUsername string,
	sm *state.StateManager) error {

	args := m.Called(tdlibClient, message, info, crawlID, channelUsername, sm)
	return args.Error(0)
}

// MockMessageFetcher implements the MessageFetcher interface for testing.
type MockMessageFetcher struct {
	mock.Mock
}

func (m *MockMessageFetcher) FetchMessages(tdlibClient crawler.TDLibClient, chatID int64, fromMessageID int64) ([]*client.Message, error) {
	args := m.Called(tdlibClient, chatID, fromMessageID)
	return args.Get(0).([]*client.Message), args.Error(1)
}

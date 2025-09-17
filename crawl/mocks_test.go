// Package crawl provides functionality to crawl Telegram channels and extract data.
package crawl

import (
	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/researchaccelerator-hub/telegram-scraper/model"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/stretchr/testify/mock"
	"github.com/zelenin/go-tdlib/client"
)

// MockTDLibClient is a comprehensive mock implementation of the TDLibClient interface.
type MockTDLibClient struct {
	mock.Mock
}

func (m *MockTDLibClient) GetBasicGroupFullInfo(req *client.GetBasicGroupFullInfoRequest) (*client.BasicGroupFullInfo, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.BasicGroupFullInfo), args.Error(1)
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

// Added methods to complete the TDLibClient interface
func (m *MockTDLibClient) GetMessageThread(req *client.GetMessageThreadRequest) (*client.MessageThreadInfo, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.MessageThreadInfo), args.Error(1)
}

func (m *MockTDLibClient) AddMessageReaction(req *client.AddMessageReactionRequest) (*client.Ok, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.Ok), args.Error(1)
}

func (m *MockTDLibClient) GetMessageViewers(req *client.GetMessageViewersRequest) (*client.MessageViewers, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.MessageViewers), args.Error(1)
}

func (m *MockTDLibClient) GetRepliedMessage(req *client.GetRepliedMessageRequest) (*client.Message, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.Message), args.Error(1)
}

type GetChatMessagesRequest struct {
	ChatId     int64
	MessageIds []int64
}

func (m *MockTDLibClient) GetChatMessages(req *GetChatMessagesRequest) (*client.Messages, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.Messages), args.Error(1)
}

func (m *MockTDLibClient) GetChatMessageByDate(req *client.GetChatMessageByDateRequest) (*client.Message, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.Message), args.Error(1)
}

func (m *MockTDLibClient) SearchChatMessages(req *client.SearchChatMessagesRequest) (*client.Messages, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.Messages), args.Error(1)
}

func (m *MockTDLibClient) GetUser(req *client.GetUserRequest) (*client.User, error) {
	args := m.Called(req)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*client.User), args.Error(1)
}

// DeleteFile implements the DeleteFile method required by TDLibClient interface
func (m *MockTDLibClient) DeleteFile(req *client.DeleteFileRequest) (*client.Ok, error) {
	args := m.Called(req)
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
	messageId int64,
	chatId int64,
	info *channelInfo,
	crawlID string,
	channelUsername string,
	sm *state.StateManagementInterface,
	cfg common.CrawlerConfig) ([]string, error) {

	args := m.Called(tdlibClient, message, messageId, chatId, info, crawlID, channelUsername, sm, cfg)
	return args.Get(0).([]string), args.Error(1)
}

// MockMessageFetcher implements the MessageFetcher interface for testing.
type MockMessageFetcher struct {
	mock.Mock
}

func (m *MockMessageFetcher) FetchMessages(tdlibClient crawler.TDLibClient, chatID int64, fromMessageID int64) ([]*client.Message, error) {
	args := m.Called(tdlibClient, chatID, fromMessageID)
	return args.Get(0).([]*client.Message), args.Error(1)
}

// MockStateManager implements the StateManagementInterface for testing.
type MockStateManager struct {
	mock.Mock
}

// Initialize initializes the state with seed URLs
func (m *MockStateManager) Initialize(seedURLs []string) error {
	args := m.Called(seedURLs)
	return args.Error(0)
}

func (m *MockStateManager) InitializeDiscoveredChannels() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockStateManager) InitializeRandomWalkLayer() error {
	args := m.Called()
	return args.Error(0)
}

// GetPage retrieves a page by ID
func (m *MockStateManager) GetPage(id string) (state.Page, error) {
	args := m.Called(id)
	return args.Get(0).(state.Page), args.Error(1)
}

// UpdatePage updates a page's information
func (m *MockStateManager) UpdatePage(page state.Page) error {
	args := m.Called(page)
	return args.Error(0)
}

// UpdateMessage updates or adds a message to a page
func (m *MockStateManager) UpdateMessage(pageID string, chatID int64, messageID int64, status string) error {
	args := m.Called(pageID, chatID, messageID, status)
	return args.Error(0)
}

// AddLayer adds a new layer of pages
func (m *MockStateManager) AddLayer(pages []state.Page) error {
	args := m.Called(pages)
	return args.Error(0)
}

// GetLayerByDepth retrieves all pages at a specific depth
func (m *MockStateManager) GetLayerByDepth(depth int) ([]state.Page, error) {
	args := m.Called(depth)
	return args.Get(0).([]state.Page), args.Error(1)
}

// GetMaxDepth returns the highest depth value among all layers
func (m *MockStateManager) GetMaxDepth() (int, error) {
	args := m.Called()
	return args.Int(0), args.Error(1)
}

// GetState returns a copy of the current state
func (m *MockStateManager) GetState() state.State {
	args := m.Called()
	return args.Get(0).(state.State)
}

// SetState updates the entire state
func (m *MockStateManager) SetState(state state.State) {
	m.Called(state)
}

// SaveState persists the current state
func (m *MockStateManager) SaveState() error {
	args := m.Called()
	return args.Error(0)
}

// StorePost stores a post in the state
func (m *MockStateManager) StorePost(channelID string, post model.Post) error {
	args := m.Called(channelID, post)
	return args.Error(0)
}

// StoreFile stores a file in the state
func (m *MockStateManager) StoreFile(channelID string, sourceFilePath string, fileName string) (string, string, error) {
	args := m.Called(channelID, sourceFilePath, fileName)
	return args.String(0), args.String(1), args.Error(2)
}

// GetPreviousCrawls returns a list of previous crawl IDs
func (m *MockStateManager) GetPreviousCrawls() ([]string, error) {
	args := m.Called()
	return args.Get(0).([]string), args.Error(1)
}

// UpdateCrawlMetadata updates the crawl metadata
func (m *MockStateManager) UpdateCrawlMetadata(crawlID string, metadata map[string]interface{}) error {
	args := m.Called(crawlID, metadata)
	return args.Error(0)
}

// FindIncompleteCrawl checks if there is an existing incomplete crawl
func (m *MockStateManager) FindIncompleteCrawl(crawlID string) (string, bool, error) {
	args := m.Called(crawlID)
	return args.String(0), args.Bool(1), args.Error(2)
}

// ExportPagesToBinding exports pages to the binding
func (m *MockStateManager) ExportPagesToBinding(crawlID string) error {
	args := m.Called(crawlID)
	return args.Error(0)
}

// HasProcessedMedia checks if media has been processed
func (m *MockStateManager) HasProcessedMedia(mediaID string) (bool, error) {
	args := m.Called(mediaID)
	return args.Bool(0), args.Error(1)
}

// MarkMediaAsProcessed marks media as processed
func (m *MockStateManager) MarkMediaAsProcessed(mediaID string) error {
	args := m.Called(mediaID)
	return args.Error(0)
}

func (m *MockStateManager) GetRandomDiscoveredChannel() (string, error) {
	args := m.Called()
	return args.String(0), args.Error(1)
}

func (m *MockStateManager) IsDiscoveredChannel(channelID string) bool {
	args := m.Called(channelID)
	return args.Bool(0)
}

func (m *MockStateManager) AddDiscoveredChannel(channelID string) error {
	args := m.Called(channelID)
	return args.Error(0)
}

func (m *MockStateManager) SaveEdgeRecords(edges []*state.EdgeRecord) error {
	args := m.Called(edges)
	return args.Error(0)
}

// Close closes the state manager
func (m *MockStateManager) Close() error {
	args := m.Called()
	return args.Error(0)
}

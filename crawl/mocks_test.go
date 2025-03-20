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
	//TODO implement me
	panic("implement me")
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
	messageId int64,
	chatId int64,
	info *channelInfo,
	crawlID string,
	channelUsername string,
	sm *state.StateManagementInterface,
	cfg common.CrawlerConfig) ([]string, error) {

	args := m.Called(tdlibClient, messageId, chatId, info, crawlID, channelUsername, sm, cfg)
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

// MockStateManager is a mock implementation of the StateManager type
type MockStateManager struct {
	mock.Mock
}

// UpdateStatePage mocks the UpdateStatePage method
func (m *MockStateManager) UpdateStatePage(page state.Page) {
	m.Called(page)
}

// UpdateStateMessage mocks the UpdateStateMessage method
func (m *MockStateManager) UpdateStateMessage(messageId int64, chatId int64, owner *state.Page, status string) {
	m.Called(messageId, chatId, owner, status)
}

// StoreState mocks the StoreState method
func (m *MockStateManager) StoreState() {
	m.Called()
}

// AppendLayerAndPersist mocks the AppendLayerAndPersist method
func (m *MockStateManager) AppendLayerAndPersist(pages []*state.Page) {
	m.Called(pages)
}

// StateSetup mocks the StateSetup method
func (m *MockStateManager) StateSetup(seedlist []string) (state.DaprStateStore, error) {
	args := m.Called(seedlist)
	return args.Get(0).(state.DaprStateStore), args.Error(1)
}

// StoreLayers mocks the StoreLayers method
func (m *MockStateManager) StoreLayers(layers []*state.Layer) error {
	args := m.Called(layers)
	return args.Error(0)
}

// StoreData mocks the StoreData method
func (m *MockStateManager) StoreData(channelname string, post model.Post) error {
	args := m.Called(channelname, post)
	return args.Error(0)
}

// UploadBlobFileAndDelete mocks the UploadBlobFileAndDelete method
func (m *MockStateManager) UploadBlobFileAndDelete(channelid, rawURL, filePath string) error {
	args := m.Called(channelid, rawURL, filePath)
	return args.Error(0)
}

// UploadStateToStorage mocks the UploadStateToStorage method
func (m *MockStateManager) UploadStateToStorage(channelid string) error {
	args := m.Called(channelid)
	return args.Error(0)
}

// UpdateCrawlManagement mocks the UpdateCrawlManagement method
func (m *MockStateManager) UpdateCrawlManagement(management state.CrawlManagement) error {
	args := m.Called(management)
	return args.Error(0)
}

// GetLayers mocks the GetLayers method
func (m *MockStateManager) GetLayers(ids []string) ([]*state.Layer, error) {
	args := m.Called(ids)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*state.Layer), args.Error(1)
}

// GetLastPreviousCrawlId mocks the GetLastPreviousCrawlId method
func (m *MockStateManager) GetLastPreviousCrawlId() ([]string, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}

// Mocking the internal helpers that might be called
func (m *MockStateManager) shouldUseAzure() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *MockStateManager) shouldUseDapr() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *MockStateManager) loadDaprState() (state.DaprStateStore, error) {
	args := m.Called()
	return args.Get(0).(state.DaprStateStore), args.Error(1)
}

func (m *MockStateManager) saveDaprState(store state.DaprStateStore) error {
	args := m.Called(store)
	return args.Error(0)
}

func (m *MockStateManager) saveDaprStateToFile(store state.DaprStateStore, channelid string) error {
	args := m.Called(store, channelid)
	return args.Error(0)
}

func (m *MockStateManager) savePageToFile(store state.Page, channelid string) error {
	args := m.Called(store, channelid)
	return args.Error(0)
}

func (m *MockStateManager) generateStorageKey(contname, crawlexecutionid string) string {
	args := m.Called(contname, crawlexecutionid)
	return args.String(0)
}

func (m *MockStateManager) storageExists() (bool, error) {
	args := m.Called()
	return args.Bool(0), args.Error(1)
}

func (m *MockStateManager) layersToState(seedlist []*state.Layer) error {
	args := m.Called(seedlist)
	return args.Error(0)
}

func (m *MockStateManager) loadListFromDapr() (state.DaprStateStore, error) {
	args := m.Called()
	return args.Get(0).(state.DaprStateStore), args.Error(1)
}

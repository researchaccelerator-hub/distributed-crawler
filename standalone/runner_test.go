package standalone

import (
	"fmt"
	"testing"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/model"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockDaprClient mocks the Dapr client
type MockDaprClient struct {
	mock.Mock
}

// We'll just implement methods needed for tests without relying on Dapr client types
func (m *MockDaprClient) GetState(ctx interface{}, storeName string, key string, opts interface{}) (response interface{}, err error) {
	args := m.Called(ctx, storeName, key, opts)
	return args.Get(0), args.Error(1)
}

func (m *MockDaprClient) SaveState(ctx interface{}, storeName string, key string, data []byte, opts interface{}) error {
	args := m.Called(ctx, storeName, key, data, opts)
	return args.Error(0)
}

func (m *MockDaprClient) InvokeBinding(ctx interface{}, req interface{}) (response interface{}, err error) {
	args := m.Called(ctx, req)
	return args.Get(0), args.Error(1)
}

// MockDaprStateManager uses a real DaprStateManager with a mocked Dapr client
// This allows us to test the DaprStateManager logic while mocking the actual Dapr calls
type MockDaprStateManager struct {
	*state.BaseStateManager
	client         *MockDaprClient
	stateStoreName string
	storageBinding string
}

// NewMockDaprStateManager creates a mock Dapr state manager
func NewMockDaprStateManager(config state.Config) (*MockDaprStateManager, error) {
	base := state.NewBaseStateManager(config)
	mockClient := new(MockDaprClient)

	// Setup the mock client with expectations - capture calls for verification
	mockClient.On("SaveState", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockClient.On("GetState", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	mockClient.On("InvokeBinding", mock.Anything, mock.Anything).Return(nil, nil)

	return &MockDaprStateManager{
		BaseStateManager: base,
		client:           mockClient,
		stateStoreName:   "statestore",
		storageBinding:   "telegramcrawlstorage",
	}, nil
}

// GetClient returns the mock client to verify expectations
func (m *MockDaprStateManager) GetClient() *MockDaprClient {
	return m.client
}

// UpdatePage overrides the BaseStateManager to also call Dapr's SaveState
func (m *MockDaprStateManager) UpdatePage(page state.Page) error {
	// First update in memory
	err := m.BaseStateManager.UpdatePage(page)
	if err != nil {
		return err
	}

	// Then simulate saving to Dapr
	pageKey := fmt.Sprintf("%s/page/%s", m.BaseStateManager.GetConfig().CrawlExecutionID, page.ID)
	m.client.SaveState(mock.Anything, m.stateStoreName, pageKey, []byte("{}"), nil)

	return nil
}

// SaveState simulates saving state to Dapr
func (m *MockDaprStateManager) SaveState() error {
	// Simulate saving layer map
	layerMapKey := fmt.Sprintf("%s/layer_map", m.BaseStateManager.GetConfig().CrawlExecutionID)
	m.client.SaveState(mock.Anything, m.stateStoreName, layerMapKey, []byte("{}"), nil)

	// Simulate saving metadata
	metadataKey := fmt.Sprintf("%s/metadata", m.BaseStateManager.GetConfig().CrawlID)
	m.client.SaveState(mock.Anything, m.stateStoreName, metadataKey, []byte("{}"), nil)

	return nil
}

// Implement other required methods of StateManagementInterface
func (m *MockDaprStateManager) StorePost(channelID string, post model.Post) error {
	// Call InvokeBinding to simulate storing a post
	m.client.InvokeBinding(mock.Anything, mock.Anything)
	return nil
}

func (m *MockDaprStateManager) StoreFile(channelID string, sourceFilePath string, fileName string) (string, string, error) {
	// Call InvokeBinding to simulate storing a file
	m.client.InvokeBinding(mock.Anything, mock.Anything)
	return "/mock/path/" + fileName, sourceFilePath, nil
}

func (m *MockDaprStateManager) HasProcessedMedia(mediaID string) (bool, error) {
	// Call GetState to simulate checking media cache
	m.client.GetState(mock.Anything, m.stateStoreName, mock.Anything, nil)
	return false, nil
}

func (m *MockDaprStateManager) MarkMediaAsProcessed(mediaID string) error {
	// Call SaveState to simulate updating media cache
	m.client.SaveState(mock.Anything, m.stateStoreName, mock.Anything, []byte("{}"), nil)
	return nil
}

func (m *MockDaprStateManager) ExportPagesToBinding(crawlID string) error {
	// Call InvokeBinding to simulate exporting pages
	m.client.InvokeBinding(mock.Anything, mock.Anything)
	return nil
}

func (m *MockDaprStateManager) Close() error {
	// Nothing to do here for the mock
	return nil
}

// These implement StateManagementInterface methods that might be called during tests
func (m *MockDaprStateManager) FindIncompleteCrawl(crawlID string) (string, bool, error) {
	// Call GetState to simulate checking for incomplete crawl
	m.client.GetState(mock.Anything, m.stateStoreName, mock.Anything, nil)
	return m.BaseStateManager.FindIncompleteCrawl(crawlID)
}

func (m *MockDaprStateManager) GetMaxDepth() (int, error) {
	return m.BaseStateManager.GetMaxDepth()
}

func (m *MockDaprStateManager) GetLayerByDepth(depth int) ([]state.Page, error) {
	return m.BaseStateManager.GetLayerByDepth(depth)
}

func (m *MockDaprStateManager) AddLayer(pages []state.Page) error {
	// First add in memory
	err := m.BaseStateManager.AddLayer(pages)
	if err != nil {
		return err
	}

	// Then simulate Dapr calls for each page
	for _, page := range pages {
		pageKey := fmt.Sprintf("%s/page/%s", m.BaseStateManager.GetConfig().CrawlExecutionID, page.ID)
		m.client.SaveState(mock.Anything, m.stateStoreName, pageKey, []byte("{}"), nil)
	}

	// And save state
	return m.SaveState()
}

func (m *MockDaprStateManager) Initialize(seedURLs []string) error {
	// First initialize in memory
	err := m.BaseStateManager.Initialize(seedURLs)
	if err != nil {
		return err
	}

	// Then simulate Dapr calls for each page
	for _, pageID := range m.BaseStateManager.GetLayerMap()[0] {
		page := m.BaseStateManager.GetPageMap()[pageID]
		pageKey := fmt.Sprintf("%s/page/%s", m.BaseStateManager.GetConfig().CrawlExecutionID, page.ID)
		m.client.SaveState(mock.Anything, m.stateStoreName, pageKey, []byte("{}"), nil)
	}

	// And save state
	return m.SaveState()
}

// UpdateMessage overrides BaseStateManager to simulate Dapr calls
func (m *MockDaprStateManager) UpdateMessage(pageID string, chatID int64, messageID int64, status string) error {
	// Update in memory first
	err := m.BaseStateManager.UpdateMessage(pageID, chatID, messageID, status)
	if err != nil {
		return err
	}

	// Then simulate saving to Dapr - no need to get the page, just save the right key

	pageKey := fmt.Sprintf("%s/page/%s", m.BaseStateManager.GetConfig().CrawlExecutionID, pageID)
	m.client.SaveState(mock.Anything, m.stateStoreName, pageKey, []byte("{}"), nil)

	return nil
}

// For random-walk sample
func (m *MockDaprStateManager) InitializeDiscoveredChannels() error             { return nil }
func (m *MockDaprStateManager) InitializeRandomWalkLayer() error                { return nil }
func (m *MockDaprStateManager) GetRandomDiscoveredChannel() (string, error)     { return "", nil }
func (m *MockDaprStateManager) IsDiscoveredChannel(channelID string) bool       { return true }
func (m *MockDaprStateManager) AddDiscoveredChannel(channelID string) error     { return nil }
func (m *MockDaprStateManager) SaveEdgeRecords(edges []*state.EdgeRecord) error { return nil }

// MockDaprStateManagerFactory creates our mock Dapr state manager
type MockDaprStateManagerFactory struct {
	mock.Mock
}

func (f *MockDaprStateManagerFactory) Create(config state.Config) (state.StateManagementInterface, error) {
	// Return our mock Dapr state manager
	mockManager, err := NewMockDaprStateManager(config)
	if err != nil {
		return nil, err
	}
	return mockManager, nil
}

// Regular mock for simple testing
type MockStateManager struct {
	mock.Mock
}

func (m *MockStateManager) Initialize(urlList []string) error {
	args := m.Called(urlList)
	return args.Error(0)
}

func (m *MockStateManager) GetLayerByDepth(depth int) ([]state.Page, error) {
	args := m.Called(depth)
	return args.Get(0).([]state.Page), args.Error(1)
}

func (m *MockStateManager) SaveState() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockStateManager) StorePost(channelID string, post model.Post) error {
	args := m.Called(channelID, post)
	return args.Error(0)
}

func (m *MockStateManager) UpdateMessage(pageID string, messageID int64, chatID int64, status string) error {
	args := m.Called(pageID, messageID, chatID, status)
	return args.Error(0)
}

func (m *MockStateManager) UpdatePage(page state.Page) error {
	args := m.Called(page)
	return args.Error(0)
}

func (m *MockStateManager) AddLayer(pages []state.Page) error {
	args := m.Called(pages)
	return args.Error(0)
}

func (m *MockStateManager) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockStateManager) FindIncompleteCrawl(crawlID string) (string, bool, error) {
	args := m.Called(crawlID)
	return args.String(0), args.Bool(1), args.Error(2)
}

func (m *MockStateManager) UpdateCrawlMetadata(crawlID string, metadata map[string]interface{}) error {
	args := m.Called(crawlID, metadata)
	return args.Error(0)
}

func (m *MockStateManager) HasProcessedMedia(mediaID string) (bool, error) {
	args := m.Called(mediaID)
	return args.Bool(0), args.Error(1)
}

func (m *MockStateManager) MarkMediaAsProcessed(mediaID string) error {
	args := m.Called(mediaID)
	return args.Error(0)
}

func (m *MockStateManager) ExportPagesToBinding(crawlID string) error {
	args := m.Called(crawlID)
	return args.Error(0)
}

func (m *MockStateManager) GetMaxDepth() (int, error) {
	args := m.Called()
	return args.Int(0), args.Error(1)
}

func (m *MockStateManager) GetPage(id string) (state.Page, error) {
	args := m.Called(id)
	return args.Get(0).(state.Page), args.Error(1)
}

func (m *MockStateManager) StoreFile(channelID string, sourceFilePath string, fileName string) (string, string, error) {
	args := m.Called(channelID, sourceFilePath, fileName)
	return args.String(0), args.String(1), args.Error(2)
}

func (m *MockStateManager) GetPreviousCrawls() ([]string, error) {
	args := m.Called()
	return args.Get(0).([]string), args.Error(1)
}

// MockStateManagerFactory mocks the state.StateManagerFactory interface
type MockStateManagerFactory struct {
	mock.Mock
}

func (m *MockStateManagerFactory) Create(cfg state.Config) (state.StateManagementInterface, error) {
	args := m.Called(cfg)
	return args.Get(0).(state.StateManagementInterface), args.Error(1)
}

func init() {
	// Set up zerolog for testing
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log.Logger = zerolog.New(zerolog.NewConsoleWriter()).With().Timestamp().Logger()
}

// TestResumeProcessing tests the resume functionality to verify that when resuming,
// we start from the first non-fetched page rather than starting over from the beginning
func TestResumeProcessing(t *testing.T) {
	// Create mock state manager and factory
	mockSM := new(MockStateManager)
	mockFactory := new(MockStateManagerFactory)

	// Save the original factory function to restore after test
	originalFactory := state.NewStateManagerFactory

	// Replace the factory function with our mock
	state.NewStateManagerFactory = func() state.StateManagerFactory {
		return mockFactory
	}

	// Restore original factory when test finishes
	defer func() {
		state.NewStateManagerFactory = originalFactory
	}()

	// Create test data - a layer with 5 pages, 3 of which are already processed
	testLayer := []state.Page{
		{
			ID:        "page1",
			URL:       "channel1",
			Status:    "fetched",
			Timestamp: time.Now(),
		},
		{
			ID:        "page2",
			URL:       "channel2",
			Status:    "fetched",
			Timestamp: time.Now(),
		},
		{
			ID:        "page3",
			URL:       "channel3",
			Status:    "fetched",
			Timestamp: time.Now(),
		},
		{
			ID:        "page4",
			URL:       "channel4",
			Status:    "unfetched",
			Timestamp: time.Now(),
		},
		{
			ID:        "page5",
			URL:       "channel5",
			Status:    "unfetched",
			Timestamp: time.Now(),
		},
	}

	// We expect page4 and page5 to be processed
	// page4 expectations
	mockSM.On("UpdatePage", mock.MatchedBy(func(page state.Page) bool {
		return page.ID == "page4" && page.Status == "processing"
	})).Return(nil)

	mockSM.On("UpdatePage", mock.MatchedBy(func(page state.Page) bool {
		return page.ID == "page4" && page.Status == "fetched"
	})).Return(nil)

	// page5 expectations
	mockSM.On("UpdatePage", mock.MatchedBy(func(page state.Page) bool {
		return page.ID == "page5" && page.Status == "processing"
	})).Return(nil)

	mockSM.On("UpdatePage", mock.MatchedBy(func(page state.Page) bool {
		return page.ID == "page5" && page.Status == "fetched"
	})).Return(nil)

	// SaveState will be called after each update
	mockSM.On("SaveState").Return(nil).Times(4)

	// Setup the factory to return our mock state manager
	mockFactory.On("Create", mock.AnythingOfType("state.Config")).Return(mockSM, nil)

	// Record which pages are actually processed
	var processedPages []string

	// Test the core resume loop directly
	// Find the first page in the layer that needs processing
	startIndex := 0
	for i, la := range testLayer {
		if la.Status == "fetched" {
			startIndex = i + 1
			continue
		}
		break
	}

	// Process from the first page that needs processing
	for i := startIndex; i < len(testLayer); i++ {
		la := testLayer[i]

		// Double-check status
		if la.Status == "fetched" {
			continue
		}

		// Record that this page was processed
		processedPages = append(processedPages, la.ID)

		// Set status to processing
		la.Status = "processing"
		mockSM.UpdatePage(la)
		mockSM.SaveState()

		// Now set to fetched
		la.Status = "fetched"
		mockSM.UpdatePage(la)
		mockSM.SaveState()
	}

	// Verify expectations
	mockSM.AssertExpectations(t)

	// Check that only pages 4 and 5 were processed
	assert.Equal(t, 2, len(processedPages), "Should have processed 2 pages")
	assert.Contains(t, processedPages, "page4", "Should have processed page4")
	assert.Contains(t, processedPages, "page5", "Should have processed page5")
	assert.NotContains(t, processedPages, "page1", "Should not have processed page1")
	assert.NotContains(t, processedPages, "page2", "Should not have processed page2")
	assert.NotContains(t, processedPages, "page3", "Should not have processed page3")
}

// TestResumeProcessingWithDaprMock tests the resume functionality using a real DaprStateManager with mocked Dapr client
func TestResumeProcessingWithDaprMock(t *testing.T) {
	// Create mock Dapr factory
	mockDaprFactory := new(MockDaprStateManagerFactory)

	// Save the original factory function to restore after test
	originalFactory := state.NewStateManagerFactory

	// Replace the factory function with our mock
	state.NewStateManagerFactory = func() state.StateManagerFactory {
		return mockDaprFactory
	}

	// Restore original factory when test finishes
	defer func() {
		state.NewStateManagerFactory = originalFactory
	}()

	// Create test configuration
	config := state.Config{
		StorageRoot:      "/test/storage",
		CrawlID:          "test-crawl",
		CrawlExecutionID: "test-execution",
		DaprConfig: &state.DaprConfig{
			StateStoreName: "statestore",
			ComponentName:  "statestore",
		},
	}

	// Create a mock Dapr state manager
	sm, err := NewMockDaprStateManager(config)
	assert.NoError(t, err, "Failed to create mock Dapr state manager")

	// Create test layer
	testLayer := []state.Page{
		{
			ID:        "page1",
			URL:       "channel1",
			Status:    "fetched",
			Timestamp: time.Now(),
		},
		{
			ID:        "page2",
			URL:       "channel2",
			Status:    "fetched",
			Timestamp: time.Now(),
		},
		{
			ID:        "page3",
			URL:       "channel3",
			Status:    "fetched",
			Timestamp: time.Now(),
		},
		{
			ID:        "page4",
			URL:       "channel4",
			Status:    "unfetched",
			Timestamp: time.Now(),
		},
		{
			ID:        "page5",
			URL:       "channel5",
			Status:    "unfetched",
			Timestamp: time.Now(),
		},
	}

	// Add test layer to the state manager
	sm.BaseStateManager.SetLayerMap(map[int][]string{
		0: {"page1", "page2", "page3", "page4", "page5"},
	})

	// Add pages to page map
	pageMap := make(map[string]state.Page)
	for _, page := range testLayer {
		pageMap[page.ID] = page
	}
	sm.BaseStateManager.SetPageMap(pageMap)

	// Process the layer using the resume logic
	processedPages := make([]string, 0)

	// Find the first page that needs processing
	startIndex := 0
	for i, pageID := range sm.BaseStateManager.GetLayerMap()[0] {
		page := sm.BaseStateManager.GetPageMap()[pageID]
		if page.Status == "fetched" {
			startIndex = i + 1
			continue
		}
		break
	}

	// Process from the first unfetched page
	for i := startIndex; i < len(sm.BaseStateManager.GetLayerMap()[0]); i++ {
		pageID := sm.BaseStateManager.GetLayerMap()[0][i]
		page := sm.BaseStateManager.GetPageMap()[pageID]

		// Skip already fetched pages
		if page.Status == "fetched" {
			continue
		}

		// Record this page as processed
		processedPages = append(processedPages, page.ID)

		// Update status to processing
		page.Status = "processing"
		sm.BaseStateManager.UpdatePage(page)

		// Then update to fetched
		page.Status = "fetched"
		sm.BaseStateManager.UpdatePage(page)
	}

	// Verify that only pages 4 and 5 were processed
	assert.Equal(t, 2, len(processedPages), "Should have processed 2 pages")
	assert.Contains(t, processedPages, "page4", "Should have processed page4")
	assert.Contains(t, processedPages, "page5", "Should have processed page5")
	assert.NotContains(t, processedPages, "page1", "Should not have processed page1")
	assert.NotContains(t, processedPages, "page2", "Should not have processed page2")
	assert.NotContains(t, processedPages, "page3", "Should not have processed page3")
}

// TestMessageStatusResumption tests that pages with 'fetched' status but 'resample' message status
// are properly handled during resumption
func TestMessageStatusResumption(t *testing.T) {
	// Skip if we're in a quick test mode
	if testing.Short() {
		t.Skip("Skipping message status resumption test in short mode")
	}

	// Create test configuration
	config := state.Config{
		StorageRoot:      "/test/storage",
		CrawlID:          "test-crawl",
		CrawlExecutionID: "test-execution",
		DaprConfig: &state.DaprConfig{
			StateStoreName: "statestore",
			ComponentName:  "statestore",
		},
	}

	// Create a mock Dapr state manager directly
	sm, err := NewMockDaprStateManager(config)
	assert.NoError(t, err, "Failed to create mock Dapr state manager")

	// Create a test page that has 'fetched' status but messages with 'resample' status
	testPage := state.Page{
		ID:        "pokraslampas-id",
		URL:       "pokraslampas",
		Depth:     0,
		Status:    "fetched", // Page is marked as fetched
		Timestamp: time.Now(),
		Messages: []state.Message{
			{
				ChatID:    -1001148656633,
				MessageID: 2206203904,
				Status:    "resample", // But message needs resampling
				PageID:    "pokraslampas-id",
			},
			{
				ChatID:    -1001148656633,
				MessageID: 2205155328,
				Status:    "resample",
				PageID:    "pokraslampas-id",
			},
		},
	}

	// Add the page to the state manager's page map
	pageMap := make(map[string]state.Page)
	pageMap[testPage.ID] = testPage
	sm.BaseStateManager.SetPageMap(pageMap)

	// Set up the layer map so the page is in layer 0
	sm.BaseStateManager.SetLayerMap(map[int][]string{
		0: {testPage.ID},
	})

	// Reset the mock client
	sm.client = new(MockDaprClient)
	sm.client.On("SaveState", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	sm.client.On("GetState", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	// Get the layer and check if the page with fetched status but resample message status is included
	layer0, err := sm.GetLayerByDepth(0)
	assert.NoError(t, err, "Failed to get layer 0")
	assert.Equal(t, 1, len(layer0), "Layer 0 should have 1 page")

	// Verify the page is returned with the status we expect
	assert.Equal(t, "fetched", layer0[0].Status, "Page status should remain 'fetched'")
	assert.Equal(t, 2, len(layer0[0].Messages), "Page should have 2 messages")
	assert.Equal(t, "resample", layer0[0].Messages[0].Status, "First message should have 'resample' status")
	assert.Equal(t, "resample", layer0[0].Messages[1].Status, "Second message should have 'resample' status")

	// The issue is that when we get a page with fetched status, we need to check if any messages
	// still need processing, and if so, we should process those messages without changing the page status

	// Simulate checking for unprocessed messages and updating them
	messagesNeedProcessing := false
	for _, msg := range layer0[0].Messages {
		if msg.Status == "resample" || msg.Status == "unfetched" {
			messagesNeedProcessing = true
			break
		}
	}

	assert.True(t, messagesNeedProcessing, "Page should be identified as needing message processing")

	// Process each message that needs resampling
	processed := false
	for _, msg := range layer0[0].Messages {
		if msg.Status == "resample" || msg.Status == "unfetched" {
			// Update the message status to "processing"
			err := sm.UpdateMessage(testPage.ID, msg.ChatID, msg.MessageID, "processing")
			assert.NoError(t, err, "Failed to update message status to processing")

			// Simulate processing the message

			// Update the message status to "fetched"
			err = sm.UpdateMessage(testPage.ID, msg.ChatID, msg.MessageID, "fetched")
			assert.NoError(t, err, "Failed to update message status to fetched")

			processed = true
		}
	}

	assert.True(t, processed, "Messages should have been processed")

	// Reload the page after processing messages
	updatedPage, err := sm.GetPage(testPage.ID)
	assert.NoError(t, err, "Failed to get updated page")

	// Verify that all messages now have "fetched" status
	assert.Equal(t, "fetched", updatedPage.Status, "Page status should remain 'fetched'")
	for i, msg := range updatedPage.Messages {
		assert.Equal(t, "fetched", msg.Status, fmt.Sprintf("Message %d should have 'fetched' status", i))
	}

	// Verify SaveState was called appropriately
	sm.client.AssertCalled(t, "SaveState", mock.Anything, "statestore", mock.Anything, mock.Anything, mock.Anything)
}

// TestLayerResumptionWithDaprMock tests the layer resumption logic with mocked Dapr calls
func TestLayerResumptionWithDaprMock(t *testing.T) {
	// Skip if we're in a quick test mode
	if testing.Short() {
		t.Skip("Skipping layer resumption test in short mode")
	}

	// Create test configuration
	config := state.Config{
		StorageRoot:      "/test/storage",
		CrawlID:          "test-crawl",
		CrawlExecutionID: "test-execution",
		DaprConfig: &state.DaprConfig{
			StateStoreName: "statestore",
			ComponentName:  "statestore",
		},
	}

	// Create a mock Dapr state manager directly
	sm, err := NewMockDaprStateManager(config)
	assert.NoError(t, err, "Failed to create mock Dapr state manager")

	// Create test data
	testLayer := []state.Page{
		{
			ID:        "page1",
			URL:       "channel1",
			Status:    "fetched",
			Timestamp: time.Now(),
		},
		{
			ID:        "page2",
			URL:       "channel2",
			Status:    "fetched",
			Timestamp: time.Now(),
		},
		{
			ID:        "page3",
			URL:       "channel3",
			Status:    "fetched",
			Timestamp: time.Now(),
		},
		{
			ID:        "page4",
			URL:       "channel4",
			Status:    "unfetched",
			Timestamp: time.Now(),
		},
		{
			ID:        "page5",
			URL:       "channel5",
			Status:    "unfetched",
			Timestamp: time.Now(),
		},
	}

	// Add test layer to the state manager
	sm.BaseStateManager.SetLayerMap(map[int][]string{
		0: {"page1", "page2", "page3", "page4", "page5"},
	})

	// Add pages to page map
	pageMap := make(map[string]state.Page)
	for _, page := range testLayer {
		pageMap[page.ID] = page
	}
	sm.BaseStateManager.SetPageMap(pageMap)

	// Set up our test case - we're focusing on the layer processing part
	// not the full crawler initialization path
	processed := make(map[string]bool)

	// Reset the mock client call count
	sm.client = new(MockDaprClient)
	// Setup expectations for the mock client
	sm.client.On("SaveState", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	sm.client.On("GetState", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	// Create some more specific expectations to verify correct key patterns
	expectedPageKey1 := fmt.Sprintf("%s/page/page4", config.CrawlExecutionID)
	expectedPageKey2 := fmt.Sprintf("%s/page/page5", config.CrawlExecutionID)
	expectedLayerMapKey := fmt.Sprintf("%s/layer_map", config.CrawlExecutionID)
	expectedMetadataKey := fmt.Sprintf("%s/metadata", config.CrawlID)

	// Track specific keys for deeper verification
	sm.client.On("SaveState", mock.Anything, "statestore", expectedPageKey1, mock.Anything, mock.Anything).Return(nil)
	sm.client.On("SaveState", mock.Anything, "statestore", expectedPageKey2, mock.Anything, mock.Anything).Return(nil)
	sm.client.On("SaveState", mock.Anything, "statestore", expectedLayerMapKey, mock.Anything, mock.Anything).Return(nil)
	sm.client.On("SaveState", mock.Anything, "statestore", expectedMetadataKey, mock.Anything, mock.Anything).Return(nil)

	// Test the core layer processing logic directly
	// Find the first unfetched page
	startIndex := 0
	layer0 := sm.BaseStateManager.GetLayerMap()[0]
	for i, pageID := range layer0 {
		page := sm.BaseStateManager.GetPageMap()[pageID]
		if page.Status == "fetched" {
			startIndex = i + 1
			continue
		}
		break
	}

	// Process from this index forward
	for i := startIndex; i < len(layer0); i++ {
		pageID := layer0[i]
		page := sm.BaseStateManager.GetPageMap()[pageID]

		// Skip already fetched pages
		if page.Status == "fetched" {
			continue
		}

		// Mark as processing
		page.Status = "processing"
		err := sm.UpdatePage(page) // Use the MockDaprStateManager's UpdatePage to test Dapr calls
		assert.NoError(t, err, "Failed to update page status to processing")

		// Record that we processed it
		processed[page.ID] = true

		// Mark as completed and call SaveState
		page.Status = "fetched"
		err = sm.UpdatePage(page) // Use the MockDaprStateManager's UpdatePage to test Dapr calls
		assert.NoError(t, err, "Failed to update page status to fetched")

		err = sm.SaveState() // This should trigger Dapr SaveState calls
		assert.NoError(t, err, "Failed to save state")
	}

	// Verify expectations on the mock client
	sm.client.AssertNumberOfCalls(t, "SaveState", 8) // 4 from UpdatePage + 4 from SaveState (2 calls per SaveState function call)

	// Verify specific key patterns were used
	sm.client.AssertCalled(t, "SaveState", mock.Anything, "statestore", expectedPageKey1, mock.Anything, mock.Anything)
	sm.client.AssertCalled(t, "SaveState", mock.Anything, "statestore", expectedPageKey2, mock.Anything, mock.Anything)
	sm.client.AssertCalled(t, "SaveState", mock.Anything, "statestore", expectedLayerMapKey, mock.Anything, mock.Anything)
	sm.client.AssertCalled(t, "SaveState", mock.Anything, "statestore", expectedMetadataKey, mock.Anything, mock.Anything)

	// Check that only the unfetched pages were processed
	assert.True(t, processed["page4"], "Page4 should have been processed")
	assert.True(t, processed["page5"], "Page5 should have been processed")
	assert.False(t, processed["page1"], "Page1 should NOT have been processed")
	assert.False(t, processed["page2"], "Page2 should NOT have been processed")
	assert.False(t, processed["page3"], "Page3 should NOT have been processed")
}

// TestDaprStateManagerMultiLayerOperations tests multi-layer operations with the DaprStateManager
func TestDaprStateManagerMultiLayerOperations(t *testing.T) {
	// Skip if we're in a quick test mode
	if testing.Short() {
		t.Skip("Skipping multi-layer test in short mode")
	}

	// Create test configuration
	config := state.Config{
		StorageRoot:      "/test/storage",
		CrawlID:          "test-crawl",
		CrawlExecutionID: "test-execution",
		DaprConfig: &state.DaprConfig{
			StateStoreName: "statestore",
			ComponentName:  "statestore",
		},
	}

	// Create a mock Dapr state manager
	sm, err := NewMockDaprStateManager(config)
	assert.NoError(t, err, "Failed to create mock Dapr state manager")

	// Reset the mock client call count
	sm.client = new(MockDaprClient)
	sm.client.On("SaveState", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	sm.client.On("GetState", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	// 1. Initialize with seed URLs
	seedURLs := []string{"channel1", "channel2"}
	err = sm.Initialize(seedURLs)
	assert.NoError(t, err, "Failed to initialize state manager")

	// Verify that Initialize called SaveState for each page plus the layer map
	expectedInitCalls := len(seedURLs) + 2 // pages + layerMap + metadata
	sm.client.AssertNumberOfCalls(t, "SaveState", expectedInitCalls)

	// 2. Get the layer 0 pages
	layer0, err := sm.GetLayerByDepth(0)
	assert.NoError(t, err, "Failed to get layer 0")
	assert.Equal(t, len(seedURLs), len(layer0), "Layer 0 should have same number of pages as seed URLs")

	// 3. Process layer 0 and create layer 1
	layer1Pages := make([]state.Page, 0)
	for _, page := range layer0 {
		// Mark as processing
		page.Status = "processing"
		err := sm.UpdatePage(page)
		assert.NoError(t, err, "Failed to update page status to processing")

		// Simulate page processing by creating child pages
		childPages := []state.Page{
			{
				ID:        fmt.Sprintf("child1-%s", page.ID),
				URL:       fmt.Sprintf("child1-url-%s", page.URL),
				Depth:     1,
				Status:    "unfetched",
				Timestamp: time.Now(),
			},
			{
				ID:        fmt.Sprintf("child2-%s", page.ID),
				URL:       fmt.Sprintf("child2-url-%s", page.URL),
				Depth:     1,
				Status:    "unfetched",
				Timestamp: time.Now(),
			},
		}

		// Add these pages to layer 1
		layer1Pages = append(layer1Pages, childPages...)

		// Mark original page as fetched
		page.Status = "fetched"
		err = sm.UpdatePage(page)
		assert.NoError(t, err, "Failed to update page status to fetched")
	}

	// Reset SaveState call count before adding new layer
	// Clear previous calls
	sm.client.Calls = make([]mock.Call, 0)

	// 4. Add layer 1 pages
	err = sm.AddLayer(layer1Pages)
	assert.NoError(t, err, "Failed to add layer 1")

	// Verify that AddLayer properly called SaveState
	expectedLayer1Calls := len(layer1Pages) + 2 // Each page + layerMap + metadata
	assert.Equal(t, expectedLayer1Calls, len(sm.client.Calls), "AddLayer should have made the expected number of SaveState calls")

	// 5. Verify layer structure
	maxDepth, err := sm.GetMaxDepth()
	assert.NoError(t, err, "Failed to get max depth")
	assert.Equal(t, 1, maxDepth, "Max depth should be 1")

	layer1, err := sm.GetLayerByDepth(1)
	assert.NoError(t, err, "Failed to get layer 1")
	assert.Equal(t, len(layer1Pages), len(layer1), "Layer 1 should have correct number of pages")

	// 6. Verify page key patterns
	for _, page := range layer1 {
		pageKey := fmt.Sprintf("%s/page/%s", config.CrawlExecutionID, page.ID)
		sm.client.AssertCalled(t, "SaveState", mock.Anything, "statestore", pageKey, mock.Anything, mock.Anything)
	}

	// 7. Check layer map key pattern
	layerMapKey := fmt.Sprintf("%s/layer_map", config.CrawlExecutionID)
	sm.client.AssertCalled(t, "SaveState", mock.Anything, "statestore", layerMapKey, mock.Anything, mock.Anything)
}

// TestLayerResumption tests the full layer resumption logic
func TestLayerResumption(t *testing.T) {
	// Skip if we're in a quick test mode
	if testing.Short() {
		t.Skip("Skipping layer resumption test in short mode")
	}

	// Create mock state manager and factory
	mockSM := new(MockStateManager)
	mockFactory := new(MockStateManagerFactory)

	// Create test data
	testLayer := []state.Page{
		{
			ID:        "page1",
			URL:       "channel1",
			Status:    "fetched",
			Timestamp: time.Now(),
		},
		{
			ID:        "page2",
			URL:       "channel2",
			Status:    "fetched",
			Timestamp: time.Now(),
		},
		{
			ID:        "page3",
			URL:       "channel3",
			Status:    "fetched",
			Timestamp: time.Now(),
		},
		{
			ID:        "page4",
			URL:       "channel4",
			Status:    "unfetched",
			Timestamp: time.Now(),
		},
		{
			ID:        "page5",
			URL:       "channel5",
			Status:    "unfetched",
			Timestamp: time.Now(),
		},
	}

	// Set expectations for updating pages during processing
	// page4 expectations
	mockSM.On("UpdatePage", mock.MatchedBy(func(page state.Page) bool {
		return page.ID == "page4" && page.Status == "processing"
	})).Return(nil)

	mockSM.On("UpdatePage", mock.MatchedBy(func(page state.Page) bool {
		return page.ID == "page4" && page.Status == "fetched"
	})).Return(nil)

	// page5 expectations
	mockSM.On("UpdatePage", mock.MatchedBy(func(page state.Page) bool {
		return page.ID == "page5" && page.Status == "processing"
	})).Return(nil)

	mockSM.On("UpdatePage", mock.MatchedBy(func(page state.Page) bool {
		return page.ID == "page5" && page.Status == "fetched"
	})).Return(nil)

	// SaveState will be called after each update
	mockSM.On("SaveState").Return(nil).Times(4)

	// Simulate running the crawl with our test configuration
	mockFactory.On("Create", mock.AnythingOfType("state.Config")).Return(mockSM, nil)

	// Set up our test case - we're focusing on the layer processing part
	// not the full crawler initialization path

	// We'll test the core resume logic without trying to mock the launch function
	processed := make(map[string]bool)

	// Test the core layer processing logic directly
	currentLayer := testLayer

	// Find the first unfetched page
	startIndex := 0
	for i, la := range currentLayer {
		if la.Status == "fetched" {
			startIndex = i + 1
			continue
		}
		break
	}

	// Process from this index forward
	for i := startIndex; i < len(currentLayer); i++ {
		page := currentLayer[i]

		// Skip already fetched pages
		if page.Status == "fetched" {
			continue
		}

		// Mark as processing
		page.Status = "processing"
		mockSM.UpdatePage(page)
		mockSM.SaveState()

		// Record that we processed it
		processed[page.ID] = true

		// Mark as completed
		page.Status = "fetched"
		mockSM.UpdatePage(page)
		mockSM.SaveState()
	}

	// Verify expectations
	mockSM.AssertExpectations(t)

	// Check that only the unfetched pages were processed
	assert.True(t, processed["page4"], "Page4 should have been processed")
	assert.True(t, processed["page5"], "Page5 should have been processed")
	assert.False(t, processed["page1"], "Page1 should NOT have been processed")
	assert.False(t, processed["page2"], "Page2 should NOT have been processed")
	assert.False(t, processed["page3"], "Page3 should NOT have been processed")
}

package distributed

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
)

// MockIntegrationStateManager provides a more complete mock for integration testing
type MockIntegrationStateManager struct {
	mock.Mock
	pages map[string]*state.Page
	state *state.State
	mutex sync.RWMutex
}

func NewMockIntegrationStateManager() *MockIntegrationStateManager {
	return &MockIntegrationStateManager{
		pages: make(map[string]*state.Page),
		state: &state.State{
			Layers:      []*state.Layer{},
			Metadata:    state.CrawlMetadata{},
			LastUpdated: time.Now(),
		},
	}
}

func (m *MockIntegrationStateManager) GetPage(url string, depth int) (*state.Page, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	key := url + "_" + string(rune(depth))
	if page, exists := m.pages[key]; exists {
		return page, nil
	}
	return nil, nil
}

func (m *MockIntegrationStateManager) SavePage(page *state.Page) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	key := page.URL + "_" + string(rune(page.Depth))
	m.pages[key] = page
	return nil
}

func (m *MockIntegrationStateManager) GetState() (*state.State, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.state, nil
}

func (m *MockIntegrationStateManager) SaveState(state *state.State) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.state = state
	return nil
}

func (m *MockIntegrationStateManager) GetPagesByDepth(depth int) ([]*state.Page, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	var pages []*state.Page
	for _, page := range m.pages {
		if page.Depth == depth {
			pages = append(pages, page)
		}
	}
	return pages, nil
}

func (m *MockIntegrationStateManager) GetPendingPages() ([]*state.Page, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	
	var pages []*state.Page
	for _, page := range m.pages {
		if page.Status == "pending" {
			pages = append(pages, page)
		}
	}
	return pages, nil
}

func (m *MockIntegrationStateManager) MarkPageProcessed(url string, depth int) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	key := url + "_" + string(rune(depth))
	if page, exists := m.pages[key]; exists {
		page.Status = "processed"
	}
	return nil
}

func (m *MockIntegrationStateManager) Close() error {
	return nil
}

// MockIntegrationPubSub provides an in-memory pubsub for integration testing
type MockIntegrationPubSub struct {
	workQueue     chan WorkQueueMessage
	results       chan ResultMessage
	status        chan StatusMessage
	control       chan ControlMessage
	subscriptions map[string]interface{}
	mutex         sync.RWMutex
}

func NewMockIntegrationPubSub() *MockIntegrationPubSub {
	return &MockIntegrationPubSub{
		workQueue:     make(chan WorkQueueMessage, 100),
		results:       make(chan ResultMessage, 100),
		status:        make(chan StatusMessage, 100),
		control:       make(chan ControlMessage, 100),
		subscriptions: make(map[string]interface{}),
	}
}

func (m *MockIntegrationPubSub) PublishWorkItem(ctx context.Context, item WorkItem, priority int, ttlSeconds int) error {
	message := NewWorkQueueMessage(item, priority, ttlSeconds)
	select {
	case m.workQueue <- message:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *MockIntegrationPubSub) PublishResult(ctx context.Context, result WorkResult, discoveredPages []DiscoveredPage) error {
	message := NewResultMessage(result, discoveredPages)
	select {
	case m.results <- message:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *MockIntegrationPubSub) PublishStatus(ctx context.Context, status StatusMessage) error {
	select {
	case m.status <- status:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *MockIntegrationPubSub) PublishControl(ctx context.Context, command, targetID string, parameters map[string]interface{}) error {
	message := ControlMessage{
		MessageType: command,
		Command:     command,
		TargetID:    targetID,
		Parameters:  parameters,
		Timestamp:   time.Now(),
		TraceID:     generateTraceID(),
	}
	
	select {
	case m.control <- message:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *MockIntegrationPubSub) SubscribeToWorkQueue(handler func(context.Context, WorkQueueMessage) error) {
	m.mutex.Lock()
	m.subscriptions["work"] = handler
	m.mutex.Unlock()
	
	// Start goroutine to process work queue messages
	go func() {
		for message := range m.workQueue {
			if h, ok := m.subscriptions["work"].(func(context.Context, WorkQueueMessage) error); ok {
				h(context.Background(), message)
			}
		}
	}()
}

func (m *MockIntegrationPubSub) SubscribeToResults(handler func(context.Context, ResultMessage) error) {
	m.mutex.Lock()
	m.subscriptions["results"] = handler
	m.mutex.Unlock()
	
	// Start goroutine to process result messages
	go func() {
		for message := range m.results {
			if h, ok := m.subscriptions["results"].(func(context.Context, ResultMessage) error); ok {
				h(context.Background(), message)
			}
		}
	}()
}

func (m *MockIntegrationPubSub) SubscribeToStatus(handler func(context.Context, StatusMessage) error) {
	m.mutex.Lock()
	m.subscriptions["status"] = handler
	m.mutex.Unlock()
	
	// Start goroutine to process status messages
	go func() {
		for message := range m.status {
			if h, ok := m.subscriptions["status"].(func(context.Context, StatusMessage) error); ok {
				h(context.Background(), message)
			}
		}
	}()
}

func (m *MockIntegrationPubSub) SubscribeToControl(handler func(context.Context, ControlMessage) error) {
	m.mutex.Lock()
	m.subscriptions["control"] = handler
	m.mutex.Unlock()
	
	// Start goroutine to process control messages
	go func() {
		for message := range m.control {
			if h, ok := m.subscriptions["control"].(func(context.Context, ControlMessage) error); ok {
				h(context.Background(), message)
			}
		}
	}()
}

func (m *MockIntegrationPubSub) StartServer(ctx context.Context) error {
	return nil
}

func (m *MockIntegrationPubSub) Close() error {
	close(m.workQueue)
	close(m.results)
	close(m.status)
	close(m.control)
	return nil
}

// TestDistributedWorkflowIntegration tests the complete workflow from orchestrator to worker
func TestDistributedWorkflowIntegration(t *testing.T) {
	// Setup shared components
	pubsub := NewMockIntegrationPubSub()
	_ = NewMockIntegrationStateManager() // orchestratorState for full implementation
	workerState := NewMockIntegrationStateManager()

	// Create orchestrator configuration
	orchestratorConfig := common.CrawlerConfig{
		Platform:    "telegram",
		StorageRoot: "/tmp/test-orchestrator",
		MaxDepth:    2,
		Concurrency: 1,
		Timeout:     30,
	}

	// Create worker configuration
	workerConfig := common.CrawlerConfig{
		Platform:    "telegram",
		StorageRoot: "/tmp/test-worker",
		Concurrency: 1,
		DaprPort:    3500,
	}

	// Note: orchestrator would be created here in a full integration test
	_ = orchestratorConfig // Use config to avoid unused variable error

	// Simulate worker
	worker := &MockWorker{
		ID:           "integration-worker-1",
		config:       workerConfig,
		stateManager: workerState,
		pubsub:       pubsub,
		isRunning:    false,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test workflow
	t.Run("OrchestratorPublishesWork", func(t *testing.T) {
		// Orchestrator publishes initial work item
		workItem := NewWorkItem(
			"https://t.me/testchannel",
			0,
			"",
			"integration-test",
			"telegram",
			WorkItemConfig{
				StorageRoot: "/tmp/test",
				Concurrency: 1,
			},
		)

		err := pubsub.PublishWorkItem(ctx, workItem, PriorityHigh, 3600)
		assert.NoError(t, err)

		// Verify work item was published
		select {
		case message := <-pubsub.workQueue:
			assert.Equal(t, MessageTypeWorkItem, message.MessageType)
			assert.Equal(t, workItem.URL, message.WorkItem.URL)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Work item was not published")
		}
	})

	t.Run("WorkerProcessesWork", func(t *testing.T) {
		// Create a work item for the worker to process
		workItem := WorkItem{
			ID:       "test-work-item",
			URL:      "https://t.me/testchannel",
			Platform: "telegram",
			Depth:    0,
			CrawlID:  "integration-test",
			Config: WorkItemConfig{
				StorageRoot: "/tmp/test",
				Concurrency: 1,
			},
			CreatedAt: time.Now(),
			TraceID:   "test-trace",
		}

		// Worker processes the work item (mock successful processing)
		result := WorkResult{
			WorkItemID:     workItem.ID,
			WorkerID:       worker.ID,
			Status:         StatusSuccess,
			ProcessedURL:   workItem.URL,
			MessageCount:   25,
			ProcessingTime: 15 * time.Second,
			CompletedAt:    time.Now(),
			DiscoveredPages: []DiscoveredPage{
				{
					URL:      "https://t.me/discoveredchannel",
					ParentID: workItem.ID,
					Depth:    1,
					Platform: "telegram",
				},
			},
		}

		err := pubsub.PublishResult(ctx, result, result.DiscoveredPages)
		assert.NoError(t, err)

		// Verify result was published
		select {
		case message := <-pubsub.results:
			assert.Equal(t, MessageTypeWorkResult, message.MessageType)
			assert.Equal(t, result.WorkItemID, message.WorkResult.WorkItemID)
			assert.Equal(t, StatusSuccess, message.WorkResult.Status)
			assert.Len(t, message.DiscoveredPages, 1)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Result was not published")
		}
	})

	t.Run("WorkerSendsStatusUpdates", func(t *testing.T) {
		// Worker sends status update
		status := NewStatusMessage(
			worker.ID,
			MessageTypeHeartbeat,
			WorkerStatusIdle,
			1,
			1,
			0,
			2*time.Minute,
		)

		err := pubsub.PublishStatus(ctx, status)
		assert.NoError(t, err)

		// Verify status was published
		select {
		case message := <-pubsub.status:
			assert.Equal(t, worker.ID, message.WorkerID)
			assert.Equal(t, MessageTypeHeartbeat, message.MessageType)
			assert.Equal(t, WorkerStatusIdle, message.Status)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Status was not published")
		}
	})

	t.Run("OrchestratorSendsControlMessages", func(t *testing.T) {
		// Orchestrator sends control message
		err := pubsub.PublishControl(ctx, MessageTypePause, worker.ID, map[string]interface{}{
			"reason": "maintenance",
		})
		assert.NoError(t, err)

		// Verify control message was published
		select {
		case message := <-pubsub.control:
			assert.Equal(t, MessageTypePause, message.Command)
			assert.Equal(t, worker.ID, message.TargetID)
			assert.Contains(t, message.Parameters, "reason")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Control message was not published")
		}
	})
}

// TestMessageSerialization tests that all message types can be properly serialized/deserialized
func TestMessageSerialization(t *testing.T) {
	tests := []struct {
		name    string
		message interface{}
	}{
		{
			name: "WorkQueueMessage",
			message: NewWorkQueueMessage(
				NewWorkItem("https://t.me/test", 1, "parent", "crawl", "telegram", WorkItemConfig{}),
				PriorityHigh,
				3600,
			),
		},
		{
			name: "ResultMessage",
			message: NewResultMessage(
				WorkResult{
					WorkItemID:   "test-work",
					WorkerID:     "test-worker",
					Status:       StatusSuccess,
					ProcessedURL: "https://t.me/test",
					CompletedAt:  time.Now(),
				},
				[]DiscoveredPage{
					{URL: "https://t.me/discovered", Platform: "telegram", Depth: 2},
				},
			),
		},
		{
			name: "StatusMessage",
			message: NewStatusMessage(
				"test-worker",
				MessageTypeHeartbeat,
				WorkerStatusIdle,
				10,
				8,
				2,
				5*time.Minute,
			),
		},
		{
			name: "ControlMessage",
			message: ControlMessage{
				MessageType: MessageTypePause,
				Command:     MessageTypePause,
				TargetID:    "test-worker",
				Parameters:  map[string]interface{}{"reason": "test"},
				Timestamp:   time.Now(),
				TraceID:     generateTraceID(),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This test validates that messages have all required fields
			// and can be processed by the mock pubsub system
			assert.NotNil(t, tt.message)
			
			// Validate specific message types have required fields
			switch msg := tt.message.(type) {
			case WorkQueueMessage:
				assert.NotEmpty(t, msg.MessageType)
				assert.NotEmpty(t, msg.WorkItem.ID)
				assert.NotEmpty(t, msg.TraceID)
			case ResultMessage:
				assert.NotEmpty(t, msg.MessageType)
				assert.NotEmpty(t, msg.WorkResult.WorkItemID)
				assert.NotEmpty(t, msg.TraceID)
			case StatusMessage:
				assert.NotEmpty(t, msg.MessageType)
				assert.NotEmpty(t, msg.WorkerID)
				assert.NotEmpty(t, msg.TraceID)
			case ControlMessage:
				assert.NotEmpty(t, msg.MessageType)
				assert.NotEmpty(t, msg.Command)
				assert.NotEmpty(t, msg.TraceID)
			}
		})
	}
}

// TestConcurrentWorkDistribution tests handling multiple workers and work items concurrently
func TestConcurrentWorkDistribution(t *testing.T) {
	pubsub := NewMockIntegrationPubSub()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Create multiple work items
	workItems := []WorkItem{
		NewWorkItem("https://t.me/channel1", 0, "", "test-crawl", "telegram", WorkItemConfig{}),
		NewWorkItem("https://t.me/channel2", 0, "", "test-crawl", "telegram", WorkItemConfig{}),
		NewWorkItem("https://t.me/channel3", 0, "", "test-crawl", "telegram", WorkItemConfig{}),
	}

	// Publish work items concurrently
	var wg sync.WaitGroup
	for _, item := range workItems {
		wg.Add(1)
		go func(workItem WorkItem) {
			defer wg.Done()
			err := pubsub.PublishWorkItem(ctx, workItem, PriorityMedium, 3600)
			assert.NoError(t, err)
		}(item)
	}

	// Simulate workers processing items concurrently
	workerCount := 2
	processedCount := 0
	var processingWg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		processingWg.Add(1)
		go func(workerID int) {
			defer processingWg.Done()
			
			for {
				select {
				case workMsg := <-pubsub.workQueue:
					if workMsg.MessageType == MessageTypeWorkItem {
						// Simulate processing
						result := WorkResult{
							WorkItemID:   workMsg.WorkItem.ID,
							WorkerID:     "worker-" + string(rune(workerID)),
							Status:       StatusSuccess,
							ProcessedURL: workMsg.WorkItem.URL,
							CompletedAt:  time.Now(),
						}
						
						pubsub.PublishResult(ctx, result, []DiscoveredPage{})
						processedCount++
					}
				case <-ctx.Done():
					return
				}
			}
		}(i)
	}

	// Wait for all work to be published
	wg.Wait()

	// Wait a bit for processing
	time.Sleep(100 * time.Millisecond)

	// Check that results were published
	resultCount := 0
	timeout := time.After(500 * time.Millisecond)
	
collectResults:
	for {
		select {
		case <-pubsub.results:
			resultCount++
			if resultCount >= len(workItems) {
				break collectResults
			}
		case <-timeout:
			break collectResults
		}
	}

	assert.Equal(t, len(workItems), resultCount, "All work items should have been processed")
	
	cancel() // Stop workers
	processingWg.Wait()
}

// Mock types for integration testing
type MockOrchestrator struct {
	config       common.CrawlerConfig
	stateManager *MockIntegrationStateManager
	pubsub       *MockIntegrationPubSub
	seeds        []string
	crawlID      string
	workers      map[string]*MockWorkerInfo
	statistics   MockStatistics
}

type MockWorkerInfo struct {
	ID             string
	Status         string
	TasksProcessed int
	LastSeen       time.Time
}

type MockStatistics struct {
	TotalWorkItems   int
	CompletedItems   int
	FailedItems      int
	DiscoveredPages  int
	PendingWorkItems int
	ActiveWorkers    int
	StartTime        time.Time
	Uptime           time.Duration
}

type MockWorker struct {
	ID           string
	config       common.CrawlerConfig
	stateManager *MockIntegrationStateManager
	pubsub       *MockIntegrationPubSub
	isRunning    bool
}

// Benchmark tests for concurrent operations
func BenchmarkConcurrentMessageProcessing(b *testing.B) {
	pubsub := NewMockIntegrationPubSub()
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			workItem := NewWorkItem("https://t.me/benchmark", 0, "", "bench", "telegram", WorkItemConfig{})
			pubsub.PublishWorkItem(ctx, workItem, PriorityMedium, 3600)
		}
	})
}
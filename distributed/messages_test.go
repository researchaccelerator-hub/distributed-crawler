package distributed

import (
	"encoding/json"
	"testing"
	"time"
)

func TestWorkItem_Serialization(t *testing.T) {
	now := time.Now()
	deadline := now.Add(1 * time.Hour)
	assignedAt := now.Add(5 * time.Minute)

	workItem := WorkItem{
		ID:         "work-123",
		URL:        "https://example.com/channel",
		Depth:      2,
		CrawlID:    "crawl-456",
		Platform:   "telegram",
		ParentID:   "parent-789",
		RetryCount: 1,
		AssignedTo: "worker-001",
		CreatedAt:  now,
		AssignedAt: &assignedAt,
		Deadline:   &deadline,
		TraceID:    "trace-abc",
		Config: WorkItemConfig{
			StorageRoot:    "/data/crawl",
			Concurrency:    5,
			Timeout:        300,
			MaxComments:    100,
			MaxPosts:       1000,
			MaxDepth:       3,
			MaxPages:       500,
			MinUsers:       10,
			CrawlLabel:     "test-crawl",
			YouTubeAPIKey:  "test-key",
		},
		Metadata: map[string]interface{}{
			"priority": "high",
			"source":   "orchestrator",
		},
	}

	// Serialize to JSON
	jsonData, err := json.Marshal(workItem)
	if err != nil {
		t.Fatalf("Failed to serialize WorkItem: %v", err)
	}

	// Deserialize from JSON
	var deserializedItem WorkItem
	err = json.Unmarshal(jsonData, &deserializedItem)
	if err != nil {
		t.Fatalf("Failed to deserialize WorkItem: %v", err)
	}

	// Verify fields
	if deserializedItem.ID != workItem.ID {
		t.Errorf("ID = %s, want %s", deserializedItem.ID, workItem.ID)
	}
	if deserializedItem.URL != workItem.URL {
		t.Errorf("URL = %s, want %s", deserializedItem.URL, workItem.URL)
	}
	if deserializedItem.Depth != workItem.Depth {
		t.Errorf("Depth = %d, want %d", deserializedItem.Depth, workItem.Depth)
	}
	if deserializedItem.CrawlID != workItem.CrawlID {
		t.Errorf("CrawlID = %s, want %s", deserializedItem.CrawlID, workItem.CrawlID)
	}
	if deserializedItem.Platform != workItem.Platform {
		t.Errorf("Platform = %s, want %s", deserializedItem.Platform, workItem.Platform)
	}
	if deserializedItem.RetryCount != workItem.RetryCount {
		t.Errorf("RetryCount = %d, want %d", deserializedItem.RetryCount, workItem.RetryCount)
	}
	if deserializedItem.AssignedTo != workItem.AssignedTo {
		t.Errorf("AssignedTo = %s, want %s", deserializedItem.AssignedTo, workItem.AssignedTo)
	}
	if deserializedItem.TraceID != workItem.TraceID {
		t.Errorf("TraceID = %s, want %s", deserializedItem.TraceID, workItem.TraceID)
	}

	// Verify config
	if deserializedItem.Config.StorageRoot != workItem.Config.StorageRoot {
		t.Errorf("Config.StorageRoot = %s, want %s",
			deserializedItem.Config.StorageRoot, workItem.Config.StorageRoot)
	}
	if deserializedItem.Config.Concurrency != workItem.Config.Concurrency {
		t.Errorf("Config.Concurrency = %d, want %d",
			deserializedItem.Config.Concurrency, workItem.Config.Concurrency)
	}
}

func TestWorkQueueMessage_Serialization(t *testing.T) {
	now := time.Now()

	msg := WorkQueueMessage{
		MessageType: MessageTypeWorkItem,
		Priority:    PriorityHigh,
		Timestamp:   now,
		TTL:         3600,
		TraceID:     "trace-123",
		WorkItem: WorkItem{
			ID:       "work-456",
			URL:      "https://test.com",
			Depth:    1,
			CrawlID:  "crawl-789",
			Platform: "youtube",
			Config: WorkItemConfig{
				MaxPosts: 500,
			},
		},
	}

	// Serialize
	jsonData, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to serialize WorkQueueMessage: %v", err)
	}

	// Deserialize
	var deserializedMsg WorkQueueMessage
	err = json.Unmarshal(jsonData, &deserializedMsg)
	if err != nil {
		t.Fatalf("Failed to deserialize WorkQueueMessage: %v", err)
	}

	// Verify
	if deserializedMsg.MessageType != msg.MessageType {
		t.Errorf("MessageType = %s, want %s", deserializedMsg.MessageType, msg.MessageType)
	}
	if deserializedMsg.Priority != msg.Priority {
		t.Errorf("Priority = %d, want %d", deserializedMsg.Priority, msg.Priority)
	}
	if deserializedMsg.TTL != msg.TTL {
		t.Errorf("TTL = %d, want %d", deserializedMsg.TTL, msg.TTL)
	}
	if deserializedMsg.WorkItem.ID != msg.WorkItem.ID {
		t.Errorf("WorkItem.ID = %s, want %s", deserializedMsg.WorkItem.ID, msg.WorkItem.ID)
	}
}

func TestWorkResult_Serialization(t *testing.T) {
	result := WorkResult{
		WorkItemID:       "work-123",
		WorkerID:         "worker-001",
		Status:           StatusSuccess,
		ProcessedURL:     "https://example.com",
		MessageCount:     50,
		ProcessingTime:   5 * time.Minute,
		CompletedAt:      time.Now(),
		RetryRecommended: false,
		DiscoveredPages: []DiscoveredPage{
			{
				URL:      "https://example.com/page1",
				ParentID: "work-123",
				Depth:    2,
				Platform: "telegram",
			},
		},
		Metadata: map[string]interface{}{
			"notes": "completed successfully",
		},
	}

	// Serialize
	jsonData, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("Failed to serialize WorkResult: %v", err)
	}

	// Deserialize
	var deserializedResult WorkResult
	err = json.Unmarshal(jsonData, &deserializedResult)
	if err != nil {
		t.Fatalf("Failed to deserialize WorkResult: %v", err)
	}

	// Verify
	if deserializedResult.WorkItemID != result.WorkItemID {
		t.Errorf("WorkItemID = %s, want %s", deserializedResult.WorkItemID, result.WorkItemID)
	}
	if deserializedResult.Status != result.Status {
		t.Errorf("Status = %s, want %s", deserializedResult.Status, result.Status)
	}
	if deserializedResult.MessageCount != result.MessageCount {
		t.Errorf("MessageCount = %d, want %d", deserializedResult.MessageCount, result.MessageCount)
	}
	if len(deserializedResult.DiscoveredPages) != len(result.DiscoveredPages) {
		t.Errorf("DiscoveredPages count = %d, want %d",
			len(deserializedResult.DiscoveredPages), len(result.DiscoveredPages))
	}
}

func TestResultMessage_Serialization(t *testing.T) {
	msg := ResultMessage{
		MessageType: MessageTypeWorkResult,
		Timestamp:   time.Now(),
		TraceID:     "trace-xyz",
		WorkResult: WorkResult{
			WorkItemID:   "work-789",
			WorkerID:     "worker-002",
			Status:       StatusSuccess,
			MessageCount: 100,
		},
		DiscoveredPages: []DiscoveredPage{
			{URL: "https://page1.com", Depth: 3},
			{URL: "https://page2.com", Depth: 3},
		},
	}

	// Serialize
	jsonData, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to serialize ResultMessage: %v", err)
	}

	// Deserialize
	var deserializedMsg ResultMessage
	err = json.Unmarshal(jsonData, &deserializedMsg)
	if err != nil {
		t.Fatalf("Failed to deserialize ResultMessage: %v", err)
	}

	// Verify
	if deserializedMsg.MessageType != msg.MessageType {
		t.Errorf("MessageType = %s, want %s", deserializedMsg.MessageType, msg.MessageType)
	}
	if deserializedMsg.TraceID != msg.TraceID {
		t.Errorf("TraceID = %s, want %s", deserializedMsg.TraceID, msg.TraceID)
	}
	if len(deserializedMsg.DiscoveredPages) != len(msg.DiscoveredPages) {
		t.Errorf("DiscoveredPages count = %d, want %d",
			len(deserializedMsg.DiscoveredPages), len(msg.DiscoveredPages))
	}
}

func TestStatusMessage_Serialization(t *testing.T) {
	currentWork := "work-123"
	msg := StatusMessage{
		MessageType:    MessageTypeHeartbeat,
		WorkerID:       "worker-003",
		Status:         WorkerStatusBusy,
		CurrentWork:    &currentWork,
		QueueLength:    5,
		TasksProcessed: 100,
		TasksSuccess:   95,
		TasksError:     5,
		Timestamp:      time.Now(),
		ResourceUsage: map[string]interface{}{
			"cpu":    75.5,
			"memory": 1024,
		},
	}

	// Serialize
	jsonData, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to serialize StatusMessage: %v", err)
	}

	// Deserialize
	var deserializedMsg StatusMessage
	err = json.Unmarshal(jsonData, &deserializedMsg)
	if err != nil {
		t.Fatalf("Failed to deserialize StatusMessage: %v", err)
	}

	// Verify
	if deserializedMsg.MessageType != msg.MessageType {
		t.Errorf("MessageType = %s, want %s", deserializedMsg.MessageType, msg.MessageType)
	}
	if deserializedMsg.WorkerID != msg.WorkerID {
		t.Errorf("WorkerID = %s, want %s", deserializedMsg.WorkerID, msg.WorkerID)
	}
	if deserializedMsg.Status != msg.Status {
		t.Errorf("Status = %s, want %s", deserializedMsg.Status, msg.Status)
	}
	if deserializedMsg.TasksProcessed != msg.TasksProcessed {
		t.Errorf("TasksProcessed = %d, want %d", deserializedMsg.TasksProcessed, msg.TasksProcessed)
	}
}

func TestDiscoveredPage_Serialization(t *testing.T) {
	page := DiscoveredPage{
		URL:      "https://discovered.com/channel",
		ParentID: "parent-abc",
		Depth:    3,
		Platform: "bluesky",
	}

	// Serialize
	jsonData, err := json.Marshal(page)
	if err != nil {
		t.Fatalf("Failed to serialize DiscoveredPage: %v", err)
	}

	// Deserialize
	var deserializedPage DiscoveredPage
	err = json.Unmarshal(jsonData, &deserializedPage)
	if err != nil {
		t.Fatalf("Failed to deserialize DiscoveredPage: %v", err)
	}

	// Verify
	if deserializedPage.URL != page.URL {
		t.Errorf("URL = %s, want %s", deserializedPage.URL, page.URL)
	}
	if deserializedPage.Depth != page.Depth {
		t.Errorf("Depth = %d, want %d", deserializedPage.Depth, page.Depth)
	}
	if deserializedPage.Platform != page.Platform {
		t.Errorf("Platform = %s, want %s", deserializedPage.Platform, page.Platform)
	}
}

func TestMessageTypeConstants(t *testing.T) {
	tests := []struct {
		name     string
		constant string
		expected string
	}{
		{"work item", MessageTypeWorkItem, "work_item"},
		{"poison pill", MessageTypePoisonPill, "poison_pill"},
		{"work result", MessageTypeWorkResult, "work_result"},
		{"discovered pages", MessageTypeDiscoveredPages, "discovered_pages"},
		{"heartbeat", MessageTypeHeartbeat, "heartbeat"},
		{"worker started", MessageTypeWorkerStarted, "worker_started"},
		{"worker stopping", MessageTypeWorkerStopping, "worker_stopping"},
		{"pause", MessageTypePause, "pause"},
		{"resume", MessageTypeResume, "resume"},
		{"stop", MessageTypeStop, "stop"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.constant != tt.expected {
				t.Errorf("Constant = %s, want %s", tt.constant, tt.expected)
			}
		})
	}
}

func TestStatusConstants(t *testing.T) {
	tests := []struct {
		name     string
		constant string
		expected string
	}{
		{"success", StatusSuccess, "success"},
		{"error", StatusError, "error"},
		{"partial", StatusPartial, "partial"},
		{"retry", StatusRetry, "retry"},
		{"worker active", WorkerStatusActive, "active"},
		{"worker idle", WorkerStatusIdle, "idle"},
		{"worker busy", WorkerStatusBusy, "busy"},
		{"worker error", WorkerStatusError, "error"},
		{"worker offline", WorkerStatusOffline, "offline"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.constant != tt.expected {
				t.Errorf("Constant = %s, want %s", tt.constant, tt.expected)
			}
		})
	}
}

func TestPriorityConstants(t *testing.T) {
	if PriorityHigh != 1 {
		t.Errorf("PriorityHigh = %d, want 1", PriorityHigh)
	}
	if PriorityMedium != 3 {
		t.Errorf("PriorityMedium = %d, want 3", PriorityMedium)
	}
	if PriorityLow != 5 {
		t.Errorf("PriorityLow = %d, want 5", PriorityLow)
	}
}

func TestTopicConstants(t *testing.T) {
	tests := []struct {
		name     string
		constant string
		expected string
	}{
		{"work queue", TopicWorkQueue, "crawl-work-queue"},
		{"results", TopicResults, "crawl-results"},
		{"worker status", TopicWorkerStatus, "worker-status"},
		{"orchestrator", TopicOrchestrator, "orchestrator-commands"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.constant != tt.expected {
				t.Errorf("Constant = %s, want %s", tt.constant, tt.expected)
			}
		})
	}
}

func TestWorkItemConfig_Serialization(t *testing.T) {
	now := time.Now()
	config := WorkItemConfig{
		StorageRoot:       "/data/test",
		Concurrency:       10,
		Timeout:           600,
		MinPostDate:       now.Add(-24 * time.Hour),
		PostRecency:       now.Add(-1 * time.Hour),
		DateBetweenMin:    now.Add(-48 * time.Hour),
		DateBetweenMax:    now,
		SampleSize:        100,
		MaxComments:       500,
		MaxPosts:          10000,
		MaxDepth:          5,
		MaxPages:          1000,
		MinUsers:          50,
		CrawlLabel:        "integration-test",
		SkipMediaDownload: true,
		YouTubeAPIKey:     "test-api-key-789",
	}

	// Serialize
	jsonData, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("Failed to serialize WorkItemConfig: %v", err)
	}

	// Deserialize
	var deserializedConfig WorkItemConfig
	err = json.Unmarshal(jsonData, &deserializedConfig)
	if err != nil {
		t.Fatalf("Failed to deserialize WorkItemConfig: %v", err)
	}

	// Verify
	if deserializedConfig.StorageRoot != config.StorageRoot {
		t.Errorf("StorageRoot = %s, want %s", deserializedConfig.StorageRoot, config.StorageRoot)
	}
	if deserializedConfig.Concurrency != config.Concurrency {
		t.Errorf("Concurrency = %d, want %d", deserializedConfig.Concurrency, config.Concurrency)
	}
	if deserializedConfig.MaxPosts != config.MaxPosts {
		t.Errorf("MaxPosts = %d, want %d", deserializedConfig.MaxPosts, config.MaxPosts)
	}
	if deserializedConfig.SkipMediaDownload != config.SkipMediaDownload {
		t.Errorf("SkipMediaDownload = %v, want %v", deserializedConfig.SkipMediaDownload, config.SkipMediaDownload)
	}
}

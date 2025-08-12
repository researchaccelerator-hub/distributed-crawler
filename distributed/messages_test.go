package distributed

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewWorkItem(t *testing.T) {
	config := WorkItemConfig{
		StorageRoot: "/tmp/test",
		Concurrency: 2,
		Timeout:     30,
	}

	item := NewWorkItem("test-url", 1, "test-parent", "test-crawl", "telegram", config)

	assert.NotEmpty(t, item.ID)
	assert.Equal(t, "test-url", item.URL)
	assert.Equal(t, 1, item.Depth)
	assert.Equal(t, "test-parent", item.ParentID)
	assert.Equal(t, "test-crawl", item.CrawlID)
	assert.Equal(t, "telegram", item.Platform)
	assert.Equal(t, config, item.Config)
	assert.WithinDuration(t, time.Now(), item.CreatedAt, time.Second)
	assert.NotEmpty(t, item.TraceID)
}

func TestNewWorkQueueMessage(t *testing.T) {
	item := WorkItem{
		ID:       "test-id",
		URL:      "test-url",
		Platform: "telegram",
	}

	message := NewWorkQueueMessage(item, 5, 3600)

	assert.Equal(t, MessageTypeWorkItem, message.MessageType)
	assert.Equal(t, item, message.WorkItem)
	assert.Equal(t, 5, message.Priority)
	assert.Equal(t, 3600, message.TTL)
	assert.WithinDuration(t, time.Now(), message.Timestamp, time.Second)
	assert.NotEmpty(t, message.TraceID)
}

func TestNewResultMessage(t *testing.T) {
	result := WorkResult{
		WorkItemID:   "test-work-id",
		WorkerID:     "test-worker",
		Status:       StatusSuccess,
		ProcessedURL: "test-url",
	}

	discoveredPages := []DiscoveredPage{
		{
			URL:      "discovered-url-1",
			ParentID: "test-work-id",
			Depth:    2,
			Platform: "telegram",
		},
		{
			URL:      "discovered-url-2",
			ParentID: "test-work-id",
			Depth:    2,
			Platform: "telegram",
		},
	}

	message := NewResultMessage(result, discoveredPages)

	assert.Equal(t, MessageTypeWorkResult, message.MessageType)
	assert.Equal(t, result, message.WorkResult)
	assert.Equal(t, discoveredPages, message.DiscoveredPages)
	assert.WithinDuration(t, time.Now(), message.Timestamp, time.Second)
	assert.NotEmpty(t, message.TraceID)
}

func TestNewStatusMessage(t *testing.T) {
	uptime := 5 * time.Minute

	message := NewStatusMessage(
		"worker-1",
		MessageTypeHeartbeat,
		WorkerStatusIdle,
		10,
		8,
		2,
		uptime,
	)

	assert.Equal(t, "worker-1", message.WorkerID)
	assert.Equal(t, MessageTypeHeartbeat, message.MessageType)
	assert.Equal(t, WorkerStatusIdle, message.Status)
	assert.Equal(t, 10, message.TasksProcessed)
	assert.Equal(t, 8, message.TasksSuccess)
	assert.Equal(t, 2, message.TasksError)
	assert.Equal(t, uptime, message.Uptime)
	assert.WithinDuration(t, time.Now(), message.Timestamp, time.Second)
	assert.NotEmpty(t, message.TraceID)
}

func TestWorkItemValidation(t *testing.T) {
	tests := []struct {
		name    string
		item    WorkItem
		wantErr bool
	}{
		{
			name: "valid work item",
			item: WorkItem{
				ID:       "test-id",
				URL:      "https://t.me/testchannel",
				Platform: "telegram",
				CrawlID:  "test-crawl",
				Config: WorkItemConfig{
					StorageRoot: "/tmp/test",
				},
			},
			wantErr: false,
		},
		{
			name: "missing ID",
			item: WorkItem{
				URL:      "https://t.me/testchannel",
				Platform: "telegram",
				CrawlID:  "test-crawl",
			},
			wantErr: true,
		},
		{
			name: "missing URL",
			item: WorkItem{
				ID:       "test-id",
				Platform: "telegram",
				CrawlID:  "test-crawl",
			},
			wantErr: true,
		},
		{
			name: "missing platform",
			item: WorkItem{
				ID:      "test-id",
				URL:     "https://t.me/testchannel",
				CrawlID: "test-crawl",
			},
			wantErr: true,
		},
		{
			name: "unsupported platform",
			item: WorkItem{
				ID:       "test-id",
				URL:      "https://example.com",
				Platform: "unsupported",
				CrawlID:  "test-crawl",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.item.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestWorkResultValidation(t *testing.T) {
	tests := []struct {
		name    string
		result  WorkResult
		wantErr bool
	}{
		{
			name: "valid success result",
			result: WorkResult{
				WorkItemID:   "test-work-id",
				WorkerID:     "test-worker",
				Status:       StatusSuccess,
				ProcessedURL: "https://t.me/testchannel",
			},
			wantErr: false,
		},
		{
			name: "valid error result",
			result: WorkResult{
				WorkItemID:   "test-work-id",
				WorkerID:     "test-worker",
				Status:       StatusError,
				ProcessedURL: "https://t.me/testchannel",
				Error:        "connection failed",
			},
			wantErr: false,
		},
		{
			name: "missing work item ID",
			result: WorkResult{
				WorkerID:     "test-worker",
				Status:       StatusSuccess,
				ProcessedURL: "https://t.me/testchannel",
			},
			wantErr: true,
		},
		{
			name: "missing worker ID",
			result: WorkResult{
				WorkItemID:   "test-work-id",
				Status:       StatusSuccess,
				ProcessedURL: "https://t.me/testchannel",
			},
			wantErr: true,
		},
		{
			name: "invalid status",
			result: WorkResult{
				WorkItemID:   "test-work-id",
				WorkerID:     "test-worker",
				Status:       "invalid",
				ProcessedURL: "https://t.me/testchannel",
			},
			wantErr: true,
		},
		{
			name: "error status without error message",
			result: WorkResult{
				WorkItemID:   "test-work-id",
				WorkerID:     "test-worker",
				Status:       StatusError,
				ProcessedURL: "https://t.me/testchannel",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.result.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDiscoveredPageValidation(t *testing.T) {
	tests := []struct {
		name    string
		page    DiscoveredPage
		wantErr bool
	}{
		{
			name: "valid discovered page",
			page: DiscoveredPage{
				URL:      "https://t.me/newchannel",
				ParentID: "parent-work-id",
				Depth:    2,
				Platform: "telegram",
			},
			wantErr: false,
		},
		{
			name: "missing URL",
			page: DiscoveredPage{
				ParentID: "parent-work-id",
				Depth:    2,
				Platform: "telegram",
			},
			wantErr: true,
		},
		{
			name: "missing platform",
			page: DiscoveredPage{
				URL:      "https://t.me/newchannel",
				ParentID: "parent-work-id",
				Depth:    2,
			},
			wantErr: true,
		},
		{
			name: "negative depth",
			page: DiscoveredPage{
				URL:      "https://t.me/newchannel",
				ParentID: "parent-work-id",
				Depth:    -1,
				Platform: "telegram",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.page.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestStatusMessageValidation(t *testing.T) {
	tests := []struct {
		name    string
		status  StatusMessage
		wantErr bool
	}{
		{
			name: "valid status message",
			status: StatusMessage{
				WorkerID:    "worker-1",
				MessageType: MessageTypeHeartbeat,
				Status:      WorkerStatusIdle,
			},
			wantErr: false,
		},
		{
			name: "missing worker ID",
			status: StatusMessage{
				MessageType: MessageTypeHeartbeat,
				Status:      WorkerStatusIdle,
			},
			wantErr: true,
		},
		{
			name: "invalid message type",
			status: StatusMessage{
				WorkerID:    "worker-1",
				MessageType: "invalid",
				Status:      WorkerStatusIdle,
			},
			wantErr: true,
		},
		{
			name: "invalid status",
			status: StatusMessage{
				WorkerID:    "worker-1",
				MessageType: MessageTypeHeartbeat,
				Status:      "invalid",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.status.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGenerateTraceID(t *testing.T) {
	id1 := generateTraceID()
	id2 := generateTraceID()

	assert.NotEmpty(t, id1)
	assert.NotEmpty(t, id2)
	assert.NotEqual(t, id1, id2)
	assert.True(t, len(id1) > 16) // Should be trace_YYYYMMDDHHMMSS_XXXXXXXX format
	assert.True(t, len(id2) > 16)
}

func TestConstants(t *testing.T) {
	// Test that all constants are defined and not empty
	assert.NotEmpty(t, TopicWorkQueue)
	assert.NotEmpty(t, TopicResults)
	assert.NotEmpty(t, TopicWorkerStatus)
	assert.NotEmpty(t, TopicOrchestrator)

	assert.NotEmpty(t, MessageTypeWorkItem)
	assert.NotEmpty(t, MessageTypeWorkResult)
	assert.NotEmpty(t, MessageTypeHeartbeat)
	assert.NotEmpty(t, MessageTypeWorkerStarted)
	assert.NotEmpty(t, MessageTypeWorkerStopping)

	assert.NotEmpty(t, StatusSuccess)
	assert.NotEmpty(t, StatusError)
	assert.NotEmpty(t, StatusRetry)

	assert.NotEmpty(t, WorkerStatusIdle)
	assert.NotEmpty(t, WorkerStatusBusy)
	assert.NotEmpty(t, WorkerStatusActive)
	assert.NotEmpty(t, WorkerStatusOffline)
}
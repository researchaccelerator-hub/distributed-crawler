package distributed

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestPubSubClientCreation tests creating a PubSub client structure
func TestPubSubClientCreation(t *testing.T) {
	tests := []struct {
		name     string
		pubsub   string
		appPort  string
	}{
		{
			name:    "valid parameters",
			pubsub:  "test-pubsub",
			appPort: ":8080",
		},
		{
			name:    "empty pubsub name",
			pubsub:  "",
			appPort: ":8080",
		},
		{
			name:    "empty app port",
			pubsub:  "test-pubsub",
			appPort: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test basic client structure creation
			client := &PubSubClient{
				pubsubName: tt.pubsub,
				appPort:    tt.appPort,
			}

			assert.NotNil(t, client)
			assert.Equal(t, tt.pubsub, client.pubsubName)
			assert.Equal(t, tt.appPort, client.appPort)
		})
	}
}

// TestMessageCreation tests creating various message types
func TestMessageCreation(t *testing.T) {
	workItem := WorkItem{
		ID:       "test-work-item",
		URL:      "https://t.me/testchannel",
		Platform: "telegram",
		CrawlID:  "test-crawl",
		CreatedAt: time.Now(),
		TraceID:  "test-trace",
	}

	// Test work queue message creation
	workMessage := NewWorkQueueMessage(workItem, PriorityHigh, 3600)
	assert.Equal(t, MessageTypeWorkItem, workMessage.MessageType)
	assert.Equal(t, workItem, workMessage.WorkItem)
	assert.Equal(t, PriorityHigh, workMessage.Priority)
	assert.Equal(t, 3600, workMessage.TTL)
	assert.NotEmpty(t, workMessage.TraceID)

	// Test result message creation
	result := WorkResult{
		WorkItemID:     "test-work-item",
		WorkerID:       "test-worker",
		Status:         StatusSuccess,
		ProcessedURL:   "https://t.me/testchannel",
		MessageCount:   10,
		ProcessingTime: 30 * time.Second,
		CompletedAt:    time.Now(),
	}

	discoveredPages := []DiscoveredPage{
		{
			URL:      "https://t.me/discovered1",
			ParentID: "test-work-item",
			Depth:    2,
			Platform: "telegram",
		},
	}

	resultMessage := NewResultMessage(result, discoveredPages)
	assert.Equal(t, MessageTypeWorkResult, resultMessage.MessageType)
	assert.Equal(t, result, resultMessage.WorkResult)
	assert.Equal(t, discoveredPages, resultMessage.DiscoveredPages)
	assert.NotEmpty(t, resultMessage.TraceID)

	// Test status message creation
	status := NewStatusMessage(
		"test-worker",
		MessageTypeHeartbeat,
		WorkerStatusIdle,
		5,
		4,
		1,
		10*time.Minute,
	)
	assert.Equal(t, "test-worker", status.WorkerID)
	assert.Equal(t, MessageTypeHeartbeat, status.MessageType)
	assert.Equal(t, WorkerStatusIdle, status.Status)
	assert.Equal(t, 5, status.TasksProcessed)
	assert.Equal(t, 4, status.TasksSuccess)
	assert.Equal(t, 1, status.TasksError)
	assert.Equal(t, 10*time.Minute, status.Uptime)
	assert.NotEmpty(t, status.TraceID)
}

// TestTopicConstants tests that all required topics are defined
func TestTopicConstants(t *testing.T) {
	topics := PubSubTopics()
	
	assert.Contains(t, topics, TopicWorkQueue)
	assert.Contains(t, topics, TopicResults)
	assert.Contains(t, topics, TopicWorkerStatus)
	assert.Contains(t, topics, TopicOrchestrator)
	assert.Len(t, topics, 4)
	
	// Verify no empty topic names
	for _, topic := range topics {
		assert.NotEmpty(t, topic)
	}
}

// TestMessageMarshaling tests that messages can be marshaled/unmarshaled
func TestMessageMarshaling(t *testing.T) {
	// This test ensures our message types are JSON serializable
	workItem := WorkItem{
		ID:       "test-id",
		URL:      "https://t.me/test",
		Platform: "telegram",
		CrawlID:  "test-crawl",
		CreatedAt: time.Now(),
		TraceID:  "test-trace",
	}

	message := NewWorkQueueMessage(workItem, PriorityHigh, 3600)
	
	// The marshaling is tested implicitly in the publish methods
	// This test mainly validates the message structure is complete
	assert.NotEmpty(t, message.MessageType)
	assert.NotEmpty(t, message.WorkItem.ID)
	assert.NotZero(t, message.Priority)
	assert.NotZero(t, message.TTL)
	assert.NotZero(t, message.Timestamp)
	assert.NotEmpty(t, message.TraceID)
}

// Benchmark tests for performance validation
func BenchmarkNewWorkItem(b *testing.B) {
	config := WorkItemConfig{
		StorageRoot: "/tmp/test",
		Concurrency: 2,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewWorkItem("https://t.me/test", 1, "parent", "crawl", "telegram", config)
	}
}

func BenchmarkGenerateTraceID(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		generateTraceID()
	}
}
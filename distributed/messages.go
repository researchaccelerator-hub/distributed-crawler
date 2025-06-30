// Package distributed provides message types and contracts for distributed crawling
package distributed

import (
	"fmt"
	"math/rand"
	"time"
)

// Message Types
const (
	// Work Queue Messages
	MessageTypeWorkItem    = "work_item"
	MessageTypePoisonPill  = "poison_pill"
	
	// Result Messages
	MessageTypeWorkResult       = "work_result"
	MessageTypeDiscoveredPages  = "discovered_pages"
	
	// Status Messages
	MessageTypeHeartbeat      = "heartbeat"
	MessageTypeWorkerStarted  = "worker_started"
	MessageTypeWorkerStopping = "worker_stopping"
	
	// Control Messages
	MessageTypePause  = "pause"
	MessageTypeResume = "resume"
	MessageTypeStop   = "stop"
)

// Status Values
const (
	StatusSuccess = "success"
	StatusError   = "error"
	StatusPartial = "partial"
	StatusRetry   = "retry"
	
	WorkerStatusActive  = "active"
	WorkerStatusIdle    = "idle"
	WorkerStatusBusy    = "busy"
	WorkerStatusError   = "error"
	WorkerStatusOffline = "offline"
)

// Priority Levels
const (
	PriorityHigh   = 1
	PriorityMedium = 3
	PriorityLow    = 5
)

// Topic Names (should match config/distributed.go defaults)
const (
	TopicWorkQueue       = "crawl-work-queue"
	TopicResults         = "crawl-results"
	TopicWorkerStatus    = "worker-status"
	TopicOrchestrator    = "orchestrator-commands"
)

// WorkQueueMessage represents a message in the work queue
type WorkQueueMessage struct {
	MessageType string    `json:"message_type"` // "work_item", "poison_pill"
	WorkItem    WorkItem  `json:"work_item"`
	Priority    int       `json:"priority"`     // 1=high, 5=low
	Timestamp   time.Time `json:"timestamp"`
	TTL         int       `json:"ttl_seconds"`  // Time to live
	TraceID     string    `json:"trace_id,omitempty"` // For distributed tracing
}

// WorkItem represents a single crawl task to be distributed to workers
type WorkItem struct {
	ID          string                 `json:"id"`
	URL         string                 `json:"url"`
	Depth       int                    `json:"depth"`
	CrawlID     string                 `json:"crawl_id"`
	Platform    string                 `json:"platform"`
	Config      WorkItemConfig         `json:"config"`
	ParentID    string                 `json:"parent_id,omitempty"`
	RetryCount  int                    `json:"retry_count"`
	AssignedTo  string                 `json:"assigned_to,omitempty"`
	CreatedAt   time.Time              `json:"created_at"`
	AssignedAt  *time.Time             `json:"assigned_at,omitempty"`
	Deadline    *time.Time             `json:"deadline,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
	TraceID     string                 `json:"trace_id,omitempty"`
}

// WorkItemConfig contains crawl-specific configuration for a work item
type WorkItemConfig struct {
	StorageRoot       string    `json:"storage_root"`
	Concurrency       int       `json:"concurrency"`
	Timeout           int       `json:"timeout"`
	MinPostDate       time.Time `json:"min_post_date,omitempty"`
	PostRecency       time.Time `json:"post_recency,omitempty"`
	DateBetweenMin    time.Time `json:"date_between_min,omitempty"`
	DateBetweenMax    time.Time `json:"date_between_max,omitempty"`
	SampleSize        int       `json:"sample_size,omitempty"`
	MaxComments       int       `json:"max_comments"`
	MaxPosts          int       `json:"max_posts"`
	MaxDepth          int       `json:"max_depth"`
	MaxPages          int       `json:"max_pages"`
	MinUsers          int       `json:"min_users"`
	CrawlLabel        string    `json:"crawl_label,omitempty"`
	SkipMediaDownload bool      `json:"skip_media_download"`
	YouTubeAPIKey     string    `json:"youtube_api_key,omitempty"`
}

// ResultMessage represents a message containing work results
type ResultMessage struct {
	MessageType     string           `json:"message_type"` // "work_result", "discovered_pages"
	WorkResult      WorkResult       `json:"work_result"`
	DiscoveredPages []DiscoveredPage `json:"discovered_pages"`
	Timestamp       time.Time        `json:"timestamp"`
	TraceID         string           `json:"trace_id,omitempty"`
}

// WorkResult represents the result of a completed work item
type WorkResult struct {
	WorkItemID        string                 `json:"work_item_id"`
	WorkerID          string                 `json:"worker_id"`
	Status            string                 `json:"status"` // success, error, partial
	ProcessedURL      string                 `json:"processed_url"`
	MessageCount      int                    `json:"message_count"`
	DiscoveredPages   []DiscoveredPage       `json:"discovered_pages"`
	Error             string                 `json:"error,omitempty"`
	ProcessingTime    time.Duration          `json:"processing_time"`
	Metadata          map[string]interface{} `json:"metadata"`
	CompletedAt       time.Time              `json:"completed_at"`
	RetryRecommended  bool                   `json:"retry_recommended,omitempty"`
}

// DiscoveredPage represents a newly discovered page from crawling
type DiscoveredPage struct {
	URL      string `json:"url"`
	ParentID string `json:"parent_id"`
	Depth    int    `json:"depth"`
	Platform string `json:"platform"`
}

// StatusMessage represents worker status for heartbeats
type StatusMessage struct {
	MessageType    string                 `json:"message_type"` // "heartbeat", "worker_started", "worker_stopping"
	WorkerID       string                 `json:"worker_id"`
	Status         string                 `json:"status"`       // "active", "idle", "busy", "error"
	CurrentWork    *string                `json:"current_work,omitempty"`
	QueueLength    int                    `json:"queue_length"`
	ResourceUsage  map[string]interface{} `json:"resource_usage"`
	TasksProcessed int                    `json:"tasks_processed"`
	TasksSuccess   int                    `json:"tasks_success"`
	TasksError     int                    `json:"tasks_error"`
	Timestamp      time.Time              `json:"timestamp"`
	Uptime         time.Duration          `json:"uptime"`
	TraceID        string                 `json:"trace_id,omitempty"`
}

// ControlMessage represents control commands for orchestrator/workers
type ControlMessage struct {
	MessageType string                 `json:"message_type"` // "pause", "resume", "stop"
	Command     string                 `json:"command"`
	TargetID    string                 `json:"target_id,omitempty"` // Specific worker ID or "all"
	Parameters  map[string]interface{} `json:"parameters,omitempty"`
	Timestamp   time.Time              `json:"timestamp"`
	TraceID     string                 `json:"trace_id,omitempty"`
}

// PubSubTopics returns all topic names for subscription setup
func PubSubTopics() []string {
	return []string{
		TopicWorkQueue,
		TopicResults,
		TopicWorkerStatus,
		TopicOrchestrator,
	}
}

// NewWorkItem creates a new work item with default values
func NewWorkItem(url string, depth int, parentID, crawlID, platform string, config WorkItemConfig) WorkItem {
	return WorkItem{
		ID:         generateWorkItemID(),
		URL:        url,
		Depth:      depth,
		ParentID:   parentID,
		CrawlID:    crawlID,
		Platform:   platform,
		Config:     config,
		RetryCount: 0,
		CreatedAt:  time.Now(),
		TraceID:    generateTraceID(),
	}
}

// NewWorkQueueMessage creates a new work queue message
func NewWorkQueueMessage(item WorkItem, priority int, ttlSeconds int) WorkQueueMessage {
	return WorkQueueMessage{
		MessageType: MessageTypeWorkItem,
		WorkItem:    item,
		Priority:    priority,
		Timestamp:   time.Now(),
		TTL:         ttlSeconds,
		TraceID:     generateTraceID(),
	}
}

// NewStatusMessage creates a new status message
func NewStatusMessage(workerID, messageType, status string, tasksProcessed, tasksSuccess, tasksError int, uptime time.Duration) StatusMessage {
	return StatusMessage{
		MessageType:    messageType,
		WorkerID:       workerID,
		Status:         status,
		TasksProcessed: tasksProcessed,
		TasksSuccess:   tasksSuccess,
		TasksError:     tasksError,
		Timestamp:      time.Now(),
		Uptime:         uptime,
		TraceID:        generateTraceID(),
	}
}

// NewResultMessage creates a new result message
func NewResultMessage(result WorkResult, discoveredPages []DiscoveredPage) ResultMessage {
	return ResultMessage{
		MessageType:     MessageTypeWorkResult,
		WorkResult:      result,
		DiscoveredPages: discoveredPages,
		Timestamp:       time.Now(),
		TraceID:         generateTraceID(),
	}
}

// generateWorkItemID generates a unique work item ID
func generateWorkItemID() string {
	// Simple implementation for now - in production you might want UUID
	return "work_" + time.Now().Format("20060102150405") + "_" + generateRandomString(6)
}

// generateTraceID generates a trace ID for distributed tracing
func generateTraceID() string {
	return "trace_" + time.Now().Format("20060102150405") + "_" + generateRandomString(8)
}

// generateRandomString generates a random alphanumeric string
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rng.Intn(len(charset))]
	}
	return string(b)
}

// Validate validates a WorkItem
func (w *WorkItem) Validate() error {
	if w.ID == "" {
		return fmt.Errorf("work item ID cannot be empty")
	}
	if w.URL == "" {
		return fmt.Errorf("work item URL cannot be empty")
	}
	if w.Platform == "" {
		return fmt.Errorf("work item platform cannot be empty")
	}
	if w.Platform != "telegram" && w.Platform != "youtube" {
		return fmt.Errorf("unsupported platform: %s", w.Platform)
	}
	return nil
}

// Validate validates a WorkResult
func (w *WorkResult) Validate() error {
	if w.WorkItemID == "" {
		return fmt.Errorf("work result WorkItemID cannot be empty")
	}
	if w.WorkerID == "" {
		return fmt.Errorf("work result WorkerID cannot be empty")
	}
	if w.Status != StatusSuccess && w.Status != StatusError && w.Status != StatusPartial && w.Status != StatusRetry {
		return fmt.Errorf("invalid status: %s", w.Status)
	}
	if w.Status == StatusError && w.Error == "" {
		return fmt.Errorf("error status requires error message")
	}
	return nil
}

// Validate validates a DiscoveredPage
func (d *DiscoveredPage) Validate() error {
	if d.URL == "" {
		return fmt.Errorf("discovered page URL cannot be empty")
	}
	if d.Platform == "" {
		return fmt.Errorf("discovered page platform cannot be empty")
	}
	if d.Depth < 0 {
		return fmt.Errorf("discovered page depth cannot be negative")
	}
	return nil
}

// Validate validates a StatusMessage
func (s *StatusMessage) Validate() error {
	if s.WorkerID == "" {
		return fmt.Errorf("status message WorkerID cannot be empty")
	}
	
	validMessageTypes := []string{MessageTypeHeartbeat, MessageTypeWorkerStarted, MessageTypeWorkerStopping}
	valid := false
	for _, msgType := range validMessageTypes {
		if s.MessageType == msgType {
			valid = true
			break
		}
	}
	if !valid {
		return fmt.Errorf("invalid message type: %s", s.MessageType)
	}
	
	validStatuses := []string{WorkerStatusActive, WorkerStatusIdle, WorkerStatusBusy, WorkerStatusError, WorkerStatusOffline}
	valid = false
	for _, status := range validStatuses {
		if s.Status == status {
			valid = true
			break
		}
	}
	if !valid {
		return fmt.Errorf("invalid status: %s", s.Status)
	}
	
	return nil
}
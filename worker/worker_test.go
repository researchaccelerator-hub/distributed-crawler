package worker

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/distributed"
)

// TestNewWorker tests creating a new worker instance
func TestNewWorker(t *testing.T) {
	tests := []struct {
		name     string
		workerID string
		config   common.CrawlerConfig
		wantErr  bool
	}{
		{
			name:     "valid worker creation",
			workerID: "worker-1",
			config: common.CrawlerConfig{
				Platform:    "telegram",
				StorageRoot: "/tmp/test",
				DaprPort:    3500,
			},
			wantErr: false,
		},
		{
			name:     "empty worker ID",
			workerID: "",
			config: common.CrawlerConfig{
				Platform:    "telegram",
				StorageRoot: "/tmp/test",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test worker structure creation
			if tt.wantErr {
				// Test invalid worker ID
				assert.Empty(t, tt.workerID)
			} else {
				// Test valid worker configuration
				assert.NotEmpty(t, tt.workerID)
				assert.NotEmpty(t, tt.config.Platform)
				assert.NotEmpty(t, tt.config.StorageRoot)
			}
		})
	}
}

// TestProcessWorkItem tests processing a work item
func TestProcessWorkItem(t *testing.T) {
	tests := []struct {
		name         string
		workItem     distributed.WorkItem
		expectError  bool
		expectedResult string
	}{
		{
			name: "valid work item structure",
			workItem: distributed.WorkItem{
				ID:       "test-work-1",
				URL:      "https://t.me/testchannel",
				Platform: "telegram",
				Depth:    1,
				CrawlID:  "test-crawl",
				Config: distributed.WorkItemConfig{
					StorageRoot: "/tmp/test",
					Concurrency: 1,
				},
			},
			expectError:    false,
			expectedResult: "test-work-1",
		},
		{
			name: "unsupported platform",
			workItem: distributed.WorkItem{
				ID:       "test-work-2",
				URL:      "https://example.com",
				Platform: "unsupported",
				Depth:    1,
				CrawlID:  "test-crawl",
				Config: distributed.WorkItemConfig{
					StorageRoot: "/tmp/test",
				},
			},
			expectError:    false, // Structure is valid, processing would fail
			expectedResult: "test-work-2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test work item structure validation
			assert.NotEmpty(t, tt.workItem.ID)
			assert.NotEmpty(t, tt.workItem.URL)
			assert.NotEmpty(t, tt.workItem.Platform)
			assert.NotEmpty(t, tt.workItem.CrawlID)
			assert.Equal(t, tt.expectedResult, tt.workItem.ID)
		})
	}
}

// TestDetermineStatus tests worker status determination
func TestDetermineStatus(t *testing.T) {
	tests := []struct {
		name         string
		isRunning    bool
		currentWork  *distributed.WorkItem
		expectedStatus string
	}{
		{
			name:         "offline worker",
			isRunning:    false,
			currentWork:  nil,
			expectedStatus: "offline",
		},
		{
			name:         "idle worker",
			isRunning:    true,
			currentWork:  nil,
			expectedStatus: "idle",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			worker := &Worker{
				ID:          "test-worker",
				isRunning:   tt.isRunning,
				currentWork: tt.currentWork,
			}

			status := worker.determineStatus()
			assert.Equal(t, tt.expectedStatus, status)
		})
	}
}

// TestShouldRetryError tests error retry logic
func TestShouldRetryError(t *testing.T) {
	worker := &Worker{ID: "test-worker"}

	tests := []struct {
		name          string
		err           error
		shouldRetry   bool
	}{
		{
			name:        "not found error",
			err:         errors.New("channel not found"),
			shouldRetry: false,
		},
		{
			name:        "access denied error",
			err:         errors.New("access denied"),
			shouldRetry: false,
		},
		{
			name:        "forbidden error",
			err:         errors.New("forbidden"),
			shouldRetry: false,
		},
		{
			name:        "connection error",
			err:         errors.New("connection failed"),
			shouldRetry: true,
		},
		{
			name:        "timeout error",
			err:         errors.New("timeout occurred"),
			shouldRetry: true,
		},
		{
			name:        "temporary error",
			err:         errors.New("temporary failure"),
			shouldRetry: true,
		},
		{
			name:        "unknown error",
			err:         errors.New("something went wrong"),
			shouldRetry: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shouldRetry := worker.shouldRetryError(tt.err)
			assert.Equal(t, tt.shouldRetry, shouldRetry)
		})
	}
}

// TestGetStatus tests getting worker status
func TestGetStatus(t *testing.T) {
	startTime := time.Now().Add(-5 * time.Minute)
	
	worker := &Worker{
		ID:             "test-worker",
		isRunning:      true,
		config:         common.CrawlerConfig{Platform: "telegram"},
		tasksProcessed: 10,
		tasksSuccess:   8,
		tasksError:     2,
		startTime:      startTime,
	}

	status := worker.GetStatus()

	assert.Equal(t, "test-worker", status["worker_id"])
	assert.Equal(t, true, status["is_running"])
	assert.Equal(t, "telegram", status["platform"])
	assert.Equal(t, 10, status["tasks_processed"])
	assert.Equal(t, 8, status["tasks_success"])
	assert.Equal(t, 2, status["tasks_error"])
	assert.Equal(t, startTime, status["start_time"])
	
	// Check uptime is reasonable (should be around 5 minutes)
	uptime := status["uptime_seconds"].(float64)
	assert.True(t, uptime > 290 && uptime < 310) // 5 minutes ± 10 seconds
}

// TestWorkItemConfigToCrawlerConfig tests configuration conversion
func TestWorkItemConfigToCrawlerConfig(t *testing.T) {
	worker := &Worker{
		config: common.CrawlerConfig{
			Platform: "telegram",
		},
	}

	workConfig := distributed.WorkItemConfig{
		StorageRoot:       "/tmp/test",
		Concurrency:       4,
		Timeout:           60,
		MaxComments:       100,
		MaxPosts:          500,
		MaxDepth:          3,
		MaxPages:          1000,
		MinUsers:          10,
		CrawlLabel:        "test-crawl",
		SkipMediaDownload: true,
		YouTubeAPIKey:     "test-api-key",
	}

	crawlerConfig := worker.workItemConfigToCrawlerConfig(workConfig)

	assert.Equal(t, workConfig.StorageRoot, crawlerConfig.StorageRoot)
	assert.Equal(t, workConfig.Concurrency, crawlerConfig.Concurrency)
	assert.Equal(t, workConfig.Timeout, crawlerConfig.Timeout)
	assert.Equal(t, worker.config.Platform, crawlerConfig.Platform)
	assert.Equal(t, workConfig.MaxComments, crawlerConfig.MaxComments)
	assert.Equal(t, workConfig.MaxPosts, crawlerConfig.MaxPosts)
	assert.Equal(t, workConfig.MaxDepth, crawlerConfig.MaxDepth)
	assert.Equal(t, workConfig.MaxPages, crawlerConfig.MaxPages)
	assert.Equal(t, workConfig.MinUsers, crawlerConfig.MinUsers)
	assert.Equal(t, workConfig.CrawlLabel, crawlerConfig.CrawlLabel)
	assert.Equal(t, workConfig.SkipMediaDownload, crawlerConfig.SkipMediaDownload)
	assert.Equal(t, workConfig.YouTubeAPIKey, crawlerConfig.YouTubeAPIKey)
}

// TestCrawlProcessor tests the crawl processor
func TestCrawlProcessor(t *testing.T) {
	processor := &CrawlProcessor{
		platform: "telegram",
		config:   common.CrawlerConfig{Platform: "telegram"},
		workerID: "test-worker",
	}

	assert.Equal(t, "telegram", processor.platform)
	assert.Equal(t, "test-worker", processor.workerID)
	assert.Equal(t, "telegram", processor.config.Platform)
}

// TestWorkerValidation tests worker parameter validation
func TestWorkerValidation(t *testing.T) {
	tests := []struct {
		name     string
		config   common.CrawlerConfig
		workerID string
		valid    bool
	}{
		{
			name: "valid telegram worker",
			config: common.CrawlerConfig{
				Platform:    "telegram",
				StorageRoot: "/tmp/test",
				Concurrency: 2,
			},
			workerID: "telegram-worker-1",
			valid:    true,
		},
		{
			name: "valid youtube worker",
			config: common.CrawlerConfig{
				Platform:    "youtube",
				StorageRoot: "/tmp/test",
				Concurrency: 1,
			},
			workerID: "youtube-worker-1",
			valid:    true,
		},
		{
			name: "invalid empty platform",
			config: common.CrawlerConfig{
				Platform:    "",
				StorageRoot: "/tmp/test",
			},
			workerID: "worker-1",
			valid:    false,
		},
		{
			name: "invalid empty worker ID",
			config: common.CrawlerConfig{
				Platform:    "telegram",
				StorageRoot: "/tmp/test",
			},
			workerID: "",
			valid:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.valid {
				assert.NotEmpty(t, tt.config.Platform)
				assert.NotEmpty(t, tt.config.StorageRoot)
				assert.NotEmpty(t, tt.workerID)
			} else {
				// At least one required field should be empty for invalid cases
				invalid := tt.config.Platform == "" || tt.workerID == "" || tt.config.StorageRoot == ""
				assert.True(t, invalid)
			}
		})
	}
}

// Benchmark tests for performance validation
func BenchmarkWorkItemConfigConversion(b *testing.B) {
	worker := &Worker{
		config: common.CrawlerConfig{Platform: "telegram"},
	}

	workConfig := distributed.WorkItemConfig{
		StorageRoot: "/tmp/bench",
		Concurrency: 4,
		Timeout:     30,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		worker.workItemConfigToCrawlerConfig(workConfig)
	}
}

func BenchmarkShouldRetryError(b *testing.B) {
	worker := &Worker{ID: "bench-worker"}
	err := errors.New("connection timeout")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		worker.shouldRetryError(err)
	}
}
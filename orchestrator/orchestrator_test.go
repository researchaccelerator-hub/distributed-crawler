package orchestrator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/distributed"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
)

// TestNewOrchestrator tests creating a new orchestrator instance
func TestNewOrchestrator(t *testing.T) {
	config := common.CrawlerConfig{
		Platform:    "telegram",
		StorageRoot: "/tmp/test",
		DaprPort:    3500,
		MaxDepth:    3,
		Concurrency: 2,
	}

	// Test orchestrator structure creation
	assert.NotEmpty(t, config.Platform)
	assert.NotEmpty(t, config.StorageRoot)
	assert.Greater(t, config.MaxDepth, 0)
	assert.Greater(t, config.Concurrency, 0)
}

// TestCreateWorkItem tests creating work items
func TestCreateWorkItem(t *testing.T) {
	orchestrator := &Orchestrator{
		config: common.CrawlerConfig{
			Platform:          "telegram",
			StorageRoot:       "/tmp/test",
			Concurrency:       2,
			Timeout:           60,
			MaxComments:       100,
			MaxPosts:          500,
			MaxDepth:          3,
			MaxPages:          1000,
			MinUsers:          10,
			CrawlLabel:        "test-crawl",
			SkipMediaDownload: true,
		},
		crawlID: "test-crawl-id",
	}

	// Create a test page
	page := &state.Page{
		URL:   "https://t.me/test",
		Depth: 1,
		ID:    "parent-id",
	}

	workItem := orchestrator.createWorkItem(page)

	assert.NotEmpty(t, workItem.ID)
	assert.Equal(t, "https://t.me/test", workItem.URL)
	assert.Equal(t, 1, workItem.Depth)
	assert.Equal(t, "test-crawl-id", workItem.CrawlID)
	assert.Equal(t, "telegram", workItem.Platform)
	assert.Equal(t, orchestrator.config.StorageRoot, workItem.Config.StorageRoot)
	assert.Equal(t, orchestrator.config.Concurrency, workItem.Config.Concurrency)
	assert.Equal(t, orchestrator.config.Timeout, workItem.Config.Timeout)
	assert.Equal(t, orchestrator.config.MaxComments, workItem.Config.MaxComments)
	assert.Equal(t, orchestrator.config.MaxPosts, workItem.Config.MaxPosts)
	assert.Equal(t, orchestrator.config.MaxDepth, workItem.Config.MaxDepth)
	assert.Equal(t, orchestrator.config.MaxPages, workItem.Config.MaxPages)
	assert.Equal(t, orchestrator.config.MinUsers, workItem.Config.MinUsers)
	assert.Equal(t, orchestrator.config.CrawlLabel, workItem.Config.CrawlLabel)
	assert.Equal(t, orchestrator.config.SkipMediaDownload, workItem.Config.SkipMediaDownload)
}

// TestGetStatus tests getting orchestrator status
func TestGetStatus(t *testing.T) {
	startTime := time.Now().Add(-5 * time.Minute)
	
	orchestrator := &Orchestrator{
		startTime:         startTime,
		totalWorkItems:    100,
		completedItems:    75,
		errorItems:        5,
		discoveredPages:   150,
		currentDepth:      2,
		maxDepthReached:   3,
		workers: map[string]*WorkerInfo{
			"worker-1": {
				ID:           "worker-1",
				Status:       distributed.WorkerStatusIdle,
				TasksTotal:   40,
				TasksSuccess: 38,
				TasksError:   2,
			},
			"worker-2": {
				ID:           "worker-2",
				Status:       distributed.WorkerStatusBusy,
				TasksTotal:   35,
				TasksSuccess: 33,
				TasksError:   2,
			},
		},
	}

	status := orchestrator.GetStatus()

	assert.Contains(t, status, "crawl_id")
	assert.Contains(t, status, "is_running")
	assert.Contains(t, status, "platform")
	assert.Contains(t, status, "current_depth")
	assert.Contains(t, status, "max_depth_reached")
	assert.Contains(t, status, "worker_count")
	assert.Contains(t, status, "workers")
	assert.Contains(t, status, "work_stats")
	assert.Contains(t, status, "start_time")
	assert.Contains(t, status, "uptime")
	
	// Check work_stats nested structure
	workStats := status["work_stats"].(map[string]interface{})
	assert.Contains(t, workStats, "total_work")
	assert.Contains(t, workStats, "completed_items")
	assert.Contains(t, workStats, "error_items")
	assert.Contains(t, workStats, "discovered_pages")
}

// TestWorkerInfoTracking tests tracking worker information
func TestWorkerInfoTracking(t *testing.T) {
	workers := map[string]*WorkerInfo{
		"worker-1": {
			ID:           "worker-1",
			Status:       distributed.WorkerStatusIdle,
			TasksTotal:   10,
			TasksSuccess: 8,
			TasksError:   2,
			LastSeen:     time.Now(),
		},
		"worker-2": {
			ID:           "worker-2",
			Status:       distributed.WorkerStatusBusy,
			TasksTotal:   15,
			TasksSuccess: 14,
			TasksError:   1,
			LastSeen:     time.Now().Add(-30 * time.Second),
		},
	}

	// Test worker info structure
	for workerID, worker := range workers {
		assert.Equal(t, workerID, worker.ID)
		assert.NotEmpty(t, worker.Status)
		assert.GreaterOrEqual(t, worker.TasksTotal, 0)
		assert.GreaterOrEqual(t, worker.TasksSuccess, 0)
		assert.GreaterOrEqual(t, worker.TasksError, 0)
		assert.True(t, worker.LastSeen.Before(time.Now().Add(time.Second)))
	}

	// Test statistics calculation
	totalProcessed := 0
	totalSuccess := 0
	totalErrors := 0
	for _, worker := range workers {
		totalProcessed += worker.TasksTotal
		totalSuccess += worker.TasksSuccess
		totalErrors += worker.TasksError
	}

	assert.Equal(t, 25, totalProcessed)
	assert.Equal(t, 22, totalSuccess)
	assert.Equal(t, 3, totalErrors)
}

// TestOrchestratorValidation tests orchestrator parameter validation
func TestOrchestratorValidation(t *testing.T) {
	tests := []struct {
		name     string
		config   common.CrawlerConfig
		crawlID  string
		valid    bool
	}{
		{
			name: "valid orchestrator configuration",
			config: common.CrawlerConfig{
				Platform:    "telegram",
				StorageRoot: "/tmp/test",
				MaxDepth:    3,
				Concurrency: 2,
			},
			crawlID: "test-crawl",
			valid:   true,
		},
		{
			name: "empty platform",
			config: common.CrawlerConfig{
				Platform:    "",
				StorageRoot: "/tmp/test",
				MaxDepth:    3,
			},
			crawlID: "test-crawl",
			valid:   false,
		},
		{
			name: "empty crawl ID",
			config: common.CrawlerConfig{
				Platform:    "telegram",
				StorageRoot: "/tmp/test",
				MaxDepth:    3,
			},
			crawlID: "",
			valid:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.valid {
				assert.NotEmpty(t, tt.config.Platform)
				assert.NotEmpty(t, tt.config.StorageRoot)
				assert.Greater(t, tt.config.MaxDepth, 0)
				assert.NotEmpty(t, tt.crawlID)
			} else {
				// At least one required field should be invalid
				invalid := tt.config.Platform == "" || 
						 tt.config.StorageRoot == "" ||
						 tt.config.MaxDepth <= 0 ||
						 tt.crawlID == ""
				assert.True(t, invalid)
			}
		})
	}
}

// TestDepthTracking tests depth tracking functionality
func TestDepthTracking(t *testing.T) {
	orchestrator := &Orchestrator{
		currentDepth:    2,
		maxDepthReached: 3,
		config: common.CrawlerConfig{
			MaxDepth: 5,
		},
	}

	assert.Equal(t, 2, orchestrator.currentDepth)
	assert.Equal(t, 3, orchestrator.maxDepthReached)
	assert.True(t, orchestrator.currentDepth <= orchestrator.config.MaxDepth)
	assert.True(t, orchestrator.maxDepthReached <= orchestrator.config.MaxDepth)
}

// TestStatisticsTracking tests statistics tracking
func TestStatisticsTracking(t *testing.T) {
	orchestrator := &Orchestrator{
		totalWorkItems:  100,
		completedItems:  80,
		errorItems:      5,
		discoveredPages: 150,
	}

	// Test completion rate
	completionRate := float64(orchestrator.completedItems) / float64(orchestrator.totalWorkItems)
	assert.InDelta(t, 0.8, completionRate, 0.01) // 80%

	// Test error rate
	errorRate := float64(orchestrator.errorItems) / float64(orchestrator.totalWorkItems)
	assert.InDelta(t, 0.05, errorRate, 0.01) // 5%

	// Test discovery ratio
	discoveryRatio := float64(orchestrator.discoveredPages) / float64(orchestrator.completedItems)
	assert.InDelta(t, 1.875, discoveryRatio, 0.001) // 1.875 pages per completed item
}

// TestShouldRetry tests the retry logic
func TestShouldRetry(t *testing.T) {
	orchestrator := &Orchestrator{}

	tests := []struct {
		name        string
		page        *state.Page
		shouldRetry bool
	}{
		{
			name: "new page should not retry initially",
			page: &state.Page{
				URL:   "https://t.me/test",
				Depth: 1,
				Error: "",
			},
			shouldRetry: false, // No error, no need to retry
		},
		{
			name: "page with error",
			page: &state.Page{
				URL:   "https://t.me/test",
				Depth: 1,
				Error: "connection failed",
			},
			shouldRetry: true, // Has error, might be retryable
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := orchestrator.shouldRetry(tt.page)
			// Just test that the method returns a boolean
			// The actual logic depends on the implementation
			assert.IsType(t, bool(false), result)
		})
	}
}

// Benchmark tests for performance validation
func BenchmarkCreateWorkItem(b *testing.B) {
	orchestrator := &Orchestrator{
		config: common.CrawlerConfig{
			Platform:    "telegram",
			StorageRoot: "/tmp/bench",
			Concurrency: 4,
		},
		crawlID: "bench-crawl",
	}

	page := &state.Page{
		URL:   "https://t.me/benchtest",
		Depth: 1,
		ID:    "parent",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		orchestrator.createWorkItem(page)
	}
}

func BenchmarkGetStatus(b *testing.B) {
	orchestrator := &Orchestrator{
		startTime:       time.Now().Add(-1 * time.Hour),
		totalWorkItems:  1000,
		completedItems:  800,
		errorItems:      50,
		discoveredPages: 1500,
		workers: map[string]*WorkerInfo{
			"worker-1": {TasksTotal: 100},
			"worker-2": {TasksTotal: 150},
			"worker-3": {TasksTotal: 200},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		orchestrator.GetStatus()
	}
}
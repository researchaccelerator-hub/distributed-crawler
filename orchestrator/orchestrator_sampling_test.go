package orchestrator

import (
	"testing"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/distributed"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
)

func TestOrchestratorCreateWorkItemSampling(t *testing.T) {
	tests := []struct {
		name           string
		crawlerConfig  common.CrawlerConfig
		page          *state.Page
		expectedConfig distributed.WorkItemConfig
	}{
		{
			name: "youtube random sampling configuration",
			crawlerConfig: common.CrawlerConfig{
				StorageRoot:      "/tmp/youtube-random",
				Platform:         "youtube",
				Concurrency:      4,
				Timeout:          60,
				SamplingMethod:   "random",
				MinChannelVideos: 50,
				YouTubeAPIKey:    "random-api-key",
				MaxComments:      25,
				MaxPosts:         100,
				SkipMediaDownload: true,
			},
			page: &state.Page{
				URL:   "https://youtube.com/c/example",
				Depth: 1,
				ID:    "page-123",
			},
			expectedConfig: distributed.WorkItemConfig{
				StorageRoot:       "/tmp/youtube-random",
				Concurrency:       4,
				Timeout:           60,
				MaxComments:       25,
				MaxPosts:          100,
				SkipMediaDownload: true,
				YouTubeAPIKey:     "random-api-key",
				SamplingMethod:    "random",
				MinChannelVideos:  50,
			},
		},
		{
			name: "youtube snowball sampling configuration",
			crawlerConfig: common.CrawlerConfig{
				StorageRoot:      "/tmp/youtube-snowball",
				Platform:         "youtube",
				Concurrency:      2,
				Timeout:          45,
				SamplingMethod:   "snowball",
				MinChannelVideos: 25,
				YouTubeAPIKey:    "snowball-key",
				CrawlLabel:       "snowball-test",
				MaxDepth:         3,
			},
			page: &state.Page{
				URL:   "https://youtube.com/c/seed",
				Depth: 0,
				ID:    "seed-page",
			},
			expectedConfig: distributed.WorkItemConfig{
				StorageRoot:      "/tmp/youtube-snowball",
				Concurrency:      2,
				Timeout:          45,
				MaxDepth:         3,
				CrawlLabel:       "snowball-test",
				YouTubeAPIKey:    "snowball-key",
				SamplingMethod:   "snowball",
				MinChannelVideos: 25,
			},
		},
		{
			name: "telegram channel sampling configuration",
			crawlerConfig: common.CrawlerConfig{
				StorageRoot:      "/tmp/telegram",
				Platform:         "telegram",
				Concurrency:      1,
				Timeout:          30,
				SamplingMethod:   "channel",
				MinChannelVideos: 0, // Not relevant for Telegram
				MinUsers:         100,
				MaxPages:         500,
			},
			page: &state.Page{
				URL:   "https://t.me/testchannel",
				Depth: 2,
				ID:    "telegram-page",
			},
			expectedConfig: distributed.WorkItemConfig{
				StorageRoot:      "/tmp/telegram",
				Concurrency:      1,
				Timeout:          30,
				MinUsers:         100,
				MaxPages:         500,
				SamplingMethod:   "channel",
				MinChannelVideos: 0,
			},
		},
		{
			name: "complete configuration with time fields",
			crawlerConfig: common.CrawlerConfig{
				StorageRoot:       "/tmp/complete",
				Platform:          "youtube",
				Concurrency:       8,
				Timeout:           120,
				MinPostDate:       time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				PostRecency:       time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC),
				DateBetweenMin:    time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),
				DateBetweenMax:    time.Date(2023, 9, 1, 0, 0, 0, 0, time.UTC),
				SampleSize:        1000,
				SamplingMethod:    "random",
				MinChannelVideos:  75,
				YouTubeAPIKey:     "complete-key",
				CrawlLabel:        "complete-crawl",
				SkipMediaDownload: false,
			},
			page: &state.Page{
				URL:   "https://youtube.com/c/complete",
				Depth: 1,
				ID:    "complete-page",
			},
			expectedConfig: distributed.WorkItemConfig{
				StorageRoot:       "/tmp/complete",
				Concurrency:       8,
				Timeout:           120,
				MinPostDate:       time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				PostRecency:       time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC),
				DateBetweenMin:    time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),
				DateBetweenMax:    time.Date(2023, 9, 1, 0, 0, 0, 0, time.UTC),
				SampleSize:        1000,
				YouTubeAPIKey:     "complete-key",
				CrawlLabel:        "complete-crawl",
				SkipMediaDownload: false,
				SamplingMethod:    "random",
				MinChannelVideos:  75,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orchestrator := &Orchestrator{
				config:  tt.crawlerConfig,
				crawlID: "test-crawl-id",
			}

			workItem := orchestrator.createWorkItem(tt.page)

			// Verify that the work item was created correctly
			if workItem.URL != tt.page.URL {
				t.Errorf("Expected URL %s, got %s", tt.page.URL, workItem.URL)
			}
			if workItem.Depth != tt.page.Depth {
				t.Errorf("Expected depth %d, got %d", tt.page.Depth, workItem.Depth)
			}
			if workItem.ParentID != tt.page.ID {
				t.Errorf("Expected parent ID %s, got %s", tt.page.ID, workItem.ParentID)
			}
			if workItem.CrawlID != "test-crawl-id" {
				t.Errorf("Expected crawl ID 'test-crawl-id', got %s", workItem.CrawlID)
			}
			if workItem.Platform != tt.crawlerConfig.Platform {
				t.Errorf("Expected platform %s, got %s", tt.crawlerConfig.Platform, workItem.Platform)
			}

			// Verify the configuration fields
			config := workItem.Config

			if config.StorageRoot != tt.expectedConfig.StorageRoot {
				t.Errorf("StorageRoot: expected %s, got %s", tt.expectedConfig.StorageRoot, config.StorageRoot)
			}
			if config.Concurrency != tt.expectedConfig.Concurrency {
				t.Errorf("Concurrency: expected %d, got %d", tt.expectedConfig.Concurrency, config.Concurrency)
			}
			if config.Timeout != tt.expectedConfig.Timeout {
				t.Errorf("Timeout: expected %d, got %d", tt.expectedConfig.Timeout, config.Timeout)
			}
			if config.SamplingMethod != tt.expectedConfig.SamplingMethod {
				t.Errorf("SamplingMethod: expected %s, got %s", tt.expectedConfig.SamplingMethod, config.SamplingMethod)
			}
			if config.MinChannelVideos != tt.expectedConfig.MinChannelVideos {
				t.Errorf("MinChannelVideos: expected %d, got %d", tt.expectedConfig.MinChannelVideos, config.MinChannelVideos)
			}
			if config.YouTubeAPIKey != tt.expectedConfig.YouTubeAPIKey {
				t.Errorf("YouTubeAPIKey: expected %s, got %s", tt.expectedConfig.YouTubeAPIKey, config.YouTubeAPIKey)
			}
			if config.MaxComments != tt.expectedConfig.MaxComments {
				t.Errorf("MaxComments: expected %d, got %d", tt.expectedConfig.MaxComments, config.MaxComments)
			}
			if config.SkipMediaDownload != tt.expectedConfig.SkipMediaDownload {
				t.Errorf("SkipMediaDownload: expected %t, got %t", tt.expectedConfig.SkipMediaDownload, config.SkipMediaDownload)
			}

			// Verify time fields if they were set
			if !tt.expectedConfig.MinPostDate.IsZero() && !config.MinPostDate.Equal(tt.expectedConfig.MinPostDate) {
				t.Errorf("MinPostDate: expected %v, got %v", tt.expectedConfig.MinPostDate, config.MinPostDate)
			}
			if !tt.expectedConfig.PostRecency.IsZero() && !config.PostRecency.Equal(tt.expectedConfig.PostRecency) {
				t.Errorf("PostRecency: expected %v, got %v", tt.expectedConfig.PostRecency, config.PostRecency)
			}
		})
	}
}

func TestOrchestratorCreateWorkItemDefaultSampling(t *testing.T) {
	// Test that when sampling fields are not set, they get appropriate default values
	orchestrator := &Orchestrator{
		config: common.CrawlerConfig{
			StorageRoot: "/tmp/default",
			Platform:    "youtube",
			Concurrency: 2,
			// No sampling fields set
		},
		crawlID: "default-crawl",
	}

	page := &state.Page{
		URL:   "https://youtube.com/c/default",
		Depth: 0,
		ID:    "default-page",
	}

	workItem := orchestrator.createWorkItem(page)

	// Verify that default/zero values are used for sampling fields
	if workItem.Config.SamplingMethod != "" {
		t.Errorf("Expected empty sampling method, got %s", workItem.Config.SamplingMethod)
	}
	if workItem.Config.MinChannelVideos != 0 {
		t.Errorf("Expected zero min channel videos, got %d", workItem.Config.MinChannelVideos)
	}
	if workItem.Config.YouTubeAPIKey != "" {
		t.Errorf("Expected empty YouTube API key, got %s", workItem.Config.YouTubeAPIKey)
	}
}

func TestOrchestratorCreateWorkItemSamplingMethods(t *testing.T) {
	// Test each sampling method individually
	samplingMethods := []string{"channel", "random", "snowball"}
	
	for _, method := range samplingMethods {
		t.Run("method_"+method, func(t *testing.T) {
			orchestrator := &Orchestrator{
				config: common.CrawlerConfig{
					StorageRoot:      "/tmp/test",
					Platform:         "youtube",
					SamplingMethod:   method,
					MinChannelVideos: 42,
					YouTubeAPIKey:    "method-test-key",
				},
				crawlID: "method-test",
			}

			page := &state.Page{
				URL:   "https://youtube.com/c/method-test",
				Depth: 1,
				ID:    "method-page",
			}

			workItem := orchestrator.createWorkItem(page)

			if workItem.Config.SamplingMethod != method {
				t.Errorf("Expected sampling method %s, got %s", method, workItem.Config.SamplingMethod)
			}
			if workItem.Config.MinChannelVideos != 42 {
				t.Errorf("Expected min channel videos 42, got %d", workItem.Config.MinChannelVideos)
			}
		})
	}
}

func TestOrchestratorCreateWorkItemValidStructure(t *testing.T) {
	// Test that the work item has the correct structure and required fields
	orchestrator := &Orchestrator{
		config: common.CrawlerConfig{
			StorageRoot:      "/tmp/structure",
			Platform:         "youtube",
			SamplingMethod:   "snowball",
			MinChannelVideos: 30,
		},
		crawlID: "structure-test",
	}

	page := &state.Page{
		URL:   "https://youtube.com/c/structure",
		Depth: 2,
		ID:    "structure-page",
	}

	workItem := orchestrator.createWorkItem(page)

	// Verify that required fields are set
	if workItem.ID == "" {
		t.Errorf("Expected work item to have an ID")
	}
	if workItem.CreatedAt.IsZero() {
		t.Errorf("Expected work item to have a creation time")
	}
	if workItem.TraceID == "" {
		t.Errorf("Expected work item to have a trace ID")
	}
	if workItem.RetryCount != 0 {
		t.Errorf("Expected work item to have zero retry count initially, got %d", workItem.RetryCount)
	}

	// Verify that the returned type is correct
	if workItem.URL != page.URL {
		t.Errorf("Work item URL mismatch")
	}
	if workItem.Platform != orchestrator.config.Platform {
		t.Errorf("Work item platform mismatch")
	}
}
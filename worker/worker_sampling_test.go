package worker

import (
	"testing"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/distributed"
)

func TestWorkItemConfigToCrawlerConfigSampling(t *testing.T) {
	tests := []struct {
		name           string
		workItemConfig distributed.WorkItemConfig
		workerPlatform string
		expectedConfig common.CrawlerConfig
	}{
		{
			name: "basic sampling configuration",
			workItemConfig: distributed.WorkItemConfig{
				StorageRoot:      "/tmp/test",
				Concurrency:      2,
				Timeout:          30,
				SamplingMethod:   "random",
				MinChannelVideos: 25,
				YouTubeAPIKey:    "test-api-key",
			},
			workerPlatform: "youtube",
			expectedConfig: common.CrawlerConfig{
				StorageRoot:      "/tmp/test",
				Concurrency:      2,
				Timeout:          30,
				Platform:         "youtube",
				SamplingMethod:   "random",
				MinChannelVideos: 25,
				YouTubeAPIKey:    "test-api-key",
			},
		},
		{
			name: "snowball sampling configuration",
			workItemConfig: distributed.WorkItemConfig{
				StorageRoot:      "/tmp/snowball",
				Concurrency:      4,
				Timeout:          60,
				SamplingMethod:   "snowball",
				MinChannelVideos: 100,
				YouTubeAPIKey:    "snowball-key",
				MaxComments:      50,
				MaxPosts:         200,
				MaxDepth:         3,
			},
			workerPlatform: "youtube",
			expectedConfig: common.CrawlerConfig{
				StorageRoot:      "/tmp/snowball",
				Concurrency:      4,
				Timeout:          60,
				Platform:         "youtube",
				SamplingMethod:   "snowball",
				MinChannelVideos: 100,
				YouTubeAPIKey:    "snowball-key",
				MaxComments:      50,
				MaxPosts:         200,
				MaxDepth:         3,
			},
		},
		{
			name: "telegram channel sampling",
			workItemConfig: distributed.WorkItemConfig{
				StorageRoot:      "/tmp/telegram",
				Concurrency:      1,
				Timeout:          45,
				SamplingMethod:   "channel",
				MinChannelVideos: 0, // Not relevant for Telegram
				MinUsers:         1000,
			},
			workerPlatform: "telegram",
			expectedConfig: common.CrawlerConfig{
				StorageRoot:      "/tmp/telegram",
				Concurrency:      1,
				Timeout:          45,
				Platform:         "telegram",
				SamplingMethod:   "channel",
				MinChannelVideos: 0,
				MinUsers:         1000,
			},
		},
		{
			name: "complete configuration with all fields",
			workItemConfig: distributed.WorkItemConfig{
				StorageRoot:       "/tmp/complete",
				Concurrency:       8,
				Timeout:           90,
				MinPostDate:       time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				PostRecency:       time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC),
				DateBetweenMin:    time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),
				DateBetweenMax:    time.Date(2023, 9, 1, 0, 0, 0, 0, time.UTC),
				SampleSize:        500,
				MaxComments:       25,
				MaxPosts:          100,
				MaxDepth:          2,
				MaxPages:          1000,
				MinUsers:          50,
				CrawlLabel:        "complete-test",
				SkipMediaDownload: true,
				YouTubeAPIKey:     "complete-key",
				SamplingMethod:    "random",
				MinChannelVideos:  75,
			},
			workerPlatform: "youtube",
			expectedConfig: common.CrawlerConfig{
				StorageRoot:       "/tmp/complete",
				Concurrency:       8,
				Timeout:           90,
				Platform:          "youtube",
				MinPostDate:       time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				PostRecency:       time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC),
				DateBetweenMin:    time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),
				DateBetweenMax:    time.Date(2023, 9, 1, 0, 0, 0, 0, time.UTC),
				SampleSize:        500,
				MaxComments:       25,
				MaxPosts:          100,
				MaxDepth:          2,
				MaxPages:          1000,
				MinUsers:          50,
				CrawlLabel:        "complete-test",
				SkipMediaDownload: true,
				YouTubeAPIKey:     "complete-key",
				SamplingMethod:    "random",
				MinChannelVideos:  75,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a worker with the specified platform
			worker := &Worker{
				config: common.CrawlerConfig{
					Platform: tt.workerPlatform,
				},
			}

			result := worker.workItemConfigToCrawlerConfig(tt.workItemConfig)

			// Verify all fields are correctly converted
			if result.StorageRoot != tt.expectedConfig.StorageRoot {
				t.Errorf("StorageRoot: expected %s, got %s", tt.expectedConfig.StorageRoot, result.StorageRoot)
			}
			if result.Concurrency != tt.expectedConfig.Concurrency {
				t.Errorf("Concurrency: expected %d, got %d", tt.expectedConfig.Concurrency, result.Concurrency)
			}
			if result.Timeout != tt.expectedConfig.Timeout {
				t.Errorf("Timeout: expected %d, got %d", tt.expectedConfig.Timeout, result.Timeout)
			}
			if result.Platform != tt.expectedConfig.Platform {
				t.Errorf("Platform: expected %s, got %s", tt.expectedConfig.Platform, result.Platform)
			}
			if result.SamplingMethod != tt.expectedConfig.SamplingMethod {
				t.Errorf("SamplingMethod: expected %s, got %s", tt.expectedConfig.SamplingMethod, result.SamplingMethod)
			}
			if result.MinChannelVideos != tt.expectedConfig.MinChannelVideos {
				t.Errorf("MinChannelVideos: expected %d, got %d", tt.expectedConfig.MinChannelVideos, result.MinChannelVideos)
			}
			if result.YouTubeAPIKey != tt.expectedConfig.YouTubeAPIKey {
				t.Errorf("YouTubeAPIKey: expected %s, got %s", tt.expectedConfig.YouTubeAPIKey, result.YouTubeAPIKey)
			}
			if result.MaxComments != tt.expectedConfig.MaxComments {
				t.Errorf("MaxComments: expected %d, got %d", tt.expectedConfig.MaxComments, result.MaxComments)
			}
			if result.MaxPosts != tt.expectedConfig.MaxPosts {
				t.Errorf("MaxPosts: expected %d, got %d", tt.expectedConfig.MaxPosts, result.MaxPosts)
			}
			if result.MaxDepth != tt.expectedConfig.MaxDepth {
				t.Errorf("MaxDepth: expected %d, got %d", tt.expectedConfig.MaxDepth, result.MaxDepth)
			}
			if result.SkipMediaDownload != tt.expectedConfig.SkipMediaDownload {
				t.Errorf("SkipMediaDownload: expected %t, got %t", tt.expectedConfig.SkipMediaDownload, result.SkipMediaDownload)
			}
		})
	}
}

func TestWorkItemConfigToCrawlerConfigEmptyFields(t *testing.T) {
	// Test with minimal configuration to ensure defaults are handled correctly
	workItemConfig := distributed.WorkItemConfig{
		StorageRoot: "/tmp/minimal",
		Concurrency: 1,
	}

	worker := &Worker{
		config: common.CrawlerConfig{
			Platform: "youtube",
		},
	}

	result := worker.workItemConfigToCrawlerConfig(workItemConfig)

	// Verify that empty/zero values are handled correctly
	if result.SamplingMethod != "" {
		t.Errorf("Expected empty sampling method, got %s", result.SamplingMethod)
	}
	if result.MinChannelVideos != 0 {
		t.Errorf("Expected zero min channel videos, got %d", result.MinChannelVideos)
	}
	if result.YouTubeAPIKey != "" {
		t.Errorf("Expected empty YouTube API key, got %s", result.YouTubeAPIKey)
	}
	if result.Platform != "youtube" {
		t.Errorf("Expected platform youtube, got %s", result.Platform)
	}
}

func TestWorkItemConfigSamplingMethodValues(t *testing.T) {
	samplingMethods := []string{"channel", "random", "snowball"}
	
	worker := &Worker{
		config: common.CrawlerConfig{
			Platform: "youtube",
		},
	}

	for _, method := range samplingMethods {
		t.Run("method_"+method, func(t *testing.T) {
			workItemConfig := distributed.WorkItemConfig{
				StorageRoot:      "/tmp/test",
				SamplingMethod:   method,
				MinChannelVideos: 30,
			}

			result := worker.workItemConfigToCrawlerConfig(workItemConfig)

			if result.SamplingMethod != method {
				t.Errorf("Expected sampling method %s, got %s", method, result.SamplingMethod)
			}
		})
	}
}

func TestWorkItemConfigMinChannelVideosTypes(t *testing.T) {
	// Test that MinChannelVideos int64 field is handled correctly
	testValues := []int64{0, 1, 10, 50, 100, 1000}
	
	worker := &Worker{
		config: common.CrawlerConfig{
			Platform: "youtube",
		},
	}

	for _, value := range testValues {
		t.Run("min_videos_"+string(rune(value+48)), func(t *testing.T) {
			workItemConfig := distributed.WorkItemConfig{
				StorageRoot:      "/tmp/test",
				MinChannelVideos: value,
			}

			result := worker.workItemConfigToCrawlerConfig(workItemConfig)

			if result.MinChannelVideos != value {
				t.Errorf("Expected min channel videos %d, got %d", value, result.MinChannelVideos)
			}
		})
	}
}
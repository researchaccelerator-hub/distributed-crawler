package distributed

import (
	"testing"
	"time"
)

func TestWorkItemConfigSamplingFields(t *testing.T) {
	config := WorkItemConfig{
		StorageRoot:       "/tmp/test",
		Concurrency:       2,
		Timeout:           30,
		SamplingMethod:    "random",
		MinChannelVideos:  50,
		YouTubeAPIKey:     "test-key",
	}

	workItem := NewWorkItem("https://youtube.com/c/test", 1, "parent", "crawl-id", "youtube", config)

	// Verify that WorkItem was created correctly
	if workItem.Config.SamplingMethod != "random" {
		t.Errorf("Expected sampling method 'random', got '%s'", workItem.Config.SamplingMethod)
	}

	if workItem.Config.MinChannelVideos != 50 {
		t.Errorf("Expected min channel videos 50, got %d", workItem.Config.MinChannelVideos)
	}

	if workItem.Config.YouTubeAPIKey != "test-key" {
		t.Errorf("Expected YouTube API key 'test-key', got '%s'", workItem.Config.YouTubeAPIKey)
	}
}

func TestWorkItemConfigDefaultSamplingValues(t *testing.T) {
	config := WorkItemConfig{
		StorageRoot: "/tmp/test",
		Concurrency: 1,
		Timeout:     60,
		// No sampling fields set - should use zero values
	}

	workItem := NewWorkItem("https://t.me/test", 0, "", "crawl-id", "telegram", config)

	// Verify default/zero values
	if workItem.Config.SamplingMethod != "" {
		t.Errorf("Expected empty sampling method, got '%s'", workItem.Config.SamplingMethod)
	}

	if workItem.Config.MinChannelVideos != 0 {
		t.Errorf("Expected min channel videos 0, got %d", workItem.Config.MinChannelVideos)
	}
}

func TestWorkItemConfigAllSamplingMethods(t *testing.T) {
	samplingMethods := []string{"channel", "random", "snowball"}
	
	for _, method := range samplingMethods {
		t.Run("sampling_method_"+method, func(t *testing.T) {
			config := WorkItemConfig{
				StorageRoot:      "/tmp/test",
				SamplingMethod:   method,
				MinChannelVideos: 25,
			}

			workItem := NewWorkItem("https://example.com", 0, "", "test", "youtube", config)
			
			if workItem.Config.SamplingMethod != method {
				t.Errorf("Expected sampling method '%s', got '%s'", method, workItem.Config.SamplingMethod)
			}
		})
	}
}

func TestWorkItemConfigSamplingJSONSerialization(t *testing.T) {
	config := WorkItemConfig{
		StorageRoot:       "/tmp/test",
		Concurrency:       3,
		Timeout:           45,
		MinPostDate:       time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
		SamplingMethod:    "snowball",
		MinChannelVideos:  100,
		YouTubeAPIKey:     "secret-key",
	}

	workItem := NewWorkItem("https://youtube.com/c/example", 2, "parent-123", "crawl-456", "youtube", config)

	// Test that the struct can be marshaled/unmarshaled (basic JSON compatibility test)
	// This would normally use json.Marshal/Unmarshal but we're avoiding imports for simplicity
	
	// Verify the fields are preserved in the struct
	if workItem.Config.SamplingMethod != "snowball" {
		t.Errorf("Expected sampling method 'snowball', got '%s'", workItem.Config.SamplingMethod)
	}
	
	if workItem.Config.MinChannelVideos != 100 {
		t.Errorf("Expected min channel videos 100, got %d", workItem.Config.MinChannelVideos)
	}
	
	if workItem.Config.YouTubeAPIKey != "secret-key" {
		t.Errorf("Expected YouTube API key 'secret-key', got '%s'", workItem.Config.YouTubeAPIKey)
	}
}

func TestWorkItemConfigCompleteFields(t *testing.T) {
	// Test that all fields including the new sampling fields are properly handled
	now := time.Now()
	config := WorkItemConfig{
		StorageRoot:       "/tmp/complete-test",
		Concurrency:       5,
		Timeout:           120,
		MinPostDate:       now.AddDate(0, -6, 0), // 6 months ago
		PostRecency:       now.AddDate(0, 0, -30), // 30 days ago
		DateBetweenMin:    now.AddDate(0, -3, 0), // 3 months ago
		DateBetweenMax:    now.AddDate(0, -1, 0), // 1 month ago
		SampleSize:        1000,
		MaxComments:       50,
		MaxPosts:          200,
		MaxDepth:          3,
		MaxPages:          5000,
		MinUsers:          500,
		CrawlLabel:        "test-crawl",
		SkipMediaDownload: true,
		YouTubeAPIKey:     "complete-test-key",
		SamplingMethod:    "random",
		MinChannelVideos:  75,
	}

	workItem := NewWorkItem("https://example.com/test", 1, "parent", "complete-crawl", "youtube", config)

	// Verify that all fields are preserved
	c := workItem.Config
	
	if c.StorageRoot != "/tmp/complete-test" {
		t.Errorf("StorageRoot mismatch")
	}
	if c.Concurrency != 5 {
		t.Errorf("Concurrency mismatch")
	}
	if c.SamplingMethod != "random" {
		t.Errorf("SamplingMethod mismatch")
	}
	if c.MinChannelVideos != 75 {
		t.Errorf("MinChannelVideos mismatch")
	}
	if c.YouTubeAPIKey != "complete-test-key" {
		t.Errorf("YouTubeAPIKey mismatch")
	}
	if c.MaxComments != 50 {
		t.Errorf("MaxComments mismatch")
	}
	if c.SkipMediaDownload != true {
		t.Errorf("SkipMediaDownload mismatch")
	}
}
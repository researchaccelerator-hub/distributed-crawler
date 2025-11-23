package client

import (
	"context"
	"testing"
	"time"

	youtubemodel "github.com/researchaccelerator-hub/telegram-scraper/model/youtube"
)

func TestNewYouTubeDataClient(t *testing.T) {
	tests := []struct {
		name    string
		apiKey  string
		wantErr bool
	}{
		{
			name:    "valid API key",
			apiKey:  "test-api-key-12345",
			wantErr: false,
		},
		{
			name:    "empty API key",
			apiKey:  "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewYouTubeDataClient(tt.apiKey)

			if (err != nil) != tt.wantErr {
				t.Errorf("NewYouTubeDataClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if client == nil {
					t.Error("Expected non-nil client when no error")
					return
				}

				if client.apiKey != tt.apiKey {
					t.Errorf("Expected apiKey %s, got %s", tt.apiKey, client.apiKey)
				}

				if client.channelCache == nil {
					t.Error("Expected channelCache to be initialized")
				}

				if client.uploadsPlaylistCache == nil {
					t.Error("Expected uploadsPlaylistCache to be initialized")
				}

				if client.videoStatsCache == nil {
					t.Error("Expected videoStatsCache to be initialized")
				}

				if client.rng == nil {
					t.Error("Expected rng to be initialized")
				}

				// Verify batch config defaults
				if client.batchConfig.MinBatchSize != 10 {
					t.Errorf("Expected MinBatchSize 10, got %d", client.batchConfig.MinBatchSize)
				}

				if client.batchConfig.MaxBatchSize != 50 {
					t.Errorf("Expected MaxBatchSize 50, got %d", client.batchConfig.MaxBatchSize)
				}

				if client.batchConfig.OptimalBatchSize != 50 {
					t.Errorf("Expected OptimalBatchSize 50, got %d", client.batchConfig.OptimalBatchSize)
				}
			}
		})
	}
}

func TestYouTubeDataClient_Disconnect(t *testing.T) {
	client, err := NewYouTubeDataClient("test-key")
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Simulate a service being connected
	client.service = nil // Service would normally be set by Connect

	err = client.Disconnect(context.Background())
	if err != nil {
		t.Errorf("Disconnect() error = %v, want nil", err)
	}

	if client.service != nil {
		t.Error("Expected service to be nil after disconnect")
	}
}

func TestYouTubeDataClient_GetChannelInfo_NotConnected(t *testing.T) {
	client, err := NewYouTubeDataClient("test-key")
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Don't call Connect() - service should be nil
	_, err = client.GetChannelInfo(context.Background(), "UCtest123")

	if err == nil {
		t.Error("Expected error when client not connected, got nil")
	}

	if err != nil && err.Error() != "YouTube client not connected" {
		t.Errorf("Expected 'YouTube client not connected' error, got: %v", err)
	}
}

func TestYouTubeDataClient_GetVideosFromChannel_NotConnected(t *testing.T) {
	client, err := NewYouTubeDataClient("test-key")
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	fromTime := time.Now().Add(-24 * time.Hour)
	toTime := time.Now()

	// Don't call Connect() - service should be nil
	_, err = client.GetVideosFromChannel(context.Background(), "UCtest123", fromTime, toTime, 10)

	if err == nil {
		t.Error("Expected error when client not connected, got nil")
	}

	if err != nil && err.Error() != "YouTube client not connected" {
		t.Errorf("Expected 'YouTube client not connected' error, got: %v", err)
	}
}

func TestYouTubeDataClient_GenerateRandomPrefix(t *testing.T) {
	client, err := NewYouTubeDataClient("test-key")
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	tests := []struct {
		name   string
		length int
	}{
		{"length 5", 5},
		{"length 10", 10},
		{"length 1", 1},
		{"length 15", 15},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefix := client.generateRandomPrefix(tt.length)

			// Check prefix starts with "watch?v="
			expectedStart := "watch?v="
			if len(prefix) < len(expectedStart) {
				t.Errorf("Prefix too short: %s", prefix)
				return
			}

			if prefix[:len(expectedStart)] != expectedStart {
				t.Errorf("Prefix should start with '%s', got: %s", expectedStart, prefix)
			}

			// Check total length is expectedStart + length + "-"
			expectedLength := len(expectedStart) + tt.length + 1
			if len(prefix) != expectedLength {
				t.Errorf("Expected prefix length %d, got %d (prefix: %s)", expectedLength, len(prefix), prefix)
			}

			// Check it ends with "-"
			if prefix[len(prefix)-1] != '-' {
				t.Errorf("Prefix should end with '-', got: %s", prefix)
			}

			// Generate multiple prefixes to check they're different (randomness test)
			if tt.length >= 5 {
				prefix2 := client.generateRandomPrefix(tt.length)
				prefix3 := client.generateRandomPrefix(tt.length)

				// At least one should be different (probabilistically)
				if prefix == prefix2 && prefix2 == prefix3 {
					t.Error("All three prefixes are identical - randomness may be broken")
				}
			}
		})
	}
}

func TestYouTubeDataClient_GetDynamicBatchSize(t *testing.T) {
	client, err := NewYouTubeDataClient("test-key")
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	tests := []struct {
		name           string
		currentVideos  int
		targetLimit    int
		expectedResult int
	}{
		{
			name:           "far from limit",
			currentVideos:  0,
			targetLimit:    1000,
			expectedResult: 50, // OptimalBatchSize
		},
		{
			name:           "close to limit",
			currentVideos:  990,
			targetLimit:    1000,
			expectedResult: 10, // MinBatchSize
		},
		{
			name:           "very close to limit",
			currentVideos:  995,
			targetLimit:    1000,
			expectedResult: 5, // Remaining count (less than MinBatchSize treated as MinBatchSize logic)
		},
		{
			name:           "medium distance",
			currentVideos:  970,
			targetLimit:    1000,
			expectedResult: 30, // Remaining (between min and optimal)
		},
		{
			name:           "at limit",
			currentVideos:  1000,
			targetLimit:    1000,
			expectedResult: 10, // MinBatchSize when at or past limit
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := client.getDynamicBatchSize(tt.currentVideos, tt.targetLimit)

			if result != tt.expectedResult {
				t.Errorf("getDynamicBatchSize() = %d, want %d", result, tt.expectedResult)
			}
		})
	}
}

func TestYouTubeDataClient_CacheOperations(t *testing.T) {
	client, err := NewYouTubeDataClient("test-key")
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Test video stats cache
	testVideo := &youtubemodel.YouTubeVideo{
		ID:           "test123",
		Title:        "Test Video",
		ViewCount:    1000,
		LikeCount:    100,
		CommentCount: 50,
	}

	// Check cache miss
	cachedVideo, exists := client.getCachedVideoStats("test123")
	if exists {
		t.Error("Expected cache miss for video that hasn't been cached")
	}
	if cachedVideo != nil {
		t.Error("Expected nil video for cache miss")
	}

	// Cache the video
	client.cacheVideoStats(testVideo)

	// Check cache hit
	cachedVideo, exists = client.getCachedVideoStats("test123")
	if !exists {
		t.Error("Expected cache hit for video that was cached")
	}
	if cachedVideo == nil {
		t.Fatal("Expected non-nil video for cache hit")
	}

	// Verify cached data
	if cachedVideo.ID != testVideo.ID {
		t.Errorf("Cached video ID = %s, want %s", cachedVideo.ID, testVideo.ID)
	}
	if cachedVideo.ViewCount != testVideo.ViewCount {
		t.Errorf("Cached video ViewCount = %d, want %d", cachedVideo.ViewCount, testVideo.ViewCount)
	}

	// Test cache miss for different video
	_, exists = client.getCachedVideoStats("different-id")
	if exists {
		t.Error("Expected cache miss for different video ID")
	}
}

func TestExtractChannelIDsFromText(t *testing.T) {
	tests := []struct {
		name     string
		text     string
		expected []string
	}{
		{
			name:     "single standard channel ID",
			text:     "Check out this channel: https://youtube.com/channel/UCtest123",
			expected: []string{"UCtest123"},
		},
		{
			name:     "single handle",
			text:     "Follow me at https://youtube.com/@testuser",
			expected: []string{"@testuser"},
		},
		{
			name: "multiple channel IDs",
			text: "Visit https://youtube.com/channel/UC123 and https://youtube.com/channel/UC456",
			expected: []string{"UC123", "UC456"},
		},
		{
			name: "mixed formats",
			text: "Check https://youtube.com/channel/UCtest and https://youtube.com/@handle",
			expected: []string{"UCtest", "@handle"},
		},
		{
			name:     "no channel IDs",
			text:     "This is just plain text with no links",
			expected: []string{},
		},
		{
			name:     "empty text",
			text:     "",
			expected: []string{},
		},
		{
			name: "handle with dots and dashes",
			text: "https://youtube.com/@test.user-name",
			expected: []string{"@test.user-name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractChannelIDsFromText(tt.text)

			if len(result) != len(tt.expected) {
				t.Errorf("Expected %d channel IDs, got %d: %v", len(tt.expected), len(result), result)
				return
			}

			// Check each expected ID is present
			for _, expectedID := range tt.expected {
				found := false
				for _, resultID := range result {
					if resultID == expectedID {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Expected channel ID '%s' not found in result: %v", expectedID, result)
				}
			}
		})
	}
}

func TestMin(t *testing.T) {
	tests := []struct {
		name     string
		a        int
		b        int
		expected int
	}{
		{"a less than b", 5, 10, 5},
		{"b less than a", 10, 5, 5},
		{"equal", 7, 7, 7},
		{"zero and positive", 0, 10, 0},
		{"negative and positive", -5, 10, -5},
		{"both negative", -10, -5, -10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := min(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("min(%d, %d) = %d, want %d", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

func TestNewYouTubeClientAdapter(t *testing.T) {
	tests := []struct {
		name    string
		apiKey  string
		wantErr bool
	}{
		{
			name:    "valid API key",
			apiKey:  "test-api-key",
			wantErr: false,
		},
		{
			name:    "empty API key",
			apiKey:  "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter, err := NewYouTubeClientAdapter(tt.apiKey)

			if (err != nil) != tt.wantErr {
				t.Errorf("NewYouTubeClientAdapter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if adapter == nil {
					t.Error("Expected non-nil adapter")
					return
				}

				ytAdapter, ok := adapter.(*YouTubeClientAdapter)
				if !ok {
					t.Error("Expected adapter to be *YouTubeClientAdapter")
					return
				}

				if ytAdapter.client == nil {
					t.Error("Expected underlying client to be initialized")
				}
			}
		})
	}
}

func TestYouTubeClientAdapter_GetChannelType(t *testing.T) {
	adapter, err := NewYouTubeClientAdapter("test-key")
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}

	channelType := adapter.GetChannelType()
	if channelType != "youtube" {
		t.Errorf("GetChannelType() = %s, want 'youtube'", channelType)
	}
}

func TestYouTubeDataClient_GetRandomVideos_NotConnected(t *testing.T) {
	client, err := NewYouTubeDataClient("test-key")
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	fromTime := time.Now().Add(-24 * time.Hour)
	toTime := time.Now()

	// Don't call Connect() - service should be nil
	_, err = client.GetRandomVideos(context.Background(), fromTime, toTime, 10)

	if err == nil {
		t.Error("Expected error when client not connected, got nil")
	}

	if err != nil && err.Error() != "YouTube client not connected" {
		t.Errorf("Expected 'YouTube client not connected' error, got: %v", err)
	}
}

func TestYouTubeDataClient_GetSnowballVideos_NotConnected(t *testing.T) {
	client, err := NewYouTubeDataClient("test-key")
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	fromTime := time.Now().Add(-24 * time.Hour)
	toTime := time.Now()
	seedChannels := []string{"UCtest123"}

	// Don't call Connect() - service should be nil
	_, err = client.GetSnowballVideos(context.Background(), seedChannels, fromTime, toTime, 10)

	if err == nil {
		t.Error("Expected error when client not connected, got nil")
	}

	if err != nil && err.Error() != "YouTube client not connected" {
		t.Errorf("Expected 'YouTube client not connected' error, got: %v", err)
	}
}

func TestYouTubeDataClient_GetSnowballVideos_NoSeedChannels(t *testing.T) {
	client, err := NewYouTubeDataClient("test-key")
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	fromTime := time.Now().Add(-24 * time.Hour)
	toTime := time.Now()
	seedChannels := []string{}

	// Even without connection, should fail due to no seed channels
	_, err = client.GetSnowballVideos(context.Background(), seedChannels, fromTime, toTime, 10)

	if err == nil {
		t.Error("Expected error when no seed channels provided, got nil")
	}

	expectedMsg := "at least one seed channel ID is required for snowball sampling"
	if err != nil && err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got: %v", expectedMsg, err)
	}
}

func TestBatchConfig(t *testing.T) {
	config := BatchConfig{
		MinBatchSize:     5,
		MaxBatchSize:     100,
		OptimalBatchSize: 50,
	}

	if config.MinBatchSize != 5 {
		t.Errorf("MinBatchSize = %d, want 5", config.MinBatchSize)
	}

	if config.MaxBatchSize != 100 {
		t.Errorf("MaxBatchSize = %d, want 100", config.MaxBatchSize)
	}

	if config.OptimalBatchSize != 50 {
		t.Errorf("OptimalBatchSize = %d, want 50", config.OptimalBatchSize)
	}
}

func TestYouTubeDataClient_ThreadSafety(t *testing.T) {
	client, err := NewYouTubeDataClient("test-key")
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Test concurrent cache operations
	done := make(chan bool)
	const goroutines = 10

	// Test concurrent caching
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			video := &youtubemodel.YouTubeVideo{
				ID:        string(rune('A' + id)),
				ViewCount: int64(id * 100),
			}
			client.cacheVideoStats(video)
			done <- true
		}(i)
	}

	for i := 0; i < goroutines; i++ {
		<-done
	}

	// Test concurrent reads
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			_, _ = client.getCachedVideoStats(string(rune('A' + id)))
			done <- true
		}(i)
	}

	for i := 0; i < goroutines; i++ {
		<-done
	}

	// Test concurrent prefix generation (uses RNG with mutex)
	for i := 0; i < goroutines; i++ {
		go func() {
			_ = client.generateRandomPrefix(10)
			done <- true
		}()
	}

	for i := 0; i < goroutines; i++ {
		<-done
	}

	// If we get here without deadlock or race conditions, test passes
}

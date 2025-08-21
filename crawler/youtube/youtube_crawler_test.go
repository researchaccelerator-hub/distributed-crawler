package youtube

import (
	"context"
	"testing"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/model"
	youtubemodel "github.com/researchaccelerator-hub/telegram-scraper/model/youtube"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
)

// MockYouTubeClient implements youtubemodel.YouTubeClient for testing
type MockYouTubeClient struct{}

func (m *MockYouTubeClient) Connect(ctx context.Context) error {
	return nil
}

func (m *MockYouTubeClient) Disconnect(ctx context.Context) error {
	return nil
}

func (m *MockYouTubeClient) GetChannelInfo(ctx context.Context, channelID string) (*youtubemodel.YouTubeChannel, error) {
	return &youtubemodel.YouTubeChannel{
		ID:    channelID,
		Title: "Test Channel",
	}, nil
}

func (m *MockYouTubeClient) GetVideos(ctx context.Context, channelID string, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	return []*youtubemodel.YouTubeVideo{}, nil
}

func (m *MockYouTubeClient) GetVideosFromChannel(ctx context.Context, channelID string, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	return []*youtubemodel.YouTubeVideo{}, nil
}

func (m *MockYouTubeClient) GetRandomVideos(ctx context.Context, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	return []*youtubemodel.YouTubeVideo{}, nil
}

func (m *MockYouTubeClient) GetSnowballVideos(ctx context.Context, seedChannelIDs []string, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	return []*youtubemodel.YouTubeVideo{}, nil
}

func (m *MockYouTubeClient) GetVideosByIDs(ctx context.Context, videoIDs []string) ([]*youtubemodel.YouTubeVideo, error) {
	return []*youtubemodel.YouTubeVideo{}, nil
}

// MockYouTubeStateManager implements state.StateManagementInterface for testing
type MockYouTubeStateManager struct{}

func (m *MockYouTubeStateManager) Initialize(urls []string) error        { return nil }
func (m *MockYouTubeStateManager) GetPage(id string) (state.Page, error) { return state.Page{}, nil }
func (m *MockYouTubeStateManager) UpdatePage(page state.Page) error      { return nil }
func (m *MockYouTubeStateManager) UpdateMessage(pageID string, chatID int64, messageID int64, status string) error {
	return nil
}
func (m *MockYouTubeStateManager) AddLayer(pages []state.Page) error                 { return nil }
func (m *MockYouTubeStateManager) GetLayerByDepth(depth int) ([]state.Page, error)   { return nil, nil }
func (m *MockYouTubeStateManager) GetMaxDepth() (int, error)                         { return 0, nil }
func (m *MockYouTubeStateManager) SaveState() error                                  { return nil }
func (m *MockYouTubeStateManager) ExportPagesToBinding(crawlID string) error         { return nil }
func (m *MockYouTubeStateManager) StorePost(channelID string, post model.Post) error { return nil }
func (m *MockYouTubeStateManager) StoreFile(channelID string, sourceFilePath string, fileName string) (string, string, error) {
	return "", "", nil
}
func (m *MockYouTubeStateManager) GetPreviousCrawls() ([]string, error) { return nil, nil }
func (m *MockYouTubeStateManager) UpdateCrawlMetadata(crawlID string, metadata map[string]interface{}) error {
	return nil
}
func (m *MockYouTubeStateManager) FindIncompleteCrawl(crawlID string) (string, bool, error) {
	return "", false, nil
}
func (m *MockYouTubeStateManager) HasProcessedMedia(mediaID string) (bool, error) { return false, nil }
func (m *MockYouTubeStateManager) MarkMediaAsProcessed(mediaID string) error      { return nil }
func (m *MockYouTubeStateManager) Close() error                                   { return nil }

func TestYouTubeCrawlerInitialize(t *testing.T) {
	tests := []struct {
		name              string
		config            map[string]interface{}
		expectError       bool
		expectedMethod    SamplingMethod
		expectedMinVideos int64
	}{
		{
			name: "basic initialization",
			config: map[string]interface{}{
				"client":        &MockYouTubeClient{},
				"state_manager": &MockYouTubeStateManager{},
			},
			expectError:       false,
			expectedMethod:    SamplingMethodChannel,
			expectedMinVideos: 10,
		},
		{
			name: "initialization with crawler config - random sampling",
			config: map[string]interface{}{
				"client":        &MockYouTubeClient{},
				"state_manager": &MockYouTubeStateManager{},
				"crawler_config": map[string]interface{}{
					"sampling_method":    "random",
					"min_channel_videos": int64(25),
				},
			},
			expectError:       false,
			expectedMethod:    SamplingMethodRandom,
			expectedMinVideos: 25,
		},
		{
			name: "initialization with crawler config - snowball sampling",
			config: map[string]interface{}{
				"client":        &MockYouTubeClient{},
				"state_manager": &MockYouTubeStateManager{},
				"crawler_config": map[string]interface{}{
					"sampling_method":    "snowball",
					"seed_channels":      []interface{}{"UC123", "UC456"},
					"min_channel_videos": int64(50),
				},
			},
			expectError:       false,
			expectedMethod:    SamplingMethodSnowball,
			expectedMinVideos: 50,
		},
		{
			name: "initialization with different numeric types",
			config: map[string]interface{}{
				"client":        &MockYouTubeClient{},
				"state_manager": &MockYouTubeStateManager{},
				"crawler_config": map[string]interface{}{
					"sampling_method":    "channel",
					"min_channel_videos": int(100), // int instead of int64
				},
			},
			expectError:       false,
			expectedMethod:    SamplingMethodChannel,
			expectedMinVideos: 100,
		},
		{
			name: "initialization with float64 numeric type",
			config: map[string]interface{}{
				"client":        &MockYouTubeClient{},
				"state_manager": &MockYouTubeStateManager{},
				"crawler_config": map[string]interface{}{
					"sampling_method":    "channel",
					"min_channel_videos": float64(75), // float64
				},
			},
			expectError:       false,
			expectedMethod:    SamplingMethodChannel,
			expectedMinVideos: 75,
		},
		{
			name: "missing client",
			config: map[string]interface{}{
				"state_manager": &MockYouTubeStateManager{},
			},
			expectError: true,
		},
		{
			name: "missing state manager",
			config: map[string]interface{}{
				"client": &MockYouTubeClient{},
			},
			expectError: true,
		},
		{
			name: "invalid client type",
			config: map[string]interface{}{
				"client":        "not a client",
				"state_manager": &MockYouTubeStateManager{},
			},
			expectError: true,
		},
		{
			name: "invalid state manager type",
			config: map[string]interface{}{
				"client":        &MockYouTubeClient{},
				"state_manager": "not a state manager",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			crawler := NewYouTubeCrawler().(*YouTubeCrawler)

			err := crawler.Initialize(context.Background(), tt.config)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Expected no error but got: %s", err.Error())
				return
			}

			// Verify configuration was applied correctly
			if crawler.config.SamplingMethod != tt.expectedMethod {
				t.Errorf("Expected sampling method %s, got %s", tt.expectedMethod, crawler.config.SamplingMethod)
			}

			if crawler.config.MinChannelVideos != tt.expectedMinVideos {
				t.Errorf("Expected min channel videos %d, got %d", tt.expectedMinVideos, crawler.config.MinChannelVideos)
			}

			// Verify initialization state
			if !crawler.initialized {
				t.Errorf("Expected crawler to be initialized")
			}
		})
	}
}

func TestYouTubeCrawlerSnowballValidation(t *testing.T) {
	tests := []struct {
		name           string
		config         map[string]interface{}
		expectedMethod SamplingMethod
		expectWarning  bool
	}{
		{
			name: "snowball with seed channels",
			config: map[string]interface{}{
				"client":        &MockYouTubeClient{},
				"state_manager": &MockYouTubeStateManager{},
				"crawler_config": map[string]interface{}{
					"sampling_method": "snowball",
					"seed_channels":   []interface{}{"UC123", "UC456"},
				},
			},
			expectedMethod: SamplingMethodSnowball,
			expectWarning:  false,
		},
		{
			name: "snowball without seed channels - fallback to channel",
			config: map[string]interface{}{
				"client":        &MockYouTubeClient{},
				"state_manager": &MockYouTubeStateManager{},
				"crawler_config": map[string]interface{}{
					"sampling_method": "snowball",
				},
			},
			expectedMethod: SamplingMethodChannel,
			expectWarning:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			crawler := NewYouTubeCrawler().(*YouTubeCrawler)

			err := crawler.Initialize(context.Background(), tt.config)
			if err != nil {
				t.Errorf("Expected no error but got: %s", err.Error())
				return
			}

			if crawler.config.SamplingMethod != tt.expectedMethod {
				t.Errorf("Expected sampling method %s, got %s", tt.expectedMethod, crawler.config.SamplingMethod)
			}
		})
	}
}

func TestYouTubeCrawlerSeedChannelsExtraction(t *testing.T) {
	tests := []struct {
		name          string
		seedChannels  interface{}
		expectedSeeds []string
	}{
		{
			name:          "valid seed channels",
			seedChannels:  []interface{}{"UC123", "UC456", "UC789"},
			expectedSeeds: []string{"UC123", "UC456", "UC789"},
		},
		{
			name:          "empty seed channels",
			seedChannels:  []interface{}{},
			expectedSeeds: []string{},
		},
		{
			name:          "mixed types in seed channels",
			seedChannels:  []interface{}{"UC123", 456, "UC789"},
			expectedSeeds: []string{"UC123", "UC789"}, // Non-string items should be filtered out
		},
		{
			name:          "non-slice seed channels",
			seedChannels:  "not a slice",
			expectedSeeds: []string{}, // Should default to empty
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := map[string]interface{}{
				"client":        &MockYouTubeClient{},
				"state_manager": &MockYouTubeStateManager{},
				"crawler_config": map[string]interface{}{
					"sampling_method": "snowball",
					"seed_channels":   tt.seedChannels,
				},
			}

			crawler := NewYouTubeCrawler().(*YouTubeCrawler)
			err := crawler.Initialize(context.Background(), config)

			if err != nil {
				t.Errorf("Expected no error but got: %s", err.Error())
				return
			}

			if len(crawler.config.SeedChannels) != len(tt.expectedSeeds) {
				t.Errorf("Expected %d seed channels, got %d", len(tt.expectedSeeds), len(crawler.config.SeedChannels))
				return
			}

			for i, expected := range tt.expectedSeeds {
				if i >= len(crawler.config.SeedChannels) || crawler.config.SeedChannels[i] != expected {
					t.Errorf("Expected seed channel[%d] to be %s, got %s", i, expected, crawler.config.SeedChannels[i])
				}
			}
		})
	}
}

func TestYouTubeCrawlerDefaultConfig(t *testing.T) {
	crawler := NewYouTubeCrawler().(*YouTubeCrawler)

	// Test that default configuration is set correctly
	if crawler.defaultConfig.SamplingMethod != SamplingMethodChannel {
		t.Errorf("Expected default sampling method to be %s, got %s", SamplingMethodChannel, crawler.defaultConfig.SamplingMethod)
	}

	if crawler.defaultConfig.MinChannelVideos != 10 {
		t.Errorf("Expected default min channel videos to be 10, got %d", crawler.defaultConfig.MinChannelVideos)
	}

	// Test that config starts with default values
	if crawler.config.SamplingMethod != SamplingMethodChannel {
		t.Errorf("Expected initial config sampling method to be %s, got %s", SamplingMethodChannel, crawler.config.SamplingMethod)
	}

	if crawler.config.MinChannelVideos != 10 {
		t.Errorf("Expected initial config min channel videos to be 10, got %d", crawler.config.MinChannelVideos)
	}

	// Test that crawler is not initialized initially
	if crawler.initialized {
		t.Errorf("Expected crawler to not be initialized initially")
	}
}

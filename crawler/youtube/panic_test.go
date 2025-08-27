package youtube

import (
	"context"
	"testing"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/researchaccelerator-hub/telegram-scraper/model"
	youtubemodel "github.com/researchaccelerator-hub/telegram-scraper/model/youtube"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
)

// MockPanicClient is a mock YouTube client that panics on GetVideosFromChannel
type MockPanicClient struct{}

func (m *MockPanicClient) Connect(ctx context.Context) error {
	return nil
}

func (m *MockPanicClient) GetChannelInfo(ctx context.Context, channelID string) (*youtubemodel.YouTubeChannel, error) {
	return &youtubemodel.YouTubeChannel{
		ID:    channelID,
		Title: "Test Channel",
	}, nil
}

func (m *MockPanicClient) GetVideos(ctx context.Context, channelID string, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	panic("test panic in GetVideos")
}

func (m *MockPanicClient) GetVideosFromChannel(ctx context.Context, channelID string, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	panic("test panic in GetVideosFromChannel")
}

func (m *MockPanicClient) GetRandomVideos(ctx context.Context, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	panic("test panic in GetRandomVideos")
}

func (m *MockPanicClient) GetSnowballVideos(ctx context.Context, seedChannels []string, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	panic("test panic in GetSnowballVideos")
}

func (m *MockPanicClient) GetVideosByIDs(ctx context.Context, videoIDs []string) ([]*youtubemodel.YouTubeVideo, error) {
	panic("test panic in GetVideosByIDs")
}

func (m *MockPanicClient) Disconnect(ctx context.Context) error {
	return nil
}

// MockStateManager is a simple mock state manager
type MockStateManager struct{}

func (m *MockStateManager) Initialize(seedURLs []string) error    { return nil }
func (m *MockStateManager) GetPage(id string) (state.Page, error) { return state.Page{}, nil }
func (m *MockStateManager) UpdatePage(page state.Page) error      { return nil }
func (m *MockStateManager) UpdateMessage(pageID string, chatID int64, messageID int64, status string) error {
	return nil
}
func (m *MockStateManager) AddLayer(pages []state.Page) error                 { return nil }
func (m *MockStateManager) GetLayerByDepth(depth int) ([]state.Page, error)   { return nil, nil }
func (m *MockStateManager) GetMaxDepth() (int, error)                         { return 0, nil }
func (m *MockStateManager) SaveState() error                                  { return nil }
func (m *MockStateManager) ExportPagesToBinding(crawlID string) error         { return nil }
func (m *MockStateManager) StorePost(channelID string, post model.Post) error { return nil }
func (m *MockStateManager) StoreFile(channelID string, sourceFilePath string, fileName string) (string, string, error) {
	return "", "", nil
}
func (m *MockStateManager) GetPreviousCrawls() ([]string, error) { return nil, nil }
func (m *MockStateManager) UpdateCrawlMetadata(crawlID string, metadata map[string]interface{}) error {
	return nil
}
func (m *MockStateManager) FindIncompleteCrawl(crawlID string) (string, bool, error) {
	return "", false, nil
}
func (m *MockStateManager) HasProcessedMedia(mediaID string) (bool, error) { return false, nil }
func (m *MockStateManager) MarkMediaAsProcessed(mediaID string) error      { return nil }
func (m *MockStateManager) Close() error                                   { return nil }

// Used for random-walk sampling
func (m *MockStateManager) GetRandomDiscoveredChannel() (string, error)    { return "", nil }
func (m *MockStateManager) IsDiscoveredChannel(channelID string) bool      { return true }
func (m *MockStateManager) AddDiscoveredChannel(channelID string) error    { return nil }
func (m *MockStateManager) AddEdgeRecords(edges []*state.EdgeRecord) error { return nil }

func TestPanicRecovery(t *testing.T) {
	// Create a YouTube crawler with a mock client that will panic
	c := &YouTubeCrawler{
		client:       &MockPanicClient{},
		stateManager: &MockStateManager{},
		initialized:  true,
		config: YouTubeCrawlerConfig{
			SamplingMethod: SamplingMethodChannel,
		},
	}

	// Create a test job
	job := crawler.CrawlJob{
		Target: crawler.CrawlTarget{
			ID:   "test-channel",
			Type: crawler.PlatformYouTube,
		},
		FromTime:   time.Now().Add(-24 * time.Hour),
		ToTime:     time.Now(),
		Limit:      10,
		SampleSize: 0, // No sampling for tests
	}

	// Call FetchMessages which should panic but be recovered
	result, err := c.FetchMessages(context.Background(), job)

	// Verify that the panic was caught and converted to an error
	if err == nil {
		t.Fatal("Expected an error due to panic recovery, but got nil")
	}

	if len(result.Errors) == 0 {
		t.Fatal("Expected errors in result due to panic recovery, but got none")
	}

	if len(result.Posts) != 0 {
		t.Errorf("Expected 0 posts due to panic, but got %d", len(result.Posts))
	}

	// Check that the error message mentions the panic
	expectedSubstring := "panic in YouTube FetchMessages"
	if !containsSubstring(err.Error(), expectedSubstring) {
		t.Errorf("Expected error message to contain '%s', but got: %s", expectedSubstring, err.Error())
	}
}

func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && findSubstring(s, substr)
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

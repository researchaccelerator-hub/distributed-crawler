package youtube

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	youtubemodel "github.com/researchaccelerator-hub/telegram-scraper/model/youtube"
	"github.com/researchaccelerator-hub/telegram-scraper/null_handler"
)

// MockConcurrentClient is a mock YouTube client for testing concurrent access
type MockConcurrentClient struct{}

func (m *MockConcurrentClient) Connect(ctx context.Context) error { return nil }

func (m *MockConcurrentClient) GetChannelInfo(ctx context.Context, channelID string) (*youtubemodel.YouTubeChannel, error) {
	// Simulate some work and return different channel data for different IDs
	return &youtubemodel.YouTubeChannel{
		ID:              channelID,
		Title:           "Test Channel " + channelID,
		SubscriberCount: 1000,
		VideoCount:      100,
		ViewCount:       50000,
	}, nil
}

func (m *MockConcurrentClient) GetVideos(ctx context.Context, channelID string, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	return m.GetVideosFromChannel(ctx, channelID, fromTime, toTime, limit)
}

func (m *MockConcurrentClient) GetVideosFromChannel(ctx context.Context, channelID string, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	// Return a few test videos for this channel
	videos := make([]*youtubemodel.YouTubeVideo, 2)
	for i := range videos {
		videos[i] = &youtubemodel.YouTubeVideo{
			ID:        channelID + "_video_" + string(rune(i+'1')),
			ChannelID: channelID,
			Title:     "Test Video " + string(rune(i+'1')),
			ViewCount: int64(1000 + i*100),
		}
	}
	return videos, nil
}

func (m *MockConcurrentClient) GetRandomVideos(ctx context.Context, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	return []*youtubemodel.YouTubeVideo{}, nil
}

func (m *MockConcurrentClient) GetSnowballVideos(ctx context.Context, seedChannels []string, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	return []*youtubemodel.YouTubeVideo{}, nil
}

func (m *MockConcurrentClient) GetVideosByIDs(ctx context.Context, videoIDs []string) ([]*youtubemodel.YouTubeVideo, error) {
	return []*youtubemodel.YouTubeVideo{}, nil
}

func (m *MockConcurrentClient) Disconnect(ctx context.Context) error { return nil }

func TestConcurrentMapAccess(t *testing.T) {
	// Note: Channel cache is now managed by the YouTube client, not the crawler

	// Create a YouTube crawler
	c := &YouTubeCrawler{
		client:       &MockConcurrentClient{},
		stateManager: &MockStateManager{},
		initialized:  true,
		config: YouTubeCrawlerConfig{
			SamplingMethod: SamplingMethodChannel,
		},
	}

	// Run multiple FetchMessages concurrently to trigger map access race conditions
	const numGoroutines = 10
	const numChannelsPerGoroutine = 5

	var wg sync.WaitGroup
	errChan := make(chan error, numGoroutines*numChannelsPerGoroutine)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for i := 0; i < numChannelsPerGoroutine; i++ {
				channelID := "test_channel_" + string(rune('A'+goroutineID)) + "_" + string(rune('0'+i))

				job := crawler.CrawlJob{
					Target: crawler.CrawlTarget{
						ID:   channelID,
						Type: crawler.PlatformYouTube,
					},
					FromTime:      time.Now().Add(-24 * time.Hour),
					ToTime:        time.Now(),
					Limit:         2,
					SampleSize:    0, // No sampling for tests
					NullValidator: *null_handler.NewValidator(null_handler.PlatformYouTube),
				}

				_, err := c.FetchMessages(context.Background(), job)
				if err != nil {
					errChan <- err
				}
			}
		}(g)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(errChan)

	// Check for any errors
	for err := range errChan {
		t.Errorf("FetchMessages failed: %v", err)
	}

	// Note: Cache verification now happens within the YouTube client
	// The test passes if we get here without any concurrent map write panics
	t.Logf("Concurrent map access test passed - cache is managed by YouTube client")
}

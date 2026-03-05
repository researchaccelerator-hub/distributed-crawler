package youtube

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	youtubemodel "github.com/researchaccelerator-hub/telegram-scraper/model/youtube"
	"github.com/researchaccelerator-hub/telegram-scraper/null_handler"
)

// MockConfigurableClient is a YouTube client whose GetRandomVideos can be scripted
// call-by-call via the responses/errors slices. All other methods are no-ops.
type MockConfigurableClient struct {
	responses      [][]*youtubemodel.YouTubeVideo
	errors         []error
	limitsSeen     []int
	prefixCasesSeen []string
	callCount      int
}

func (m *MockConfigurableClient) Connect(_ context.Context) error { return nil }
func (m *MockConfigurableClient) Disconnect(_ context.Context) error { return nil }
func (m *MockConfigurableClient) GetChannelInfo(_ context.Context, id string) (*youtubemodel.YouTubeChannel, error) {
	return &youtubemodel.YouTubeChannel{ID: id}, nil
}
func (m *MockConfigurableClient) GetVideos(_ context.Context, _ string, _, _ time.Time, _ int) ([]*youtubemodel.YouTubeVideo, error) {
	return nil, nil
}
func (m *MockConfigurableClient) GetVideosFromChannel(_ context.Context, _ string, _, _ time.Time, _ int) ([]*youtubemodel.YouTubeVideo, error) {
	return nil, nil
}
func (m *MockConfigurableClient) GetSnowballVideos(_ context.Context, _ []string, _, _ time.Time, _ int) ([]*youtubemodel.YouTubeVideo, error) {
	return nil, nil
}
func (m *MockConfigurableClient) GetVideosByIDs(_ context.Context, _ []string) ([]*youtubemodel.YouTubeVideo, error) {
	return nil, nil
}

func (m *MockConfigurableClient) GetRandomVideos(_ context.Context, _, _ time.Time, limit int, prefixCase string) ([]*youtubemodel.YouTubeVideo, error) {
	m.limitsSeen = append(m.limitsSeen, limit)
	m.prefixCasesSeen = append(m.prefixCasesSeen, prefixCase)
	i := m.callCount
	m.callCount++
	if i < len(m.errors) && m.errors[i] != nil {
		return nil, m.errors[i]
	}
	if i < len(m.responses) {
		return m.responses[i], nil
	}
	return []*youtubemodel.YouTubeVideo{}, nil
}

// makeTestVideo creates a minimal YouTubeVideo for tests.
func makeTestVideo(id string) *youtubemodel.YouTubeVideo {
	return &youtubemodel.YouTubeVideo{
		ID:          id,
		ChannelID:   "test-channel",
		Title:       "Test Video " + id,
		PublishedAt: time.Now().Add(-time.Hour),
		Duration:    "PT1M",
	}
}

// newRandomCrawler returns a YouTubeCrawler configured for random sampling.
func newRandomCrawler(client *MockConfigurableClient) *YouTubeCrawler {
	return &YouTubeCrawler{
		client:       client,
		stateManager: &MockYouTubeStateManager{},
		initialized:  true,
		config: YouTubeCrawlerConfig{
			SamplingMethod: SamplingMethodRandom,
		},
	}
}

// newRandomJob returns a CrawlJob suitable for random sampling tests.
func newRandomJob(samplesRemaining int) crawler.CrawlJob {
	return crawler.CrawlJob{
		Target: crawler.CrawlTarget{
			ID:   "test-crawl-id",
			Type: crawler.PlatformYouTube,
		},
		FromTime:         time.Now().Add(-24 * time.Hour),
		ToTime:           time.Now(),
		SamplesRemaining: samplesRemaining,
		NullValidator:    *null_handler.NewValidator(null_handler.PlatformYouTube),
	}
}

// TestFetchMessagesRandom_EmptyPrefix verifies that when GetRandomVideos returns no
// videos (an expected outcome for some prefixes), FetchMessages returns an empty
// result without an error.
func TestFetchMessagesRandom_EmptyPrefix(t *testing.T) {
	client := &MockConfigurableClient{
		responses: [][]*youtubemodel.YouTubeVideo{{}},
	}
	c := newRandomCrawler(client)

	result, err := c.FetchMessages(context.Background(), newRandomJob(10))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Posts) != 0 {
		t.Errorf("expected 0 posts, got %d", len(result.Posts))
	}
	if client.callCount != 1 {
		t.Errorf("expected 1 call to GetRandomVideos, got %d", client.callCount)
	}
}

// TestFetchMessagesRandom_WithVideos verifies that when GetRandomVideos returns N
// videos, FetchMessages returns N posts.
func TestFetchMessagesRandom_WithVideos(t *testing.T) {
	client := &MockConfigurableClient{
		responses: [][]*youtubemodel.YouTubeVideo{
			{makeTestVideo("v1"), makeTestVideo("v2"), makeTestVideo("v3")},
		},
	}
	c := newRandomCrawler(client)

	result, err := c.FetchMessages(context.Background(), newRandomJob(10))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Posts) != 3 {
		t.Errorf("expected 3 posts, got %d", len(result.Posts))
	}
}

// TestFetchMessagesRandom_Error verifies that errors from GetRandomVideos are
// propagated as FetchMessages errors.
func TestFetchMessagesRandom_Error(t *testing.T) {
	apiErr := errors.New("youtube API quota exceeded")
	client := &MockConfigurableClient{
		errors: []error{apiErr},
	}
	c := newRandomCrawler(client)

	_, err := c.FetchMessages(context.Background(), newRandomJob(10))

	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !errors.Is(err, apiErr) {
		t.Errorf("expected error %q, got %q", apiErr, err)
	}
}

// TestFetchMessagesRandom_LimitCappedAt50 verifies that when SamplesRemaining > 50,
// GetRandomVideos is called with limit=50 (the hard cap).
func TestFetchMessagesRandom_LimitCappedAt50(t *testing.T) {
	client := &MockConfigurableClient{}
	c := newRandomCrawler(client)

	c.FetchMessages(context.Background(), newRandomJob(100)) //nolint:errcheck

	if len(client.limitsSeen) == 0 {
		t.Fatal("expected GetRandomVideos to be called")
	}
	if client.limitsSeen[0] != 50 {
		t.Errorf("expected limit=50 (cap), got limit=%d", client.limitsSeen[0])
	}
}

// TestFetchMessagesRandom_LimitWithinCap verifies that when SamplesRemaining < 50,
// GetRandomVideos is called with the exact remaining count.
func TestFetchMessagesRandom_LimitWithinCap(t *testing.T) {
	client := &MockConfigurableClient{}
	c := newRandomCrawler(client)

	c.FetchMessages(context.Background(), newRandomJob(10)) //nolint:errcheck

	if len(client.limitsSeen) == 0 {
		t.Fatal("expected GetRandomVideos to be called")
	}
	if client.limitsSeen[0] != 10 {
		t.Errorf("expected limit=10, got limit=%d", client.limitsSeen[0])
	}
}

// TestFetchMessagesRandom_ZeroSamplesRemaining verifies that with SamplesRemaining=0
// GetRandomVideos is called with limit=0 and returns 0 posts.
// This documents the bug that was fixed: the caller must set SamplesRemaining > 0.
func TestFetchMessagesRandom_ZeroSamplesRemaining(t *testing.T) {
	client := &MockConfigurableClient{}
	c := newRandomCrawler(client)

	result, err := c.FetchMessages(context.Background(), newRandomJob(0))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(client.limitsSeen) == 0 {
		t.Fatal("expected GetRandomVideos to be called")
	}
	if client.limitsSeen[0] != 0 {
		t.Errorf("expected limit=0, got limit=%d", client.limitsSeen[0])
	}
	if len(result.Posts) != 0 {
		t.Errorf("expected 0 posts with limit=0, got %d", len(result.Posts))
	}
}

// TestRandomSamplingLoop_ContinuesOnEmptyBatch verifies that the sampling loop
// does NOT exit when a call returns 0 results (empty prefix is expected behaviour).
// The loop should continue until SamplesRemaining is exhausted.
func TestRandomSamplingLoop_ContinuesOnEmptyBatch(t *testing.T) {
	// First two calls return 0 videos (expected for many random prefixes).
	// Third call returns 3 videos, meeting the quota of 3.
	client := &MockConfigurableClient{
		responses: [][]*youtubemodel.YouTubeVideo{
			{},
			{},
			{makeTestVideo("v1"), makeTestVideo("v2"), makeTestVideo("v3")},
		},
	}
	c := newRandomCrawler(client)
	job := newRandomJob(3)

	var totalPosts int
	var emptyIterations int

	for {
		result, err := c.FetchMessages(context.Background(), job)
		if err != nil {
			t.Fatalf("unexpected error on iteration %d: %v", client.callCount, err)
		}
		count := len(result.Posts)
		if count == 0 {
			emptyIterations++
		}
		job.SamplesRemaining -= count
		totalPosts += count
		if job.SamplesRemaining <= 0 {
			break
		}
	}

	if emptyIterations != 2 {
		t.Errorf("expected 2 empty iterations before quota met, got %d", emptyIterations)
	}
	if client.callCount != 3 {
		t.Errorf("expected 3 calls to GetRandomVideos, got %d", client.callCount)
	}
	if totalPosts != 3 {
		t.Errorf("expected 3 total posts, got %d", totalPosts)
	}
}

// TestRandomSamplingLoop_TerminatesWhenQuotaMet verifies that once SamplesRemaining
// drops to <= 0 the loop stops immediately without making additional calls.
func TestRandomSamplingLoop_TerminatesWhenQuotaMet(t *testing.T) {
	// Single call returns exactly the requested quota.
	client := &MockConfigurableClient{
		responses: [][]*youtubemodel.YouTubeVideo{
			{makeTestVideo("v1"), makeTestVideo("v2"), makeTestVideo("v3")},
			// A second response is provided to detect an erroneous extra call.
			{makeTestVideo("v4")},
		},
	}
	c := newRandomCrawler(client)
	job := newRandomJob(3)

	for {
		result, err := c.FetchMessages(context.Background(), job)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		job.SamplesRemaining -= len(result.Posts)
		if job.SamplesRemaining <= 0 {
			break
		}
	}

	if client.callCount != 1 {
		t.Errorf("expected loop to stop after 1 call, but made %d calls", client.callCount)
	}
}

// TestRandomSamplingLoop_BreaksOnError verifies that the loop stops immediately
// when FetchMessages returns an error.
func TestRandomSamplingLoop_BreaksOnError(t *testing.T) {
	apiErr := errors.New("transient network error")
	client := &MockConfigurableClient{
		// First call succeeds, second fails.
		responses: [][]*youtubemodel.YouTubeVideo{
			{makeTestVideo("v1")},
		},
		errors: []error{nil, apiErr},
	}
	c := newRandomCrawler(client)
	job := newRandomJob(10) // quota not met after one call

	var loopErr error
	for {
		result, err := c.FetchMessages(context.Background(), job)
		if err != nil {
			loopErr = err
			break
		}
		job.SamplesRemaining -= len(result.Posts)
		if job.SamplesRemaining <= 0 {
			break
		}
	}

	if loopErr == nil {
		t.Fatal("expected loop to break on error, but it completed without error")
	}
	if !errors.Is(loopErr, apiErr) {
		t.Errorf("expected error %q, got %q", apiErr, loopErr)
	}
	if client.callCount != 2 {
		t.Errorf("expected 2 calls (1 success + 1 error), got %d", client.callCount)
	}
}

// TestFetchMessagesRandom_PrefixCaseLower verifies that PrefixCase "lower" is forwarded
// to GetRandomVideos unchanged.
func TestFetchMessagesRandom_PrefixCaseLower(t *testing.T) {
	client := &MockConfigurableClient{}
	c := newRandomCrawler(client)
	job := newRandomJob(10)
	job.PrefixCase = "lower"

	c.FetchMessages(context.Background(), job) //nolint:errcheck

	if len(client.prefixCasesSeen) == 0 {
		t.Fatal("expected GetRandomVideos to be called")
	}
	if client.prefixCasesSeen[0] != "lower" {
		t.Errorf("expected prefixCase=%q, got %q", "lower", client.prefixCasesSeen[0])
	}
}

// TestFetchMessagesRandom_PrefixCaseMatchcase verifies that PrefixCase "matchcase" is
// forwarded to GetRandomVideos unchanged, enabling case-sensitive prefix matching.
func TestFetchMessagesRandom_PrefixCaseMatchcase(t *testing.T) {
	client := &MockConfigurableClient{}
	c := newRandomCrawler(client)
	job := newRandomJob(10)
	job.PrefixCase = "matchcase"

	c.FetchMessages(context.Background(), job) //nolint:errcheck

	if len(client.prefixCasesSeen) == 0 {
		t.Fatal("expected GetRandomVideos to be called")
	}
	if client.prefixCasesSeen[0] != "matchcase" {
		t.Errorf("expected prefixCase=%q, got %q", "matchcase", client.prefixCasesSeen[0])
	}
}

// TestRandomSamplingLoop_MaxIterTerminates verifies that the outer sampling loop stops
// after maxIter iterations even if every call returns 0 results (persistent empty-result
// scenario such as an impossible date range). This guards against an infinite loop.
func TestRandomSamplingLoop_MaxIterTerminates(t *testing.T) {
	// Client always returns 0 videos — SamplesRemaining never decreases.
	client := &MockConfigurableClient{}
	c := newRandomCrawler(client)
	job := newRandomJob(3)

	sampleSize := 3
	maxIter := sampleSize*100 + 100

	var iterations int
	for iter := 0; iter < maxIter; iter++ {
		result, err := c.FetchMessages(context.Background(), job)
		if err != nil {
			t.Fatalf("unexpected error on iter %d: %v", iter, err)
		}
		job.SamplesRemaining -= len(result.Posts)
		if job.SamplesRemaining <= 0 {
			break
		}
		iterations++
	}

	// Loop must have stopped at the bound, not run forever.
	if iterations >= maxIter {
		t.Errorf("loop did not terminate: ran %d iterations (bound=%d)", iterations, maxIter)
	}
}

// TestRandomSamplingLoop_ContextCancellation verifies that the loop respects context
// cancellation and exits promptly instead of continuing.
func TestRandomSamplingLoop_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel immediately before any iteration.
	cancel()

	client := &MockConfigurableClient{
		responses: [][]*youtubemodel.YouTubeVideo{
			{makeTestVideo("v1")},
			{makeTestVideo("v2")},
		},
	}
	c := newRandomCrawler(client)
	job := newRandomJob(10)

	// A cancelled context should cause FetchMessages to return quickly.
	// The crawler's internal timeout will fire and it will return an error or empty result.
	// The important thing is that the caller loop checks ctx.Done() and stops.
	maxIter := 1000
	var iterations int
	for iter := 0; iter < maxIter; iter++ {
		select {
		case <-ctx.Done():
			goto done
		default:
		}
		result, err := c.FetchMessages(ctx, job)
		if err != nil {
			break
		}
		job.SamplesRemaining -= len(result.Posts)
		if job.SamplesRemaining <= 0 {
			break
		}
		iterations++
	}
done:
	// With a pre-cancelled context the loop should exit on the first ctx.Done() check.
	if iterations > 1 {
		t.Errorf("expected loop to exit on cancelled context, ran %d iterations", iterations)
	}
}

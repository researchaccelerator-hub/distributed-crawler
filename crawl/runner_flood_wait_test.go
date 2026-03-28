package crawl

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/researchaccelerator-hub/telegram-scraper/telegramhelper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/zelenin/go-tdlib/client"
)

// poolMockTelegramService implements telegramhelper.TelegramService for pool-level
// tests. It always returns the same pre-built TDLib client, avoiding real
// Telegram authentication.
type poolMockTelegramService struct {
	tdlib crawler.TDLibClient
}

func (s *poolMockTelegramService) InitializeClient(_ string) (crawler.TDLibClient, error) {
	return s.tdlib, nil
}

func (s *poolMockTelegramService) InitializeClientWithConfig(_ string, _ common.CrawlerConfig) (crawler.TDLibClient, error) {
	return s.tdlib, nil
}

func (s *poolMockTelegramService) GetMe(_ crawler.TDLibClient) (*client.User, error) {
	return nil, nil
}

// messageWithMention builds a single message containing one @mention at the given offset.
func messageWithMention(text, username string) *client.Message {
	offset := int32(len(text) - len(username) - 1) // points past the '@'
	return &client.Message{
		Id:     1,
		ChatId: -100123,
		Content: &client.MessageText{
			Text: &client.FormattedText{
				Text: text,
				Entities: []*client.TextEntity{
					{
						Offset: offset,
						Length: int32(len(username) + 1), // include '@'
						Type:   &client.TextEntityTypeMention{},
					},
				},
			},
		},
	}
}

// TestStandardMode_ShortFloodWait verifies that a short FLOOD_WAIT (below the
// retire threshold) causes SearchPublicChat to be retried after sleeping, and
// that the channel is successfully validated on the retry call.
func TestStandardMode_ShortFloodWait(t *testing.T) {
	sm := new(MockStateManager)
	tdlib := new(MockTDLibClient)

	owner := &state.Page{
		ID:         "page-1",
		URL:        "source_channel",
		Depth:      0,
		Status:     "unfetched",
		SequenceID: "seq-1",
	}

	cfg := common.CrawlerConfig{
		SamplingMethod: "random-walk",
		TandemCrawl:    false,
		CrawlID:        "crawl-1",
		WalkbackRate:   0,
	}

	// Use FLOOD_WAIT_0 to sleep 0 seconds so the test is fast.
	floodErr := fmt.Errorf("FLOOD_WAIT_0")
	chatType := client.ChatTypeSupergroup{SupergroupId: 42}
	validChat := &client.Chat{Id: -100999, Type: &chatType}

	messages := []*client.Message{
		messageWithMention("Check out @flood_chan", "@flood_chan"),
	}
	info := &channelInfo{}

	sm.On("UpdateMessage", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	sm.On("UpdatePage", mock.Anything).Return(nil)
	sm.On("IsInvalidChannel", "flood_chan").Return(false)
	sm.On("IsDiscoveredChannel", "flood_chan").Return(false)
	sm.On("GetCachedChatID", "flood_chan").Return(int64(0), false)

	// First call returns FLOOD_WAIT; second call (retry) succeeds.
	tdlib.On("SearchPublicChat", mock.Anything).Return(nil, floodErr).Once()
	tdlib.On("SearchPublicChat", mock.Anything).Return(validChat, nil).Once()

	sm.On("AddDiscoveredChannel", "flood_chan").Return(nil)
	sm.On("UpsertSeedChannelChatID", "flood_chan", int64(-100999)).Return(nil)
	sm.On("AddPageToPageBuffer", mock.Anything).Return(nil)
	sm.On("SaveEdgeRecords", mock.Anything).Return(nil)

	processor := &MockMessageProcessor{}
	processor.On("ProcessMessage",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return([]string{"flood_chan"}, nil)

	_, err := processAllMessagesWithProcessor(
		tdlib, info, messages, "crawl-1", "source_channel", sm, processor, owner, cfg,
	)

	// No error — the retry succeeded.
	assert.NoError(t, err)

	// The channel must NOT have been blacklisted.
	sm.AssertNotCalled(t, "MarkChannelInvalid", mock.Anything, mock.Anything)

	// SearchPublicChat must have been called twice: once (FLOOD_WAIT) + once (retry).
	tdlib.AssertNumberOfCalls(t, "SearchPublicChat", 2)
}

// TestStandardMode_LongFloodWait verifies that a long FLOOD_WAIT (at or above the
// retire threshold) returns an error wrapping ErrFloodWaitRetire and does not
// blacklist the channel as not_found.
func TestStandardMode_LongFloodWait(t *testing.T) {
	sm := new(MockStateManager)
	tdlib := new(MockTDLibClient)

	owner := &state.Page{
		ID:         "page-1",
		URL:        "source_channel",
		Depth:      0,
		Status:     "unfetched",
		SequenceID: "seq-1",
	}

	cfg := common.CrawlerConfig{
		SamplingMethod: "random-walk",
		TandemCrawl:    false,
		CrawlID:        "crawl-1",
	}

	longBan := 72560 // 20 hours — well above threshold
	floodErr := fmt.Errorf("FLOOD_WAIT_%d", longBan)

	messages := []*client.Message{
		messageWithMention("Check out @target_chan", "@target_chan"),
	}
	info := &channelInfo{}

	sm.On("UpdateMessage", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	sm.On("UpdatePage", mock.Anything).Return(nil)
	sm.On("IsInvalidChannel", "target_chan").Return(false)
	sm.On("IsDiscoveredChannel", "target_chan").Return(false)
	sm.On("GetCachedChatID", "target_chan").Return(int64(0), false)

	// SearchPublicChat returns a long FLOOD_WAIT.
	tdlib.On("SearchPublicChat", mock.Anything).Return(nil, floodErr)

	processor := &MockMessageProcessor{}
	processor.On("ProcessMessage",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return([]string{"target_chan"}, nil)

	_, err := processAllMessagesWithProcessor(
		tdlib, info, messages, "crawl-1", "source_channel", sm, processor, owner, cfg,
	)

	// Error must wrap ErrFloodWaitRetire.
	assert.True(t, errors.Is(err, ErrFloodWaitRetire), "expected ErrFloodWaitRetire, got: %v", err)

	// The channel must NOT have been blacklisted.
	sm.AssertNotCalled(t, "MarkChannelInvalid", mock.Anything, mock.Anything)
}

// TestStandardMode_NotFoundError verifies that a genuine not-found error
// (non-FLOOD_WAIT) still calls MarkChannelInvalid as before.
func TestStandardMode_NotFoundError(t *testing.T) {
	sm := new(MockStateManager)
	tdlib := new(MockTDLibClient)

	owner := &state.Page{
		ID:         "page-1",
		URL:        "source_channel",
		Depth:      0,
		Status:     "unfetched",
		SequenceID: "seq-1",
	}

	cfg := common.CrawlerConfig{
		SamplingMethod: "random-walk",
		TandemCrawl:    false,
		CrawlID:        "crawl-1",
		WalkbackRate:   0,
	}

	notFoundErr := fmt.Errorf("[404] USERNAME_NOT_OCCUPIED")

	messages := []*client.Message{
		messageWithMention("Check out @ghost_chan", "@ghost_chan"),
	}
	info := &channelInfo{}

	sm.On("UpdateMessage", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	sm.On("UpdatePage", mock.Anything).Return(nil)
	sm.On("IsInvalidChannel", "ghost_chan").Return(false)
	sm.On("IsDiscoveredChannel", "ghost_chan").Return(false)
	sm.On("GetCachedChatID", "ghost_chan").Return(int64(0), false)

	tdlib.On("SearchPublicChat", mock.Anything).Return(nil, notFoundErr)
	sm.On("MarkChannelInvalid", "ghost_chan", "not_found").Return(nil)

	// Walkback triggered — no valid channels discovered.
	sm.On("GetRandomSeedChannel").Return("walkback_target", nil)
	sm.On("AddPageToPageBuffer", mock.Anything).Return(nil)
	sm.On("SaveEdgeRecords", mock.Anything).Return(nil)

	processor := &MockMessageProcessor{}
	processor.On("ProcessMessage",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return([]string{"ghost_chan"}, nil)

	_, err := processAllMessagesWithProcessor(
		tdlib, info, messages, "crawl-1", "source_channel", sm, processor, owner, cfg,
	)

	assert.NoError(t, err)
	// A real not-found error DOES blacklist the channel.
	sm.AssertCalled(t, "MarkChannelInvalid", "ghost_chan", "not_found")
}

// TestStandardMode_ShortFloodWait_RetryFailsWithNotFound verifies that when a short
// FLOOD_WAIT is followed by a genuine not-found error on the retry, the channel is
// blacklisted and no error is returned. This exercises the chat==nil exit path of the
// retry loop.
func TestStandardMode_ShortFloodWait_RetryFailsWithNotFound(t *testing.T) {
	sm := new(MockStateManager)
	tdlib := new(MockTDLibClient)

	owner := &state.Page{
		ID:         "page-1",
		URL:        "source_channel",
		Depth:      0,
		Status:     "unfetched",
		SequenceID: "seq-1",
	}

	cfg := common.CrawlerConfig{
		SamplingMethod: "random-walk",
		TandemCrawl:    false,
		CrawlID:        "crawl-1",
		WalkbackRate:   0,
	}

	floodErr := fmt.Errorf("FLOOD_WAIT_0")
	notFoundErr := fmt.Errorf("[404] USERNAME_NOT_OCCUPIED")

	messages := []*client.Message{
		messageWithMention("Check out @retry_chan", "@retry_chan"),
	}
	info := &channelInfo{}

	sm.On("UpdateMessage", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	sm.On("UpdatePage", mock.Anything).Return(nil)
	sm.On("IsInvalidChannel", "retry_chan").Return(false)
	sm.On("IsDiscoveredChannel", "retry_chan").Return(false)
	sm.On("GetCachedChatID", "retry_chan").Return(int64(0), false)

	// First call: FLOOD_WAIT_0 (sleep 0s); second call: genuine not-found.
	tdlib.On("SearchPublicChat", mock.Anything).Return(nil, floodErr).Once()
	tdlib.On("SearchPublicChat", mock.Anything).Return(nil, notFoundErr).Once()

	sm.On("MarkChannelInvalid", "retry_chan", "not_found").Return(nil)

	// Walkback triggered since no channels were discovered.
	sm.On("GetRandomSeedChannel").Return("walkback_target", nil)
	sm.On("AddPageToPageBuffer", mock.Anything).Return(nil)
	sm.On("SaveEdgeRecords", mock.Anything).Return(nil)

	processor := &MockMessageProcessor{}
	processor.On("ProcessMessage",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return([]string{"retry_chan"}, nil)

	_, err := processAllMessagesWithProcessor(
		tdlib, info, messages, "crawl-1", "source_channel", sm, processor, owner, cfg,
	)

	// No error — the not-found on retry is handled normally (channel skipped).
	assert.NoError(t, err)
	// The channel IS blacklisted because the retry returned a genuine not-found.
	sm.AssertCalled(t, "MarkChannelInvalid", "retry_chan", "not_found")
	// SearchPublicChat called twice: once (FLOOD_WAIT) + once (retry).
	tdlib.AssertNumberOfCalls(t, "SearchPublicChat", 2)
}

// TestRunForChannelWithPool_RetireOnFloodWait verifies that when RunForChannel
// returns an error wrapping ErrFloodWaitRetire, RunForChannelWithPool permanently
// retires the connection from the pool (pool shrinks to 0).
func TestRunForChannelWithPool_RetireOnFloodWait(t *testing.T) {
	tdlib := new(MockTDLibClient)
	tdlib.On("Close").Return(nil, nil)

	svc := &poolMockTelegramService{tdlib: tdlib}
	pool := telegramhelper.NewConnectionPoolForTesting(svc, 1)

	// Seed the pool with one available connection.
	ctx := context.Background()
	_, connID, err := pool.GetConnection(ctx)
	require.NoError(t, err)
	pool.ReleaseConnection(connID)

	// Install pool as global, restoring original on cleanup.
	poolMu.Lock()
	prev := connectionPool
	connectionPool = pool
	poolMu.Unlock()
	defer func() {
		poolMu.Lock()
		connectionPool = prev
		poolMu.Unlock()
	}()

	// Inject a stub that returns ErrFloodWaitRetire without running the TDLib pipeline.
	prevFn := runForChannelFn
	runForChannelFn = func(_ crawler.TDLibClient, _ *state.Page, _ string, _ state.StateManagementInterface, _ common.CrawlerConfig) ([]*state.Page, error) {
		return nil, fmt.Errorf("simulated long ban: %w", ErrFloodWaitRetire)
	}
	defer func() { runForChannelFn = prevFn }()

	sm := new(MockStateManager)
	p := &state.Page{ID: "p1", URL: "some_chan", SequenceID: "s1"}
	cfg := common.CrawlerConfig{SamplingMethod: "random-walk", CrawlID: "crawl-1"}

	_, procErr := RunForChannelWithPool(ctx, p, "", sm, cfg)

	// Error must propagate intact.
	assert.True(t, errors.Is(procErr, ErrFloodWaitRetire))
	// Connection was retired — pool is now empty.
	assert.Equal(t, 0, ConnectionPoolTotalSize())
}

// TestRunForChannelWithPool_ReleaseOnNormalError verifies that when RunForChannel
// returns a non-FLOOD_WAIT error, the connection is released back to the pool
// (pool remains at size 1).
func TestRunForChannelWithPool_ReleaseOnNormalError(t *testing.T) {
	tdlib := new(MockTDLibClient)

	svc := &poolMockTelegramService{tdlib: tdlib}
	pool := telegramhelper.NewConnectionPoolForTesting(svc, 1)

	ctx := context.Background()
	_, connID, err := pool.GetConnection(ctx)
	require.NoError(t, err)
	pool.ReleaseConnection(connID)

	poolMu.Lock()
	prev := connectionPool
	connectionPool = pool
	poolMu.Unlock()
	defer func() {
		poolMu.Lock()
		connectionPool = prev
		poolMu.Unlock()
	}()

	someErr := fmt.Errorf("some unrelated error")
	prevFn := runForChannelFn
	runForChannelFn = func(_ crawler.TDLibClient, _ *state.Page, _ string, _ state.StateManagementInterface, _ common.CrawlerConfig) ([]*state.Page, error) {
		return nil, someErr
	}
	defer func() { runForChannelFn = prevFn }()

	sm := new(MockStateManager)
	p := &state.Page{ID: "p1", URL: "some_chan", SequenceID: "s1"}
	cfg := common.CrawlerConfig{SamplingMethod: "random-walk", CrawlID: "crawl-1"}

	_, procErr := RunForChannelWithPool(ctx, p, "", sm, cfg)

	// Error must propagate intact.
	assert.ErrorIs(t, procErr, someErr)
	// Connection was released — pool still has 1 connection.
	assert.Equal(t, 1, ConnectionPoolTotalSize())
}

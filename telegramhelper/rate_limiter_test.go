package telegramhelper

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/common"
	tdlibclient "github.com/zelenin/go-tdlib/client"
	"golang.org/x/time/rate"
)

// --- helpers ---

func newUnlimitedCfg() common.TelegramRateLimitConfig {
	// rate.Inf — no blocking, no jitter, suitable for fast unit tests
	return common.TelegramRateLimitConfig{
		GetChatHistoryRate:        0, // 0 → rate.Inf via callsPerMinuteToRate
		SearchPublicChatRate:      0,
		GetSupergroupInfoRate:     0,
		GetChatHistoryJitterMs:    0,
		SearchPublicChatJitterMs:  0,
		GetSupergroupInfoJitterMs: 0,
	}
}

// callCountMock wraps MockTDLibClient and counts calls per method.
type callCountMock struct {
	MockTDLibClient
	chatHistoryCalls   atomic.Int32
	searchPublicCalls  atomic.Int32
	supergroupCalls    atomic.Int32
	basicGroupCalls    atomic.Int32
	getMessageCalls    atomic.Int32
}

func (m *callCountMock) GetChatHistory(req *tdlibclient.GetChatHistoryRequest) (*tdlibclient.Messages, error) {
	m.chatHistoryCalls.Add(1)
	return nil, nil
}

func (m *callCountMock) SearchPublicChat(req *tdlibclient.SearchPublicChatRequest) (*tdlibclient.Chat, error) {
	m.searchPublicCalls.Add(1)
	return nil, nil
}

func (m *callCountMock) GetSupergroupFullInfo(req *tdlibclient.GetSupergroupFullInfoRequest) (*tdlibclient.SupergroupFullInfo, error) {
	m.supergroupCalls.Add(1)
	return nil, nil
}

func (m *callCountMock) GetBasicGroupFullInfo(req *tdlibclient.GetBasicGroupFullInfoRequest) (*tdlibclient.BasicGroupFullInfo, error) {
	m.basicGroupCalls.Add(1)
	return nil, nil
}

func (m *callCountMock) GetMessage(req *tdlibclient.GetMessageRequest) (*tdlibclient.Message, error) {
	m.getMessageCalls.Add(1)
	return nil, nil
}

// --- unit tests ---

func TestCallsPerMinuteToRate(t *testing.T) {
	if callsPerMinuteToRate(0) != rate.Inf {
		t.Error("0 calls/min should produce rate.Inf")
	}
	if callsPerMinuteToRate(-1) != rate.Inf {
		t.Error("negative calls/min should produce rate.Inf")
	}

	r := callsPerMinuteToRate(60)
	expected := rate.Every(time.Second)
	if r != expected {
		t.Errorf("60 calls/min: got %v, want %v", r, expected)
	}

	r30 := callsPerMinuteToRate(30)
	expected30 := rate.Every(2 * time.Second)
	if r30 != expected30 {
		t.Errorf("30 calls/min: got %v, want %v", r30, expected30)
	}
}

func TestRateLimitedTDLibClient_DelegatesRateLimitedMethods(t *testing.T) {
	mock := &callCountMock{}
	wrapped := NewRateLimitedTDLibClient(mock, newUnlimitedCfg())

	wrapped.GetChatHistory(&tdlibclient.GetChatHistoryRequest{})
	wrapped.SearchPublicChat(&tdlibclient.SearchPublicChatRequest{})
	wrapped.GetSupergroupFullInfo(&tdlibclient.GetSupergroupFullInfoRequest{})
	wrapped.GetBasicGroupFullInfo(&tdlibclient.GetBasicGroupFullInfoRequest{})

	if mock.chatHistoryCalls.Load() != 1 {
		t.Errorf("GetChatHistory: expected 1 inner call, got %d", mock.chatHistoryCalls.Load())
	}
	if mock.searchPublicCalls.Load() != 1 {
		t.Errorf("SearchPublicChat: expected 1 inner call, got %d", mock.searchPublicCalls.Load())
	}
	if mock.supergroupCalls.Load() != 1 {
		t.Errorf("GetSupergroupFullInfo: expected 1 inner call, got %d", mock.supergroupCalls.Load())
	}
	if mock.basicGroupCalls.Load() != 1 {
		t.Errorf("GetBasicGroupFullInfo: expected 1 inner call, got %d", mock.basicGroupCalls.Load())
	}
}

func TestRateLimitedTDLibClient_DelegatesPassThroughMethods(t *testing.T) {
	mock := &callCountMock{}
	wrapped := NewRateLimitedTDLibClient(mock, newUnlimitedCfg())

	// Pass-through methods should reach the inner client without any rate limiting.
	wrapped.GetMessage(&tdlibclient.GetMessageRequest{})
	if mock.getMessageCalls.Load() != 1 {
		t.Errorf("GetMessage: expected 1 inner call, got %d", mock.getMessageCalls.Load())
	}
}

func TestRateLimitedTDLibClient_IndependentLimiters(t *testing.T) {
	// Use a slow rate for GetChatHistory and unlimited for SearchPublicChat.
	// Verify that calling SearchPublicChat is not blocked by the GetChatHistory limiter.
	cfg := common.TelegramRateLimitConfig{
		GetChatHistoryRate:   1, // 1/min → very slow
		SearchPublicChatRate: 0, // unlimited
		GetSupergroupInfoRate: 0,
	}
	mock := &callCountMock{}
	wrapped := NewRateLimitedTDLibClient(mock, cfg)

	// Consume the single GetChatHistory token.
	wrapped.GetChatHistory(&tdlibclient.GetChatHistoryRequest{})

	// SearchPublicChat should not be blocked by GetChatHistory's limiter.
	start := time.Now()
	wrapped.SearchPublicChat(&tdlibclient.SearchPublicChatRequest{})
	elapsed := time.Since(start)

	if elapsed > 100*time.Millisecond {
		t.Errorf("SearchPublicChat was unexpectedly delayed (%v); limiters should be independent", elapsed)
	}
}

func TestRateLimitedTDLibClient_RateIsEnforced(t *testing.T) {
	// 120 calls/min = 1 call per 500ms. The first call uses the pre-filled token (fast).
	// The second call must wait ~500ms.
	cfg := common.TelegramRateLimitConfig{
		GetChatHistoryRate:      120, // 1 per 500ms
		GetChatHistoryJitterMs:  0,   // no jitter so timing is predictable
		SearchPublicChatRate:    0,
		GetSupergroupInfoRate:   0,
	}
	mock := &callCountMock{}
	wrapped := NewRateLimitedTDLibClient(mock, cfg)

	wrapped.GetChatHistory(&tdlibclient.GetChatHistoryRequest{}) // uses pre-filled token

	start := time.Now()
	wrapped.GetChatHistory(&tdlibclient.GetChatHistoryRequest{}) // must wait for next token
	elapsed := time.Since(start)

	const minExpected = 400 * time.Millisecond
	if elapsed < minExpected {
		t.Errorf("second GetChatHistory call returned too quickly (%v < %v); rate limit not enforced", elapsed, minExpected)
	}
}

func TestRateLimitedTDLibClient_JitterZeroIsFast(t *testing.T) {
	// With jitter=0 and unlimited rate the call must complete well under 10ms.
	mock := &callCountMock{}
	wrapped := NewRateLimitedTDLibClient(mock, newUnlimitedCfg())

	start := time.Now()
	for i := 0; i < 10; i++ {
		wrapped.GetChatHistory(&tdlibclient.GetChatHistoryRequest{})
	}
	elapsed := time.Since(start)

	if elapsed > 50*time.Millisecond {
		t.Errorf("10 calls with no rate/jitter took %v; expected < 50ms", elapsed)
	}
}

// slowMock wraps callCountMock but makes GetMessage sleep to simulate a server hit.
type slowMock struct {
	callCountMock
	getMessageDelay time.Duration
}

func (m *slowMock) GetMessage(req *tdlibclient.GetMessageRequest) (*tdlibclient.Message, error) {
	m.getMessageCalls.Add(1)
	time.Sleep(m.getMessageDelay)
	return nil, nil
}

func TestRateLimitedTDLibClient_ReactiveGetMessage_CacheHitIsFree(t *testing.T) {
	// Inner returns instantly (cache hit, < 5ms) → limiter must not add any delay.
	mock := &slowMock{getMessageDelay: 0}
	cfg := common.TelegramRateLimitConfig{
		GetMessageServerHitRate:     1, // 1/min — would add ~60s delay if triggered
		GetMessageServerHitJitterMs: 0,
	}
	wrapped := NewRateLimitedTDLibClient(mock, cfg)

	// Consume the pre-filled token so any reactive wait would be obvious.
	wrapped.GetMessage(&tdlibclient.GetMessageRequest{})

	start := time.Now()
	wrapped.GetMessage(&tdlibclient.GetMessageRequest{})
	elapsed := time.Since(start)

	if elapsed > 100*time.Millisecond {
		t.Errorf("cache-hit GetMessage was throttled (%v); should be free", elapsed)
	}
}

func TestRateLimitedTDLibClient_ReactiveGetMessage_ServerHitThrottled(t *testing.T) {
	// Inner sleeps 20ms to trigger a server-hit detection (> 15ms threshold).
	// Rate: 120/min = 1 token per 500ms, burst=1.
	// First call uses the pre-filled token (no delay). Second call must wait ~500ms.
	mock := &slowMock{getMessageDelay: 20 * time.Millisecond}
	cfg := common.TelegramRateLimitConfig{
		GetMessageServerHitRate:     120, // 1 per 500ms
		GetMessageServerHitJitterMs: 0,   // no jitter for predictable timing
	}
	wrapped := NewRateLimitedTDLibClient(mock, cfg)

	wrapped.GetMessage(&tdlibclient.GetMessageRequest{}) // uses pre-filled token

	start := time.Now()
	wrapped.GetMessage(&tdlibclient.GetMessageRequest{}) // must wait for next token
	elapsed := time.Since(start)

	const minExpected = 400 * time.Millisecond
	if elapsed < minExpected {
		t.Errorf("server-hit GetMessage returned too quickly (%v < %v); reactive throttle not applied", elapsed, minExpected)
	}
}

func TestDefaultRateLimitConfig(t *testing.T) {
	cfg := common.DefaultTelegramRateLimitConfig()

	if cfg.GetChatHistoryRate != 30 {
		t.Errorf("GetChatHistoryRate: got %v, want 30", cfg.GetChatHistoryRate)
	}
	if cfg.SearchPublicChatRate != 6 {
		t.Errorf("SearchPublicChatRate: got %v, want 6", cfg.SearchPublicChatRate)
	}
	if cfg.GetSupergroupInfoRate != 20 {
		t.Errorf("GetSupergroupInfoRate: got %v, want 20", cfg.GetSupergroupInfoRate)
	}
	if cfg.GetChatHistoryJitterMs <= 0 {
		t.Error("GetChatHistoryJitterMs should be > 0")
	}
	if cfg.SearchPublicChatJitterMs <= 0 {
		t.Error("SearchPublicChatJitterMs should be > 0")
	}
	if cfg.GetSupergroupInfoJitterMs <= 0 {
		t.Error("GetSupergroupInfoJitterMs should be > 0")
	}
}

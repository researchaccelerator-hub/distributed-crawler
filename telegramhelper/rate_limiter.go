package telegramhelper

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/rs/zerolog/log"
	tdlibclient "github.com/zelenin/go-tdlib/client"
	"golang.org/x/time/rate"
)

// endpointStats tracks cache/server/flood-wait counts for a single API endpoint.
type endpointStats struct {
	cacheHits     int
	serverHits    int
	floodWaits    int
	totalTime     time.Duration
	rateLimitTime time.Duration
}

// APICallStats accumulates per-endpoint timing stats for a single channel processing run.
type APICallStats struct {
	mu        sync.Mutex
	endpoints map[string]*endpointStats
}

func newAPICallStats() *APICallStats {
	return &APICallStats{endpoints: make(map[string]*endpointStats)}
}

func (s *APICallStats) record(endpoint string, callDuration time.Duration, rateLimitWait time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.endpoints[endpoint]
	if !ok {
		e = &endpointStats{}
		s.endpoints[endpoint] = e
	}
	e.totalTime += callDuration
	e.rateLimitTime += rateLimitWait
	if callDuration >= 2*time.Second {
		e.floodWaits++
	} else if callDuration < 5*time.Millisecond {
		e.cacheHits++
	} else {
		e.serverHits++
	}
}

// Reset clears all accumulated stats for reuse between channels.
func (s *APICallStats) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.endpoints = make(map[string]*endpointStats)
}

// Log emits a single summary line per endpoint.
func (s *APICallStats) Log(channel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for endpoint, e := range s.endpoints {
		log.Info().
			Str("log_tag", "api_stats").
			Str("channel", channel).
			Str("api_endpoint", endpoint).
			Int("cache_hits", e.cacheHits).
			Int("server_hits", e.serverHits).
			Int("flood_waits", e.floodWaits).
			Dur("total_time", e.totalTime).
			Dur("rate_limit_time", e.rateLimitTime).
			Msg("API call stats")
	}
}

// RateLimitedTDLibClient wraps a TDLibClient and enforces independent per-method rate limits
// with configurable jitter. Each instance owns its own limiters, so connections in the pool
// never share quota.
//
// Proactive limiters (GetChatHistory, SearchPublicChat, GetSupergroupFullInfo,
// GetBasicGroupFullInfo) wait before the call. The GetMessage limiter is reactive:
// a token is consumed only when the call misses TDLib's local cache and actually hits
// the Telegram server, preserving the fast-path for cache hits.
type RateLimitedTDLibClient struct {
	inner               crawler.TDLibClient
	getChatHistoryLim   *rate.Limiter
	searchPublicChatLim *rate.Limiter
	supergroupInfoLim   *rate.Limiter
	getMessageLim       *rate.Limiter // reactive: token consumed only on cache miss

	getChatHistoryJitterMs      int
	searchPublicChatJitterMs    int
	supergroupInfoJitterMs      int
	getMessageServerHitJitterMs int

	Stats *APICallStats
}

// callsPerMinuteToRate converts a calls-per-minute value to a rate.Limit.
// A value <= 0 disables rate limiting (rate.Inf).
func callsPerMinuteToRate(callsPerMinute float64) rate.Limit {
	if callsPerMinute <= 0 {
		return rate.Inf
	}
	return rate.Every(time.Duration(float64(time.Minute) / callsPerMinute))
}

// NewRateLimitedTDLibClient wraps inner with per-method rate limiters configured by cfg.
func NewRateLimitedTDLibClient(inner crawler.TDLibClient, cfg common.TelegramRateLimitConfig) *RateLimitedTDLibClient {
	return &RateLimitedTDLibClient{
		inner:                       inner,
		getChatHistoryLim:           rate.NewLimiter(callsPerMinuteToRate(cfg.GetChatHistoryRate), 1),
		searchPublicChatLim:         rate.NewLimiter(callsPerMinuteToRate(cfg.SearchPublicChatRate), 1),
		supergroupInfoLim:           rate.NewLimiter(callsPerMinuteToRate(cfg.GetSupergroupInfoRate), 1),
		getMessageLim:               rate.NewLimiter(callsPerMinuteToRate(cfg.GetMessageServerHitRate), 1),
		getChatHistoryJitterMs:      cfg.GetChatHistoryJitterMs,
		searchPublicChatJitterMs:    cfg.SearchPublicChatJitterMs,
		supergroupInfoJitterMs:      cfg.GetSupergroupInfoJitterMs,
		getMessageServerHitJitterMs: cfg.GetMessageServerHitJitterMs,
		Stats:                       newAPICallStats(),
	}
}

// LogRateLimitConfig emits a single Info line with all configured rate limits and jitter.
// Call once per crawl at startup.
func LogRateLimitConfig(cfg common.TelegramRateLimitConfig) {
	log.Info().
		Float64("get_chat_history_rate_per_min", cfg.GetChatHistoryRate).
		Int("get_chat_history_jitter_ms", cfg.GetChatHistoryJitterMs).
		Float64("search_public_chat_rate_per_min", cfg.SearchPublicChatRate).
		Int("search_public_chat_jitter_ms", cfg.SearchPublicChatJitterMs).
		Float64("get_supergroup_info_rate_per_min", cfg.GetSupergroupInfoRate).
		Int("get_supergroup_info_jitter_ms", cfg.GetSupergroupInfoJitterMs).
		Float64("get_message_server_hit_rate_per_min", cfg.GetMessageServerHitRate).
		Int("get_message_server_hit_jitter_ms", cfg.GetMessageServerHitJitterMs).
		Msg("Telegram API rate limits configured")
}

// waitWithJitter blocks until the limiter grants a token, then sleeps a random jitter.
func (c *RateLimitedTDLibClient) waitWithJitter(lim *rate.Limiter, jitterMaxMs int, apiCall string) {
	lim.Wait(context.Background()) //nolint:errcheck // context.Background() never cancels
	jitterMs := 0
	if jitterMaxMs > 0 {
		jitterMs = rand.IntN(jitterMaxMs + 1)
	}
	log.Debug().Str("api_call", apiCall).Int("jitter_ms", jitterMs).Msg("Telegram API rate limit wait")
	if jitterMs > 0 {
		time.Sleep(time.Duration(jitterMs) * time.Millisecond)
	}
}

// --- Rate-limited methods ---

// callAndDetect waits for the rate limiter, then times only the actual inner call so that
// DetectCacheOrServer receives an accurate duration (not inflated by the rate limit wait).
func (c *RateLimitedTDLibClient) callAndDetect(lim *rate.Limiter, jitterMaxMs int, apiCall string, fn func() error) error {
	waitStart := time.Now()
	c.waitWithJitter(lim, jitterMaxMs, apiCall)
	rateLimitWait := time.Since(waitStart)
	start := time.Now()
	err := fn()
	duration := time.Since(start)
	DetectCacheOrServer(start, apiCall)
	c.Stats.record(apiCall, duration, rateLimitWait)
	return err
}

func (c *RateLimitedTDLibClient) GetChatHistory(req *tdlibclient.GetChatHistoryRequest) (*tdlibclient.Messages, error) {
	var result *tdlibclient.Messages
	err := c.callAndDetect(c.getChatHistoryLim, c.getChatHistoryJitterMs, "GetChatHistory", func() error {
		var e error
		result, e = c.inner.GetChatHistory(req)
		return e
	})
	return result, err
}

func (c *RateLimitedTDLibClient) SearchPublicChat(req *tdlibclient.SearchPublicChatRequest) (*tdlibclient.Chat, error) {
	var result *tdlibclient.Chat
	err := c.callAndDetect(c.searchPublicChatLim, c.searchPublicChatJitterMs, "SearchPublicChat", func() error {
		var e error
		result, e = c.inner.SearchPublicChat(req)
		return e
	})
	return result, err
}

func (c *RateLimitedTDLibClient) GetSupergroupFullInfo(req *tdlibclient.GetSupergroupFullInfoRequest) (*tdlibclient.SupergroupFullInfo, error) {
	var result *tdlibclient.SupergroupFullInfo
	err := c.callAndDetect(c.supergroupInfoLim, c.supergroupInfoJitterMs, "GetSupergroupFullInfo", func() error {
		var e error
		result, e = c.inner.GetSupergroupFullInfo(req)
		return e
	})
	return result, err
}

func (c *RateLimitedTDLibClient) GetBasicGroupFullInfo(req *tdlibclient.GetBasicGroupFullInfoRequest) (*tdlibclient.BasicGroupFullInfo, error) {
	var result *tdlibclient.BasicGroupFullInfo
	err := c.callAndDetect(c.supergroupInfoLim, c.supergroupInfoJitterMs, "GetBasicGroupFullInfo", func() error {
		var e error
		result, e = c.inner.GetBasicGroupFullInfo(req)
		return e
	})
	return result, err
}

// --- Pass-through methods ---

// GetMessage is handled reactively: a rate limiter token is consumed only when the call
// misses TDLib's local cache (i.e. hits the Telegram server). Cache hits are free.
// This preserves full throughput for the common case while throttling sustained server pressure.
func (c *RateLimitedTDLibClient) GetMessage(req *tdlibclient.GetMessageRequest) (*tdlibclient.Message, error) {
	start := time.Now()
	result, err := c.inner.GetMessage(req)
	duration := time.Since(start)
	cacheHit := DetectCacheOrServer(start, "GetMessage")
	var rateLimitWait time.Duration
	if !cacheHit {
		r := c.getMessageLim.Reserve()
		delay := r.Delay()
		jitterMs := 0
		if c.getMessageServerHitJitterMs > 0 {
			jitterMs = rand.IntN(c.getMessageServerHitJitterMs + 1)
		}
		rateLimitWait = delay + time.Duration(jitterMs)*time.Millisecond
		if rateLimitWait > 0 {
			log.Debug().
				Dur("throttle_delay_ms", delay).
				Int("jitter_ms", jitterMs).
				Str("api_call", "GetMessage").
				Msg("Telegram API reactive throttle (server hit)")
			time.Sleep(rateLimitWait)
		}
	}
	c.Stats.record("GetMessage", duration, rateLimitWait)

	return result, err
}

func (c *RateLimitedTDLibClient) GetMessageLink(req *tdlibclient.GetMessageLinkRequest) (*tdlibclient.MessageLink, error) {
	return c.inner.GetMessageLink(req)
}

func (c *RateLimitedTDLibClient) GetMessageThreadHistory(req *tdlibclient.GetMessageThreadHistoryRequest) (*tdlibclient.Messages, error) {
	return c.inner.GetMessageThreadHistory(req)
}

func (c *RateLimitedTDLibClient) GetMessageThread(req *tdlibclient.GetMessageThreadRequest) (*tdlibclient.MessageThreadInfo, error) {
	return c.inner.GetMessageThread(req)
}

func (c *RateLimitedTDLibClient) GetRemoteFile(req *tdlibclient.GetRemoteFileRequest) (*tdlibclient.File, error) {
	return c.inner.GetRemoteFile(req)
}

func (c *RateLimitedTDLibClient) DownloadFile(req *tdlibclient.DownloadFileRequest) (*tdlibclient.File, error) {
	return c.inner.DownloadFile(req)
}

func (c *RateLimitedTDLibClient) GetChat(req *tdlibclient.GetChatRequest) (*tdlibclient.Chat, error) {
	start := time.Now()
	result, err := c.inner.GetChat(req)
	c.Stats.record("GetChat", time.Since(start), 0)
	return result, err
}

func (c *RateLimitedTDLibClient) GetSupergroup(req *tdlibclient.GetSupergroupRequest) (*tdlibclient.Supergroup, error) {
	start := time.Now()
	result, err := c.inner.GetSupergroup(req)
	c.Stats.record("GetSupergroup", time.Since(start), 0)
	return result, err
}

func (c *RateLimitedTDLibClient) Close() (*tdlibclient.Ok, error) {
	return c.inner.Close()
}

func (c *RateLimitedTDLibClient) GetMe() (*tdlibclient.User, error) {
	return c.inner.GetMe()
}

func (c *RateLimitedTDLibClient) GetUser(req *tdlibclient.GetUserRequest) (*tdlibclient.User, error) {
	return c.inner.GetUser(req)
}

func (c *RateLimitedTDLibClient) DeleteFile(req *tdlibclient.DeleteFileRequest) (*tdlibclient.Ok, error) {
	return c.inner.DeleteFile(req)
}

package crawl

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/researchaccelerator-hub/telegram-scraper/telegramhelper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// ---------------------------------------------------------------------------
// validateSingleEdge tests
// ---------------------------------------------------------------------------

// mockValidateFn returns a ValidateFunc that returns the given result.
func mockValidateFn(result telegramhelper.ChannelValidationResult, err error) ValidateFunc {
	return func(_ string, _ *http.Client) (telegramhelper.ChannelValidationResult, error) {
		return result, err
	}
}

func TestValidateSingleEdge_Valid(t *testing.T) {
	sm := new(MockStateManager)
	cfg := common.CrawlerConfig{CrawlID: "crawl-1"}

	sm.On("IsInvalidChannel", "testchan").Return(false)
	sm.On("IsChannelDiscovered", "testchan", "crawl-1").Return(false, nil)
	sm.On("ClaimDiscoveredChannel", "testchan", "crawl-1").Return(true, nil)
	sm.On("UpsertSeedChannelChatID", "testchan", int64(0)).Return(nil)

	edge := &state.PendingEdge{
		PendingID:          1,
		BatchID:            "batch-1",
		CrawlID:            "crawl-1",
		DestinationChannel: "testchan",
		SourceType:         "mention",
		ValidationStatus:   "validating",
	}

	rl := telegramhelper.NewValidatorRateLimiter(0, 0)
	ctx := context.Background()
	vfn := mockValidateFn(telegramhelper.ChannelValidationResult{Status: "valid"}, nil)

	update := validateSingleEdge(ctx, sm, cfg, &http.Client{}, rl, edge, vfn)

	assert.Equal(t, 1, update.PendingID)
	assert.Equal(t, "valid", update.ValidationStatus)
	sm.AssertCalled(t, "ClaimDiscoveredChannel", "testchan", "crawl-1")
	sm.AssertCalled(t, "UpsertSeedChannelChatID", "testchan", int64(0))
}

func TestValidateSingleEdge_HTTPError(t *testing.T) {
	sm := new(MockStateManager)
	cfg := common.CrawlerConfig{CrawlID: "crawl-1"}

	sm.On("IsInvalidChannel", "testchan").Return(false)
	sm.On("IsChannelDiscovered", "testchan", "crawl-1").Return(false, nil)

	edge := &state.PendingEdge{
		PendingID:          1,
		BatchID:            "batch-1",
		CrawlID:            "crawl-1",
		DestinationChannel: "testchan",
		SourceType:         "mention",
		ValidationStatus:   "validating",
	}

	rl := telegramhelper.NewValidatorRateLimiter(0, 0)
	ctx := context.Background()
	vfn := mockValidateFn(telegramhelper.ChannelValidationResult{}, fmt.Errorf("connection refused"))

	update := validateSingleEdge(ctx, sm, cfg, &http.Client{}, rl, edge, vfn)

	assert.Equal(t, 1, update.PendingID)
	assert.Equal(t, "invalid", update.ValidationStatus)
	assert.Equal(t, "http_error", update.ValidationReason)
	// HTTP error path returns immediately without calling MarkChannelInvalid —
	// that only happens for successful HTTP responses that parse as invalid.
	sm.AssertNotCalled(t, "MarkChannelInvalid", mock.Anything, mock.Anything)
}

func TestValidateSingleEdge_NotChannel(t *testing.T) {
	sm := new(MockStateManager)
	cfg := common.CrawlerConfig{CrawlID: "crawl-1"}

	sm.On("IsInvalidChannel", "userchan").Return(false)
	sm.On("IsChannelDiscovered", "userchan", "crawl-1").Return(false, nil)
	sm.On("MarkChannelInvalid", "userchan", "not_supergroup").Return(nil)

	edge := &state.PendingEdge{
		PendingID:          3,
		CrawlID:            "crawl-1",
		DestinationChannel: "userchan",
	}

	rl := telegramhelper.NewValidatorRateLimiter(0, 0)
	ctx := context.Background()
	vfn := mockValidateFn(telegramhelper.ChannelValidationResult{Status: "not_channel", Reason: "not_supergroup"}, nil)

	update := validateSingleEdge(ctx, sm, cfg, &http.Client{}, rl, edge, vfn)

	assert.Equal(t, "not_channel", update.ValidationStatus)
	assert.Equal(t, "not_supergroup", update.ValidationReason)
	sm.AssertCalled(t, "MarkChannelInvalid", "userchan", "not_supergroup")
}

func TestValidateSingleEdge_AlreadyInvalid(t *testing.T) {
	sm := new(MockStateManager)
	cfg := common.CrawlerConfig{CrawlID: "crawl-1"}

	sm.On("IsInvalidChannel", "badchan").Return(true)

	edge := &state.PendingEdge{
		PendingID:          1,
		DestinationChannel: "badchan",
		CrawlID:            "crawl-1",
	}

	rl := telegramhelper.NewValidatorRateLimiter(0, 0)
	ctx := context.Background()
	// validateFn should never be called — pass nil to catch accidental calls
	update := validateSingleEdge(ctx, sm, cfg, &http.Client{}, rl, edge, nil)

	assert.Equal(t, "invalid", update.ValidationStatus)
	assert.Equal(t, "cached_invalid", update.ValidationReason)
	sm.AssertNotCalled(t, "IsChannelDiscovered", mock.Anything, mock.Anything)
}

func TestValidateSingleEdge_AlreadyDiscovered(t *testing.T) {
	sm := new(MockStateManager)
	cfg := common.CrawlerConfig{CrawlID: "crawl-1"}

	sm.On("IsInvalidChannel", "known_chan").Return(false)
	sm.On("IsChannelDiscovered", "known_chan", "crawl-1").Return(true, nil)

	edge := &state.PendingEdge{
		PendingID:          2,
		DestinationChannel: "known_chan",
		CrawlID:            "crawl-1",
	}

	rl := telegramhelper.NewValidatorRateLimiter(0, 0)
	ctx := context.Background()
	update := validateSingleEdge(ctx, sm, cfg, &http.Client{}, rl, edge, nil)

	assert.Equal(t, "already_discovered", update.ValidationStatus)
}

func TestValidateSingleEdge_ValidButRaceLost(t *testing.T) {
	sm := new(MockStateManager)
	cfg := common.CrawlerConfig{CrawlID: "crawl-1"}

	sm.On("IsInvalidChannel", "raced_chan").Return(false)
	sm.On("IsChannelDiscovered", "raced_chan", "crawl-1").Return(false, nil)
	sm.On("ClaimDiscoveredChannel", "raced_chan", "crawl-1").Return(false, nil) // another validator won

	edge := &state.PendingEdge{
		PendingID:          4,
		CrawlID:            "crawl-1",
		DestinationChannel: "raced_chan",
	}

	rl := telegramhelper.NewValidatorRateLimiter(0, 0)
	ctx := context.Background()
	vfn := mockValidateFn(telegramhelper.ChannelValidationResult{Status: "valid"}, nil)

	update := validateSingleEdge(ctx, sm, cfg, &http.Client{}, rl, edge, vfn)

	assert.Equal(t, "already_discovered", update.ValidationStatus)
}

// ---------------------------------------------------------------------------
// processWalkbackBatch tests
// ---------------------------------------------------------------------------

func TestProcessWalkbackBatch_ForcedWalkback(t *testing.T) {
	sm := new(MockStateManager)
	cfg := common.CrawlerConfig{WalkbackRate: 50}

	batch := &state.PendingEdgeBatch{
		BatchID:       "batch-1",
		CrawlID:       "crawl-1",
		SourceChannel: "source_chan",
		SourcePageID:  "page-1",
		SourceDepth:   0,
		SequenceID:    "seq-1",
	}

	// All edges invalid — no valid channels → forced walkback
	allEdges := []*state.PendingEdge{
		{PendingID: 1, ValidationStatus: "invalid", SourceType: "mention"},
		{PendingID: 2, ValidationStatus: "not_channel", SourceType: "url"},
	}

	sm.On("GetRandomDiscoveredChannel").Return("walkback_target", nil)
	sm.On("AddPageToPageBuffer", mock.MatchedBy(func(p *state.Page) bool {
		return p.URL == "walkback_target" && p.Depth == 1
	})).Return(nil)
	sm.On("SaveEdgeRecords", mock.MatchedBy(func(edges []*state.EdgeRecord) bool {
		return len(edges) == 1 && edges[0].Walkback && !edges[0].Skipped
	})).Return(nil)
	sm.On("FlushBatchStats", "batch-1", "crawl-1", allEdges).Return(nil)
	sm.On("CompletePendingBatch", "batch-1").Return(nil)

	ctx := context.Background()
	err := processWalkbackBatch(ctx, sm, cfg, batch, allEdges)

	assert.NoError(t, err)
	sm.AssertExpectations(t)
}

func TestProcessWalkbackBatch_Forward(t *testing.T) {
	sm := new(MockStateManager)
	cfg := common.CrawlerConfig{WalkbackRate: 0} // never walkback

	batch := &state.PendingEdgeBatch{
		BatchID:       "batch-2",
		CrawlID:       "crawl-1",
		SourceChannel: "source_chan",
		SourcePageID:  "page-1",
		SourceDepth:   0,
		SequenceID:    "seq-1",
	}

	allEdges := []*state.PendingEdge{
		{PendingID: 1, ValidationStatus: "valid", DestinationChannel: "chan_a", SourceType: "mention"},
		{PendingID: 2, ValidationStatus: "valid", DestinationChannel: "chan_b", SourceType: "url"},
		{PendingID: 3, ValidationStatus: "invalid", DestinationChannel: "bad_chan", SourceType: "plaintext"},
	}

	// Forward mode: should pick one of chan_a/chan_b, skip the other
	sm.On("AddPageToPageBuffer", mock.MatchedBy(func(p *state.Page) bool {
		return (p.URL == "chan_a" || p.URL == "chan_b") && p.Depth == 1
	})).Return(nil)
	sm.On("SaveEdgeRecords", mock.MatchedBy(func(edges []*state.EdgeRecord) bool {
		// 1 primary (not skipped) + 1 skipped = 2 edges
		if len(edges) != 2 {
			return false
		}
		primaryCount := 0
		skippedCount := 0
		for _, e := range edges {
			if !e.Skipped {
				primaryCount++
			} else {
				skippedCount++
			}
		}
		return primaryCount == 1 && skippedCount == 1
	})).Return(nil)
	sm.On("FlushBatchStats", "batch-2", "crawl-1", allEdges).Return(nil)
	sm.On("CompletePendingBatch", "batch-2").Return(nil)

	ctx := context.Background()
	err := processWalkbackBatch(ctx, sm, cfg, batch, allEdges)

	assert.NoError(t, err)
	sm.AssertExpectations(t)
	// GetRandomDiscoveredChannel should NOT be called in forward mode
	sm.AssertNotCalled(t, "GetRandomDiscoveredChannel")
}

func TestProcessWalkbackBatch_CompletionOrder(t *testing.T) {
	// Verify: AddPageToPageBuffer → SaveEdgeRecords → FlushBatchStats → CompletePendingBatch
	sm := new(MockStateManager)
	cfg := common.CrawlerConfig{WalkbackRate: 100} // always walkback

	batch := &state.PendingEdgeBatch{
		BatchID:       "batch-3",
		CrawlID:       "crawl-1",
		SourceChannel: "source_chan",
		SourcePageID:  "page-1",
		SourceDepth:   0,
		SequenceID:    "seq-1",
	}

	allEdges := []*state.PendingEdge{
		{PendingID: 1, ValidationStatus: "valid", DestinationChannel: "chan_a", SourceType: "mention"},
	}

	var callOrder []string
	sm.On("GetRandomDiscoveredChannel").Return("walkback_url", nil).Run(func(args mock.Arguments) {
		callOrder = append(callOrder, "GetRandomDiscoveredChannel")
	})
	sm.On("AddPageToPageBuffer", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		callOrder = append(callOrder, "AddPageToPageBuffer")
	})
	sm.On("SaveEdgeRecords", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		callOrder = append(callOrder, "SaveEdgeRecords")
	})
	sm.On("FlushBatchStats", mock.Anything, mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		callOrder = append(callOrder, "FlushBatchStats")
	})
	sm.On("CompletePendingBatch", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		callOrder = append(callOrder, "CompletePendingBatch")
	})

	ctx := context.Background()
	err := processWalkbackBatch(ctx, sm, cfg, batch, allEdges)

	assert.NoError(t, err)
	assert.Equal(t, []string{
		"GetRandomDiscoveredChannel",
		"AddPageToPageBuffer",
		"SaveEdgeRecords",
		"FlushBatchStats",
		"CompletePendingBatch",
	}, callOrder)
}

// ---------------------------------------------------------------------------
// RunValidationLoop context cancellation test
// ---------------------------------------------------------------------------

func TestRunValidationLoop_ContextCancellation(t *testing.T) {
	sm := new(MockStateManager)
	cfg := common.CrawlerConfig{
		ValidatorRequestRate:    6000, // fast for test
		ValidatorRequestJitterMs: 0,
		ValidatorClaimBatchSize: 5,
	}

	// Both goroutines will poll and find nothing, then ctx cancels
	sm.On("ClaimPendingEdges", 5).Return(([]*state.PendingEdge)(nil), nil)
	sm.On("ClaimWalkbackBatch").Return((*state.PendingEdgeBatch)(nil), ([]*state.PendingEdge)(nil), nil)
	sm.On("LoadInvalidChannels").Return(nil)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := RunValidationLoop(ctx, sm, cfg)

	// Should exit cleanly with context error
	assert.Error(t, err)
}

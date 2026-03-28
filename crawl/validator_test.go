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
	"golang.org/x/sync/errgroup"
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
	sm.On("IsChannelDiscovered", "testchan").Return(false, nil)
	sm.On("ClaimDiscoveredChannel", "testchan", "crawl-1", "").Return(true, nil)
	sm.On("InsertSeedChannelIfNew", "testchan").Return(nil)

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

	update, _ := validateSingleEdge(ctx, sm, cfg, &http.Client{}, rl, edge, vfn)

	assert.Equal(t, 1, update.PendingID)
	assert.Equal(t, "valid", update.ValidationStatus)
	sm.AssertCalled(t, "ClaimDiscoveredChannel", "testchan", "crawl-1", "")
	sm.AssertCalled(t, "InsertSeedChannelIfNew", "testchan")
}

func TestValidateSingleEdge_TransientError(t *testing.T) {
	sm := new(MockStateManager)
	cfg := common.CrawlerConfig{CrawlID: "crawl-1"}

	sm.On("IsInvalidChannel", "testchan").Return(false)
	sm.On("IsChannelDiscovered", "testchan").Return(false, nil)

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
	vfn := mockValidateFn(telegramhelper.ChannelValidationResult{}, &telegramhelper.ValidationHTTPError{
		Kind:    telegramhelper.ErrTransient,
		Wrapped: fmt.Errorf("connection refused"),
	})

	update, kind := validateSingleEdge(ctx, sm, cfg, &http.Client{}, rl, edge, vfn)

	assert.Equal(t, 1, update.PendingID)
	assert.Equal(t, "pending", update.ValidationStatus)
	assert.Equal(t, outcomeTransient, kind)
	sm.AssertNotCalled(t, "MarkChannelInvalid", mock.Anything, mock.Anything)
}

func TestValidateSingleEdge_BlockedError(t *testing.T) {
	sm := new(MockStateManager)
	cfg := common.CrawlerConfig{CrawlID: "crawl-1"}

	sm.On("IsInvalidChannel", "testchan").Return(false)
	sm.On("IsChannelDiscovered", "testchan").Return(false, nil)

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
	vfn := mockValidateFn(telegramhelper.ChannelValidationResult{}, &telegramhelper.ValidationHTTPError{
		Kind:    telegramhelper.ErrBlocked,
		Wrapped: fmt.Errorf("unexpected status 403"),
	})

	update, kind := validateSingleEdge(ctx, sm, cfg, &http.Client{}, rl, edge, vfn)

	assert.Equal(t, 1, update.PendingID)
	assert.Equal(t, "pending", update.ValidationStatus)
	assert.Equal(t, outcomeBlocked, kind)
	sm.AssertNotCalled(t, "MarkChannelInvalid", mock.Anything, mock.Anything)
}

func TestValidateSingleEdge_NotChannel(t *testing.T) {
	sm := new(MockStateManager)
	cfg := common.CrawlerConfig{CrawlID: "crawl-1"}

	sm.On("IsInvalidChannel", "userchan").Return(false)
	sm.On("IsChannelDiscovered", "userchan").Return(false, nil)
	sm.On("MarkChannelInvalid", "userchan", "not_supergroup").Return(nil)

	edge := &state.PendingEdge{
		PendingID:          3,
		CrawlID:            "crawl-1",
		DestinationChannel: "userchan",
	}

	rl := telegramhelper.NewValidatorRateLimiter(0, 0)
	ctx := context.Background()
	vfn := mockValidateFn(telegramhelper.ChannelValidationResult{Status: "not_channel", Reason: "not_supergroup"}, nil)

	update, _ := validateSingleEdge(ctx, sm, cfg, &http.Client{}, rl, edge, vfn)

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
	update, _ := validateSingleEdge(ctx, sm, cfg, &http.Client{}, rl, edge, nil)

	assert.Equal(t, "invalid", update.ValidationStatus)
	assert.Equal(t, "cached_invalid", update.ValidationReason)
	sm.AssertNotCalled(t, "IsChannelDiscovered", mock.Anything)
}

func TestValidateSingleEdge_AlreadyDiscovered(t *testing.T) {
	sm := new(MockStateManager)
	cfg := common.CrawlerConfig{CrawlID: "crawl-1"}

	sm.On("IsInvalidChannel", "known_chan").Return(false)
	sm.On("IsChannelDiscovered", "known_chan").Return(true, nil)

	edge := &state.PendingEdge{
		PendingID:          2,
		DestinationChannel: "known_chan",
		CrawlID:            "crawl-1",
	}

	rl := telegramhelper.NewValidatorRateLimiter(0, 0)
	ctx := context.Background()
	update, _ := validateSingleEdge(ctx, sm, cfg, &http.Client{}, rl, edge, nil)

	assert.Equal(t, "duplicate", update.ValidationStatus)
}

func TestValidateSingleEdge_ValidButRaceLost(t *testing.T) {
	sm := new(MockStateManager)
	cfg := common.CrawlerConfig{CrawlID: "crawl-1"}

	sm.On("IsInvalidChannel", "raced_chan").Return(false)
	sm.On("IsChannelDiscovered", "raced_chan").Return(false, nil)
	sm.On("ClaimDiscoveredChannel", "raced_chan", "crawl-1", "").Return(false, nil) // another validator won

	edge := &state.PendingEdge{
		PendingID:          4,
		CrawlID:            "crawl-1",
		DestinationChannel: "raced_chan",
	}

	rl := telegramhelper.NewValidatorRateLimiter(0, 0)
	ctx := context.Background()
	vfn := mockValidateFn(telegramhelper.ChannelValidationResult{Status: "valid"}, nil)

	update, _ := validateSingleEdge(ctx, sm, cfg, &http.Client{}, rl, edge, vfn)

	assert.Equal(t, "duplicate", update.ValidationStatus)
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

	sm.On("GetRandomSeedChannel").Return("walkback_target", 100, nil)
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
	// GetRandomSeedChannel should NOT be called in forward mode
	sm.AssertNotCalled(t, "GetRandomSeedChannel")
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
	sm.On("GetRandomSeedChannel").Return("walkback_url", 100, nil).Run(func(args mock.Arguments) {
		callOrder = append(callOrder, "GetRandomSeedChannel")
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
		"GetRandomSeedChannel",
		"AddPageToPageBuffer",
		"SaveEdgeRecords",
		"CompletePendingBatch",
		"FlushBatchStats",
	}, callOrder)
}

// ---------------------------------------------------------------------------
// RunValidationLoop context cancellation test
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// TC-4: RunValidationLoop processes edges when available
// ---------------------------------------------------------------------------

func TestRunValidationLoop_ProcessesAvailableEdge(t *testing.T) {
	// An edge whose channel is cached as invalid is processed without any HTTP
	// call — allowing the test to verify end-to-end edge processing without a
	// live HTTP server.
	sm := new(MockStateManager)
	cfg := common.CrawlerConfig{
		ValidatorRequestRate:     6000, // fast for test
		ValidatorRequestJitterMs: 0,
		ValidatorClaimBatchSize:  5,
	}

	edge := &state.PendingEdge{
		PendingID:          99,
		CrawlID:            "crawl-1",
		DestinationChannel: "known_bad",
	}

	// Return the edge on the first claim, then nothing on subsequent polls.
	sm.On("ClaimPendingEdges", 5).Return([]*state.PendingEdge{edge}, nil).Once()
	sm.On("ClaimPendingEdges", 5).Return(([]*state.PendingEdge)(nil), nil)

	sm.On("IsInvalidChannel", "known_bad").Return(true)
	sm.On("UpdatePendingEdge", state.PendingEdgeUpdate{
		PendingID:        99,
		ValidationStatus: "invalid",
		ValidationReason: "cached_invalid",
	}).Return(nil)

	sm.On("ClaimWalkbackBatch").Return((*state.PendingEdgeBatch)(nil), ([]*state.PendingEdge)(nil), nil)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	RunValidationLoop(ctx, sm, cfg) //nolint:errcheck — exits with context error

	sm.AssertCalled(t, "UpdatePendingEdge", state.PendingEdgeUpdate{
		PendingID:        99,
		ValidationStatus: "invalid",
		ValidationReason: "cached_invalid",
	})
}

// ---------------------------------------------------------------------------
// Blocked-state tests
// ---------------------------------------------------------------------------

// TestRunEdgeValidation_EntersBlockedState verifies that after blockedThreshold
// consecutive ErrBlocked outcomes the loop calls InsertAccessEvent and leaves
// subsequent edges as "pending".
func TestRunEdgeValidation_EntersBlockedState(t *testing.T) {
	sm := new(MockStateManager)
	cfg := common.CrawlerConfig{
		CrawlID:                  "crawl-1",
		ValidatorRequestRate:     6000,
		ValidatorRequestJitterMs: 0,
		ValidatorClaimBatchSize:  blockedThreshold + 1, // claim enough to trip the threshold
	}

	// Build blockedThreshold+1 edges that all return ErrBlocked.
	edges := make([]*state.PendingEdge, blockedThreshold+1)
	for i := range edges {
		edges[i] = &state.PendingEdge{
			PendingID:          i + 1,
			CrawlID:            "crawl-1",
			DestinationChannel: fmt.Sprintf("chan%d", i),
			ValidationStatus:   "validating",
		}
		sm.On("IsInvalidChannel", edges[i].DestinationChannel).Return(false)
		sm.On("IsChannelDiscovered", edges[i].DestinationChannel).Return(false, nil)
		sm.On("UpdatePendingEdge", state.PendingEdgeUpdate{
			PendingID:        i + 1,
			ValidationStatus: "pending",
		}).Return(nil)
	}

	// First claim returns all edges; subsequent claims return nothing.
	sm.On("ClaimPendingEdges", blockedThreshold+1).Return(edges, nil).Once()
	sm.On("ClaimPendingEdges", blockedThreshold+1).Return(([]*state.PendingEdge)(nil), nil)

	sm.On("InsertAccessEvent", "ip_blocked").Return(nil)
	sm.On("ClaimWalkbackBatch").Return((*state.PendingEdgeBatch)(nil), ([]*state.PendingEdge)(nil), nil)

	blockedFn := func(_ string, _ *http.Client) (telegramhelper.ChannelValidationResult, error) {
		return telegramhelper.ChannelValidationResult{}, &telegramhelper.ValidationHTTPError{
			Kind:    telegramhelper.ErrBlocked,
			Wrapped: fmt.Errorf("403 Forbidden"),
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	idle := newSharedIdleTracker(1, 0, nil)
	err := runEdgeValidation(ctx, sm, cfg, &http.Client{},
		telegramhelper.NewValidatorRateLimiter(0, 0), blockedThreshold+1, blockedFn, 0, idle)
	assert.Error(t, err) // exits via context cancellation

	sm.AssertCalled(t, "InsertAccessEvent", "ip_blocked")
	// All edges must be left pending, not marked invalid.
	for i := range edges {
		sm.AssertCalled(t, "UpdatePendingEdge", state.PendingEdgeUpdate{
			PendingID:        i + 1,
			ValidationStatus: "pending",
		})
	}
}

// TestRunEdgeValidation_IdleTimeout verifies that the validator exits cleanly
// after ValidatorIdleTimeout with no pending edges.
func TestRunEdgeValidation_IdleTimeout(t *testing.T) {
	sm := new(MockStateManager)
	cfg := common.CrawlerConfig{
		CrawlID:              "crawl-1",
		ValidatorIdleTimeout: 100 * time.Millisecond,
	}

	// Always return no edges
	sm.On("ClaimPendingEdges", 10).Return(([]*state.PendingEdge)(nil), nil)

	neverCalledFn := func(_ string, _ *http.Client) (telegramhelper.ChannelValidationResult, error) {
		t.Fatal("validate should never be called when there are no edges")
		return telegramhelper.ChannelValidationResult{}, nil
	}

	idle := newSharedIdleTracker(1, 100*time.Millisecond, nil)
	ctx := context.Background()
	err := runEdgeValidation(ctx, sm, cfg, &http.Client{},
		telegramhelper.NewValidatorRateLimiter(0, 0), 10, neverCalledFn, 0, idle)

	assert.NoError(t, err) // should exit cleanly, not via context cancellation
}

// TestRunEdgeValidation_IdleTimeoutResetsOnActivity verifies that the idle
// timeout resets when edges become available.
func TestRunEdgeValidation_IdleTimeoutResetsOnActivity(t *testing.T) {
	sm := new(MockStateManager)
	cfg := common.CrawlerConfig{
		CrawlID:                 "crawl-1",
		ValidatorIdleTimeout:    150 * time.Millisecond,
		ValidatorRequestRate:    6000,
		ValidatorRequestJitterMs: 0,
	}

	validEdge := &state.PendingEdge{
		PendingID:          1,
		CrawlID:            "crawl-1",
		DestinationChannel: "testchan",
		ValidationStatus:   "validating",
	}

	sm.On("IsInvalidChannel", "testchan").Return(false)
	sm.On("IsChannelDiscovered", "testchan").Return(false, nil)
	sm.On("ClaimDiscoveredChannel", "testchan", "crawl-1", "").Return(true, nil)
	sm.On("InsertSeedChannelIfNew", "testchan").Return(nil)
	sm.On("UpdatePendingEdge", mock.Anything).Return(nil)

	callCount := 0
	// Return empty, empty, one edge, then empty forever
	sm.On("ClaimPendingEdges", 10).Return(([]*state.PendingEdge)(nil), nil).Run(func(args mock.Arguments) {
		callCount++
	}).Once() // call 1: empty
	sm.On("ClaimPendingEdges", 10).Return(([]*state.PendingEdge)(nil), nil).Once() // call 2: empty
	sm.On("ClaimPendingEdges", 10).Return([]*state.PendingEdge{validEdge}, nil).Once() // call 3: edge
	sm.On("ClaimPendingEdges", 10).Return(([]*state.PendingEdge)(nil), nil) // call 4+: empty

	validFn := func(_ string, _ *http.Client) (telegramhelper.ChannelValidationResult, error) {
		return telegramhelper.ChannelValidationResult{Status: "valid"}, nil
	}

	idle := newSharedIdleTracker(1, 150*time.Millisecond, nil)
	start := time.Now()
	ctx := context.Background()
	err := runEdgeValidation(ctx, sm, cfg, &http.Client{},
		telegramhelper.NewValidatorRateLimiter(0, 0), 10, validFn, 0, idle)

	assert.NoError(t, err)
	// Should have taken longer than 150ms because the timer reset after the edge at call 3
	assert.Greater(t, time.Since(start), 150*time.Millisecond)
}

// TestRunEdgeValidation_ProbeResumesValidation verifies that after entering
// blocked state a successful probe clears the state and edges are processed again.
func TestRunEdgeValidation_ProbeResumesValidation(t *testing.T) {
	sm := new(MockStateManager)
	cfg := common.CrawlerConfig{
		CrawlID:                  "crawl-1",
		ValidatorRequestRate:     6000,
		ValidatorRequestJitterMs: 0,
		ValidatorClaimBatchSize:  blockedThreshold,
	}

	// Phase 1 edges — all blocked, tips us into blocked state.
	phase1 := make([]*state.PendingEdge, blockedThreshold)
	for i := range phase1 {
		phase1[i] = &state.PendingEdge{
			PendingID:          i + 1,
			CrawlID:            "crawl-1",
			DestinationChannel: fmt.Sprintf("blocked%d", i),
		}
		sm.On("IsInvalidChannel", phase1[i].DestinationChannel).Return(false)
		sm.On("IsChannelDiscovered", phase1[i].DestinationChannel).Return(false, nil)
		sm.On("UpdatePendingEdge", state.PendingEdgeUpdate{
			PendingID:        i + 1,
			ValidationStatus: "pending",
		}).Return(nil)
	}

	// Phase 2 edge — valid, processed after probe unblocks.
	phase2Edge := &state.PendingEdge{
		PendingID:          100,
		CrawlID:            "crawl-1",
		DestinationChannel: "goodchan",
	}
	sm.On("IsInvalidChannel", "goodchan").Return(false)
	sm.On("IsChannelDiscovered", "goodchan").Return(false, nil)
	sm.On("ClaimDiscoveredChannel", "goodchan", "crawl-1", "").Return(true, nil)
	sm.On("InsertSeedChannelIfNew", "goodchan").Return(nil)
	sm.On("UpdatePendingEdge", state.PendingEdgeUpdate{
		PendingID:        100,
		ValidationStatus: "valid",
	}).Return(nil)

	sm.On("ClaimPendingEdges", blockedThreshold).Return(phase1, nil).Once()
	sm.On("ClaimPendingEdges", blockedThreshold).Return([]*state.PendingEdge{phase2Edge}, nil).Once()
	sm.On("ClaimPendingEdges", blockedThreshold).Return(([]*state.PendingEdge)(nil), nil)

	sm.On("InsertAccessEvent", "ip_blocked").Return(nil)
	sm.On("ClaimWalkbackBatch").Return((*state.PendingEdgeBatch)(nil), ([]*state.PendingEdge)(nil), nil)

	callCount := 0
	validateFn := func(username string, _ *http.Client) (telegramhelper.ChannelValidationResult, error) {
		callCount++
		// The first blockedThreshold calls are for phase1 edges (blocked).
		// The next call is the probe (probeChannel = "telegram") — succeeds.
		// Calls after that are for phase2 edge — valid.
		if username == probeChannel {
			return telegramhelper.ChannelValidationResult{Status: "valid"}, nil
		}
		if username == "goodchan" {
			return telegramhelper.ChannelValidationResult{Status: "valid"}, nil
		}
		return telegramhelper.ChannelValidationResult{}, &telegramhelper.ValidationHTTPError{
			Kind:    telegramhelper.ErrBlocked,
			Wrapped: fmt.Errorf("403 Forbidden"),
		}
	}

	// Use a very short probe interval so the test doesn't wait 5 minutes.
	// We do this by manipulating time via a short context timeout: the test
	// sets lastProbeAt to zero implicitly (zero value), so the probe fires
	// immediately on the first blocked-state iteration.
	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()

	idle := newSharedIdleTracker(1, 0, nil)
	err := runEdgeValidation(ctx, sm, cfg, &http.Client{},
		telegramhelper.NewValidatorRateLimiter(0, 0), blockedThreshold, validateFn, 0, idle)
	assert.Error(t, err) // exits via context cancellation

	sm.AssertCalled(t, "InsertAccessEvent", "ip_blocked")
	sm.AssertCalled(t, "UpdatePendingEdge", state.PendingEdgeUpdate{
		PendingID:        100,
		ValidationStatus: "valid",
	})
}

// ---------------------------------------------------------------------------
// sharedIdleTracker tests
// ---------------------------------------------------------------------------

func TestSharedIdleTracker_AllIdleNoActiveCheck(t *testing.T) {
	// No crawl-active func → should shut down when all workers idle past timeout.
	idle := newSharedIdleTracker(2, 10*time.Millisecond, nil)
	idle.MarkIdle(0)
	idle.MarkIdle(1)
	time.Sleep(15 * time.Millisecond)
	assert.True(t, idle.ShouldShutdown())
}

func TestSharedIdleTracker_NotAllIdle(t *testing.T) {
	idle := newSharedIdleTracker(2, 10*time.Millisecond, nil)
	idle.MarkIdle(0)
	// Worker 1 is still active
	time.Sleep(15 * time.Millisecond)
	assert.False(t, idle.ShouldShutdown())
}

func TestSharedIdleTracker_CrawlActiveSuppressesShutdown(t *testing.T) {
	// All workers idle past timeout, but crawl is still active → don't shut down.
	crawlActive := func() bool { return true }
	idle := newSharedIdleTracker(1, 10*time.Millisecond, crawlActive)
	idle.MarkIdle(0)
	time.Sleep(15 * time.Millisecond)
	assert.False(t, idle.ShouldShutdown())
}

func TestSharedIdleTracker_CrawlInactiveAllowsShutdown(t *testing.T) {
	// All workers idle past timeout, crawl is done → shut down.
	crawlActive := func() bool { return false }
	idle := newSharedIdleTracker(1, 10*time.Millisecond, crawlActive)
	idle.MarkIdle(0)
	time.Sleep(15 * time.Millisecond)
	assert.True(t, idle.ShouldShutdown())
}

func TestSharedIdleTracker_ActivityResetsIdle(t *testing.T) {
	idle := newSharedIdleTracker(1, 50*time.Millisecond, nil)
	idle.MarkIdle(0)
	time.Sleep(20 * time.Millisecond)
	idle.MarkActive(0) // reset
	idle.MarkIdle(0)   // restart idle timer
	time.Sleep(20 * time.Millisecond)
	// Only 20ms since re-idle, not 50ms → should not shut down yet.
	assert.False(t, idle.ShouldShutdown())
}

func TestSharedIdleTracker_TimeoutDisabled(t *testing.T) {
	idle := newSharedIdleTracker(1, 0, nil) // timeout = 0 → disabled
	idle.MarkIdle(0)
	time.Sleep(5 * time.Millisecond)
	assert.False(t, idle.ShouldShutdown())
}

func TestSharedIdleTracker_SetWorkerCount(t *testing.T) {
	idle := newSharedIdleTracker(1, 10*time.Millisecond, nil)
	idle.MarkIdle(0)
	time.Sleep(15 * time.Millisecond)
	// Increase worker count — now worker 1 isn't idle yet.
	idle.SetWorkerCount(2)
	assert.False(t, idle.ShouldShutdown())
}

// ---------------------------------------------------------------------------
// runEdgeValidation — crawl-active suppresses idle timeout
// ---------------------------------------------------------------------------

func TestRunEdgeValidation_IdleTimeoutSuppressedWhenCrawlActive(t *testing.T) {
	sm := new(MockStateManager)
	cfg := common.CrawlerConfig{
		CrawlID:              "crawl-1",
		ValidatorIdleTimeout: 80 * time.Millisecond,
	}

	sm.On("ClaimPendingEdges", 10).Return(([]*state.PendingEdge)(nil), nil)

	neverCalledFn := func(_ string, _ *http.Client) (telegramhelper.ChannelValidationResult, error) {
		t.Fatal("validate should never be called")
		return telegramhelper.ChannelValidationResult{}, nil
	}

	// crawlActive always returns true → idle timeout should be suppressed.
	idle := newSharedIdleTracker(1, 80*time.Millisecond, func() bool { return true })

	// Use a context timeout to force exit — the worker should NOT exit via idle timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	err := runEdgeValidation(ctx, sm, cfg, &http.Client{},
		telegramhelper.NewValidatorRateLimiter(0, 0), 10, neverCalledFn, 0, idle)

	// Should exit via context cancellation, NOT idle timeout (which returns nil).
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

// ---------------------------------------------------------------------------
// workerManager tests
// ---------------------------------------------------------------------------

func TestWorkerManager_StartAndStop(t *testing.T) {
	sm := new(MockStateManager)
	cfg := common.CrawlerConfig{
		CrawlID:                  "crawl-1",
		ValidatorRequestRate:     6000,
		ValidatorRequestJitterMs: 0,
		ValidatorIdleTimeout:     50 * time.Millisecond,
	}

	sm.On("ClaimPendingEdges", 10).Return(([]*state.PendingEdge)(nil), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)
	idle := newSharedIdleTracker(1, 0, nil) // no idle timeout

	wm := newWorkerManager(ctx, g, sm, cfg, idle, 6000, 0, 10,
		func(_ string, _ *http.Client) (telegramhelper.ChannelValidationResult, error) {
			return telegramhelper.ChannelValidationResult{}, nil
		}, nil)

	// Start a worker with no proxy (direct connection).
	err := wm.StartWorker(0, "")
	assert.NoError(t, err)
	assert.Equal(t, 1, wm.ActiveCount())

	// Starting same worker again is a no-op.
	err = wm.StartWorker(0, "")
	assert.NoError(t, err)
	assert.Equal(t, 1, wm.ActiveCount())

	// Stop the worker.
	wm.StopWorker(0)
	assert.Equal(t, 0, wm.ActiveCount())

	// Stopping again is a no-op.
	wm.StopWorker(0)
	assert.Equal(t, 0, wm.ActiveCount())

	cancel()
}

func TestWorkerManager_MultipleWorkers(t *testing.T) {
	sm := new(MockStateManager)
	cfg := common.CrawlerConfig{
		CrawlID:                  "crawl-1",
		ValidatorRequestRate:     6000,
		ValidatorRequestJitterMs: 0,
	}

	sm.On("ClaimPendingEdges", 10).Return(([]*state.PendingEdge)(nil), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)
	idle := newSharedIdleTracker(3, 0, nil)

	wm := newWorkerManager(ctx, g, sm, cfg, idle, 6000, 0, 10,
		func(_ string, _ *http.Client) (telegramhelper.ChannelValidationResult, error) {
			return telegramhelper.ChannelValidationResult{}, nil
		}, nil)

	assert.NoError(t, wm.StartWorker(0, ""))
	assert.NoError(t, wm.StartWorker(1, ""))
	assert.NoError(t, wm.StartWorker(2, ""))
	assert.Equal(t, 3, wm.ActiveCount())

	wm.StopWorker(2)
	assert.Equal(t, 2, wm.ActiveCount())

	wm.StopWorker(1)
	wm.StopWorker(0)
	assert.Equal(t, 0, wm.ActiveCount())

	cancel()
}

// ---------------------------------------------------------------------------
// RunValidationLoop tests
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

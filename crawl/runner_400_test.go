// Package crawl provides functionality to crawl Telegram channels and extract data.
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

// ── isTDLib400 unit tests ─────────────────────────────────────────────────

func TestIsTDLib400_BracketedFormat(t *testing.T) {
	assert.True(t, isTDLib400(fmt.Errorf("[400] CHANNEL_INVALID")))
}

func TestIsTDLib400_UsernameInvalid_Unbracketed(t *testing.T) {
	assert.True(t, isTDLib400(fmt.Errorf("400 USERNAME_INVALID")))
}

func TestIsTDLib400_UsernameNotOccupied_Unbracketed(t *testing.T) {
	assert.True(t, isTDLib400(fmt.Errorf("400 USERNAME_NOT_OCCUPIED")))
}

func TestIsTDLib400_NoMessagesFoundInChat(t *testing.T) {
	assert.True(t, isTDLib400(fmt.Errorf("no messages found in the chat")))
}

func TestIsTDLib400_404NotMatched(t *testing.T) {
	// 404 is "not found", not "bad request" — must not trigger replacement
	assert.False(t, isTDLib400(fmt.Errorf("[404] USERNAME_NOT_OCCUPIED")))
}

func TestIsTDLib400_FloodWaitNotMatched(t *testing.T) {
	assert.False(t, isTDLib400(fmt.Errorf("FLOOD_WAIT_300")))
}

func TestIsTDLib400_NilError(t *testing.T) {
	assert.False(t, isTDLib400(nil))
}

func TestIsTDLib400_UnrelatedError(t *testing.T) {
	assert.False(t, isTDLib400(fmt.Errorf("network timeout")))
}

// A wrapped error whose inner message contains a 400 string must still be detected.
func TestIsTDLib400_WrappedError_Detected(t *testing.T) {
	inner := fmt.Errorf("400 USERNAME_INVALID")
	wrapped := fmt.Errorf("getChannelInfo failed: %w", inner)
	assert.True(t, isTDLib400(wrapped))
}

// ── getChannelInfoWithDeps: 400 error wrapping ────────────────────────────

// SearchPublicChat returning "400 USERNAME_INVALID" must produce an error
// that wraps ErrTDLib400.
func TestGetChannelInfoWithDeps_SearchPublicChat_UsernameInvalid_WrapsErrTDLib400(t *testing.T) {
	tdlib := new(MockTDLibClient)
	page := &state.Page{URL: "bad_channel"}
	cfg := common.CrawlerConfig{SamplingMethod: "random-walk"}

	tdlib.On("SearchPublicChat", mock.Anything).Return(nil, fmt.Errorf("400 USERNAME_INVALID"))

	_, _, err := getChannelInfoWithDeps(tdlib, page, nil, nil, nil, 0, cfg)

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrTDLib400), "expected ErrTDLib400, got: %v", err)
}

// SearchPublicChat returning "400 USERNAME_NOT_OCCUPIED" must wrap ErrTDLib400.
func TestGetChannelInfoWithDeps_SearchPublicChat_UsernameNotOccupied_WrapsErrTDLib400(t *testing.T) {
	tdlib := new(MockTDLibClient)
	page := &state.Page{URL: "gone_channel"}
	cfg := common.CrawlerConfig{SamplingMethod: "random-walk"}

	tdlib.On("SearchPublicChat", mock.Anything).Return(nil, fmt.Errorf("400 USERNAME_NOT_OCCUPIED"))

	_, _, err := getChannelInfoWithDeps(tdlib, page, nil, nil, nil, 0, cfg)

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrTDLib400), "expected ErrTDLib400, got: %v", err)
}

// GetChat returning "[400] CHANNEL_INVALID" on the cached-chat-ID path must
// wrap ErrTDLib400.
func TestGetChannelInfoWithDeps_GetChat_CachedPath_WrapsErrTDLib400(t *testing.T) {
	tdlib := new(MockTDLibClient)
	page := &state.Page{URL: "cached_bad_channel"}
	cfg := common.CrawlerConfig{SamplingMethod: "random-walk"}

	// cachedChatID != 0 routes through GetChat, not SearchPublicChat.
	tdlib.On("GetChat", mock.Anything).Return(nil, fmt.Errorf("[400] CHANNEL_INVALID"))

	_, _, err := getChannelInfoWithDeps(tdlib, page, nil, nil, nil, 12345, cfg)

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrTDLib400), "expected ErrTDLib400, got: %v", err)
}

// A non-400 error from SearchPublicChat must NOT be wrapped as ErrTDLib400.
func TestGetChannelInfoWithDeps_SearchPublicChat_NonFatalError_NotWrapped(t *testing.T) {
	tdlib := new(MockTDLibClient)
	page := &state.Page{URL: "some_channel"}
	cfg := common.CrawlerConfig{SamplingMethod: "random-walk"}

	tdlib.On("SearchPublicChat", mock.Anything).Return(nil, fmt.Errorf("network timeout"))

	_, _, err := getChannelInfoWithDeps(tdlib, page, nil, nil, nil, 0, cfg)

	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrTDLib400), "did not expect ErrTDLib400, got: %v", err)
}

// ── isChannelActiveWithinPeriod / getLatestMessageTime ─────────────────────

// When GetChatHistory returns zero messages, getLatestMessageTime must return
// an error containing "no messages found in the chat".
func TestGetLatestMessageTime_EmptyHistory_ReturnsNoMessagesError(t *testing.T) {
	tdlib := new(MockTDLibClient)

	tdlib.On("GetChatHistory", mock.Anything).Return(
		&client.Messages{Messages: []*client.Message{}}, nil,
	)

	_, err := getLatestMessageTime(tdlib, 12345)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no messages found in the chat")
	// The error string must be detected by isTDLib400.
	assert.True(t, isTDLib400(err))
}

// When GetChatHistory itself errors, getLatestMessageTime propagates the error.
func TestGetLatestMessageTime_GetChatHistoryError_Propagated(t *testing.T) {
	tdlib := new(MockTDLibClient)

	tdlib.On("GetChatHistory", mock.Anything).Return(nil, fmt.Errorf("chat history unavailable"))

	_, err := getLatestMessageTime(tdlib, 12345)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "chat history unavailable")
}

// ── Handle400Replacement: core logic ──────────────────────────────────────

// Happy path: forward edge with a skipped edge available.
// The skipped edge is promoted and added to the page buffer; no walkback fires.
func TestHandle400Replacement_ForwardEdge_SkippedEdgeAvailable(t *testing.T) {
	sm := new(MockStateManager)
	page := &state.Page{
		ID:         "p1",
		URL:        "bad_channel",
		SequenceID: "seq-1",
		ParentID:   "parent-1",
		Depth:      2,
	}
	cfg := common.CrawlerConfig{CrawlID: "crawl-1"}

	forwardEdge := &state.EdgeRecord{
		DestinationChannel: "bad_channel",
		SourceChannel:      "source_chan",
		SequenceID:         "seq-1",
		Walkback:           false,
	}
	skippedEdge := &state.EdgeRecord{
		DestinationChannel: "skipped_chan",
		SourceChannel:      "source_chan",
		SequenceID:         "seq-1",
		Skipped:            true,
	}

	sm.On("MarkChannelInvalid", "bad_channel", "tdlib_400").Return(nil)
	sm.On("MarkSeedChannelInvalid", "bad_channel").Return(nil)
	sm.On("GetEdgeRecord", "seq-1", "bad_channel").Return(forwardEdge, nil)
	sm.On("DeleteEdgeRecord", "seq-1", "bad_channel").Return(nil)
	sm.On("GetRandomSkippedEdge", "seq-1", "source_chan").Return(skippedEdge, nil)
	sm.On("PromoteEdge", "seq-1", "skipped_chan").Return(nil)
	sm.On("AddPageToPageBuffer", mock.MatchedBy(func(p *state.Page) bool {
		return p.URL == "skipped_chan" && p.SequenceID == "seq-1" && p.Depth == 2
	})).Return(nil)

	err := Handle400Replacement(sm, page, cfg)

	assert.NoError(t, err)
	sm.AssertCalled(t, "MarkChannelInvalid", "bad_channel", "tdlib_400")
	sm.AssertCalled(t, "DeleteEdgeRecord", "seq-1", "bad_channel")
	sm.AssertCalled(t, "PromoteEdge", "seq-1", "skipped_chan")
	sm.AssertCalled(t, "AddPageToPageBuffer", mock.Anything)
	// Walkback must NOT fire when a skipped edge is available.
	sm.AssertNotCalled(t, "GetRandomSeedChannel")
	sm.AssertNotCalled(t, "SaveEdgeRecords", mock.Anything)
}

// Forward edge with no skipped edges remaining → falls back to walkback.
func TestHandle400Replacement_ForwardEdge_NoSkippedEdges_FallsBackToWalkback(t *testing.T) {
	sm := new(MockStateManager)
	page := &state.Page{
		ID:         "p1",
		URL:        "bad_channel",
		SequenceID: "seq-1",
		ParentID:   "parent-1",
		Depth:      2,
	}
	cfg := common.CrawlerConfig{CrawlID: "crawl-1"}

	forwardEdge := &state.EdgeRecord{
		DestinationChannel: "bad_channel",
		SourceChannel:      "source_chan",
		SequenceID:         "seq-1",
		Walkback:           false,
	}

	sm.On("MarkChannelInvalid", "bad_channel", "tdlib_400").Return(nil)
	sm.On("MarkSeedChannelInvalid", "bad_channel").Return(nil)
	sm.On("GetEdgeRecord", "seq-1", "bad_channel").Return(forwardEdge, nil)
	sm.On("DeleteEdgeRecord", "seq-1", "bad_channel").Return(nil)
	sm.On("GetRandomSkippedEdge", "seq-1", "source_chan").Return((*state.EdgeRecord)(nil), nil)
	sm.On("GetRandomSeedChannel").Return("walkback_chan", 100, nil)
	sm.On("AddPageToPageBuffer", mock.MatchedBy(func(p *state.Page) bool {
		return p.URL == "walkback_chan"
	})).Return(nil)
	sm.On("SaveEdgeRecords", mock.Anything).Return(nil)

	err := Handle400Replacement(sm, page, cfg)

	assert.NoError(t, err)
	sm.AssertCalled(t, "GetRandomSeedChannel")
	sm.AssertNotCalled(t, "PromoteEdge", mock.Anything, mock.Anything)
}

// Forward edge, GetRandomSkippedEdge returns an error → falls back to walkback.
func TestHandle400Replacement_ForwardEdge_GetSkippedEdgeErrors_FallsBackToWalkback(t *testing.T) {
	sm := new(MockStateManager)
	page := &state.Page{
		ID:         "p1",
		URL:        "bad_channel",
		SequenceID: "seq-1",
		ParentID:   "parent-1",
		Depth:      2,
	}
	cfg := common.CrawlerConfig{CrawlID: "crawl-1"}

	forwardEdge := &state.EdgeRecord{
		DestinationChannel: "bad_channel",
		SourceChannel:      "source_chan",
		SequenceID:         "seq-1",
		Walkback:           false,
	}

	sm.On("MarkChannelInvalid", "bad_channel", "tdlib_400").Return(nil)
	sm.On("MarkSeedChannelInvalid", "bad_channel").Return(nil)
	sm.On("GetEdgeRecord", "seq-1", "bad_channel").Return(forwardEdge, nil)
	sm.On("DeleteEdgeRecord", "seq-1", "bad_channel").Return(nil)
	sm.On("GetRandomSkippedEdge", "seq-1", "source_chan").Return((*state.EdgeRecord)(nil), fmt.Errorf("db timeout"))
	sm.On("GetRandomSeedChannel").Return("walkback_chan", 100, nil)
	sm.On("AddPageToPageBuffer", mock.Anything).Return(nil)
	sm.On("SaveEdgeRecords", mock.Anything).Return(nil)

	err := Handle400Replacement(sm, page, cfg)

	assert.NoError(t, err)
	sm.AssertCalled(t, "GetRandomSeedChannel")
}

// Original edge was a walkback → make another walkback decision.
func TestHandle400Replacement_WalkbackEdge_MakesAnotherWalkback(t *testing.T) {
	sm := new(MockStateManager)
	page := &state.Page{
		ID:         "p1",
		URL:        "bad_walkback_channel",
		SequenceID: "seq-1",
		ParentID:   "parent-1",
		Depth:      3,
	}
	cfg := common.CrawlerConfig{CrawlID: "crawl-1"}

	walkbackEdge := &state.EdgeRecord{
		DestinationChannel: "bad_walkback_channel",
		SourceChannel:      "original_source",
		SequenceID:         "seq-1",
		Walkback:           true,
	}

	sm.On("MarkChannelInvalid", "bad_walkback_channel", "tdlib_400").Return(nil)
	sm.On("MarkSeedChannelInvalid", "bad_walkback_channel").Return(nil)
	sm.On("GetEdgeRecord", "seq-1", "bad_walkback_channel").Return(walkbackEdge, nil)
	sm.On("DeleteEdgeRecord", "seq-1", "bad_walkback_channel").Return(nil)
	sm.On("GetRandomSeedChannel").Return("new_walkback_chan", 100, nil)
	sm.On("AddPageToPageBuffer", mock.MatchedBy(func(p *state.Page) bool {
		return p.URL == "new_walkback_chan"
	})).Return(nil)
	sm.On("SaveEdgeRecords", mock.MatchedBy(func(edges []*state.EdgeRecord) bool {
		return len(edges) == 1 && edges[0].Walkback && edges[0].SequenceID == "seq-1"
	})).Return(nil)

	err := Handle400Replacement(sm, page, cfg)

	assert.NoError(t, err)
	// Skipped-edge path must NOT be tried when the original was a walkback.
	sm.AssertNotCalled(t, "GetRandomSkippedEdge", mock.Anything, mock.Anything)
	sm.AssertCalled(t, "GetRandomSeedChannel")
	sm.AssertCalled(t, "SaveEdgeRecords", mock.Anything)
}

// No edge record found (GetEdgeRecord returns nil, nil) → falls back to
// walkback using the channel URL itself as the source channel.
func TestHandle400Replacement_NoEdgeRecord_FallsBackToWalkback(t *testing.T) {
	sm := new(MockStateManager)
	page := &state.Page{
		ID:         "p1",
		URL:        "orphan_channel",
		SequenceID: "seq-1",
		ParentID:   "parent-1",
		Depth:      1,
	}
	cfg := common.CrawlerConfig{CrawlID: "crawl-1"}

	sm.On("MarkChannelInvalid", "orphan_channel", "tdlib_400").Return(nil)
	sm.On("MarkSeedChannelInvalid", "orphan_channel").Return(nil)
	sm.On("GetEdgeRecord", "seq-1", "orphan_channel").Return((*state.EdgeRecord)(nil), nil)
	sm.On("DeleteEdgeRecord", "seq-1", "orphan_channel").Return(nil)
	sm.On("IsSeedChannel", "orphan_channel").Return(false)
	sm.On("GetRandomSeedChannel").Return("walkback_chan", 100, nil)
	sm.On("AddPageToPageBuffer", mock.Anything).Return(nil)
	sm.On("SaveEdgeRecords", mock.Anything).Return(nil)

	err := Handle400Replacement(sm, page, cfg)

	assert.NoError(t, err)
	sm.AssertCalled(t, "GetRandomSeedChannel")
}

// No edge record + IsSeedChannel = true → picks from seed_channels, no edge written.
func TestHandle400Replacement_SeedChannel_PicksFromSeedTable(t *testing.T) {
	sm := new(MockStateManager)
	page := &state.Page{
		ID:         "p1",
		URL:        "seed_chan",
		SequenceID: "seq-1",
		Depth:      0,
	}
	cfg := common.CrawlerConfig{CrawlID: "crawl-1"}

	sm.On("MarkChannelInvalid", "seed_chan", "tdlib_400").Return(nil)
	sm.On("MarkSeedChannelInvalid", "seed_chan").Return(nil)
	sm.On("GetEdgeRecord", "seq-1", "seed_chan").Return((*state.EdgeRecord)(nil), nil)
	sm.On("DeleteEdgeRecord", "seq-1", "seed_chan").Return(nil)
	sm.On("IsSeedChannel", "seed_chan").Return(true)
	sm.On("GetRandomSeedChannel").Return("replacement_seed", 100, nil)
	sm.On("AddPageToPageBuffer", mock.Anything).Return(nil)

	err := Handle400Replacement(sm, page, cfg)

	assert.NoError(t, err)
	sm.AssertCalled(t, "GetRandomSeedChannel")
	sm.AssertNotCalled(t, "SaveEdgeRecords", mock.Anything)
	sm.AssertNotCalled(t, "GetRandomDiscoveredChannel")
}

// GetEdgeRecord returning an error must be propagated to the caller.
func TestHandle400Replacement_GetEdgeRecordError_Propagated(t *testing.T) {
	sm := new(MockStateManager)
	page := &state.Page{
		ID:         "p1",
		URL:        "bad_channel",
		SequenceID: "seq-1",
	}
	cfg := common.CrawlerConfig{CrawlID: "crawl-1"}

	sm.On("MarkChannelInvalid", "bad_channel", "tdlib_400").Return(nil)
	sm.On("MarkSeedChannelInvalid", "bad_channel").Return(nil)
	sm.On("GetEdgeRecord", "seq-1", "bad_channel").Return((*state.EdgeRecord)(nil), fmt.Errorf("db timeout"))

	err := Handle400Replacement(sm, page, cfg)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "db timeout")
	// No page must have been added to the buffer.
	sm.AssertNotCalled(t, "AddPageToPageBuffer", mock.Anything)
}

// MarkChannelInvalid failure is non-fatal — replacement still proceeds.
func TestHandle400Replacement_MarkInvalidFails_ReplacementStillProceeds(t *testing.T) {
	sm := new(MockStateManager)
	page := &state.Page{
		ID:         "p1",
		URL:        "bad_channel",
		SequenceID: "seq-1",
		ParentID:   "parent-1",
		Depth:      1,
	}
	cfg := common.CrawlerConfig{CrawlID: "crawl-1"}

	forwardEdge := &state.EdgeRecord{
		DestinationChannel: "bad_channel",
		SourceChannel:      "source_chan",
		SequenceID:         "seq-1",
	}
	skippedEdge := &state.EdgeRecord{
		DestinationChannel: "skipped_chan",
		SourceChannel:      "source_chan",
		SequenceID:         "seq-1",
		Skipped:            true,
	}

	sm.On("MarkChannelInvalid", "bad_channel", "tdlib_400").Return(fmt.Errorf("db write failed"))
	sm.On("MarkSeedChannelInvalid", "bad_channel").Return(nil)
	sm.On("GetEdgeRecord", "seq-1", "bad_channel").Return(forwardEdge, nil)
	sm.On("DeleteEdgeRecord", "seq-1", "bad_channel").Return(nil)
	sm.On("GetRandomSkippedEdge", "seq-1", "source_chan").Return(skippedEdge, nil)
	sm.On("PromoteEdge", "seq-1", "skipped_chan").Return(nil)
	sm.On("AddPageToPageBuffer", mock.Anything).Return(nil)

	err := Handle400Replacement(sm, page, cfg)

	// MarkChannelInvalid failed, but replacement must still succeed.
	assert.NoError(t, err)
	sm.AssertCalled(t, "AddPageToPageBuffer", mock.Anything)
}

// DeleteEdgeRecord failure is non-fatal — replacement still proceeds.
func TestHandle400Replacement_DeleteEdgeRecordFails_ReplacementStillProceeds(t *testing.T) {
	sm := new(MockStateManager)
	page := &state.Page{
		ID:         "p1",
		URL:        "bad_channel",
		SequenceID: "seq-1",
		ParentID:   "parent-1",
		Depth:      1,
	}
	cfg := common.CrawlerConfig{CrawlID: "crawl-1"}

	forwardEdge := &state.EdgeRecord{
		DestinationChannel: "bad_channel",
		SourceChannel:      "source_chan",
		SequenceID:         "seq-1",
	}
	skippedEdge := &state.EdgeRecord{
		DestinationChannel: "skipped_chan",
		SourceChannel:      "source_chan",
		SequenceID:         "seq-1",
		Skipped:            true,
	}

	sm.On("MarkChannelInvalid", "bad_channel", "tdlib_400").Return(nil)
	sm.On("MarkSeedChannelInvalid", "bad_channel").Return(nil)
	sm.On("GetEdgeRecord", "seq-1", "bad_channel").Return(forwardEdge, nil)
	sm.On("DeleteEdgeRecord", "seq-1", "bad_channel").Return(fmt.Errorf("delete failed"))
	sm.On("GetRandomSkippedEdge", "seq-1", "source_chan").Return(skippedEdge, nil)
	sm.On("PromoteEdge", "seq-1", "skipped_chan").Return(nil)
	sm.On("AddPageToPageBuffer", mock.Anything).Return(nil)

	err := Handle400Replacement(sm, page, cfg)

	assert.NoError(t, err)
	sm.AssertCalled(t, "AddPageToPageBuffer", mock.Anything)
}

// PromoteEdge failure is non-fatal — the replacement page is still buffered.
func TestHandle400Replacement_PromoteEdgeFails_PageStillBuffered(t *testing.T) {
	sm := new(MockStateManager)
	page := &state.Page{
		ID:         "p1",
		URL:        "bad_channel",
		SequenceID: "seq-1",
		ParentID:   "parent-1",
		Depth:      1,
	}
	cfg := common.CrawlerConfig{CrawlID: "crawl-1"}

	forwardEdge := &state.EdgeRecord{
		DestinationChannel: "bad_channel",
		SourceChannel:      "source_chan",
		SequenceID:         "seq-1",
	}
	skippedEdge := &state.EdgeRecord{
		DestinationChannel: "skipped_chan",
		SourceChannel:      "source_chan",
		SequenceID:         "seq-1",
		Skipped:            true,
	}

	sm.On("MarkChannelInvalid", "bad_channel", "tdlib_400").Return(nil)
	sm.On("MarkSeedChannelInvalid", "bad_channel").Return(nil)
	sm.On("GetEdgeRecord", "seq-1", "bad_channel").Return(forwardEdge, nil)
	sm.On("DeleteEdgeRecord", "seq-1", "bad_channel").Return(nil)
	sm.On("GetRandomSkippedEdge", "seq-1", "source_chan").Return(skippedEdge, nil)
	sm.On("PromoteEdge", "seq-1", "skipped_chan").Return(fmt.Errorf("promote failed"))
	sm.On("AddPageToPageBuffer", mock.Anything).Return(nil)

	err := Handle400Replacement(sm, page, cfg)

	assert.NoError(t, err)
	sm.AssertCalled(t, "AddPageToPageBuffer", mock.Anything)
}

// AddPageToPageBuffer failure must be returned as an error.
func TestHandle400Replacement_AddPageToBufferFails_ReturnsError(t *testing.T) {
	sm := new(MockStateManager)
	page := &state.Page{
		ID:         "p1",
		URL:        "bad_channel",
		SequenceID: "seq-1",
		ParentID:   "parent-1",
		Depth:      1,
	}
	cfg := common.CrawlerConfig{CrawlID: "crawl-1"}

	forwardEdge := &state.EdgeRecord{
		DestinationChannel: "bad_channel",
		SourceChannel:      "source_chan",
		SequenceID:         "seq-1",
	}
	skippedEdge := &state.EdgeRecord{
		DestinationChannel: "skipped_chan",
		SourceChannel:      "source_chan",
		SequenceID:         "seq-1",
		Skipped:            true,
	}

	sm.On("MarkChannelInvalid", "bad_channel", "tdlib_400").Return(nil)
	sm.On("MarkSeedChannelInvalid", "bad_channel").Return(nil)
	sm.On("GetEdgeRecord", "seq-1", "bad_channel").Return(forwardEdge, nil)
	sm.On("DeleteEdgeRecord", "seq-1", "bad_channel").Return(nil)
	sm.On("GetRandomSkippedEdge", "seq-1", "source_chan").Return(skippedEdge, nil)
	sm.On("PromoteEdge", "seq-1", "skipped_chan").Return(nil)
	sm.On("AddPageToPageBuffer", mock.Anything).Return(fmt.Errorf("buffer write failed"))

	err := Handle400Replacement(sm, page, cfg)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "buffer write failed")
}

// The promoted replacement page must carry the original sequence ID (same chain),
// not a new one — forward edges continue the existing chain.
func TestHandle400Replacement_PromotedPage_InheritsOriginalSequenceID(t *testing.T) {
	sm := new(MockStateManager)
	page := &state.Page{
		ID:         "p1",
		URL:        "bad_channel",
		SequenceID: "original-seq",
		ParentID:   "parent-1",
		Depth:      4,
	}
	cfg := common.CrawlerConfig{CrawlID: "crawl-1"}

	forwardEdge := &state.EdgeRecord{
		DestinationChannel: "bad_channel",
		SourceChannel:      "source_chan",
		SequenceID:         "original-seq",
	}
	skippedEdge := &state.EdgeRecord{
		DestinationChannel: "skipped_chan",
		SourceChannel:      "source_chan",
		SequenceID:         "original-seq",
		Skipped:            true,
	}

	var capturedPage *state.Page
	sm.On("MarkChannelInvalid", "bad_channel", "tdlib_400").Return(nil)
	sm.On("MarkSeedChannelInvalid", "bad_channel").Return(nil)
	sm.On("GetEdgeRecord", "original-seq", "bad_channel").Return(forwardEdge, nil)
	sm.On("DeleteEdgeRecord", "original-seq", "bad_channel").Return(nil)
	sm.On("GetRandomSkippedEdge", "original-seq", "source_chan").Return(skippedEdge, nil)
	sm.On("PromoteEdge", "original-seq", "skipped_chan").Return(nil)
	sm.On("AddPageToPageBuffer", mock.MatchedBy(func(p *state.Page) bool {
		capturedPage = p
		return true
	})).Return(nil)

	require.NoError(t, Handle400Replacement(sm, page, cfg))

	require.NotNil(t, capturedPage)
	assert.Equal(t, "original-seq", capturedPage.SequenceID, "promoted page must continue the same chain")
	assert.Equal(t, "skipped_chan", capturedPage.URL)
	assert.Equal(t, 4, capturedPage.Depth)
}

// ── handle400Walkback: sequence ID semantics ───────────────────────────────

// The walkback edge_record must carry the original sequenceID (chain continuity).
// The new walkback page must carry a brand-new sequenceID (starts a fresh chain).
func TestHandle400Walkback_SequenceIDSemantics(t *testing.T) {
	sm := new(MockStateManager)
	page := &state.Page{
		ID:         "p1",
		URL:        "bad_channel",
		SequenceID: "original-seq",
		ParentID:   "parent-1",
		Depth:      2,
	}

	var capturedEdges []*state.EdgeRecord
	var capturedPage *state.Page

	sm.On("GetRandomSeedChannel").Return("walkback_target", 100, nil)
	sm.On("AddPageToPageBuffer", mock.MatchedBy(func(p *state.Page) bool {
		capturedPage = p
		return true
	})).Return(nil)
	sm.On("SaveEdgeRecords", mock.MatchedBy(func(edges []*state.EdgeRecord) bool {
		capturedEdges = edges
		return true
	})).Return(nil)

	err := handle400Walkback(sm, page, "source_chan", "original-seq")
	require.NoError(t, err)

	// Edge record: belongs to the current chain (preserves original seq).
	require.Len(t, capturedEdges, 1)
	assert.Equal(t, "original-seq", capturedEdges[0].SequenceID)
	assert.True(t, capturedEdges[0].Walkback)
	assert.Equal(t, "walkback_target", capturedEdges[0].DestinationChannel)
	assert.Equal(t, "source_chan", capturedEdges[0].SourceChannel)

	// Page: starts a NEW chain (fresh sequence ID, not the original).
	require.NotNil(t, capturedPage)
	assert.NotEqual(t, "original-seq", capturedPage.SequenceID, "walkback page must start a new chain")
	assert.Equal(t, "walkback_target", capturedPage.URL)
	assert.Equal(t, 2, capturedPage.Depth)
}

// Walkback exhaustion: all GetRandomSeedChannel attempts return the
// excluded channel → must return ErrWalkbackExhausted.
func TestHandle400Walkback_AllCandidatesExcluded_ReturnsErrWalkbackExhausted(t *testing.T) {
	sm := new(MockStateManager)
	page := &state.Page{
		ID:         "p1",
		URL:        "bad_channel",
		SequenceID: "seq-1",
	}

	// Always returns the excluded channel — exhausts all maxWalkbackAttempts.
	sm.On("GetRandomSeedChannel").Return("bad_channel", 100, nil)

	err := handle400Walkback(sm, page, "source_chan", "seq-1")

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrWalkbackExhausted))
}

// Walkback succeeds when the first few candidates are excluded but one eventually passes.
func TestHandle400Walkback_ExcludedCandidatesThenSuccess(t *testing.T) {
	sm := new(MockStateManager)
	page := &state.Page{
		ID:         "p1",
		URL:        "bad_channel",
		SequenceID: "seq-1",
		ParentID:   "parent-1",
		Depth:      1,
	}

	// First two calls return the excluded channel; third returns a valid one.
	sm.On("GetRandomSeedChannel").Return("bad_channel", 100, nil).Times(2)
	sm.On("GetRandomSeedChannel").Return("good_walkback", 100, nil).Once()
	sm.On("AddPageToPageBuffer", mock.Anything).Return(nil)
	sm.On("SaveEdgeRecords", mock.Anything).Return(nil)

	err := handle400Walkback(sm, page, "source_chan", "seq-1")

	assert.NoError(t, err)
	sm.AssertNumberOfCalls(t, "GetRandomSeedChannel", 3)
}

// ── RunForChannelWithPool: ErrTDLib400 propagation ─────────────────────────

// When runForChannelFn returns ErrTDLib400, RunForChannelWithPool must:
//   - propagate the error (errors.Is(err, ErrTDLib400) == true)
//   - release the connection back to the pool (not retire it)
func TestRunForChannelWithPool_400Error_PropagatedAndConnectionReleased(t *testing.T) {
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

	prevFn := runForChannelFn
	runForChannelFn = func(_ crawler.TDLibClient, _ *state.Page, _ string, _ state.StateManagementInterface, _ common.CrawlerConfig) ([]*state.Page, error) {
		return nil, fmt.Errorf("bad channel: %w", ErrTDLib400)
	}
	defer func() { runForChannelFn = prevFn }()

	sm := new(MockStateManager)
	p := &state.Page{ID: "p1", URL: "bad_chan", SequenceID: "seq-1"}
	cfg := common.CrawlerConfig{SamplingMethod: "random-walk", CrawlID: "crawl-1"}

	_, procErr := RunForChannelWithPool(ctx, p, "", sm, cfg)

	assert.True(t, errors.Is(procErr, ErrTDLib400), "expected ErrTDLib400, got: %v", procErr)
	// Connection must be released (not retired) — pool size stays at 1.
	assert.Equal(t, 1, ConnectionPoolTotalSize())
}

// Confirm ErrTDLib400 does NOT cause pool retirement (unlike ErrFloodWaitRetire).
func TestRunForChannelWithPool_400VsFloodWait_OnlyFloodWaitRetires(t *testing.T) {
	tdlib400 := new(MockTDLibClient)
	svc400 := &poolMockTelegramService{tdlib: tdlib400}
	pool400 := telegramhelper.NewConnectionPoolForTesting(svc400, 1)

	ctx := context.Background()
	_, connID, err := pool400.GetConnection(ctx)
	require.NoError(t, err)
	pool400.ReleaseConnection(connID)

	poolMu.Lock()
	prev := connectionPool
	connectionPool = pool400
	poolMu.Unlock()
	defer func() {
		poolMu.Lock()
		connectionPool = prev
		poolMu.Unlock()
	}()

	prevFn := runForChannelFn
	runForChannelFn = func(_ crawler.TDLibClient, _ *state.Page, _ string, _ state.StateManagementInterface, _ common.CrawlerConfig) ([]*state.Page, error) {
		return nil, fmt.Errorf("bad channel: %w", ErrTDLib400)
	}
	defer func() { runForChannelFn = prevFn }()

	sm := new(MockStateManager)
	p := &state.Page{ID: "p1", URL: "bad_chan", SequenceID: "seq-1"}
	cfg := common.CrawlerConfig{SamplingMethod: "random-walk", CrawlID: "crawl-1"}

	_, _ = RunForChannelWithPool(ctx, p, "", sm, cfg)

	// 400 must not retire the connection — pool still has 1 available.
	assert.Equal(t, 1, ConnectionPoolTotalSize(), "ErrTDLib400 must not retire the connection")
}

// ── handle400SeedReplacement: error paths and sequence ID ─────────────────

// GetRandomSeedChannel returning an error must be propagated.
func TestHandle400SeedReplacement_GetRandomSeedChannelError_Propagated(t *testing.T) {
	sm := new(MockStateManager)
	page := &state.Page{
		ID:         "p1",
		URL:        "seed_chan",
		SequenceID: "seq-1",
	}
	cfg := common.CrawlerConfig{CrawlID: "crawl-1"}

	sm.On("MarkChannelInvalid", "seed_chan", "tdlib_400").Return(nil)
	sm.On("MarkSeedChannelInvalid", "seed_chan").Return(nil)
	sm.On("GetEdgeRecord", "seq-1", "seed_chan").Return((*state.EdgeRecord)(nil), nil)
	sm.On("DeleteEdgeRecord", "seq-1", "seed_chan").Return(nil)
	sm.On("IsSeedChannel", "seed_chan").Return(true)
	sm.On("GetRandomSeedChannel").Return("", 0, fmt.Errorf("seed table empty"))

	err := Handle400Replacement(sm, page, cfg)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "seed table empty")
	sm.AssertNotCalled(t, "AddPageToPageBuffer", mock.Anything)
}

// AddPageToPageBuffer failing inside handle400SeedReplacement must propagate the error.
func TestHandle400SeedReplacement_AddPageToBufferFails_ReturnsError(t *testing.T) {
	sm := new(MockStateManager)
	page := &state.Page{
		ID:         "p1",
		URL:        "seed_chan",
		SequenceID: "seq-1",
	}
	cfg := common.CrawlerConfig{CrawlID: "crawl-1"}

	sm.On("MarkChannelInvalid", "seed_chan", "tdlib_400").Return(nil)
	sm.On("MarkSeedChannelInvalid", "seed_chan").Return(nil)
	sm.On("GetEdgeRecord", "seq-1", "seed_chan").Return((*state.EdgeRecord)(nil), nil)
	sm.On("DeleteEdgeRecord", "seq-1", "seed_chan").Return(nil)
	sm.On("IsSeedChannel", "seed_chan").Return(true)
	sm.On("GetRandomSeedChannel").Return("replacement_seed", 100, nil)
	sm.On("AddPageToPageBuffer", mock.Anything).Return(fmt.Errorf("buffer write failed"))

	err := Handle400Replacement(sm, page, cfg)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "buffer write failed")
}

// Seed replacement page must start a fresh SequenceID (new chain, no edge record).
func TestHandle400SeedReplacement_ReplacementPageHasFreshSequenceID(t *testing.T) {
	sm := new(MockStateManager)
	page := &state.Page{
		ID:         "p1",
		URL:        "seed_chan",
		SequenceID: "original-seq",
		ParentID:   "parent-1",
		Depth:      0,
	}
	cfg := common.CrawlerConfig{CrawlID: "crawl-1"}

	var capturedPage *state.Page
	sm.On("MarkChannelInvalid", "seed_chan", "tdlib_400").Return(nil)
	sm.On("MarkSeedChannelInvalid", "seed_chan").Return(nil)
	sm.On("GetEdgeRecord", "original-seq", "seed_chan").Return((*state.EdgeRecord)(nil), nil)
	sm.On("DeleteEdgeRecord", "original-seq", "seed_chan").Return(nil)
	sm.On("IsSeedChannel", "seed_chan").Return(true)
	sm.On("GetRandomSeedChannel").Return("replacement_seed", 100, nil)
	sm.On("AddPageToPageBuffer", mock.MatchedBy(func(p *state.Page) bool {
		capturedPage = p
		return true
	})).Return(nil)

	require.NoError(t, Handle400Replacement(sm, page, cfg))

	require.NotNil(t, capturedPage)
	assert.Equal(t, "replacement_seed", capturedPage.URL)
	assert.NotEqual(t, "original-seq", capturedPage.SequenceID, "seed replacement must start a new chain")
	assert.NotEmpty(t, capturedPage.SequenceID)
	// No edge record must be written — seeds have no incoming edge.
	sm.AssertNotCalled(t, "SaveEdgeRecords", mock.Anything)
}

// ── End-to-end detection: each error string triggers ErrTDLib400 ───────────

// Table-driven: confirms each known trigger string is detected by isTDLib400
// and that they are correctly NOT matched for non-400 errors.
func TestIsTDLib400_KnownTriggers(t *testing.T) {
	cases := []struct {
		errStr  string
		want    bool
		comment string
	}{
		{"400 USERNAME_INVALID", true, "unbracketed format seen in prod logs"},
		{"400 USERNAME_NOT_OCCUPIED", true, "unbracketed format seen in prod logs"},
		{"[400] CHANNEL_INVALID", true, "bracketed TDLib format"},
		{"no messages found in the chat", true, "empty/inaccessible channel"},
		{"[404] USERNAME_NOT_OCCUPIED", false, "404 is not 400"},
		{"FLOOD_WAIT_300", false, "flood wait is not a 400"},
		{"network timeout", false, "unrelated error"},
		{"", false, "empty string"},
	}

	for _, tc := range cases {
		t.Run(tc.comment, func(t *testing.T) {
			got := isTDLib400(fmt.Errorf("%s", tc.errStr))
			assert.Equal(t, tc.want, got, "isTDLib400(%q)", tc.errStr)
		})
	}
}

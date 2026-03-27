package crawl

import (
	"testing"

	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/zelenin/go-tdlib/client"
)

// TestTandemMode_WithEdges verifies that tandem mode creates a batch, inserts
// pending edges, and closes the batch — without calling SearchPublicChat.
func TestTandemMode_WithEdges(t *testing.T) {
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
		TandemCrawl:    true,
		CrawlID:        "crawl-1",
	}

	// Build a message with outlinks that will be extracted.
	// We use a text message with mentions.
	textContent := &client.MessageText{
		Text: &client.FormattedText{
			Text: "Check out @valid_channel and @another_chan",
			Entities: []*client.TextEntity{
				{
					Offset: 10,
					Length: 14,
					Type:   &client.TextEntityTypeMention{},
				},
				{
					Offset: 29,
					Length: 13,
					Type:   &client.TextEntityTypeMention{},
				},
			},
		},
	}

	messages := []*client.Message{
		{
			Id:      1,
			ChatId:  -100123,
			Content: textContent,
		},
	}

	info := &channelInfo{}

	// Mock expectations
	sm.On("UpdateMessage", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	sm.On("UpdatePage", mock.Anything).Return(nil)
	sm.On("IsInvalidChannel", mock.Anything).Return(false)
	sm.On("IsDiscoveredChannel", mock.Anything).Return(false)
	sm.On("CreatePendingBatch", mock.MatchedBy(func(b *state.PendingEdgeBatch) bool {
		return b.CrawlID == "crawl-1" && b.SourceChannel == "source_channel" && b.Status == "open"
	})).Return(nil).Once()
	sm.On("InsertPendingEdge", mock.MatchedBy(func(e *state.PendingEdge) bool {
		return e.CrawlID == "crawl-1" && e.ValidationStatus == "pending"
	})).Return(nil).Times(2)
	sm.On("ClosePendingBatch", mock.AnythingOfType("string")).Return(nil).Once()

	// SearchPublicChat should NEVER be called in tandem mode.
	// (We don't set expectations for it — if called, the test will fail.)

	processor := &MockMessageProcessor{}
	processor.On("ProcessMessage",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return([]string{"valid_channel", "another_chan"}, nil)

	result, err := processAllMessagesWithProcessor(
		tdlib, info, messages, "crawl-1", "source_channel", sm, processor, owner, cfg,
	)

	assert.NoError(t, err)
	assert.Empty(t, result) // tandem mode returns no discovered pages directly
	sm.AssertExpectations(t)
	sm.AssertNotCalled(t, "SearchPublicChat", mock.Anything)
	processor.AssertExpectations(t)
}

// TestTandemMode_NoEdges verifies that when no valid edges are found, the
// crawler performs a forced walkback itself (no batch created).
func TestTandemMode_NoEdges(t *testing.T) {
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
		TandemCrawl:    true,
		CrawlID:        "crawl-1",
	}

	// Message with no outlinks
	textContent := &client.MessageText{
		Text: &client.FormattedText{
			Text: "Just a regular message with no channels",
		},
	}
	messages := []*client.Message{
		{
			Id:      1,
			ChatId:  -100123,
			Content: textContent,
		},
	}

	info := &channelInfo{}

	sm.On("UpdateMessage", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	sm.On("UpdatePage", mock.Anything).Return(nil)
	sm.On("GetRandomDiscoveredChannel").Return("walkback_target", nil)
	sm.On("AddPageToPageBuffer", mock.MatchedBy(func(p *state.Page) bool {
		return p.URL == "walkback_target" && p.Depth == 1
	})).Return(nil)
	sm.On("SaveEdgeRecords", mock.MatchedBy(func(edges []*state.EdgeRecord) bool {
		return len(edges) == 1 && edges[0].Walkback && edges[0].DestinationChannel == "walkback_target"
	})).Return(nil)

	processor := &MockMessageProcessor{}
	processor.On("ProcessMessage",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return([]string{}, nil)

	result, err := processAllMessagesWithProcessor(
		tdlib, info, messages, "crawl-1", "source_channel", sm, processor, owner, cfg,
	)

	assert.NoError(t, err)
	assert.Empty(t, result)
	// No batch should have been created
	sm.AssertNotCalled(t, "CreatePendingBatch", mock.Anything)
	sm.AssertNotCalled(t, "ClosePendingBatch", mock.Anything)
	sm.AssertExpectations(t)
}

// TestTandemMode_InvalidChannelSkipped verifies that channels already known
// invalid are not written to pending_edges.
func TestTandemMode_InvalidChannelSkipped(t *testing.T) {
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
		TandemCrawl:    true,
		CrawlID:        "crawl-1",
	}

	textContent := &client.MessageText{
		Text: &client.FormattedText{
			Text: "Check @invalid_chan",
			Entities: []*client.TextEntity{
				{Offset: 6, Length: 13, Type: &client.TextEntityTypeMention{}},
			},
		},
	}
	messages := []*client.Message{
		{Id: 1, ChatId: -100123, Content: textContent},
	}

	info := &channelInfo{}

	sm.On("UpdateMessage", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	sm.On("UpdatePage", mock.Anything).Return(nil)
	sm.On("IsInvalidChannel", "invalid_chan").Return(true)
	// Forced walkback since no edges
	sm.On("GetRandomDiscoveredChannel").Return("walkback_target", nil)
	sm.On("AddPageToPageBuffer", mock.Anything).Return(nil)
	sm.On("SaveEdgeRecords", mock.Anything).Return(nil)

	processor := &MockMessageProcessor{}
	processor.On("ProcessMessage",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return([]string{"invalid_chan"}, nil)

	_, err := processAllMessagesWithProcessor(
		tdlib, info, messages, "crawl-1", "source_channel", sm, processor, owner, cfg,
	)

	assert.NoError(t, err)
	sm.AssertNotCalled(t, "CreatePendingBatch", mock.Anything)
	sm.AssertNotCalled(t, "InsertPendingEdge", mock.Anything)
}

// TestNonTandemMode_Unchanged verifies the standard path still works.
func TestNonTandemMode_Unchanged(t *testing.T) {
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

	textContent := &client.MessageText{
		Text: &client.FormattedText{
			Text: "Check out @valid_channel",
			Entities: []*client.TextEntity{
				{Offset: 10, Length: 14, Type: &client.TextEntityTypeMention{}},
			},
		},
	}
	messages := []*client.Message{
		{Id: 1, ChatId: -100123, Content: textContent},
	}

	info := &channelInfo{}

	sm.On("UpdateMessage", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	sm.On("UpdatePage", mock.Anything).Return(nil)
	sm.On("IsInvalidChannel", "valid_channel").Return(false)
	sm.On("IsDiscoveredChannel", "valid_channel").Return(false)
	sm.On("GetCachedChatID", "valid_channel").Return(int64(0), false)

	// SearchPublicChat IS called in non-tandem mode
	chatType := client.ChatTypeSupergroup{SupergroupId: 123}
	chat := &client.Chat{Id: -100123, Type: &chatType}
	tdlib.On("SearchPublicChat", mock.Anything).Return(chat, nil)
	sm.On("AddDiscoveredChannel", "valid_channel").Return(nil)
	sm.On("UpsertSeedChannelChatID", "valid_channel", int64(-100123)).Return(nil)

	// Walkback decision with WalkbackRate=0 means forward
	sm.On("AddPageToPageBuffer", mock.Anything).Return(nil)
	sm.On("SaveEdgeRecords", mock.Anything).Return(nil)

	processor := &MockMessageProcessor{}
	processor.On("ProcessMessage",
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Return([]string{"valid_channel"}, nil)

	_, err := processAllMessagesWithProcessor(
		tdlib, info, messages, "crawl-1", "source_channel", sm, processor, owner, cfg,
	)

	assert.NoError(t, err)
	tdlib.AssertCalled(t, "SearchPublicChat", mock.Anything)
	sm.AssertNotCalled(t, "CreatePendingBatch", mock.Anything)
}

package crawl

import (
	"errors"
	"fmt"
	"testing"

	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/zelenin/go-tdlib/client"
)

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
// retire threshold) causes the channel to be skipped without blacklisting, but
// does not abort processing (no error returned).
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

	shortBan := floodWaitRetireThresholdSecs - 1 // below threshold
	floodErr := fmt.Errorf("FLOOD_WAIT_%d", shortBan)

	messages := []*client.Message{
		messageWithMention("Check out @flood_chan", "@flood_chan"),
	}
	info := &channelInfo{}

	sm.On("UpdateMessage", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	sm.On("UpdatePage", mock.Anything).Return(nil)
	sm.On("IsInvalidChannel", "flood_chan").Return(false)
	sm.On("IsDiscoveredChannel", "flood_chan").Return(false)
	sm.On("GetCachedChatID", "flood_chan").Return(int64(0), false)

	// SearchPublicChat returns a short FLOOD_WAIT error.
	tdlib.On("SearchPublicChat", mock.Anything).Return(nil, floodErr)

	// Walkback triggered because no channels were discovered.
	sm.On("GetRandomDiscoveredChannel").Return("walkback_target", nil)
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

	// No error — short FLOOD_WAIT is handled by skipping the channel.
	assert.NoError(t, err)

	// The channel must NOT have been blacklisted.
	sm.AssertNotCalled(t, "MarkChannelInvalid", mock.Anything, mock.Anything)
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
	sm.On("GetRandomDiscoveredChannel").Return("walkback_target", nil)
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

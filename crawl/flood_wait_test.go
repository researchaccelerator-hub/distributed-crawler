package crawl

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseFloodWaitSecs(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		wantSecs    int
		wantIsFlood bool
	}{
		{
			name:        "nil error",
			err:         nil,
			wantSecs:    0,
			wantIsFlood: false,
		},
		{
			name:        "unrelated error",
			err:         errors.New("connection refused"),
			wantSecs:    0,
			wantIsFlood: false,
		},
		{
			name:        "bare FLOOD_WAIT with seconds",
			err:         fmt.Errorf("FLOOD_WAIT_72560"),
			wantSecs:    72560,
			wantIsFlood: true,
		},
		{
			name:        "prefixed FLOOD_WAIT as seen from TDLib",
			err:         fmt.Errorf("[429] FLOOD_WAIT_300"),
			wantSecs:    300,
			wantIsFlood: true,
		},
		{
			name:        "short ban below retire threshold",
			err:         fmt.Errorf("FLOOD_WAIT_30"),
			wantSecs:    30,
			wantIsFlood: true,
		},
		{
			name:        "exactly at retire threshold",
			err:         fmt.Errorf("FLOOD_WAIT_300"),
			wantSecs:    300,
			wantIsFlood: true,
		},
		{
			name:        "FLOOD_WAIT_0",
			err:         fmt.Errorf("FLOOD_WAIT_0"),
			wantSecs:    0,
			wantIsFlood: true,
		},
		{
			name:        "FLOOD_WAIT with no trailing digits",
			err:         fmt.Errorf("FLOOD_WAIT_"),
			wantSecs:    0,
			wantIsFlood: true,
		},
		{
			name:        "FLOOD_WAIT embedded in longer message",
			err:         fmt.Errorf("rpc error: code 429 FLOOD_WAIT_600 please wait"),
			wantSecs:    600,
			wantIsFlood: true,
		},
		{
			name:        "wrapped error containing FLOOD_WAIT",
			err:         fmt.Errorf("SearchPublicChat failed: %w", fmt.Errorf("FLOOD_WAIT_1800")),
			wantSecs:    1800,
			wantIsFlood: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			secs, isFlood := parseFloodWaitSecs(tc.err)
			assert.Equal(t, tc.wantIsFlood, isFlood, "isFlood mismatch")
			assert.Equal(t, tc.wantSecs, secs, "secs mismatch")
		})
	}
}

// TestParseFloodWaitSecs_ThresholdBoundary confirms the boundary between
// short (skip-only) and long (retire) bans.
func TestParseFloodWaitSecs_ThresholdBoundary(t *testing.T) {
	belowThreshold := fmt.Errorf("FLOOD_WAIT_%d", floodWaitRetireThresholdSecs-1)
	atThreshold := fmt.Errorf("FLOOD_WAIT_%d", floodWaitRetireThresholdSecs)
	aboveThreshold := fmt.Errorf("FLOOD_WAIT_%d", floodWaitRetireThresholdSecs+1)

	secsBelow, _ := parseFloodWaitSecs(belowThreshold)
	secsAt, _ := parseFloodWaitSecs(atThreshold)
	secsAbove, _ := parseFloodWaitSecs(aboveThreshold)

	assert.Less(t, secsBelow, floodWaitRetireThresholdSecs, "below threshold should not trigger retire")
	assert.GreaterOrEqual(t, secsAt, floodWaitRetireThresholdSecs, "at threshold should trigger retire")
	assert.Greater(t, secsAbove, floodWaitRetireThresholdSecs, "above threshold should trigger retire")
}

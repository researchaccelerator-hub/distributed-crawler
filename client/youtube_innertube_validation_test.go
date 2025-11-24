package client

import (
	"testing"
	"time"
)

func TestValidateChannelID(t *testing.T) {
	tests := []struct {
		name      string
		channelID string
		wantErr   bool
	}{
		// Valid cases
		{"valid channel ID", "UC1234567890123456789012", false},
		{"valid channel ID with underscore", "UC12345678901234567890_2", false},
		{"valid channel ID with dash", "UC12345678901234567890-2", false},
		{"valid handle with @", "@googledevelopers", false},
		{"valid handle with underscore", "@google_dev", false},
		{"valid handle with dot", "@google.dev", false},
		{"valid handle with dash", "@google-dev", false},

		// Invalid cases - empty
		{"empty channel ID", "", true},

		// Invalid cases - wrong length
		{"too short", "UC123", true},
		{"too long", "UC12345678901234567890123", true},

		// Invalid cases - wrong prefix
		{"no UC prefix", "AB1234567890123456789012", true},
		{"lowercase uc", "uc1234567890123456789012", true},

		// Invalid cases - invalid characters
		{"special char in ID", "UC123456789012345678901!", true},
		{"space in ID", "UC123456789012345 789012", true},
		{"unicode in ID", "UC1234567890123456789日本", true},

		// Invalid cases - handle format
		{"invalid handle with space", "@google dev", true},
		{"invalid handle with special char", "@google!", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateChannelID(tt.channelID)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateChannelID(%q) error = %v, wantErr %v", tt.channelID, err, tt.wantErr)
			}
		})
	}
}

func TestValidateVideoID(t *testing.T) {
	tests := []struct {
		name    string
		videoID string
		wantErr bool
	}{
		// Valid cases
		{"valid video ID", "dQw4w9WgXcQ", false},
		{"valid with underscore", "dQw4w9Wg_cQ", false},
		{"valid with dash", "dQw4w9Wg-cQ", false},
		{"valid all numbers", "12345678901", false},
		{"valid all letters", "abcdefghijk", false},
		{"valid mixed case", "ABCdefGHIjk", false},

		// Invalid cases - empty
		{"empty video ID", "", true},

		// Invalid cases - wrong length
		{"too short", "dQw4w9W", true},
		{"too long", "dQw4w9WgXcQQ", true},

		// Invalid cases - invalid characters
		{"special char", "dQw4w9Wg!cQ", true},
		{"space", "dQw4w9Wg cQ", true},
		{"unicode", "dQw4w9W日本", true},
		{"slash", "dQw4w9W/cQ", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateVideoID(tt.videoID)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateVideoID(%q) error = %v, wantErr %v", tt.videoID, err, tt.wantErr)
			}
		})
	}
}

func TestValidateVideoIDs(t *testing.T) {
	tests := []struct {
		name     string
		videoIDs []string
		wantErr  bool
	}{
		// Valid cases
		{"single valid ID", []string{"dQw4w9WgXcQ"}, false},
		{"multiple valid IDs", []string{"dQw4w9WgXcQ", "ABCdefGHIjk"}, false},

		// Invalid cases
		{"empty slice", []string{}, true},
		{"nil slice", nil, true},
		{"one invalid ID", []string{"dQw4w9WgXcQ", "invalid"}, true},
		{"first invalid", []string{"invalid", "dQw4w9WgXcQ"}, true},
		{"all invalid", []string{"invalid1", "invalid2"}, true},
		{"empty string in slice", []string{"dQw4w9WgXcQ", ""}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateVideoIDs(tt.videoIDs)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateVideoIDs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateTimeRange(t *testing.T) {
	now := time.Now()
	past := now.Add(-24 * time.Hour)
	future := now.Add(24 * time.Hour)

	tests := []struct {
		name     string
		fromTime time.Time
		toTime   time.Time
		wantErr  bool
	}{
		// Valid cases
		{"both zero", time.Time{}, time.Time{}, false},
		{"only fromTime set", past, time.Time{}, false},
		{"only toTime set", time.Time{}, future, false},
		{"fromTime before toTime", past, future, false},
		{"fromTime before now, toTime is now", past, now, false},

		// Invalid cases
		{"fromTime after toTime", future, past, true},
		{"fromTime equals toTime", now, now, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTimeRange(tt.fromTime, tt.toTime)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateTimeRange() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateLimit(t *testing.T) {
	tests := []struct {
		name    string
		limit   int
		wantErr bool
	}{
		// Valid cases
		{"zero (unlimited)", 0, false},
		{"negative (unlimited)", -1, false},
		{"minimum valid", 1, false},
		{"reasonable limit", 100, false},
		{"large but reasonable", 10000, false},
		{"max reasonable", 1000000, false},

		// Invalid cases
		{"exceeds max", 1000001, true},
		{"way too large", 999999999, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateLimit(tt.limit)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateLimit(%d) error = %v, wantErr %v", tt.limit, err, tt.wantErr)
			}
		})
	}
}

func TestValidateChannelIDs(t *testing.T) {
	tests := []struct {
		name       string
		channelIDs []string
		wantErr    bool
	}{
		// Valid cases
		{"single valid ID", []string{"UC1234567890123456789012"}, false},
		{"multiple valid IDs", []string{"UC1234567890123456789012", "UC9876543210987654321098"}, false},
		{"mixed IDs and handles", []string{"UC1234567890123456789012", "@google"}, false},

		// Invalid cases
		{"empty slice", []string{}, true},
		{"nil slice", nil, true},
		{"one invalid ID", []string{"UC1234567890123456789012", "invalid"}, true},
		{"first invalid", []string{"invalid", "UC1234567890123456789012"}, true},
		{"all invalid", []string{"invalid1", "invalid2"}, true},
		{"empty string in slice", []string{"UC1234567890123456789012", ""}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateChannelIDs(tt.channelIDs)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateChannelIDs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Benchmark validation functions
func BenchmarkValidateChannelID(b *testing.B) {
	channelID := "UC1234567890123456789012"
	for i := 0; i < b.N; i++ {
		_ = validateChannelID(channelID)
	}
}

func BenchmarkValidateVideoID(b *testing.B) {
	videoID := "dQw4w9WgXcQ"
	for i := 0; i < b.N; i++ {
		_ = validateVideoID(videoID)
	}
}

func BenchmarkValidateVideoIDs(b *testing.B) {
	videoIDs := []string{"dQw4w9WgXcQ", "ABCdefGHIjk", "12345678901"}
	for i := 0; i < b.N; i++ {
		_ = validateVideoIDs(videoIDs)
	}
}

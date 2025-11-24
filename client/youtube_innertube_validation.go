package client

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Validation constants
const (
	// YouTube channel IDs are 24 characters starting with UC
	channelIDLength = 24
	channelIDPrefix = "UC"

	// YouTube video IDs are 11 characters
	videoIDLength = 11

	// Reasonable limits
	minLimit         = 1
	maxReasonableLimit = 1000000 // 1 million
)

// Regular expressions for validation
var (
	// Channel ID: UC followed by 22 alphanumeric/underscore/dash characters
	channelIDPattern = regexp.MustCompile(`^UC[a-zA-Z0-9_-]{22}$`)

	// Video ID: 11 alphanumeric/underscore/dash characters
	videoIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{11}$`)

	// Handle format: @ followed by alphanumeric/underscore/period/dash
	handlePattern = regexp.MustCompile(`^@?[a-zA-Z0-9._-]+$`)
)

// validateChannelID validates a YouTube channel ID format
// Valid formats:
// - UC followed by 22 characters (standard channel ID)
// - @handle (will be resolved by YouTube)
func validateChannelID(channelID string) error {
	if channelID == "" {
		return fmt.Errorf("channel ID cannot be empty")
	}

	// Check if it's a handle (starts with @)
	if strings.HasPrefix(channelID, "@") {
		if !handlePattern.MatchString(channelID) {
			return fmt.Errorf("invalid channel handle format: %s", channelID)
		}
		return nil
	}

	// Check standard channel ID format
	if len(channelID) != channelIDLength {
		return fmt.Errorf("channel ID must be %d characters, got %d: %s",
			channelIDLength, len(channelID), channelID)
	}

	if !strings.HasPrefix(channelID, channelIDPrefix) {
		return fmt.Errorf("channel ID must start with %s (or use @handle format), got: %s",
			channelIDPrefix, channelID)
	}

	if !channelIDPattern.MatchString(channelID) {
		return fmt.Errorf("invalid channel ID format (must be UC + 22 alphanumeric/underscore/dash chars): %s",
			channelID)
	}

	return nil
}

// validateVideoID validates a YouTube video ID format
// Video IDs are 11 alphanumeric characters with underscore and dash allowed
func validateVideoID(videoID string) error {
	if videoID == "" {
		return fmt.Errorf("video ID cannot be empty")
	}

	if len(videoID) != videoIDLength {
		return fmt.Errorf("video ID must be %d characters, got %d: %s",
			videoIDLength, len(videoID), videoID)
	}

	if !videoIDPattern.MatchString(videoID) {
		return fmt.Errorf("invalid video ID format (must be 11 alphanumeric/underscore/dash chars): %s",
			videoID)
	}

	return nil
}

// validateVideoIDs validates a slice of video IDs
func validateVideoIDs(videoIDs []string) error {
	if len(videoIDs) == 0 {
		return fmt.Errorf("video IDs slice cannot be empty")
	}

	for i, videoID := range videoIDs {
		if err := validateVideoID(videoID); err != nil {
			return fmt.Errorf("invalid video ID at index %d: %w", i, err)
		}
	}

	return nil
}

// validateTimeRange validates that fromTime is before toTime
// Zero times are allowed (means no constraint)
func validateTimeRange(fromTime, toTime time.Time) error {
	// If both are zero, no validation needed
	if fromTime.IsZero() && toTime.IsZero() {
		return nil
	}

	// If only fromTime is set, that's okay
	if !fromTime.IsZero() && toTime.IsZero() {
		return nil
	}

	// If only toTime is set, that's okay
	if fromTime.IsZero() && !toTime.IsZero() {
		return nil
	}

	// Both are set - fromTime must be before toTime
	if !fromTime.Before(toTime) {
		return fmt.Errorf("fromTime (%v) must be before toTime (%v)", fromTime, toTime)
	}

	return nil
}

// validateLimit validates that limit is within reasonable bounds
// Limit of 0 or negative means unlimited, which is allowed
func validateLimit(limit int) error {
	// 0 or negative means unlimited - that's okay
	if limit <= 0 {
		return nil
	}

	// Positive limit must be at least minLimit
	if limit < minLimit {
		return fmt.Errorf("limit must be at least %d or 0 for unlimited, got %d", minLimit, limit)
	}

	// Check for unreasonably large limits (potential abuse)
	if limit > maxReasonableLimit {
		return fmt.Errorf("limit exceeds maximum reasonable value of %d, got %d", maxReasonableLimit, limit)
	}

	return nil
}

// validateChannelIDs validates a slice of channel IDs
func validateChannelIDs(channelIDs []string) error {
	if len(channelIDs) == 0 {
		return fmt.Errorf("channel IDs slice cannot be empty")
	}

	for i, channelID := range channelIDs {
		if err := validateChannelID(channelID); err != nil {
			return fmt.Errorf("invalid channel ID at index %d: %w", i, err)
		}
	}

	return nil
}

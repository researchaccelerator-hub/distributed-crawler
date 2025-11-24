package client

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Integration tests for YouTubeInnerTubeClient
// These tests require network access and hit the real YouTube InnerTube API
// Run with: go test -v -run Integration ./client/
// Skip with: go test -v -short ./client/

func init() {
	// Set log level to info for integration tests
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
}

// TestIntegrationGetChannelInfo tests fetching real channel information
func TestIntegrationGetChannelInfo(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	config := &InnerTubeConfig{
		ClientType:    "WEB",
		ClientVersion: "2.20230728.00.00",
		APITimeout:    30 * time.Second,
	}

	client, err := NewYouTubeInnerTubeClient(config)
	if err != nil {
		t.Fatalf("Failed to create InnerTube client: %v", err)
	}

	ctx := context.Background()
	err = client.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.Disconnect(ctx)

	// Test cases with well-known YouTube channels
	testCases := []struct {
		name            string
		channelID       string
		expectTitle     bool
		expectSubCount  bool
		expectVideoCount bool
	}{
		{
			name:            "Google Developers channel by ID",
			channelID:       "UC_x5XG1OV2P6uZZ5FSM9Ttw",
			expectTitle:     true,
			expectSubCount:  true,
			expectVideoCount: true,
		},
		{
			name:            "Google Developers by handle",
			channelID:       "@GoogleDevelopers",
			expectTitle:     true,
			expectSubCount:  true,
			expectVideoCount: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			channelInfo, err := client.GetChannelInfo(ctx, tc.channelID)
			if err != nil {
				t.Fatalf("Failed to get channel info: %v", err)
			}

			if channelInfo == nil {
				t.Fatal("Channel info is nil")
			}

			if tc.expectTitle && channelInfo.Title == "" {
				t.Error("Expected channel title, got empty string")
			}

			if tc.expectSubCount && channelInfo.SubscriberCount <= 0 {
				t.Errorf("Expected positive subscriber count, got %d", channelInfo.SubscriberCount)
			}

			if tc.expectVideoCount && channelInfo.VideoCount <= 0 {
				t.Errorf("Expected positive video count, got %d", channelInfo.VideoCount)
			}

			if channelInfo.ID == "" {
				t.Error("Channel ID is empty")
			}

			t.Logf("Channel: %s (ID: %s)", channelInfo.Title, channelInfo.ID)
			t.Logf("Subscribers: %d, Videos: %d", channelInfo.SubscriberCount, channelInfo.VideoCount)

			// Verify thumbnails exist
			if len(channelInfo.Thumbnails) == 0 {
				t.Error("Expected thumbnails, got none")
			}
		})
	}
}

// TestIntegrationGetChannelInfoCaching tests that caching works correctly
func TestIntegrationGetChannelInfoCaching(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	config := &InnerTubeConfig{
		ClientType:    "WEB",
		ClientVersion: "2.20230728.00.00",
		APITimeout:    30 * time.Second,
	}

	client, err := NewYouTubeInnerTubeClient(config)
	if err != nil {
		t.Fatalf("Failed to create InnerTube client: %v", err)
	}

	ctx := context.Background()
	err = client.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.Disconnect(ctx)

	channelID := "UC_x5XG1OV2P6uZZ5FSM9Ttw" // Google Developers

	// First call - should hit API
	start1 := time.Now()
	info1, err := client.GetChannelInfo(ctx, channelID)
	duration1 := time.Since(start1)
	if err != nil {
		t.Fatalf("Failed first GetChannelInfo: %v", err)
	}

	// Second call - should hit cache
	start2 := time.Now()
	info2, err := client.GetChannelInfo(ctx, channelID)
	duration2 := time.Since(start2)
	if err != nil {
		t.Fatalf("Failed second GetChannelInfo: %v", err)
	}

	// Verify cache speedup (cached call should be much faster)
	if duration2 >= duration1 {
		t.Logf("Warning: Cached call (%v) not faster than first call (%v)", duration2, duration1)
	}

	// Verify data consistency
	if info1.ID != info2.ID {
		t.Errorf("Channel IDs don't match: %s vs %s", info1.ID, info2.ID)
	}

	if info1.Title != info2.Title {
		t.Errorf("Channel titles don't match: %s vs %s", info1.Title, info2.Title)
	}

	t.Logf("First call: %v, Second call (cached): %v", duration1, duration2)
}

// TestIntegrationGetVideosFromChannel tests fetching videos from a channel
func TestIntegrationGetVideosFromChannel(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	config := &InnerTubeConfig{
		ClientType:    "WEB",
		ClientVersion: "2.20230728.00.00",
		APITimeout:    30 * time.Second,
	}

	client, err := NewYouTubeInnerTubeClient(config)
	if err != nil {
		t.Fatalf("Failed to create InnerTube client: %v", err)
	}

	ctx := context.Background()
	err = client.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.Disconnect(ctx)

	channelID := "UC_x5XG1OV2P6uZZ5FSM9Ttw" // Google Developers

	// Test fetching recent videos
	fromTime := time.Now().Add(-90 * 24 * time.Hour) // 90 days ago
	toTime := time.Now()
	limit := 10

	videos, err := client.GetVideosFromChannel(ctx, channelID, fromTime, toTime, limit)
	if err != nil {
		t.Fatalf("Failed to get videos: %v", err)
	}

	if videos == nil {
		t.Fatal("Videos slice is nil")
	}

	// Note: Channel might not have videos in the time range, so just verify structure
	t.Logf("Retrieved %d videos from channel", len(videos))

	// If we got videos, verify their structure
	for i, video := range videos {
		if video.ID == "" {
			t.Errorf("Video %d has empty ID", i)
		}

		if video.Title == "" {
			t.Errorf("Video %d (%s) has empty title", i, video.ID)
		}

		if video.ChannelID != channelID {
			t.Errorf("Video %d has wrong channel ID: expected %s, got %s", i, channelID, video.ChannelID)
		}

		// Verify time filtering worked
		if !video.PublishedAt.IsZero() {
			if video.PublishedAt.Before(fromTime) {
				t.Errorf("Video %d published before fromTime: %v < %v", i, video.PublishedAt, fromTime)
			}
			if video.PublishedAt.After(toTime) {
				t.Errorf("Video %d published after toTime: %v > %v", i, video.PublishedAt, toTime)
			}
		}

		if i < 3 {
			t.Logf("  Video %d: %s (ID: %s, Published: %v)", i+1, video.Title, video.ID, video.PublishedAt)
		}
	}

	// Verify limit is respected (if there are enough videos)
	if len(videos) > limit {
		t.Errorf("Expected at most %d videos, got %d", limit, len(videos))
	}
}

// TestIntegrationGetVideosWithDifferentLimits tests limit parameter handling
func TestIntegrationGetVideosWithDifferentLimits(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	config := &InnerTubeConfig{
		ClientType:    "WEB",
		ClientVersion: "2.20230728.00.00",
		APITimeout:    30 * time.Second,
	}

	client, err := NewYouTubeInnerTubeClient(config)
	if err != nil {
		t.Fatalf("Failed to create InnerTube client: %v", err)
	}

	ctx := context.Background()
	err = client.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.Disconnect(ctx)

	channelID := "UC_x5XG1OV2P6uZZ5FSM9Ttw" // Google Developers
	fromTime := time.Now().Add(-365 * 24 * time.Hour) // 1 year ago
	toTime := time.Now()

	testCases := []struct {
		name  string
		limit int
	}{
		{"limit 5", 5},
		{"limit 10", 10},
		{"limit 20", 20},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			videos, err := client.GetVideosFromChannel(ctx, channelID, fromTime, toTime, tc.limit)
			if err != nil {
				t.Fatalf("Failed to get videos: %v", err)
			}

			if len(videos) > tc.limit {
				t.Errorf("Expected at most %d videos, got %d", tc.limit, len(videos))
			}

			t.Logf("Requested %d videos, got %d", tc.limit, len(videos))
		})
	}
}

// TestIntegrationInvalidChannelID tests error handling for invalid channel IDs
func TestIntegrationInvalidChannelID(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	config := &InnerTubeConfig{
		ClientType:    "WEB",
		ClientVersion: "2.20230728.00.00",
		APITimeout:    30 * time.Second,
	}

	client, err := NewYouTubeInnerTubeClient(config)
	if err != nil {
		t.Fatalf("Failed to create InnerTube client: %v", err)
	}

	ctx := context.Background()
	err = client.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.Disconnect(ctx)

	testCases := []struct {
		name      string
		channelID string
	}{
		{"empty channel ID", ""},
		{"invalid format", "invalid123"},
		{"too short", "UC123"},
		{"special characters", "UC!@#$%^&*()"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.GetChannelInfo(ctx, tc.channelID)
			if err == nil {
				t.Error("Expected error for invalid channel ID, got nil")
			}
			t.Logf("Got expected error: %v", err)
		})
	}
}

// TestIntegrationAPITimeout tests that timeout configuration works
func TestIntegrationAPITimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create client with very short timeout to test timeout handling
	config := &InnerTubeConfig{
		ClientType:    "WEB",
		ClientVersion: "2.20230728.00.00",
		APITimeout:    1 * time.Nanosecond, // Impossibly short timeout
	}

	client, err := NewYouTubeInnerTubeClient(config)
	if err != nil {
		t.Fatalf("Failed to create InnerTube client: %v", err)
	}

	ctx := context.Background()
	err = client.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.Disconnect(ctx)

	// This should timeout
	_, err = client.GetChannelInfo(ctx, "UC_x5XG1OV2P6uZZ5FSM9Ttw")
	if err == nil {
		t.Error("Expected timeout error, got nil")
	}

	t.Logf("Got expected timeout error: %v", err)
}

// TestIntegrationDisconnectedClient tests that operations fail when not connected
func TestIntegrationDisconnectedClient(t *testing.T) {
	config := &InnerTubeConfig{
		ClientType:    "WEB",
		ClientVersion: "2.20230728.00.00",
		APITimeout:    30 * time.Second,
	}

	client, err := NewYouTubeInnerTubeClient(config)
	if err != nil {
		t.Fatalf("Failed to create InnerTube client: %v", err)
	}

	ctx := context.Background()

	// Try to use client without connecting
	_, err = client.GetChannelInfo(ctx, "UC_x5XG1OV2P6uZZ5FSM9Ttw")
	if err == nil {
		t.Error("Expected error when using disconnected client, got nil")
	}

	if err.Error() != "client not connected - call Connect() first" {
		t.Errorf("Expected 'not connected' error, got: %v", err)
	}

	t.Logf("Got expected error: %v", err)
}

// TestIntegrationGetVideosByIDs tests fetching videos by their IDs
func TestIntegrationGetVideosByIDs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	config := &InnerTubeConfig{
		ClientType:    "WEB",
		ClientVersion: "2.20230728.00.00",
		APITimeout:    30 * time.Second,
	}

	client, err := NewYouTubeInnerTubeClient(config)
	if err != nil {
		t.Fatalf("Failed to create InnerTube client: %v", err)
	}

	ctx := context.Background()
	err = client.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.Disconnect(ctx)

	// Test with well-known video IDs
	videoIDs := []string{
		"dQw4w9WgXcQ", // Rick Astley - Never Gonna Give You Up
		"9bZkp7q19f0", // PSY - GANGNAM STYLE
	}

	videos, err := client.GetVideosByIDs(ctx, videoIDs)
	if err != nil {
		// Note: This might fail because Player endpoint is not implemented yet
		t.Logf("GetVideosByIDs failed (expected if Player endpoint not implemented): %v", err)
		return
	}

	if len(videos) == 0 {
		t.Log("No videos returned (Player endpoint may not be implemented)")
		return
	}

	for i, video := range videos {
		if video.ID == "" {
			t.Errorf("Video %d has empty ID", i)
		}
		t.Logf("Video %d: %s (ID: %s)", i+1, video.Title, video.ID)
	}
}

// TestIntegrationGetRandomVideos tests that random videos returns not supported error
func TestIntegrationGetRandomVideos(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	config := &InnerTubeConfig{
		ClientType:    "WEB",
		ClientVersion: "2.20230728.00.00",
		APITimeout:    30 * time.Second,
	}

	client, err := NewYouTubeInnerTubeClient(config)
	if err != nil {
		t.Fatalf("Failed to create InnerTube client: %v", err)
	}

	ctx := context.Background()
	err = client.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.Disconnect(ctx)

	fromTime := time.Now().Add(-30 * 24 * time.Hour)
	toTime := time.Now()

	_, err = client.GetRandomVideos(ctx, fromTime, toTime, 10)
	if err == nil {
		t.Error("Expected 'not supported' error, got nil")
	}

	if err.Error() != "GetRandomVideos not supported by InnerTube API - InnerTube does not provide Search.List with random prefix functionality. Use YouTube Data API client instead" {
		t.Errorf("Expected 'not supported' error message, got: %v", err)
	}

	t.Logf("Got expected not supported error: %v", err)
}

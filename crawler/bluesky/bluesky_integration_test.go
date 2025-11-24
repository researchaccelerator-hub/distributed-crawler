package bluesky

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/client"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func init() {
	// Set log level to info for integration tests
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
}

// TestIntegrationBlueskyFirehose tests connecting to the real Bluesky firehose
// and receiving live posts for a short duration
func TestIntegrationBlueskyFirehose(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create real Bluesky client
	config := client.BlueskyConfig{
		JetStreamURL: "wss://jetstream2.us-east.bsky.network/subscribe",
		WantedCollections: []string{
			"app.bsky.feed.post", // Only posts
		},
		BufferSize: 1000,
	}

	blueskyClient, err := client.NewBlueskyClient(config)
	if err != nil {
		t.Fatalf("Failed to create Bluesky client: %v", err)
	}

	ctx := context.Background()
	err = blueskyClient.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect to Bluesky: %v", err)
	}
	defer blueskyClient.Disconnect(ctx)

	// Test fetching messages for a short time window (10 seconds)
	fromTime := time.Now()
	toTime := fromTime.Add(10 * time.Second)
	limit := 50 // Limit to 50 posts

	t.Logf("Collecting posts for 10 seconds from Bluesky firehose...")

	messages, err := blueskyClient.GetMessages(ctx, "firehose", fromTime, toTime, limit)
	if err != nil {
		t.Fatalf("Failed to get messages: %v", err)
	}

	t.Logf("Collected %d posts from firehose", len(messages))

	if len(messages) == 0 {
		t.Error("Expected to receive at least some posts from the firehose")
		return
	}

	// Validate message structure
	for i, msg := range messages {
		if msg.GetID() == "" {
			t.Errorf("Message %d has empty ID", i)
		}
		if msg.GetTimestamp().IsZero() {
			t.Errorf("Message %d has zero timestamp", i)
		}

		// Log first 3 messages
		if i < 3 {
			t.Logf("  Post %d: ID=%s, Type=%s, Time=%v",
				i+1, msg.GetID(), msg.GetType(), msg.GetTimestamp())
			if blueskyMsg, ok := msg.(*client.BlueskyMessage); ok {
				t.Logf("    Text: %s", blueskyMsg.Text)
			}
		}
	}
}

// TestIntegrationBlueskySpecificUser tests fetching posts from a specific user DID
func TestIntegrationBlueskySpecificUser(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Use bsky.app's DID as a test subject (high-volume account)
	testDID := "did:plc:z72i7hdynmk6r22z27h6tvur" // bsky.app official account

	config := client.BlueskyConfig{
		JetStreamURL: "wss://jetstream2.us-east.bsky.network/subscribe",
		WantedCollections: []string{
			"app.bsky.feed.post",
		},
		WantedDids: []string{testDID},
		BufferSize: 100,
	}

	blueskyClient, err := client.NewBlueskyClient(config)
	if err != nil {
		t.Fatalf("Failed to create Bluesky client: %v", err)
	}

	ctx := context.Background()
	err = blueskyClient.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect to Bluesky: %v", err)
	}
	defer blueskyClient.Disconnect(ctx)

	// Wait for up to 30 seconds to catch a post from this user
	fromTime := time.Now()
	toTime := fromTime.Add(30 * time.Second)
	limit := 10

	t.Logf("Waiting up to 30 seconds for posts from %s...", testDID)

	messages, err := blueskyClient.GetMessages(ctx, testDID, fromTime, toTime, limit)
	if err != nil {
		t.Fatalf("Failed to get messages: %v", err)
	}

	t.Logf("Collected %d posts from user %s", len(messages), testDID)

	// Note: This might be 0 if the user doesn't post during our collection window
	// That's okay - we're just testing the filtering works
	if len(messages) > 0 {
		for _, msg := range messages {
			if msg.GetChannelID() != testDID {
				t.Errorf("Expected channel ID %s, got %s", testDID, msg.GetChannelID())
			}
		}
		t.Logf("Successfully filtered to specific user")
	} else {
		t.Logf("No posts from user in 30s window (this is okay, user may not be active)")
	}
}

// TestIntegrationBlueskyCrawlerFullFlow tests the full crawler flow with real connection
func TestIntegrationBlueskyCrawlerFullFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create real Bluesky client
	config := client.BlueskyConfig{
		JetStreamURL: "wss://jetstream2.us-east.bsky.network/subscribe",
		WantedCollections: []string{
			"app.bsky.feed.post",
		},
		BufferSize: 1000,
	}

	blueskyClient, err := client.NewBlueskyClient(config)
	if err != nil {
		t.Fatalf("Failed to create Bluesky client: %v", err)
	}

	// Create mock state manager
	mockStateManager := &MockStateManager{}

	// Create and initialize crawler
	bc := NewBlueskyCrawler().(*BlueskyCrawler)
	crawlerConfig := map[string]interface{}{
		"client":        blueskyClient,
		"state_manager": mockStateManager,
		"crawl_label":   "integration-test",
	}

	ctx := context.Background()
	err = bc.Initialize(ctx, crawlerConfig)
	if err != nil {
		t.Fatalf("Failed to initialize crawler: %v", err)
	}

	// Test GetChannelInfo for firehose
	target := crawler.CrawlTarget{
		Type: crawler.PlatformBluesky,
		ID:   "firehose",
	}

	channelInfo, err := bc.GetChannelInfo(ctx, target)
	if err != nil {
		t.Fatalf("Failed to get channel info: %v", err)
	}

	if channelInfo.ChannelID != "firehose" {
		t.Errorf("Expected channel ID 'firehose', got '%s'", channelInfo.ChannelID)
	}

	t.Logf("Channel Info: %s - %s", channelInfo.ChannelName, channelInfo.ChannelDescription)

	// Test FetchMessages
	job := crawler.CrawlJob{
		Target:   target,
		FromTime: time.Now(),
		ToTime:   time.Now().Add(10 * time.Second),
		Limit:    20,
	}

	t.Logf("Fetching messages for 10 seconds...")

	result, err := bc.FetchMessages(ctx, job)
	if err != nil {
		t.Fatalf("Failed to fetch messages: %v", err)
	}

	t.Logf("Crawl result: %d posts, %d errors",
		len(result.Posts), len(result.Errors))

	if len(result.Posts) == 0 {
		t.Error("Expected to fetch at least some posts")
	}

	// Verify posts were stored in mock state manager
	if len(mockStateManager.posts) == 0 {
		t.Error("Expected posts to be stored in state manager")
	}

	t.Logf("Successfully stored %d posts in state manager", len(mockStateManager.posts))

	// Log first few posts
	for i := 0; i < min(3, len(result.Posts)); i++ {
		post := result.Posts[i]
		t.Logf("  Post %d: %s (by %s)", i+1, post.PostUID, post.ChannelID)
	}
}

// TestIntegrationBlueskyConnectionResilience tests connection handling
func TestIntegrationBlueskyConnectionResilience(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	config := client.BlueskyConfig{
		JetStreamURL: "wss://jetstream2.us-east.bsky.network/subscribe",
		WantedCollections: []string{
			"app.bsky.feed.post",
		},
		BufferSize: 100,
	}

	blueskyClient, err := client.NewBlueskyClient(config)
	if err != nil {
		t.Fatalf("Failed to create Bluesky client: %v", err)
	}

	ctx := context.Background()

	// Test double connect (should handle gracefully)
	err = blueskyClient.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed first connect: %v", err)
	}

	err = blueskyClient.Connect(ctx)
	if err != nil {
		t.Logf("Second connect returned error (expected): %v", err)
	}

	// Test getting messages
	messages, err := blueskyClient.GetMessages(ctx, "firehose", time.Now(), time.Now().Add(5*time.Second), 10)
	if err != nil {
		t.Fatalf("Failed to get messages: %v", err)
	}

	t.Logf("Collected %d messages", len(messages))

	// Test disconnect
	err = blueskyClient.Disconnect(ctx)
	if err != nil {
		t.Fatalf("Failed to disconnect: %v", err)
	}

	// Test operations after disconnect should fail or handle gracefully
	_, err = blueskyClient.GetMessages(ctx, "firehose", time.Now(), time.Now().Add(1*time.Second), 1)
	if err == nil {
		t.Log("GetMessages after disconnect didn't error (may reconnect automatically)")
	} else {
		t.Logf("GetMessages after disconnect failed as expected: %v", err)
	}
}

// Helper function (Go 1.21+)
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

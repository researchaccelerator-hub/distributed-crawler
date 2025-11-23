package client

import (
	"context"
	"testing"
	"time"

	blueskymodel "github.com/researchaccelerator-hub/telegram-scraper/model/bluesky"
)

func TestNewBlueskyClient(t *testing.T) {
	tests := []struct {
		name             string
		config           BlueskyConfig
		expectedURL      string
		expectedBuffer   int
		expectedCursor   *int64
	}{
		{
			name:           "default configuration",
			config:         BlueskyConfig{},
			expectedURL:    "wss://jetstream2.us-east.bsky.network/subscribe",
			expectedBuffer: 10000,
		},
		{
			name: "custom configuration",
			config: BlueskyConfig{
				JetStreamURL: "wss://custom.jetstream.url",
				BufferSize:   5000,
			},
			expectedURL:    "wss://custom.jetstream.url",
			expectedBuffer: 5000,
		},
		{
			name: "with collections filter",
			config: BlueskyConfig{
				WantedCollections: []string{"app.bsky.feed.post", "app.bsky.feed.repost"},
			},
			expectedURL:    "wss://jetstream2.us-east.bsky.network/subscribe",
			expectedBuffer: 10000,
		},
		{
			name: "with DIDs filter",
			config: BlueskyConfig{
				WantedDids: []string{"did:plc:test123"},
			},
			expectedURL:    "wss://jetstream2.us-east.bsky.network/subscribe",
			expectedBuffer: 10000,
		},
		{
			name: "with cursor",
			config: BlueskyConfig{
				Cursor: func() *int64 { c := int64(123456789); return &c }(),
			},
			expectedURL:    "wss://jetstream2.us-east.bsky.network/subscribe",
			expectedBuffer: 10000,
			expectedCursor: func() *int64 { c := int64(123456789); return &c }(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewBlueskyClient(tt.config)
			if err != nil {
				t.Fatalf("NewBlueskyClient() error = %v", err)
			}

			if client == nil {
				t.Fatal("Expected non-nil client")
			}

			if client.config.JetStreamURL != tt.expectedURL {
				t.Errorf("JetStreamURL = %s, want %s", client.config.JetStreamURL, tt.expectedURL)
			}

			if client.config.BufferSize != tt.expectedBuffer {
				t.Errorf("BufferSize = %d, want %d", client.config.BufferSize, tt.expectedBuffer)
			}

			if cap(client.eventBuffer) != tt.expectedBuffer {
				t.Errorf("Event buffer capacity = %d, want %d", cap(client.eventBuffer), tt.expectedBuffer)
			}

			if tt.expectedCursor != nil {
				if client.cursor == nil {
					t.Error("Expected cursor to be set")
				} else if *client.cursor != *tt.expectedCursor {
					t.Errorf("Cursor = %d, want %d", *client.cursor, *tt.expectedCursor)
				}
			}

			if client.stopChan == nil {
				t.Error("Expected stopChan to be initialized")
			}

			if !client.isConnected {
				// Should not be connected initially
			} else {
				t.Error("Client should not be connected on creation")
			}
		})
	}
}

func TestBlueskyClient_BuildJetStreamURL(t *testing.T) {
	tests := []struct {
		name        string
		config      BlueskyConfig
		expectInURL []string
	}{
		{
			name:        "no filters",
			config:      BlueskyConfig{},
			expectInURL: []string{"wss://jetstream2.us-east.bsky.network/subscribe"},
		},
		{
			name: "with one collection",
			config: BlueskyConfig{
				WantedCollections: []string{"app.bsky.feed.post"},
			},
			expectInURL: []string{"wantedCollections=app.bsky.feed.post"},
		},
		{
			name: "with multiple collections",
			config: BlueskyConfig{
				WantedCollections: []string{"app.bsky.feed.post", "app.bsky.feed.repost"},
			},
			expectInURL: []string{
				"wantedCollections=app.bsky.feed.post",
				"wantedCollections=app.bsky.feed.repost",
			},
		},
		{
			name: "with DIDs",
			config: BlueskyConfig{
				WantedDids: []string{"did:plc:abc", "did:plc:xyz"},
			},
			expectInURL: []string{
				"wantedDids=did%3Aplc%3Aabc", // URL encoded
				"wantedDids=did%3Aplc%3Axyz",
			},
		},
		{
			name: "with cursor",
			config: BlueskyConfig{
				Cursor: func() *int64 { c := int64(123456); return &c }(),
			},
			expectInURL: []string{"cursor=123456"},
		},
		{
			name: "with all filters",
			config: BlueskyConfig{
				WantedCollections: []string{"app.bsky.feed.post"},
				WantedDids:        []string{"did:plc:test"},
				Cursor:            func() *int64 { c := int64(789); return &c }(),
			},
			expectInURL: []string{
				"wantedCollections=app.bsky.feed.post",
				"wantedDids=did%3Aplc%3Atest",
				"cursor=789",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewBlueskyClient(tt.config)
			if err != nil {
				t.Fatalf("NewBlueskyClient() error = %v", err)
			}

			url, err := client.buildJetStreamURL()
			if err != nil {
				t.Fatalf("buildJetStreamURL() error = %v", err)
			}

			// Check that all expected strings are in the URL
			for _, expected := range tt.expectInURL {
				if !contains(url, expected) {
					t.Errorf("Expected URL to contain '%s', got: %s", expected, url)
				}
			}
		})
	}
}

func TestBlueskyClient_Disconnect_NotConnected(t *testing.T) {
	client, err := NewBlueskyClient(BlueskyConfig{})
	if err != nil {
		t.Fatalf("NewBlueskyClient() error = %v", err)
	}

	// Disconnect without connecting should not error
	err = client.Disconnect(context.Background())
	if err != nil {
		t.Errorf("Disconnect() error = %v, want nil", err)
	}
}

func TestBlueskyClient_GetChannelInfo(t *testing.T) {
	client, err := NewBlueskyClient(BlueskyConfig{})
	if err != nil {
		t.Fatalf("NewBlueskyClient() error = %v", err)
	}

	tests := []struct {
		name      string
		channelID string
	}{
		{"DID format", "did:plc:z72i7hdynmk6r22z27h6tvur"},
		{"handle format", "user.bsky.social"},
		{"firehose", "firehose"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			channel, err := client.GetChannelInfo(context.Background(), tt.channelID)
			if err != nil {
				t.Fatalf("GetChannelInfo() error = %v", err)
			}

			if channel == nil {
				t.Fatal("Expected non-nil channel")
			}

			if channel.GetID() != tt.channelID {
				t.Errorf("Channel ID = %s, want %s", channel.GetID(), tt.channelID)
			}

			if channel.GetName() != tt.channelID {
				t.Errorf("Channel Name = %s, want %s", channel.GetName(), tt.channelID)
			}
		})
	}
}

func TestBlueskyClient_GetMessages_NotConnected(t *testing.T) {
	client, err := NewBlueskyClient(BlueskyConfig{})
	if err != nil {
		t.Fatalf("NewBlueskyClient() error = %v", err)
	}

	// Try to get messages without connecting
	fromTime := time.Now().Add(-1 * time.Hour)
	toTime := time.Now()

	_, err = client.GetMessages(context.Background(), "did:plc:test", fromTime, toTime, 10)
	if err == nil {
		t.Error("Expected error when not connected, got nil")
	}

	expectedMsg := "not connected to Bluesky JetStream"
	if err != nil && err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got: %v", expectedMsg, err)
	}
}

func TestBlueskyClient_EventToMessage(t *testing.T) {
	client, err := NewBlueskyClient(BlueskyConfig{})
	if err != nil {
		t.Fatalf("NewBlueskyClient() error = %v", err)
	}

	// Create a test event
	event := &blueskymodel.BlueskyEvent{
		DID:    "did:plc:test123",
		TimeUS: 1234567890000000, // Microseconds
		Kind:   "commit",
		Commit: &blueskymodel.CommitEvent{
			Rev:        "rev123",
			Operation:  blueskymodel.OperationCreate,
			Collection: blueskymodel.CollectionPost,
			RKey:       "abc123",
			CID:        "cid123",
			Record: map[string]interface{}{
				"text":      "Test post",
				"createdAt": "2025-01-01T12:00:00Z",
				"langs":     []interface{}{"en"},
			},
		},
	}

	// Convert to message - this is internal but we need to test it works
	msg, err := client.eventToMessage(event)
	if err != nil {
		t.Fatalf("eventToMessage() error = %v", err)
	}

	if msg == nil {
		t.Fatal("Expected non-nil message")
	}

	// Verify message fields
	if msg.GetChannelID() != "did:plc:test123" {
		t.Errorf("Channel ID = %s, want 'did:plc:test123'", msg.GetChannelID())
	}

	if msg.GetID() != "abc123" {
		t.Errorf("Message ID = %s, want 'abc123'", msg.GetID())
	}

	blueskyMsg, ok := msg.(*BlueskyMessage)
	if !ok {
		t.Fatal("Expected message to be *BlueskyMessage")
	}

	if blueskyMsg.Text != "Test post" {
		t.Errorf("Message text = %s, want 'Test post'", blueskyMsg.Text)
	}

	if blueskyMsg.Type != "app.bsky.feed.post" {
		t.Errorf("Message type = %s, want 'app.bsky.feed.post'", blueskyMsg.Type)
	}
}

func TestBlueskyClient_EventToMessage_Repost(t *testing.T) {
	client, err := NewBlueskyClient(BlueskyConfig{})
	if err != nil {
		t.Fatalf("NewBlueskyClient() error = %v", err)
	}

	// Create a repost event
	event := &blueskymodel.BlueskyEvent{
		DID:    "did:plc:reposter",
		TimeUS: 1234567890000000,
		Kind:   "commit",
		Commit: &blueskymodel.CommitEvent{
			Operation:  blueskymodel.OperationCreate,
			Collection: blueskymodel.CollectionRepost,
			RKey:       "repost123",
			CID:        "repostcid",
			Record: map[string]interface{}{
				"createdAt": "2025-01-01T12:00:00Z",
				"subject": map[string]interface{}{
					"uri": "at://did:plc:original/app.bsky.feed.post/original123",
					"cid": "originalcid",
				},
			},
		},
	}

	msg, err := client.eventToMessage(event)
	if err != nil {
		t.Fatalf("eventToMessage() error = %v", err)
	}

	blueskyMsg, ok := msg.(*BlueskyMessage)
	if !ok {
		t.Fatal("Expected message to be *BlueskyMessage")
	}

	if blueskyMsg.Type != "app.bsky.feed.repost" {
		t.Errorf("Message type = %s, want 'app.bsky.feed.repost'", blueskyMsg.Type)
	}
}

func TestBlueskyClient_EventToMessage_Like(t *testing.T) {
	client, err := NewBlueskyClient(BlueskyConfig{})
	if err != nil {
		t.Fatalf("NewBlueskyClient() error = %v", err)
	}

	// Create a like event
	event := &blueskymodel.BlueskyEvent{
		DID:    "did:plc:liker",
		TimeUS: 1234567890000000,
		Kind:   "commit",
		Commit: &blueskymodel.CommitEvent{
			Operation:  blueskymodel.OperationCreate,
			Collection: blueskymodel.CollectionLike,
			RKey:       "like123",
			CID:        "likecid",
			Record: map[string]interface{}{
				"createdAt": "2025-01-01T12:00:00Z",
				"subject": map[string]interface{}{
					"uri": "at://did:plc:liked/app.bsky.feed.post/liked123",
					"cid": "likedcid",
				},
			},
		},
	}

	msg, err := client.eventToMessage(event)
	if err != nil {
		t.Fatalf("eventToMessage() error = %v", err)
	}

	blueskyMsg, ok := msg.(*BlueskyMessage)
	if !ok {
		t.Fatal("Expected message to be *BlueskyMessage")
	}

	if blueskyMsg.Type != "app.bsky.feed.like" {
		t.Errorf("Message type = %s, want 'app.bsky.feed.like'", blueskyMsg.Type)
	}
}

func TestBlueskyClient_EventToMessage_InvalidEvent(t *testing.T) {
	client, err := NewBlueskyClient(BlueskyConfig{})
	if err != nil {
		t.Fatalf("NewBlueskyClient() error = %v", err)
	}

	tests := []struct {
		name  string
		event *blueskymodel.BlueskyEvent
	}{
		{
			name: "nil commit",
			event: &blueskymodel.BlueskyEvent{
				DID:    "did:plc:test",
				TimeUS: 123456,
				Kind:   "commit",
				Commit: nil,
			},
		},
		{
			name: "unsupported operation",
			event: &blueskymodel.BlueskyEvent{
				DID:    "did:plc:test",
				TimeUS: 123456,
				Kind:   "commit",
				Commit: &blueskymodel.CommitEvent{
					Operation:  blueskymodel.OperationUpdate,
					Collection: blueskymodel.CollectionPost,
				},
			},
		},
		{
			name: "unsupported collection",
			event: &blueskymodel.BlueskyEvent{
				DID:    "did:plc:test",
				TimeUS: 123456,
				Kind:   "commit",
				Commit: &blueskymodel.CommitEvent{
					Operation:  blueskymodel.OperationCreate,
					Collection: "app.bsky.unknown",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := client.eventToMessage(tt.event)
			if err == nil {
				t.Error("Expected error for invalid event, got nil")
			}
		})
	}
}

func TestBlueskyConfig(t *testing.T) {
	cursor := int64(123456789)
	config := BlueskyConfig{
		JetStreamURL:      "wss://test.url",
		WantedCollections: []string{"app.bsky.feed.post"},
		WantedDids:        []string{"did:plc:test"},
		BufferSize:        5000,
		Cursor:            &cursor,
	}

	if config.JetStreamURL != "wss://test.url" {
		t.Errorf("JetStreamURL = %s, want 'wss://test.url'", config.JetStreamURL)
	}

	if len(config.WantedCollections) != 1 {
		t.Errorf("WantedCollections length = %d, want 1", len(config.WantedCollections))
	}

	if len(config.WantedDids) != 1 {
		t.Errorf("WantedDids length = %d, want 1", len(config.WantedDids))
	}

	if config.BufferSize != 5000 {
		t.Errorf("BufferSize = %d, want 5000", config.BufferSize)
	}

	if config.Cursor == nil || *config.Cursor != cursor {
		t.Errorf("Cursor = %v, want %d", config.Cursor, cursor)
	}
}

func TestBlueskyClient_GetChannelType(t *testing.T) {
	client, err := NewBlueskyClient(BlueskyConfig{})
	if err != nil {
		t.Fatalf("NewBlueskyClient() error = %v", err)
	}

	channelType := client.GetChannelType()
	if channelType != "bluesky" {
		t.Errorf("GetChannelType() = %s, want 'bluesky'", channelType)
	}
}

func TestBlueskyClient_ThreadSafety(t *testing.T) {
	client, err := NewBlueskyClient(BlueskyConfig{})
	if err != nil {
		t.Fatalf("NewBlueskyClient() error = %v", err)
	}

	done := make(chan bool)
	const goroutines = 10

	// Test concurrent access to isConnected flag
	for i := 0; i < goroutines; i++ {
		go func() {
			client.mu.RLock()
			_ = client.isConnected
			client.mu.RUnlock()
			done <- true
		}()
	}

	for i := 0; i < goroutines; i++ {
		<-done
	}

	// Test concurrent cursor updates
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			client.mu.Lock()
			cursor := int64(id * 1000)
			client.cursor = &cursor
			client.mu.Unlock()
			done <- true
		}(i)
	}

	for i := 0; i < goroutines; i++ {
		<-done
	}

	// If we get here without deadlock or race conditions, test passes
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && containsSubstring(s, substr))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

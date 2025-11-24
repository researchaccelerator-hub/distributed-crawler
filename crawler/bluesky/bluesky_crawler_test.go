package bluesky

import (
	"context"
	"testing"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/client"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/researchaccelerator-hub/telegram-scraper/model"
	blueskymodel "github.com/researchaccelerator-hub/telegram-scraper/model/bluesky"
)

// MockBlueskyClient implements client.Client for testing
type MockBlueskyClient struct {
	messages []client.Message
	connected bool
}

func (m *MockBlueskyClient) Connect(ctx context.Context) error {
	m.connected = true
	return nil
}

func (m *MockBlueskyClient) Disconnect(ctx context.Context) error {
	m.connected = false
	return nil
}

func (m *MockBlueskyClient) GetChannelInfo(ctx context.Context, channelID string) (client.Channel, error) {
	return &client.BlueskyChannel{
		ID:   channelID,
		Name: channelID,
	}, nil
}

func (m *MockBlueskyClient) GetMessages(ctx context.Context, channelID string, fromTime, toTime time.Time, limit int) ([]client.Message, error) {
	return m.messages, nil
}

func (m *MockBlueskyClient) GetChannelType() string {
	return "bluesky"
}

// MockStateManager implements state.StateManagementInterface for testing
type MockStateManager struct {
	posts []model.Post
}

func (m *MockStateManager) Initialize(seedURLs []string) error                     { return nil }
func (m *MockStateManager) AddLayer(pages []interface{}) error                     { return nil }
func (m *MockStateManager) GetLayerByDepth(depth int) ([]interface{}, error)       { return nil, nil }
func (m *MockStateManager) UpdatePage(page interface{}) error                      { return nil }
func (m *MockStateManager) GetMaxDepth() (int, error)                              { return 0, nil }
func (m *MockStateManager) SaveState() error                                       { return nil }
func (m *MockStateManager) Close() error                                           { return nil }
func (m *MockStateManager) FindIncompleteCrawl(crawlID string) (string, error)    { return "", nil }
func (m *MockStateManager) UpdateCrawlMetadata(crawlID string, metadata map[string]interface{}) error { return nil }
func (m *MockStateManager) StorePost(channelID string, post model.Post) error {
	m.posts = append(m.posts, post)
	return nil
}
func (m *MockStateManager) HasProcessedMedia(mediaID string) (bool, error)          { return false, nil }
func (m *MockStateManager) MarkMediaAsProcessed(mediaID string) error               { return nil }

func TestNewBlueskyCrawler(t *testing.T) {
	crawler := NewBlueskyCrawler()

	if crawler == nil {
		t.Fatal("NewBlueskyCrawler returned nil")
	}

	bc, ok := crawler.(*BlueskyCrawler)
	if !ok {
		t.Fatal("NewBlueskyCrawler did not return *BlueskyCrawler")
	}

	if bc.initialized {
		t.Error("New crawler should not be initialized")
	}
}

func TestBlueskyCrawlerInitialize(t *testing.T) {
	bc := NewBlueskyCrawler().(*BlueskyCrawler)
	mockClient := &MockBlueskyClient{}
	mockStateManager := &MockStateManager{}

	config := map[string]interface{}{
		"client":        mockClient,
		"state_manager": mockStateManager,
		"crawl_label":   "test-label",
	}

	err := bc.Initialize(context.Background(), config)
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	if !bc.initialized {
		t.Error("Crawler should be initialized")
	}

	if bc.crawlLabel != "test-label" {
		t.Errorf("Expected crawl_label 'test-label', got '%s'", bc.crawlLabel)
	}
}

func TestBlueskyCrawlerInitializeErrors(t *testing.T) {
	tests := []struct {
		name   string
		config map[string]interface{}
		wantErr bool
	}{
		{
			name:    "missing client",
			config:  map[string]interface{}{"state_manager": &MockStateManager{}},
			wantErr: true,
		},
		{
			name:    "missing state_manager",
			config:  map[string]interface{}{"client": &MockBlueskyClient{}},
			wantErr: true,
		},
		{
			name:    "invalid client type",
			config:  map[string]interface{}{"client": "not a client", "state_manager": &MockStateManager{}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bc := NewBlueskyCrawler().(*BlueskyCrawler)
			err := bc.Initialize(context.Background(), tt.config)

			if (err != nil) != tt.wantErr {
				t.Errorf("Initialize() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateTarget(t *testing.T) {
	bc := NewBlueskyCrawler().(*BlueskyCrawler)

	tests := []struct {
		name    string
		target  crawler.CrawlTarget
		wantErr bool
	}{
		{
			name:    "valid firehose",
			target:  crawler.CrawlTarget{Type: crawler.PlatformBluesky, ID: "firehose"},
			wantErr: false,
		},
		{
			name:    "valid DID",
			target:  crawler.CrawlTarget{Type: crawler.PlatformBluesky, ID: "did:plc:z72i7hdynmk6r22z27h6tvur"},
			wantErr: false,
		},
		{
			name:    "valid handle",
			target:  crawler.CrawlTarget{Type: crawler.PlatformBluesky, ID: "user.bsky.social"},
			wantErr: false,
		},
		{
			name:    "invalid platform",
			target:  crawler.CrawlTarget{Type: crawler.PlatformYouTube, ID: "firehose"},
			wantErr: true,
		},
		{
			name:    "empty ID",
			target:  crawler.CrawlTarget{Type: crawler.PlatformBluesky, ID: ""},
			wantErr: true,
		},
		{
			name:    "invalid DID format",
			target:  crawler.CrawlTarget{Type: crawler.PlatformBluesky, ID: "did:invalid"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := bc.ValidateTarget(tt.target)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateTarget() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGetChannelInfo(t *testing.T) {
	bc := NewBlueskyCrawler().(*BlueskyCrawler)
	mockClient := &MockBlueskyClient{}

	config := map[string]interface{}{
		"client":        mockClient,
		"state_manager": &MockStateManager{},
	}

	bc.Initialize(context.Background(), config)

	tests := []struct {
		name     string
		targetID string
		wantName string
	}{
		{
			name:     "firehose",
			targetID: "firehose",
			wantName: "Bluesky Firehose",
		},
		{
			name:     "user DID",
			targetID: "did:plc:xyz123",
			wantName: "did:plc:xyz123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := crawler.CrawlTarget{
				Type: crawler.PlatformBluesky,
				ID:   tt.targetID,
			}

			channelData, err := bc.GetChannelInfo(context.Background(), target)
			if err != nil {
				t.Fatalf("GetChannelInfo failed: %v", err)
			}

			if channelData.ChannelName != tt.wantName {
				t.Errorf("Expected channel name '%s', got '%s'", tt.wantName, channelData.ChannelName)
			}
		})
	}
}

func TestFetchMessages(t *testing.T) {
	bc := NewBlueskyCrawler().(*BlueskyCrawler)

	// Create mock messages
	mockMessages := []client.Message{
		&client.BlueskyMessage{
			ID:          "abc123",
			ChannelID:   "did:plc:test",
			Text:        "Test post",
			Description: "Test post",
			Timestamp:   time.Now(),
			Type:        "app.bsky.feed.post",
		},
	}

	mockClient := &MockBlueskyClient{messages: mockMessages}
	mockStateManager := &MockStateManager{}

	config := map[string]interface{}{
		"client":        mockClient,
		"state_manager": mockStateManager,
		"crawl_label":   "test-crawl",
	}

	bc.Initialize(context.Background(), config)

	target := crawler.CrawlTarget{
		Type: crawler.PlatformBluesky,
		ID:   "did:plc:test",
	}

	job := crawler.CrawlJob{
		Target:   target,
		FromTime: time.Now().Add(-1 * time.Hour),
		ToTime:   time.Now(),
		Limit:    10,
	}

	result, err := bc.FetchMessages(context.Background(), job)
	if err != nil {
		t.Fatalf("FetchMessages failed: %v", err)
	}

	if len(result.Posts) != 1 {
		t.Errorf("Expected 1 post, got %d", len(result.Posts))
	}

	if len(mockStateManager.posts) != 1 {
		t.Errorf("Expected 1 post stored in state manager, got %d", len(mockStateManager.posts))
	}

	post := result.Posts[0]
	if post.PlatformName != "Bluesky" {
		t.Errorf("Expected platform 'Bluesky', got '%s'", post.PlatformName)
	}

	if post.CrawlLabel != "test-crawl" {
		t.Errorf("Expected crawl label 'test-crawl', got '%s'", post.CrawlLabel)
	}
}

func TestConvertMessageToPost(t *testing.T) {
	bc := NewBlueskyCrawler().(*BlueskyCrawler)
	bc.crawlLabel = "test-label"

	msg := &client.BlueskyMessage{
		ID:          "test123",
		ChannelID:   "did:plc:user123",
		SenderID:    "did:plc:user123",
		SenderName:  "testuser",
		Text:        "Hello Bluesky!",
		Description: "Hello Bluesky!",
		Timestamp:   time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC),
		Language:    "en",
		Type:        "app.bsky.feed.post",
	}

	channelData := &model.ChannelData{
		ChannelID:   "did:plc:user123",
		ChannelName: "Test User",
	}

	post, err := bc.convertMessageToPost(msg, channelData)
	if err != nil {
		t.Fatalf("convertMessageToPost failed: %v", err)
	}

	// Verify post fields
	if post.PostUID != "did:plc:user123-test123" {
		t.Errorf("Expected PostUID 'did:plc:user123-test123', got '%s'", post.PostUID)
	}

	if post.Description != "Hello Bluesky!" {
		t.Errorf("Expected description 'Hello Bluesky!', got '%s'", post.Description)
	}

	if post.PlatformName != "Bluesky" {
		t.Errorf("Expected platform 'Bluesky', got '%s'", post.PlatformName)
	}

	if post.ChannelID != "did:plc:user123" {
		t.Errorf("Expected ChannelID 'did:plc:user123', got '%s'", post.ChannelID)
	}

	if post.Handle != "testuser" {
		t.Errorf("Expected handle 'testuser', got '%s'", post.Handle)
	}

	if post.CrawlLabel != "test-label" {
		t.Errorf("Expected crawl label 'test-label', got '%s'", post.CrawlLabel)
	}

	expectedURL := "https://bsky.app/profile/did:plc:user123/post/test123"
	if post.URL != expectedURL {
		t.Errorf("Expected URL '%s', got '%s'", expectedURL, post.URL)
	}
}

func TestGetPlatformType(t *testing.T) {
	bc := NewBlueskyCrawler().(*BlueskyCrawler)

	platformType := bc.GetPlatformType()
	if platformType != crawler.PlatformBluesky {
		t.Errorf("Expected platform type 'bluesky', got '%s'", platformType)
	}
}

func TestConvertBlueskyPostToModelPost(t *testing.T) {
	bc := NewBlueskyCrawler().(*BlueskyCrawler)
	bc.crawlLabel = "test"

	blueskyPost := &blueskymodel.BlueskyPost{
		DID:       "did:plc:xyz",
		RecordKey: "abc123",
		Text:      "Test post with reply",
		CreatedAt: time.Now(),
		Languages: []string{"en"},
		ReplyTo: &blueskymodel.ReplyReference{
			Parent: &blueskymodel.StrongRef{
				URI: "at://did:plc:parent/app.bsky.feed.post/parent123",
				CID: "cid123",
			},
		},
	}

	channelData := &model.ChannelData{
		ChannelID:   "did:plc:xyz",
		ChannelName: "Test Channel",
	}

	post, err := bc.convertBlueskyPostToModelPost(blueskyPost, channelData)
	if err != nil {
		t.Fatalf("convertBlueskyPostToModelPost failed: %v", err)
	}

	if post.Description != "Test post with reply" {
		t.Errorf("Expected description 'Test post with reply', got '%s'", post.Description)
	}

	if post.LanguageCode != "en" {
		t.Errorf("Expected language 'en', got '%s'", post.LanguageCode)
	}

	if post.RepliedID == nil {
		t.Error("Expected RepliedID to be set")
	} else if *post.RepliedID != "parent123" {
		t.Errorf("Expected RepliedID 'parent123', got '%s'", *post.RepliedID)
	}

	if post.IsReply == nil || !*post.IsReply {
		t.Error("Expected IsReply to be true")
	}
}

func TestProcessEmbed(t *testing.T) {
	bc := NewBlueskyCrawler().(*BlueskyCrawler)

	post := &model.Post{
		ChannelID: "did:plc:test",
		PostType:  []string{"post"},
	}

	tests := []struct {
		name         string
		embed        *blueskymodel.BlueskyEmbed
		wantPostType []string
		wantHasEmbed bool
	}{
		{
			name: "image embed",
			embed: &blueskymodel.BlueskyEmbed{
				Type: blueskymodel.EmbedTypeImages,
				Images: []blueskymodel.EmbedImage{
					{
						Image: &blueskymodel.BlobRef{
							Ref: &blueskymodel.BlobReference{
								Link: "bafytest123",
							},
						},
					},
				},
			},
			wantPostType: []string{"post", "image"},
			wantHasEmbed: true,
		},
		{
			name: "external link",
			embed: &blueskymodel.BlueskyEmbed{
				Type: blueskymodel.EmbedTypeExternal,
				External: &blueskymodel.EmbedExternal{
					URI: "https://example.com",
				},
			},
			wantPostType: []string{"post", "link"},
			wantHasEmbed: false,
		},
		{
			name: "quote post",
			embed: &blueskymodel.BlueskyEmbed{
				Type: blueskymodel.EmbedTypeRecord,
			},
			wantPostType: []string{"post", "quote"},
			wantHasEmbed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			post.PostType = []string{"post"}
			post.HasEmbedMedia = nil
			post.Outlinks = []string{}

			bc.processEmbed(tt.embed, post)

			if len(post.PostType) != len(tt.wantPostType) {
				t.Errorf("Expected %d post types, got %d", len(tt.wantPostType), len(post.PostType))
			}

			for i, pt := range tt.wantPostType {
				if i >= len(post.PostType) || post.PostType[i] != pt {
					t.Errorf("Expected post type[%d] '%s', got '%s'", i, pt, post.PostType[i])
				}
			}

			if tt.wantHasEmbed {
				if post.HasEmbedMedia == nil || !*post.HasEmbedMedia {
					t.Error("Expected HasEmbedMedia to be true")
				}
			}
		})
	}
}

package bluesky

import (
	"testing"
	"time"

	blueskymodel "github.com/researchaccelerator-hub/telegram-scraper/model/bluesky"
)

func TestProcessPostEvent(t *testing.T) {
	event := &blueskymodel.BlueskyEvent{
		DID:    "did:plc:test123",
		TimeUS: 1234567890,
		Kind:   "commit",
		Commit: &blueskymodel.CommitEvent{
			Rev:        "rev123",
			Operation:  blueskymodel.OperationCreate,
			Collection: blueskymodel.CollectionPost,
			RKey:       "abc123",
			CID:        "cid123",
			Record: map[string]interface{}{
				"text":      "Hello world!",
				"createdAt": "2025-01-01T12:00:00Z",
				"langs":     []interface{}{"en", "es"},
			},
		},
	}

	post, err := processPostEvent(event)
	if err != nil {
		t.Fatalf("processPostEvent failed: %v", err)
	}

	if post.DID != "did:plc:test123" {
		t.Errorf("Expected DID 'did:plc:test123', got '%s'", post.DID)
	}

	if post.RecordKey != "abc123" {
		t.Errorf("Expected RecordKey 'abc123', got '%s'", post.RecordKey)
	}

	if post.Text != "Hello world!" {
		t.Errorf("Expected text 'Hello world!', got '%s'", post.Text)
	}

	if len(post.Languages) != 2 {
		t.Errorf("Expected 2 languages, got %d", len(post.Languages))
	}

	if post.Languages[0] != "en" {
		t.Errorf("Expected first language 'en', got '%s'", post.Languages[0])
	}

	expectedTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	if !post.CreatedAt.Equal(expectedTime) {
		t.Errorf("Expected time %v, got %v", expectedTime, post.CreatedAt)
	}
}

func TestProcessPostEventWithReply(t *testing.T) {
	event := &blueskymodel.BlueskyEvent{
		DID:    "did:plc:test",
		TimeUS: 1234567890,
		Kind:   "commit",
		Commit: &blueskymodel.CommitEvent{
			Operation:  blueskymodel.OperationCreate,
			Collection: blueskymodel.CollectionPost,
			RKey:       "reply123",
			Record: map[string]interface{}{
				"text":      "This is a reply",
				"createdAt": "2025-01-01T12:00:00Z",
				"reply": map[string]interface{}{
					"root": map[string]interface{}{
						"uri": "at://did:plc:root/app.bsky.feed.post/root123",
						"cid": "rootcid",
					},
					"parent": map[string]interface{}{
						"uri": "at://did:plc:parent/app.bsky.feed.post/parent123",
						"cid": "parentcid",
					},
				},
			},
		},
	}

	post, err := processPostEvent(event)
	if err != nil {
		t.Fatalf("processPostEvent failed: %v", err)
	}

	if post.ReplyTo == nil {
		t.Fatal("Expected ReplyTo to be set")
	}

	if post.ReplyTo.Root == nil {
		t.Error("Expected Root to be set")
	} else if post.ReplyTo.Root.URI != "at://did:plc:root/app.bsky.feed.post/root123" {
		t.Errorf("Expected root URI, got '%s'", post.ReplyTo.Root.URI)
	}

	if post.ReplyTo.Parent == nil {
		t.Error("Expected Parent to be set")
	} else if post.ReplyTo.Parent.URI != "at://did:plc:parent/app.bsky.feed.post/parent123" {
		t.Errorf("Expected parent URI, got '%s'", post.ReplyTo.Parent.URI)
	}
}

func TestProcessPostEventWithEmbed(t *testing.T) {
	event := &blueskymodel.BlueskyEvent{
		DID:    "did:plc:test",
		TimeUS: 1234567890,
		Kind:   "commit",
		Commit: &blueskymodel.CommitEvent{
			Operation:  blueskymodel.OperationCreate,
			Collection: blueskymodel.CollectionPost,
			RKey:       "embed123",
			Record: map[string]interface{}{
				"text":      "Check out this image!",
				"createdAt": "2025-01-01T12:00:00Z",
				"embed": map[string]interface{}{
					"$type": "app.bsky.embed.images",
					"images": []interface{}{
						map[string]interface{}{
							"alt": "Test image",
							"image": map[string]interface{}{
								"$type":    "blob",
								"mimeType": "image/jpeg",
								"size":     float64(12345),
								"ref": map[string]interface{}{
									"$link": "bafytest123",
								},
							},
						},
					},
				},
			},
		},
	}

	post, err := processPostEvent(event)
	if err != nil {
		t.Fatalf("processPostEvent failed: %v", err)
	}

	if post.Embed == nil {
		t.Fatal("Expected Embed to be set")
	}

	if post.Embed.Type != "app.bsky.embed.images" {
		t.Errorf("Expected embed type 'app.bsky.embed.images', got '%s'", post.Embed.Type)
	}

	if len(post.Embed.Images) != 1 {
		t.Fatalf("Expected 1 image, got %d", len(post.Embed.Images))
	}

	img := post.Embed.Images[0]
	if img.Alt != "Test image" {
		t.Errorf("Expected alt 'Test image', got '%s'", img.Alt)
	}

	if img.Image == nil {
		t.Fatal("Expected image blob to be set")
	}

	if img.Image.MimeType != "image/jpeg" {
		t.Errorf("Expected mime type 'image/jpeg', got '%s'", img.Image.MimeType)
	}
}

func TestProcessRepostEvent(t *testing.T) {
	event := &blueskymodel.BlueskyEvent{
		DID:    "did:plc:reposter",
		TimeUS: 1234567890,
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

	repost, err := processRepostEvent(event)
	if err != nil {
		t.Fatalf("processRepostEvent failed: %v", err)
	}

	if repost.DID != "did:plc:reposter" {
		t.Errorf("Expected DID 'did:plc:reposter', got '%s'", repost.DID)
	}

	if repost.RecordKey != "repost123" {
		t.Errorf("Expected RecordKey 'repost123', got '%s'", repost.RecordKey)
	}

	if repost.Subject == nil {
		t.Fatal("Expected Subject to be set")
	}

	if repost.Subject.URI != "at://did:plc:original/app.bsky.feed.post/original123" {
		t.Errorf("Expected subject URI, got '%s'", repost.Subject.URI)
	}
}

func TestProcessLikeEvent(t *testing.T) {
	event := &blueskymodel.BlueskyEvent{
		DID:    "did:plc:liker",
		TimeUS: 1234567890,
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

	like, err := processLikeEvent(event)
	if err != nil {
		t.Fatalf("processLikeEvent failed: %v", err)
	}

	if like.DID != "did:plc:liker" {
		t.Errorf("Expected DID 'did:plc:liker', got '%s'", like.DID)
	}

	if like.Subject == nil {
		t.Fatal("Expected Subject to be set")
	}

	if like.Subject.URI != "at://did:plc:liked/app.bsky.feed.post/liked123" {
		t.Errorf("Expected subject URI, got '%s'", like.Subject.URI)
	}
}

func TestParseFacets(t *testing.T) {
	facetsData := []interface{}{
		map[string]interface{}{
			"index": map[string]interface{}{
				"byteStart": float64(0),
				"byteEnd":   float64(10),
			},
			"features": []interface{}{
				map[string]interface{}{
					"$type": "app.bsky.richtext.facet#mention",
					"did":   "did:plc:mentioned",
				},
			},
		},
		map[string]interface{}{
			"index": map[string]interface{}{
				"byteStart": float64(15),
				"byteEnd":   float64(35),
			},
			"features": []interface{}{
				map[string]interface{}{
					"$type": "app.bsky.richtext.facet#link",
					"uri":   "https://example.com",
				},
			},
		},
	}

	facets := parseFacets(facetsData)

	if len(facets) != 2 {
		t.Fatalf("Expected 2 facets, got %d", len(facets))
	}

	// Test first facet (mention)
	facet1 := facets[0]
	if facet1.Index == nil {
		t.Fatal("Expected Index to be set")
	}
	if facet1.Index.ByteStart != 0 || facet1.Index.ByteEnd != 10 {
		t.Errorf("Expected byte range 0-10, got %d-%d", facet1.Index.ByteStart, facet1.Index.ByteEnd)
	}

	if len(facet1.Features) != 1 {
		t.Fatalf("Expected 1 feature, got %d", len(facet1.Features))
	}

	if facet1.Features[0].Type != blueskymodel.FacetMention {
		t.Errorf("Expected mention type, got '%s'", facet1.Features[0].Type)
	}

	if facet1.Features[0].DID != "did:plc:mentioned" {
		t.Errorf("Expected DID 'did:plc:mentioned', got '%s'", facet1.Features[0].DID)
	}

	// Test second facet (link)
	facet2 := facets[1]
	if len(facet2.Features) != 1 {
		t.Fatalf("Expected 1 feature, got %d", len(facet2.Features))
	}

	if facet2.Features[0].Type != blueskymodel.FacetLink {
		t.Errorf("Expected link type, got '%s'", facet2.Features[0].Type)
	}

	if facet2.Features[0].URI != "https://example.com" {
		t.Errorf("Expected URI 'https://example.com', got '%s'", facet2.Features[0].URI)
	}
}

func TestExtractOutlinks(t *testing.T) {
	post := &blueskymodel.BlueskyPost{
		Facets: []blueskymodel.BlueskyFacet{
			{
				Features: []blueskymodel.FacetFeature{
					{
						Type: blueskymodel.FacetMention,
						DID:  "did:plc:user1",
					},
					{
						Type: blueskymodel.FacetLink,
						URI:  "https://example.com",
					},
				},
			},
			{
				Features: []blueskymodel.FacetFeature{
					{
						Type: blueskymodel.FacetMention,
						DID:  "did:plc:user2",
					},
				},
			},
		},
	}

	outlinks := extractOutlinks(post)

	if len(outlinks) != 3 {
		t.Errorf("Expected 3 outlinks, got %d", len(outlinks))
	}

	// Check that we got the DID and URL
	expectedLinks := map[string]bool{
		"did:plc:user1":      false,
		"did:plc:user2":      false,
		"https://example.com": false,
	}

	for _, link := range outlinks {
		if _, exists := expectedLinks[link]; exists {
			expectedLinks[link] = true
		} else {
			t.Errorf("Unexpected outlink: %s", link)
		}
	}

	for link, found := range expectedLinks {
		if !found {
			t.Errorf("Expected outlink '%s' not found", link)
		}
	}
}

func TestExtractPostIDFromURI(t *testing.T) {
	tests := []struct {
		uri      string
		expected string
	}{
		{
			uri:      "at://did:plc:xyz/app.bsky.feed.post/abc123",
			expected: "abc123",
		},
		{
			uri:      "at://did:plc:xyz/app.bsky.feed.post/xyz789",
			expected: "xyz789",
		},
		{
			uri:      "abc123",
			expected: "abc123",
		},
		{
			uri:      "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.uri, func(t *testing.T) {
			result := extractPostIDFromURI(tt.uri)
			if result != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestParseEmbed(t *testing.T) {
	tests := []struct {
		name      string
		embedData map[string]interface{}
		wantType  string
		wantCheck func(*testing.T, *blueskymodel.BlueskyEmbed)
	}{
		{
			name: "image embed",
			embedData: map[string]interface{}{
				"$type": "app.bsky.embed.images",
				"images": []interface{}{
					map[string]interface{}{
						"alt": "Test",
					},
				},
			},
			wantType: "app.bsky.embed.images",
			wantCheck: func(t *testing.T, embed *blueskymodel.BlueskyEmbed) {
				if len(embed.Images) != 1 {
					t.Errorf("Expected 1 image, got %d", len(embed.Images))
				}
			},
		},
		{
			name: "external link",
			embedData: map[string]interface{}{
				"$type": "app.bsky.embed.external",
				"external": map[string]interface{}{
					"uri":         "https://example.com",
					"title":       "Example",
					"description": "An example link",
				},
			},
			wantType: "app.bsky.embed.external",
			wantCheck: func(t *testing.T, embed *blueskymodel.BlueskyEmbed) {
				if embed.External == nil {
					t.Fatal("Expected External to be set")
				}
				if embed.External.URI != "https://example.com" {
					t.Errorf("Expected URI 'https://example.com', got '%s'", embed.External.URI)
				}
				if embed.External.Title != "Example" {
					t.Errorf("Expected title 'Example', got '%s'", embed.External.Title)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			embed := parseEmbed(tt.embedData)

			if embed.Type != tt.wantType {
				t.Errorf("Expected type '%s', got '%s'", tt.wantType, embed.Type)
			}

			if tt.wantCheck != nil {
				tt.wantCheck(t, embed)
			}
		})
	}
}

func TestProcessCommitEvent(t *testing.T) {
	tests := []struct {
		name      string
		event     *blueskymodel.BlueskyEvent
		wantErr   bool
		checkType string
	}{
		{
			name: "post event",
			event: &blueskymodel.BlueskyEvent{
				DID: "did:plc:test",
				Commit: &blueskymodel.CommitEvent{
					Operation:  blueskymodel.OperationCreate,
					Collection: blueskymodel.CollectionPost,
					RKey:       "test123",
					Record: map[string]interface{}{
						"text":      "Hello",
						"createdAt": "2025-01-01T00:00:00Z",
					},
				},
			},
			wantErr:   false,
			checkType: "*bluesky.BlueskyPost",
		},
		{
			name: "repost event",
			event: &blueskymodel.BlueskyEvent{
				DID: "did:plc:test",
				Commit: &blueskymodel.CommitEvent{
					Operation:  blueskymodel.OperationCreate,
					Collection: blueskymodel.CollectionRepost,
					RKey:       "repost123",
					Record: map[string]interface{}{
						"createdAt": "2025-01-01T00:00:00Z",
						"subject": map[string]interface{}{
							"uri": "at://test",
							"cid": "test",
						},
					},
				},
			},
			wantErr:   false,
			checkType: "*bluesky.BlueskyRepost",
		},
		{
			name: "update operation (should error)",
			event: &blueskymodel.BlueskyEvent{
				DID: "did:plc:test",
				Commit: &blueskymodel.CommitEvent{
					Operation: blueskymodel.OperationUpdate,
				},
			},
			wantErr: true,
		},
		{
			name: "unsupported collection",
			event: &blueskymodel.BlueskyEvent{
				DID: "did:plc:test",
				Commit: &blueskymodel.CommitEvent{
					Operation:  blueskymodel.OperationCreate,
					Collection: "app.bsky.unknown",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := processCommitEvent(tt.event)

			if (err != nil) != tt.wantErr {
				t.Errorf("processCommitEvent() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr && result == nil {
				t.Error("Expected result to be non-nil when no error")
			}
		})
	}
}

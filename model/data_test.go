package model

import (
	"encoding/json"
	"testing"
	"time"
)

func TestPost_Serialization(t *testing.T) {
	now := time.Now()
	videoLength := 120
	isVerified := true
	hasEmbedMedia := true
	isReply := false
	sharedID := "shared-123"
	quotedID := "quoted-456"
	repliedID := "replied-789"

	post := Post{
		PostLink:     "https://example.com/post/123",
		ChannelID:    "channel-456",
		PostUID:      "post-uid-789",
		URL:          "https://example.com/post/123",
		PublishedAt:  now,
		CreatedAt:    now,
		LanguageCode: "en",
		Engagement:   1500,
		ViewCount:    10000,
		LikeCount:    500,
		ShareCount:   100,
		CommentCount: 50,
		CrawlLabel:   "test-crawl",
		ChannelName:  "Test Channel",
		IsAd:         false,
		VideoLength:  &videoLength,
		IsVerified:   &isVerified,
		PlatformName: "Bluesky",
		SharedID:     &sharedID,
		QuotedID:     &quotedID,
		RepliedID:    &repliedID,
		HasEmbedMedia: &hasEmbedMedia,
		Description:   "Test post description",
		PostType:      []string{"post", "image"},
		IsReply:       &isReply,
		LikesCount:    500,
		SharesCount:   100,
		CommentsCount: 50,
		ViewsCount:    10000,
		SearchableText: "Test searchable text",
		AllText:        "Test all text content",
		ThumbURL:       "https://cdn.example.com/thumb.jpg",
		MediaURL:       "https://cdn.example.com/media.mp4",
		Handle:         "@testuser",
		CaptureTime:    now,
		ChannelData: ChannelData{
			ChannelID:          "channel-456",
			ChannelName:        "Test Channel",
			ChannelDescription: "A test channel",
			ChannelURLExternal: "https://example.com/channel/456",
			ChannelURL:         "channel-456",
			CountryCode:        "US",
			PublishedAt:        now,
			ChannelEngagementData: EngagementData{
				FollowerCount: 50000,
				LikeCount:     100000,
				PostCount:     1000,
				ViewsCount:    500000,
			},
		},
		Comments: []Comment{
			{
				Text:      "Great post!",
				ViewCount: 100,
				Handle:    "@commenter1",
				Reactions: map[string]int{"👍": 5, "❤️": 3},
			},
		},
		Reactions: map[string]int{"👍": 250, "❤️": 150, "🔥": 100},
		Outlinks:  []string{"https://link1.com", "https://link2.com"},
		MediaData: MediaData{
			DocumentName: "video.mp4",
		},
	}

	// Serialize to JSON
	jsonData, err := json.Marshal(post)
	if err != nil {
		t.Fatalf("Failed to serialize Post: %v", err)
	}

	// Deserialize from JSON
	var deserializedPost Post
	err = json.Unmarshal(jsonData, &deserializedPost)
	if err != nil {
		t.Fatalf("Failed to deserialize Post: %v", err)
	}

	// Verify basic fields
	if deserializedPost.PostUID != post.PostUID {
		t.Errorf("PostUID = %s, want %s", deserializedPost.PostUID, post.PostUID)
	}
	if deserializedPost.ChannelID != post.ChannelID {
		t.Errorf("ChannelID = %s, want %s", deserializedPost.ChannelID, post.ChannelID)
	}
	if deserializedPost.PlatformName != post.PlatformName {
		t.Errorf("PlatformName = %s, want %s", deserializedPost.PlatformName, post.PlatformName)
	}
	if deserializedPost.ViewCount != post.ViewCount {
		t.Errorf("ViewCount = %d, want %d", deserializedPost.ViewCount, post.ViewCount)
	}
	if deserializedPost.LikeCount != post.LikeCount {
		t.Errorf("LikeCount = %d, want %d", deserializedPost.LikeCount, post.LikeCount)
	}

	// Verify pointer fields
	if deserializedPost.VideoLength == nil || *deserializedPost.VideoLength != *post.VideoLength {
		t.Error("VideoLength not correctly deserialized")
	}
	if deserializedPost.IsVerified == nil || *deserializedPost.IsVerified != *post.IsVerified {
		t.Error("IsVerified not correctly deserialized")
	}
	if deserializedPost.SharedID == nil || *deserializedPost.SharedID != *post.SharedID {
		t.Error("SharedID not correctly deserialized")
	}

	// Verify nested structures
	if deserializedPost.ChannelData.ChannelID != post.ChannelData.ChannelID {
		t.Errorf("ChannelData.ChannelID = %s, want %s",
			deserializedPost.ChannelData.ChannelID, post.ChannelData.ChannelID)
	}
	if deserializedPost.ChannelData.ChannelEngagementData.FollowerCount != post.ChannelData.ChannelEngagementData.FollowerCount {
		t.Errorf("ChannelEngagementData.FollowerCount = %d, want %d",
			deserializedPost.ChannelData.ChannelEngagementData.FollowerCount,
			post.ChannelData.ChannelEngagementData.FollowerCount)
	}

	// Verify collections
	if len(deserializedPost.Comments) != len(post.Comments) {
		t.Errorf("Comments count = %d, want %d", len(deserializedPost.Comments), len(post.Comments))
	}
	if len(deserializedPost.Reactions) != len(post.Reactions) {
		t.Errorf("Reactions count = %d, want %d", len(deserializedPost.Reactions), len(post.Reactions))
	}
	if len(deserializedPost.Outlinks) != len(post.Outlinks) {
		t.Errorf("Outlinks count = %d, want %d", len(deserializedPost.Outlinks), len(post.Outlinks))
	}
	if len(deserializedPost.PostType) != len(post.PostType) {
		t.Errorf("PostType count = %d, want %d", len(deserializedPost.PostType), len(post.PostType))
	}
}

func TestChannelData_Serialization(t *testing.T) {
	now := time.Now()

	channelData := ChannelData{
		ChannelID:             "ch-123",
		ChannelName:           "Test Channel",
		ChannelDescription:    "A comprehensive test channel",
		ChannelProfileImage:   "https://cdn.example.com/profile.jpg",
		ChannelURLExternal:    "https://platform.com/channel/ch-123",
		ChannelURL:            "ch-123",
		CountryCode:           "GB",
		PublishedAt:           now,
		ChannelEngagementData: EngagementData{
			FollowerCount:  100000,
			FollowingCount: 500,
			LikeCount:      250000,
			PostCount:      5000,
			ViewsCount:     10000000,
			CommentCount:   50000,
			ShareCount:     25000,
		},
	}

	// Serialize
	jsonData, err := json.Marshal(channelData)
	if err != nil {
		t.Fatalf("Failed to serialize ChannelData: %v", err)
	}

	// Deserialize
	var deserializedData ChannelData
	err = json.Unmarshal(jsonData, &deserializedData)
	if err != nil {
		t.Fatalf("Failed to deserialize ChannelData: %v", err)
	}

	// Verify
	if deserializedData.ChannelID != channelData.ChannelID {
		t.Errorf("ChannelID = %s, want %s", deserializedData.ChannelID, channelData.ChannelID)
	}
	if deserializedData.CountryCode != channelData.CountryCode {
		t.Errorf("CountryCode = %s, want %s", deserializedData.CountryCode, channelData.CountryCode)
	}
	if deserializedData.ChannelEngagementData.FollowerCount != channelData.ChannelEngagementData.FollowerCount {
		t.Errorf("FollowerCount = %d, want %d",
			deserializedData.ChannelEngagementData.FollowerCount,
			channelData.ChannelEngagementData.FollowerCount)
	}
}

func TestComment_Serialization(t *testing.T) {
	comment := Comment{
		Text:       "This is a test comment",
		ViewCount:  500,
		ReplyCount: 10,
		Handle:     "@testuser",
		Reactions: map[string]int{
			"👍": 25,
			"❤️": 15,
			"😂": 5,
		},
	}

	// Serialize
	jsonData, err := json.Marshal(comment)
	if err != nil {
		t.Fatalf("Failed to serialize Comment: %v", err)
	}

	// Deserialize
	var deserializedComment Comment
	err = json.Unmarshal(jsonData, &deserializedComment)
	if err != nil {
		t.Fatalf("Failed to deserialize Comment: %v", err)
	}

	// Verify
	if deserializedComment.Text != comment.Text {
		t.Errorf("Text = %s, want %s", deserializedComment.Text, comment.Text)
	}
	if deserializedComment.ViewCount != comment.ViewCount {
		t.Errorf("ViewCount = %d, want %d", deserializedComment.ViewCount, comment.ViewCount)
	}
	if deserializedComment.Handle != comment.Handle {
		t.Errorf("Handle = %s, want %s", deserializedComment.Handle, comment.Handle)
	}
	if len(deserializedComment.Reactions) != len(comment.Reactions) {
		t.Errorf("Reactions count = %d, want %d", len(deserializedComment.Reactions), len(comment.Reactions))
	}
}

func TestEngagementData_Serialization(t *testing.T) {
	engagement := EngagementData{
		FollowerCount:  50000,
		FollowingCount: 200,
		LikeCount:      100000,
		PostCount:      2500,
		ViewsCount:     5000000,
		CommentCount:   25000,
		ShareCount:     12500,
	}

	// Serialize
	jsonData, err := json.Marshal(engagement)
	if err != nil {
		t.Fatalf("Failed to serialize EngagementData: %v", err)
	}

	// Deserialize
	var deserializedEngagement EngagementData
	err = json.Unmarshal(jsonData, &deserializedEngagement)
	if err != nil {
		t.Fatalf("Failed to deserialize EngagementData: %v", err)
	}

	// Verify all fields
	if deserializedEngagement.FollowerCount != engagement.FollowerCount {
		t.Errorf("FollowerCount = %d, want %d", deserializedEngagement.FollowerCount, engagement.FollowerCount)
	}
	if deserializedEngagement.LikeCount != engagement.LikeCount {
		t.Errorf("LikeCount = %d, want %d", deserializedEngagement.LikeCount, engagement.LikeCount)
	}
	if deserializedEngagement.ViewsCount != engagement.ViewsCount {
		t.Errorf("ViewsCount = %d, want %d", deserializedEngagement.ViewsCount, engagement.ViewsCount)
	}
}

func TestOCRData_Serialization(t *testing.T) {
	ocrData := OCRData{
		OCRText:  "Extracted text from image",
		ThumbURL: "https://cdn.example.com/thumb.jpg",
	}

	// Serialize
	jsonData, err := json.Marshal(ocrData)
	if err != nil {
		t.Fatalf("Failed to serialize OCRData: %v", err)
	}

	// Deserialize
	var deserializedOCR OCRData
	err = json.Unmarshal(jsonData, &deserializedOCR)
	if err != nil {
		t.Fatalf("Failed to deserialize OCRData: %v", err)
	}

	// Verify
	if deserializedOCR.OCRText != ocrData.OCRText {
		t.Errorf("OCRText = %s, want %s", deserializedOCR.OCRText, ocrData.OCRText)
	}
	if deserializedOCR.ThumbURL != ocrData.ThumbURL {
		t.Errorf("ThumbURL = %s, want %s", deserializedOCR.ThumbURL, ocrData.ThumbURL)
	}
}

func TestPerformanceScores_Serialization(t *testing.T) {
	likes := 1000
	shares := 500
	comments := 250

	scores := PerformanceScores{
		Likes:    &likes,
		Shares:   &shares,
		Comments: &comments,
		Views:    50000.5,
	}

	// Serialize
	jsonData, err := json.Marshal(scores)
	if err != nil {
		t.Fatalf("Failed to serialize PerformanceScores: %v", err)
	}

	// Deserialize
	var deserializedScores PerformanceScores
	err = json.Unmarshal(jsonData, &deserializedScores)
	if err != nil {
		t.Fatalf("Failed to deserialize PerformanceScores: %v", err)
	}

	// Verify
	if deserializedScores.Likes == nil || *deserializedScores.Likes != *scores.Likes {
		t.Error("Likes not correctly deserialized")
	}
	if deserializedScores.Shares == nil || *deserializedScores.Shares != *scores.Shares {
		t.Error("Shares not correctly deserialized")
	}
	if deserializedScores.Views != scores.Views {
		t.Errorf("Views = %f, want %f", deserializedScores.Views, scores.Views)
	}
}

func TestMediaData_Serialization(t *testing.T) {
	mediaData := MediaData{
		DocumentName: "video_file.mp4",
	}

	// Serialize
	jsonData, err := json.Marshal(mediaData)
	if err != nil {
		t.Fatalf("Failed to serialize MediaData: %v", err)
	}

	// Deserialize
	var deserializedMedia MediaData
	err = json.Unmarshal(jsonData, &deserializedMedia)
	if err != nil {
		t.Fatalf("Failed to deserialize MediaData: %v", err)
	}

	// Verify
	if deserializedMedia.DocumentName != mediaData.DocumentName {
		t.Errorf("DocumentName = %s, want %s", deserializedMedia.DocumentName, mediaData.DocumentName)
	}
}

func TestPost_EmptyCollections(t *testing.T) {
	post := Post{
		PostUID:      "empty-post",
		ChannelID:    "channel-1",
		PlatformName: "Test",
		Comments:     []Comment{},
		Reactions:    map[string]int{},
		Outlinks:     []string{},
		PostType:     []string{},
	}

	// Serialize
	jsonData, err := json.Marshal(post)
	if err != nil {
		t.Fatalf("Failed to serialize Post with empty collections: %v", err)
	}

	// Deserialize
	var deserializedPost Post
	err = json.Unmarshal(jsonData, &deserializedPost)
	if err != nil {
		t.Fatalf("Failed to deserialize Post with empty collections: %v", err)
	}

	// Verify empty collections are preserved
	if deserializedPost.Comments == nil {
		t.Error("Comments should be empty slice, not nil")
	}
	if deserializedPost.Reactions == nil {
		t.Error("Reactions should be empty map, not nil")
	}
	if deserializedPost.Outlinks == nil {
		t.Error("Outlinks should be empty slice, not nil")
	}
}

func TestPost_NilPointers(t *testing.T) {
	post := Post{
		PostUID:       "nil-pointers-post",
		ChannelID:     "channel-2",
		PlatformName:  "Test",
		VideoLength:   nil,
		IsVerified:    nil,
		HasEmbedMedia: nil,
		SharedID:      nil,
		QuotedID:      nil,
		RepliedID:     nil,
		IsReply:       nil,
	}

	// Serialize
	jsonData, err := json.Marshal(post)
	if err != nil {
		t.Fatalf("Failed to serialize Post with nil pointers: %v", err)
	}

	// Deserialize
	var deserializedPost Post
	err = json.Unmarshal(jsonData, &deserializedPost)
	if err != nil {
		t.Fatalf("Failed to deserialize Post with nil pointers: %v", err)
	}

	// Verify nil pointers remain nil
	if deserializedPost.VideoLength != nil {
		t.Error("VideoLength should be nil")
	}
	if deserializedPost.IsVerified != nil {
		t.Error("IsVerified should be nil")
	}
	if deserializedPost.SharedID != nil {
		t.Error("SharedID should be nil")
	}
}

func TestComment_EmptyReactions(t *testing.T) {
	comment := Comment{
		Text:      "Comment with no reactions",
		Handle:    "@user",
		Reactions: map[string]int{},
	}

	// Serialize
	jsonData, err := json.Marshal(comment)
	if err != nil {
		t.Fatalf("Failed to serialize Comment: %v", err)
	}

	// Deserialize
	var deserializedComment Comment
	err = json.Unmarshal(jsonData, &deserializedComment)
	if err != nil {
		t.Fatalf("Failed to deserialize Comment: %v", err)
	}

	// Verify
	if deserializedComment.Reactions == nil {
		t.Error("Reactions should be empty map, not nil")
	}
	if len(deserializedComment.Reactions) != 0 {
		t.Errorf("Reactions should be empty, got %d items", len(deserializedComment.Reactions))
	}
}

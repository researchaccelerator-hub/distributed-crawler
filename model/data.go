// Package model defines the data structures used to represent Telegram data
// in a standardized format for storage and processing.
package model

import "time"

// Post represents a complete Telegram or YouTube post with all associated metadata.
// This struct is used for storing and exporting post data in a standardized format.
type Post struct {
	PostLink                string            `json:"post_link"`
	ChannelID               string            `json:"channel_id"`
	PostUID                 string            `json:"post_uid"`
	URL                     string            `json:"url"`
	PublishedAt             time.Time         `json:"published_at"`
	CreatedAt               time.Time         `json:"created_at"`
	LanguageCode            string            `json:"language_code"`
	Engagement              int               `json:"engagement"`
	ViewCount               int               `json:"view_count"`
	LikeCount               int               `json:"like_count"`
	ShareCount              int               `json:"share_count"`
	CommentCount            int               `json:"comment_count"`
	CrawlLabel              string            `json:"crawl_label"`
	ListIDs                 []interface{}     `json:"list_ids"`
	ChannelName             string            `json:"channel_name"`
	SearchTerms             []interface{}     `json:"search_terms"`
	SearchTermIDs           []interface{}     `json:"search_term_ids"`
	ProjectIDs              []interface{}     `json:"project_ids"`
	ExerciseIDs             []interface{}     `json:"exercise_ids"`
	LabelData               []interface{}     `json:"label_data"`
	LabelsMetadata          []interface{}     `json:"labels_metadata"`
	ProjectLabeledPostIDs   []interface{}     `json:"project_labeled_post_ids"`
	LabelerIDs              []interface{}     `json:"labeler_ids"`
	AllLabels               []interface{}     `json:"all_labels"`
	LabelIDs                []interface{}     `json:"label_ids"`
	IsAd                    bool              `json:"is_ad"`
	TranscriptText          string            `json:"transcript_text"`
	ImageText               string            `json:"image_text"`
	VideoLength             *int              `json:"video_length"`
	IsVerified              *bool             `json:"is_verified"`
	ChannelData             ChannelData       `json:"channel_data"`
	PlatformName            string            `json:"platform_name"`
	SharedID                *string           `json:"shared_id"`
	QuotedID                *string           `json:"quoted_id"`
	RepliedID               *string           `json:"replied_id"`
	AILabel                 *string           `json:"ai_label"`
	RootPostID              *string           `json:"root_post_id"`
	EngagementStepsCount    int               `json:"engagement_steps_count"`
	OCRData                 []OCRData         `json:"ocr_data"`
	PerformanceScores       PerformanceScores `json:"performance_scores"`
	HasEmbedMedia           *bool             `json:"has_embed_media"`
	Description             string            `json:"description"`
	RepostChannelData       *string           `json:"repost_channel_data"`
	PostType                []string          `json:"post_type"`
	InnerLink               InnerLink         `json:"inner_link"`
	PostTitle               *string           `json:"post_title"`
	MediaData               MediaData         `json:"media_data"`
	IsReply                 *bool             `json:"is_reply"`
	AdFields                *string           `json:"ad_fields"`
	LikesCount              int               `json:"likes_count"`
	SharesCount             int               `json:"shares_count"`
	CommentsCount           int               `json:"comments_count"`
	ViewsCount              int               `json:"views_count"`
	SearchableText          string            `json:"searchable_text"`
	AllText                 string            `json:"all_text"`
	ContrastAgentProjectIDs []interface{}     `json:"contrast_agent_project_ids"`
	AgentIDs                []interface{}     `json:"agent_ids"`
	SegmentIDs              []interface{}     `json:"segment_ids"`
	ThumbURL                string            `json:"thumb_url"`
	MediaURL                string            `json:"media_url"`
	Comments                []Comment         `json:"comments"`
	Reactions               map[string]int    `json:"reactions"`
	Outlinks                []string          `json:"outlinks"`
	CaptureTime             time.Time         `json:"capture_time"`
	Handle                  string            `json:"handle"`
}
// Comment represents a single comment on a Telegram post, including
// its text content, reaction counts, and metadata.
type Comment struct {
	Text       string         `json:"text"`
	Reactions  map[string]int `json:"reactions"`
	ViewCount  int            `json:"view_count"`
	ReplyCount int            `json:"reply_count"`
	Handle     string         `json:"handle"`
}
// ChannelData contains information about a Telegram or YouTube channel, including
// its identifying information, engagement metrics, and URLs.
type ChannelData struct {
	ChannelID             string         `json:"channel_id"`
	ChannelName           string         `json:"channel_name"`
	ChannelDescription    string         `json:"channel_description"`
	ChannelProfileImage   string         `json:"channel_profile_image"`
	ChannelEngagementData EngagementData `json:"channel_engagement_data"`
	ChannelURLExternal    string         `json:"channel_url_external"`
	ChannelURL            string         `json:"channel_url"`
	CountryCode           string         `json:"country_code"`
	PublishedAt           time.Time      `json:"published_at"`
}

// EngagementData contains metrics about a channel's audience engagement,
// including follower counts, like counts, and other interaction statistics.
type EngagementData struct {
	FollowerCount  int `json:"follower_count"`
	FollowingCount int `json:"following_count"`
	LikeCount      int `json:"like_count"`
	PostCount      int `json:"post_count"`
	ViewsCount     int `json:"views_count"`
	CommentCount   int `json:"comment_count"`
	ShareCount     int `json:"share_count"`
}

// OCRData stores the text extracted from images through optical character recognition (OCR),
// along with the thumbnail URL of the processed image.
type OCRData struct {
	OCRText  string `json:"ocr_text"`
	ThumbURL string `json:"thumb_url"`
}

// PerformanceScores tracks engagement metrics for a post, including likes, shares,
// comments, and views, to measure its performance and reach.
type PerformanceScores struct {
	Likes    *int    `json:"likes"`
	Shares   *int    `json:"shares"`
	Comments *int    `json:"comments"`
	Views    float64 `json:"views"`
}

// InnerLink represents internal links within a Telegram post.
// This is a placeholder struct for future implementation of internal link tracking.
type InnerLink struct {
}

// MediaData contains information about media files associated with a post,
// such as images, videos, or other documents.
type MediaData struct {
	// DocumentName is the filename of the document as stored in the system
	DocumentName string `json:"document_name"`
}

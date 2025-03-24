package model

import "time"

type Post struct {
	PostLink                string            `json:"post_link"`
	ChannelID               int64             `json:"channel_id"`
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
type Comment struct {
	Text       string         `json:"text"`
	Reactions  map[string]int `json:"reactions"`
	ViewCount  int            `json:"view_count"`
	ReplyCount int            `json:"reply_count"`
	Handle     string         `json:"handle"`
}
type ChannelData struct {
	ChannelID             int64          `json:"channel_id"`
	ChannelName           string         `json:"channel_name"`
	ChannelProfileImage   string         `json:"channel_profile_image"`
	ChannelEngagementData EngagementData `json:"channel_engagement_data"`
	ChannelURLExternal    string         `json:"channel_url_external"`
	ChannelURL            string         `json:"channel_url"`
}

type EngagementData struct {
	FollowerCount  int `json:"follower_count"`
	FollowingCount int `json:"following_count"`
	LikeCount      int `json:"like_count"`
	PostCount      int `json:"post_count"`
	ViewsCount     int `json:"views_count"`
	CommentCount   int `json:"comment_count"`
	ShareCount     int `json:"share_count"`
}

type OCRData struct {
	OCRText  string `json:"ocr_text"`
	ThumbURL string `json:"thumb_url"`
}

type PerformanceScores struct {
	Likes    *int    `json:"likes"`
	Shares   *int    `json:"shares"`
	Comments *int    `json:"comments"`
	Views    float64 `json:"views"`
}

type InnerLink struct {
}

type MediaData struct {
	DocumentName string `json:"document_name"`
}

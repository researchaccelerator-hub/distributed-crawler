// Package bluesky defines data structures specific to Bluesky/AT Protocol events.
package bluesky

import "time"

// BlueskyEvent represents a JetStream event from the Bluesky firehose.
// Events are received in real-time and contain information about actions
// on the Bluesky network (posts, reposts, likes, follows, etc.).
type BlueskyEvent struct {
	DID    string        `json:"did"`     // Author's DID (Decentralized Identifier)
	TimeUS int64         `json:"time_us"` // Event timestamp in microseconds
	Kind   string        `json:"kind"`    // Event kind: "commit", "handle", "account"
	Commit *CommitEvent  `json:"commit"`  // Commit details (if kind == "commit")
}

// CommitEvent represents a commit operation in the AT Protocol repository.
// Commits contain the actual data changes (create, update, delete).
type CommitEvent struct {
	Rev        string                 `json:"rev"`        // Revision identifier
	Operation  string                 `json:"operation"`  // "create", "update", or "delete"
	Collection string                 `json:"collection"` // Collection type (e.g., "app.bsky.feed.post")
	RKey       string                 `json:"rkey"`       // Record key
	Record     map[string]interface{} `json:"record"`     // The actual record data
	CID        string                 `json:"cid"`        // Content identifier
}

// BlueskyPost represents a parsed Bluesky post with all relevant metadata.
// This is an intermediate structure before conversion to model.Post.
type BlueskyPost struct {
	DID         string          `json:"did"`          // Author DID
	RecordKey   string          `json:"record_key"`   // Record key (rkey)
	Text        string          `json:"text"`         // Post text content
	CreatedAt   time.Time       `json:"created_at"`   // Post creation time
	ReplyTo     *ReplyReference `json:"reply_to"`     // Parent post reference (if reply)
	QuoteOf     *StrongRef      `json:"quote_of"`     // Quoted post reference (if quote post)
	Embed       *BlueskyEmbed   `json:"embed"`        // Embedded media/links
	Facets      []BlueskyFacet  `json:"facets"`       // Mentions, links, hashtags
	Languages   []string        `json:"languages"`    // Language codes
	Handle      string          `json:"handle"`       // Author handle (@username)
	DisplayName string          `json:"display_name"` // Author display name
	CID         string          `json:"cid"`          // Content identifier
}

// BlueskyRepost represents a repost (share) event.
type BlueskyRepost struct {
	DID       string     `json:"did"`        // Author DID (person who reposted)
	RecordKey string     `json:"record_key"` // Record key
	CreatedAt time.Time  `json:"created_at"` // Repost creation time
	Subject   *StrongRef `json:"subject"`    // Original post being reposted
	Handle    string     `json:"handle"`     // Author handle
	CID       string     `json:"cid"`        // Content identifier
}

// BlueskyLike represents a like event.
type BlueskyLike struct {
	DID       string     `json:"did"`        // Author DID (person who liked)
	RecordKey string     `json:"record_key"` // Record key
	CreatedAt time.Time  `json:"created_at"` // Like creation time
	Subject   *StrongRef `json:"subject"`    // Post being liked
	Handle    string     `json:"handle"`     // Author handle
	CID       string     `json:"cid"`        // Content identifier
}

// ReplyReference contains information about the post being replied to.
type ReplyReference struct {
	Root   *StrongRef `json:"root"`   // Root post of the thread
	Parent *StrongRef `json:"parent"` // Immediate parent post
}

// StrongRef is a strong reference to another record (post, repost, etc.).
type StrongRef struct {
	URI string `json:"uri"` // AT URI (e.g., "at://did:plc:.../app.bsky.feed.post/...")
	CID string `json:"cid"` // Content identifier
}

// BlueskyEmbed represents embedded content in a post (images, videos, links, etc.).
type BlueskyEmbed struct {
	Type      string                 `json:"$type"`      // Embed type
	Images    []EmbedImage           `json:"images"`     // Image embeds
	External  *EmbedExternal         `json:"external"`   // External link embed
	Record    *EmbedRecord           `json:"record"`     // Record embed (quote post)
	Video     *EmbedVideo            `json:"video"`      // Video embed
	RecordKey string                 `json:"record_key"` // For record embeds
	RawData   map[string]interface{} `json:"-"`          // Raw embed data for unknown types
}

// EmbedImage represents an embedded image.
type EmbedImage struct {
	Alt   string     `json:"alt"`   // Alt text
	Image *BlobRef   `json:"image"` // Image blob reference
	Thumb *string    `json:"thumb"` // Thumbnail URL (if available)
}

// EmbedExternal represents an external link embed.
type EmbedExternal struct {
	URI         string   `json:"uri"`         // External URL
	Title       string   `json:"title"`       // Link title
	Description string   `json:"description"` // Link description
	Thumb       *BlobRef `json:"thumb"`       // Thumbnail blob
}

// EmbedRecord represents an embedded record (quote post).
type EmbedRecord struct {
	Record *StrongRef `json:"record"` // Referenced record
}

// EmbedVideo represents an embedded video.
type EmbedVideo struct {
	Video    *BlobRef `json:"video"`    // Video blob reference
	Captions []string `json:"captions"` // Video captions
	Alt      string   `json:"alt"`      // Alt text
}

// BlobRef is a reference to a blob (file) in the AT Protocol.
type BlobRef struct {
	Type     string          `json:"$type"`    // Blob type
	Ref      *BlobReference  `json:"ref"`      // Reference to the blob
	MimeType string          `json:"mimeType"` // MIME type
	Size     int             `json:"size"`     // Size in bytes
}

// BlobReference contains the actual blob reference data.
type BlobReference struct {
	Link string `json:"$link"` // CID of the blob
}

// BlueskyFacet represents a facet in post text (mention, link, hashtag, etc.).
// Facets are used to identify and link entities within the text.
type BlueskyFacet struct {
	Index    *FacetIndex            `json:"index"`    // Character range in text
	Features []FacetFeature         `json:"features"` // Feature types
	RawData  map[string]interface{} `json:"-"`        // Raw facet data
}

// FacetIndex defines a character range within the post text.
type FacetIndex struct {
	ByteStart int `json:"byteStart"` // Start position (byte offset)
	ByteEnd   int `json:"byteEnd"`   // End position (byte offset)
}

// FacetFeature represents a specific feature type within a facet.
type FacetFeature struct {
	Type    string                 `json:"$type"`    // Feature type
	DID     string                 `json:"did"`      // For mentions
	URI     string                 `json:"uri"`      // For links
	Tag     string                 `json:"tag"`      // For hashtags
	RawData map[string]interface{} `json:"-"`        // Raw feature data
}

// BlueskyProfile represents a user profile on Bluesky.
type BlueskyProfile struct {
	DID         string    `json:"did"`          // User DID
	Handle      string    `json:"handle"`       // User handle (@username)
	DisplayName string    `json:"displayName"`  // Display name
	Description string    `json:"description"`  // Bio
	Avatar      string    `json:"avatar"`       // Avatar URL
	Banner      string    `json:"banner"`       // Banner URL
	FollowersCount int    `json:"followersCount"` // Number of followers
	FollowsCount   int    `json:"followsCount"`   // Number of follows
	PostsCount     int    `json:"postsCount"`     // Number of posts
	CreatedAt      time.Time `json:"createdAt"`   // Account creation time
}

// EventType constants for different Bluesky event collections.
const (
	CollectionPost   = "app.bsky.feed.post"
	CollectionRepost = "app.bsky.feed.repost"
	CollectionLike   = "app.bsky.feed.like"
	CollectionFollow = "app.bsky.graph.follow"
	CollectionBlock  = "app.bsky.graph.block"
	CollectionProfile = "app.bsky.actor.profile"
)

// Operation constants for commit operations.
const (
	OperationCreate = "create"
	OperationUpdate = "update"
	OperationDelete = "delete"
)

// Facet feature type constants.
const (
	FacetMention = "app.bsky.richtext.facet#mention"
	FacetLink    = "app.bsky.richtext.facet#link"
	FacetTag     = "app.bsky.richtext.facet#tag"
)

// Embed type constants.
const (
	EmbedTypeImages   = "app.bsky.embed.images"
	EmbedTypeExternal = "app.bsky.embed.external"
	EmbedTypeRecord   = "app.bsky.embed.record"
	EmbedTypeVideo    = "app.bsky.embed.video"
	EmbedTypeRecordWithMedia = "app.bsky.embed.recordWithMedia"
)

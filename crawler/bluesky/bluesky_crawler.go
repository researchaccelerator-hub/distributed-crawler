// Package bluesky implements the Crawler interface for Bluesky/AT Protocol.
package bluesky

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/client"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/researchaccelerator-hub/telegram-scraper/model"
	blueskymodel "github.com/researchaccelerator-hub/telegram-scraper/model/bluesky"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/rs/zerolog/log"
)

// BlueskyCrawler implements the Crawler interface for Bluesky.
type BlueskyCrawler struct {
	client       client.Client
	stateManager state.StateManagementInterface
	crawlLabel   string
	initialized  bool
}

// NewBlueskyCrawler creates a new Bluesky crawler instance.
func NewBlueskyCrawler() crawler.Crawler {
	return &BlueskyCrawler{
		initialized: false,
	}
}

// Initialize sets up the Bluesky crawler with the necessary configuration.
func (c *BlueskyCrawler) Initialize(ctx context.Context, config map[string]interface{}) error {
	log.Info().Msg("Initializing Bluesky crawler")

	// Extract client from config
	if clientObj, ok := config["client"]; ok {
		if blueskyClient, ok := clientObj.(client.Client); ok {
			c.client = blueskyClient
		} else {
			return fmt.Errorf("invalid client type in config")
		}
	} else {
		return fmt.Errorf("client not provided in config")
	}

	// Extract state manager from config
	if smObj, ok := config["state_manager"]; ok {
		if sm, ok := smObj.(state.StateManagementInterface); ok {
			c.stateManager = sm
		} else {
			return fmt.Errorf("invalid state_manager type in config")
		}
	} else {
		return fmt.Errorf("state_manager not provided in config")
	}

	// Extract crawl label (optional)
	if label, ok := config["crawl_label"].(string); ok {
		c.crawlLabel = label
	}

	c.initialized = true
	log.Info().Str("crawl_label", c.crawlLabel).Msg("Bluesky crawler initialized successfully")
	return nil
}

// ValidateTarget checks if a target is valid for the Bluesky crawler.
func (c *BlueskyCrawler) ValidateTarget(target crawler.CrawlTarget) error {
	if target.Type != crawler.PlatformBluesky {
		return fmt.Errorf("invalid platform type: %s (expected bluesky)", target.Type)
	}

	// Validate target ID format
	id := target.ID
	if id == "" {
		return fmt.Errorf("target ID cannot be empty")
	}

	// Allow "firehose" for full network crawling
	if id == "firehose" {
		return nil
	}

	// Validate DID format (did:plc:... or did:web:...)
	if strings.HasPrefix(id, "did:") {
		parts := strings.Split(id, ":")
		if len(parts) < 3 {
			return fmt.Errorf("invalid DID format: %s", id)
		}
		return nil
	}

	// Validate handle format (@username.bsky.social or username.bsky.social)
	if strings.Contains(id, ".") {
		return nil
	}

	return fmt.Errorf("invalid target ID format: %s (expected DID, handle, or 'firehose')", id)
}

// GetChannelInfo retrieves information about a Bluesky user/channel.
func (c *BlueskyCrawler) GetChannelInfo(ctx context.Context, target crawler.CrawlTarget) (*model.ChannelData, error) {
	log.Info().Str("target_id", target.ID).Msg("Getting Bluesky channel info")

	if !c.initialized {
		return nil, fmt.Errorf("crawler not initialized")
	}

	// For "firehose", return generic channel data
	if target.ID == "firehose" {
		return &model.ChannelData{
			ChannelID:          "firehose",
			ChannelName:        "Bluesky Firehose",
			ChannelDescription: "Full Bluesky network firehose",
			ChannelURLExternal: "https://bsky.app",
			ChannelURL:         "firehose",
			PublishedAt:        time.Now(),
		}, nil
	}

	// Get channel info from client
	channel, err := c.client.GetChannelInfo(ctx, target.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get channel info: %w", err)
	}

	// Convert to ChannelData
	channelData := &model.ChannelData{
		ChannelID:          channel.GetID(),
		ChannelName:        channel.GetName(),
		ChannelDescription: channel.GetName(), // Bluesky client may not provide description
		ChannelURLExternal: fmt.Sprintf("https://bsky.app/profile/%s", target.ID),
		ChannelURL:         target.ID,
		PublishedAt:        time.Now(),
	}

	return channelData, nil
}

// FetchMessages retrieves messages (posts) from the Bluesky firehose.
func (c *BlueskyCrawler) FetchMessages(ctx context.Context, job crawler.CrawlJob) (crawler.CrawlResult, error) {
	log.Info().
		Str("target_id", job.Target.ID).
		Time("from_time", job.FromTime).
		Time("to_time", job.ToTime).
		Int("limit", job.Limit).
		Msg("Fetching Bluesky messages")

	if !c.initialized {
		return crawler.CrawlResult{}, fmt.Errorf("crawler not initialized")
	}

	result := crawler.CrawlResult{
		Posts:  make([]model.Post, 0),
		Errors: make([]error, 0),
	}

	// Get channel info
	channelData, err := c.GetChannelInfo(ctx, job.Target)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get channel info")
		result.Errors = append(result.Errors, err)
		return result, err
	}

	// Get messages from client
	messages, err := c.client.GetMessages(ctx, job.Target.ID, job.FromTime, job.ToTime, job.Limit)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get messages")
		result.Errors = append(result.Errors, err)
		return result, err
	}

	log.Info().Int("message_count", len(messages)).Msg("Retrieved messages from Bluesky")

	// Convert messages to Posts
	for _, msg := range messages {
		post, err := c.convertMessageToPost(msg, channelData)
		if err != nil {
			log.Warn().Err(err).Str("msg_id", msg.GetID()).Msg("Failed to convert message to post")
			result.Errors = append(result.Errors, err)
			continue
		}

		// Store the post using state manager
		if c.stateManager != nil {
			if err := c.stateManager.StorePost(job.Target.ID, *post); err != nil {
				log.Error().Err(err).Str("post_id", post.PostUID).Msg("Failed to store post")
				result.Errors = append(result.Errors, err)
			}
		}

		result.Posts = append(result.Posts, *post)
	}

	log.Info().
		Int("posts_count", len(result.Posts)).
		Int("errors_count", len(result.Errors)).
		Msg("Completed Bluesky message fetch")

	return result, nil
}

// convertMessageToPost converts a Bluesky message to the unified Post model.
func (c *BlueskyCrawler) convertMessageToPost(msg client.Message, channelData *model.ChannelData) (*model.Post, error) {
	// Create post UID
	postUID := fmt.Sprintf("%s-%s", msg.GetChannelID(), msg.GetID())

	// Build post URL (AT URI or web URL)
	postURL := fmt.Sprintf("https://bsky.app/profile/%s/post/%s", msg.GetChannelID(), msg.GetID())

	// Extract text content
	description := msg.GetText()
	if description == "" {
		description = msg.GetDescription()
	}

	// Create the post
	post := &model.Post{
		PostUID:     postUID,
		URL:         postURL,
		PostLink:    postURL,
		ChannelID:   msg.GetChannelID(),
		ChannelName: channelData.ChannelName,
		Handle:      msg.GetSenderName(),
		PublishedAt: msg.GetTimestamp(),
		CreatedAt:   time.Now(),
		CaptureTime: time.Now(),

		// Content
		Description:    description,
		SearchableText: description,
		AllText:        description,

		// Platform
		PlatformName: "Bluesky",
		CrawlLabel:   c.crawlLabel,

		// Engagement (basic from message)
		ViewCount:    int(msg.GetViews()),
		CommentCount: int(msg.GetCommentCount()),

		// Channel data
		ChannelData: *channelData,

		// Language
		LanguageCode: msg.GetLanguage(),

		// Initialize empty slices/maps
		Reactions: make(map[string]int),
		Outlinks:  make([]string, 0),
		PostType:  []string{"post"},
		Comments:  make([]model.Comment, 0),
	}

	// Extract reactions (likes mapped to reactions)
	if msg.GetReactions() != nil {
		for k, v := range msg.GetReactions() {
			post.Reactions[k] = int(v)
			post.LikeCount += int(v)
		}
	}

	// Calculate engagement
	post.Engagement = post.ViewCount + post.LikeCount + post.CommentCount + post.ShareCount

	// Extract thumbnails
	thumbnails := msg.GetThumbnails()
	if len(thumbnails) > 0 {
		// Use the first thumbnail
		for _, thumbURL := range thumbnails {
			post.ThumbURL = thumbURL
			break
		}
	}

	return post, nil
}

// convertBlueskyPostToModelPost converts a parsed Bluesky post to the unified Post model.
// This is for more detailed post data when we have the full Bluesky post structure.
func (c *BlueskyCrawler) convertBlueskyPostToModelPost(blueskyPost *blueskymodel.BlueskyPost, channelData *model.ChannelData) (*model.Post, error) {
	// Create post UID
	postUID := fmt.Sprintf("%s-%s", blueskyPost.DID, blueskyPost.RecordKey)

	// Build post URL
	postURL := fmt.Sprintf("https://bsky.app/profile/%s/post/%s", blueskyPost.DID, blueskyPost.RecordKey)

	// Create the post
	post := &model.Post{
		PostUID:     postUID,
		URL:         postURL,
		PostLink:    postURL,
		ChannelID:   blueskyPost.DID,
		ChannelName: channelData.ChannelName,
		Handle:      blueskyPost.Handle,
		PublishedAt: blueskyPost.CreatedAt,
		CreatedAt:   time.Now(),
		CaptureTime: time.Now(),

		// Content
		Description:    blueskyPost.Text,
		SearchableText: blueskyPost.Text,
		AllText:        blueskyPost.Text,

		// Platform
		PlatformName: "Bluesky",
		CrawlLabel:   c.crawlLabel,

		// Channel data
		ChannelData: *channelData,

		// Language
		LanguageCode: "",

		// Initialize empty slices/maps
		Reactions: make(map[string]int),
		Outlinks:  make([]string, 0),
		PostType:  []string{"post"},
		Comments:  make([]model.Comment, 0),
	}

	// Set language if available
	if len(blueskyPost.Languages) > 0 {
		post.LanguageCode = blueskyPost.Languages[0]
	}

	// Handle reply
	if blueskyPost.ReplyTo != nil && blueskyPost.ReplyTo.Parent != nil {
		parentID := extractPostIDFromURI(blueskyPost.ReplyTo.Parent.URI)
		post.RepliedID = &parentID
		post.IsReply = boolPtr(true)
	}

	// Handle quote post
	if blueskyPost.QuoteOf != nil {
		quotedID := extractPostIDFromURI(blueskyPost.QuoteOf.URI)
		post.QuotedID = &quotedID
	}

	// Extract outlinks from facets
	post.Outlinks = extractOutlinks(blueskyPost)

	// Handle embeds
	if blueskyPost.Embed != nil {
		c.processEmbed(blueskyPost.Embed, post)
	}

	// Calculate engagement (we don't have like/share counts from firehose directly)
	post.Engagement = post.ViewCount + post.LikeCount + post.CommentCount + post.ShareCount

	return post, nil
}

// processEmbed processes embed data and updates the post accordingly.
func (c *BlueskyCrawler) processEmbed(embed *blueskymodel.BlueskyEmbed, post *model.Post) {
	switch embed.Type {
	case blueskymodel.EmbedTypeImages:
		if len(embed.Images) > 0 {
			// Use first image as thumbnail
			if embed.Images[0].Image != nil && embed.Images[0].Image.Ref != nil {
				// Construct blob URL
				blobCID := embed.Images[0].Image.Ref.Link
				post.ThumbURL = fmt.Sprintf("https://cdn.bsky.app/img/feed_thumbnail/plain/%s/%s@jpeg", post.ChannelID, blobCID)
			}
			post.PostType = append(post.PostType, "image")
			post.HasEmbedMedia = boolPtr(true)
		}

	case blueskymodel.EmbedTypeExternal:
		if embed.External != nil {
			// Add external link to outlinks
			if embed.External.URI != "" {
				post.Outlinks = append(post.Outlinks, embed.External.URI)
			}
			post.PostType = append(post.PostType, "link")
		}

	case blueskymodel.EmbedTypeVideo:
		if embed.Video != nil && embed.Video.Video != nil && embed.Video.Video.Ref != nil {
			// Construct video URL
			blobCID := embed.Video.Video.Ref.Link
			post.MediaURL = fmt.Sprintf("https://video.bsky.app/watch/%s/%s/playlist.m3u8", post.ChannelID, blobCID)
			post.PostType = append(post.PostType, "video")
			post.HasEmbedMedia = boolPtr(true)
		}

	case blueskymodel.EmbedTypeRecord:
		// Quote post - already handled via QuotedID
		post.PostType = append(post.PostType, "quote")
	}
}

// GetPlatformType returns the platform type for this crawler.
func (c *BlueskyCrawler) GetPlatformType() crawler.PlatformType {
	return crawler.PlatformBluesky
}

// Close cleans up resources used by the crawler.
func (c *BlueskyCrawler) Close() error {
	log.Info().Msg("Closing Bluesky crawler")

	if c.client != nil {
		if err := c.client.Disconnect(context.Background()); err != nil {
			log.Warn().Err(err).Msg("Error disconnecting Bluesky client")
			return err
		}
	}

	c.initialized = false
	return nil
}

// Helper function to create bool pointer
func boolPtr(b bool) *bool {
	return &b
}

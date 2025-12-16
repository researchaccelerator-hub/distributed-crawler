// Package telegram implements the Telegram-specific crawler functionality
package telegram

import (
	"context"
	"fmt"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/client"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/researchaccelerator-hub/telegram-scraper/model"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/rs/zerolog/log"
)

// TelegramCrawler implements the crawler.Crawler interface for Telegram
type TelegramCrawler struct {
	client       client.Client
	stateManager state.StateManager
	initialized  bool
}

// NewTelegramCrawler creates a new Telegram crawler
func NewTelegramCrawler() crawler.Crawler {
	return &TelegramCrawler{
		initialized: false,
	}
}

// Initialize sets up the Telegram crawler
func (c *TelegramCrawler) Initialize(ctx context.Context, config map[string]interface{}) error {
	if c.initialized {
		return nil
	}

	// Extract client and state manager from config
	clientObj, ok := config["client"]
	if !ok {
		return fmt.Errorf("client not provided in config")
	}

	telegramClient, ok := clientObj.(client.Client)
	if !ok {
		return fmt.Errorf("provided client is not a valid Telegram client")
	}

	stateManagerObj, ok := config["state_manager"]
	if !ok {
		return fmt.Errorf("state_manager not provided in config")
	}

	stateManager, ok := stateManagerObj.(state.StateManager)
	if !ok {
		return fmt.Errorf("provided state_manager is not valid")
	}

	c.client = telegramClient
	c.stateManager = stateManager
	c.initialized = true

	return nil
}

// ValidateTarget checks if a target is valid for Telegram
func (c *TelegramCrawler) ValidateTarget(target crawler.CrawlTarget) error {
	if target.Type != crawler.PlatformTelegram {
		return fmt.Errorf("invalid target type: %s, expected: telegram", target.Type)
	}

	if target.ID == "" {
		return fmt.Errorf("target ID cannot be empty")
	}

	return nil
}

// GetChannelInfo retrieves information about a Telegram channel
func (c *TelegramCrawler) GetChannelInfo(ctx context.Context, target crawler.CrawlTarget) (*model.ChannelData, error) {
	if err := c.ValidateTarget(target); err != nil {
		return nil, err
	}

	if !c.initialized {
		return nil, fmt.Errorf("crawler not initialized")
	}

	channel, err := c.client.GetChannelInfo(ctx, target.ID)
	if err != nil {
		log.Error().Err(err).Str("channel_id", target.ID).Msg("Failed to get channel info")
		return nil, err
	}

	// Convert client.Channel to model.ChannelData
	channelData := &model.ChannelData{
		ChannelID:          channel.GetID(),
		ChannelName:        channel.GetName(),
		ChannelDescription: channel.GetDescription(),
		// ChannelProfile image unavailable
		ChannelEngagementData: model.EngagementData{
			FollowerCount: int(channel.GetMemberCount()),
			// FollowingCount unavailable
			// LikeCount unavailable
			// PostCount unavailable
			// ViewCount unavailable
			// Comment count unavailable
			// Share count unavailable
		},
		ChannelURL:         fmt.Sprintf("https://t.me/%s", channel.GetID()),
		ChannelURLExternal: fmt.Sprintf("https://t.me/%s", channel.GetID()),
		CountryCode:        channel.GetCountry(),
		// PublishedAt unavailable
	}

	return channelData, nil
}

// FetchMessages retrieves messages from Telegram
func (c *TelegramCrawler) FetchMessages(ctx context.Context, job crawler.CrawlJob) (crawler.CrawlResult, error) {
	if err := c.ValidateTarget(job.Target); err != nil {
		return crawler.CrawlResult{}, err
	}

	if !c.initialized {
		return crawler.CrawlResult{}, fmt.Errorf("crawler not initialized")
	}

	log.Info().
		Str("channel_id", job.Target.ID).
		Time("from_time", job.FromTime).
		Time("to_time", job.ToTime).
		Int("limit", job.Limit).
		Msg("Starting Telegram crawl")

	messages, err := c.client.GetMessages(ctx, job.Target.ID, job.FromTime, job.ToTime, job.Limit)
	if err != nil {
		log.Error().Err(err).Str("channel_id", job.Target.ID).Msg("Failed to get messages")
		return crawler.CrawlResult{}, err
	}

	log.Info().
		Str("channel_id", job.Target.ID).
		Int("message_count", len(messages)).
		Msg("Retrieved messages")

	// Process and convert messages to model.Post format
	posts := make([]model.Post, 0, len(messages))
	for _, msg := range messages {
		post := c.convertMessageToPost(msg)
		result := job.NullValidator.ValidatePost(&post)
		if !result.Valid {
			log.Error().Strs("errors", result.Errors).Msg("FetchMessages: Missing critical fields in telegram post data")
		}
		posts = append(posts, post)
	}

	return crawler.CrawlResult{
		Posts:  posts,
		Errors: nil,
	}, nil
}

// GetPlatformType returns the type of platform this crawler supports
func (c *TelegramCrawler) GetPlatformType() crawler.PlatformType {
	return crawler.PlatformTelegram
}

// Close cleans up resources
func (c *TelegramCrawler) Close() error {
	if c.client != nil {
		if err := c.client.Disconnect(context.Background()); err != nil {
			log.Error().Err(err).Msg("Error disconnecting client")
			return err
		}
	}
	return nil
}

// convertMessageToPost converts a client.Message to model.Post
func (c *TelegramCrawler) convertMessageToPost(message client.Message) model.Post {
	// TODO: get more available fieds in here like crawllabel
	post := model.Post{
		// PostLink unavailable
		ChannelID: message.GetChannelID(),
		PostUID:   message.GetID(),
		// URL unavailable
		PublishedAt: message.GetTimestamp(),
		CreatedAt:   time.Now(),
		// Language Code unavailable
		// Engagement unavailable
		ViewCount: int(message.GetViews()),
		// LikeCount unavailable
		// ShareCount unavailable
		// CommentCount unavailable
		// CrawlLabel unavaiable
		// ListIDs unavailable
		ChannelName: message.GetChannelID(),
		// SearchTerms unavailable
		// SearchTermIDs unavailable
		// ProjectIDs unavailable
		// ExerciseIDs unavailable
		// LabelData unavailable
		// LabelsMetadata unavailable
		// ProjectLabeledPostIDs unavailable
		// LabelerIDs unavailable
		// AllLabels unavailable
		// LabelIDs unavailable
		// IsAd unavailable
		// TranscriptText unavailable
		// ImageText unavailable
		// VideoLength unavailable
		// IsVerified unavailable
		// ChannelData unavailable (CHECK THIS)
		PlatformName: "telegram",
		// SharedID unavailable
		// QuotedID unavailable
		// RepliedID unavailable
		// AILabel unavailable
		// RootPostID unavailable
		// EngagementStepsCount unavailable
		// OCRData unavailable
		// Performance Scores unavailable
		// HasEmbedMedia unavailable
		Description: message.GetText(),
		// RepostChannelData unavailable
		// PostType unavailable
		// InnerLink unavailable
		// PostTitle unavailable
		// MediaData unavailable
		// IsReply unavailable
		// AdFields unavailable
		// LikesCount unavailable
		// SharesCount unavailable
		// CommentsCount unavailable
		ViewsCount:     int(message.GetViews()),
		SearchableText: message.GetText(),
		AllText:        message.GetText(),
		// ContrastAgentProjectIDs unavailable
		// AgentIDs unavailable
		// SegmentIDs unavailable
		// ThumbURL unavailable
		// MediaURL unavailable
		// Comments collected elsewhere
		// Reactions collected below
		// Outlinks collected later
		CaptureTime: time.Now(),
		Handle:      message.GetSenderName(),
	}

	// Set reactions if available
	if len(message.GetReactions()) > 0 {
		// Convert map[string]int64 to map[string]int
		reactions := make(map[string]int)
		for k, v := range message.GetReactions() {
			reactions[k] = int(v)
		}
		post.Reactions = reactions
	}

	// Add more fields as needed based on the message content

	return post
}

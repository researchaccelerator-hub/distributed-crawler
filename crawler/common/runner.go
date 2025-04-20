// Package common provides shared functionality for all crawler implementations
package common

import (
	"context"
	"fmt"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/rs/zerolog/log"
)

// CrawlRunner manages crawl jobs and processes them with the appropriate crawler
type CrawlRunner struct {
	factory      crawler.CrawlerFactory
	stateManager state.StateManager
	crawlers     map[crawler.PlatformType]crawler.Crawler
}

// NewCrawlRunner creates a new runner
func NewCrawlRunner(factory crawler.CrawlerFactory, stateManager state.StateManager) *CrawlRunner {
	return &CrawlRunner{
		factory:      factory,
		stateManager: stateManager,
		crawlers:     make(map[crawler.PlatformType]crawler.Crawler),
	}
}

// ExecuteJob runs a crawl job
func (r *CrawlRunner) ExecuteJob(ctx context.Context, job crawler.CrawlJob) (crawler.CrawlResult, error) {
	// Get or create crawler for the target type
	c, err := r.getCrawler(ctx, job.Target.Type)
	if err != nil {
		log.Error().Err(err).Str("platform", string(job.Target.Type)).Msg("Failed to get crawler")
		return crawler.CrawlResult{}, err
	}

	// Validate the target
	if err := c.ValidateTarget(job.Target); err != nil {
		log.Error().Err(err).Str("platform", string(job.Target.Type)).Str("target_id", job.Target.ID).Msg("Invalid target")
		return crawler.CrawlResult{}, err
	}

	// Execute the crawl
	result, err := c.FetchMessages(ctx, job)
	if err != nil {
		log.Error().Err(err).Str("platform", string(job.Target.Type)).Str("target_id", job.Target.ID).Msg("Failed to fetch messages")
		return crawler.CrawlResult{}, err
	}

	// Store the results using the state manager
	if len(result.Posts) > 0 {
		for _, post := range result.Posts {
			if err := r.stateManager.SavePost(ctx, post); err != nil {
				log.Error().Err(err).Str("post_id", post.PostUID).Msg("Failed to save post")
				// Continue processing other posts even if one fails
			}
		}
	}

	return result, nil
}

// getCrawler gets or creates a crawler for the specified type
func (r *CrawlRunner) getCrawler(ctx context.Context, platformType crawler.PlatformType) (crawler.Crawler, error) {
	// Check if we already have an initialized crawler
	if c, exists := r.crawlers[platformType]; exists {
		return c, nil
	}

	// Create a new crawler
	c, err := r.factory.GetCrawler(platformType)
	if err != nil {
		return nil, fmt.Errorf("failed to create crawler for platform %s: %w", platformType, err)
	}

	// Initialize the crawler
	config := map[string]interface{}{
		"state_manager": r.stateManager,
		// Additional configuration options would be set here based on the platform
	}

	if err := c.Initialize(ctx, config); err != nil {
		return nil, fmt.Errorf("failed to initialize crawler for platform %s: %w", platformType, err)
	}

	// Store the initialized crawler
	r.crawlers[platformType] = c
	return c, nil
}

// ExecuteBatchJobs runs multiple crawl jobs
func (r *CrawlRunner) ExecuteBatchJobs(ctx context.Context, jobs []crawler.CrawlJob) []crawler.CrawlResult {
	results := make([]crawler.CrawlResult, 0, len(jobs))

	for _, job := range jobs {
		result, err := r.ExecuteJob(ctx, job)
		if err != nil {
			log.Error().Err(err).
				Str("platform", string(job.Target.Type)).
				Str("target_id", job.Target.ID).
				Msg("Job failed")

			// Add empty result with error
			results = append(results, crawler.CrawlResult{
				Posts:  nil,
				Errors: []error{err},
			})
		} else {
			results = append(results, result)
		}
	}

	return results
}

// Close cleans up all crawlers
func (r *CrawlRunner) Close() error {
	var lastErr error
	for platformType, c := range r.crawlers {
		if err := c.Close(); err != nil {
			log.Error().Err(err).Str("platform", string(platformType)).Msg("Error closing crawler")
			lastErr = err
		}
	}
	return lastErr
}

// GetChannelInfo retrieves information about a channel
func (r *CrawlRunner) GetChannelInfo(ctx context.Context, target crawler.CrawlTarget) (*state.ChannelInfo, error) {
	// Get the appropriate crawler
	c, err := r.getCrawler(ctx, target.Type)
	if err != nil {
		return nil, err
	}

	// Get channel info
	channelData, err := c.GetChannelInfo(ctx, target)
	if err != nil {
		return nil, err
	}

	// Convert to state.ChannelInfo
	info := &state.ChannelInfo{
		ChannelID:   target.ID,
		Name:        channelData.ChannelName,
		Description: "",  // This would be populated from channelData if available
		MemberCount: int64(channelData.ChannelEngagementData.FollowerCount),
		URL:         channelData.ChannelURL,
		Platform:    string(target.Type),
		LastCrawled: time.Now(),
	}

	return info, nil
}
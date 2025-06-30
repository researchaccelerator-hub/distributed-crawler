// Package orchestrator provides the central coordinator for distributed crawling
package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/distributed"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/rs/zerolog/log"
)

// Orchestrator manages the distributed crawl workflow
type Orchestrator struct {
	crawlID      string
	config       common.CrawlerConfig
	stateManager state.StateManagementInterface
	pubsub       *distributed.PubSubClient
	
	// Internal state
	workers      map[string]*WorkerInfo
	isRunning    bool
	mu           sync.RWMutex // Protects workers map and other shared state
	
	// Work tracking
	activeWork     map[string]*distributed.WorkItem // work_item_id -> WorkItem
	completedWork  map[string]*distributed.WorkResult
	workMu         sync.RWMutex // Protects work tracking maps
	
	// Processing state
	currentDepth   int
	maxDepthReached int
	
	// Statistics
	totalWorkItems    int
	completedItems    int
	errorItems        int
	discoveredPages   int
	startTime         time.Time
}


// WorkerInfo tracks information about active workers
type WorkerInfo struct {
	ID           string                 `json:"id"`
	Status       string                 `json:"status"` // active, idle, busy, error, offline
	LastSeen     time.Time              `json:"last_seen"`
	CurrentWork  *string                `json:"current_work,omitempty"`
	TasksTotal   int                    `json:"tasks_total"`
	TasksSuccess int                    `json:"tasks_success"`
	TasksError   int                    `json:"tasks_error"`
	Metadata     map[string]interface{} `json:"metadata"`
}

// NewOrchestrator creates a new orchestrator instance
func NewOrchestrator(crawlID string, config common.CrawlerConfig) (*Orchestrator, error) {
	log.Info().Str("crawl_id", crawlID).Msg("Creating new orchestrator instance")

	// Create state manager for this crawl
	smFactory := state.NewStateManagerFactory()
	cfg := state.Config{
		StorageRoot:      config.StorageRoot,
		CrawlID:          crawlID,
		CrawlExecutionID: common.GenerateCrawlID(),
		Platform:         config.Platform,
		DaprConfig: &state.DaprConfig{
			StateStoreName: "statestore",
			ComponentName:  "statestore",
		},
		MaxPagesConfig: &state.MaxPagesConfig{
			MaxPages: config.MaxPages,
		},
	}

	sm, err := smFactory.Create(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create state manager: %w", err)
	}

	// Create PubSub client
	pubsub, err := distributed.NewPubSubClient("pubsub", fmt.Sprintf(":%d", config.DaprPort))
	if err != nil {
		return nil, fmt.Errorf("failed to create PubSub client: %w", err)
	}

	orchestrator := &Orchestrator{
		crawlID:         crawlID,
		config:          config,
		stateManager:    sm,
		pubsub:          pubsub,
		workers:         make(map[string]*WorkerInfo),
		isRunning:       false,
		activeWork:      make(map[string]*distributed.WorkItem),
		completedWork:   make(map[string]*distributed.WorkResult),
		startTime:       time.Now(),
	}

	log.Info().Str("crawl_id", crawlID).Msg("Orchestrator instance created successfully")
	return orchestrator, nil
}

// Start begins the orchestrator workflow
func (o *Orchestrator) Start(ctx context.Context, seedURLs []string) error {
	log.Info().Str("crawl_id", o.crawlID).Int("seed_count", len(seedURLs)).Msg("Starting orchestrator")

	if o.isRunning {
		return fmt.Errorf("orchestrator is already running")
	}

	o.isRunning = true

	// Initialize the crawl with seed URLs
	err := o.stateManager.Initialize(seedURLs)
	if err != nil {
		return fmt.Errorf("failed to initialize state with seed URLs: %w", err)
	}

	// Set up PubSub subscriptions
	o.pubsub.SubscribeToResults(o.handleResultMessage)
	o.pubsub.SubscribeToStatus(o.handleStatusMessage)

	// Start PubSub server
	err = o.pubsub.StartServer(ctx)
	if err != nil {
		return fmt.Errorf("failed to start PubSub server: %w", err)
	}

	// Start background goroutines
	go o.workDistributor(ctx)
	go o.healthMonitor(ctx)

	log.Info().Str("crawl_id", o.crawlID).Msg("Orchestrator started successfully")
	return nil
}

// Stop gracefully stops the orchestrator
func (o *Orchestrator) Stop(ctx context.Context) error {
	log.Info().Str("crawl_id", o.crawlID).Msg("Stopping orchestrator")

	o.isRunning = false

	// Close PubSub client
	if err := o.pubsub.Close(); err != nil {
		log.Warn().Err(err).Msg("Error closing PubSub client")
	}

	// Close state manager
	if err := o.stateManager.Close(); err != nil {
		log.Warn().Err(err).Msg("Error closing state manager")
	}

	log.Info().Str("crawl_id", o.crawlID).Msg("Orchestrator stopped")
	return nil
}

// workDistributor manages the distribution of work items to workers
func (o *Orchestrator) workDistributor(ctx context.Context) {
	log.Info().Msg("Starting work distributor")
	
	ticker := time.NewTicker(5 * time.Second) // Check for work every 5 seconds
	defer ticker.Stop()

	for o.isRunning {
		select {
		case <-ctx.Done():
			log.Info().Msg("Work distributor stopping due to context cancellation")
			return
		case <-ticker.C:
			if err := o.distributeWork(ctx); err != nil {
				log.Error().Err(err).Msg("Error distributing work")
			}
		}
	}

	log.Info().Msg("Work distributor stopped")
}

// distributeWork checks for pending work and distributes it to available workers
func (o *Orchestrator) distributeWork(ctx context.Context) error {
	// Get the current layer of pages that need processing
	pages, err := o.stateManager.GetLayerByDepth(o.currentDepth)
	if err != nil {
		return fmt.Errorf("failed to get layer at depth %d: %w", o.currentDepth, err)
	}

	// If no pages at current depth, move to next depth or finish
	if len(pages) == 0 {
		// Check if there are any deeper layers
		maxDepth, err := o.stateManager.GetMaxDepth()
		if err != nil {
			return fmt.Errorf("failed to get max depth: %w", err)
		}

		if o.currentDepth <= maxDepth {
			o.currentDepth++
			log.Info().Int("new_depth", o.currentDepth).Msg("Moving to next depth")
			return nil
		}

		// No more work - check if all active work is completed
		o.workMu.RLock()
		activeCount := len(o.activeWork)
		o.workMu.RUnlock()

		if activeCount == 0 {
			log.Info().Msg("All work completed, crawl finished")
			o.markCrawlCompleted()
		}
		return nil
	}

	// Filter pages that need processing
	pendingPages := make([]*state.Page, 0)
	for i := range pages {
		page := &pages[i]
		if page.Status == "unfetched" || (page.Status == "error" && o.shouldRetry(page)) {
			pendingPages = append(pendingPages, page)
		}
	}

	if len(pendingPages) == 0 {
		log.Debug().Int("depth", o.currentDepth).Msg("No pending pages at current depth")
		return nil
	}

	log.Info().
		Int("depth", o.currentDepth).
		Int("total_pages", len(pages)).
		Int("pending_pages", len(pendingPages)).
		Msg("Distributing work for current layer")

	// Create and publish work items
	for _, page := range pendingPages {
		workItem := o.createWorkItem(page)
		
		// Track the work item
		o.workMu.Lock()
		o.activeWork[workItem.ID] = &workItem
		o.totalWorkItems++
		o.workMu.Unlock()

		// Update page status to processing
		page.Status = "processing"
		page.Timestamp = time.Now()
		if err := o.stateManager.UpdatePage(*page); err != nil {
			log.Error().Err(err).Str("page_url", page.URL).Msg("Failed to update page status")
		}

		// Publish work item
		err = o.pubsub.PublishWorkItem(ctx, workItem, distributed.PriorityMedium, 3600)
		if err != nil {
			log.Error().Err(err).Str("work_item_id", workItem.ID).Msg("Failed to publish work item")
			
			// Revert page status
			page.Status = "unfetched"
			o.stateManager.UpdatePage(*page)
			
			// Remove from active work
			o.workMu.Lock()
			delete(o.activeWork, workItem.ID)
			o.workMu.Unlock()
			
			continue
		}

		log.Debug().
			Str("work_item_id", workItem.ID).
			Str("url", workItem.URL).
			Int("depth", workItem.Depth).
			Msg("Published work item")
	}

	return nil
}

// createWorkItem creates a work item from a state page
func (o *Orchestrator) createWorkItem(page *state.Page) distributed.WorkItem {
	config := distributed.WorkItemConfig{
		StorageRoot:       o.config.StorageRoot,
		Concurrency:       o.config.Concurrency,
		Timeout:           o.config.Timeout,
		MinPostDate:       o.config.MinPostDate,
		PostRecency:       o.config.PostRecency,
		DateBetweenMin:    o.config.DateBetweenMin,
		DateBetweenMax:    o.config.DateBetweenMax,
		SampleSize:        o.config.SampleSize,
		MaxComments:       o.config.MaxComments,
		MaxPosts:          o.config.MaxPosts,
		MaxDepth:          o.config.MaxDepth,
		MaxPages:          o.config.MaxPages,
		MinUsers:          o.config.MinUsers,
		CrawlLabel:        o.config.CrawlLabel,
		SkipMediaDownload: o.config.SkipMediaDownload,
		YouTubeAPIKey:     o.config.YouTubeAPIKey,
	}

	return distributed.NewWorkItem(page.URL, page.Depth, page.ID, o.crawlID, o.config.Platform, config)
}

// shouldRetry determines if a failed page should be retried
func (o *Orchestrator) shouldRetry(page *state.Page) bool {
	// Simple retry logic - could be enhanced with backoff, error analysis, etc.
	maxRetries := 3
	retryCount := 0 // This would need to be tracked in page metadata
	
	return retryCount < maxRetries
}

// handleResultMessage processes work results from workers
func (o *Orchestrator) handleResultMessage(ctx context.Context, message distributed.ResultMessage) error {
	result := message.WorkResult
	
	log.Info().
		Str("work_item_id", result.WorkItemID).
		Str("worker_id", result.WorkerID).
		Str("status", result.Status).
		Int("message_count", result.MessageCount).
		Int("discovered_count", len(result.DiscoveredPages)).
		Dur("processing_time", result.ProcessingTime).
		Msg("Received work result")

	// Update work tracking
	o.workMu.Lock()
	workItem, exists := o.activeWork[result.WorkItemID]
	if exists {
		delete(o.activeWork, result.WorkItemID)
		o.completedWork[result.WorkItemID] = &result
		
		if result.Status == distributed.StatusSuccess {
			o.completedItems++
		} else {
			o.errorItems++
		}
	}
	o.workMu.Unlock()

	if !exists {
		log.Warn().Str("work_item_id", result.WorkItemID).Msg("Received result for unknown work item")
		return nil
	}

	// Update page status in state manager
	pages, err := o.stateManager.GetLayerByDepth(workItem.Depth)
	if err != nil {
		log.Error().Err(err).Int("depth", workItem.Depth).Msg("Failed to get layer for result processing")
		return err
	}

	// Find and update the page
	for i := range pages {
		if pages[i].URL == workItem.URL {
			if result.Status == distributed.StatusSuccess {
				pages[i].Status = "fetched"
			} else {
				pages[i].Status = "error" 
				pages[i].Error = result.Error
			}
			pages[i].Timestamp = result.CompletedAt
			
			if err := o.stateManager.UpdatePage(pages[i]); err != nil {
				log.Error().Err(err).Str("url", pages[i].URL).Msg("Failed to update page after result")
			}
			break
		}
	}

	// Process discovered pages
	if len(result.DiscoveredPages) > 0 {
		err := o.processDiscoveredPages(result.DiscoveredPages, workItem.Depth)
		if err != nil {
			log.Error().Err(err).Msg("Failed to process discovered pages")
		} else {
			o.discoveredPages += len(result.DiscoveredPages)
		}
	}

	return nil
}

// processDiscoveredPages adds discovered pages as a new layer
func (o *Orchestrator) processDiscoveredPages(discoveredPages []distributed.DiscoveredPage, currentDepth int) error {
	if len(discoveredPages) == 0 {
		return nil
	}

	// Convert discovered pages to state pages
	newPages := make([]state.Page, 0, len(discoveredPages))
	for _, dp := range discoveredPages {
		page := state.Page{
			URL:       dp.URL,
			Depth:     currentDepth + 1,
			Status:    "unfetched",
			Timestamp: time.Now(),
			ParentID:  dp.ParentID,
		}
		newPages = append(newPages, page)
	}

	// Add as new layer
	err := o.stateManager.AddLayer(newPages)
	if err != nil {
		return fmt.Errorf("failed to add discovered pages as new layer: %w", err)
	}

	log.Info().
		Int("count", len(newPages)).
		Int("new_depth", currentDepth+1).
		Msg("Added discovered pages as new layer")

	return nil
}

// handleStatusMessage processes worker status updates
func (o *Orchestrator) handleStatusMessage(ctx context.Context, message distributed.StatusMessage) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Update or create worker info
	worker, exists := o.workers[message.WorkerID]
	if !exists {
		worker = &WorkerInfo{
			ID: message.WorkerID,
		}
		o.workers[message.WorkerID] = worker
	}

	worker.Status = message.Status
	worker.LastSeen = message.Timestamp
	worker.TasksTotal = message.TasksProcessed
	worker.TasksSuccess = message.TasksSuccess
	worker.TasksError = message.TasksError

	if message.CurrentWork != nil {
		worker.CurrentWork = message.CurrentWork
	}

	log.Debug().
		Str("worker_id", message.WorkerID).
		Str("status", message.Status).
		Int("tasks_processed", message.TasksProcessed).
		Msg("Updated worker status")

	return nil
}

// markCrawlCompleted marks the crawl as completed
func (o *Orchestrator) markCrawlCompleted() {
	metadata := map[string]interface{}{
		"status":            "completed",
		"end_time":          time.Now(),
		"total_work_items":  o.totalWorkItems,
		"completed_items":   o.completedItems,
		"error_items":       o.errorItems,
		"discovered_pages":  o.discoveredPages,
		"max_depth_reached": o.currentDepth,
		"duration":          time.Since(o.startTime),
	}

	if err := o.stateManager.UpdateCrawlMetadata(o.crawlID, metadata); err != nil {
		log.Error().Err(err).Msg("Failed to update crawl completion metadata")
	} else {
		log.Info().Interface("stats", metadata).Msg("Crawl marked as completed")
	}
}

// healthMonitor tracks worker health and reassigns failed work
func (o *Orchestrator) healthMonitor(ctx context.Context) {
	log.Info().Msg("Starting health monitor")
	
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for o.isRunning {
		select {
		case <-ctx.Done():
			log.Info().Msg("Health monitor stopping due to context cancellation")
			return
		case <-ticker.C:
			o.checkWorkerHealth(ctx)
			o.logCrawlProgress()
		}
	}

	log.Info().Msg("Health monitor stopped")
}

// checkWorkerHealth checks for failed workers and reassigns their work
func (o *Orchestrator) checkWorkerHealth(ctx context.Context) {
	o.mu.Lock()
	defer o.mu.Unlock()

	now := time.Now()
	workerTimeout := 5 * time.Minute // Consider worker offline after 5 minutes

	failedWorkers := make([]string, 0)
	for workerID, worker := range o.workers {
		if now.Sub(worker.LastSeen) > workerTimeout && worker.Status != distributed.WorkerStatusOffline {
			log.Warn().
				Str("worker_id", workerID).
				Time("last_seen", worker.LastSeen).
				Msg("Worker appears to have failed")

			worker.Status = distributed.WorkerStatusOffline
			failedWorkers = append(failedWorkers, workerID)
		}
	}

	// Reassign work from failed workers
	if len(failedWorkers) > 0 {
		o.reassignWorkFromFailedWorkers(ctx, failedWorkers)
	}
}

// reassignWorkFromFailedWorkers reassigns active work from failed workers
func (o *Orchestrator) reassignWorkFromFailedWorkers(ctx context.Context, failedWorkers []string) {
	o.workMu.Lock()
	defer o.workMu.Unlock()

	reassignedCount := 0
	for workItemID, workItem := range o.activeWork {
		for _, failedWorkerID := range failedWorkers {
			if workItem.AssignedTo == failedWorkerID {
				// Reset work item for reassignment
				workItem.AssignedTo = ""
				workItem.RetryCount++
				workItem.CreatedAt = time.Now()

				// Republish work item
				err := o.pubsub.PublishWorkItem(ctx, *workItem, distributed.PriorityHigh, 3600)
				if err != nil {
					log.Error().
						Err(err).
						Str("work_item_id", workItemID).
						Str("failed_worker", failedWorkerID).
						Msg("Failed to reassign work item")
				} else {
					reassignedCount++
					log.Info().
						Str("work_item_id", workItemID).
						Str("failed_worker", failedWorkerID).
						Int("retry_count", workItem.RetryCount).
						Msg("Reassigned work item from failed worker")
				}
			}
		}
	}

	if reassignedCount > 0 {
		log.Info().
			Int("reassigned_count", reassignedCount).
			Int("failed_worker_count", len(failedWorkers)).
			Msg("Reassigned work items from failed workers")
	}
}

// logCrawlProgress logs current crawl progress
func (o *Orchestrator) logCrawlProgress() {
	o.workMu.RLock()
	activeCount := len(o.activeWork)
	completedCount := o.completedItems
	errorCount := o.errorItems
	totalCount := o.totalWorkItems
	o.workMu.RUnlock()

	o.mu.RLock()
	workerCount := len(o.workers)
	activeWorkers := 0
	for _, worker := range o.workers {
		if worker.Status == distributed.WorkerStatusActive || 
		   worker.Status == distributed.WorkerStatusBusy || 
		   worker.Status == distributed.WorkerStatusIdle {
			activeWorkers++
		}
	}
	o.mu.RUnlock()

	log.Info().
		Int("current_depth", o.currentDepth).
		Int("active_work", activeCount).
		Int("completed_work", completedCount).
		Int("error_work", errorCount).
		Int("total_work", totalCount).
		Int("total_workers", workerCount).
		Int("active_workers", activeWorkers).
		Int("discovered_pages", o.discoveredPages).
		Dur("uptime", time.Since(o.startTime)).
		Msg("Crawl progress status")
}

// GetStatus returns the current status of the orchestrator
func (o *Orchestrator) GetStatus() map[string]interface{} {
	o.mu.RLock()
	workerCount := len(o.workers)
	workers := make(map[string]*WorkerInfo)
	for k, v := range o.workers {
		workers[k] = v
	}
	o.mu.RUnlock()

	o.workMu.RLock()
	activeWork := len(o.activeWork)
	completedWork := len(o.completedWork)
	totalWork := o.totalWorkItems
	completedItems := o.completedItems
	errorItems := o.errorItems
	discoveredPages := o.discoveredPages
	o.workMu.RUnlock()

	return map[string]interface{}{
		"crawl_id":          o.crawlID,
		"is_running":        o.isRunning,
		"platform":          o.config.Platform,
		"current_depth":     o.currentDepth,
		"max_depth_reached": o.maxDepthReached,
		"worker_count":      workerCount,
		"workers":           workers,
		"work_stats": map[string]interface{}{
			"active_work":      activeWork,
			"completed_work":   completedWork,
			"total_work":       totalWork,
			"completed_items":  completedItems,
			"error_items":      errorItems,
			"discovered_pages": discoveredPages,
		},
		"uptime":    time.Since(o.startTime),
		"start_time": o.startTime,
	}
}
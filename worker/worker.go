// Package worker provides the distributed crawl worker implementation
package worker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/crawl"
	"github.com/researchaccelerator-hub/telegram-scraper/distributed"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/rs/zerolog/log"
)

// Worker represents a distributed crawl worker instance
type Worker struct {
	ID           string
	config       common.CrawlerConfig
	stateManager state.StateManagementInterface
	pubsub       *distributed.PubSubClient
	processor    *CrawlProcessor
	isRunning    bool

	// Work tracking
	currentWork   *distributed.WorkItem
	workStartTime time.Time

	// Statistics
	tasksProcessed int
	tasksSuccess   int
	tasksError     int
	startTime      time.Time
}

// CrawlProcessor handles the actual crawling work
type CrawlProcessor struct {
	platform string
	config   common.CrawlerConfig
	workerID string
}

// NewWorker creates a new worker instance
func NewWorker(workerID string, config common.CrawlerConfig) (*Worker, error) {
	log.Info().Str("worker_id", workerID).Msg("Creating new worker instance")

	if workerID == "" {
		return nil, fmt.Errorf("worker ID cannot be empty")
	}

	// Create state manager for this worker
	smFactory := state.NewStateManagerFactory()
	cfg := state.Config{
		StorageRoot: config.StorageRoot,
		Platform:    config.Platform,
		DaprConfig: &state.DaprConfig{
			StateStoreName: "statestore",
			ComponentName:  "statestore",
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

	// Create crawl processor
	processor := &CrawlProcessor{
		platform: config.Platform,
		config:   config,
		workerID: workerID,
	}

	worker := &Worker{
		ID:           workerID,
		config:       config,
		stateManager: sm,
		pubsub:       pubsub,
		processor:    processor,
		isRunning:    false,
		startTime:    time.Now(),
	}

	log.Info().Str("worker_id", workerID).Str("platform", config.Platform).Msg("Worker instance created successfully")
	return worker, nil
}

// Start begins the worker's operation
func (w *Worker) Start(ctx context.Context) error {
	log.Info().Str("worker_id", w.ID).Msg("Starting worker")

	if w.isRunning {
		return fmt.Errorf("worker is already running")
	}

	w.isRunning = true

	// Set up PubSub subscriptions
	w.pubsub.SubscribeToWorkQueue(w.handleWorkMessage)

	// Start PubSub server
	err := w.pubsub.StartServer(ctx)
	if err != nil {
		return fmt.Errorf("failed to start PubSub server: %w", err)
	}

	// Initialize connection pool for Telegram crawling
	if w.config.Platform == "telegram" {
		poolSize := w.config.Concurrency
		if poolSize < 1 {
			poolSize = 1
		}

		// Initialize the connection pool for this worker
		crawl.InitConnectionPool(poolSize, w.config.StorageRoot, w.config)
	}

	// Start background goroutines
	go w.heartbeatSender(ctx)

	// Send initial startup status
	w.sendStatusUpdate(distributed.MessageTypeWorkerStarted, distributed.WorkerStatusActive)

	log.Info().Str("worker_id", w.ID).Msg("Worker started successfully")
	return nil
}

// Stop gracefully stops the worker
func (w *Worker) Stop(ctx context.Context) error {
	log.Info().Str("worker_id", w.ID).Msg("Stopping worker")

	w.isRunning = false

	// Send final status update
	w.sendStatusUpdate(distributed.MessageTypeWorkerStopping, distributed.WorkerStatusOffline)

	// Close connection pool if initialized
	if w.config.Platform == "telegram" {
		crawl.CloseConnectionPool()
	}

	// Close PubSub client
	if err := w.pubsub.Close(); err != nil {
		log.Warn().Err(err).Msg("Error closing PubSub client")
	}

	// Close state manager
	if err := w.stateManager.Close(); err != nil {
		log.Warn().Err(err).Msg("Error closing state manager")
	}

	log.Info().Str("worker_id", w.ID).Msg("Worker stopped")
	return nil
}

// handleWorkMessage processes work queue messages
func (w *Worker) handleWorkMessage(ctx context.Context, message distributed.WorkQueueMessage) error {
	if message.MessageType != distributed.MessageTypeWorkItem {
		log.Debug().Str("message_type", message.MessageType).Msg("Ignoring non-work message")
		return nil
	}

	workItem := message.WorkItem

	log.Info().
		Str("work_item_id", workItem.ID).
		Str("url", workItem.URL).
		Int("depth", workItem.Depth).
		Str("platform", workItem.Platform).
		Msg("Received work item")

	// Set current work and mark as busy
	w.currentWork = &workItem
	w.workStartTime = time.Now()
	w.sendStatusUpdate(distributed.MessageTypeHeartbeat, distributed.WorkerStatusBusy)

	// Process the work item
	result, err := w.ProcessWorkItem(ctx, workItem)
	if err != nil {
		log.Error().
			Err(err).
			Str("work_item_id", workItem.ID).
			Msg("Failed to process work item")

		// Create error result
		result = &distributed.WorkResult{
			WorkItemID:     workItem.ID,
			WorkerID:       w.ID,
			Status:         distributed.StatusError,
			ProcessedURL:   workItem.URL,
			Error:          err.Error(),
			ProcessingTime: time.Since(w.workStartTime),
			CompletedAt:    time.Now(),
			Metadata: map[string]interface{}{
				"error_type": "processing_error",
			},
		}

		w.tasksError++
	} else {
		w.tasksSuccess++
	}

	w.tasksProcessed++

	// Send result back to orchestrator
	err = w.pubsub.PublishResult(ctx, *result, result.DiscoveredPages)
	if err != nil {
		log.Error().Err(err).Str("work_item_id", workItem.ID).Msg("Failed to publish result")
		return err
	}

	// Clear current work and mark as idle
	w.currentWork = nil
	w.sendStatusUpdate(distributed.MessageTypeHeartbeat, distributed.WorkerStatusIdle)

	log.Info().
		Str("work_item_id", workItem.ID).
		Str("status", result.Status).
		Dur("processing_time", result.ProcessingTime).
		Msg("Work item processed and result sent")

	return nil
}

// heartbeatSender sends periodic status updates to the orchestrator
func (w *Worker) heartbeatSender(ctx context.Context) {
	log.Info().Str("worker_id", w.ID).Msg("Starting heartbeat sender")

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for w.isRunning {
		select {
		case <-ctx.Done():
			log.Info().Str("worker_id", w.ID).Msg("Heartbeat sender stopping due to context cancellation")
			return
		case <-ticker.C:
			status := w.determineStatus()
			w.sendStatusUpdate(distributed.MessageTypeHeartbeat, status)
		}
	}

	log.Info().Str("worker_id", w.ID).Msg("Heartbeat sender stopped")
}

// sendStatusUpdate sends a status message to the orchestrator
func (w *Worker) sendStatusUpdate(messageType, status string) {
	var currentWorkID *string
	if w.currentWork != nil {
		currentWorkID = &w.currentWork.ID
	}

	uptime := time.Since(w.startTime)

	statusMsg := distributed.NewStatusMessage(
		w.ID,
		messageType,
		status,
		w.tasksProcessed,
		w.tasksSuccess,
		w.tasksError,
		uptime,
	)

	statusMsg.CurrentWork = currentWorkID

	err := w.pubsub.PublishStatus(context.Background(), statusMsg)
	if err != nil {
		log.Error().
			Err(err).
			Str("message_type", messageType).
			Msg("Failed to send status update")
	} else {
		log.Debug().
			Str("worker_id", w.ID).
			Str("message_type", messageType).
			Str("status", status).
			Int("tasks_processed", w.tasksProcessed).
			Msg("Status update sent")
	}
}

// determineStatus determines the current worker status
func (w *Worker) determineStatus() string {
	if !w.isRunning {
		return "offline"
	}
	// For Phase 1, always return idle
	// In Phase 2, this will check current work state
	return "idle"
}

// ProcessWorkItem processes a single work item using existing crawl logic
func (w *Worker) ProcessWorkItem(ctx context.Context, item distributed.WorkItem) (*distributed.WorkResult, error) {
	startTime := time.Now()

	log.Info().
		Str("worker_id", w.ID).
		Str("work_item_id", item.ID).
		Str("url", item.URL).
		Str("platform", item.Platform).
		Msg("Processing work item")

	// Create a state page for the crawl
	page := &state.Page{
		URL:       item.URL,
		Depth:     item.Depth,
		Status:    "processing",
		Timestamp: time.Now(),
		ParentID:  item.ParentID,
	}

	var discoveredChannels []*state.Page
	var messageCount int
	var err error

	// Process based on platform
	switch item.Platform {
	case "telegram":
		discoveredChannels, err = w.processTelegramChannel(ctx, page, item)
	case "youtube":
		discoveredChannels, err = w.processYouTubeChannel(ctx, page, item)
	default:
		err = fmt.Errorf("unsupported platform: %s", item.Platform)
	}

	processingTime := time.Since(startTime)

	// Create result
	result := &distributed.WorkResult{
		WorkItemID:     item.ID,
		WorkerID:       w.ID,
		ProcessedURL:   item.URL,
		MessageCount:   messageCount,
		ProcessingTime: processingTime,
		CompletedAt:    time.Now(),
		Metadata: map[string]interface{}{
			"platform": item.Platform,
			"depth":    item.Depth,
		},
	}

	if err != nil {
		result.Status = distributed.StatusError
		result.Error = err.Error()
		result.RetryRecommended = w.shouldRetryError(err)
	} else {
		result.Status = distributed.StatusSuccess

		// Convert discovered channels to distributed format
		if len(discoveredChannels) > 0 {
			result.DiscoveredPages = make([]distributed.DiscoveredPage, 0, len(discoveredChannels))
			for _, channel := range discoveredChannels {
				result.DiscoveredPages = append(result.DiscoveredPages, distributed.DiscoveredPage{
					URL:      channel.URL,
					ParentID: channel.ParentID,
					Depth:    channel.Depth,
					Platform: item.Platform,
				})
			}
		}
	}

	log.Info().
		Str("worker_id", w.ID).
		Str("work_item_id", item.ID).
		Str("status", result.Status).
		Int("discovered_count", len(result.DiscoveredPages)).
		Dur("processing_time", processingTime).
		Msg("Work item processing completed")

	return result, nil
}

// processTelegramChannel processes a Telegram channel using existing crawl logic
func (w *Worker) processTelegramChannel(ctx context.Context, page *state.Page, item distributed.WorkItem) ([]*state.Page, error) {
	// Convert work item config to crawler config
	crawlCfg := w.workItemConfigToCrawlerConfig(item.Config)

	// Use the existing connection pool for processing
	if crawl.IsConnectionPoolInitialized() {
		log.Debug().Str("url", page.URL).Msg("Using connection pool for Telegram processing")
		return crawl.RunForChannelWithPool(ctx, page, item.Config.StorageRoot, w.stateManager, crawlCfg)
	} else {
		// Fallback to creating a single connection
		log.Debug().Str("url", page.URL).Msg("Creating single connection for Telegram processing")
		connect, err := crawl.Connect(item.Config.StorageRoot, crawlCfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create Telegram connection: %w", err)
		}
		return crawl.RunForChannel(connect, page, item.Config.StorageRoot, w.stateManager, crawlCfg)
	}
}

// processYouTubeChannel processes a YouTube channel (placeholder - would need YouTube client integration)
func (w *Worker) processYouTubeChannel(ctx context.Context, page *state.Page, item distributed.WorkItem) ([]*state.Page, error) {
	// For Phase 2, this would integrate with YouTube crawling logic similar to standalone mode
	// For now, return an error to indicate it's not implemented in distributed mode yet
	return nil, fmt.Errorf("YouTube processing in distributed mode not yet implemented")
}

// workItemConfigToCrawlerConfig converts distributed WorkItemConfig to common.CrawlerConfig
func (w *Worker) workItemConfigToCrawlerConfig(config distributed.WorkItemConfig) common.CrawlerConfig {
	return common.CrawlerConfig{
		StorageRoot:       config.StorageRoot,
		Concurrency:       config.Concurrency,
		Timeout:           config.Timeout,
		Platform:          w.config.Platform,
		MinPostDate:       config.MinPostDate,
		PostRecency:       config.PostRecency,
		DateBetweenMin:    config.DateBetweenMin,
		DateBetweenMax:    config.DateBetweenMax,
		SampleSize:        config.SampleSize,
		MaxComments:       config.MaxComments,
		MaxPosts:          config.MaxPosts,
		MaxDepth:          config.MaxDepth,
		MaxPages:          config.MaxPages,
		MinUsers:          config.MinUsers,
		CrawlLabel:        config.CrawlLabel,
		SkipMediaDownload: config.SkipMediaDownload,
		YouTubeAPIKey:     config.YouTubeAPIKey,
	}
}

// shouldRetryError determines if an error should trigger a retry
func (w *Worker) shouldRetryError(err error) bool {
	// Simple retry logic - could be enhanced based on error types
	errorStr := err.Error()

	// Don't retry certain types of errors
	if strings.Contains(errorStr, "not found") ||
		strings.Contains(errorStr, "access denied") ||
		strings.Contains(errorStr, "forbidden") {
		return false
	}

	// Retry network-related errors
	if strings.Contains(errorStr, "connection") ||
		strings.Contains(errorStr, "timeout") ||
		strings.Contains(errorStr, "temporary") {
		return true
	}

	// Default to retry for unknown errors
	return true
}

// GetStatus returns the current status of the worker
func (w *Worker) GetStatus() map[string]interface{} {
	uptime := time.Since(w.startTime)

	return map[string]interface{}{
		"worker_id":       w.ID,
		"is_running":      w.isRunning,
		"platform":        w.config.Platform,
		"tasks_processed": w.tasksProcessed,
		"tasks_success":   w.tasksSuccess,
		"tasks_error":     w.tasksError,
		"uptime_seconds":  uptime.Seconds(),
		"start_time":      w.startTime,
	}
}

// GetProcessor returns the crawl processor for direct access if needed
func (w *Worker) GetProcessor() *CrawlProcessor {
	return w.processor
}

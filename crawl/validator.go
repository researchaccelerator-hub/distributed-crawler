package crawl

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/researchaccelerator-hub/telegram-scraper/telegramhelper"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

// edgeOutcomeKind classifies the result of validateSingleEdge so the caller
// can track consecutive blocked responses without inspecting the update status.
type edgeOutcomeKind int

const (
	outcomeDefinitive edgeOutcomeKind = iota // valid/invalid/not_channel — act on it
	outcomeTransient                         // network error — retry later
	outcomeBlocked                           // IP block or soft-block — pause validation
)

const (
	edgePollInterval            = 2 * time.Second
	walkbackPollInterval        = 3 * time.Second
	staleBatchRecoveryInterval  = 5 * time.Minute
	staleBatchRecoveryThreshold = 10 * time.Minute

	// Blocked-state thresholds.
	blockedThreshold = 5              // consecutive ErrBlocked results before entering blocked state
	probeInterval    = 5 * time.Minute
	probeChannel     = "telegram"     // well-known canary channel for probe requests

	// Dynamic scaler defaults.
	scalerPollInterval    = 30 * time.Second
	edgesPerWorker        = 100 // queue-depth / edgesPerWorker = desired worker count
	scaleUpCooldown       = 1 * time.Minute
)

// validatorBlockedState tracks consecutive access-blocked outcomes so the
// edge-validation loop can pause HTTP requests and probe periodically.
type validatorBlockedState struct {
	active           bool
	consecutiveCount int
	lastProbeAt      time.Time
}

// ---------------------------------------------------------------------------
// sharedIdleTracker — all workers must be idle before the pod shuts down
// ---------------------------------------------------------------------------

// CrawlActiveFunc returns true if the crawl is still active (e.g. crawler
// pods are processing channels). When set on sharedIdleTracker, the idle
// timeout only fires if the crawl is no longer active.
type CrawlActiveFunc func() bool

// sharedIdleTracker coordinates idle-timeout across multiple edge-validation
// workers. The pod only shuts down when ALL workers have been starved for
// longer than the configured timeout AND the crawl is no longer active.
type sharedIdleTracker struct {
	mu              sync.Mutex
	workerCount     int
	idleWorkers     map[int]time.Time // workerID → when it went idle
	timeout         time.Duration
	crawlActiveFunc CrawlActiveFunc // nil = no crawl-active check
}

func newSharedIdleTracker(workerCount int, timeout time.Duration, crawlActiveFn CrawlActiveFunc) *sharedIdleTracker {
	return &sharedIdleTracker{
		workerCount:     workerCount,
		idleWorkers:     make(map[int]time.Time, workerCount),
		timeout:         timeout,
		crawlActiveFunc: crawlActiveFn,
	}
}

// MarkIdle records that a worker has no edges to claim.
func (t *sharedIdleTracker) MarkIdle(workerID int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.idleWorkers[workerID]; !exists {
		t.idleWorkers[workerID] = time.Now()
	}
}

// MarkActive records that a worker found edges.
func (t *sharedIdleTracker) MarkActive(workerID int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.idleWorkers, workerID)
}

// SetWorkerCount updates the expected number of workers (called by the scaler).
func (t *sharedIdleTracker) SetWorkerCount(n int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.workerCount = n
}

// ShouldShutdown returns true when ALL workers have been idle for longer than
// the configured timeout AND the crawl is no longer active (no claimed pages,
// no incomplete batches). If crawlActiveFunc is nil, only the idle timeout is
// checked.
func (t *sharedIdleTracker) ShouldShutdown() bool {
	if t.timeout <= 0 {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.idleWorkers) < t.workerCount {
		return false
	}
	for _, since := range t.idleWorkers {
		if time.Since(since) < t.timeout {
			return false
		}
	}
	// All workers have been idle past the timeout. Check if the crawl is
	// still active — if so, keep worker 0 alive to process edges when
	// they eventually arrive.
	if t.crawlActiveFunc != nil && t.crawlActiveFunc() {
		return false
	}
	return true
}

// ---------------------------------------------------------------------------
// workerHandle — manages a single edge-validation goroutine
// ---------------------------------------------------------------------------

type workerHandle struct {
	id     int
	proxy  string
	cancel context.CancelFunc
	done   chan struct{}
}

// ---------------------------------------------------------------------------
// workerManager — dynamic start/stop of edge-validation workers
// ---------------------------------------------------------------------------

// workerManager owns the set of active edge-validation workers and exposes
// methods to start and stop individual workers by proxy index. When a
// common.ProxyLifecycleManager is set, the manager creates/destroys proxy
// infrastructure (e.g. ACI containers) alongside workers.
type workerManager struct {
	mu        sync.Mutex
	workers   map[int]*workerHandle // proxyIndex → handle
	parentCtx context.Context
	sm        state.StateManagementInterface
	cfg       common.CrawlerConfig
	idle      *sharedIdleTracker
	g         *errgroup.Group // collects worker goroutine errors
	proxyLM   common.ProxyLifecycleManager // nil when using static proxy list

	// Shared config derived once at startup.
	requestRate float64
	jitterMs    int
	claimSize   int
	validateFn  ValidateFunc
}

func newWorkerManager(
	ctx context.Context,
	g *errgroup.Group,
	sm state.StateManagementInterface,
	cfg common.CrawlerConfig,
	idle *sharedIdleTracker,
	requestRate float64,
	jitterMs int,
	claimSize int,
	validateFn ValidateFunc,
	proxyLM common.ProxyLifecycleManager,
) *workerManager {
	return &workerManager{
		workers:     make(map[int]*workerHandle),
		parentCtx:   ctx,
		sm:          sm,
		cfg:         cfg,
		idle:        idle,
		g:           g,
		requestRate: requestRate,
		jitterMs:    jitterMs,
		claimSize:   claimSize,
		validateFn:  validateFn,
		proxyLM:     proxyLM,
	}
}

// StartWorker launches an edge-validation goroutine for the given proxy index.
// No-op if a worker with that index is already running.
func (wm *workerManager) StartWorker(proxyIndex int, proxyAddr string) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if _, exists := wm.workers[proxyIndex]; exists {
		return nil // already running
	}

	httpClient, err := telegramhelper.NewValidatorHTTPClientWithProxy(
		proxyAddr, wm.cfg.ProxyUser, wm.cfg.ProxyPass, 10*time.Second,
	)
	if err != nil {
		return fmt.Errorf("worker %d: failed to create HTTP client: %w", proxyIndex, err)
	}

	rateLimiter := telegramhelper.NewValidatorRateLimiter(wm.requestRate, wm.jitterMs)

	workerCtx, cancel := context.WithCancel(wm.parentCtx)
	done := make(chan struct{})

	handle := &workerHandle{
		id:     proxyIndex,
		proxy:  proxyAddr,
		cancel: cancel,
		done:   done,
	}
	wm.workers[proxyIndex] = handle

	wm.g.Go(func() error {
		defer close(done)
		err := runEdgeValidation(workerCtx, wm.sm, wm.cfg, httpClient, rateLimiter,
			wm.claimSize, wm.validateFn, proxyIndex, wm.idle)
		// Worker exiting — remove from map so the scaler can restart it if needed.
		wm.mu.Lock()
		delete(wm.workers, proxyIndex)
		wm.mu.Unlock()
		return err
	})

	log.Info().Int("worker_id", proxyIndex).Str("proxy", proxyAddr).
		Str("log_tag", "val_scaler").Msg("Started edge-validation worker")

	return nil
}

// StopWorker cancels the worker's context, waits for it to exit, and
// destroys the proxy infrastructure if a common.ProxyLifecycleManager is set.
func (wm *workerManager) StopWorker(proxyIndex int) {
	wm.mu.Lock()
	handle, exists := wm.workers[proxyIndex]
	if !exists {
		wm.mu.Unlock()
		return
	}
	// Remove from map before releasing the lock so no one else tries to stop it.
	delete(wm.workers, proxyIndex)
	wm.mu.Unlock()

	handle.cancel()
	<-handle.done

	wm.idle.MarkActive(proxyIndex) // clean up idle entry

	log.Info().Int("worker_id", proxyIndex).Str("proxy", handle.proxy).
		Str("log_tag", "val_scaler").Msg("Stopped edge-validation worker")
}

// ActiveCount returns the number of running workers.
func (wm *workerManager) ActiveCount() int {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	return len(wm.workers)
}

// ---------------------------------------------------------------------------
// RunValidationLoop — multi-proxy with dynamic scaling
// ---------------------------------------------------------------------------

// RunValidationLoop runs edge-validation workers (one per proxy) plus a
// walkback processor and a dynamic scaler. When multiple proxy addresses are
// provided, each gets its own HTTP client, rate limiter, and blocked-state
// tracker. A dynamic scaler goroutine monitors the pending_edges queue depth
// and starts/stops workers to match demand.
//
// When cfg.ProxyLifecycle is non-nil, the scaler creates/destroys proxy
// infrastructure (e.g. ACI containers) alongside workers. On exit,
// DestroyAllProxies is called for final cleanup.
func RunValidationLoop(ctx context.Context, sm state.StateManagementInterface, cfg common.CrawlerConfig) error {
	proxyLM := cfg.ProxyLifecycle
	proxyAddrs := cfg.ProxyAddrs
	if len(proxyAddrs) == 0 && cfg.ProxyAddr != "" {
		proxyAddrs = []string{cfg.ProxyAddr}
	}
	if len(proxyAddrs) == 0 {
		proxyAddrs = []string{""} // direct connection, 1 worker
	}

	// When using managed proxies, the scaler creates them on demand.
	// Only verify the first proxy up front (it's always running).
	if proxyLM == nil {
		for i, addr := range proxyAddrs {
			if err := common.VerifyOutboundIP(addr, cfg.ProxyUser, cfg.ProxyPass); err != nil {
				return fmt.Errorf("validator: proxy %d (%s) IP verification failed: %w", i, addr, err)
			}
		}
	} else {
		if err := common.VerifyOutboundIP(proxyAddrs[0], cfg.ProxyUser, cfg.ProxyPass); err != nil {
			return fmt.Errorf("validator: initial proxy IP verification failed: %w", err)
		}
	}

	requestRate := cfg.ValidatorRequestRate
	if requestRate <= 0 {
		requestRate = 6
	}
	jitterMs := cfg.ValidatorRequestJitterMs
	if jitterMs <= 0 {
		jitterMs = 200
	}
	claimSize := cfg.ValidatorClaimBatchSize
	if claimSize <= 0 {
		claimSize = 10
	}

	initialWorkers := 1

	// Build crawl-active check: the validator stays alive as long as any
	// crawler pod has claimed pages (actively processing a channel) or there
	// are incomplete batches awaiting validation.
	crawlActiveFn := func() bool {
		claimedPages, err := sm.CountClaimedPages()
		if err != nil {
			log.Warn().Str("log_tag", "val_idle").Err(err).Msg("CountClaimedPages failed, assuming crawl active")
			return true // err on the side of staying alive
		}
		incompleteBatches, err := sm.CountIncompleteBatches(cfg.CrawlID)
		if err != nil {
			log.Warn().Str("log_tag", "val_idle").Err(err).Msg("CountIncompleteBatches failed, assuming crawl active")
			return true
		}
		active := claimedPages > 0 || incompleteBatches > 0
		if active {
			log.Info().Str("log_tag", "val_idle").
				Int("claimed_pages", claimedPages).
				Int("incomplete_batches", incompleteBatches).
				Msg("Crawl still active — suppressing idle shutdown")
		}
		return active
	}

	idle := newSharedIdleTracker(initialWorkers, cfg.ValidatorIdleTimeout, crawlActiveFn)

	log.Info().
		Int("proxy_count", len(proxyAddrs)).
		Int("initial_workers", initialWorkers).
		Bool("managed_proxies", proxyLM != nil).
		Float64("request_rate_per_min_per_worker", requestRate).
		Float64("max_aggregate_rate_per_min", requestRate*float64(len(proxyAddrs))).
		Int("claim_batch_size", claimSize).
		Str("log_tag", "val_edge").Msg("Starting multi-proxy validation loop")

	g, ctx := errgroup.WithContext(ctx)

	wm := newWorkerManager(ctx, g, sm, cfg, idle, requestRate, jitterMs, claimSize, telegramhelper.ValidateChannelHTTP, proxyLM)

	// Ensure all managed proxies are cleaned up on exit.
	if proxyLM != nil {
		defer func() {
			log.Info().Str("log_tag", "val_scaler").Msg("Destroying all managed proxies on exit")
			if err := proxyLM.DestroyAllProxies(context.Background()); err != nil {
				log.Warn().Str("log_tag", "val_scaler").Err(err).Msg("Failed to destroy managed proxies on exit")
			}
		}()
	}

	// Launch initial worker (proxy index 0 is always pre-provisioned).
	if err := wm.StartWorker(0, proxyAddrs[0]); err != nil {
		return fmt.Errorf("validator: failed to start initial worker: %w", err)
	}

	// Dynamic scaler — only useful when there are multiple proxies available.
	if len(proxyAddrs) > 1 || proxyLM != nil {
		g.Go(func() error {
			return runDynamicScaler(ctx, sm, wm, idle, proxyAddrs)
		})
	}

	// Single walkback processor (DB-bound, not HTTP-bound).
	g.Go(func() error {
		return runWalkbackProcessor(ctx, sm, cfg)
	})

	return g.Wait()
}

// ---------------------------------------------------------------------------
// runDynamicScaler — adjusts worker count based on pending_edges queue depth
// ---------------------------------------------------------------------------

// runDynamicScaler periodically checks the pending_edges queue depth and
// starts additional edge-validation workers when demand increases. Workers
// are never scaled down — once a proxy is created it stays alive until the
// validator exits (idle timeout with crawl-active check). This avoids ACI
// churn over long crawls.
func runDynamicScaler(
	ctx context.Context,
	sm state.StateManagementInterface,
	wm *workerManager,
	idle *sharedIdleTracker,
	proxyAddrs []string,
) error {
	ticker := time.NewTicker(scalerPollInterval)
	defer ticker.Stop()

	maxWorkers := len(proxyAddrs)
	var lastScaleUp time.Time

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		pendingCount, err := sm.CountPendingEdges()
		if err != nil {
			log.Warn().Str("log_tag", "val_scaler").Err(err).Msg("Failed to count pending edges")
			continue
		}

		currentWorkers := wm.ActiveCount()
		desired := pendingCount / edgesPerWorker
		if desired < 1 {
			desired = 1
		}
		if desired > maxWorkers {
			desired = maxWorkers
		}

		log.Debug().Str("log_tag", "val_scaler").
			Int("pending_edges", pendingCount).
			Int("current_workers", currentWorkers).
			Int("desired_workers", desired).
			Int("max_workers", maxWorkers).
			Msg("Scaler tick")

		if desired > currentWorkers && time.Since(lastScaleUp) >= scaleUpCooldown {
			// Scale up — start workers for available proxies.
			toAdd := desired - currentWorkers
			added := 0
			for i := 0; i < maxWorkers && added < toAdd; i++ {
				addr := proxyAddrs[i]
				// Create managed proxy if needed.
				if wm.proxyLM != nil && addr == "" {
					var createErr error
					addr, createErr = wm.proxyLM.CreateProxy(ctx, i)
					if createErr != nil {
						log.Warn().Str("log_tag", "val_scaler").Err(createErr).
							Int("proxy_index", i).Msg("Failed to create managed proxy")
						continue
					}
					// Update the address list so we don't re-create next tick.
					proxyAddrs[i] = addr

					// Wait for proxy to be reachable before starting the worker.
					if tcpErr := common.CheckProxyTCP(addr, 30*time.Second); tcpErr != nil {
						log.Warn().Str("log_tag", "val_scaler").Err(tcpErr).
							Int("proxy_index", i).Str("addr", addr).Msg("Managed proxy not reachable, destroying")
						_ = wm.proxyLM.DestroyProxy(ctx, i)
						proxyAddrs[i] = ""
						continue
					}
				}
				if startErr := wm.StartWorker(i, addr); startErr != nil {
					log.Warn().Str("log_tag", "val_scaler").Err(startErr).
						Int("proxy_index", i).Msg("Failed to start worker")
				} else {
					added++
				}
			}
			if added > 0 {
				lastScaleUp = time.Now()
				idle.SetWorkerCount(wm.ActiveCount())
				log.Info().Str("log_tag", "val_scaler").
					Int("added", added).
					Int("active_workers", wm.ActiveCount()).
					Int("pending_edges", pendingCount).
					Msg("Scaled up workers")
			}
		}
	}
}

// ---------------------------------------------------------------------------
// runEdgeValidation — single worker loop (one per proxy)
// ---------------------------------------------------------------------------

// runEdgeValidation continuously claims and validates pending edges via HTTP.
// When consecutive ErrBlocked responses reach blockedThreshold the loop enters
// a blocked state: it stops claiming edges and instead probes t.me every
// probeInterval. Validation resumes once the probe succeeds.
func runEdgeValidation(
	ctx context.Context,
	sm state.StateManagementInterface,
	cfg common.CrawlerConfig,
	httpClient *http.Client,
	rateLimiter *telegramhelper.ValidatorRateLimiter,
	claimSize int,
	validateFn ValidateFunc,
	workerID int,
	idle *sharedIdleTracker,
) error {
	var blocked validatorBlockedState
	var edgeStarvedSince time.Time
	var edgeStarvedPolls int

	wlog := log.With().Int("worker_id", workerID).Logger()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// --- Blocked state: probe instead of claiming ---
		if blocked.active {
			if time.Since(blocked.lastProbeAt) < probeInterval {
				sleepCtx(ctx, edgePollInterval)
				continue
			}
			blocked.lastProbeAt = time.Now()
			_, probeErr := validateFn(probeChannel, httpClient)
			if probeErr == nil {
				wlog.Info().Str("log_tag", "val_edge").Msg("Probe succeeded, resuming validation")
				blocked.active = false
				blocked.consecutiveCount = 0
				continue
			}
			wlog.Warn().Str("log_tag", "val_edge").Err(probeErr).Msg("Probe failed, still blocked")
			sleepCtx(ctx, edgePollInterval)
			continue
		}

		// --- Normal: claim and validate edges ---
		edges, err := sm.ClaimPendingEdges(claimSize)
		if err != nil {
			wlog.Warn().Str("log_tag", "val_edge").Err(err).Msg("Failed to claim pending edges")
			sleepCtx(ctx, edgePollInterval)
			continue
		}

		if len(edges) == 0 {
			idle.MarkIdle(workerID)
			if edgeStarvedSince.IsZero() {
				edgeStarvedSince = time.Now()
			}
			edgeStarvedPolls++
			if edgeStarvedPolls%10 == 0 {
				wlog.Info().Str("log_tag", "val_edge").
					Dur("starved_for", time.Since(edgeStarvedSince).Round(time.Second)).
					Int("empty_polls", edgeStarvedPolls).
					Msg("Worker starved — no pending edges to claim")
			}
			if idle.ShouldShutdown() {
				wlog.Info().Str("log_tag", "val_edge").
					Dur("idle_timeout", cfg.ValidatorIdleTimeout).
					Dur("starved_for", time.Since(edgeStarvedSince).Round(time.Second)).
					Msg("All workers idle — timeout reached, shutting down")
				return nil
			}
			sleepCtx(ctx, edgePollInterval)
			continue
		}

		// Reset starvation tracking when edges are available
		idle.MarkActive(workerID)
		if edgeStarvedPolls > 0 {
			wlog.Info().Str("log_tag", "val_edge").
				Dur("starved_for", time.Since(edgeStarvedSince).Round(time.Second)).
				Int("empty_polls", edgeStarvedPolls).
				Int("claimed", len(edges)).
				Msg("Worker starvation ended — edges available")
			edgeStarvedSince = time.Time{}
			edgeStarvedPolls = 0
		}

		processEdgeBatch(ctx, sm, cfg, httpClient, rateLimiter, edges, validateFn, &blocked, wlog)
	}
}

// processEdgeBatch validates a claimed batch of edges and updates their status.
func processEdgeBatch(
	ctx context.Context,
	sm state.StateManagementInterface,
	cfg common.CrawlerConfig,
	httpClient *http.Client,
	rateLimiter *telegramhelper.ValidatorRateLimiter,
	edges []*state.PendingEdge,
	validateFn ValidateFunc,
	blocked *validatorBlockedState,
	wlog zerolog.Logger,
) {
	batchStart := time.Now()
	var httpCount, skippedInvalid, skippedDuplicate, validCount, invalidCount, notChannelCount, blockedCount, transientCount int

	for _, edge := range edges {
		select {
		case <-ctx.Done():
			return
		default:
		}

		update, kind := validateSingleEdge(ctx, sm, cfg, httpClient, rateLimiter, edge, validateFn)

		// Track stats by outcome
		switch update.ValidationStatus {
		case "invalid":
			if update.ValidationReason == "cached_invalid" {
				skippedInvalid++
			} else {
				invalidCount++
				httpCount++
			}
		case "duplicate":
			skippedDuplicate++
		case "valid":
			validCount++
			httpCount++
		case "not_channel":
			notChannelCount++
			httpCount++
		case "pending":
			if kind == outcomeBlocked {
				blockedCount++
			} else {
				transientCount++
			}
		}

		switch kind {
		case outcomeBlocked:
			blocked.consecutiveCount++
			wlog.Warn().Str("channel", edge.DestinationChannel).
				Int("consecutive_blocked", blocked.consecutiveCount).
				Str("log_tag", "val_edge").Msg("Access blocked, edge left pending")
			if !blocked.active && blocked.consecutiveCount >= blockedThreshold {
				blocked.active = true
				wlog.Warn().Str("log_tag", "val_edge").Int("threshold", blockedThreshold).
					Msg("Entering blocked state")
				if eventErr := sm.InsertAccessEvent("ip_blocked"); eventErr != nil {
					wlog.Warn().Str("log_tag", "val_edge").Err(eventErr).Msg("Failed to insert access event")
				}
			}
		case outcomeTransient:
			if blocked.consecutiveCount > 0 {
				blocked.consecutiveCount--
			}
		default: // outcomeDefinitive
			blocked.consecutiveCount = 0
		}

		if updateErr := sm.UpdatePendingEdge(update); updateErr != nil {
			wlog.Warn().Err(updateErr).Int("pending_id", edge.PendingID).
				Str("log_tag", "val_edge").Msg("Failed to update edge status")
		}
	}

	batchDuration := time.Since(batchStart)
	wlog.Info().Str("log_tag", "val_edge").
		Int("claimed", len(edges)).
		Int("http_validated", httpCount).
		Int("skipped_invalid", skippedInvalid).
		Int("skipped_duplicate", skippedDuplicate).
		Int("valid", validCount).
		Int("invalid", invalidCount).
		Int("not_channel", notChannelCount).
		Int("blocked", blockedCount).
		Int("transient", transientCount).
		Dur("batch_duration_ms", batchDuration).
		Msg("Edge validation batch complete")
}

// ValidateFunc is the signature for channel validation. Production code uses
// telegramhelper.ValidateChannelHTTP; tests can inject a mock.
type ValidateFunc func(username string, httpClient *http.Client) (telegramhelper.ChannelValidationResult, error)

// validateSingleEdge validates one edge and returns the update to apply along
// with the outcome kind. On ErrTransient or ErrBlocked the update status is
// "pending" so the edge is left for re-claim; no edge is permanently
// invalidated due to an access problem.
func validateSingleEdge(
	ctx context.Context,
	sm state.StateManagementInterface,
	cfg common.CrawlerConfig,
	httpClient *http.Client,
	rateLimiter *telegramhelper.ValidatorRateLimiter,
	edge *state.PendingEdge,
	validateFn ValidateFunc,
) (state.PendingEdgeUpdate, edgeOutcomeKind) {
	channel := edge.DestinationChannel

	// Check invalid channel cache (in-memory, fast)
	if sm.IsInvalidChannel(channel) {
		log.Debug().Str("log_tag", "val_edge").Str("channel", channel).Msg("Already invalid, skipping HTTP")
		return state.PendingEdgeUpdate{
			PendingID:        edge.PendingID,
			ValidationStatus: "invalid",
			ValidationReason: "cached_invalid",
		}, outcomeDefinitive
	}

	// Check if already discovered by any crawl (DB check, no INSERT)
	discovered, err := sm.IsChannelDiscovered(channel)
	if err != nil {
		log.Warn().Str("log_tag", "val_edge").Err(err).Str("channel", channel).Msg("IsChannelDiscovered check failed")
		// Fall through to HTTP validation
	} else if discovered {
		log.Debug().Str("log_tag", "val_edge").Str("channel", channel).Msg("Already discovered, skipping HTTP")
		return state.PendingEdgeUpdate{
			PendingID:        edge.PendingID,
			ValidationStatus: "duplicate",
		}, outcomeDefinitive
	}

	// Rate limit wait
	if waitErr := rateLimiter.Wait(ctx); waitErr != nil {
		// Context cancelled — return the edge as still pending
		return state.PendingEdgeUpdate{
			PendingID:        edge.PendingID,
			ValidationStatus: "pending",
		}, outcomeTransient
	}

	// HTTP validate
	result, httpErr := validateFn(channel, httpClient)
	if httpErr != nil {
		var validErr *telegramhelper.ValidationHTTPError
		if errors.As(httpErr, &validErr) && validErr.Kind == telegramhelper.ErrBlocked {
			log.Warn().Str("log_tag", "val_edge").Err(httpErr).Str("channel", channel).Msg("Access blocked, edge left pending")
			return state.PendingEdgeUpdate{
				PendingID:        edge.PendingID,
				ValidationStatus: "pending",
			}, outcomeBlocked
		}
		log.Warn().Str("log_tag", "val_edge").Err(httpErr).Str("channel", channel).Msg("Transient HTTP error, edge left pending")
		return state.PendingEdgeUpdate{
			PendingID:        edge.PendingID,
			ValidationStatus: "pending",
		}, outcomeTransient
	}

	log.Info().Str("channel", channel).Str("source_channel", edge.SourceChannel).
		Str("status", result.Status).Str("reason", result.Reason).
		Str("source_type", edge.SourceType).Str("log_tag", "val_edge").Msg("Validation result")

	// Apply side effects based on result
	switch result.Status {
	case "valid":
		// Claim first-discovery
		claimed, claimErr := sm.ClaimDiscoveredChannel(channel, edge.CrawlID, edge.SourceChannel)
		if claimErr != nil {
			log.Warn().Str("log_tag", "val_edge").Err(claimErr).Str("channel", channel).Msg("ClaimDiscoveredChannel failed")
		}
		if !claimed {
			// Another validator already claimed this channel
			return state.PendingEdgeUpdate{
				PendingID:        edge.PendingID,
				ValidationStatus: "duplicate",
			}, outcomeDefinitive
		}
		// Ensure channel exists in seed_channels for future crawl runs
		if upsertErr := sm.InsertSeedChannelIfNew(channel); upsertErr != nil {
			log.Warn().Str("log_tag", "val_edge").Err(upsertErr).Str("channel", channel).Msg("Failed to ensure seed channel")
		}
		return state.PendingEdgeUpdate{
			PendingID:        edge.PendingID,
			ValidationStatus: "valid",
		}, outcomeDefinitive

	case "not_channel":
		if invalidErr := sm.MarkChannelInvalid(channel, result.Reason); invalidErr != nil {
			log.Warn().Str("log_tag", "val_edge").Err(invalidErr).Str("channel", channel).Msg("Failed to mark channel invalid")
		}
		return state.PendingEdgeUpdate{
			PendingID:        edge.PendingID,
			ValidationStatus: "not_channel",
			ValidationReason: result.Reason,
		}, outcomeDefinitive

	case "invalid":
		if invalidErr := sm.MarkChannelInvalid(channel, result.Reason); invalidErr != nil {
			log.Warn().Str("log_tag", "val_edge").Err(invalidErr).Str("channel", channel).Msg("Failed to mark channel invalid")
		}
		return state.PendingEdgeUpdate{
			PendingID:        edge.PendingID,
			ValidationStatus: "invalid",
			ValidationReason: result.Reason,
		}, outcomeDefinitive
	}

	// Should not reach here
	return state.PendingEdgeUpdate{
		PendingID:        edge.PendingID,
		ValidationStatus: "invalid",
		ValidationReason: "unknown_status",
	}, outcomeDefinitive
}

// runWalkbackProcessor continuously checks for closed batches where all edges
// have been validated, then makes the walkback decision and writes to
// edge_records + page_buffer.
func runWalkbackProcessor(
	ctx context.Context,
	sm state.StateManagementInterface,
	cfg common.CrawlerConfig,
) error {
	staleTicker := time.NewTicker(staleBatchRecoveryInterval)
	defer staleTicker.Stop()
	var batchStarvedSince time.Time
	var batchStarvedPolls int

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-staleTicker.C:
			if n, recErr := sm.RecoverStaleBatchClaims(staleBatchRecoveryThreshold); recErr != nil {
				log.Warn().Str("log_tag", "val_walkback").Err(recErr).Msg("Failed to recover stale batch claims")
			} else if n > 0 {
				log.Info().Str("log_tag", "val_walkback").Int("recovered", n).Msg("Recovered stale batch claims")
			}
			if n, recErr := sm.RecoverStaleValidatingEdges(staleBatchRecoveryThreshold); recErr != nil {
				log.Warn().Str("log_tag", "val_walkback").Err(recErr).Msg("Failed to recover stale validating edges")
			} else if n > 0 {
				log.Info().Str("log_tag", "val_walkback").Int("recovered", n).Msg("Recovered stale validating edges")
			}
		default:
		}

		batch, edges, err := sm.ClaimWalkbackBatch()
		if err != nil {
			log.Warn().Str("log_tag", "val_walkback").Err(err).Msg("Failed to claim walkback batch")
			sleepCtx(ctx, walkbackPollInterval)
			continue
		}

		if batch == nil {
			if batchStarvedSince.IsZero() {
				batchStarvedSince = time.Now()
			}
			batchStarvedPolls++
			if batchStarvedPolls%10 == 0 {
				log.Info().Str("log_tag", "val_walkback").
					Dur("starved_for", time.Since(batchStarvedSince).Round(time.Second)).
					Int("empty_polls", batchStarvedPolls).
					Msg("Walkback starved — no closed batches to claim")
			}
			sleepCtx(ctx, walkbackPollInterval)
			continue
		}

		if batchStarvedPolls > 0 {
			log.Info().Str("log_tag", "val_walkback").
				Dur("starved_for", time.Since(batchStarvedSince).Round(time.Second)).
				Int("empty_polls", batchStarvedPolls).
				Msg("Walkback starvation ended — batch available")
			batchStarvedSince = time.Time{}
			batchStarvedPolls = 0
		}

		log.Info().Str("log_tag", "val_walkback").Str("batch_id", batch.BatchID).Str("source_channel", batch.SourceChannel).
			Int("edge_count", len(edges)).Msg("Processing batch")

		if processErr := processWalkbackBatch(ctx, sm, cfg, batch, edges); processErr != nil {
			log.Error().Err(processErr).Str("batch_id", batch.BatchID).
				Str("log_tag", "val_walkback").Msg("Failed to process batch")
			// Don't return error — continue processing other batches
		}
	}
}

// processWalkbackBatch handles the walkback decision, edge_records, page_buffer,
// stats flush, and batch completion for a single batch.
func processWalkbackBatch(
	ctx context.Context,
	sm state.StateManagementInterface,
	cfg common.CrawlerConfig,
	batch *state.PendingEdgeBatch,
	allEdges []*state.PendingEdge,
) error {
	// Collect valid first-claimed channels (not duplicate)
	var validFirstClaimed []string
	for _, e := range allEdges {
		if e.ValidationStatus == "valid" {
			validFirstClaimed = append(validFirstClaimed, e.DestinationChannel)
		}
	}

	newChannelCount := len(validFirstClaimed)
	walkback := false
	rndNum := -1

	if newChannelCount == 0 {
		walkback = true
	} else {
		rndNum = rand.IntN(100) + 1
		if cfg.WalkbackRate >= rndNum {
			walkback = true
		}
	}

	var nextURL string
	var sequenceID string

	log.Info().Int("walkback_rate", cfg.WalkbackRate).Int("random_num", rndNum).
		Bool("walkback", walkback).Int("valid_channels", newChannelCount).
		Str("source_channel", batch.SourceChannel).Str("batch_id", batch.BatchID).
		Str("log_tag", "rw_walkback").Msg("Walkback decision data (validator)")

	var pageSequenceID string
	if walkback {
		// Walkback: pick random from discovered channels, avoiding source and newly validated channels.
		exclude := make(map[string]bool, len(validFirstClaimed))
		for _, ch := range validFirstClaimed {
			exclude[ch] = true
		}
		var walkErr error
		nextURL, walkErr = pickWalkbackChannel(sm, batch.SourceChannel, exclude)
		if walkErr != nil {
			return walkErr
		}
		// The walkback edge belongs to the current chain; the next crawl starts a new chain.
		sequenceID = batch.SequenceID
		pageSequenceID = uuid.New().String()
	} else {
		// Forward: pick random from valid first-claimed channels
		idx := rand.IntN(newChannelCount)
		nextURL = validFirstClaimed[idx]
		// Remove chosen from slice for skipped edge creation
		validFirstClaimed = append(validFirstClaimed[:idx], validFirstClaimed[idx+1:]...)
		sequenceID = batch.SequenceID
		pageSequenceID = batch.SequenceID
	}

	// Build next page for page_buffer.
	// Set CrawlID from the batch so the page lands under the correct crawl
	// even when a validator is processing a batch from a different crawl.
	page := &state.Page{
		ID:         uuid.New().String(),
		ParentID:   batch.SourcePageID,
		Depth:      batch.SourceDepth + 1,
		URL:        nextURL,
		SequenceID: pageSequenceID,
		Status:     "unfetched",
		CrawlID:    batch.CrawlID,
	}

	// Write to page_buffer — unblocks the crawler
	if err := sm.AddPageToPageBuffer(page); err != nil {
		return err
	}

	// Build edge records — only from validFirstClaimed (+ the chosen one)
	var edgeRecords []*state.EdgeRecord

	// Primary edge (the one we're following)
	primary := &state.EdgeRecord{
		DestinationChannel: nextURL,
		SourceChannel:      batch.SourceChannel,
		Walkback:           walkback,
		Skipped:            false,
		DiscoveryTime:      time.Now(),
		SequenceID:         sequenceID,
		CrawlID:            batch.CrawlID,
	}
	edgeRecords = append(edgeRecords, primary)

	// Skipped edges (valid channels we didn't choose)
	for _, ch := range validFirstClaimed {
		skipped := &state.EdgeRecord{
			DestinationChannel: ch,
			SourceChannel:      batch.SourceChannel,
			Walkback:           false,
			Skipped:            true,
			DiscoveryTime:      time.Now(),
			SequenceID:         batch.SequenceID,
			CrawlID:            batch.CrawlID,
		}
		edgeRecords = append(edgeRecords, skipped)
	}

	if err := sm.SaveEdgeRecords(edgeRecords); err != nil {
		return err
	}

	// Mark batch completed first so a crash after this point leaves orphan
	// pending_edges (harmless, swept up by RecoverOrphanEdges at next startup)
	// rather than a re-claimable empty batch that would stall the crawl.
	if err := sm.CompletePendingBatch(batch.BatchID); err != nil {
		return err
	}

	// Flush stats and delete pending_edges (best-effort; orphans cleaned at startup)
	if err := sm.FlushBatchStats(batch.BatchID, batch.CrawlID, allEdges); err != nil {
		log.Warn().Str("log_tag", "val_walkback").Err(err).Str("batch_id", batch.BatchID).Msg("FlushBatchStats failed; orphan edges will be cleaned at next startup")
	}

	// Tally validation outcomes for the batch summary.
	var validCount, invalidCount, duplicateCount, pendingCount, validatingCount, unknownCount int
	for _, e := range allEdges {
		switch e.ValidationStatus {
		case "valid":
			validCount++
		case "not_channel", "invalid":
			invalidCount++
		case "duplicate":
			duplicateCount++
		case "pending":
			pendingCount++
		case "validating":
			validatingCount++
		default:
			log.Warn().Str("batch_id", batch.BatchID).Str("status", e.ValidationStatus).
				Str("channel", e.DestinationChannel).Str("log_tag", "val_walkback").Msg("Unexpected edge validation status")
			unknownCount++
		}
	}

	log.Info().Str("batch_id", batch.BatchID).Str("source_channel", batch.SourceChannel).
		Int("total_edges", len(allEdges)).Int("valid", validCount).
		Int("invalid", invalidCount).Int("duplicate", duplicateCount).
		Int("pending", pendingCount).Int("validating", validatingCount).Int("unknown", unknownCount).
		Int("edge_records", len(edgeRecords)).
		Str("next_url", nextURL).Bool("walkback", walkback).
		Str("log_tag", "val_walkback").Msg("Batch completed")

	return nil
}

// sleepCtx sleeps for the given duration or until ctx is cancelled.
func sleepCtx(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}

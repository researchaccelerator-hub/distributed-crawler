package crawl

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/researchaccelerator-hub/telegram-scraper/telegramhelper"
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
)

// validatorBlockedState tracks consecutive access-blocked outcomes so the
// edge-validation loop can pause HTTP requests and probe periodically.
type validatorBlockedState struct {
	active           bool
	consecutiveCount int
	lastProbeAt      time.Time
}

// RunValidationLoop runs two goroutines:
//   - Edge validator: claims and HTTP-validates pending edges
//   - Walkback processor: claims closed batches and makes walkback decisions
//
// Both goroutines exit when the context is cancelled.
func RunValidationLoop(ctx context.Context, sm state.StateManagementInterface, cfg common.CrawlerConfig) error {
	if err := common.VerifyOutboundIP(cfg.ProxyAddr, cfg.ProxyUser, cfg.ProxyPass); err != nil {
		return fmt.Errorf("validator: proxy IP verification failed: %w", err)
	}

	httpClient, err := telegramhelper.NewValidatorHTTPClientWithProxy(
		cfg.ProxyAddr, cfg.ProxyUser, cfg.ProxyPass, 10*time.Second,
	)
	if err != nil {
		return fmt.Errorf("validator: failed to create HTTP client: %w", err)
	}

	requestRate := cfg.ValidatorRequestRate
	if requestRate <= 0 {
		requestRate = 6 // default: 6 calls/min = 10s interval, matching SearchPublicChat pace
	}
	jitterMs := cfg.ValidatorRequestJitterMs
	if jitterMs <= 0 {
		jitterMs = 200
	}
	claimSize := cfg.ValidatorClaimBatchSize
	if claimSize <= 0 {
		claimSize = 10
	}

	rateLimiter := telegramhelper.NewValidatorRateLimiter(requestRate, jitterMs)

	log.Info().
		Float64("request_rate_per_min", requestRate).
		Int("jitter_ms", jitterMs).
		Int("claim_batch_size", claimSize).
		Msg("validator: starting validation loop")

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return runEdgeValidation(ctx, sm, cfg, httpClient, rateLimiter, claimSize, telegramhelper.ValidateChannelHTTP)
	})

	g.Go(func() error {
		return runWalkbackProcessor(ctx, sm, cfg)
	})

	return g.Wait()
}

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
) error {
	var blocked validatorBlockedState

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
				log.Info().Msg("validator-edge: probe succeeded, resuming validation")
				blocked.active = false
				blocked.consecutiveCount = 0
				continue // immediately resume claiming — no sleep needed
			}
			log.Warn().Err(probeErr).Msg("validator-edge: probe failed, still blocked")
			sleepCtx(ctx, edgePollInterval)
			continue
		}

		// --- Normal: claim and validate edges ---
		edges, err := sm.ClaimPendingEdges(claimSize)
		if err != nil {
			log.Warn().Err(err).Msg("validator-edge: failed to claim pending edges")
			sleepCtx(ctx, edgePollInterval)
			continue
		}

		if len(edges) == 0 {
			sleepCtx(ctx, edgePollInterval)
			continue
		}

		for _, edge := range edges {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			update, kind := validateSingleEdge(ctx, sm, cfg, httpClient, rateLimiter, edge, validateFn)

			switch kind {
			case outcomeBlocked:
				blocked.consecutiveCount++
				log.Warn().Str("channel", edge.DestinationChannel).
					Int("consecutive_blocked", blocked.consecutiveCount).
					Msg("validator-edge: access blocked, edge left pending")
				if !blocked.active && blocked.consecutiveCount >= blockedThreshold {
					blocked.active = true
					// Leave lastProbeAt at zero so the first probe fires
					// immediately — confirm the block rather than waiting
					// a full probeInterval before checking.
					log.Warn().Int("threshold", blockedThreshold).
						Msg("validator-edge: entering blocked state")
					if eventErr := sm.InsertAccessEvent("ip_blocked"); eventErr != nil {
						log.Warn().Err(eventErr).Msg("validator-edge: failed to insert access event")
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
				log.Warn().Err(updateErr).Int("pending_id", edge.PendingID).
					Msg("validator-edge: failed to update edge status")
			}
		}
	}
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
		log.Debug().Str("channel", channel).Msg("validator-edge: already invalid, skipping HTTP")
		return state.PendingEdgeUpdate{
			PendingID:        edge.PendingID,
			ValidationStatus: "invalid",
			ValidationReason: "cached_invalid",
		}, outcomeDefinitive
	}

	// Check if already discovered by any crawl (DB check, no INSERT)
	discovered, err := sm.IsChannelDiscovered(channel)
	if err != nil {
		log.Warn().Err(err).Str("channel", channel).Msg("validator-edge: IsChannelDiscovered check failed")
		// Fall through to HTTP validation
	} else if discovered {
		log.Debug().Str("channel", channel).Msg("validator-edge: already discovered, skipping HTTP")
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
			log.Warn().Err(httpErr).Str("channel", channel).Msg("validator-edge: access blocked, edge left pending")
			return state.PendingEdgeUpdate{
				PendingID:        edge.PendingID,
				ValidationStatus: "pending",
			}, outcomeBlocked
		}
		log.Warn().Err(httpErr).Str("channel", channel).Msg("validator-edge: transient HTTP error, edge left pending")
		return state.PendingEdgeUpdate{
			PendingID:        edge.PendingID,
			ValidationStatus: "pending",
		}, outcomeTransient
	}

	log.Info().Str("channel", channel).Str("source_channel", edge.SourceChannel).
		Str("status", result.Status).Str("reason", result.Reason).
		Str("source_type", edge.SourceType).Msg("validator-edge: validation result")

	// Apply side effects based on result
	switch result.Status {
	case "valid":
		// Claim first-discovery
		claimed, claimErr := sm.ClaimDiscoveredChannel(channel, edge.CrawlID)
		if claimErr != nil {
			log.Warn().Err(claimErr).Str("channel", channel).Msg("validator-edge: ClaimDiscoveredChannel failed")
		}
		if !claimed {
			// Another validator already claimed this channel
			return state.PendingEdgeUpdate{
				PendingID:        edge.PendingID,
				ValidationStatus: "duplicate",
			}, outcomeDefinitive
		}
		// Cache the channel so future lookups can skip SearchPublicChat
		if upsertErr := sm.UpsertSeedChannelChatID(channel, 0); upsertErr != nil {
			log.Warn().Err(upsertErr).Str("channel", channel).Msg("validator-edge: failed to cache channel")
		}
		return state.PendingEdgeUpdate{
			PendingID:        edge.PendingID,
			ValidationStatus: "valid",
		}, outcomeDefinitive

	case "not_channel":
		if invalidErr := sm.MarkChannelInvalid(channel, result.Reason); invalidErr != nil {
			log.Warn().Err(invalidErr).Str("channel", channel).Msg("validator-edge: failed to mark channel invalid")
		}
		return state.PendingEdgeUpdate{
			PendingID:        edge.PendingID,
			ValidationStatus: "not_channel",
			ValidationReason: result.Reason,
		}, outcomeDefinitive

	case "invalid":
		if invalidErr := sm.MarkChannelInvalid(channel, result.Reason); invalidErr != nil {
			log.Warn().Err(invalidErr).Str("channel", channel).Msg("validator-edge: failed to mark channel invalid")
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

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-staleTicker.C:
			if n, recErr := sm.RecoverStaleBatchClaims(staleBatchRecoveryThreshold); recErr != nil {
				log.Warn().Err(recErr).Msg("validator-walkback: failed to recover stale batch claims")
			} else if n > 0 {
				log.Info().Int("recovered", n).Msg("validator-walkback: recovered stale batch claims")
			}
			if n, recErr := sm.RecoverStaleValidatingEdges(staleBatchRecoveryThreshold); recErr != nil {
				log.Warn().Err(recErr).Msg("validator-walkback: failed to recover stale validating edges")
			} else if n > 0 {
				log.Info().Int("recovered", n).Msg("validator-walkback: recovered stale validating edges")
			}
		default:
		}

		batch, edges, err := sm.ClaimWalkbackBatch()
		if err != nil {
			log.Warn().Err(err).Msg("validator-walkback: failed to claim walkback batch")
			sleepCtx(ctx, walkbackPollInterval)
			continue
		}

		if batch == nil {
			sleepCtx(ctx, walkbackPollInterval)
			continue
		}

		log.Info().Str("batch_id", batch.BatchID).Str("source_channel", batch.SourceChannel).
			Int("edge_count", len(edges)).Msg("validator-walkback: processing batch")

		if processErr := processWalkbackBatch(ctx, sm, cfg, batch, edges); processErr != nil {
			log.Error().Err(processErr).Str("batch_id", batch.BatchID).
				Msg("validator-walkback: failed to process batch")
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
		Msg("random-walk-walkback: Walkback decision data (validator)")

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
		log.Warn().Err(err).Str("batch_id", batch.BatchID).Msg("validator-walkback: FlushBatchStats failed; orphan edges will be cleaned at next startup")
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
				Str("channel", e.DestinationChannel).Msg("validator-walkback: unexpected edge validation status")
			unknownCount++
		}
	}

	log.Info().Str("batch_id", batch.BatchID).Str("source_channel", batch.SourceChannel).
		Int("total_edges", len(allEdges)).Int("valid", validCount).
		Int("invalid", invalidCount).Int("duplicate", duplicateCount).
		Int("pending", pendingCount).Int("validating", validatingCount).Int("unknown", unknownCount).
		Int("edge_records", len(edgeRecords)).
		Str("next_url", nextURL).Bool("walkback", walkback).
		Msg("validator-walkback: batch completed")

	return nil
}

// sleepCtx sleeps for the given duration or until ctx is cancelled.
func sleepCtx(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}

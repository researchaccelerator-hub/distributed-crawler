package dapr

import (
	"testing"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/model"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// noopStateManager satisfies state.StateManagementInterface with no-op stubs.
// Embed and override only the methods relevant to each test.
type noopStateManager struct{}

func (n *noopStateManager) Initialize(_ []string) error                              { return nil }
func (n *noopStateManager) GetPage(_ string) (state.Page, error)                    { return state.Page{}, nil }
func (n *noopStateManager) UpdatePage(_ state.Page) error                           { return nil }
func (n *noopStateManager) UpdateMessage(_ string, _ int64, _ int64, _ string) error { return nil }
func (n *noopStateManager) AddLayer(_ []state.Page) error                           { return nil }
func (n *noopStateManager) GetLayerByDepth(_ int) ([]state.Page, error)             { return nil, nil }
func (n *noopStateManager) GetMaxDepth() (int, error)                               { return 0, nil }
func (n *noopStateManager) SaveState() error                                        { return nil }
func (n *noopStateManager) ExportPagesToBinding(_ string) error                     { return nil }
func (n *noopStateManager) StorePost(_ string, _ model.Post) error                  { return nil }
func (n *noopStateManager) StoreFile(_ string, _ string, _ string) (string, string, error) {
	return "", "", nil
}
func (n *noopStateManager) GetPreviousCrawls() ([]string, error)                          { return nil, nil }
func (n *noopStateManager) UpdateCrawlMetadata(_ string, _ map[string]interface{}) error  { return nil }
func (n *noopStateManager) FindIncompleteCrawl(_ string) (string, bool, error)            { return "", false, nil }
func (n *noopStateManager) HasProcessedMedia(_ string) (bool, error)                      { return false, nil }
func (n *noopStateManager) MarkMediaAsProcessed(_ string) error                           { return nil }
func (n *noopStateManager) LoadSeedChannels() error                                       { return nil }
func (n *noopStateManager) UpsertSeedChannelChatID(_ string, _ int64) error               { return nil }
func (n *noopStateManager) GetCachedChatID(_ string) (int64, bool)                        { return 0, false }
func (n *noopStateManager) IsSeedChannel(_ string) bool                                   { return false }
func (n *noopStateManager) InitializeDiscoveredChannels() error                           { return nil }
func (n *noopStateManager) InitializeRandomWalkLayer() error                              { return nil }
func (n *noopStateManager) GetRandomDiscoveredChannel() (string, error)                   { return "", nil }
func (n *noopStateManager) IsDiscoveredChannel(_ string) bool                             { return false }
func (n *noopStateManager) AddDiscoveredChannel(_ string) error                           { return nil }
func (n *noopStateManager) StoreChannelData(_ string, _ *model.ChannelData) error         { return nil }
func (n *noopStateManager) SaveEdgeRecords(_ []*state.EdgeRecord) error                   { return nil }
func (n *noopStateManager) GetPagesFromPageBuffer(_ int) ([]state.Page, error)            { return nil, nil }
func (n *noopStateManager) ExecuteDatabaseOperation(_ string, _ []any) error              { return nil }
func (n *noopStateManager) AddPageToPageBuffer(_ *state.Page) error                       { return nil }
func (n *noopStateManager) DeletePageBufferPages(_ []string, _ []string) error             { return nil }
func (n *noopStateManager) GetChannelLastCrawled(_ string) (time.Time, error)             { return time.Time{}, nil }
func (n *noopStateManager) MarkChannelCrawled(_ string, _ int64) error                    { return nil }
func (n *noopStateManager) LoadInvalidChannels() error                                    { return nil }
func (n *noopStateManager) IsInvalidChannel(_ string) bool                                { return false }
func (n *noopStateManager) MarkChannelInvalid(_ string, _ string) error                   { return nil }
func (n *noopStateManager) MarkSeedChannelInvalid(_ string) error                         { return nil }
func (n *noopStateManager) UploadCombinedFile(_ string) error                             { return nil }
func (n *noopStateManager) CreatePendingBatch(_ *state.PendingEdgeBatch) error            { return nil }
func (n *noopStateManager) InsertPendingEdge(_ *state.PendingEdge) error                  { return nil }
func (n *noopStateManager) ClosePendingBatch(_ string) error                              { return nil }
func (n *noopStateManager) ClaimPendingEdges(_ int) ([]*state.PendingEdge, error)         { return nil, nil }
func (n *noopStateManager) UpdatePendingEdge(_ state.PendingEdgeUpdate) error             { return nil }
func (n *noopStateManager) ClaimWalkbackBatch() (*state.PendingEdgeBatch, []*state.PendingEdge, error) {
	return nil, nil, nil
}
func (n *noopStateManager) CompletePendingBatch(_ string) error                           { return nil }
func (n *noopStateManager) RecoverStaleBatchClaims(_ time.Duration) (int, error)          { return 0, nil }
func (n *noopStateManager) FlushBatchStats(_ string, _ string, _ []*state.PendingEdge) error { return nil }
func (n *noopStateManager) GetRandomSeedChannel() (string, error)                         { return "", nil }
func (n *noopStateManager) ClaimDiscoveredChannel(_ string, _ string) (bool, error)       { return false, nil }
func (n *noopStateManager) IsChannelDiscovered(_ string) (bool, error)                    { return false, nil }
func (n *noopStateManager) CountIncompleteBatches(_ string) (int, error)                  { return 0, nil }
func (n *noopStateManager) InsertAccessEvent(_ string) error                              { return nil }
func (n *noopStateManager) GetEdgeRecord(_, _ string) (*state.EdgeRecord, error)          { return nil, nil }
func (n *noopStateManager) DeleteEdgeRecord(_, _ string) error                            { return nil }
func (n *noopStateManager) GetRandomSkippedEdge(_, _ string) (*state.EdgeRecord, error)  { return nil, nil }
func (n *noopStateManager) PromoteEdge(_, _ string) error                                 { return nil }
func (n *noopStateManager) Close() error                                                  { return nil }

// blockedValidatorSM always returns an empty page buffer with N incomplete batches,
// simulating a crawler that is fully blocked waiting for a crashed validator.
type blockedValidatorSM struct {
	noopStateManager
	incompleteBatches int
}

func (b *blockedValidatorSM) GetPagesFromPageBuffer(_ int) ([]state.Page, error) {
	return nil, nil
}

func (b *blockedValidatorSM) CountIncompleteBatches(_ string) (int, error) {
	return b.incompleteBatches, nil
}

func TestRunRandomWalkLayerless_CircuitBreaker_Fires(t *testing.T) {
	orig := layerlessPollInterval
	layerlessPollInterval = 10 * time.Millisecond
	defer func() { layerlessPollInterval = orig }()

	sm := &blockedValidatorSM{incompleteBatches: 3}
	cfg := common.CrawlerConfig{
		TandemCrawl:      true,
		ValidatorTimeout: 100 * time.Millisecond,
		CrawlID:          "test-crawl",
		Concurrency:      1,
	}

	err := RunRandomWalkLayerless(sm, cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "circuit breaker")
	assert.Contains(t, err.Error(), "3 incomplete batches")
}

func TestRunRandomWalkLayerless_CircuitBreaker_Disabled(t *testing.T) {
	orig := layerlessPollInterval
	layerlessPollInterval = 10 * time.Millisecond
	defer func() { layerlessPollInterval = orig }()

	sm := &blockedValidatorSM{incompleteBatches: 3}
	cfg := common.CrawlerConfig{
		TandemCrawl:      true,
		ValidatorTimeout: 0, // disabled
		MaxCrawlDuration: 150 * time.Millisecond,
		CrawlID:          "test-crawl",
		Concurrency:      1,
	}

	err := RunRandomWalkLayerless(sm, cfg)
	// Should exit cleanly via MaxCrawlDuration, not via circuit breaker error.
	assert.NoError(t, err)
}

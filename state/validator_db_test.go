package state

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	daprc "github.com/dapr/go-sdk/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// jsonRows serialises a [][]any as JSON (mimics DAPR binding response).
func jsonRows(rows [][]any) []byte {
	data, _ := json.Marshal(rows)
	return data
}

// newValidatorDSM is a convenience wrapper around newTestDSMRW for validator tests.
func newValidatorDSM(mc *mockDaprClient) *DaprStateManager {
	return newTestDSMRW(mc, defaultRWConfig())
}

// ---------------------------------------------------------------------------
// CreatePendingBatch
// ---------------------------------------------------------------------------

func TestCreatePendingBatch_SQL(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newValidatorDSM(mc)

	batch := &PendingEdgeBatch{
		BatchID:       "batch-1",
		CrawlID:       "crawl-1",
		SourceChannel: "src_chan",
		SourcePageID:  "page-1",
		SourceDepth:   0,
		SequenceID:    "seq-1",
	}

	err := dsm.CreatePendingBatch(batch)
	require.NoError(t, err)

	mc.mu.Lock()
	defer mc.mu.Unlock()
	require.Len(t, mc.bindingCalls, 1)
	assert.Equal(t, "exec", mc.bindingCalls[0].Operation)
	assert.Contains(t, mc.bindingCalls[0].Metadata["sql"], "INSERT INTO pending_edge_batches")

	var params []any
	json.Unmarshal([]byte(mc.bindingCalls[0].Metadata["params"]), &params)
	assert.Equal(t, "batch-1", params[0])
	assert.Equal(t, "crawl-1", params[1])
	assert.Equal(t, "src_chan", params[2])
	assert.Equal(t, "page-1", params[3])
	assert.Equal(t, float64(0), params[4])
	assert.Equal(t, "seq-1", params[5])
	assert.Equal(t, "open", params[6])
}

// ---------------------------------------------------------------------------
// InsertPendingEdge
// ---------------------------------------------------------------------------

func TestInsertPendingEdge_SQL(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newValidatorDSM(mc)

	edge := &PendingEdge{
		BatchID:            "batch-1",
		CrawlID:            "crawl-1",
		DestinationChannel: "dest_chan",
		SourceChannel:      "src_chan",
		SequenceID:         "seq-1",
		SourceType:         "mention",
	}

	err := dsm.InsertPendingEdge(edge)
	require.NoError(t, err)

	mc.mu.Lock()
	defer mc.mu.Unlock()
	require.Len(t, mc.bindingCalls, 1)
	assert.Contains(t, mc.bindingCalls[0].Metadata["sql"], "INSERT INTO pending_edges")

	var params []any
	json.Unmarshal([]byte(mc.bindingCalls[0].Metadata["params"]), &params)
	assert.Equal(t, "batch-1", params[0])
	assert.Equal(t, "dest_chan", params[2])
	assert.Equal(t, "mention", params[6])
	assert.Equal(t, "pending", params[7])
}

// ---------------------------------------------------------------------------
// ClosePendingBatch
// ---------------------------------------------------------------------------

func TestClosePendingBatch_SQL(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newValidatorDSM(mc)

	err := dsm.ClosePendingBatch("batch-42")
	require.NoError(t, err)

	mc.mu.Lock()
	defer mc.mu.Unlock()
	require.Len(t, mc.bindingCalls, 1)
	assert.Contains(t, mc.bindingCalls[0].Metadata["sql"], "SET status = 'closed'")

	var params []any
	json.Unmarshal([]byte(mc.bindingCalls[0].Metadata["params"]), &params)
	assert.Equal(t, "batch-42", params[0])
}

// ---------------------------------------------------------------------------
// ClaimPendingEdges
// ---------------------------------------------------------------------------

func TestClaimPendingEdges_ParsesRows(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, in *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			mc := &mockDaprClient{}
			mc.recordCall(in)
			rows := [][]any{
				{float64(1), "batch-1", "crawl-1", "chan_a", "src", "seq-1", "2026-03-16T12:00:00Z", "mention"},
				{float64(2), "batch-1", "crawl-1", "chan_b", "src", "seq-1", "2026-03-16T12:00:01Z", "url"},
			}
			return &daprc.BindingEvent{Data: jsonRows(rows)}, nil
		},
	}
	dsm := newValidatorDSM(mc)

	edges, err := dsm.ClaimPendingEdges(10)
	require.NoError(t, err)
	require.Len(t, edges, 2)

	assert.Equal(t, 1, edges[0].PendingID)
	assert.Equal(t, "chan_a", edges[0].DestinationChannel)
	assert.Equal(t, "mention", edges[0].SourceType)
	assert.Equal(t, "validating", edges[0].ValidationStatus)
	assert.Equal(t, 2, edges[1].PendingID)
	assert.Equal(t, "chan_b", edges[1].DestinationChannel)

	mc.mu.Lock()
	defer mc.mu.Unlock()
	assert.Contains(t, mc.bindingCalls[0].Metadata["sql"], "FOR UPDATE SKIP LOCKED")
}

func TestClaimPendingEdges_EmptyResult(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, in *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			return &daprc.BindingEvent{Data: jsonRows([][]any{})}, nil
		},
	}
	dsm := newValidatorDSM(mc)

	edges, err := dsm.ClaimPendingEdges(10)
	require.NoError(t, err)
	assert.Empty(t, edges)
}

func TestClaimPendingEdges_LimitInSQL(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, in *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			return &daprc.BindingEvent{Data: jsonRows([][]any{})}, nil
		},
	}
	dsm := newValidatorDSM(mc)

	dsm.ClaimPendingEdges(25)

	mc.mu.Lock()
	defer mc.mu.Unlock()
	assert.Contains(t, mc.bindingCalls[0].Metadata["sql"], "LIMIT 25")
}

// ---------------------------------------------------------------------------
// UpdatePendingEdge
// ---------------------------------------------------------------------------

func TestUpdatePendingEdge_SQL(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newValidatorDSM(mc)

	err := dsm.UpdatePendingEdge(PendingEdgeUpdate{
		PendingID:        42,
		ValidationStatus: "valid",
		ValidationReason: "",
	})
	require.NoError(t, err)

	mc.mu.Lock()
	defer mc.mu.Unlock()
	require.Len(t, mc.bindingCalls, 1)
	assert.Contains(t, mc.bindingCalls[0].Metadata["sql"], "UPDATE pending_edges SET validation_status")

	var params []any
	json.Unmarshal([]byte(mc.bindingCalls[0].Metadata["params"]), &params)
	assert.Equal(t, "valid", params[0])
	assert.Equal(t, "", params[1])
	assert.Equal(t, float64(42), params[2])
}

// ---------------------------------------------------------------------------
// ClaimWalkbackBatch
// ---------------------------------------------------------------------------

func TestClaimWalkbackBatch_FullFlow(t *testing.T) {
	callCount := 0
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, in *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			callCount++
			if callCount == 1 {
				rows := [][]any{
					{"batch-1", "crawl-1", "src_chan", "page-1", float64(0), "seq-1", float64(1)},
				}
				return &daprc.BindingEvent{Data: jsonRows(rows)}, nil
			}
			rows := [][]any{
				{float64(1), "batch-1", "crawl-1", "chan_a", "src_chan", "seq-1", "2026-03-16T12:00:00Z", "mention", "valid", ""},
				{float64(2), "batch-1", "crawl-1", "chan_b", "src_chan", "seq-1", "2026-03-16T12:00:01Z", "url", "invalid", "not_found"},
			}
			return &daprc.BindingEvent{Data: jsonRows(rows)}, nil
		},
	}
	dsm := newValidatorDSM(mc)

	batch, edges, err := dsm.ClaimWalkbackBatch()
	require.NoError(t, err)
	require.NotNil(t, batch)
	require.Len(t, edges, 2)

	assert.Equal(t, "batch-1", batch.BatchID)
	assert.Equal(t, "src_chan", batch.SourceChannel)
	assert.Equal(t, 0, batch.SourceDepth)
	assert.Equal(t, "processing", batch.Status)
	assert.Equal(t, 1, batch.AttemptCount)

	assert.Equal(t, "valid", edges[0].ValidationStatus)
	assert.Equal(t, "invalid", edges[1].ValidationStatus)
	assert.Equal(t, "not_found", edges[1].ValidationReason)

	mc.mu.Lock()
	defer mc.mu.Unlock()
	assert.Contains(t, mc.bindingCalls[0].Metadata["sql"], "FOR UPDATE SKIP LOCKED")
	assert.Contains(t, mc.bindingCalls[0].Metadata["sql"], "NOT EXISTS")
}

func TestClaimWalkbackBatch_NoneReady(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			return &daprc.BindingEvent{Data: jsonRows([][]any{})}, nil
		},
	}
	dsm := newValidatorDSM(mc)

	batch, edges, err := dsm.ClaimWalkbackBatch()
	require.NoError(t, err)
	assert.Nil(t, batch)
	assert.Nil(t, edges)
}

func TestClaimWalkbackBatch_DBErrorOnEdgeFetch(t *testing.T) {
	callCount := 0
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			callCount++
			if callCount == 1 {
				return &daprc.BindingEvent{Data: jsonRows([][]any{
					{"batch-1", "crawl-1", "src", "page-1", float64(0), "seq-1", float64(1)},
				})}, nil
			}
			return nil, fmt.Errorf("disk full")
		},
	}
	dsm := newValidatorDSM(mc)

	batch, _, err := dsm.ClaimWalkbackBatch()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "disk full")
	assert.NotNil(t, batch) // batch claimed but edge fetch failed
}

// ---------------------------------------------------------------------------
// CompletePendingBatch
// ---------------------------------------------------------------------------

func TestCompletePendingBatch_SQL(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newValidatorDSM(mc)

	err := dsm.CompletePendingBatch("batch-99")
	require.NoError(t, err)

	mc.mu.Lock()
	defer mc.mu.Unlock()
	require.Len(t, mc.bindingCalls, 1)
	assert.Contains(t, mc.bindingCalls[0].Metadata["sql"], "SET status = 'completed'")
}

// ---------------------------------------------------------------------------
// FlushBatchStats
// ---------------------------------------------------------------------------

func TestFlushBatchStats_AggregatesAndDeletes(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newValidatorDSM(mc)

	edges := []*PendingEdge{
		{SourceType: "mention", ValidationStatus: "valid"},
		{SourceType: "mention", ValidationStatus: "invalid"},
		{SourceType: "mention", ValidationStatus: "valid"},
		{SourceType: "url", ValidationStatus: "not_channel"},
		{SourceType: "", ValidationStatus: "already_discovered"},
	}

	err := dsm.FlushBatchStats("batch-1", "crawl-1", edges)
	require.NoError(t, err)

	mc.mu.Lock()
	defer mc.mu.Unlock()
	// 3 source types (mention, url, unknown) → 3 upserts + 1 delete = 4
	require.Len(t, mc.bindingCalls, 4)

	// Verify upserts contain ON CONFLICT
	for i := 0; i < 3; i++ {
		assert.Contains(t, mc.bindingCalls[i].Metadata["sql"], "ON CONFLICT")
	}
	// Last call is DELETE
	assert.Contains(t, mc.bindingCalls[3].Metadata["sql"], "DELETE FROM pending_edges")

	// Find mention aggregate and verify counts
	for i := 0; i < 3; i++ {
		var params []any
		json.Unmarshal([]byte(mc.bindingCalls[i].Metadata["params"]), &params)
		if params[1] == "mention" {
			assert.Equal(t, float64(3), params[2]) // total
			assert.Equal(t, float64(2), params[3]) // valid
			assert.Equal(t, float64(0), params[4]) // not_channel
			assert.Equal(t, float64(1), params[5]) // invalid
			assert.Equal(t, float64(0), params[6]) // already_discovered
		}
	}
}

func TestFlushBatchStats_EmptySourceTypeMappedToUnknown(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newValidatorDSM(mc)

	edges := []*PendingEdge{
		{SourceType: "", ValidationStatus: "valid"},
	}

	err := dsm.FlushBatchStats("batch-1", "crawl-1", edges)
	require.NoError(t, err)

	mc.mu.Lock()
	defer mc.mu.Unlock()
	// 1 upsert + 1 delete
	require.Len(t, mc.bindingCalls, 2)

	var params []any
	json.Unmarshal([]byte(mc.bindingCalls[0].Metadata["params"]), &params)
	assert.Equal(t, "unknown", params[1])
}

// ---------------------------------------------------------------------------
// GetRandomSeedChannel
// ---------------------------------------------------------------------------

func TestGetRandomSeedChannel_ReturnsChannel(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			return &daprc.BindingEvent{Data: jsonRows([][]any{{"random_chan"}})}, nil
		},
	}
	dsm := newValidatorDSM(mc)

	ch, err := dsm.GetRandomSeedChannel()
	require.NoError(t, err)
	assert.Equal(t, "random_chan", ch)

	mc.mu.Lock()
	defer mc.mu.Unlock()
	assert.Contains(t, mc.bindingCalls[0].Metadata["sql"], "ORDER BY RANDOM()")
}

func TestGetRandomSeedChannel_EmptyTable(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			return &daprc.BindingEvent{Data: jsonRows([][]any{})}, nil
		},
	}
	dsm := newValidatorDSM(mc)

	_, err := dsm.GetRandomSeedChannel()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no seed channels found")
}

// ---------------------------------------------------------------------------
// ClaimDiscoveredChannel
// ---------------------------------------------------------------------------

func TestClaimDiscoveredChannel_FirstClaim(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			// CTE returns COUNT(*) = 1 (we inserted)
			return &daprc.BindingEvent{Data: jsonRows([][]any{{float64(1)}})}, nil
		},
	}
	dsm := newValidatorDSM(mc)

	claimed, err := dsm.ClaimDiscoveredChannel("new_chan", "crawl-1")
	require.NoError(t, err)
	assert.True(t, claimed)

	mc.mu.Lock()
	defer mc.mu.Unlock()
	assert.Contains(t, mc.bindingCalls[0].Metadata["sql"], "ON CONFLICT")
	assert.Contains(t, mc.bindingCalls[0].Metadata["sql"], "DO NOTHING")
	assert.Contains(t, mc.bindingCalls[0].Metadata["sql"], "COUNT(*)")
}

func TestClaimDiscoveredChannel_AlreadyClaimed(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			return &daprc.BindingEvent{Data: jsonRows([][]any{{float64(0)}})}, nil
		},
	}
	dsm := newValidatorDSM(mc)

	claimed, err := dsm.ClaimDiscoveredChannel("existing_chan", "crawl-1")
	require.NoError(t, err)
	assert.False(t, claimed)
}

func TestClaimDiscoveredChannel_SQLInjectionSafe(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			return &daprc.BindingEvent{Data: jsonRows([][]any{{float64(0)}})}, nil
		},
	}
	dsm := newValidatorDSM(mc)

	_, err := dsm.ClaimDiscoveredChannel("it's_a_test", "crawl-1")
	require.NoError(t, err)

	mc.mu.Lock()
	defer mc.mu.Unlock()
	assert.Contains(t, mc.bindingCalls[0].Metadata["sql"], "it''s_a_test")
}

// ---------------------------------------------------------------------------
// IsChannelDiscovered
// ---------------------------------------------------------------------------

func TestIsChannelDiscovered_Found(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			return &daprc.BindingEvent{Data: jsonRows([][]any{{float64(1)}})}, nil
		},
	}
	dsm := newValidatorDSM(mc)

	found, err := dsm.IsChannelDiscovered("known_chan", "crawl-1")
	require.NoError(t, err)
	assert.True(t, found)
}

func TestIsChannelDiscovered_NotFound(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			return &daprc.BindingEvent{Data: jsonRows([][]any{})}, nil
		},
	}
	dsm := newValidatorDSM(mc)

	found, err := dsm.IsChannelDiscovered("unknown_chan", "crawl-1")
	require.NoError(t, err)
	assert.False(t, found)
}

// ---------------------------------------------------------------------------
// CountIncompleteBatches
// ---------------------------------------------------------------------------

func TestCountIncompleteBatches_ReturnsCount(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			return &daprc.BindingEvent{Data: jsonRows([][]any{{float64(7)}})}, nil
		},
	}
	dsm := newValidatorDSM(mc)

	count, err := dsm.CountIncompleteBatches("crawl-1")
	require.NoError(t, err)
	assert.Equal(t, 7, count)
}

func TestCountIncompleteBatches_Zero(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			return &daprc.BindingEvent{Data: jsonRows([][]any{{float64(0)}})}, nil
		},
	}
	dsm := newValidatorDSM(mc)

	count, err := dsm.CountIncompleteBatches("crawl-1")
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestCountIncompleteBatches_SQLInjectionSafe(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			return &daprc.BindingEvent{Data: jsonRows([][]any{{float64(0)}})}, nil
		},
	}
	dsm := newValidatorDSM(mc)

	_, err := dsm.CountIncompleteBatches("'; DROP TABLE pending_edge_batches; --")
	require.NoError(t, err)

	mc.mu.Lock()
	defer mc.mu.Unlock()
	assert.Contains(t, mc.bindingCalls[0].Metadata["sql"], "''")
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

func TestCreatePendingBatch_DBError(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}
	dsm := newValidatorDSM(mc)

	err := dsm.CreatePendingBatch(&PendingEdgeBatch{BatchID: "b"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")
}

func TestClaimPendingEdges_DBError(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			return nil, fmt.Errorf("timeout")
		},
	}
	dsm := newValidatorDSM(mc)

	edges, err := dsm.ClaimPendingEdges(5)
	assert.Error(t, err)
	assert.Nil(t, edges)
}

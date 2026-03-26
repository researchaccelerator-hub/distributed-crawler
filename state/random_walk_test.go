package state

// Tests covering random-walk state management methods in DaprStateManager and BaseStateManager.
//
// Coverage:
//   - GetCachedChatID / UpsertSeedChannelChatID    — in-memory cache operations
//   - IsInvalidChannel / MarkChannelInvalid         — invalid channel TTL cache
//   - GetChannelLastCrawled                         — parameterized SQL regression + result parsing
//   - MarkChannelCrawled                            — DB write + cache update
//   - AddPageToPageBuffer / GetPagesFromPageBuffer — page buffer round-trip
//   - DeletePageBufferPages                         — scoped DELETE by ID
//   - SaveEdgeRecords                               — in-memory append + DB writes with SequenceID
//   - AddDiscoveredChannel / IsDiscoveredChannel / GetRandomDiscoveredChannel — DiscoveredChannels
//   - InitializeRandomWalkLayer                     — seed page generation from discovered set

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	daprc "github.com/dapr/go-sdk/client"
)

// newTestDSMRW extends the base newTestDSM helper with the additional fields
// required by random-walk methods (chatIDCache, invalidChannelCache, urlCache,
// databaseBinding, and a configurable Config).
func newTestDSMRW(mc *mockDaprClient, cfg Config) *DaprStateManager {
	base := NewBaseStateManager(cfg)
	var c daprc.Client = mc
	return &DaprStateManager{
		BaseStateManager:      base,
		client:                &c,
		stateStoreName:        "statestore",
		storageBinding:        "telegramcrawlstorage",
		databaseBinding:       "postgres",
		mediaCache:            make(map[string]MediaCacheItem),
		mediaCacheShards:      make(map[string]*MediaCache),
		activeMediaCache:      MediaCache{Items: make(map[string]MediaCacheItem)},
		mediaCacheIndex:       MediaCacheIndex{MediaIndex: make(map[string]string)},
		maxCacheItemsPerShard: 5000,
		cacheExpirationDays:   30,
		chatIDCache:           make(map[string]int64),
		chatIDCacheMu:         sync.RWMutex{},
		invalidChannelCache:   make(map[string]time.Time),
		invalidChannelCacheMu: sync.RWMutex{},
		urlCache:              make(map[string]string),
	}
}

func defaultRWConfig() Config {
	return Config{
		StorageRoot:      "/test/root",
		CrawlID:          "crawl-rw-123",
		CrawlExecutionID: "exec-rw-456",
		SamplingMethod:   "random-walk",
	}
}

// ────────────────────────────────────────────────────────────────────────────
// GetCachedChatID / UpsertSeedChannelChatID
// ────────────────────────────────────────────────────────────────────────────

func TestGetCachedChatID_Miss(t *testing.T) {
	dsm := newTestDSMRW(&mockDaprClient{}, defaultRWConfig())
	_, ok := dsm.GetCachedChatID("unknown")
	if ok {
		t.Fatal("expected cache miss for unknown channel")
	}
}

func TestGetCachedChatID_Hit(t *testing.T) {
	dsm := newTestDSMRW(&mockDaprClient{}, defaultRWConfig())
	dsm.chatIDCacheMu.Lock()
	dsm.chatIDCache["mychan"] = 12345
	dsm.chatIDCacheMu.Unlock()

	id, ok := dsm.GetCachedChatID("mychan")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if id != 12345 {
		t.Fatalf("expected 12345, got %d", id)
	}
}

func TestUpsertSeedChannelChatID_UpdatesCache(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newTestDSMRW(mc, defaultRWConfig())

	if err := dsm.UpsertSeedChannelChatID("seedchan", 99); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	id, ok := dsm.GetCachedChatID("seedchan")
	if !ok || id != 99 {
		t.Fatalf("expected cached chat ID 99, got %d (ok=%v)", id, ok)
	}
}

func TestUpsertSeedChannelChatID_CallsDB(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newTestDSMRW(mc, defaultRWConfig())

	_ = dsm.UpsertSeedChannelChatID("dbchan", 42)

	if len(mc.bindingCalls) == 0 {
		t.Fatal("expected a DB binding call")
	}
	call := mc.bindingCalls[0]
	if !strings.Contains(call.Metadata["sql"], "seed_channels") {
		t.Fatalf("expected SQL to reference seed_channels, got: %s", call.Metadata["sql"])
	}
}

// ────────────────────────────────────────────────────────────────────────────
// IsInvalidChannel / MarkChannelInvalid
// ────────────────────────────────────────────────────────────────────────────

func TestIsInvalidChannel_NotInCache(t *testing.T) {
	dsm := newTestDSMRW(&mockDaprClient{}, defaultRWConfig())
	if dsm.IsInvalidChannel("fresh") {
		t.Fatal("expected false for unknown channel")
	}
}

func TestIsInvalidChannel_InCache_Fresh(t *testing.T) {
	dsm := newTestDSMRW(&mockDaprClient{}, defaultRWConfig())
	dsm.invalidChannelCacheMu.Lock()
	dsm.invalidChannelCache["badchan"] = time.Now()
	dsm.invalidChannelCacheMu.Unlock()

	if !dsm.IsInvalidChannel("badchan") {
		t.Fatal("expected true for recently invalidated channel")
	}
}

func TestIsInvalidChannel_InCache_Expired(t *testing.T) {
	dsm := newTestDSMRW(&mockDaprClient{}, defaultRWConfig())
	// 35 days ago — past the 30-day TTL
	dsm.invalidChannelCacheMu.Lock()
	dsm.invalidChannelCache["oldchan"] = time.Now().Add(-35 * 24 * time.Hour)
	dsm.invalidChannelCacheMu.Unlock()

	if dsm.IsInvalidChannel("oldchan") {
		t.Fatal("expected false for channel whose invalidation has expired")
	}
}

func TestMarkChannelInvalid_UpdatesCache(t *testing.T) {
	dsm := newTestDSMRW(&mockDaprClient{}, defaultRWConfig())
	if err := dsm.MarkChannelInvalid("newbad", "private"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dsm.IsInvalidChannel("newbad") {
		t.Fatal("expected channel to be invalid after marking")
	}
}

func TestMarkChannelInvalid_CallsDB(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newTestDSMRW(mc, defaultRWConfig())
	_ = dsm.MarkChannelInvalid("dbinvalid", "banned")
	if len(mc.bindingCalls) == 0 {
		t.Fatal("expected a DB binding call")
	}
	call := mc.bindingCalls[0]
	if !strings.Contains(call.Metadata["sql"], "invalid_channels") {
		t.Fatalf("expected SQL to reference invalid_channels, got: %s", call.Metadata["sql"])
	}
}

// ────────────────────────────────────────────────────────────────────────────
// GetChannelLastCrawled — SQL injection fix regression + result parsing
// ────────────────────────────────────────────────────────────────────────────

func TestGetChannelLastCrawled_ParameterizedSQL(t *testing.T) {
	// Regression: before the fix, username was interpolated directly into the SQL
	// string, enabling injection. After the fix the SQL uses $1 and params is
	// provided as a separate metadata key.
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, in *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			sql := in.Metadata["sql"]
			if !strings.Contains(sql, "$1") {
				t.Errorf("SQL injection regression: expected parameterized $1 in SQL, got: %s", sql)
			}
			if strings.Contains(sql, "' OR '1'='1") {
				t.Error("SQL injection regression: raw username value found in SQL query")
			}
			params := in.Metadata["params"]
			if params == "" {
				t.Error("expected params metadata to be set")
			}
			emptyRows, _ := json.Marshal([][]interface{}{})
			return &daprc.BindingEvent{Data: emptyRows}, nil
		},
	}
	dsm := newTestDSMRW(mc, defaultRWConfig())
	_, err := dsm.GetChannelLastCrawled("' OR '1'='1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGetChannelLastCrawled_NullResult(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			emptyRows, _ := json.Marshal([][]interface{}{})
			return &daprc.BindingEvent{Data: emptyRows}, nil
		},
	}
	dsm := newTestDSMRW(mc, defaultRWConfig())
	ts, err := dsm.GetChannelLastCrawled("nocrawl")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ts.IsZero() {
		t.Fatalf("expected zero time for channel with no crawl record, got %v", ts)
	}
}

func TestGetChannelLastCrawled_RFC3339Timestamp(t *testing.T) {
	want := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			rows := [][]interface{}{{want.Format(time.RFC3339)}}
			data, _ := json.Marshal(rows)
			return &daprc.BindingEvent{Data: data}, nil
		},
	}
	dsm := newTestDSMRW(mc, defaultRWConfig())
	got, err := dsm.GetChannelLastCrawled("chan1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got.Equal(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestGetChannelLastCrawled_Float64Unix(t *testing.T) {
	epoch := int64(1700000000)
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			rows := [][]interface{}{{float64(epoch)}}
			data, _ := json.Marshal(rows)
			return &daprc.BindingEvent{Data: data}, nil
		},
	}
	dsm := newTestDSMRW(mc, defaultRWConfig())
	got, err := dsm.GetChannelLastCrawled("chan2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Unix() != epoch {
		t.Fatalf("expected unix %d, got %d", epoch, got.Unix())
	}
}

func TestGetChannelLastCrawled_BindingError(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			return nil, errors.New("db down")
		},
	}
	dsm := newTestDSMRW(mc, defaultRWConfig())
	_, err := dsm.GetChannelLastCrawled("anychan")
	if err == nil {
		t.Fatal("expected error from binding failure")
	}
}

// ────────────────────────────────────────────────────────────────────────────
// MarkChannelCrawled
// ────────────────────────────────────────────────────────────────────────────

func TestMarkChannelCrawled_UpdatesCacheAndCallsDB(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newTestDSMRW(mc, defaultRWConfig())

	if err := dsm.MarkChannelCrawled("crawled1", 777); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	id, ok := dsm.GetCachedChatID("crawled1")
	if !ok || id != 777 {
		t.Fatalf("expected chatID 777 in cache, got %d ok=%v", id, ok)
	}
	if len(mc.bindingCalls) == 0 {
		t.Fatal("expected DB binding call")
	}
	if !strings.Contains(mc.bindingCalls[0].Metadata["sql"], "seed_channels") {
		t.Fatalf("expected SQL to reference seed_channels")
	}
}

// ────────────────────────────────────────────────────────────────────────────
// AddPageToPageBuffer
// ────────────────────────────────────────────────────────────────────────────

func TestAddPageToPageBuffer_Success(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newTestDSMRW(mc, defaultRWConfig())

	page := &Page{
		ID:         "page-001",
		ParentID:   "parent-001",
		Depth:      1,
		URL:        "https://t.me/somechan",
		SequenceID: "seq-abc",
	}

	if err := dsm.AddPageToPageBuffer(page); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mc.bindingCalls) == 0 {
		t.Fatal("expected a DB binding call")
	}
	call := mc.bindingCalls[0]
	if !strings.Contains(call.Metadata["sql"], "page_buffer") {
		t.Fatalf("expected SQL to reference page_buffer, got: %s", call.Metadata["sql"])
	}
}

func TestAddPageToPageBuffer_ParamsContainCrawlID(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newTestDSMRW(mc, defaultRWConfig())

	page := &Page{ID: "pg-1", ParentID: "p-0", Depth: 0, URL: "t.me/c", SequenceID: "s1"}
	_ = dsm.AddPageToPageBuffer(page)

	call := mc.bindingCalls[0]
	if !strings.Contains(call.Metadata["params"], "crawl-rw-123") {
		t.Fatalf("expected crawl_id in params, got: %s", call.Metadata["params"])
	}
}

func TestAddPageToPageBuffer_SequenceIDInParams(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newTestDSMRW(mc, defaultRWConfig())

	page := &Page{ID: "pg-2", ParentID: "p-0", Depth: 0, URL: "t.me/d", SequenceID: "unique-seq-xyz"}
	_ = dsm.AddPageToPageBuffer(page)

	call := mc.bindingCalls[0]
	if !strings.Contains(call.Metadata["params"], "unique-seq-xyz") {
		t.Fatalf("expected sequence_id in params, got: %s", call.Metadata["params"])
	}
}

func TestAddPageToPageBuffer_ErrorPropagated(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			return nil, errors.New("constraint violation")
		},
	}
	dsm := newTestDSMRW(mc, defaultRWConfig())
	page := &Page{ID: "p", ParentID: "pp", Depth: 0, URL: "t.me/e", SequenceID: "s"}
	err := dsm.AddPageToPageBuffer(page)
	if err == nil {
		t.Fatal("expected error to propagate from DB failure")
	}
}

// ────────────────────────────────────────────────────────────────────────────
// GetPagesFromPageBuffer
// ────────────────────────────────────────────────────────────────────────────

func TestGetPagesFromPageBuffer_EmptyResult(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			data, _ := json.Marshal([][]interface{}{})
			return &daprc.BindingEvent{Data: data}, nil
		},
	}
	dsm := newTestDSMRW(mc, defaultRWConfig())
	pages, err := dsm.GetPagesFromPageBuffer(10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pages) != 0 {
		t.Fatalf("expected empty pages, got %d", len(pages))
	}
}

func TestGetPagesFromPageBuffer_ParsesPages(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			// Columns: page_id, parent_id, depth, url, crawl_id, sequence_id
			rows := [][]interface{}{
				{"pid-1", "par-0", float64(1), "https://t.me/chan1", "crawl-rw-123", "seq-111"},
				{"pid-2", "par-0", float64(2), "https://t.me/chan2", "crawl-rw-123", nil},
			}
			data, _ := json.Marshal(rows)
			return &daprc.BindingEvent{Data: data}, nil
		},
	}
	dsm := newTestDSMRW(mc, defaultRWConfig())
	pages, err := dsm.GetPagesFromPageBuffer(10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pages) != 2 {
		t.Fatalf("expected 2 pages, got %d", len(pages))
	}

	// First page — has a sequence ID
	if pages[0].ID != "pid-1" {
		t.Errorf("page[0].ID: want pid-1, got %s", pages[0].ID)
	}
	if pages[0].Depth != 1 {
		t.Errorf("page[0].Depth: want 1, got %d", pages[0].Depth)
	}
	if pages[0].SequenceID != "seq-111" {
		t.Errorf("page[0].SequenceID: want seq-111, got %q", pages[0].SequenceID)
	}

	// Second page — nil sequence_id should yield empty string (not panic)
	if pages[1].SequenceID != "" {
		t.Errorf("page[1].SequenceID: want empty, got %q", pages[1].SequenceID)
	}
}

func TestGetPagesFromPageBuffer_QueryScopedToCrawlID(t *testing.T) {
	var capturedParams string
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, in *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			capturedParams = in.Metadata["params"]
			data, _ := json.Marshal([][]interface{}{})
			return &daprc.BindingEvent{Data: data}, nil
		},
	}
	dsm := newTestDSMRW(mc, defaultRWConfig())
	_, _ = dsm.GetPagesFromPageBuffer(10)

	// crawl_id must be passed as a parameter, not interpolated into the SQL.
	if !strings.Contains(capturedParams, "crawl-rw-123") {
		t.Fatalf("expected crawl_id in params for multi-pod isolation, got: %s", capturedParams)
	}
}

func TestGetPagesFromPageBuffer_BindingError(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			return nil, errors.New("db timeout")
		},
	}
	dsm := newTestDSMRW(mc, defaultRWConfig())
	_, err := dsm.GetPagesFromPageBuffer(10)
	if err == nil {
		t.Fatal("expected error from binding failure")
	}
}


// ────────────────────────────────────────────────────────────────────────────
// SaveEdgeRecords
// ────────────────────────────────────────────────────────────────────────────

func TestSaveEdgeRecords_StoresInMemory(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newTestDSMRW(mc, defaultRWConfig())

	edges := []*EdgeRecord{
		{SourceChannel: "src", DestinationChannel: "dst", SequenceID: "seqA"},
	}
	if err := dsm.SaveEdgeRecords(edges); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(dsm.BaseStateManager.edgeRecords) != 1 {
		t.Fatalf("expected 1 edge in memory, got %d", len(dsm.BaseStateManager.edgeRecords))
	}
}

func TestSaveEdgeRecords_SequenceIDPropagatedToDB(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newTestDSMRW(mc, defaultRWConfig())

	edges := []*EdgeRecord{
		{SourceChannel: "alpha", DestinationChannel: "beta", SequenceID: "chain-xyz"},
	}
	_ = dsm.SaveEdgeRecords(edges)

	if len(mc.bindingCalls) == 0 {
		t.Fatal("expected DB binding call")
	}
	call := mc.bindingCalls[0]
	if !strings.Contains(call.Metadata["params"], "chain-xyz") {
		t.Fatalf("expected sequence_id 'chain-xyz' in DB params, got: %s", call.Metadata["params"])
	}
}

func TestSaveEdgeRecords_CrawlIDInParams(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newTestDSMRW(mc, defaultRWConfig())

	edges := []*EdgeRecord{
		{SourceChannel: "s", DestinationChannel: "d", SequenceID: "seq1"},
	}
	_ = dsm.SaveEdgeRecords(edges)

	call := mc.bindingCalls[0]
	if !strings.Contains(call.Metadata["params"], "crawl-rw-123") {
		t.Fatalf("expected crawl_id in DB params, got: %s", call.Metadata["params"])
	}
}

func TestSaveEdgeRecords_MultipleEdges_OneCallEach(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newTestDSMRW(mc, defaultRWConfig())

	edges := []*EdgeRecord{
		{SourceChannel: "s1", DestinationChannel: "d1", SequenceID: "seq1"},
		{SourceChannel: "s2", DestinationChannel: "d2", SequenceID: "seq2"},
		{SourceChannel: "s3", DestinationChannel: "d3", SequenceID: "seq3"},
	}
	if err := dsm.SaveEdgeRecords(edges); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// One DB call per edge record
	if len(mc.bindingCalls) != 3 {
		t.Fatalf("expected 3 binding calls (one per edge), got %d", len(mc.bindingCalls))
	}
}

func TestSaveEdgeRecords_NilEdgeSkipped(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newTestDSMRW(mc, defaultRWConfig())

	edges := []*EdgeRecord{
		{SourceChannel: "valid", DestinationChannel: "dst", SequenceID: "s"},
		nil, // should be skipped without panic
	}
	if err := dsm.SaveEdgeRecords(edges); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Only 1 DB call for the non-nil edge
	if len(mc.bindingCalls) != 1 {
		t.Fatalf("expected 1 binding call, got %d", len(mc.bindingCalls))
	}
}

// ────────────────────────────────────────────────────────────────────────────
// AddDiscoveredChannel / IsDiscoveredChannel / GetRandomDiscoveredChannel
// ────────────────────────────────────────────────────────────────────────────

func TestAddDiscoveredChannel_NewChannel(t *testing.T) {
	bsm := NewBaseStateManager(defaultRWConfig())
	if err := bsm.AddDiscoveredChannel("newchan"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bsm.IsDiscoveredChannel("newchan") {
		t.Fatal("expected channel to be in discovered set")
	}
}

func TestAddDiscoveredChannel_Duplicate(t *testing.T) {
	bsm := NewBaseStateManager(defaultRWConfig())
	_ = bsm.AddDiscoveredChannel("dup")
	err := bsm.AddDiscoveredChannel("dup")
	if err == nil {
		t.Fatal("expected error when adding duplicate channel")
	}
}

func TestIsDiscoveredChannel_NotFound(t *testing.T) {
	bsm := NewBaseStateManager(defaultRWConfig())
	if bsm.IsDiscoveredChannel("absent") {
		t.Fatal("expected false for channel not in set")
	}
}

func TestGetRandomDiscoveredChannel_Empty(t *testing.T) {
	bsm := NewBaseStateManager(defaultRWConfig())
	_, err := bsm.GetRandomDiscoveredChannel()
	if err == nil {
		t.Fatal("expected error when no channels discovered")
	}
}

func TestGetRandomDiscoveredChannel_WithChannels(t *testing.T) {
	bsm := NewBaseStateManager(defaultRWConfig())
	_ = bsm.AddDiscoveredChannel("alpha")
	_ = bsm.AddDiscoveredChannel("beta")
	_ = bsm.AddDiscoveredChannel("gamma")

	ch, err := bsm.GetRandomDiscoveredChannel()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	valid := map[string]bool{"alpha": true, "beta": true, "gamma": true}
	if !valid[ch] {
		t.Fatalf("random channel %q not in discovered set", ch)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// InitializeRandomWalkLayer
// ────────────────────────────────────────────────────────────────────────────

func TestInitializeRandomWalkLayer_ZeroSeedSize(t *testing.T) {
	cfg := defaultRWConfig()
	cfg.SeedSize = 0
	mc := &mockDaprClient{}
	dsm := newTestDSMRW(mc, cfg)
	err := dsm.InitializeRandomWalkLayer()
	if err == nil {
		t.Fatal("expected error for zero seed size")
	}
}

func TestInitializeRandomWalkLayer_SeedSizeExceedsDiscovered(t *testing.T) {
	cfg := defaultRWConfig()
	cfg.SeedSize = 10
	mc := &mockDaprClient{}
	dsm := newTestDSMRW(mc, cfg)
	// Only add 3 channels
	_ = dsm.AddDiscoveredChannel("c1")
	_ = dsm.AddDiscoveredChannel("c2")
	_ = dsm.AddDiscoveredChannel("c3")

	err := dsm.InitializeRandomWalkLayer()
	if err == nil {
		t.Fatal("expected error when seed size exceeds discovered channels")
	}
}

func TestInitializeRandomWalkLayer_CreatesCorrectNumberOfPages(t *testing.T) {
	cfg := defaultRWConfig()
	cfg.SeedSize = 3
	mc := &mockDaprClient{}
	dsm := newTestDSMRW(mc, cfg)

	channels := []string{"c1", "c2", "c3", "c4", "c5"}
	for _, ch := range channels {
		_ = dsm.AddDiscoveredChannel(ch)
	}

	if err := dsm.InitializeRandomWalkLayer(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var insertCount int
	for _, call := range mc.bindingCalls {
		if strings.Contains(call.Metadata["sql"], "page_buffer") {
			insertCount++
		}
	}
	if insertCount != 3 {
		t.Fatalf("expected 3 seed pages at depth 0, got %d", insertCount)
	}
}

func TestInitializeRandomWalkLayer_PagesHaveSequenceIDs(t *testing.T) {
	cfg := defaultRWConfig()
	cfg.SeedSize = 2
	mc := &mockDaprClient{}
	dsm := newTestDSMRW(mc, cfg)

	_ = dsm.AddDiscoveredChannel("x1")
	_ = dsm.AddDiscoveredChannel("x2")

	if err := dsm.InitializeRandomWalkLayer(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var checkedPages int
	for _, call := range mc.bindingCalls {
		if !strings.Contains(call.Metadata["sql"], "page_buffer") {
			continue
		}
		var params []any
		if err := json.Unmarshal([]byte(call.Metadata["params"]), &params); err != nil {
			t.Fatalf("failed to parse params: %v", err)
		}
		// params: [page_id, parent_id, depth, url, crawl_id, sequence_id]
		seqID, _ := params[5].(string)
		if seqID == "" {
			t.Errorf("seed page has empty SequenceID; params=%v", params)
		}
		checkedPages++
	}
	if checkedPages != 2 {
		t.Fatalf("expected 2 seed pages, got %d", checkedPages)
	}
}

func TestInitializeRandomWalkLayer_NoDuplicateSeeds(t *testing.T) {
	cfg := defaultRWConfig()
	cfg.SeedSize = 3
	mc := &mockDaprClient{}
	dsm := newTestDSMRW(mc, cfg)

	// Exactly 3 channels — seed must use all 3 distinct ones
	_ = dsm.AddDiscoveredChannel("uniq1")
	_ = dsm.AddDiscoveredChannel("uniq2")
	_ = dsm.AddDiscoveredChannel("uniq3")

	if err := dsm.InitializeRandomWalkLayer(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	seen := make(map[string]bool)
	for _, call := range mc.bindingCalls {
		if !strings.Contains(call.Metadata["sql"], "page_buffer") {
			continue
		}
		var params []any
		if err := json.Unmarshal([]byte(call.Metadata["params"]), &params); err != nil {
			t.Fatalf("failed to parse params: %v", err)
		}
		// params: [page_id, parent_id, depth, url, crawl_id, sequence_id]
		url, _ := params[3].(string)
		if seen[url] {
			t.Errorf("duplicate seed URL: %s", url)
		}
		seen[url] = true
	}
}

// ────────────────────────────────────────────────────────────────────────────
// ClaimPages
// ────────────────────────────────────────────────────────────────────────────

func TestClaimPages_SQLStructure(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			data, _ := json.Marshal([][]interface{}{})
			return &daprc.BindingEvent{Data: data}, nil
		},
	}
	cfg := defaultRWConfig()
	cfg.PodName = "crawler-0"
	dsm := newTestDSMRW(mc, cfg)

	_, err := dsm.ClaimPages(3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mc.bindingCalls) == 0 {
		t.Fatal("expected a DB binding call")
	}
	sql := mc.bindingCalls[0].Metadata["sql"]
	for _, want := range []string{
		"UPDATE page_buffer SET claimed_by",
		"FOR UPDATE SKIP LOCKED",
		"RETURNING",
		"crawl-rw-123",
		"claimed_by = ''",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("SQL missing %q, got: %s", want, sql)
		}
	}
}

func TestClaimPages_PodNameInSQL(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			data, _ := json.Marshal([][]interface{}{})
			return &daprc.BindingEvent{Data: data}, nil
		},
	}
	cfg := defaultRWConfig()
	cfg.PodName = "my-pod-7"
	dsm := newTestDSMRW(mc, cfg)

	_, _ = dsm.ClaimPages(1)

	sql := mc.bindingCalls[0].Metadata["sql"]
	if !strings.Contains(sql, "my-pod-7") {
		t.Errorf("expected pod name in SQL, got: %s", sql)
	}
}

func TestClaimPages_ParsesRows(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			rows := [][]interface{}{
				{"pid-1", "par-0", float64(1), "chan1", "crawl-rw-123", "seq-111"},
				{"pid-2", "par-1", float64(2), "chan2", "crawl-rw-123", nil},
			}
			data, _ := json.Marshal(rows)
			return &daprc.BindingEvent{Data: data}, nil
		},
	}
	cfg := defaultRWConfig()
	cfg.PodName = "pod-0"
	dsm := newTestDSMRW(mc, cfg)

	pages, err := dsm.ClaimPages(5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pages) != 2 {
		t.Fatalf("expected 2 pages, got %d", len(pages))
	}
	if pages[0].ID != "pid-1" || pages[0].Depth != 1 || pages[0].SequenceID != "seq-111" {
		t.Errorf("page[0] mismatch: %+v", pages[0])
	}
	if pages[1].SequenceID != "" {
		t.Errorf("page[1].SequenceID: want empty, got %q", pages[1].SequenceID)
	}
	if pages[0].Status != "claimed" {
		t.Errorf("page[0].Status: want claimed, got %q", pages[0].Status)
	}
}

func TestClaimPages_EmptyResult(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			data, _ := json.Marshal([][]interface{}{})
			return &daprc.BindingEvent{Data: data}, nil
		},
	}
	cfg := defaultRWConfig()
	cfg.PodName = "pod-0"
	dsm := newTestDSMRW(mc, cfg)

	pages, err := dsm.ClaimPages(10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pages) != 0 {
		t.Fatalf("expected 0 pages, got %d", len(pages))
	}
}

func TestClaimPages_DBError(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			return nil, errors.New("db timeout")
		},
	}
	cfg := defaultRWConfig()
	cfg.PodName = "pod-0"
	dsm := newTestDSMRW(mc, cfg)

	_, err := dsm.ClaimPages(5)
	if err == nil {
		t.Fatal("expected error from binding failure")
	}
	if !strings.Contains(err.Error(), "claim-pages") {
		t.Errorf("expected wrapped error context, got: %v", err)
	}
}

func TestClaimPages_SQLInjectionSafety(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			data, _ := json.Marshal([][]interface{}{})
			return &daprc.BindingEvent{Data: data}, nil
		},
	}
	cfg := defaultRWConfig()
	cfg.PodName = "x'y"
	dsm := newTestDSMRW(mc, cfg)

	_, _ = dsm.ClaimPages(1)

	sql := mc.bindingCalls[0].Metadata["sql"]
	// The single quote in "x'y" must be escaped to "x''y"
	if !strings.Contains(sql, "x''y") {
		t.Errorf("expected escaped single quote in SQL, got: %s", sql)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// UnclaimPages
// ────────────────────────────────────────────────────────────────────────────

func TestUnclaimPages_SQLStructure(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newTestDSMRW(mc, defaultRWConfig())

	err := dsm.UnclaimPages([]string{"pid-1", "pid-2"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mc.bindingCalls) == 0 {
		t.Fatal("expected a DB binding call")
	}
	sql := mc.bindingCalls[0].Metadata["sql"]
	for _, want := range []string{
		"UPDATE page_buffer",
		"claimed_by = ''",
		"claimed_at = NULL",
		"crawl-rw-123",
		"'pid-1'",
		"'pid-2'",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("SQL missing %q, got: %s", want, sql)
		}
	}
}

func TestUnclaimPages_EmptyList(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newTestDSMRW(mc, defaultRWConfig())

	err := dsm.UnclaimPages([]string{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mc.bindingCalls) != 0 {
		t.Errorf("expected no DB calls for empty list, got %d", len(mc.bindingCalls))
	}
}

// ────────────────────────────────────────────────────────────────────────────
// RecoverStalePageClaims
// ────────────────────────────────────────────────────────────────────────────

func TestRecoverStalePageClaims_SQLStructure(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			data, _ := json.Marshal([][]interface{}{})
			return &daprc.BindingEvent{Data: data}, nil
		},
	}
	dsm := newTestDSMRW(mc, defaultRWConfig())

	_, err := dsm.RecoverStalePageClaims(10 * time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mc.bindingCalls) == 0 {
		t.Fatal("expected a DB binding call")
	}
	sql := mc.bindingCalls[0].Metadata["sql"]
	for _, want := range []string{
		"UPDATE page_buffer",
		"claimed_by = ''",
		"claimed_by != ''",
		"INTERVAL '10 minutes'",
		"RETURNING page_id",
		"crawl-rw-123",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("SQL missing %q, got: %s", want, sql)
		}
	}
}

func TestRecoverStalePageClaims_ReturnsCount(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			rows := [][]interface{}{
				{"pid-1"},
				{"pid-2"},
				{"pid-3"},
			}
			data, _ := json.Marshal(rows)
			return &daprc.BindingEvent{Data: data}, nil
		},
	}
	dsm := newTestDSMRW(mc, defaultRWConfig())

	count, err := dsm.RecoverStalePageClaims(10 * time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 recovered, got %d", count)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// AddPageToPageBuffer — ON CONFLICT
// ────────────────────────────────────────────────────────────────────────────

func TestAddPageToPageBuffer_OnConflict(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newTestDSMRW(mc, defaultRWConfig())

	page := &Page{ID: "pg-oc", ParentID: "p-0", Depth: 0, URL: "t.me/oc", SequenceID: "s"}
	_ = dsm.AddPageToPageBuffer(page)

	sql := mc.bindingCalls[0].Metadata["sql"]
	if !strings.Contains(sql, "ON CONFLICT (crawl_id, url) DO NOTHING") {
		t.Errorf("expected ON CONFLICT clause, got: %s", sql)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// GetPagesFromPageBuffer — excludes claimed
// ────────────────────────────────────────────────────────────────────────────

func TestGetPagesFromPageBuffer_ExcludesClaimed(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			data, _ := json.Marshal([][]interface{}{})
			return &daprc.BindingEvent{Data: data}, nil
		},
	}
	dsm := newTestDSMRW(mc, defaultRWConfig())

	_, _ = dsm.GetPagesFromPageBuffer(10)

	sql := mc.bindingCalls[0].Metadata["sql"]
	if !strings.Contains(sql, "claimed_by = ''") {
		t.Errorf("expected claimed_by filter in SELECT, got: %s", sql)
	}
}

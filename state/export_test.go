package state

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dapr/go-sdk/actor"
	actorcfg "github.com/dapr/go-sdk/actor/config"
	daprc "github.com/dapr/go-sdk/client"
	daprpb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	"google.golang.org/grpc"
)

// mockDaprClient implements daprc.Client for testing ExportPagesToBinding.
// Only GetMetadata, InvokeBinding, and GetState have meaningful implementations;
// everything else is a no-op.
type mockDaprClient struct {
	mu sync.Mutex

	getMetadataFn   func(ctx context.Context) (*daprc.GetMetadataResponse, error)
	invokeBindingFn func(ctx context.Context, in *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error)
	getStateFn      func(ctx context.Context, storeName, key string, meta map[string]string) (*daprc.StateItem, error)

	// Captured invocation data for assertions.
	bindingCalls []*daprc.InvokeBindingRequest
}

func (m *mockDaprClient) recordCall(in *daprc.InvokeBindingRequest) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bindingCalls = append(m.bindingCalls, in)
}

// --- Methods used by ExportPagesToBinding ---

func (m *mockDaprClient) GetMetadata(ctx context.Context) (*daprc.GetMetadataResponse, error) {
	if m.getMetadataFn != nil {
		return m.getMetadataFn(ctx)
	}
	return &daprc.GetMetadataResponse{
		RegisteredComponents: []*daprc.MetadataRegisteredComponents{
			{Name: "telegramcrawlstorage", Type: "bindings.localstorage"},
		},
	}, nil
}

func (m *mockDaprClient) InvokeBinding(ctx context.Context, in *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
	m.recordCall(in)
	if m.invokeBindingFn != nil {
		return m.invokeBindingFn(ctx, in)
	}
	return &daprc.BindingEvent{}, nil
}

func (m *mockDaprClient) GetState(ctx context.Context, storeName, key string, meta map[string]string) (*daprc.StateItem, error) {
	if m.getStateFn != nil {
		return m.getStateFn(ctx, storeName, key, meta)
	}
	return &daprc.StateItem{}, nil
}

// --- No-op implementations for the rest of the interface ---

func (m *mockDaprClient) InvokeOutputBinding(_ context.Context, _ *daprc.InvokeBindingRequest) error {
	return nil
}
func (m *mockDaprClient) InvokeMethod(_ context.Context, _, _, _ string) ([]byte, error) {
	return nil, nil
}
func (m *mockDaprClient) InvokeMethodWithContent(_ context.Context, _, _, _ string, _ *daprc.DataContent) ([]byte, error) {
	return nil, nil
}
func (m *mockDaprClient) InvokeMethodWithCustomContent(_ context.Context, _, _, _, _ string, _ interface{}) ([]byte, error) {
	return nil, nil
}
func (m *mockDaprClient) SetMetadata(_ context.Context, _, _ string) error { return nil }
func (m *mockDaprClient) PublishEvent(_ context.Context, _, _ string, _ interface{}, _ ...daprc.PublishEventOption) error {
	return nil
}
func (m *mockDaprClient) PublishEventfromCustomContent(_ context.Context, _, _ string, _ interface{}) error {
	return nil
}
func (m *mockDaprClient) PublishEvents(_ context.Context, _, _ string, _ []interface{}, _ ...daprc.PublishEventsOption) daprc.PublishEventsResponse {
	return daprc.PublishEventsResponse{}
}
func (m *mockDaprClient) GetSecret(_ context.Context, _, _ string, _ map[string]string) (map[string]string, error) {
	return nil, nil
}
func (m *mockDaprClient) GetBulkSecret(_ context.Context, _ string, _ map[string]string) (map[string]map[string]string, error) {
	return nil, nil
}
func (m *mockDaprClient) SaveState(_ context.Context, _, _ string, _ []byte, _ map[string]string, _ ...daprc.StateOption) error {
	return nil
}
func (m *mockDaprClient) SaveStateWithETag(_ context.Context, _, _ string, _ []byte, _ string, _ map[string]string, _ ...daprc.StateOption) error {
	return nil
}
func (m *mockDaprClient) SaveBulkState(_ context.Context, _ string, _ ...*daprc.SetStateItem) error {
	return nil
}
func (m *mockDaprClient) GetStateWithConsistency(_ context.Context, _, _ string, _ map[string]string, _ daprc.StateConsistency) (*daprc.StateItem, error) {
	return nil, nil
}
func (m *mockDaprClient) GetBulkState(_ context.Context, _ string, _ []string, _ map[string]string, _ int32) ([]*daprc.BulkStateItem, error) {
	return nil, nil
}
func (m *mockDaprClient) QueryStateAlpha1(_ context.Context, _, _ string, _ map[string]string) (*daprc.QueryResponse, error) {
	return nil, nil
}
func (m *mockDaprClient) DeleteState(_ context.Context, _, _ string, _ map[string]string) error {
	return nil
}
func (m *mockDaprClient) DeleteStateWithETag(_ context.Context, _, _ string, _ *daprc.ETag, _ map[string]string, _ *daprc.StateOptions) error {
	return nil
}
func (m *mockDaprClient) ExecuteStateTransaction(_ context.Context, _ string, _ map[string]string, _ []*daprc.StateOperation) error {
	return nil
}
func (m *mockDaprClient) GetConfigurationItem(_ context.Context, _, _ string, _ ...daprc.ConfigurationOpt) (*daprc.ConfigurationItem, error) {
	return nil, nil
}
func (m *mockDaprClient) GetConfigurationItems(_ context.Context, _ string, _ []string, _ ...daprc.ConfigurationOpt) (map[string]*daprc.ConfigurationItem, error) {
	return nil, nil
}
func (m *mockDaprClient) SubscribeConfigurationItems(_ context.Context, _ string, _ []string, _ daprc.ConfigurationHandleFunction, _ ...daprc.ConfigurationOpt) (string, error) {
	return "", nil
}
func (m *mockDaprClient) UnsubscribeConfigurationItems(_ context.Context, _ string, _ string, _ ...daprc.ConfigurationOpt) error {
	return nil
}
func (m *mockDaprClient) Subscribe(_ context.Context, _ daprc.SubscriptionOptions) (*daprc.Subscription, error) {
	return nil, nil
}
func (m *mockDaprClient) SubscribeWithHandler(_ context.Context, _ daprc.SubscriptionOptions, _ daprc.SubscriptionHandleFunction) (func() error, error) {
	return nil, nil
}
func (m *mockDaprClient) DeleteBulkState(_ context.Context, _ string, _ []string, _ map[string]string) error {
	return nil
}
func (m *mockDaprClient) DeleteBulkStateItems(_ context.Context, _ string, _ []*daprc.DeleteStateItem) error {
	return nil
}
func (m *mockDaprClient) TryLockAlpha1(_ context.Context, _ string, _ *daprc.LockRequest) (*daprc.LockResponse, error) {
	return nil, nil
}
func (m *mockDaprClient) UnlockAlpha1(_ context.Context, _ string, _ *daprc.UnlockRequest) (*daprc.UnlockResponse, error) {
	return nil, nil
}
func (m *mockDaprClient) Encrypt(_ context.Context, in io.Reader, _ daprc.EncryptOptions) (io.Reader, error) {
	return in, nil
}
func (m *mockDaprClient) Decrypt(_ context.Context, in io.Reader, _ daprc.DecryptOptions) (io.Reader, error) {
	return in, nil
}
func (m *mockDaprClient) Shutdown(_ context.Context) error          { return nil }
func (m *mockDaprClient) Wait(_ context.Context, _ time.Duration) error { return nil }
func (m *mockDaprClient) WithTraceID(ctx context.Context, _ string) context.Context {
	return ctx
}
func (m *mockDaprClient) WithAuthToken(_ string) {}
func (m *mockDaprClient) Close() {}

// Actor methods
func (m *mockDaprClient) RegisterActorTimer(_ context.Context, _ *daprc.RegisterActorTimerRequest) error {
	return nil
}
func (m *mockDaprClient) UnregisterActorTimer(_ context.Context, _ *daprc.UnregisterActorTimerRequest) error {
	return nil
}
func (m *mockDaprClient) RegisterActorReminder(_ context.Context, _ *daprc.RegisterActorReminderRequest) error {
	return nil
}
func (m *mockDaprClient) UnregisterActorReminder(_ context.Context, _ *daprc.UnregisterActorReminderRequest) error {
	return nil
}
func (m *mockDaprClient) InvokeActor(_ context.Context, _ *daprc.InvokeActorRequest) (*daprc.InvokeActorResponse, error) {
	return nil, nil
}
func (m *mockDaprClient) GetActorState(_ context.Context, _ *daprc.GetActorStateRequest) (*daprc.GetActorStateResponse, error) {
	return nil, nil
}
func (m *mockDaprClient) SaveStateTransactionally(_ context.Context, _, _ string, _ []*daprc.ActorStateOperation) error {
	return nil
}
func (m *mockDaprClient) ImplActorClientStub(stub actor.Client, _ ...actorcfg.Option) {}

// Workflow methods
func (m *mockDaprClient) StartWorkflowBeta1(_ context.Context, _ *daprc.StartWorkflowRequest) (*daprc.StartWorkflowResponse, error) {
	return nil, nil
}
func (m *mockDaprClient) GetWorkflowBeta1(_ context.Context, _ *daprc.GetWorkflowRequest) (*daprc.GetWorkflowResponse, error) {
	return nil, nil
}
func (m *mockDaprClient) PurgeWorkflowBeta1(_ context.Context, _ *daprc.PurgeWorkflowRequest) error {
	return nil
}
func (m *mockDaprClient) TerminateWorkflowBeta1(_ context.Context, _ *daprc.TerminateWorkflowRequest) error {
	return nil
}
func (m *mockDaprClient) PauseWorkflowBeta1(_ context.Context, _ *daprc.PauseWorkflowRequest) error {
	return nil
}
func (m *mockDaprClient) ResumeWorkflowBeta1(_ context.Context, _ *daprc.ResumeWorkflowRequest) error {
	return nil
}
func (m *mockDaprClient) RaiseEventWorkflowBeta1(_ context.Context, _ *daprc.RaiseEventWorkflowRequest) error {
	return nil
}

// Job methods
func (m *mockDaprClient) ScheduleJobAlpha1(_ context.Context, _ *daprc.Job) error { return nil }
func (m *mockDaprClient) GetJobAlpha1(_ context.Context, _ string) (*daprc.Job, error) {
	return nil, nil
}
func (m *mockDaprClient) DeleteJobAlpha1(_ context.Context, _ string) error { return nil }

// gRPC accessors
func (m *mockDaprClient) GrpcClient() daprpb.DaprClient  { return nil }
func (m *mockDaprClient) GrpcClientConn() *grpc.ClientConn { return nil }

// --- Helpers ---

func newTestDSM(mc *mockDaprClient) *DaprStateManager {
	cfg := Config{
		StorageRoot:      "/test/root",
		CrawlID:          "crawl-123",
		CrawlExecutionID: "exec-456",
	}
	base := NewBaseStateManager(cfg)
	var client daprc.Client = mc
	return &DaprStateManager{
		BaseStateManager:      base,
		client:                &client,
		stateStoreName:        "statestore",
		storageBinding:        "telegramcrawlstorage",
		mediaCache:            make(map[string]MediaCacheItem),
		mediaCacheShards:      make(map[string]*MediaCache),
		activeMediaCache:      MediaCache{Items: make(map[string]MediaCacheItem)},
		mediaCacheIndex:       MediaCacheIndex{MediaIndex: make(map[string]string)},
		maxCacheItemsPerShard: 5000,
		cacheExpirationDays:   30,
	}
}

// decodeBindingData reverses the base64 encoding applied before the binding call.
func decodeBindingData(t *testing.T, data []byte) []byte {
	t.Helper()
	decoded, err := base64.StdEncoding.DecodeString(string(data))
	if err != nil {
		t.Fatalf("failed to base64-decode binding data: %v", err)
	}
	return decoded
}

// parseJSONLPages decodes every line of a JSONL payload into Page structs.
func parseJSONLPages(t *testing.T, data []byte) []Page {
	t.Helper()
	var pages []Page
	for _, line := range strings.Split(strings.TrimRight(string(data), "\n"), "\n") {
		if line == "" {
			continue
		}
		var p Page
		if err := json.Unmarshal([]byte(line), &p); err != nil {
			t.Fatalf("failed to unmarshal JSONL line %q: %v", line, err)
		}
		pages = append(pages, p)
	}
	return pages
}

// --- Tests ---

func TestExportPagesToBinding_NoPagesReturnsError(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newTestDSM(mc)

	err := dsm.ExportPagesToBinding("crawl-123")
	if err == nil {
		t.Fatal("expected error for empty state, got nil")
	}
	if !strings.Contains(err.Error(), "no pages found") {
		t.Fatalf("unexpected error message: %v", err)
	}
	if len(mc.bindingCalls) != 0 {
		t.Fatalf("expected 0 binding calls, got %d", len(mc.bindingCalls))
	}
}

func TestExportPagesToBinding_SinglePageInMemory(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newTestDSM(mc)

	page := Page{
		ID:  "page-1",
		URL: "https://t.me/testchannel",
	}
	dsm.layerMap[0] = []string{page.ID}
	dsm.pageMap[page.ID] = page

	if err := dsm.ExportPagesToBinding("crawl-123"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mc.bindingCalls) != 1 {
		t.Fatalf("expected 1 binding call, got %d", len(mc.bindingCalls))
	}

	decoded := decodeBindingData(t, mc.bindingCalls[0].Data)
	pages := parseJSONLPages(t, decoded)
	if len(pages) != 1 {
		t.Fatalf("expected 1 page in output, got %d", len(pages))
	}
	if pages[0].URL != page.URL {
		t.Errorf("expected URL %q, got %q", page.URL, pages[0].URL)
	}

	// Filename must contain crawlID and "part0"
	call := mc.bindingCalls[0]
	for _, v := range call.Metadata {
		if strings.Contains(v, "crawl-123") && strings.Contains(v, "part0") {
			return
		}
	}
	t.Error("expected binding metadata to contain crawlID and part0 in the file path")
}

func TestExportPagesToBinding_MultiplePagesSingleChunk(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newTestDSM(mc)

	for i := 0; i < 5; i++ {
		id := Page{ID: strings.Repeat("x", 1) + string(rune('a'+i)), URL: "https://t.me/chan" + string(rune('a'+i))}
		dsm.layerMap[0] = append(dsm.layerMap[0], id.ID)
		dsm.pageMap[id.ID] = id
	}

	if err := dsm.ExportPagesToBinding("crawl-123"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mc.bindingCalls) != 1 {
		t.Fatalf("expected 1 binding call for small payload, got %d", len(mc.bindingCalls))
	}

	decoded := decodeBindingData(t, mc.bindingCalls[0].Data)
	pages := parseJSONLPages(t, decoded)
	if len(pages) != 5 {
		t.Errorf("expected 5 pages, got %d", len(pages))
	}
}

func TestExportPagesToBinding_ChunkSplitting(t *testing.T) {
	// Override chunk size so that each page triggers a new chunk.
	original := exportChunkSizeBytes
	exportChunkSizeBytes = 1 // force a flush after every page
	defer func() { exportChunkSizeBytes = original }()

	mc := &mockDaprClient{}
	dsm := newTestDSM(mc)

	const pageCount = 3
	for i := 0; i < pageCount; i++ {
		id := string(rune('a' + i))
		p := Page{ID: id, URL: "https://t.me/" + id}
		dsm.layerMap[0] = append(dsm.layerMap[0], id)
		dsm.pageMap[id] = p
	}

	if err := dsm.ExportPagesToBinding("crawl-123"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mc.bindingCalls) != pageCount {
		t.Fatalf("expected %d binding calls (one per page), got %d", pageCount, len(mc.bindingCalls))
	}

	// Each chunk file must carry a distinct part index.
	for i, call := range mc.bindingCalls {
		expectedPart := "part" + string(rune('0'+i))
		found := false
		for _, v := range call.Metadata {
			if strings.Contains(v, expectedPart) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("call %d: expected metadata to contain %q, got %v", i, expectedPart, call.Metadata)
		}
	}
}

func TestExportPagesToBinding_PagesAcrossLayers(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newTestDSM(mc)

	// Two pages at depth 0, one at depth 1.
	for depth, ids := range map[int][]string{
		0: {"p1", "p2"},
		1: {"p3"},
	} {
		dsm.layerMap[depth] = ids
		for _, id := range ids {
			dsm.pageMap[id] = Page{ID: id, URL: "https://t.me/" + id}
		}
	}

	if err := dsm.ExportPagesToBinding("crawl-123"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mc.bindingCalls) != 1 {
		t.Fatalf("expected 1 binding call, got %d", len(mc.bindingCalls))
	}

	decoded := decodeBindingData(t, mc.bindingCalls[0].Data)
	pages := parseJSONLPages(t, decoded)
	if len(pages) != 3 {
		t.Errorf("expected 3 pages across layers, got %d", len(pages))
	}
}

func TestExportPagesToBinding_PageFetchedFromDapr(t *testing.T) {
	remotePageID := "remote-page"
	remotePage := Page{ID: remotePageID, URL: "https://t.me/remote"}

	pageBytes, _ := json.Marshal(remotePage)
	mc := &mockDaprClient{
		getStateFn: func(_ context.Context, _, key string, _ map[string]string) (*daprc.StateItem, error) {
			if strings.Contains(key, remotePageID) {
				return &daprc.StateItem{Value: pageBytes}, nil
			}
			return &daprc.StateItem{}, nil
		},
	}
	dsm := newTestDSM(mc)

	// Page is in layerMap but NOT in pageMap — must be fetched from Dapr.
	dsm.layerMap[0] = []string{remotePageID}

	if err := dsm.ExportPagesToBinding("crawl-123"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mc.bindingCalls) != 1 {
		t.Fatalf("expected 1 binding call, got %d", len(mc.bindingCalls))
	}

	decoded := decodeBindingData(t, mc.bindingCalls[0].Data)
	pages := parseJSONLPages(t, decoded)
	if len(pages) != 1 || pages[0].URL != remotePage.URL {
		t.Errorf("expected remote page URL %q, got %+v", remotePage.URL, pages)
	}
}

func TestExportPagesToBinding_BindingError(t *testing.T) {
	mc := &mockDaprClient{
		invokeBindingFn: func(_ context.Context, _ *daprc.InvokeBindingRequest) (*daprc.BindingEvent, error) {
			return nil, errors.New("storage unavailable")
		},
	}
	dsm := newTestDSM(mc)
	dsm.layerMap[0] = []string{"p1"}
	dsm.pageMap["p1"] = Page{ID: "p1", URL: "https://t.me/p1"}

	err := dsm.ExportPagesToBinding("crawl-123")
	if err == nil {
		t.Fatal("expected error from binding failure, got nil")
	}
	if !strings.Contains(err.Error(), "failed to store pages via Dapr binding") {
		t.Errorf("unexpected error text: %v", err)
	}
}

func TestExportPagesToBinding_GetMetadataError(t *testing.T) {
	mc := &mockDaprClient{
		getMetadataFn: func(_ context.Context) (*daprc.GetMetadataResponse, error) {
			return nil, errors.New("metadata unavailable")
		},
	}
	dsm := newTestDSM(mc)
	dsm.layerMap[0] = []string{"p1"}
	dsm.pageMap["p1"] = Page{ID: "p1", URL: "https://t.me/p1"}

	err := dsm.ExportPagesToBinding("crawl-123")
	if err == nil {
		t.Fatal("expected error from metadata failure, got nil")
	}
	if len(mc.bindingCalls) != 0 {
		t.Errorf("expected no binding calls after metadata failure, got %d", len(mc.bindingCalls))
	}
}

func TestExportPagesToBinding_StoragePathContainsCrawlID(t *testing.T) {
	mc := &mockDaprClient{}
	dsm := newTestDSM(mc)
	dsm.layerMap[0] = []string{"p1"}
	dsm.pageMap["p1"] = Page{ID: "p1", URL: "https://t.me/p1"}

	const crawlID = "my-crawl-99"
	dsm.config.CrawlID = crawlID

	if err := dsm.ExportPagesToBinding(crawlID); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	call := mc.bindingCalls[0]
	pathFound := false
	for _, v := range call.Metadata {
		if strings.Contains(v, crawlID) {
			pathFound = true
			break
		}
	}
	if !pathFound {
		t.Errorf("expected crawlID %q in binding metadata path, got %v", crawlID, call.Metadata)
	}
}

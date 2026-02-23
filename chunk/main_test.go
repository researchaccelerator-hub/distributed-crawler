package chunk

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/model"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockStateManager is a minimal no-op implementation of StateManagementInterface.
type mockStateManager struct {
	uploadedFiles []string
	uploadErr     error
	mu            sync.Mutex
}

func (m *mockStateManager) UploadCombinedFile(filename string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.uploadedFiles = append(m.uploadedFiles, filename)
	return m.uploadErr
}

func (m *mockStateManager) Initialize(_ []string) error                                        { return nil }
func (m *mockStateManager) GetPage(_ string) (state.Page, error)                               { return state.Page{}, nil }
func (m *mockStateManager) UpdatePage(_ state.Page) error                                      { return nil }
func (m *mockStateManager) UpdateMessage(_ string, _ int64, _ int64, _ string) error           { return nil }
func (m *mockStateManager) AddLayer(_ []state.Page) error                                      { return nil }
func (m *mockStateManager) GetLayerByDepth(_ int) ([]state.Page, error)                        { return nil, nil }
func (m *mockStateManager) GetMaxDepth() (int, error)                                          { return 0, nil }
func (m *mockStateManager) SaveState() error                                                    { return nil }
func (m *mockStateManager) ExportPagesToBinding(_ string) error                                { return nil }
func (m *mockStateManager) StorePost(_ string, _ model.Post) error                             { return nil }
func (m *mockStateManager) StoreFile(_ string, _ string, _ string) (string, string, error)     { return "", "", nil }
func (m *mockStateManager) GetPreviousCrawls() ([]string, error)                               { return nil, nil }
func (m *mockStateManager) UpdateCrawlMetadata(_ string, _ map[string]interface{}) error       { return nil }
func (m *mockStateManager) FindIncompleteCrawl(_ string) (string, bool, error)                 { return "", false, nil }
func (m *mockStateManager) HasProcessedMedia(_ string) (bool, error)                           { return false, nil }
func (m *mockStateManager) MarkMediaAsProcessed(_ string) error                                { return nil }
func (m *mockStateManager) InitializeDiscoveredChannels() error                                { return nil }
func (m *mockStateManager) InitializeRandomWalkLayer() error                                   { return nil }
func (m *mockStateManager) GetRandomDiscoveredChannel() (string, error)                        { return "", nil }
func (m *mockStateManager) IsDiscoveredChannel(_ string) bool                                  { return false }
func (m *mockStateManager) AddDiscoveredChannel(_ string) error                                { return nil }
func (m *mockStateManager) StoreChannelData(_ string, _ *model.ChannelData) error             { return nil }
func (m *mockStateManager) SaveEdgeRecords(_ []*state.EdgeRecord) error                        { return nil }
func (m *mockStateManager) GetPagesFromLayerBuffer() ([]state.Page, error)                    { return nil, nil }
func (m *mockStateManager) WipeLayerBuffer(_ bool) error                                       { return nil }
func (m *mockStateManager) ExecuteDatabaseOperation(_ string, _ []any) error                   { return nil }
func (m *mockStateManager) AddPageToLayerBuffer(_ *state.Page) error                           { return nil }
func (m *mockStateManager) Close() error                                                        { return nil }

// newTestChunker creates a Chunker wired to temp directories for use in tests.
func newTestChunker(t *testing.T, sm state.StateManagementInterface, triggerSize, hardCapSize int64) (*Chunker, string, string, string) {
	t.Helper()
	base := t.TempDir()
	tempDir := filepath.Join(base, "temp")
	watchDir := filepath.Join(base, "watch")
	combineDir := filepath.Join(base, "combine")
	ctx := context.Background()
	c := NewChunker(ctx, sm, tempDir, watchDir, combineDir, triggerSize, hardCapSize)
	return c, tempDir, watchDir, combineDir
}

// ---- isOverflow ----

func TestIsOverflow_NilError(t *testing.T) {
	assert.False(t, isOverflow(nil))
}

func TestIsOverflow_MatchingMessage(t *testing.T) {
	err := fmt.Errorf("fsnotify: queue or buffer overflow")
	assert.True(t, isOverflow(err))
}

func TestIsOverflow_NonMatchingMessage(t *testing.T) {
	err := fmt.Errorf("some other error")
	assert.False(t, isOverflow(err))
}

// ---- processedMap (isSeen / markSeen / rotateMap) ----

func TestIsSeen_UnseenPath(t *testing.T) {
	c, _, _, _ := newTestChunker(t, &mockStateManager{}, 1024, 2048)
	assert.False(t, c.isSeen("/some/path.jsonl"))
}

func TestMarkSeen_MakesPathSeen(t *testing.T) {
	c, _, _, _ := newTestChunker(t, &mockStateManager{}, 1024, 2048)
	path := "/some/path.jsonl"
	c.markSeen(path)
	assert.True(t, c.isSeen(path))
}

func TestIsSeen_AfterRotate_StillSeen(t *testing.T) {
	c, _, _, _ := newTestChunker(t, &mockStateManager{}, 1024, 2048)
	path := "/old/path.jsonl"
	c.markSeen(path)
	c.rotateMap()
	// Path moved to previous map — should still be seen.
	assert.True(t, c.isSeen(path))
}

func TestIsSeen_AfterDoubleRotate_Forgotten(t *testing.T) {
	c, _, _, _ := newTestChunker(t, &mockStateManager{}, 1024, 2048)
	path := "/old/path.jsonl"
	c.markSeen(path)
	c.rotateMap()
	c.rotateMap() // second rotation evicts from previous
	assert.False(t, c.isSeen(path))
}

func TestRotateMap_CurrentIsEmpty(t *testing.T) {
	c, _, _, _ := newTestChunker(t, &mockStateManager{}, 1024, 2048)
	c.markSeen("/a.jsonl")
	c.rotateMap()
	assert.Equal(t, 0, len(c.processed.current))
	assert.Equal(t, 1, len(c.processed.previous))
}

func TestMarkSeen_MultiplePathsIndependent(t *testing.T) {
	c, _, _, _ := newTestChunker(t, &mockStateManager{}, 1024, 2048)
	c.markSeen("/a.jsonl")
	c.markSeen("/b.jsonl")
	assert.True(t, c.isSeen("/a.jsonl"))
	assert.True(t, c.isSeen("/b.jsonl"))
	assert.False(t, c.isSeen("/c.jsonl"))
}

// ---- NewChunker ----

func TestNewChunker_FieldsAreSet(t *testing.T) {
	sm := &mockStateManager{}
	ctx := context.Background()
	c := NewChunker(ctx, sm, "/tmp", "/watch", "/combine", 10, 20)

	assert.Equal(t, sm, c.sm)
	assert.Equal(t, "/tmp", c.tempDir)
	assert.Equal(t, "/watch", c.watchDir)
	assert.Equal(t, "/combine", c.combineDir)
	assert.Equal(t, int64(10), c.triggerSize)
	assert.Equal(t, int64(20), c.hardCapSize)
	assert.Equal(t, 300*time.Second, c.batchTimeout)
	assert.NotNil(t, c.fileChan)
	assert.NotNil(t, c.rescanSignal)
	assert.NotNil(t, c.processed)
}

// ---- shouldRotate ----

func TestShouldRotate_TrueOnFirstRotation(t *testing.T) {
	c, _, _, _ := newTestChunker(t, &mockStateManager{}, 1024, 2048)
	// Neither timestamp has been set — the first rotation is always allowed.
	assert.True(t, c.shouldRotate())
}

func TestShouldRotate_FalseWhenNoUploadSinceRotation(t *testing.T) {
	c, _, _, _ := newTestChunker(t, &mockStateManager{}, 1024, 2048)
	now := time.Now().UnixNano()
	c.lastRotationTime.Store(now)
	// lastUploadTime is still zero — no upload has occurred since the rotation.
	assert.False(t, c.shouldRotate())
}

func TestShouldRotate_FalseWhenUploadPredatesRotation(t *testing.T) {
	c, _, _, _ := newTestChunker(t, &mockStateManager{}, 1024, 2048)
	base := time.Now().UnixNano()
	c.lastUploadTime.Store(base)
	c.lastRotationTime.Store(base + int64(time.Second)) // rotation is newer
	assert.False(t, c.shouldRotate())
}

func TestShouldRotate_TrueWhenUploadAfterRotation(t *testing.T) {
	c, _, _, _ := newTestChunker(t, &mockStateManager{}, 1024, 2048)
	base := time.Now().UnixNano()
	c.lastRotationTime.Store(base)
	c.lastUploadTime.Store(base + int64(time.Second)) // upload is newer
	assert.True(t, c.shouldRotate())
}

// ---- combineFiles ----

func TestCombineFiles_ConcatenatesContent(t *testing.T) {
	c, _, _, combineDir := newTestChunker(t, &mockStateManager{}, 1<<20, 10<<20)

	// Create input files.
	dir := t.TempDir()
	file1 := filepath.Join(dir, "a.jsonl")
	file2 := filepath.Join(dir, "b.jsonl")
	require.NoError(t, os.WriteFile(file1, []byte(`{"id":1}`+"\n"), 0644))
	require.NoError(t, os.WriteFile(file2, []byte(`{"id":2}`+"\n"), 0644))

	stat1, _ := os.Stat(file1)
	stat2, _ := os.Stat(file2)

	require.NoError(t, os.MkdirAll(combineDir, 0755))

	batch := []FileEntry{
		{Path: file1, Size: stat1.Size()},
		{Path: file2, Size: stat2.Size()},
	}

	out, err := c.combineFiles(batch)
	require.NoError(t, err)
	defer os.Remove(out)

	data, err := os.ReadFile(out)
	require.NoError(t, err)
	assert.Equal(t, `{"id":1}`+"\n"+`{"id":2}`+"\n", string(data))
}

func TestCombineFiles_EmptyBatch(t *testing.T) {
	c, _, _, combineDir := newTestChunker(t, &mockStateManager{}, 1<<20, 10<<20)
	require.NoError(t, os.MkdirAll(combineDir, 0755))

	out, err := c.combineFiles([]FileEntry{})
	require.NoError(t, err)
	defer os.Remove(out)

	data, err := os.ReadFile(out)
	require.NoError(t, err)
	assert.Empty(t, data)
}

func TestCombineFiles_MissingSourceReturnsError(t *testing.T) {
	c, _, _, combineDir := newTestChunker(t, &mockStateManager{}, 1<<20, 10<<20)
	require.NoError(t, os.MkdirAll(combineDir, 0755))

	batch := []FileEntry{
		{Path: "/nonexistent/file.jsonl", Size: 100},
	}
	_, err := c.combineFiles(batch)
	assert.Error(t, err)
}

func TestCombineFiles_OutputInCombineDir(t *testing.T) {
	c, _, _, combineDir := newTestChunker(t, &mockStateManager{}, 1<<20, 10<<20)
	require.NoError(t, os.MkdirAll(combineDir, 0755))

	dir := t.TempDir()
	f := filepath.Join(dir, "x.jsonl")
	require.NoError(t, os.WriteFile(f, []byte("line\n"), 0644))
	info, _ := os.Stat(f)

	out, err := c.combineFiles([]FileEntry{{Path: f, Size: info.Size()}})
	require.NoError(t, err)
	defer os.Remove(out)

	assert.Equal(t, combineDir, filepath.Dir(out))
	assert.Contains(t, filepath.Base(out), "combined_")
}

// ---- processBatches ----

// drainBatches reads all batches from a channel until it is closed.
func drainBatches(ch <-chan []FileEntry) [][]FileEntry {
	var result [][]FileEntry
	for b := range ch {
		result = append(result, b)
	}
	return result
}

// runProcessBatches starts processBatches in a goroutine, pre-incrementing the
// internal WaitGroup as the production Start() path does.
func runProcessBatches(c *Chunker, out chan<- []FileEntry) *sync.WaitGroup {
	c.wg.Add(1)
	var done sync.WaitGroup
	done.Add(1)
	go func() {
		defer done.Done()
		c.processBatches(out)
	}()
	return &done
}

func TestProcessBatches_FlushOnTrigger(t *testing.T) {
	const triggerSize = 100
	const hardCap = 1000
	c, _, _, _ := newTestChunker(t, &mockStateManager{}, triggerSize, hardCap)

	jobsChan := make(chan []FileEntry, 10)
	done := runProcessBatches(c, jobsChan)

	// Send three 40-byte files. The third pushes total to 120 >= triggerSize=100.
	c.fileChan <- FileEntry{Path: "a.jsonl", Size: 40}
	c.fileChan <- FileEntry{Path: "b.jsonl", Size: 40}
	c.fileChan <- FileEntry{Path: "c.jsonl", Size: 40}
	close(c.fileChan)

	// processBatches closes jobsChan when done; drain it first, then wait.
	batches := drainBatches(jobsChan)
	done.Wait()

	require.Len(t, batches, 1)
	assert.Len(t, batches[0], 3) // all three accumulated; flush fires when total >= trigger
}

func TestProcessBatches_FlushOnTrigger_ExactSplit(t *testing.T) {
	const triggerSize = 60
	const hardCap = 1000
	c, _, _, _ := newTestChunker(t, &mockStateManager{}, triggerSize, hardCap)

	jobsChan := make(chan []FileEntry, 10)
	done := runProcessBatches(c, jobsChan)

	// File a (50 bytes) < trigger. Files a+b (100 bytes) >= triggerSize=60, so flush after b.
	// File c is then flushed at shutdown.
	c.fileChan <- FileEntry{Path: "a.jsonl", Size: 50}
	c.fileChan <- FileEntry{Path: "b.jsonl", Size: 50}
	c.fileChan <- FileEntry{Path: "c.jsonl", Size: 50}
	close(c.fileChan)

	batches := drainBatches(jobsChan)
	done.Wait()

	require.GreaterOrEqual(t, len(batches), 2)
}

func TestProcessBatches_HardCapDropsOversizedFile(t *testing.T) {
	const triggerSize = 1000
	const hardCap = 100
	c, _, _, _ := newTestChunker(t, &mockStateManager{}, triggerSize, hardCap)

	// Write a real temp file so os.Remove doesn't error.
	f, err := os.CreateTemp(t.TempDir(), "big_*.jsonl")
	require.NoError(t, err)
	f.Close()

	jobsChan := make(chan []FileEntry, 10)
	done := runProcessBatches(c, jobsChan)

	// File exceeds hardCap — should be dropped (deleted) and not forwarded.
	c.fileChan <- FileEntry{Path: f.Name(), Size: 200}
	// Also send a small file that should pass through.
	small, err2 := os.CreateTemp(t.TempDir(), "small_*.jsonl")
	require.NoError(t, err2)
	small.Close()
	c.fileChan <- FileEntry{Path: small.Name(), Size: 50}
	close(c.fileChan)

	batches := drainBatches(jobsChan)
	done.Wait()

	// Only the small file should end up in a batch.
	total := 0
	for _, b := range batches {
		total += len(b)
	}
	assert.Equal(t, 1, total)
	// Big file should have been deleted.
	_, statErr := os.Stat(f.Name())
	assert.True(t, os.IsNotExist(statErr), "expected oversized file to be deleted")
}

func TestProcessBatches_HardCapForceFlush(t *testing.T) {
	const triggerSize = 10000
	const hardCap = 100
	c, _, _, _ := newTestChunker(t, &mockStateManager{}, triggerSize, hardCap)

	jobsChan := make(chan []FileEntry, 10)
	done := runProcessBatches(c, jobsChan)

	// Two files that individually fit within hardCap but together would exceed it.
	c.fileChan <- FileEntry{Path: "x.jsonl", Size: 60}
	c.fileChan <- FileEntry{Path: "y.jsonl", Size: 60}
	close(c.fileChan)

	batches := drainBatches(jobsChan)
	done.Wait()

	// "x" accumulates, then when "y" would push total to 120 > hardCap,
	// a force-flush fires before "y" is added.
	require.GreaterOrEqual(t, len(batches), 2)
	assert.Len(t, batches[0], 1) // only "x"
	assert.Len(t, batches[1], 1) // only "y"
}

func TestProcessBatches_FinalPartialBatchFlushed(t *testing.T) {
	const triggerSize = 10000
	const hardCap = 100000
	c, _, _, _ := newTestChunker(t, &mockStateManager{}, triggerSize, hardCap)

	jobsChan := make(chan []FileEntry, 10)
	done := runProcessBatches(c, jobsChan)

	c.fileChan <- FileEntry{Path: "a.jsonl", Size: 10}
	c.fileChan <- FileEntry{Path: "b.jsonl", Size: 10}
	close(c.fileChan)

	batches := drainBatches(jobsChan)
	done.Wait()

	require.Len(t, batches, 1)
	assert.Len(t, batches[0], 2)
}

func TestProcessBatches_EmptyChannelProducesNoBatches(t *testing.T) {
	c, _, _, _ := newTestChunker(t, &mockStateManager{}, 100, 1000)

	jobsChan := make(chan []FileEntry, 10)
	done := runProcessBatches(c, jobsChan)

	close(c.fileChan)

	batches := drainBatches(jobsChan)
	done.Wait()

	assert.Empty(t, batches)
}

func TestProcessBatches_TotalUploadSizeTracked(t *testing.T) {
	const triggerSize = 1
	const hardCap = 1000000
	c, _, _, _ := newTestChunker(t, &mockStateManager{}, triggerSize, hardCap)

	jobsChan := make(chan []FileEntry, 10)
	done := runProcessBatches(c, jobsChan)

	c.fileChan <- FileEntry{Path: "a.jsonl", Size: 50}
	c.fileChan <- FileEntry{Path: "b.jsonl", Size: 75}
	close(c.fileChan)

	drainBatches(jobsChan)
	done.Wait()

	assert.Equal(t, int64(125), c.totalUploadSize)
	assert.Equal(t, int32(2), c.postsUploaded)
}

// ---- VerifyCleanup / recoverWatchDir / recoverCombineDir ----

// setupVerifyDirs creates all three directories used by VerifyCleanup tests.
func setupVerifyDirs(t *testing.T, c *Chunker, tempDir, watchDir, combineDir string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(tempDir, 0755))
	require.NoError(t, os.MkdirAll(watchDir, 0755))
	require.NoError(t, os.MkdirAll(combineDir, 0755))
}

func TestVerifyCleanup_EmptyDirs(t *testing.T) {
	c, tempDir, watchDir, combineDir := newTestChunker(t, &mockStateManager{}, 1024, 2048)
	setupVerifyDirs(t, c, tempDir, watchDir, combineDir)
	assert.NotPanics(t, func() { c.VerifyCleanup() })
}

func TestVerifyCleanup_MissingDirsDoNotPanic(t *testing.T) {
	base := t.TempDir()
	ctx := context.Background()
	c := NewChunker(ctx, &mockStateManager{},
		filepath.Join(base, "no_temp"),
		filepath.Join(base, "no_watch"),
		filepath.Join(base, "no_combine"),
		1024, 2048,
	)
	assert.NotPanics(t, func() { c.VerifyCleanup() })
}

// TestVerifyCleanup_UploadsWatchDirFiles is the primary happy-path test:
// files stranded in watchDir (e.g. produced after the watcher stopped) are
// combined, uploaded, and deleted.
func TestVerifyCleanup_UploadsWatchDirFiles(t *testing.T) {
	sm := &mockStateManager{}
	const hardCap = 10 << 20 // 10 MB — well above test content
	c, tempDir, watchDir, combineDir := newTestChunker(t, sm, 1<<20, hardCap)
	setupVerifyDirs(t, c, tempDir, watchDir, combineDir)

	// Simulate two files left behind after shutdown.
	require.NoError(t, os.WriteFile(filepath.Join(watchDir, "a.jsonl"), []byte(`{"id":1}`+"\n"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(watchDir, "b.jsonl"), []byte(`{"id":2}`+"\n"), 0644))

	c.VerifyCleanup()

	// Both source files must have been deleted.
	_, errA := os.Stat(filepath.Join(watchDir, "a.jsonl"))
	_, errB := os.Stat(filepath.Join(watchDir, "b.jsonl"))
	assert.True(t, os.IsNotExist(errA), "a.jsonl should have been removed")
	assert.True(t, os.IsNotExist(errB), "b.jsonl should have been removed")

	// The combined file must have been uploaded and then deleted too.
	sm.mu.Lock()
	uploaded := len(sm.uploadedFiles)
	sm.mu.Unlock()
	assert.Equal(t, 1, uploaded, "expected exactly one upload call")

	entries, _ := os.ReadDir(combineDir)
	assert.Empty(t, entries, "combineDir should be empty after upload")
}

// TestVerifyCleanup_ContentIsPreserved checks that the combined output
// contains all lines from the source files.
func TestVerifyCleanup_ContentIsPreserved(t *testing.T) {
	sm := &mockStateManager{}
	c, tempDir, watchDir, combineDir := newTestChunker(t, sm, 1<<20, 10<<20)
	setupVerifyDirs(t, c, tempDir, watchDir, combineDir)

	line1 := `{"id":1}` + "\n"
	line2 := `{"id":2}` + "\n"
	require.NoError(t, os.WriteFile(filepath.Join(watchDir, "a.jsonl"), []byte(line1), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(watchDir, "b.jsonl"), []byte(line2), 0644))

	// Intercept the upload to read the combined file before cleanup deletes it.
	var capturedContent []byte
	sm.mu.Lock()
	// Override uploadErr is nil; capture via a wrapper would require a richer mock.
	// Instead, inspect combineDir at upload time by wrapping UploadCombinedFile.
	sm.mu.Unlock()
	// Use a capturing mock instead.
	capturingSM := &capturingMockSM{}
	c.sm = capturingSM
	c.VerifyCleanup()

	capturedContent = capturingSM.lastContent
	assert.Equal(t, line1+line2, string(capturedContent),
		"combined file should contain both lines in order")
}

// capturingMockSM is a variant of mockStateManager that reads and stores the
// content of the uploaded file so tests can assert on it.
type capturingMockSM struct {
	mockStateManager
	lastContent []byte
}

func (m *capturingMockSM) UploadCombinedFile(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	m.lastContent = data
	return nil
}

// TestVerifyCleanup_HardCapDropsOversizedFile verifies that a file larger than
// hardCapSize is deleted without being uploaded, matching processBatches behaviour.
func TestVerifyCleanup_HardCapDropsOversizedFile(t *testing.T) {
	sm := &mockStateManager{}
	const hardCap = 10 // 10 bytes — tiny
	c, tempDir, watchDir, combineDir := newTestChunker(t, sm, 5, hardCap)
	setupVerifyDirs(t, c, tempDir, watchDir, combineDir)

	bigContent := make([]byte, 20) // 20 bytes > hardCap
	big := filepath.Join(watchDir, "big.jsonl")
	require.NoError(t, os.WriteFile(big, bigContent, 0644))

	c.VerifyCleanup()

	_, err := os.Stat(big)
	assert.True(t, os.IsNotExist(err), "oversized file should have been deleted")

	sm.mu.Lock()
	assert.Empty(t, sm.uploadedFiles, "no upload should occur for oversized file")
	sm.mu.Unlock()
}

// TestVerifyCleanup_HardCapSplitsBatches verifies that when watch-dir files
// together exceed hardCapSize they are split across multiple upload calls.
func TestVerifyCleanup_HardCapSplitsBatches(t *testing.T) {
	sm := &mockStateManager{}
	const hardCap = 15 // bytes — forces split between two 10-byte files
	c, tempDir, watchDir, combineDir := newTestChunker(t, sm, 5, hardCap)
	setupVerifyDirs(t, c, tempDir, watchDir, combineDir)

	content := []byte("0123456789") // exactly 10 bytes
	require.NoError(t, os.WriteFile(filepath.Join(watchDir, "a.jsonl"), content, 0644))
	require.NoError(t, os.WriteFile(filepath.Join(watchDir, "b.jsonl"), content, 0644))

	c.VerifyCleanup()

	sm.mu.Lock()
	uploads := len(sm.uploadedFiles)
	sm.mu.Unlock()
	// 10+10=20 > hardCap=15, so the second file must have triggered a second batch.
	assert.Equal(t, 2, uploads, "expected two upload calls due to hard-cap split")

	// All source files must be gone.
	entries, _ := os.ReadDir(watchDir)
	assert.Empty(t, entries)
}

// TestVerifyCleanup_UploadsLeftoverCombinedFiles verifies that combined files
// left in combineDir (from a prior crashed upload) are re-uploaded and removed.
func TestVerifyCleanup_UploadsLeftoverCombinedFiles(t *testing.T) {
	sm := &mockStateManager{}
	c, tempDir, watchDir, combineDir := newTestChunker(t, sm, 1<<20, 10<<20)
	setupVerifyDirs(t, c, tempDir, watchDir, combineDir)

	// Simulate a previously-combined file whose upload crashed.
	leftover := filepath.Join(combineDir, "combined_leftover.jsonl")
	require.NoError(t, os.WriteFile(leftover, []byte(`{"id":99}`+"\n"), 0644))

	c.VerifyCleanup()

	sm.mu.Lock()
	assert.Equal(t, 1, len(sm.uploadedFiles))
	assert.Equal(t, leftover, sm.uploadedFiles[0])
	sm.mu.Unlock()

	_, err := os.Stat(leftover)
	assert.True(t, os.IsNotExist(err), "leftover combined file should be deleted after upload")
}

// TestVerifyCleanup_TempDirFilesNotUploaded verifies that files in tempDir are
// reported but not uploaded (they are partially-written and unsafe to upload).
func TestVerifyCleanup_TempDirFilesNotUploaded(t *testing.T) {
	sm := &mockStateManager{}
	c, tempDir, watchDir, combineDir := newTestChunker(t, sm, 1<<20, 10<<20)
	setupVerifyDirs(t, c, tempDir, watchDir, combineDir)

	// Write an incomplete file to tempDir.
	require.NoError(t, os.WriteFile(filepath.Join(tempDir, "incomplete.jsonl"), []byte("partial"), 0644))

	c.VerifyCleanup()

	// The incomplete file must NOT be uploaded and must still exist.
	sm.mu.Lock()
	assert.Empty(t, sm.uploadedFiles)
	sm.mu.Unlock()

	_, err := os.Stat(filepath.Join(tempDir, "incomplete.jsonl"))
	assert.NoError(t, err, "incomplete temp file should still be present")
}

// TestVerifyCleanup_UploadFailureLeavesFilesForRecovery verifies that when an
// upload fails, source files are left on disk rather than deleted.
func TestVerifyCleanup_UploadFailureLeavesFilesForRecovery(t *testing.T) {
	sm := &mockStateManager{uploadErr: fmt.Errorf("storage unavailable")}
	c, tempDir, watchDir, combineDir := newTestChunker(t, sm, 1<<20, 10<<20)
	setupVerifyDirs(t, c, tempDir, watchDir, combineDir)

	src := filepath.Join(watchDir, "data.jsonl")
	require.NoError(t, os.WriteFile(src, []byte(`{"id":1}`+"\n"), 0644))

	c.VerifyCleanup()

	// Source file should remain because the upload failed.
	_, err := os.Stat(src)
	assert.NoError(t, err, "source file must remain when upload fails")
}

// ---- Start (directory creation) ----

func TestStart_CreatesDirs(t *testing.T) {
	c, tempDir, watchDir, combineDir := newTestChunker(t, &mockStateManager{}, 1<<20, 10<<20)

	err := c.Start()
	require.NoError(t, err)
	c.Shutdown()

	for _, dir := range []string{tempDir, watchDir, combineDir} {
		info, statErr := os.Stat(dir)
		require.NoError(t, statErr)
		assert.True(t, info.IsDir())
	}
}

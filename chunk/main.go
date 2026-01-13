package chunk

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/rs/zerolog/log"
)

type Chunker struct {
	sm           state.StateManagementInterface
	tempDir      string        // directory where crawl files are written
	watchDir     string        // directory where crawl files are moved after write is completed
	combineDir   string        // directory where combined files are stored before upload
	triggerSize  int64         // size in mb to trigger an upload
	hardCapSize  int64         // size in mb to not exceed
	batchTimeout time.Duration // length in seconds to wait after last created file before flushing batch
	fileChan     chan FileEntry

	// New fields for overflow recovery
	rescanSignal chan struct{}
	// processed    sync.Map // Map[string]time.Time to prevent duplicates
	processed *processedMap
	// for handling closing of chunker
	ctx          context.Context
	wg           sync.WaitGroup
	shutdownOnce sync.Once
}

type FileEntry struct {
	Path string
	Size int64
}

type BatchState struct {
	Files []FileEntry
	Size  int64
}

// processedMap implements a double-buffering strategy to prevent
// map bucket bloat. By rotating maps, we allow the GC to reclaim
// memory used by old map structures.
type processedMap struct {
	sync.RWMutex
	current  map[string]struct{}
	previous map[string]struct{}
}

// func resetTimer(t *time.Timer, duration time.Duration) {
// 	if !t.Stop() {
// 		// Drain the channel to prevent an immediate, spurious wake-up.
// 		select {
// 		case <-t.C:
// 		default:
// 		}
// 	}
// 	// Reset the timer for the next interval.
// 	t.Reset(duration)
// }

func NewChunker(ctx context.Context, sm state.StateManagementInterface, tempDir string, watchDir string, combineDir string, triggerSize int64, hardCapSize int64) *Chunker {
	return &Chunker{
		sm:           sm,
		tempDir:      tempDir,
		watchDir:     watchDir,
		combineDir:   combineDir,
		triggerSize:  triggerSize,
		hardCapSize:  hardCapSize,
		batchTimeout: 300 * time.Second,           // 5 minutes
		fileChan:     make(chan FileEntry, 10000), // Increased buffer for bursts
		rescanSignal: make(chan struct{}, 1),
		processed: &processedMap{
			current:  make(map[string]struct{}),
			previous: make(map[string]struct{}),
		},
		ctx: ctx,
	}
}

func (c *Chunker) Start() error {
	if err := os.MkdirAll(c.watchDir, 0755); err != nil {
		return fmt.Errorf("failed to create watch directory: %s", c.watchDir)
	}
	if err := os.MkdirAll(c.combineDir, 0755); err != nil {
		return fmt.Errorf("failed to create combine directory: %s", c.combineDir)
	}
	if err := os.MkdirAll(c.tempDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp directory: %s", c.tempDir)
	}

	jobsChan := make(chan []FileEntry, 100)
	internalBuffer := make(chan fsnotify.Event, 100000)

	c.wg.Add(5)

	// 1. Start the Recovery Worker (Independent)
	go c.recoveryWorker()

	// 2. Start the Event Watcher
	go c.watchFilesWithInternalBuffer(internalBuffer)

	// 3. Start the Event Processor (Moves events from buffer to fileChan)
	go c.processEvents(internalBuffer)

	// 4. Start the Batcher (Aggregates files into jobs)
	go c.processBatches(jobsChan)

	// 5. Start the Consumer (Combines and Uploads)
	go c.consumeBatches(jobsChan)

	log.Info().
		Int64("trigger_mb", c.triggerSize/1024/1024).
		Int64("hardcap_mb", c.hardCapSize/1024/1024).
		Timestamp().Dur("timeout_seconds", c.batchTimeout/1000).
		Msg("Chunker started with automated overflow recovery.")

	// Monitor for shutdown signal
	go func() {
		<-c.ctx.Done()
		log.Info().Msg("Chunker shutdown signal received. Starting graceful drain...")
		// The drain happens naturally as we close the first pipe in the chain
	}()

	return nil
}

// Wait allows calling process to block until the Chunker has fully drained
func (c *Chunker) Wait() {
	c.wg.Wait()
}

func (c *Chunker) watchFilesWithInternalBuffer(out chan<- fsnotify.Event) {
	defer c.wg.Done()
	defer close(out)            // closing this signals start of shutdown
	defer close(c.rescanSignal) // closing this signals recoveryWorker to close

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal().Err(err).Str("log_tag", "chunk_wf").Msg("Failed to create watcher")
	}
	defer watcher.Close()
	if err := watcher.Add(c.watchDir); err != nil {
		log.Fatal().Err(err).Str("log_tag", "chunk_wf").Msg("Failed to add watch dir")
	}
	log.Info().Str("watch_dir", c.watchDir).Str("log_tag", "chunk_wf").Msg("Watching directory for crawl data")

	for {
		select {
		case <-c.ctx.Done():
			log.Info().Str("log_tag", "chunk_wf").Msg("Chunk watcher shutting down...")
			return
		case event, ok := <-watcher.Events:
			if !ok {
				log.Error().Err(err).Str("log_tag", "chunk_wf").Msg("Encountered error in watchFilesInternalBuffer Events")
				return
			}
			out <- event
		case err, ok := <-watcher.Errors:
			if !ok {
				log.Error().Err(err).Str("log_tag", "chunk_wf").Msg("Encountered error in file watcher Errors. Not ok")
				return
			}
			log.Error().Err(err).Str("log_tag", "chunk_wf").Msg("Encountered error in file watcher Errors. Ok")
			if isOverflow(err) {
				log.Error().Msg("Overflow detected! Signaling recovery worker.")
				select {
				case c.rescanSignal <- struct{}{}:
				default:
					// Signal already pending
				}
			}
		}
	}
}

func (c *Chunker) processEvents(internalBuffer <-chan fsnotify.Event) {
	defer c.wg.Done()
	defer close(c.fileChan) // signal processBatches to finish
	// for event := range internalBuffer {
	// 	// Only process writes/creates of jsonl files
	// 	if (event.Has(fsnotify.Rename) || event.Has(fsnotify.Create)) && strings.HasSuffix(event.Name, ".jsonl") {

	// 		if c.isSeen(event.Name) {
	// 			continue
	// 		}
	// 		c.markSeen(event.Name)

	// 		info, err := os.Stat(event.Name)
	// 		if err != nil {
	// 			continue
	// 		}

	// 		c.fileChan <- FileEntry{
	// 			Path: event.Name,
	// 			Size: info.Size(),
	// 		}
	// 	}
	// }

	for event := range internalBuffer {
		if (event.Has(fsnotify.Rename) || event.Has(fsnotify.Create)) && strings.HasSuffix(event.Name, ".jsonl") {
			if c.isSeen(event.Name) {
				continue
			}
			c.markSeen(event.Name)

			info, err := os.Stat(event.Name)
			if err != nil {
				continue
			}

			c.fileChan <- FileEntry{
				Path: event.Name,
				Size: info.Size(),
			}
		}
	}
	log.Info().Str("log_tag", "chunk_pe").Msg("Chunk internal buffer drained and closed.")
}

func (c *Chunker) recoveryWorker() {
	defer c.wg.Done()
	// Rotate map every 15 minutes to prevent OOM
	rotationTicker := time.NewTicker(15 * time.Minute)
	defer rotationTicker.Stop()

	for {

		select {

		case <-c.ctx.Done():
			log.Info().Str("log_tag", "chunk_rw").Msg("Recovery worker shutting down from closed context")
			return
		case <-rotationTicker.C:
			c.rotateMap()
		case <-c.rescanSignal:
			if c.ctx.Err() != nil {
				log.Info().Str("log_tag", "chunk_rw").Msg("Recovery worker shutting down from closed rescan signal")
				return
			}

			// Cooldown to let the burst finish writing to disk
			time.Sleep(5 * time.Second)
			log.Info().Str("log_tag", "chunk_rw").Msg("Starting directory rescan for missed files...")

			entries, err := os.ReadDir(c.watchDir)
			if err != nil {
				log.Error().Err(err).Str("log_tag", "chunk_rw").Msg("Recovery rescan failed to read directory")
				continue
			}

			// Sort entries by ModTime to ensure chronological processing
			var files []fs.FileInfo
			for _, entry := range entries {
				if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".jsonl") {
					if info, err := entry.Info(); err == nil {
						files = append(files, info)
					}
				}
			}

			slices.SortFunc(files, func(a, b fs.FileInfo) int {
				return a.ModTime().Compare(b.ModTime())
			})

			for _, file := range files {
				fullPath := filepath.Join(c.watchDir, file.Name())
				if c.isSeen(fullPath) {
					continue
				}

				c.markSeen(fullPath)
				c.fileChan <- FileEntry{
					Path: fullPath,
					Size: file.Size(),
				}
			}

			log.Info().Str("log_tag", "chunk_rw").Msg("Recovery rescan complete.")
		}
	}
}

func (c *Chunker) processBatches(out chan<- []FileEntry) {
	defer c.wg.Done()
	defer close(out) // Signal to consumeBatches to finish uploads

	state := BatchState{}
	// timer := time.NewTimer(c.batchTimeout)

	flush := func() {
		if len(state.Files) > 0 {
			batchToSend := make([]FileEntry, len(state.Files))
			copy(batchToSend, state.Files)
			out <- batchToSend
			state.Files = nil
			state.Size = 0
		}
	}

	for file := range c.fileChan {
		// delete files we can't upload
		if file.Size > c.hardCapSize {
			log.Warn().Str("file_name", file.Path).Int64("total_bytes", file.Size).Str("log_tag", "chunk_pb").Msg("File exceeds hard cap. Deleting")
			if err := os.Remove(file.Path); err != nil {
				log.Error().Err(err).Str("file_name", file.Path).Str("log_tag", "chunk_pb").Msg("failed to remove file")
			}
			continue
		}
		// flush the buffer when we hit the hard cap
		if state.Size > 0 && state.Size+file.Size > c.hardCapSize {
			log.Info().Str("log_tag", "chunk_pb").Msg("Hard cap forced flush")
			flush()
		}

		state.Files = append(state.Files, file)
		state.Size += file.Size
		if len(state.Files)%1000 == 0 {
			log.Info().Int("file_count", len(state.Files)).Int64("buffer_size_bytes", state.Size).Str("log_tag", "chunk_pb").Msg("Current buffer")
		}
		if state.Size >= c.triggerSize {
			log.Debug().Str("log_tag", "chunk_pb").Msg("Trigger size reached. Flushing batch")
			flush()
		}
	}
	if len(state.Files) > 0 {
		log.Info().Str("log_tag", "chunk_pb").Int("remaining_files", len(state.Files)).
			Msg("Shutting down: Flushing final partial batch")
		flush()
	}

	log.Info().Str("log_tag", "chunk_pb").Msg("Chunk process batches shutdown complete")

}

func (c *Chunker) consumeBatches(jobs <-chan []FileEntry) {
	defer c.wg.Done()
	for batch := range jobs {
		outputName, err := c.combineFiles(batch)
		if err != nil {
			log.Error().Err(err).Str("log_tag", "chunk_cb").Msg("Failed to combine batch. Files not deleted")
			continue
		}

		info, err := os.Stat(outputName)
		if err != nil {
			log.Warn().Err(err).Str("combined_file", outputName).Str("log_tag", "chunk_cb").Msg("Could not stat file in file combined")
		} else {
			log.Info().Str("combined_file", outputName).Int64("total_bytes", info.Size()).Str("log_tag", "chunk_cb").
				Msg("Batch combined. Uploading to storage")
		}
		err = c.sm.UploadCombinedFile(outputName)

		if err != nil {
			log.Error().Err(err).Str("log_tag", "chunk_cb").Msg("Failed to upload combined file")
			continue
		}

		log.Info().Str("combined_file", outputName).Str("log_tag", "chunk_cb").Msg("Upload completed. Deleting source files")

		for _, file := range batch {
			if err := os.Remove(file.Path); err != nil {
				log.Error().Err(err).Str("file_name", file.Path).Str("log_tag", "chunk_cb").Msg("Failed to remove file")
			}
		}

		if err := os.Remove(outputName); err != nil {
			log.Error().Err(err).Str("file_name", outputName).Str("log_tag", "chunk_cb").Msg("Failed to delete combined file")
		}
	}
	log.Info().Str("log_tag", "chunk_cb").Msg("All batches uploaded and deleted")
}

func (c *Chunker) combineFiles(batch []FileEntry) (string, error) {
	outputFileName := filepath.Join(c.combineDir, fmt.Sprintf("combined_%d.jsonl", time.Now().UnixNano()))
	log.Info().Str("combined_file", outputFileName).Str("log_tag", "chunk_cf").Msg("Combining batch into files")
	outfile, err := os.Create(outputFileName)
	if err != nil {
		log.Error().Err(err).Str("output_file", outputFileName).Str("log_tag", "chunk_cf").Msg("Unable to create output file")
		return "", fmt.Errorf("Unable to create output file %s: %w", outputFileName, err)
	}
	defer outfile.Close()

	for _, entry := range batch {

		// Check file size hasn't changed
		info, err := os.Stat(entry.Path)
		if err != nil {
			log.Warn().Err(err).Str("new_file", entry.Path).Str("log_tag", "chunk_cf").Msg("Could not stat file in file combined")
		} else if info.Size() != entry.Size {
			log.Error().Str("file", entry.Path).Int64("initial_size", entry.Size).Int64("current_size", info.Size()).Str("log_tag", "chunk_cf").
				Msg("File sizes do not match before combining")
		}

		inFile, err := os.Open(entry.Path)
		if err != nil {
			log.Error().Err(err).Str("path", entry.Path).Str("log_tag", "chunk_cf").Msg("Error opening file")
			return "", fmt.Errorf("Error opening file %s: %w", entry.Path, err)
		}

		_, err = io.Copy(outfile, inFile)
		inFile.Close()
		if err != nil {
			log.Error().Err(err).Str("path", entry.Path).Str("log_tag", "chunk_cf").Msg("Error copying from file")
			return "", fmt.Errorf("Error copying from file %s: %w", entry.Path, err)
		}
	}
	return outputFileName, nil
}

func isOverflow(err error) bool {
	if err == nil {
		return false
	}
	return err.Error() == "fsnotify: queue or buffer overflow"
}

// Processed Related functions

// isSeen checks both current and previous maps for deduplication
func (c *Chunker) isSeen(path string) bool {
	c.processed.RLock()
	defer c.processed.RUnlock()
	_, inCurrent := c.processed.current[path]
	if inCurrent {
		return true
	}
	_, inPrevious := c.processed.previous[path]
	return inPrevious
}

// markSeen adds a file to the current map
func (c *Chunker) markSeen(path string) {
	c.processed.Lock()
	defer c.processed.Unlock()
	c.processed.current[path] = struct{}{}
}

// rotateMap drops the oldest map and creates a fresh one to reclaim memory
func (c *Chunker) rotateMap() {
	c.processed.Lock()
	log.Info().
		Int("prev_size", len(c.processed.previous)).
		Int("curr_size", len(c.processed.current)).
		Msg("Rotating file tracker map to reclaim memory")

	// The old 'previous' is now eligible for GC
	c.processed.previous = c.processed.current
	c.processed.current = make(map[string]struct{})
	c.processed.Unlock()

	// Explicitly trigger a GC and Return memory to OS
	debug.FreeOSMemory()
}

func (c *Chunker) VerifyCleanup() {
	dirs := map[string]string{
		"Temp":    c.tempDir,
		"Watch":   c.watchDir,
		"Combine": c.combineDir,
	}

	log.Info().Str("log_tag", "chunk_vc").Msg("--- Final Cleanup Verification ---")
	for label, path := range dirs {
		entries, err := os.ReadDir(path)
		if err != nil {
			log.Error().Err(err).Str("dir", label).Msg("Failed to read directory during verification")
			continue
		}

		if len(entries) == 0 {
			log.Info().Str("dir", label).Str("log_tag", "chunk_vc").Msg("Directory is empty")
		} else {
			var fileNames []string
			for _, e := range entries {
				fileNames = append(fileNames, e.Name())
			}
			log.Warn().
				Str("log_tag", "chunk_vc").
				Str("dir", label).
				Int("count", len(entries)).
				Strs("files", fileNames).
				Msg("Verification Warning: Directory still contains files!")
		}
	}
	log.Info().Str("log_tag", "chunk_vc").Msg("--- Cleanup Verification Complete ---")
}

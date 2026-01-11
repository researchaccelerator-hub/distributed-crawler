package chunk

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
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
	processed    sync.Map // Map[string]time.Time to prevent duplicates
}

type FileEntry struct {
	Path string
	Size int64
}

type BatchState struct {
	Files []FileEntry
	Size  int64
}

func resetTimer(t *time.Timer, duration time.Duration) {
	if !t.Stop() {
		// Drain the channel to prevent an immediate, spurious wake-up.
		select {
		case <-t.C:
		default:
		}
	}
	// Reset the timer for the next interval.
	t.Reset(duration)
}

func NewChunker(sm state.StateManagementInterface, tempDir string, watchDir string, combineDir string, triggerSize int64, hardCapSize int64) *Chunker {
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

	return nil
}

func (c *Chunker) watchFilesWithInternalBuffer(out chan<- fsnotify.Event) {
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
	for event := range internalBuffer {
		// Only process writes/creates of jsonl files
		if (event.Has(fsnotify.Rename) || event.Has(fsnotify.Create)) && strings.HasSuffix(event.Name, ".jsonl") {

			// Mark as seen immediately to prevent recovery worker from picking it up
			c.processed.Store(event.Name, time.Now())

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
}

func (c *Chunker) recoveryWorker() {
	for range c.rescanSignal {
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

			// Deduplication check
			if _, seen := c.processed.Load(fullPath); seen {
				continue
			}

			c.fileChan <- FileEntry{
				Path: fullPath,
				Size: file.Size(),
			}
			c.processed.Store(fullPath, time.Now())
		}

		// Clean up the map: Remove entries older than 15 minutes to save memory
		c.processed.Range(func(key, value any) bool {
			if time.Since(value.(time.Time)) > 15*time.Minute {
				c.processed.Delete(key)
			}
			return true
		})

		log.Info().Str("log_tag", "chunk_rw").Msg("Recovery rescan complete.")
	}
}

func (c *Chunker) processBatches(out chan<- []FileEntry) {
	state := BatchState{}
	timer := time.NewTimer(c.batchTimeout)

	flush := func() {
		if len(state.Files) > 0 {
			batchToSend := make([]FileEntry, len(state.Files))
			copy(batchToSend, state.Files)
			out <- batchToSend
			state.Files = nil
			state.Size = 0
		}
		resetTimer(timer, c.batchTimeout)
	}

	for {
		select {
		case file := <-c.fileChan:
			resetTimer(timer, c.batchTimeout)

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
		case <-timer.C:
			if len(state.Files) > 0 {
				log.Info().Str("log_tag", "chunk_pb").Msg("Timeout reached. Flushing partial batch")
				flush()
			} else {
				resetTimer(timer, c.batchTimeout)
			}
		}
	}
}

func (c *Chunker) consumeBatches(jobs <-chan []FileEntry) {
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

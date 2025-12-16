package chunk

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/rs/zerolog/log"
)

type Chunker struct {
	sm           state.StateManagementInterface
	watchDir     string        // directory where crawl files are written
	combineDir   string        // directory where combined files are stored before upload
	triggerSize  int64         // size in mb to trigger an upload
	hardCapSize  int64         // size in mb to not exceed
	batchTimeout time.Duration // length in seconds to wait after last created file before flushing batch
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

func NewChunker(sm state.StateManagementInterface, watchDir string, combineDir string, triggerSize int64, hardCapSize int64) *Chunker {

	return &Chunker{
		sm:           sm,
		watchDir:     watchDir,
		combineDir:   combineDir,
		triggerSize:  triggerSize,
		hardCapSize:  hardCapSize,
		batchTimeout: 300 * time.Second, // 5 minutes
	}
}

func (c *Chunker) Start() error {
	if err := os.MkdirAll(c.watchDir, 0755); err != nil {
		return fmt.Errorf("failed to create watch directory: %s", c.watchDir)
	}
	if err := os.MkdirAll(c.combineDir, 0755); err != nil {
		return fmt.Errorf("failed to create combine directory: %s", c.combineDir)
	}

	fileChan := make(chan FileEntry, 1000)
	jobsChan := make(chan []FileEntry, 100)

	go c.watchFiles(fileChan)

	// Greedy Knapsack Heuristic -> jobsChan
	go c.processBatches(fileChan, jobsChan)

	//
	go c.consumeBatches(jobsChan)

	log.Info().Int64("trigger_mb", c.triggerSize/1024/1024).Int64("hardcap_mb", c.hardCapSize/1024/1024).Timestamp().Dur("timeout_seconds", c.batchTimeout/1000).
		Msg("Chunker started. Waiting for files")

	return nil
}

func (c *Chunker) watchFiles(out chan<- FileEntry) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal().Err(err).Msg("Chunk-WF: Failed to create watcher")
	}
	defer watcher.Close()

	if err := watcher.Add(c.watchDir); err != nil {
		log.Fatal().Err(err).Msg("Failed to add watch dir to watcher")
	}
	log.Info().Str("watch_dir", c.watchDir).Msg("Chunk-WF: Watching directory for crawl data")

	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				log.Error().Err(err).Msg("Chunk-FW: Encountered error in file watcher Events")
				return
			}
			if event.Op&fsnotify.Write == fsnotify.Write &&
				strings.HasSuffix(event.Name, ".jsonl") {

				info, err := os.Stat(event.Name)
				if err != nil {
					log.Warn().Err(err).Str("new_file", event.Name).Msg("Chunk-FW: Could not stat file. Skipping")
					continue
				}

				out <- FileEntry{Path: event.Name, Size: info.Size()}
				log.Info().Str("file_name", filepath.Base(event.Name)).Int64("file_size", info.Size()).Msg("Chunk-FW: New file entry added")
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				log.Error().Err(err).Msg("Chunk-FW: Encountered error in file watcher Errors. Not ok")
				return
			}
			log.Error().Err(err).Msg("Chunk-FW: Encountered error in file watcher Errors. Ok")
		}
	}
}

func (c *Chunker) processBatches(in <-chan FileEntry, out chan<- []FileEntry) {
	state := BatchState{}
	timer := time.NewTimer(c.batchTimeout)

	// Helper to send current batch to output when new files haven't been added
	flush := func() {
		if len(state.Files) > 0 {
			log.Info().Int("batch_count", len(state.Files)).Int64("total_bytes", state.Size).Msg("Chunk-PB: Flushing batch")

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
		case file := <-in:
			resetTimer(timer, c.batchTimeout)

			// delete files we can't upload
			if file.Size > c.hardCapSize {
				log.Warn().Str("file_name", file.Path).Int64("total_bytes", file.Size).Msg("Chunk-PB: File exceeds hard cap. Deleting")
				if err := os.Remove(file.Path); err != nil {
					log.Error().Err(err).Str("file_name", file.Path).Msg("Chunk-PB: failed to remove file")
				}
				continue
			}
			// flush the buffer when we hit the hard cap
			if state.Size > 0 && state.Size+file.Size > c.hardCapSize {
				log.Info().Msg("Chunk-PB: Hard cap forced flush")
				flush()
			}

			state.Files = append(state.Files, file)
			state.Size += file.Size
			log.Info().Int("file_count", len(state.Files)).Int64("buffer_size_bytes", state.Size).Msg("Chunk-PB: Current buffer")
			if state.Size >= c.triggerSize {
				log.Info().Msg("Chunk-PB: trigger size reached. Flushing batch")
				flush()
			}
		case <-timer.C:
			if len(state.Files) > 0 {
				log.Info().Msg("Chunk-PB: timeout reached. Flushing partial batch")
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
			log.Error().Err(err).Msg("Chunk-CB: failed to combine batch. Files not deleted")
			continue
		}

		info, err := os.Stat(outputName)
		if err != nil {
			log.Warn().Err(err).Str("combined_file", outputName).Msg("Chunk-CB: Could not stat file in file combined")
		}
		log.Info().Str("combined_file", outputName).Int64("total_bytes", info.Size()).Msg("Chunk-CB: Batch combined. Uploading to storage")

		err = c.sm.UploadCombinedFile(outputName)

		if err != nil {
			log.Error().Err(err).Msg("Chunk-CB: Failed to upload combined file")
			continue
		}

		log.Info().Str("combined_file", outputName).Msg("Chunk-CB: Upload completed. Deleting source files")

		for _, file := range batch {
			if err := os.Remove(file.Path); err != nil {
				log.Error().Err(err).Str("file_name", file.Path).Msg("Chunk-CB: failed to remove file")
			}
		}

		if err := os.Remove(outputName); err != nil {
			log.Error().Err(err).Str("file_name", outputName).Msg("Chunk-CB: failed to delete combined file")
		}

	}
}

func (c *Chunker) combineFiles(batch []FileEntry) (string, error) {
	outputFileName := fmt.Sprintf("%s/combined_%d.jsonl", c.combineDir, time.Now().UnixNano())
	log.Info().Str("combined_file", outputFileName).Msg("Chunk-CF: Combining batch into files")
	outfile, err := os.Create(outputFileName)
	if err != nil {
		return "", fmt.Errorf("Chunk-CF: unable to create output file %s: %w", outputFileName, err)
	}
	defer outfile.Close()

	// REMOVE: after testing for change to jsonl
	// if _, err := outfile.WriteString("["); err != nil {
	// 	return "", fmt.Errorf("Chunk-CF: error writing opening bracket: %w", err)
	// }

	for _, entry := range batch {

		// Check file size hasn't changed
		info, err := os.Stat(entry.Path)
		if err != nil {
			log.Warn().Err(err).Str("new_file", entry.Path).Msg("Chunk-CF: Could not stat file in file combined")
		} else if info.Size() != entry.Size {
			log.Error().Str("file", entry.Path).Int64("initial_size", entry.Size).Int64("current_size", info.Size()).Msg("Chunk-CF: File sizes do not match before combining")
		}

		inFile, err := os.Open(entry.Path)
		if err != nil {
			return "", fmt.Errorf("Chunk-CF: error opening file %s: %w", entry.Path, err)
		}

		_, err = io.Copy(outfile, inFile)
		inFile.Close()
		if err != nil {
			return "", fmt.Errorf("Chunk-CF: error copying from file %s: %w", entry.Path, err)
		}

		// // delim for jsonl
		// if _, err := outfile.WriteString("\n"); err != nil {
		// 	return "", fmt.Errorf("Chunk-CF: error writing newline: %w", err)
		// }

		// REMOVE: after testing for change to jsonl
		// if i < len(batch)-1 {
		// 	if _, err := outfile.WriteString(","); err != nil {
		// 		return "", fmt.Errorf("Chunk-CF: error writing comma: %w", err)
		// 	}
		// }
	}
	// REMOVE: after testing for change to jsonl
	// if _, err := outfile.WriteString("]"); err != nil {
	// 	return "", fmt.Errorf("Chunk-CF: error writing closing bracket: %w", err)
	// }
	return outputFileName, nil
}

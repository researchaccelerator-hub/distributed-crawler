package telegramhelper

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
	
	"github.com/rs/zerolog/log"
)

// FileCleaner manages a scheduled task to clean up old files
type FileCleaner struct {
	baseDir           string // Base directory containing conn_* folders
	targetSubPath     string // Subpath under each conn_* folder to check for files
	cleanupInterval   time.Duration
	fileAgeThreshold  time.Duration
	stopChan          chan struct{}
	wg                sync.WaitGroup
	isRunning         bool
	isRunningMutex    sync.Mutex
	connFolderPattern *regexp.Regexp
}

// NewFileCleaner creates a new file cleaner instance
func NewFileCleaner(baseDir, targetSubPath string, cleanupIntervalMinutes, fileAgeThresholdMinutes int) *FileCleaner {
	return &FileCleaner{
		baseDir:           baseDir,
		targetSubPath:     targetSubPath,
		cleanupInterval:   time.Duration(cleanupIntervalMinutes) * time.Minute,
		fileAgeThreshold:  time.Duration(fileAgeThresholdMinutes) * time.Minute,
		stopChan:          make(chan struct{}),
		connFolderPattern: regexp.MustCompile(`^conn_\d+`),
	}
}

// Start begins the file cleaning goroutine
func (fc *FileCleaner) Start() error {
	fc.isRunningMutex.Lock()
	defer fc.isRunningMutex.Unlock()

	if fc.isRunning {
		return fmt.Errorf("file cleaner is already running")
	}

	// Don't require the directory to exist at startup
	// It will be checked during each cleaning cycle instead

	fc.isRunning = true
	fc.wg.Add(1)

	go fc.cleaningLoop()

	log.Info().
		Str("base_dir", fc.baseDir).
		Str("path_pattern", filepath.Join("conn_*", fc.targetSubPath)).
		Float64("file_age_threshold_minutes", fc.fileAgeThreshold.Minutes()).
		Float64("cleanup_interval_minutes", fc.cleanupInterval.Minutes()).
		Msg("File cleaner started")

	return nil
}

// Stop terminates the file cleaning goroutine
func (fc *FileCleaner) Stop() {
	fc.isRunningMutex.Lock()
	defer fc.isRunningMutex.Unlock()

	if !fc.isRunning {
		return
	}

	close(fc.stopChan)
	fc.wg.Wait()
	fc.isRunning = false
	log.Info().Msg("File cleaner stopped")
}

// cleaningLoop runs the cleanup process at scheduled intervals
func (fc *FileCleaner) cleaningLoop() {
	defer fc.wg.Done()

	// Run cleanup immediately on start
	fc.cleanOldFiles()

	ticker := time.NewTicker(fc.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fc.cleanOldFiles()
		case <-fc.stopChan:
			return
		}
	}
}

// cleanOldFiles removes files older than the threshold
func (fc *FileCleaner) cleanOldFiles() {
	log.Debug().Msg("Starting file cleanup in connection folders")

	cutoffTime := time.Now().Add(-fc.fileAgeThreshold)
	var totalFileCount int

	// Check if base directory exists before proceeding
	if _, err := os.Stat(fc.baseDir); os.IsNotExist(err) {
		log.Info().Str("base_dir", fc.baseDir).Msg("Base directory does not exist yet, skipping cleanup")
		return
	}

	// Read base directory to find all conn_* folders
	entries, err := os.ReadDir(fc.baseDir)
	if err != nil {
		log.Error().Err(err).Str("base_dir", fc.baseDir).Msg("Error reading base directory")
		return
	}

	// Process each connection folder
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		// Check if the folder matches the conn_* pattern
		if !fc.connFolderPattern.MatchString(entry.Name()) {
			continue
		}

		connFolderPath := filepath.Join(fc.baseDir, entry.Name())
		targetDirPath := filepath.Join(connFolderPath, fc.targetSubPath)

		// Check if the target directory exists
		if _, err := os.Stat(targetDirPath); os.IsNotExist(err) {
			// Target directory doesn't exist in this conn folder
			log.Debug().Str("folder", entry.Name()).Msg("Target path doesn't exist, skipping")
			continue
		}

		// Process files in this connection's target directory
		fileCount := fc.cleanFilesInDir(targetDirPath, cutoffTime)
		totalFileCount += fileCount
		
		// Also check for files in .tdlib/database directory to ensure complete cleanup
		databasePath := filepath.Join(connFolderPath, ".tdlib", "database")
		if _, err := os.Stat(databasePath); err == nil {
			// Look for large files in database directory too
			dbFileCount := fc.cleanFilesInDir(databasePath, cutoffTime)
			if dbFileCount > 0 {
				log.Debug().
					Str("directory", databasePath).
					Int("files_cleaned", dbFileCount).
					Msg("Cleaned database directory files")
				totalFileCount += dbFileCount
			}
		}
	}

	if totalFileCount > 0 {
		log.Info().
			Int("files_cleaned", totalFileCount).
			Float64("age_threshold_minutes", fc.fileAgeThreshold.Minutes()).
			Msg("Completed file cleanup")
	} else {
		log.Debug().Msg("No files needed cleaning")
	}
}

// cleanFilesInDir removes old files from a specific directory
func (fc *FileCleaner) cleanFilesInDir(dirPath string, cutoffTime time.Time) int {
	var fileCount int

	log.Debug().Str("path", dirPath).Msg("Checking for old files")

	err := filepath.WalkDir(dirPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			log.Error().Err(err).Str("path", path).Msg("Error accessing path")
			return filepath.SkipDir
		}

		// Skip directories
		if d.IsDir() {
			return nil
		}

		// Get file info
		info, err := d.Info()
		if err != nil {
			log.Error().Err(err).Str("path", path).Msg("Error getting file info")
			return nil
		}

		// Check if file is older than threshold
		if info.ModTime().Before(cutoffTime) {
			if err := os.Remove(path); err != nil {
				log.Error().Err(err).Str("path", path).Msg("Failed to remove file")
			} else {
				fileAge := time.Since(info.ModTime()).Minutes()
				log.Debug().
					Str("path", path).
					Float64("age_minutes", fileAge).
					Msg("Removed old file")
				fileCount++
			}
		}

		return nil
	})

	if err != nil {
		log.Error().Err(err).Str("path", dirPath).Msg("Error walking directory")
	}

	return fileCount
}

// Example usage in a real crawler
func main() {
	// Example: clean files in dynamic connection folders
	cleaner := NewFileCleaner(
		"/CRAWLS/state",       // Base directory where conn_* folders are located
		".tdlib/files/videos", // Subpath under each conn_* folder to check
		5,                     // cleanup interval minutes
		15,                    // file age threshold minutes
	)

	if err := cleaner.Start(); err != nil {
		log.Fatal().Err(err).Msg("Failed to start file cleaner")
	}

	// Keep the main function running
	// In a real crawler, this would be part of your main application
	log.Info().Msg("File cleaner is running. Press Ctrl+C to stop.")

	// Set up channel for interrupt signal
	sigChan := make(chan os.Signal, 1)

	// Block until we receive a signal
	<-sigChan

	log.Info().Msg("Stopping file cleaner...")
	cleaner.Stop()
	log.Info().Msg("Exited cleanly.")
}
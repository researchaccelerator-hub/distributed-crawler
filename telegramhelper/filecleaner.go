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
	baseDir           string   // Base directory containing conn_* folders
	targetSubPaths    []string // Subpaths under each conn_* folder to check for files
	cleanupInterval   time.Duration
	fileAgeThreshold  time.Duration
	stopChan          chan struct{}
	wg                sync.WaitGroup
	isRunning         bool
	isRunningMutex    sync.Mutex
	connFolderPattern *regexp.Regexp
}

// NewFileCleaner creates a new file cleaner instance with multiple directories to clean
// If no targetSubPaths are provided, defaults to checking only ".tdlib/files/videos"
func NewFileCleaner(baseDir string, targetSubPaths []string, cleanupIntervalMinutes, fileAgeThresholdMinutes int) *FileCleaner {
	// Default to ".tdlib/files/videos" if no paths provided
	if len(targetSubPaths) == 0 {
		targetSubPaths = []string{".tdlib/files/videos"}
	}

	return &FileCleaner{
		baseDir:           baseDir,
		targetSubPaths:    targetSubPaths,
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
	// Check if base directory exists
	if _, err := os.Stat(fc.baseDir); os.IsNotExist(err) {
		return fmt.Errorf("base directory '%s' does not exist", fc.baseDir)
	}

	fc.isRunning = true
	fc.wg.Add(1)

	go fc.cleaningLoop()

	pathsLogger := log.Info().
		Str("base_dir", fc.baseDir).
		Float64("file_age_threshold_minutes", fc.fileAgeThreshold.Minutes()).
		Float64("cleanup_interval_minutes", fc.cleanupInterval.Minutes())

	// Add all target paths to the log message
	paths := make([]string, len(fc.targetSubPaths))
	for i, path := range fc.targetSubPaths {
		paths[i] = filepath.Join("conn_*", path)
	}
	pathsLogger.Strs("path_patterns", paths).Msg("File cleaner started")

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

// cleanOldFiles removes files older than the threshold from the specified subpaths
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
		folderFileCount := 0

		// Process each target subpath for this connection
		for _, subPath := range fc.targetSubPaths {
			targetDirPath := filepath.Join(connFolderPath, subPath)

			// Check if this target directory exists
			if _, err := os.Stat(targetDirPath); os.IsNotExist(err) {
				// This specific target path doesn't exist in this connection folder
				log.Debug().
					Str("folder", entry.Name()).
					Str("subpath", subPath).
					Msg("Target path doesn't exist in connection folder, skipping")
				continue
			}

			// Process files in this target directory
			fileCount := fc.cleanFilesInDir(targetDirPath, cutoffTime)
			if fileCount > 0 {
				log.Debug().
					Str("folder", entry.Name()).
					Str("subpath", subPath).
					Int("files_cleaned", fileCount).
					Msg("Cleaned files in directory")
			}

			folderFileCount += fileCount
		}

		// Add this connection folder's count to the total
		if folderFileCount > 0 {
			log.Info().
				Str("folder", entry.Name()).
				Int("files_cleaned", folderFileCount).
				Msg("Cleaned files in connection folder")
		}
		totalFileCount += folderFileCount
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

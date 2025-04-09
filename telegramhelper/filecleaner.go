package telegramhelper

import (
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
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
	logger            *log.Logger
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
		logger:            log.New(os.Stdout, "[FileCleaner] ", log.LstdFlags),
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

	// Check if directory exists
	if _, err := os.Stat(fc.baseDir); os.IsNotExist(err) {
		return fmt.Errorf("base directory '%s' does not exist", fc.baseDir)
	}

	fc.isRunning = true
	fc.wg.Add(1)

	go fc.cleaningLoop()

	fc.logger.Printf("File cleaner started for base directory: %s", fc.baseDir)
	fc.logger.Printf("Looking for files in path pattern: %s", filepath.Join("conn_*", fc.targetSubPath))
	fc.logger.Printf("Cleaning files older than %.1f minutes every %.1f minutes",
		fc.fileAgeThreshold.Minutes(), fc.cleanupInterval.Minutes())

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
	fc.logger.Println("File cleaner stopped")
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
	fc.logger.Printf("Starting file cleanup in connection folders")

	cutoffTime := time.Now().Add(-fc.fileAgeThreshold)
	var totalFileCount int

	// Read base directory to find all conn_* folders
	entries, err := os.ReadDir(fc.baseDir)
	if err != nil {
		fc.logger.Printf("Error reading base directory %s: %v", fc.baseDir, err)
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
			// Target directory doesn't exist in this conn folder, skip to next
			fc.logger.Printf("Target path doesn't exist in %s, skipping", entry.Name())
			continue
		}

		// Process files in this connection's target directory
		fileCount := fc.cleanFilesInDir(targetDirPath, cutoffTime)
		totalFileCount += fileCount
	}

	if totalFileCount > 0 {
		fc.logger.Printf("Cleaned up %d files older than %.1f minutes across all connection folders",
			totalFileCount, fc.fileAgeThreshold.Minutes())
	} else {
		fc.logger.Printf("No files needed cleaning")
	}
}

// cleanFilesInDir removes old files from a specific directory
func (fc *FileCleaner) cleanFilesInDir(dirPath string, cutoffTime time.Time) int {
	var fileCount int

	fc.logger.Printf("Checking for old files in: %s", dirPath)

	err := filepath.WalkDir(dirPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			fc.logger.Printf("Error accessing path %s: %v", path, err)
			return filepath.SkipDir
		}

		// Skip directories
		if d.IsDir() {
			return nil
		}

		// Get file info
		info, err := d.Info()
		if err != nil {
			fc.logger.Printf("Error getting file info for %s: %v", path, err)
			return nil
		}

		// Check if file is older than threshold
		if info.ModTime().Before(cutoffTime) {
			if err := os.Remove(path); err != nil {
				fc.logger.Printf("Failed to remove file %s: %v", path, err)
			} else {
				fileAge := time.Since(info.ModTime()).Minutes()
				fc.logger.Printf("Removed old file: %s (age: %.1f minutes)", path, fileAge)
				fileCount++
			}
		}

		return nil
	})

	if err != nil {
		fc.logger.Printf("Error walking directory %s: %v", dirPath, err)
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
		log.Fatalf("Failed to start file cleaner: %v", err)
	}

	// Keep the main function running
	// In a real crawler, this would be part of your main application
	fmt.Println("File cleaner is running. Press Ctrl+C to stop.")

	// Set up channel for interrupt signal
	sigChan := make(chan os.Signal, 1)

	// Block until we receive a signal
	<-sigChan

	fmt.Println("Stopping file cleaner...")
	cleaner.Stop()
	fmt.Println("Exited cleanly.")
}

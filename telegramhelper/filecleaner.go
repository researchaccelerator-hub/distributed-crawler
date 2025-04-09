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
	baseDir           string      // Base directory containing conn_* folders
	targetSubPaths    []string    // List of subpaths under each conn_* folder to check for files
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
// Takes a slice of target subpaths instead of a single string
func NewFileCleaner(baseDir string, targetSubPaths []string, cleanupIntervalMinutes, fileAgeThresholdMinutes int) *FileCleaner {
	// Basic validation: ensure at least one target subpath is provided
	if len(targetSubPaths) == 0 {
		// Handle this case as needed - maybe return an error or use a default?
		// For now, we'll log a warning but proceed. Consider returning an error.
		log.Printf("[FileCleaner-New] Warning: No target subpaths provided for base directory '%s'", baseDir)
	}
	return &FileCleaner{
		baseDir:           baseDir,
		targetSubPaths:    targetSubPaths, // Assign the slice here
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

	// Check if base directory exists
	if _, err := os.Stat(fc.baseDir); os.IsNotExist(err) {
		return fmt.Errorf("base directory '%s' does not exist", fc.baseDir)
	}

	fc.isRunning = true
	fc.wg.Add(1)

	go fc.cleaningLoop()

	fc.logger.Printf("File cleaner started for base directory: %s", fc.baseDir)
	fc.logger.Printf("Looking for files in subpaths: %v under conn_* folders", fc.targetSubPaths)
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

// cleanOldFiles removes files older than the threshold from the specified subpaths
func (fc *FileCleaner) cleanOldFiles() {
	fc.logger.Printf("Starting file cleanup cycle in connection folders")

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

		connFolderName := entry.Name()
		connFolderPath := filepath.Join(fc.baseDir, connFolderName)

		// --- Start Iterating over Target Subpaths ---
		for _, subPath := range fc.targetSubPaths {
			if subPath == "" { // Skip empty subpath entries
				continue
			}
			targetDirPath := filepath.Join(connFolderPath, subPath)

			// Check if the specific target directory exists
			if _, err := os.Stat(targetDirPath); os.IsNotExist(err) {
				// Target directory doesn't exist in this conn folder for this subPath, skip to next subPath
				//fc.logger.Printf("Target subpath '%s' not found in %s, skipping", subPath, connFolderName) // Optional: more detailed log
				continue // Skip to the next subPath in the list
			} else if err != nil {
				// Log other errors encountered during Stat
				fc.logger.Printf("Error checking target directory %s: %v", targetDirPath, err)
				continue // Skip to the next subPath
			}

			// Process files in this connection's specific target directory
			// Assume cleanFilesInDir exists and processes files in the given directory
			fileCount := fc.cleanFilesInDir(targetDirPath, cutoffTime)
			totalFileCount += fileCount
		}
		// --- End Iterating over Target Subpaths ---
	}

	if totalFileCount > 0 {
		fc.logger.Printf("Cleaned up %d files older than %.1f minutes across all connection folders and target subpaths",
			totalFileCount, fc.fileAgeThreshold.Minutes())
	} else {
		fc.logger.Printf("No files needed cleaning in specified subpaths")
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
		[".tdlib/files/videos", ".tdlib/files/documents"], // Subpaths under each conn_* folder to check
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

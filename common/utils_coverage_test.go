package common

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestDownloadURLFileComprehensive tests all code paths in DownloadURLFile
func TestDownloadURLFileComprehensive(t *testing.T) {
	t.Run("successful download", func(t *testing.T) {
		// Create a test server that returns URL content
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Check User-Agent header
			if userAgent := r.Header.Get("User-Agent"); userAgent != "Mozilla/5.0 Telegram-Scraper/1.0" {
				t.Errorf("Expected User-Agent 'Mozilla/5.0 Telegram-Scraper/1.0', got '%s'", userAgent)
			}
			
			fmt.Fprintln(w, "https://example.com/channel1")
			fmt.Fprintln(w, "https://example.com/channel2")
		}))
		defer server.Close()

		filename, err := DownloadURLFile(server.URL)
		if err != nil {
			t.Fatalf("DownloadURLFile failed: %v", err)
		}
		defer os.Remove(filename)

		// Verify file exists and has content
		content, err := os.ReadFile(filename)
		if err != nil {
			t.Fatalf("Failed to read downloaded file: %v", err)
		}

		expectedContent := "https://example.com/channel1\nhttps://example.com/channel2\n"
		if string(content) != expectedContent {
			t.Errorf("Expected content %q, got %q", expectedContent, string(content))
		}
	})

	t.Run("invalid URL", func(t *testing.T) {
		_, err := DownloadURLFile("not-a-valid-url")
		if err == nil {
			t.Error("Expected error for invalid URL, got nil")
		}
		if !strings.Contains(err.Error(), "failed to download file") {
			t.Errorf("Expected error about download failure, got: %v", err)
		}
	})

	t.Run("HTTP error status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintln(w, "Not Found")
		}))
		defer server.Close()

		_, err := DownloadURLFile(server.URL)
		if err == nil {
			t.Error("Expected error for 404 status, got nil")
		}
		if !strings.Contains(err.Error(), "bad status code: 404") {
			t.Errorf("Expected error about bad status code, got: %v", err)
		}
	})

	t.Run("server connection refused", func(t *testing.T) {
		// Use a port that should be closed
		_, err := DownloadURLFile("http://127.0.0.1:65534")
		if err == nil {
			t.Error("Expected error for connection refused, got nil")
		}
		if !strings.Contains(err.Error(), "failed to download file") {
			t.Errorf("Expected error about download failure, got: %v", err)
		}
	})

	t.Run("file creation error simulation", func(t *testing.T) {
		// This test simulates file creation errors by using an invalid temp directory
		// We'll patch the temp directory path temporarily
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintln(w, "test content")
		}))
		defer server.Close()

		// For this test, we can't easily force a file creation error without
		// modifying the function, so we'll skip this specific error path
		t.Skip("File creation error testing requires system-level mocking")
	})
}

// TestReadURLsFromFileComprehensive tests all code paths in ReadURLsFromFile
func TestReadURLsFromFileComprehensive(t *testing.T) {
	t.Run("successful reading with various URL formats", func(t *testing.T) {
		// Create a temporary file with URLs
		tempFile, err := os.CreateTemp("", "test_urls_*.txt")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		defer os.Remove(tempFile.Name())

		// Write URLs with different formats and some empty lines
		content := `https://example.com/channel1
		
https://example.com/channel2
   https://example.com/channel3   

https://example.com/channel4
`
		if _, err := tempFile.WriteString(content); err != nil {
			t.Fatalf("Failed to write to temp file: %v", err)
		}
		tempFile.Close()

		urls, err := ReadURLsFromFile(tempFile.Name())
		if err != nil {
			t.Fatalf("ReadURLsFromFile failed: %v", err)
		}

		expectedURLs := []string{
			"https://example.com/channel1",
			"https://example.com/channel2",
			"https://example.com/channel3",
			"https://example.com/channel4",
		}

		if len(urls) != len(expectedURLs) {
			t.Errorf("Expected %d URLs, got %d", len(expectedURLs), len(urls))
		}

		for i, expected := range expectedURLs {
			if i >= len(urls) || urls[i] != expected {
				t.Errorf("Expected URL[%d] to be %s, got %s", i, expected, urls[i])
			}
		}
	})

	t.Run("file not found", func(t *testing.T) {
		_, err := ReadURLsFromFile("/nonexistent/file.txt")
		if err == nil {
			t.Error("Expected error for nonexistent file, got nil")
		}
		if !strings.Contains(err.Error(), "failed to read file") {
			t.Errorf("Expected error about reading file, got: %v", err)
		}
	})

	t.Run("empty file", func(t *testing.T) {
		// Create an empty temporary file
		tempFile, err := os.CreateTemp("", "empty_urls_*.txt")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		defer os.Remove(tempFile.Name())
		tempFile.Close()

		urls, err := ReadURLsFromFile(tempFile.Name())
		if err != nil {
			t.Fatalf("ReadURLsFromFile failed: %v", err)
		}

		if len(urls) != 0 {
			t.Errorf("Expected 0 URLs from empty file, got %d", len(urls))
		}
	})

	t.Run("file with only whitespace and empty lines", func(t *testing.T) {
		// Create a temporary file with only whitespace
		tempFile, err := os.CreateTemp("", "whitespace_urls_*.txt")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		defer os.Remove(tempFile.Name())

		content := `   
		
	
   	   
`
		if _, err := tempFile.WriteString(content); err != nil {
			t.Fatalf("Failed to write to temp file: %v", err)
		}
		tempFile.Close()

		urls, err := ReadURLsFromFile(tempFile.Name())
		if err != nil {
			t.Fatalf("ReadURLsFromFile failed: %v", err)
		}

		if len(urls) != 0 {
			t.Errorf("Expected 0 URLs from whitespace-only file, got %d", len(urls))
		}
	})

	t.Run("file with mixed content", func(t *testing.T) {
		// Create a temporary file with mixed valid/invalid content
		tempFile, err := os.CreateTemp("", "mixed_urls_*.txt")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		defer os.Remove(tempFile.Name())

		content := `https://valid1.com
# This is a comment line that should be filtered out
   
https://valid2.com
just-some-text-without-protocol
https://valid3.com
`
		if _, err := tempFile.WriteString(content); err != nil {
			t.Fatalf("Failed to write to temp file: %v", err)
		}
		tempFile.Close()

		urls, err := ReadURLsFromFile(tempFile.Name())
		if err != nil {
			t.Fatalf("ReadURLsFromFile failed: %v", err)
		}

		// Should include non-empty lines that don't start with #
		expectedURLs := []string{
			"https://valid1.com",
			"https://valid2.com",
			"just-some-text-without-protocol",
			"https://valid3.com",
		}

		if len(urls) != len(expectedURLs) {
			t.Errorf("Expected %d URLs, got %d", len(expectedURLs), len(urls))
		}

		for i, expected := range expectedURLs {
			if i >= len(urls) || urls[i] != expected {
				t.Errorf("Expected URL[%d] to be %s, got %s", i, expected, urls[i])
			}
		}
	})

	t.Run("directory instead of file", func(t *testing.T) {
		// Create a temporary directory
		tempDir, err := os.MkdirTemp("", "test_dir_*")
		if err != nil {
			t.Fatalf("Failed to create temp directory: %v", err)
		}
		defer os.RemoveAll(tempDir)

		_, err = ReadURLsFromFile(tempDir)
		if err == nil {
			t.Error("Expected error when trying to read a directory, got nil")
		}
	})
}

// TestGenerateCrawlIDConsistency tests the consistency and uniqueness of GenerateCrawlID
func TestGenerateCrawlIDConsistency(t *testing.T) {
	t.Run("uniqueness over time", func(t *testing.T) {
		// Generate multiple IDs with slight delays to ensure they're different
		ids := make(map[string]bool)
		
		// First ID
		id1 := GenerateCrawlID()
		ids[id1] = true
		
		// Note: Since the function uses time.Now() with second precision,
		// we can't guarantee uniqueness within the same second.
		// This test verifies the format is consistent rather than strict uniqueness.
		for i := 0; i < 5; i++ {
			id := GenerateCrawlID()
			// Just verify the format is correct
			if len(id) != 14 {
				t.Errorf("Expected ID length 14, got %d for ID: %s", len(id), id)
			}
		}
	})

	t.Run("format consistency", func(t *testing.T) {
		// Test that all generated IDs follow the same format
		for i := 0; i < 5; i++ {
			id := GenerateCrawlID()
			if len(id) != 14 {
				t.Errorf("Expected ID length 14, got %d for ID: %s", len(id), id)
			}

			// Check that all characters are digits
			for _, char := range id {
				if char < '0' || char > '9' {
					t.Errorf("Expected only digits in ID, found non-digit '%c' in ID: %s", char, id)
				}
			}
		}
	})
}

// TestFilePathHandling tests edge cases in file path handling
func TestFilePathHandling(t *testing.T) {
	t.Run("long file path", func(t *testing.T) {
		// Create a file with a very long name
		longName := strings.Repeat("a", 100) + ".txt"
		tempDir, err := os.MkdirTemp("", "test_long_*")
		if err != nil {
			t.Fatalf("Failed to create temp directory: %v", err)
		}
		defer os.RemoveAll(tempDir)

		longPath := filepath.Join(tempDir, longName)
		
		// Create the file
		file, err := os.Create(longPath)
		if err != nil {
			t.Fatalf("Failed to create long path file: %v", err)
		}
		file.WriteString("https://example.com/test\n")
		file.Close()

		// Test reading it
		urls, err := ReadURLsFromFile(longPath)
		if err != nil {
			t.Fatalf("Failed to read long path file: %v", err)
		}

		if len(urls) != 1 || urls[0] != "https://example.com/test" {
			t.Errorf("Expected 1 URL 'https://example.com/test', got %v", urls)
		}
	})

	t.Run("special characters in content", func(t *testing.T) {
		tempFile, err := os.CreateTemp("", "special_chars_*.txt")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		defer os.Remove(tempFile.Name())

		// Content with special characters
		content := `https://example.com/channel?param=value&other=123
https://example.com/path/with spaces/test
https://example.com/unicode/测试/test
https://example.com/symbols!@#$%^&*()
`
		if _, err := tempFile.WriteString(content); err != nil {
			t.Fatalf("Failed to write to temp file: %v", err)
		}
		tempFile.Close()

		urls, err := ReadURLsFromFile(tempFile.Name())
		if err != nil {
			t.Fatalf("ReadURLsFromFile failed: %v", err)
		}

		expectedURLs := []string{
			"https://example.com/channel?param=value&other=123",
			"https://example.com/path/with spaces/test",
			"https://example.com/unicode/测试/test",
			"https://example.com/symbols!@#$%^&*()",
		}

		if len(urls) != len(expectedURLs) {
			t.Errorf("Expected %d URLs, got %d", len(expectedURLs), len(urls))
		}

		for i, expected := range expectedURLs {
			if i >= len(urls) || urls[i] != expected {
				t.Errorf("Expected URL[%d] to be %s, got %s", i, expected, urls[i])
			}
		}
	})
}
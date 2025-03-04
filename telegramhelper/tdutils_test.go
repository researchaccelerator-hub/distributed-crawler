package telegramhelper

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// Example demonstrates how to use the downloadAndExtractTarball function
// to download and extract a tarball from a URL to a target directory.
func Example_downloadAndExtractTarball() {
	// Step 1: Create a temporary directory for extraction
	targetDir, err := os.MkdirTemp("", "example-extract")
	if err != nil {
		fmt.Printf("Failed to create temp dir: %v\n", err)
		return
	}
	defer os.RemoveAll(targetDir) // Clean up after the example runs

	// Step 2: Create a mock HTTP server serving a known tarball
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Create an in-memory tar.gz archive
		var buf bytes.Buffer
		gzWriter := gzip.NewWriter(&buf)
		tarWriter := tar.NewWriter(gzWriter)

		// Add a test file
		fileContent := []byte("test content")
		fileHeader := &tar.Header{
			Name:     "testfile.txt",
			Mode:     0644,
			Size:     int64(len(fileContent)),
			Typeflag: tar.TypeReg,
		}
		if err := tarWriter.WriteHeader(fileHeader); err != nil {
			fmt.Printf("Failed to write tar header: %v\n", err)
			return
		}
		if _, err := tarWriter.Write(fileContent); err != nil {
			fmt.Printf("Failed to write file content: %v\n", err)
			return
		}

		// Close tar and gzip writers
		tarWriter.Close()
		gzWriter.Close()

		// Write tar.gz content to response
		w.Header().Set("Content-Type", "application/x-gzip")
		w.WriteHeader(http.StatusOK)
		w.Write(buf.Bytes())
	}))
	defer server.Close()

	// Step 3: Call function to download and extract
	err = downloadAndExtractTarball(server.URL, targetDir)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// Step 4: Verify extracted file exists
	extractedFilePath := filepath.Join(targetDir, "testfile.txt")
	_, err = os.Stat(extractedFilePath)
	if os.IsNotExist(err) {
		fmt.Printf("Error: Expected file not found at %s\n", extractedFilePath)
		return
	}

	fmt.Println("Successfully downloaded and extracted tarball")

	// Output:
	// Successfully downloaded and extracted tarball
}

func TestDownloadAndExtractTarballFromReader(t *testing.T) {
	// Create a mock tar.gz archive in memory
	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)
	tarWriter := tar.NewWriter(gzWriter)

	// Add a directory
	dirHeader := &tar.Header{
		Name:     "testdir",
		Mode:     0755,
		Typeflag: tar.TypeDir,
	}
	if err := tarWriter.WriteHeader(dirHeader); err != nil {
		t.Fatal(err)
	}

	// Add a file
	fileContent := []byte("test file content")
	fileHeader := &tar.Header{
		Name:     "testdir/testfile.txt",
		Mode:     0644,
		Size:     int64(len(fileContent)),
		Typeflag: tar.TypeReg,
	}
	if err := tarWriter.WriteHeader(fileHeader); err != nil {
		t.Fatal(err)
	}
	if _, err := tarWriter.Write(fileContent); err != nil {
		t.Fatal(err)
	}

	// Close writers
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gzWriter.Close(); err != nil {
		t.Fatal(err)
	}

	// Create a temporary directory for extraction
	tempDir, err := os.MkdirTemp("", "tarball-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Use the new function to extract from reader
	err = downloadAndExtractTarballFromReader(&buf, tempDir)
	if err != nil {
		t.Fatalf("downloadAndExtractTarballFromReader failed: %v", err)
	}

	// Verify that the file was extracted correctly
	extractedFilePath := filepath.Join(tempDir, "testdir/testfile.txt")
	content, err := os.ReadFile(extractedFilePath)
	if err != nil {
		t.Fatalf("Failed to read extracted file: %v", err)
	}

	if string(content) != "test file content" {
		t.Errorf("Unexpected file content: %s", content)
	}
}

func TestDownloadAndExtractTarball(t *testing.T) {
	// Create a test server that serves a mock tarball
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check headers
		userAgent := r.Header.Get("User-Agent")
		if userAgent != "Mozilla/5.0 (Windows NT 10.0; Win64; x64)" {
			t.Errorf("Expected User-Agent header, got: %s", userAgent)
		}

		// Create and serve a tarball
		var buf bytes.Buffer
		gzWriter := gzip.NewWriter(&buf)
		tarWriter := tar.NewWriter(gzWriter)

		// Add a directory
		dirHeader := &tar.Header{
			Name:     "testdir",
			Mode:     0755,
			Typeflag: tar.TypeDir,
		}
		if err := tarWriter.WriteHeader(dirHeader); err != nil {
			t.Fatal(err)
		}

		// Add a file
		fileContent := []byte("test file content")
		fileHeader := &tar.Header{
			Name:     "testdir/testfile.txt",
			Mode:     0644,
			Size:     int64(len(fileContent)),
			Typeflag: tar.TypeReg,
		}
		if err := tarWriter.WriteHeader(fileHeader); err != nil {
			t.Fatal(err)
		}
		if _, err := tarWriter.Write(fileContent); err != nil {
			t.Fatal(err)
		}

		// Close writers
		if err := tarWriter.Close(); err != nil {
			t.Fatal(err)
		}
		if err := gzWriter.Close(); err != nil {
			t.Fatal(err)
		}

		// Send response
		w.Header().Set("Content-Type", "application/x-gzip")
		w.WriteHeader(http.StatusOK)
		w.Write(buf.Bytes())
	}))
	defer server.Close()

	tempDir, err := os.MkdirTemp("", "tarball-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	err = downloadAndExtractTarball(server.URL, tempDir)
	if err != nil {
		t.Fatalf("downloadAndExtractTarball failed: %v", err)
	}

	extractedFilePath := filepath.Join(tempDir, "testdir/testfile.txt")
	content, err := os.ReadFile(extractedFilePath)
	if err != nil {
		t.Fatalf("Failed to read extracted file: %v", err)
	}

	if string(content) != "test file content" {
		t.Errorf("Unexpected file content: %s", content)
	}
}

func TestDownloadError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	tempDir, err := os.MkdirTemp("", "tarball-error-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	err = downloadAndExtractTarball(server.URL, tempDir)
	if err == nil {
		t.Error("Expected error for 404 response, got nil")
	}
}

func TestInvalidGzipFormat(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-gzip")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("This is not a valid gzip file"))
	}))
	defer server.Close()

	tempDir, err := os.MkdirTemp("", "tarball-invalid-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	err = downloadAndExtractTarball(server.URL, tempDir)
	if err == nil {
		t.Error("Expected error for invalid gzip data, got nil")
	}
}

func TestCorruptedTarFile(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		gzWriter := gzip.NewWriter(&buf)
		gzWriter.Write([]byte("Not a valid tar file"))
		gzWriter.Close()

		w.Header().Set("Content-Type", "application/x-gzip")
		w.WriteHeader(http.StatusOK)
		w.Write(buf.Bytes())
	}))
	defer server.Close()

	tempDir, err := os.MkdirTemp("", "tarball-corrupted-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	err = downloadAndExtractTarball(server.URL, tempDir)
	if err == nil {
		t.Error("Expected error for corrupted tar data, got nil")
	}
}

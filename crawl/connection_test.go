package crawl

import (
	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestConnectionCycle(t *testing.T) {
	// Skip this test by default since it requires Telegram credentials
	if os.Getenv("RUN_INTEGRATION_TESTS") != "true" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=true to run")
	}

	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "tdlib-test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	storageRoot := filepath.Join(tempDir, "storage")

	// Create a minimal config
	cfg := common.CrawlerConfig{
		DaprMode:         false,
		Concurrency:      1,
		Timeout:          30,
		StorageRoot:      storageRoot,
		TDLibDatabaseURL: "",
		MaxPages:         108000, // Default max pages
	}

	// First connection
	client, err := Connect(storageRoot, cfg)
	if err != nil {
		t.Fatalf("Failed to create first connection: %v", err)
	}

	// Test if the client works by calling GetMe
	me, err := client.GetMe()
	if err != nil {
		client.Close()
		t.Fatalf("Failed to get self user: %v", err)
	}
	assert.NotNil(t, me, "GetMe should return user object")
	
	// Close the connection
	client.Close()

	// Reconnect using the same storage path
	client2, err := Connect(storageRoot, cfg)
	if err != nil {
		t.Fatalf("Failed to create second connection: %v", err)
	}
	defer client2.Close()

	// Verify the second connection works
	me2, err := client2.GetMe()
	if err != nil {
		t.Fatalf("Failed to get self user on second connection: %v", err)
	}
	assert.NotNil(t, me2, "GetMe should return user object on second connection")
	
	// Verify the connection reuse worked by checking it returns the same user
	if me != nil && me2 != nil {
		assert.Equal(t, me.Id, me2.Id, "User ID should be the same across connections")
	}
}

func TestConnectionPool(t *testing.T) {
	// Skip this test by default since it requires Telegram credentials
	if os.Getenv("RUN_INTEGRATION_TESTS") != "true" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=true to run")
	}

	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "tdlib-pool-test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	storageRoot := filepath.Join(tempDir, "storage")

	// Create a config with multiple database URLs for pool testing
	cfg := common.CrawlerConfig{
		DaprMode:             false,
		Concurrency:          3, // Set concurrency to match URL count
		Timeout:              30,
		StorageRoot:          storageRoot,
		TDLibDatabaseURLs:    []string{"url1", "url2", "url3"}, // Test URLs
		MaxPages:             108000,
	}

	// Initialize connection pool
	InitConnectionPool(3, storageRoot, cfg)

	// Check if pool is initialized
	assert.True(t, IsConnectionPoolInitialized(), "Connection pool should be initialized")

	// Get connection pool stats
	stats := GetConnectionPoolStats()
	assert.NotNil(t, stats, "Connection pool stats should not be nil")
	
	// Test pool shutdown
	CloseConnectionPool()
	assert.False(t, IsConnectionPoolInitialized(), "Connection pool should be shut down")
}

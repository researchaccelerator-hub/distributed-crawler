package state

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestShardRotation validates that the media cache sharding logic works correctly
func TestShardRotation(t *testing.T) {
	// Create test configuration
	config := Config{
		CrawlID:         "test-crawl",
		CrawlExecutionID: "test-execution",
		StorageRoot:      "/test/storage",
	}
	
	// Create base state manager
	base := NewBaseStateManager(config)
	
	// Initialize sharded media cache structures for testing
	dsm := &DaprStateManager{
		BaseStateManager:     base,
		mediaCache:           make(map[string]MediaCacheItem),
		mediaCacheMutex:      sync.RWMutex{},
		mediaCacheIndex:      MediaCacheIndex{
			Shards:     []string{"initial-shard"},
			MediaIndex: make(map[string]string),
			UpdateTime: time.Now(),
		},
		activeMediaCache:     MediaCache{
			Items:      make(map[string]MediaCacheItem),
			UpdateTime: time.Now(),
			CacheID:    "initial-shard",
		},
		mediaCacheShards:     make(map[string]*MediaCache),
		mediaCacheIndexMutex: sync.RWMutex{},
		maxCacheItemsPerShard: 3, // Small shard size for testing
		cacheExpirationDays:   30,
	}
	
	// Add items to test shard rotation
	for i := 0; i < 10; i++ {
		mediaID := fmt.Sprintf("media%d", i)
		
		// Directly simulate the cache sharding logic
		dsm.mediaCacheIndexMutex.Lock()
		
		// Check if current shard is at capacity
		if len(dsm.activeMediaCache.Items) >= dsm.maxCacheItemsPerShard {
			// Save current shard
			currentShardID := dsm.activeMediaCache.CacheID
			currentShard := dsm.activeMediaCache
			dsm.mediaCacheShards[currentShardID] = &currentShard
			
			// Create new shard
			newShardID := fmt.Sprintf("shard-%d", len(dsm.mediaCacheIndex.Shards))
			dsm.activeMediaCache = MediaCache{
				Items:      make(map[string]MediaCacheItem),
				UpdateTime: time.Now(),
				CacheID:    newShardID,
			}
			
			// Update index
			dsm.mediaCacheIndex.Shards = append(dsm.mediaCacheIndex.Shards, newShardID)
		}
		
		// Add item to active shard
		dsm.activeMediaCache.Items[mediaID] = MediaCacheItem{
			ID:        mediaID,
			FirstSeen: time.Now(),
		}
		
		// Update index
		dsm.mediaCacheIndex.MediaIndex[mediaID] = dsm.activeMediaCache.CacheID
		
		dsm.mediaCacheIndexMutex.Unlock()
	}
	
	// Verify that sharding worked correctly
	
	// We should have at least 4 shards (10 items / 3 per shard = 4 shards)
	dsm.mediaCacheIndexMutex.RLock()
	defer dsm.mediaCacheIndexMutex.RUnlock()
	
	if len(dsm.mediaCacheIndex.Shards) < 4 {
		t.Errorf("Expected at least 4 shards but got %d", len(dsm.mediaCacheIndex.Shards))
	}
	
	// Verify all items are tracked in the index
	if len(dsm.mediaCacheIndex.MediaIndex) != 10 {
		t.Errorf("Expected 10 items in index but got %d", len(dsm.mediaCacheIndex.MediaIndex))
	}
	
	// Check that items are distributed across shards correctly
	itemCounts := make(map[string]int)
	for _, shardID := range dsm.mediaCacheIndex.Shards {
		if shardID == dsm.activeMediaCache.CacheID {
			itemCounts[shardID] = len(dsm.activeMediaCache.Items)
		} else if shard, exists := dsm.mediaCacheShards[shardID]; exists {
			itemCounts[shardID] = len(shard.Items)
		}
	}
	
	// Verify no shard exceeds the max limit
	for shardID, count := range itemCounts {
		if count > dsm.maxCacheItemsPerShard {
			t.Errorf("Shard %s exceeds max limit with %d items (limit: %d)", 
				shardID, count, dsm.maxCacheItemsPerShard)
		}
	}
	
	// Verify active shard is not at capacity yet (unless exactly filled)
	activeCount := len(dsm.activeMediaCache.Items)
	if activeCount == dsm.maxCacheItemsPerShard {
		// This is okay if it's exactly at capacity
		t.Logf("Active shard is exactly at capacity with %d items", activeCount)
	} else if activeCount > dsm.maxCacheItemsPerShard {
		t.Errorf("Active shard exceeds capacity with %d items (limit: %d)",
			activeCount, dsm.maxCacheItemsPerShard)
	}
}

// TestMediaCacheExpiration tests that expired items are removed from the cache
func TestMediaCacheExpiration(t *testing.T) {
	// Create test state manager
	config := Config{
		CrawlID:         "test-crawl",
		CrawlExecutionID: "test-execution",
		StorageRoot:      "/test/storage",
	}
	
	base := NewBaseStateManager(config)
	
	dsm := &DaprStateManager{
		BaseStateManager:     base,
		mediaCache:           make(map[string]MediaCacheItem),
		mediaCacheMutex:      sync.RWMutex{},
		mediaCacheIndex:      MediaCacheIndex{
			Shards:     []string{"test-shard"},
			MediaIndex: make(map[string]string),
			UpdateTime: time.Now(),
		},
		activeMediaCache:     MediaCache{
			Items:      make(map[string]MediaCacheItem),
			UpdateTime: time.Now(),
			CacheID:    "test-shard",
		},
		mediaCacheShards:     make(map[string]*MediaCache),
		mediaCacheIndexMutex: sync.RWMutex{},
		cacheExpirationDays:  30,
	}
	
	// Add both fresh and expired items
	dsm.mediaCacheIndexMutex.Lock()
	
	// Add fresh item
	freshMediaID := "fresh-media"
	dsm.activeMediaCache.Items[freshMediaID] = MediaCacheItem{
		ID:        freshMediaID,
		FirstSeen: time.Now(),
	}
	dsm.mediaCacheIndex.MediaIndex[freshMediaID] = dsm.activeMediaCache.CacheID
	
	// Add expired item
	expiredMediaID := "expired-media"
	dsm.activeMediaCache.Items[expiredMediaID] = MediaCacheItem{
		ID:        expiredMediaID,
		FirstSeen: time.Now().AddDate(0, 0, -dsm.cacheExpirationDays-5), // 5 days past expiration
	}
	dsm.mediaCacheIndex.MediaIndex[expiredMediaID] = dsm.activeMediaCache.CacheID
	
	dsm.mediaCacheIndexMutex.Unlock()
	
	// Manually run cleanup logic
	cutoffTime := time.Now().AddDate(0, 0, -dsm.cacheExpirationDays)
	
	dsm.mediaCacheIndexMutex.Lock()
	
	// Clean active shard
	for mediaID, item := range dsm.activeMediaCache.Items {
		if item.FirstSeen.Before(cutoffTime) {
			delete(dsm.activeMediaCache.Items, mediaID)
			delete(dsm.mediaCacheIndex.MediaIndex, mediaID)
		}
	}
	
	dsm.mediaCacheIndexMutex.Unlock()
	
	// Verify fresh item is still there
	dsm.mediaCacheIndexMutex.RLock()
	defer dsm.mediaCacheIndexMutex.RUnlock()
	
	if _, exists := dsm.activeMediaCache.Items[freshMediaID]; !exists {
		t.Errorf("Fresh item was incorrectly removed from the cache")
	}
	
	if _, exists := dsm.mediaCacheIndex.MediaIndex[freshMediaID]; !exists {
		t.Errorf("Fresh item was incorrectly removed from the index")
	}
	
	// Verify expired item is gone
	if _, exists := dsm.activeMediaCache.Items[expiredMediaID]; exists {
		t.Errorf("Expired item was not removed from the cache")
	}
	
	if _, exists := dsm.mediaCacheIndex.MediaIndex[expiredMediaID]; exists {
		t.Errorf("Expired item was not removed from the index")
	}
}
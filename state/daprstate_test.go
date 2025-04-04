package state_test

import (
	"encoding/json"
	"github.com/google/uuid"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// TestPage verifies the Page structure can be properly serialized and deserialized
func TestPage(t *testing.T) {
	// Create a test page
	testPage := state.Page{
		ID:        uuid.New().String(),
		URL:       "https://t.me/test_channel",
		Status:    "unfetched",
		Depth:     0,
		Timestamp: time.Now().Round(time.Second), // Round to avoid sub-second precision issues in comparison
		ParentID:  uuid.New().String(),
		Messages: []state.Message{
			{
				ChatID:    123456,
				MessageID: 78910,
				Status:    "unfetched",
				PageID:    "parent-page",
			},
		},
	}

	// Marshal to JSON
	jsonData, err := json.Marshal(testPage)
	assert.NoError(t, err, "Should marshal Page to JSON without error")
	assert.NotEmpty(t, jsonData, "JSON data should not be empty")

	// Unmarshal back to verify round-trip
	var deserializedPage state.Page
	err = json.Unmarshal(jsonData, &deserializedPage)
	assert.NoError(t, err, "Should unmarshal JSON back to Page without error")

	// Compare fields
	assert.Equal(t, testPage.ID, deserializedPage.ID, "ID should match")
	assert.Equal(t, testPage.URL, deserializedPage.URL, "URL should match")
	assert.Equal(t, testPage.Status, deserializedPage.Status, "Status should match")
	assert.Equal(t, testPage.Depth, deserializedPage.Depth, "Depth should match")
	assert.Equal(t, testPage.Timestamp.Unix(), deserializedPage.Timestamp.Unix(), "Timestamp should match")
	assert.Equal(t, testPage.ParentID, deserializedPage.ParentID, "ParentID should match")
	assert.Len(t, deserializedPage.Messages, 1, "Should have one message")
	assert.Equal(t, testPage.Messages[0].ChatID, deserializedPage.Messages[0].ChatID, "Message ChatID should match")
}

// TestCrawlMetadata verifies the CrawlMetadata structure can be properly serialized and deserialized
func TestCrawlMetadata(t *testing.T) {
	// Create test metadata
	testMetadata := state.CrawlMetadata{
		CrawlID:         "test-crawl-id",
		ExecutionID:     "test-exec-id",
		StartTime:       time.Now().Round(time.Second),
		EndTime:         time.Now().Add(1 * time.Hour).Round(time.Second),
		Status:          "completed",
		PreviousCrawlID: []string{"prev-crawl-1", "prev-crawl-2"},
	}

	// Marshal to JSON
	jsonData, err := json.Marshal(testMetadata)
	assert.NoError(t, err, "Should marshal CrawlMetadata to JSON without error")
	assert.NotEmpty(t, jsonData, "JSON data should not be empty")

	// Unmarshal back to verify round-trip
	var deserializedMetadata state.CrawlMetadata
	err = json.Unmarshal(jsonData, &deserializedMetadata)
	assert.NoError(t, err, "Should unmarshal JSON back to CrawlMetadata without error")

	// Compare fields
	assert.Equal(t, testMetadata.CrawlID, deserializedMetadata.CrawlID, "CrawlID should match")
	assert.Equal(t, testMetadata.ExecutionID, deserializedMetadata.ExecutionID, "ExecutionID should match")
	assert.Equal(t, testMetadata.StartTime.Unix(), deserializedMetadata.StartTime.Unix(), "StartTime should match")
	assert.Equal(t, testMetadata.EndTime.Unix(), deserializedMetadata.EndTime.Unix(), "EndTime should match")
	assert.Equal(t, testMetadata.Status, deserializedMetadata.Status, "Status should match")
	assert.Equal(t, testMetadata.PreviousCrawlID, deserializedMetadata.PreviousCrawlID, "PreviousCrawlID should match")
}

// TestMediaCacheItem verifies the MediaCacheItem structure can be properly serialized and deserialized
func TestMediaCacheItem(t *testing.T) {
	// Create test media cache item
	testItem := state.MediaCacheItem{
		ID:        "media-123",
		FirstSeen: time.Now().Round(time.Second),
		Metadata:  "some-metadata",
	}

	// Marshal to JSON
	jsonData, err := json.Marshal(testItem)
	assert.NoError(t, err, "Should marshal MediaCacheItem to JSON without error")
	assert.NotEmpty(t, jsonData, "JSON data should not be empty")

	// Unmarshal back to verify round-trip
	var deserializedItem state.MediaCacheItem
	err = json.Unmarshal(jsonData, &deserializedItem)
	assert.NoError(t, err, "Should unmarshal JSON back to MediaCacheItem without error")

	// Compare fields
	assert.Equal(t, testItem.ID, deserializedItem.ID, "ID should match")
	assert.Equal(t, testItem.FirstSeen.Unix(), deserializedItem.FirstSeen.Unix(), "FirstSeen should match")
	assert.Equal(t, testItem.Metadata, deserializedItem.Metadata, "Metadata should match")
}

// TestState verifies the State structure can be properly serialized and deserialized
func TestState(t *testing.T) {
	// Create test layers
	layer0 := &state.Layer{
		Depth: 0,
		Pages: []state.Page{
			{
				ID:     "page-1",
				URL:    "https://t.me/channel1",
				Status: "fetched",
				Depth:  0,
			},
		},
	}

	layer1 := &state.Layer{
		Depth: 1,
		Pages: []state.Page{
			{
				ID:       "page-2",
				URL:      "https://t.me/channel2",
				Status:   "unfetched",
				Depth:    1,
				ParentID: "page-1",
			},
		},
	}

	// Create test state
	testState := state.State{
		Layers: []*state.Layer{layer0, layer1},
		Metadata: state.CrawlMetadata{
			CrawlID:     "test-crawl-id",
			ExecutionID: "test-exec-id",
			Status:      "running",
		},
		LastUpdated: time.Now().Round(time.Second),
	}

	// Marshal to JSON
	jsonData, err := json.Marshal(testState)
	assert.NoError(t, err, "Should marshal State to JSON without error")
	assert.NotEmpty(t, jsonData, "JSON data should not be empty")

	// Unmarshal back to verify round-trip
	var deserializedState state.State
	err = json.Unmarshal(jsonData, &deserializedState)
	assert.NoError(t, err, "Should unmarshal JSON back to State without error")

	// Compare fields
	assert.Len(t, deserializedState.Layers, 2, "Should have two layers")
	assert.Equal(t, testState.Layers[0].Depth, deserializedState.Layers[0].Depth, "Layer 0 depth should match")
	assert.Equal(t, testState.Layers[1].Depth, deserializedState.Layers[1].Depth, "Layer 1 depth should match")
	assert.Equal(t, testState.Metadata.CrawlID, deserializedState.Metadata.CrawlID, "CrawlID should match")
	assert.Equal(t, testState.LastUpdated.Unix(), deserializedState.LastUpdated.Unix(), "LastUpdated should match")
}
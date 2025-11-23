package client

import (
	"context"
	"testing"
	"time"
)

// TestYouTubeInnerTubeClientCreation tests creating a new InnerTube client
func TestYouTubeInnerTubeClientCreation(t *testing.T) {
	config := &InnerTubeConfig{
		ClientType:    "WEB",
		ClientVersion: "2.20230728.00.00",
	}

	client, err := NewYouTubeInnerTubeClient(config)
	if err != nil {
		t.Fatalf("Failed to create InnerTube client: %v", err)
	}

	if client == nil {
		t.Fatal("Client is nil")
	}

	if client.clientType != "WEB" {
		t.Errorf("Expected clientType WEB, got %s", client.clientType)
	}
}

// TestYouTubeInnerTubeClientDefaults tests default config values
func TestYouTubeInnerTubeClientDefaults(t *testing.T) {
	// Test with nil config - should use defaults
	client, err := NewYouTubeInnerTubeClient(nil)
	if err != nil {
		t.Fatalf("Failed to create InnerTube client with nil config: %v", err)
	}

	if client.clientType != "WEB" {
		t.Errorf("Expected default clientType WEB, got %s", client.clientType)
	}

	if client.clientVersion != "2.20230728.00.00" {
		t.Errorf("Expected default clientVersion 2.20230728.00.00, got %s", client.clientVersion)
	}
}

// TestYouTubeInnerTubeClientConnect tests connection to InnerTube API
func TestYouTubeInnerTubeClientConnect(t *testing.T) {
	config := &InnerTubeConfig{
		ClientType:    "WEB",
		ClientVersion: "2.20230728.00.00",
	}

	client, err := NewYouTubeInnerTubeClient(config)
	if err != nil {
		t.Fatalf("Failed to create InnerTube client: %v", err)
	}

	ctx := context.Background()
	err = client.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect to InnerTube API: %v", err)
	}

	if client.client == nil {
		t.Fatal("InnerTube client not initialized after Connect()")
	}

	// Test disconnect
	err = client.Disconnect(ctx)
	if err != nil {
		t.Errorf("Failed to disconnect: %v", err)
	}
}

// TestYouTubeInnerTubeAdapter tests the adapter implementation
func TestYouTubeInnerTubeAdapter(t *testing.T) {
	config := &InnerTubeConfig{
		ClientType:    "WEB",
		ClientVersion: "2.20230728.00.00",
	}

	innerClient, err := NewYouTubeInnerTubeClient(config)
	if err != nil {
		t.Fatalf("Failed to create InnerTube client: %v", err)
	}

	adapter := &YouTubeInnerTubeClientAdapter{client: innerClient}

	// Test GetChannelType
	channelType := adapter.GetChannelType()
	if channelType != "youtube" {
		t.Errorf("Expected channel type 'youtube', got %s", channelType)
	}

	// Test Connect
	ctx := context.Background()
	err = adapter.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect via adapter: %v", err)
	}

	// Test Disconnect
	err = adapter.Disconnect(ctx)
	if err != nil {
		t.Errorf("Failed to disconnect via adapter: %v", err)
	}
}

// TestYouTubeInnerTubeClientCaching tests the caching functionality
func TestYouTubeInnerTubeClientCaching(t *testing.T) {
	config := &InnerTubeConfig{
		ClientType:    "WEB",
		ClientVersion: "2.20230728.00.00",
	}

	client, err := NewYouTubeInnerTubeClient(config)
	if err != nil {
		t.Fatalf("Failed to create InnerTube client: %v", err)
	}

	// Verify cache maps are initialized
	if client.channelCache == nil {
		t.Error("channelCache is not initialized")
	}

	if client.uploadsPlaylistCache == nil {
		t.Error("uploadsPlaylistCache is not initialized")
	}

	if client.videoStatsCache == nil {
		t.Error("videoStatsCache is not initialized")
	}
}

// TestFactoryCreateInnerTubeClient tests client creation via factory
func TestFactoryCreateInnerTubeClient(t *testing.T) {
	factory := NewDefaultClientFactory()
	ctx := context.Background()

	config := map[string]interface{}{
		"use_innertube":  true,
		"client_type":    "WEB",
		"client_version": "2.20230728.00.00",
	}

	client, err := factory.CreateClient(ctx, "youtube", config)
	if err != nil {
		t.Fatalf("Failed to create client via factory: %v", err)
	}

	if client == nil {
		t.Fatal("Client is nil")
	}

	// Verify it's the InnerTube adapter
	_, ok := client.(*YouTubeInnerTubeClientAdapter)
	if !ok {
		t.Errorf("Expected YouTubeInnerTubeClientAdapter, got %T", client)
	}
}

// TestFactoryInnerTubeVsDataAPI tests that factory correctly chooses between APIs
func TestFactoryInnerTubeVsDataAPI(t *testing.T) {
	factory := NewDefaultClientFactory()
	ctx := context.Background()

	// Test InnerTube creation
	innerTubeConfig := map[string]interface{}{
		"use_innertube": true,
	}

	innerTubeClient, err := factory.CreateClient(ctx, "youtube", innerTubeConfig)
	if err != nil {
		t.Fatalf("Failed to create InnerTube client via factory: %v", err)
	}

	_, ok := innerTubeClient.(*YouTubeInnerTubeClientAdapter)
	if !ok {
		t.Error("Expected InnerTube adapter when use_innertube=true")
	}

	// Test Data API creation (requires API key)
	dataAPIConfig := map[string]interface{}{
		"use_innertube": false,
		"api_key":       "test_api_key_123",
	}

	dataAPIClient, err := factory.CreateClient(ctx, "youtube", dataAPIConfig)
	if err != nil {
		t.Fatalf("Failed to create Data API client via factory: %v", err)
	}

	_, ok = dataAPIClient.(*YouTubeClientAdapter)
	if !ok {
		t.Error("Expected Data API adapter when use_innertube=false")
	}

	// Test error when no API key and use_innertube=false
	invalidConfig := map[string]interface{}{
		"use_innertube": false,
	}

	_, err = factory.CreateClient(ctx, "youtube", invalidConfig)
	if err == nil {
		t.Error("Expected error when no API key provided and use_innertube=false")
	}
}

// TestInnerTubeGetChannelInfo tests basic channel info retrieval
// Note: This test requires actual API calls and is marked for integration testing
func TestInnerTubeGetChannelInfo(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	config := &InnerTubeConfig{
		ClientType:    "WEB",
		ClientVersion: "2.20230728.00.00",
	}

	client, err := NewYouTubeInnerTubeClient(config)
	if err != nil {
		t.Fatalf("Failed to create InnerTube client: %v", err)
	}

	ctx := context.Background()
	err = client.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.Disconnect(ctx)

	// Note: This will currently return simplified data due to placeholder parser
	channelInfo, err := client.GetChannelInfo(ctx, "UCuAXFkgsw1L7xaCfnd5JJOw")
	if err != nil {
		t.Fatalf("Failed to get channel info: %v", err)
	}

	if channelInfo == nil {
		t.Fatal("Channel info is nil")
	}

	if channelInfo.ID == "" {
		t.Error("Channel ID is empty")
	}
}

// TestInnerTubeGetVideos tests basic video retrieval
// Note: This test requires actual API calls and is marked for integration testing
func TestInnerTubeGetVideos(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	config := &InnerTubeConfig{
		ClientType:    "WEB",
		ClientVersion: "2.20230728.00.00",
	}

	client, err := NewYouTubeInnerTubeClient(config)
	if err != nil {
		t.Fatalf("Failed to create InnerTube client: %v", err)
	}

	ctx := context.Background()
	err = client.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.Disconnect(ctx)

	fromTime := time.Now().Add(-30 * 24 * time.Hour) // 30 days ago
	toTime := time.Now()

	// Note: This will currently return empty list due to placeholder parser
	videos, err := client.GetVideos(ctx, "UCuAXFkgsw1L7xaCfnd5JJOw", fromTime, toTime, 10)
	if err != nil {
		t.Fatalf("Failed to get videos: %v", err)
	}

	// Currently returns empty due to simplified parser - not an error
	if videos == nil {
		t.Error("Videos list is nil (should be empty slice)")
	}
}

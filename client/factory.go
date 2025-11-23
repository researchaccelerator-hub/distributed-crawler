package client

import (
	"context"
	"fmt"
)

// ClientFactory creates clients based on the platform type
type ClientFactory interface {
	// CreateClient creates a client for the specified platform
	CreateClient(ctx context.Context, platformType string, config map[string]interface{}) (Client, error)
}

// DefaultClientFactory implements ClientFactory
type DefaultClientFactory struct {}

// NewDefaultClientFactory creates a new DefaultClientFactory
func NewDefaultClientFactory() *DefaultClientFactory {
	return &DefaultClientFactory{}
}

// CreateClient implements ClientFactory
func (f *DefaultClientFactory) CreateClient(ctx context.Context, platformType string, config map[string]interface{}) (Client, error) {
	switch platformType {
	case "telegram":
		return NewTelegramClient(config)
	case "youtube":
		// Check if we should use InnerTube instead of Data API
		useInnerTube := getConfigBool(config, "use_innertube", false)

		if useInnerTube {
			// Use InnerTube API (no API key required)
			innerTubeConfig := &InnerTubeConfig{
				ClientType:    getConfigString(config, "client_type", "WEB"),
				ClientVersion: getConfigString(config, "client_version", "2.20230728.00.00"),
			}
			client, err := NewYouTubeInnerTubeClient(innerTubeConfig)
			if err != nil {
				return nil, fmt.Errorf("failed to create InnerTube client: %w", err)
			}
			return &YouTubeInnerTubeClientAdapter{client: client}, nil
		} else {
			// Use YouTube Data API (requires API key)
			apiKey, ok := config["api_key"].(string)
			if !ok || apiKey == "" {
				return nil, fmt.Errorf("youtube client requires api_key in config (or set use_innertube=true)")
			}
			return NewYouTubeClientAdapter(apiKey)
		}
	case "bluesky":
		// Extract Bluesky configuration
		blueskyConfig := BlueskyConfig{
			JetStreamURL:      getConfigString(config, "jetstream_url", "wss://jetstream2.us-east.bsky.network/subscribe"),
			WantedCollections: getConfigStringSlice(config, "collections", nil),
			WantedDids:        getConfigStringSlice(config, "dids", nil),
			BufferSize:        getConfigInt(config, "buffer_size", 10000),
		}

		// Handle cursor if provided
		if cursorVal, ok := config["cursor"]; ok {
			if cursor, ok := cursorVal.(int64); ok {
				blueskyConfig.Cursor = &cursor
			}
		}

		return NewBlueskyClient(blueskyConfig)
	default:
		return nil, fmt.Errorf("unsupported platform type: %s", platformType)
	}
}

// Helper functions for config extraction
func getConfigString(config map[string]interface{}, key, defaultValue string) string {
	if val, ok := config[key].(string); ok {
		return val
	}
	return defaultValue
}

func getConfigStringSlice(config map[string]interface{}, key string, defaultValue []string) []string {
	if val, ok := config[key].([]string); ok {
		return val
	}
	// Also try []interface{} and convert
	if val, ok := config[key].([]interface{}); ok {
		result := make([]string, 0, len(val))
		for _, v := range val {
			if str, ok := v.(string); ok {
				result = append(result, str)
			}
		}
		return result
	}
	return defaultValue
}

func getConfigInt(config map[string]interface{}, key string, defaultValue int) int {
	if val, ok := config[key].(int); ok {
		return val
	}
	return defaultValue
}

func getConfigBool(config map[string]interface{}, key string, defaultValue bool) bool {
	if val, ok := config[key].(bool); ok {
		return val
	}
	return defaultValue
}
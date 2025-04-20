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
		apiKey, ok := config["api_key"].(string)
		if !ok || apiKey == "" {
			return nil, fmt.Errorf("youtube client requires api_key in config")
		}
		return NewYouTubeClientAdapter(apiKey)
	default:
		return nil, fmt.Errorf("unsupported platform type: %s", platformType)
	}
}
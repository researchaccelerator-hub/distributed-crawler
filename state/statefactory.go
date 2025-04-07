package state

import (
	"fmt"
	"github.com/rs/zerolog/log"
)

// NewStateManagerFactory is a function that returns a new state manager factory.
// This is designed to be easily mocked in tests.
var NewStateManagerFactory = func() StateManagerFactory {
	return &DefaultStateManagerFactory{}
}

// DefaultStateManagerFactory is the default implementation of StateManagerFactory
// It creates state manager instances based on the provided configuration.
type DefaultStateManagerFactory struct{}

// Create returns a state manager implementation based on the configuration
func (f *DefaultStateManagerFactory) Create(config Config) (StateManagementInterface, error) {
	log.Debug().
		Interface("config", config).
		Msg("Creating new state manager")
		
	// Check for DAPR configuration
	if config.DaprConfig != nil {
		log.Info().
			Str("crawl_id", config.CrawlID).
			Msg("Creating DAPR state manager")
		return NewDaprStateManager(config)
	}
	
	// Check for Azure configuration
	if config.AzureConfig != nil && config.AzureConfig.AccountURL != "" {
		// AzureStateManager is commented out in the codebase currently
		return nil, fmt.Errorf("Azure state manager is not implemented yet")
	}
	
	// Check for local filesystem configuration
	if config.LocalConfig != nil && config.LocalConfig.BasePath != "" {
		log.Info().
			Str("base_path", config.LocalConfig.BasePath).
			Str("crawl_id", config.CrawlID).
			Msg("Creating local filesystem state manager")
		// Create a new local state manager
		return NewLocalStateManager(config)
	}
	
	// Use Dapr as the default when no specific configuration is provided
	log.Warn().Msg("No specific configuration found, defaulting to DAPR state manager")
	return NewDaprStateManager(config)
}

package state

import (
	"github.com/rs/zerolog/log"
)

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
			Str("dapr_storage_component", config.DaprConfig.StorageComponent).
			Str("crawl_id", config.CrawlID).
			Msg("Creating DAPR state manager")
		return NewDaprStateManager(config)
	}
	
	//
	//// Check for Azure configuration
	//if config.AzureConfig != nil && config.AzureConfig.AccountURL != "" {
	//	return NewAzureStateManager(config)
	//}
	//
	//// Default to local filesystem storage
	//return NewLocalStateManager(config)
	
	log.Warn().Msg("No specific configuration found, defaulting to DAPR state manager")
	return NewDaprStateManager(config)
}

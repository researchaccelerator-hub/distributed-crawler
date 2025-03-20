package state

// DefaultStateManagerFactory is the default implementation of StateManagerFactory
type DefaultStateManagerFactory struct{}

// Create returns a state manager implementation based on the configuration
func (f *DefaultStateManagerFactory) Create(config Config) (StateManagementInterface, error) {
	// Check for DAPR configuration
	if config.DaprConfig != nil {
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
	return NewDaprStateManager(config)
}

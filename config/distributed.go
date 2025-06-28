// Package config provides configuration structures for distributed crawling
package config

import (
	"fmt"
	"time"
)

// DistributedConfig holds configuration for distributed crawling mode
type DistributedConfig struct {
	// Execution mode configuration
	Mode     string `yaml:"mode" json:"mode"`         // "orchestrator", "worker", "standalone", "dapr-standalone"
	WorkerID string `yaml:"worker_id" json:"worker_id,omitempty"` // Required for worker mode

	// Worker configuration
	MaxWorkersPerNode  int           `yaml:"max_workers_per_node" json:"max_workers_per_node"`   // Max concurrent workers per node
	WorkQueueSize      int           `yaml:"work_queue_size" json:"work_queue_size"`             // Size of work queue buffer
	ResultBufferSize   int           `yaml:"result_buffer_size" json:"result_buffer_size"`       // Size of result buffer
	HeartbeatInterval  time.Duration `yaml:"heartbeat_interval" json:"heartbeat_interval"`       // Worker heartbeat frequency
	WorkTimeout        time.Duration `yaml:"work_timeout" json:"work_timeout"`                   // Timeout for individual work items
	RetryAttempts      int           `yaml:"retry_attempts" json:"retry_attempts"`               // Number of retry attempts for failed work
	RetryDelay         time.Duration `yaml:"retry_delay" json:"retry_delay"`                     // Delay between retries

	// Orchestrator configuration
	WorkDistributionInterval time.Duration `yaml:"work_distribution_interval" json:"work_distribution_interval"` // How often to check for new work
	HealthCheckInterval      time.Duration `yaml:"health_check_interval" json:"health_check_interval"`           // How often to check worker health
	WorkerTimeout            time.Duration `yaml:"worker_timeout" json:"worker_timeout"`                         // Time to consider worker as failed
	MaxConcurrentWork        int           `yaml:"max_concurrent_work" json:"max_concurrent_work"`               // Max work items in flight

	// Dapr configuration
	DaprConfig DaprDistributedConfig `yaml:"dapr" json:"dapr"`
}

// DaprDistributedConfig holds Dapr-specific configuration for distributed mode
type DaprDistributedConfig struct {
	// PubSub configuration
	PubSubComponent string `yaml:"pubsub_component" json:"pubsub_component"` // Name of Dapr pubsub component
	
	// Topic names
	WorkQueueTopic       string `yaml:"work_queue_topic" json:"work_queue_topic"`             // Topic for work distribution
	ResultsTopic         string `yaml:"results_topic" json:"results_topic"`                   // Topic for work results
	WorkerStatusTopic    string `yaml:"worker_status_topic" json:"worker_status_topic"`       // Topic for worker status updates
	OrchestratorTopic    string `yaml:"orchestrator_topic" json:"orchestrator_topic"`         // Topic for orchestrator commands

	// State store configuration
	StateStore string `yaml:"state_store" json:"state_store"` // Name of Dapr state store component

	// Message configuration
	MessageTTL      time.Duration `yaml:"message_ttl" json:"message_ttl"`           // Time to live for messages
	MessagePriority int           `yaml:"message_priority" json:"message_priority"` // Default message priority
}

// DefaultDistributedConfig returns a configuration with sensible defaults
func DefaultDistributedConfig() *DistributedConfig {
	return &DistributedConfig{
		Mode:                     "",  // Auto-detect from CLI flags
		MaxWorkersPerNode:        4,
		WorkQueueSize:            1000,
		ResultBufferSize:         1000,
		HeartbeatInterval:        30 * time.Second,
		WorkTimeout:              10 * time.Minute,
		RetryAttempts:            3,
		RetryDelay:               5 * time.Second,
		WorkDistributionInterval: 5 * time.Second,
		HealthCheckInterval:      60 * time.Second,
		WorkerTimeout:            3 * time.Minute,
		MaxConcurrentWork:        100,
		DaprConfig: DaprDistributedConfig{
			PubSubComponent:      "pubsub",
			WorkQueueTopic:       "crawl-work-queue",
			ResultsTopic:         "crawl-results",
			WorkerStatusTopic:    "worker-status",
			OrchestratorTopic:    "orchestrator-commands",
			StateStore:           "statestore",
			MessageTTL:           1 * time.Hour,
			MessagePriority:      5,
		},
	}
}

// Validate checks if the configuration is valid
func (c *DistributedConfig) Validate() error {
	// Mode validation
	validModes := map[string]bool{
		"":                true, // Empty for legacy auto-detection
		"standalone":      true,
		"dapr-standalone": true,
		"orchestrator":    true,
		"worker":          true,
	}
	
	if !validModes[c.Mode] {
		return fmt.Errorf("invalid mode '%s', must be one of: standalone, dapr-standalone, orchestrator, worker", c.Mode)
	}

	// Worker mode requires worker ID
	if c.Mode == "worker" && c.WorkerID == "" {
		return fmt.Errorf("worker mode requires worker_id to be specified")
	}

	// Validate numeric values
	if c.MaxWorkersPerNode < 1 {
		return fmt.Errorf("max_workers_per_node must be at least 1")
	}

	if c.WorkQueueSize < 1 {
		return fmt.Errorf("work_queue_size must be at least 1")
	}

	if c.ResultBufferSize < 1 {
		return fmt.Errorf("result_buffer_size must be at least 1")
	}

	if c.RetryAttempts < 0 {
		return fmt.Errorf("retry_attempts cannot be negative")
	}

	if c.MaxConcurrentWork < 1 {
		return fmt.Errorf("max_concurrent_work must be at least 1")
	}

	// Validate time durations
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("heartbeat_interval must be positive")
	}

	if c.WorkTimeout <= 0 {
		return fmt.Errorf("work_timeout must be positive")
	}

	if c.WorkerTimeout <= 0 {
		return fmt.Errorf("worker_timeout must be positive")
	}

	// Validate Dapr configuration
	if c.DaprConfig.PubSubComponent == "" {
		return fmt.Errorf("dapr.pubsub_component cannot be empty")
	}

	if c.DaprConfig.StateStore == "" {
		return fmt.Errorf("dapr.state_store cannot be empty")
	}

	return nil
}

// IsDistributedMode returns true if this is a distributed mode (orchestrator or worker)
func (c *DistributedConfig) IsDistributedMode() bool {
	return c.Mode == "orchestrator" || c.Mode == "worker"
}

// IsOrchestratorMode returns true if this is orchestrator mode
func (c *DistributedConfig) IsOrchestratorMode() bool {
	return c.Mode == "orchestrator"
}

// IsWorkerMode returns true if this is worker mode
func (c *DistributedConfig) IsWorkerMode() bool {
	return c.Mode == "worker"
}

// GetTopicNames returns all topic names for easier iteration
func (c *DistributedConfig) GetTopicNames() []string {
	return []string{
		c.DaprConfig.WorkQueueTopic,
		c.DaprConfig.ResultsTopic,
		c.DaprConfig.WorkerStatusTopic,
		c.DaprConfig.OrchestratorTopic,
	}
}
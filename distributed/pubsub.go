// Package distributed provides Dapr PubSub abstraction for distributed crawling
package distributed

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	daprc "github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"
	daprs "github.com/dapr/go-sdk/service/grpc"
	"github.com/rs/zerolog/log"
)

// PubSubClient provides an abstraction over Dapr PubSub functionality
type PubSubClient struct {
	daprClient    daprc.Client
	pubsubName    string
	appPort       string
	subscriptions map[string]func(context.Context, *common.TopicEvent) (retry bool, err error)
}

// NewPubSubClient creates a new PubSub client
func NewPubSubClient(pubsubName, appPort string) (*PubSubClient, error) {
	daprClient, err := daprc.NewClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create Dapr client: %w", err)
	}

	return &PubSubClient{
		daprClient:    daprClient,
		pubsubName:    pubsubName,
		appPort:       appPort,
		subscriptions: make(map[string]func(context.Context, *common.TopicEvent) (retry bool, err error)),
	}, nil
}

// Close closes the PubSub client
func (p *PubSubClient) Close() error {
	if p.daprClient != nil {
		p.daprClient.Close()
	}
	return nil
}

// PublishWorkItem publishes a work item to the work queue
func (p *PubSubClient) PublishWorkItem(ctx context.Context, item WorkItem, priority int, ttlSeconds int) error {
	message := NewWorkQueueMessage(item, priority, ttlSeconds)
	
	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal work queue message: %w", err)
	}

	err = p.daprClient.PublishEvent(ctx, p.pubsubName, TopicWorkQueue, data)
	if err != nil {
		return fmt.Errorf("failed to publish work item: %w", err)
	}

	log.Debug().
		Str("work_item_id", item.ID).
		Str("url", item.URL).
		Int("priority", priority).
		Str("trace_id", message.TraceID).
		Msg("Published work item to queue")

	return nil
}

// PublishResult publishes a work result
func (p *PubSubClient) PublishResult(ctx context.Context, result WorkResult, discoveredPages []DiscoveredPage) error {
	message := NewResultMessage(result, discoveredPages)
	
	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal result message: %w", err)
	}

	err = p.daprClient.PublishEvent(ctx, p.pubsubName, TopicResults, data)
	if err != nil {
		return fmt.Errorf("failed to publish result: %w", err)
	}

	log.Debug().
		Str("work_item_id", result.WorkItemID).
		Str("worker_id", result.WorkerID).
		Str("status", result.Status).
		Int("discovered_count", len(discoveredPages)).
		Str("trace_id", message.TraceID).
		Msg("Published work result")

	return nil
}

// PublishStatus publishes a worker status message
func (p *PubSubClient) PublishStatus(ctx context.Context, status StatusMessage) error {
	data, err := json.Marshal(status)
	if err != nil {
		return fmt.Errorf("failed to marshal status message: %w", err)
	}

	err = p.daprClient.PublishEvent(ctx, p.pubsubName, TopicWorkerStatus, data)
	if err != nil {
		return fmt.Errorf("failed to publish status: %w", err)
	}

	log.Debug().
		Str("worker_id", status.WorkerID).
		Str("message_type", status.MessageType).
		Str("status", status.Status).
		Str("trace_id", status.TraceID).
		Msg("Published worker status")

	return nil
}

// PublishControl publishes a control message
func (p *PubSubClient) PublishControl(ctx context.Context, command, targetID string, parameters map[string]interface{}) error {
	message := ControlMessage{
		MessageType: command,
		Command:     command,
		TargetID:    targetID,
		Parameters:  parameters,
		Timestamp:   time.Now(),
		TraceID:     generateTraceID(),
	}
	
	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal control message: %w", err)
	}

	err = p.daprClient.PublishEvent(ctx, p.pubsubName, TopicOrchestrator, data)
	if err != nil {
		return fmt.Errorf("failed to publish control message: %w", err)
	}

	log.Info().
		Str("command", command).
		Str("target_id", targetID).
		Str("trace_id", message.TraceID).
		Msg("Published control message")

	return nil
}

// SubscribeToWorkQueue subscribes to the work queue topic
func (p *PubSubClient) SubscribeToWorkQueue(handler func(context.Context, WorkQueueMessage) error) {
	p.subscriptions[TopicWorkQueue] = func(ctx context.Context, e *common.TopicEvent) (retry bool, err error) {
		log.Debug().
			Str("topic", e.Topic).
			Str("pubsub_name", e.PubsubName).
			Msg("Received work queue message")

		var message WorkQueueMessage
		if err := json.Unmarshal(e.RawData, &message); err != nil {
			log.Error().Err(err).Msg("Failed to unmarshal work queue message")
			return false, err // Don't retry on unmarshal errors
		}

		if err := handler(ctx, message); err != nil {
			log.Error().
				Err(err).
				Str("work_item_id", message.WorkItem.ID).
				Str("trace_id", message.TraceID).
				Msg("Failed to handle work queue message")
			return true, err // Retry on handler errors
		}

		return false, nil
	}
}

// SubscribeToResults subscribes to the results topic
func (p *PubSubClient) SubscribeToResults(handler func(context.Context, ResultMessage) error) {
	p.subscriptions[TopicResults] = func(ctx context.Context, e *common.TopicEvent) (retry bool, err error) {
		log.Debug().
			Str("topic", e.Topic).
			Str("pubsub_name", e.PubsubName).
			Msg("Received result message")

		var message ResultMessage
		if err := json.Unmarshal(e.RawData, &message); err != nil {
			log.Error().Err(err).Msg("Failed to unmarshal result message")
			return false, err // Don't retry on unmarshal errors
		}

		if err := handler(ctx, message); err != nil {
			log.Error().
				Err(err).
				Str("work_item_id", message.WorkResult.WorkItemID).
				Str("trace_id", message.TraceID).
				Msg("Failed to handle result message")
			return true, err // Retry on handler errors
		}

		return false, nil
	}
}

// SubscribeToStatus subscribes to the worker status topic
func (p *PubSubClient) SubscribeToStatus(handler func(context.Context, StatusMessage) error) {
	p.subscriptions[TopicWorkerStatus] = func(ctx context.Context, e *common.TopicEvent) (retry bool, err error) {
		log.Debug().
			Str("topic", e.Topic).
			Str("pubsub_name", e.PubsubName).
			Msg("Received status message")

		var message StatusMessage
		if err := json.Unmarshal(e.RawData, &message); err != nil {
			log.Error().Err(err).Msg("Failed to unmarshal status message")
			return false, err // Don't retry on unmarshal errors
		}

		if err := handler(ctx, message); err != nil {
			log.Error().
				Err(err).
				Str("worker_id", message.WorkerID).
				Str("trace_id", message.TraceID).
				Msg("Failed to handle status message")
			return true, err // Retry on handler errors
		}

		return false, nil
	}
}

// SubscribeToControl subscribes to the control topic
func (p *PubSubClient) SubscribeToControl(handler func(context.Context, ControlMessage) error) {
	p.subscriptions[TopicOrchestrator] = func(ctx context.Context, e *common.TopicEvent) (retry bool, err error) {
		log.Debug().
			Str("topic", e.Topic).
			Str("pubsub_name", e.PubsubName).
			Msg("Received control message")

		var message ControlMessage
		if err := json.Unmarshal(e.RawData, &message); err != nil {
			log.Error().Err(err).Msg("Failed to unmarshal control message")
			return false, err // Don't retry on unmarshal errors
		}

		if err := handler(ctx, message); err != nil {
			log.Error().
				Err(err).
				Str("command", message.Command).
				Str("trace_id", message.TraceID).
				Msg("Failed to handle control message")
			return true, err // Retry on handler errors
		}

		return false, nil
	}
}

// StartServer starts the Dapr service with all registered subscriptions
func (p *PubSubClient) StartServer(ctx context.Context) error {
	if len(p.subscriptions) == 0 {
		log.Info().Msg("No subscriptions registered, skipping server start")
		return nil
	}

	server, err := daprs.NewService(p.appPort)
	if err != nil {
		return fmt.Errorf("failed to create Dapr service: %w", err)
	}

	// Register all subscriptions
	for topic, handler := range p.subscriptions {
		subscription := &common.Subscription{
			PubsubName: p.pubsubName,
			Topic:      topic,
			Route:      "/" + topic, // Use topic name as route
		}

		if err := server.AddTopicEventHandler(subscription, handler); err != nil {
			return fmt.Errorf("failed to add topic event handler for %s: %w", topic, err)
		}

		log.Info().
			Str("topic", topic).
			Str("route", subscription.Route).
			Str("pubsub", p.pubsubName).
			Msg("Registered topic subscription")
	}

	log.Info().
		Str("port", p.appPort).
		Int("subscription_count", len(p.subscriptions)).
		Msg("Starting Dapr PubSub server")

	// Start server in a goroutine so it doesn't block
	go func() {
		if err := server.Start(); err != nil {
			log.Error().Err(err).Msg("Dapr PubSub server failed")
		}
	}()

	return nil
}
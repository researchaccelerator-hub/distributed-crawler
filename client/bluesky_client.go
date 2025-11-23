package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	blueskymodel "github.com/researchaccelerator-hub/telegram-scraper/model/bluesky"
	"github.com/rs/zerolog/log"
)

// BlueskyClient implements the Client interface for Bluesky/AT Protocol using WebSocket.
type BlueskyClient struct {
	config      BlueskyConfig
	conn        *websocket.Conn
	eventBuffer chan *blueskymodel.BlueskyEvent
	cursor      *int64
	isConnected bool
	mu          sync.RWMutex
	stopChan    chan struct{}
	wg          sync.WaitGroup
}

// BlueskyConfig contains configuration for the Bluesky client.
type BlueskyConfig struct {
	JetStreamURL      string   // JetStream endpoint URL
	WantedCollections []string // Filter by collection types (e.g., "app.bsky.feed.post")
	WantedDids        []string // Filter by specific DIDs
	BufferSize        int      // Event buffer size
	Cursor            *int64   // Resume position (optional)
}

// NewBlueskyClient creates a new Bluesky client with the given configuration.
func NewBlueskyClient(config BlueskyConfig) (*BlueskyClient, error) {
	// Set defaults
	if config.JetStreamURL == "" {
		config.JetStreamURL = "wss://jetstream2.us-east.bsky.network/subscribe"
	}
	if config.BufferSize == 0 {
		config.BufferSize = 10000
	}

	log.Info().
		Str("jetstream_url", config.JetStreamURL).
		Int("buffer_size", config.BufferSize).
		Int("collection_filters", len(config.WantedCollections)).
		Int("did_filters", len(config.WantedDids)).
		Msg("Creating Bluesky client")

	return &BlueskyClient{
		config:      config,
		eventBuffer: make(chan *blueskymodel.BlueskyEvent, config.BufferSize),
		cursor:      config.Cursor,
		stopChan:    make(chan struct{}),
	}, nil
}

// Connect establishes a WebSocket connection to the Bluesky JetStream.
func (c *BlueskyClient) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isConnected {
		log.Warn().Msg("Bluesky client already connected")
		return nil
	}

	// Build JetStream URL with query parameters
	jetStreamURL, err := c.buildJetStreamURL()
	if err != nil {
		return fmt.Errorf("failed to build JetStream URL: %w", err)
	}

	log.Info().Str("url", jetStreamURL).Msg("Connecting to Bluesky JetStream")

	// Create WebSocket connection
	conn, _, err := websocket.DefaultDialer.Dial(jetStreamURL, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to WebSocket: %w", err)
	}

	c.conn = conn
	c.isConnected = true

	// Start event consumption goroutine
	c.wg.Add(1)
	go c.consumeEvents(ctx)

	log.Info().Msg("Successfully connected to Bluesky JetStream")
	return nil
}

// buildJetStreamURL constructs the JetStream URL with query parameters.
func (c *BlueskyClient) buildJetStreamURL() (string, error) {
	baseURL, err := url.Parse(c.config.JetStreamURL)
	if err != nil {
		return "", err
	}

	query := baseURL.Query()

	// Add collection filters
	if len(c.config.WantedCollections) > 0 {
		for _, collection := range c.config.WantedCollections {
			query.Add("wantedCollections", collection)
		}
	}

	// Add DID filters
	if len(c.config.WantedDids) > 0 {
		for _, did := range c.config.WantedDids {
			query.Add("wantedDids", did)
		}
	}

	// Add cursor if provided
	if c.cursor != nil {
		query.Set("cursor", fmt.Sprintf("%d", *c.cursor))
	}

	baseURL.RawQuery = query.Encode()
	return baseURL.String(), nil
}

// consumeEvents reads events from the WebSocket and buffers them.
func (c *BlueskyClient) consumeEvents(ctx context.Context) {
	defer c.wg.Done()

	log.Info().Msg("Starting Bluesky event consumption")

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("Context cancelled, stopping event consumption")
			return
		case <-c.stopChan:
			log.Info().Msg("Stop signal received, stopping event consumption")
			return
		default:
			// Read next message from WebSocket
			c.mu.RLock()
			conn := c.conn
			c.mu.RUnlock()

			if conn == nil {
				log.Error().Msg("WebSocket connection is nil")
				return
			}

			// Set read deadline
			conn.SetReadDeadline(time.Now().Add(60 * time.Second))

			_, message, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					log.Info().Msg("WebSocket connection closed normally")
				} else {
					log.Error().Err(err).Msg("Error reading WebSocket message")
				}
				c.mu.Lock()
				c.isConnected = false
				c.mu.Unlock()
				return
			}

			// Parse JSON event
			var event blueskymodel.BlueskyEvent
			if err := json.Unmarshal(message, &event); err != nil {
				log.Warn().Err(err).Str("message", string(message)).Msg("Failed to parse event JSON")
				continue
			}

			// Update cursor from event time
			if event.TimeUS > 0 {
				c.mu.Lock()
				c.cursor = &event.TimeUS
				c.mu.Unlock()
			}

			// Buffer the event
			select {
			case c.eventBuffer <- &event:
				log.Debug().
					Str("did", event.DID).
					Str("kind", event.Kind).
					Int64("time_us", event.TimeUS).
					Msg("Buffered Bluesky event")
			case <-ctx.Done():
				return
			case <-c.stopChan:
				return
			default:
				log.Warn().Msg("Event buffer full, dropping event")
			}
		}
	}
}

// Disconnect closes the WebSocket connection.
func (c *BlueskyClient) Disconnect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.isConnected {
		return nil
	}

	log.Info().Msg("Disconnecting from Bluesky JetStream")

	// Signal consumption goroutine to stop
	close(c.stopChan)

	// Close WebSocket connection
	if c.conn != nil {
		c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		c.conn.Close()
	}

	// Wait for consumption goroutine to finish
	c.wg.Wait()

	// Close event buffer
	close(c.eventBuffer)

	c.isConnected = false

	log.Info().Msg("Disconnected from Bluesky JetStream")
	return nil
}

// GetChannelInfo retrieves information about a Bluesky user/channel.
func (c *BlueskyClient) GetChannelInfo(ctx context.Context, channelID string) (Channel, error) {
	// For now, return a basic channel with the DID/handle
	// TODO: Implement profile fetching via AT Protocol API if needed
	return &BlueskyChannel{
		ID:   channelID,
		Name: channelID,
	}, nil
}

// GetMessages retrieves messages (posts) from the Bluesky firehose for a specified time window.
func (c *BlueskyClient) GetMessages(ctx context.Context, channelID string, fromTime, toTime time.Time, limit int) ([]Message, error) {
	log.Info().
		Str("channel_id", channelID).
		Time("from_time", fromTime).
		Time("to_time", toTime).
		Int("limit", limit).
		Msg("Fetching Bluesky messages")

	// Ensure we're connected
	c.mu.RLock()
	connected := c.isConnected
	c.mu.RUnlock()

	if !connected {
		return nil, fmt.Errorf("not connected to Bluesky JetStream")
	}

	messages := make([]Message, 0)
	startTime := time.Now()

	// Calculate timeout based on time window
	timeout := toTime.Sub(fromTime)
	if timeout <= 0 {
		timeout = 5 * time.Minute // Default timeout
	}

	timeoutTimer := time.NewTimer(timeout)
	defer timeoutTimer.Stop()

	log.Info().
		Dur("timeout", timeout).
		Msg("Starting message collection with timeout")

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("Context cancelled during message collection")
			return messages, ctx.Err()

		case <-timeoutTimer.C:
			log.Info().
				Int("collected_messages", len(messages)).
				Dur("elapsed", time.Since(startTime)).
				Msg("Timeout reached, returning collected messages")
			return messages, nil

		case event, ok := <-c.eventBuffer:
			if !ok {
				log.Info().Msg("Event buffer closed")
				return messages, nil
			}

			// Convert event to message
			msg, err := c.eventToMessage(event)
			if err != nil {
				log.Debug().Err(err).Msg("Skipping event (not a post or conversion failed)")
				continue
			}

			// Check if message matches channel filter
			if channelID != "firehose" && msg.GetChannelID() != channelID {
				continue
			}

			// Check if message is within time range
			msgTime := msg.GetTimestamp()
			if !msgTime.IsZero() && (msgTime.Before(fromTime) || msgTime.After(toTime)) {
				continue
			}

			messages = append(messages, msg)

			log.Debug().
				Int("total_messages", len(messages)).
				Str("msg_id", msg.GetID()).
				Msg("Collected message")

			// Check if we've hit the limit
			if limit > 0 && len(messages) >= limit {
				log.Info().
					Int("limit", limit).
					Msg("Reached message limit, returning")
				return messages, nil
			}
		}
	}
}

// eventToMessage converts a Bluesky event to a Message.
func (c *BlueskyClient) eventToMessage(event *blueskymodel.BlueskyEvent) (Message, error) {
	if event.Commit == nil {
		return nil, fmt.Errorf("event has no commit data")
	}

	// Only process create operations
	if event.Commit.Operation != blueskymodel.OperationCreate {
		return nil, fmt.Errorf("not a create operation: %s", event.Commit.Operation)
	}

	// Only process posts for now
	if event.Commit.Collection != blueskymodel.CollectionPost {
		return nil, fmt.Errorf("not a post collection: %s", event.Commit.Collection)
	}

	// Create a Bluesky message
	msg := &BlueskyMessage{
		ID:        event.Commit.RKey,
		ChannelID: event.DID,
		SenderID:  event.DID,
		Type:      event.Commit.Collection,
		Reactions: make(map[string]int64),
		Thumbnails: make(map[string]string),
	}

	// Extract data from record
	record := event.Commit.Record
	if record == nil {
		return nil, fmt.Errorf("event has no record data")
	}

	// Extract text
	if text, ok := record["text"].(string); ok {
		msg.Text = text
		msg.Description = text
	}

	// Extract creation time
	if createdAt, ok := record["createdAt"].(string); ok {
		if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
			msg.Timestamp = t
		}
	}

	// Extract language
	if langs, ok := record["langs"].([]interface{}); ok && len(langs) > 0 {
		if lang, ok := langs[0].(string); ok {
			msg.Language = lang
		}
	}

	// Extract embed for thumbnails
	if embed, ok := record["embed"].(map[string]interface{}); ok {
		if images, ok := embed["images"].([]interface{}); ok && len(images) > 0 {
			if firstImg, ok := images[0].(map[string]interface{}); ok {
				if imgData, ok := firstImg["image"].(map[string]interface{}); ok {
					if ref, ok := imgData["ref"].(map[string]interface{}); ok {
						if link, ok := ref["$link"].(string); ok {
							// Construct thumbnail URL
							thumbURL := fmt.Sprintf("https://cdn.bsky.app/img/feed_thumbnail/plain/%s/%s@jpeg", event.DID, link)
							msg.Thumbnails["default"] = thumbURL
						}
					}
				}
			}
		}
	}

	return msg, nil
}

// GetChannelType returns the platform type.
func (c *BlueskyClient) GetChannelType() string {
	return "bluesky"
}

// GetCursor returns the current cursor position.
func (c *BlueskyClient) GetCursor() *int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cursor
}

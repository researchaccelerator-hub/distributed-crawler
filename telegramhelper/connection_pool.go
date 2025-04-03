package telegramhelper

import (
	"context"
	"fmt"
	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/rs/zerolog/log"
	"sync"
	"time"
)

// ConnectionPool manages a pool of Telegram client connections
type ConnectionPool struct {
	mu              sync.Mutex
	availableConns  map[string]crawler.TDLibClient
	inUseConns      map[string]crawler.TDLibClient
	maxSize         int
	service         *RealTelegramService
	storagePrefix   string
	defaultConfig   common.CrawlerConfig
	connectionCount int
}

// NewConnectionPool creates a new connection pool with the specified maximum size
func NewConnectionPool(maxSize int, storagePrefix string, defaultConfig common.CrawlerConfig) *ConnectionPool {
	pool := &ConnectionPool{
		availableConns: make(map[string]crawler.TDLibClient),
		inUseConns:     make(map[string]crawler.TDLibClient),
		maxSize:        maxSize,
		service:        &RealTelegramService{},
		storagePrefix:  storagePrefix,
		defaultConfig:  defaultConfig,
	}
	
	// If there are pre-configured database URLs, initialize connections with them
	if len(defaultConfig.TDLibDatabaseURLs) > 0 {
		log.Info().Msgf("Initializing connection pool with %d pre-configured database URLs and max size %d", 
			len(defaultConfig.TDLibDatabaseURLs), maxSize)
		pool.PreloadConnections(defaultConfig.TDLibDatabaseURLs)
	}
	
	return pool
}

// PreloadConnections initializes connections using the provided TDLib database URLs
func (p *ConnectionPool) PreloadConnections(databaseURLs []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	// Don't try to initialize more connections than the pool's max size
	maxToInitialize := p.maxSize
	if len(databaseURLs) < maxToInitialize {
		maxToInitialize = len(databaseURLs)
	}
	
	log.Info().Msgf("Pre-loading %d connections out of %d database URLs (pool max size: %d)", 
		maxToInitialize, len(databaseURLs), p.maxSize)
	
	log.Info().Msgf("Preloading %d connections to the pool", maxToInitialize)
	
	// Create connections for each database URL, up to the maximum
	for i := 0; i < maxToInitialize; i++ {
		// Create a copy of the config for this connection
		connConfig := p.defaultConfig
		connConfig.TDLibDatabaseURL = databaseURLs[i]
		
		// Initialize the client with this specific database URL
		client, err := p.service.InitializeClientWithConfig(p.storagePrefix, connConfig)
		if err != nil {
			log.Error().Err(err).Str("databaseURL", databaseURLs[i]).Msg("Failed to initialize client for pool")
			continue
		}
		
		// Generate a unique connection ID
		p.connectionCount++
		connID := fmt.Sprintf("conn-%d", p.connectionCount)
		
		// Add to available connections
		p.availableConns[connID] = client
		log.Info().Str("connectionID", connID).Str("databaseURL", databaseURLs[i]).Msg("Added connection to pool")
	}
	
	log.Info().Int("available", len(p.availableConns)).Int("maxSize", p.maxSize).Msg("Connection pool initialized")
}

// GetConnection acquires a connection from the pool or creates a new one if needed
func (p *ConnectionPool) GetConnection(ctx context.Context) (crawler.TDLibClient, string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if we have an available connection
	if len(p.availableConns) > 0 {
		// Get any available connection (we're just getting the first one here)
		var connID string
		var client crawler.TDLibClient
		for id, c := range p.availableConns {
			connID = id
			client = c
			break
		}
		
		// Move it to inUse collection
		delete(p.availableConns, connID)
		p.inUseConns[connID] = client
		
		log.Debug().Str("connectionID", connID).Msg("Reusing existing connection from pool")
		return client, connID, nil
	}

	// If no available connections and we haven't reached max size, create a new one
	if len(p.inUseConns) < p.maxSize {
		// Check if we have unused database URLs available
		var connConfig common.CrawlerConfig
		
		if len(p.defaultConfig.TDLibDatabaseURLs) > 0 {
			// Calculate which database URL to use based on the current connection count
			// This ensures we cycle through all available URLs before reusing them
			urlIndex := p.connectionCount % len(p.defaultConfig.TDLibDatabaseURLs)
			databaseURL := p.defaultConfig.TDLibDatabaseURLs[urlIndex]
			
			// Copy the default config and set the specific database URL for this connection
			connConfig = p.defaultConfig
			connConfig.TDLibDatabaseURL = databaseURL
			
			log.Info().Str("databaseURL", databaseURL).Msg("Creating new connection with specified database URL")
		} else {
			// If no specific URLs provided, use the default config
			connConfig = p.defaultConfig
			log.Info().Msg("Creating new connection with default configuration")
		}
		
		// Create a new connection
		client, err := p.service.InitializeClientWithConfig(p.storagePrefix, connConfig)
		if err != nil {
			return nil, "", fmt.Errorf("failed to initialize new client for pool: %w", err)
		}
		
		// Generate a unique connection ID
		p.connectionCount++
		connID := fmt.Sprintf("conn-%d", p.connectionCount)
		p.inUseConns[connID] = client
		
		log.Info().Str("connectionID", connID).Msg("Created new connection in pool")
		return client, connID, nil
	}

	// If we get here, the pool is exhausted
	return nil, "", fmt.Errorf("connection pool exhausted (all %d connections in use)", p.maxSize)
}

// ReleaseConnection returns a connection to the pool
func (p *ConnectionPool) ReleaseConnection(connID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	// Find the connection in the inUseConns map
	client, exists := p.inUseConns[connID]
	if !exists {
		log.Warn().Str("connectionID", connID).Msg("Attempted to release a connection that is not in the pool")
		return
	}
	
	// Move it from inUse to available
	delete(p.inUseConns, connID)
	p.availableConns[connID] = client
	
	log.Debug().Str("connectionID", connID).Msg("Connection returned to pool")
}

// Close closes all connections in the pool
func (p *ConnectionPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	// Close all available connections
	for id, client := range p.availableConns {
		log.Debug().Str("connectionID", id).Msg("Closing available connection")
		closeClientSafe(client)
	}
	
	// Close all in-use connections
	for id, client := range p.inUseConns {
		log.Debug().Str("connectionID", id).Msg("Closing in-use connection")
		closeClientSafe(client)
	}
	
	// Reset the maps
	p.availableConns = make(map[string]crawler.TDLibClient)
	p.inUseConns = make(map[string]crawler.TDLibClient)
}

// closeClientSafe safely closes a client connection with timeout
func closeClientSafe(client crawler.TDLibClient) {
	if client == nil {
		return
	}
	
	done := make(chan bool)
	go func() {
		_, err := client.Close()
		if err != nil {
			log.Error().Err(err).Msg("Error closing client connection")
		}
		done <- true
	}()
	
	// Wait for close to complete or timeout
	select {
	case <-done:
		// Close completed successfully
	case <-time.After(5 * time.Second):
		log.Warn().Msg("Timeout waiting for client connection to close")
	}
}

// Stats returns statistics about the connection pool
func (p *ConnectionPool) Stats() map[string]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	return map[string]int{
		"available": len(p.availableConns),
		"inUse":     len(p.inUseConns),
		"maxSize":   p.maxSize,
	}
}
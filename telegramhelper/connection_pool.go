package telegramhelper

import (
	"context"
	"fmt"
	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/rs/zerolog/log"
	"hash/fnv"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ConnectionPool manages a pool of Telegram client connections to enable
// efficient concurrent processing of multiple Telegram channels.
// It handles connection lifecycle, reuse, and load distribution across
// multiple TDLib database instances to maximize throughput while staying
// within Telegram's rate limits.
type ConnectionPool struct {
	mu              sync.Mutex                     // Mutex to protect concurrent access to the pool
	availableConns  map[string]crawler.TDLibClient // Map of available connections, keyed by connection ID
	inUseConns      map[string]crawler.TDLibClient // Map of in-use connections, keyed by connection ID
	maxSize         int                            // Maximum number of connections the pool can manage
	service         *RealTelegramService           // Service for creating new connections
	storagePrefix   string                         // Prefix for storage paths
	defaultConfig   common.CrawlerConfig           // Default configuration for new connections
	connectionCount int                            // Counter for assigning unique connection IDs

	// Maps connection IDs to directory names for tracking storage paths
	connDirMap map[string]string // Maps connection IDs to their directory paths
}

// NewConnectionPool creates a new connection pool with the specified maximum size.
// It initializes the pool data structures and, if database URLs are provided in the
// configuration, preloads connections to minimize startup time for subsequent requests.
//
// Parameters:
//   - maxSize: The maximum number of connections the pool will manage
//   - storagePrefix: The path prefix where TDLib databases will be stored
//   - defaultConfig: The default configuration for all connections, including database URLs
//
// Returns:
//   - A fully initialized connection pool ready for use
func NewConnectionPool(maxSize int, storagePrefix string, defaultConfig common.CrawlerConfig) *ConnectionPool {
	pool := &ConnectionPool{
		availableConns: make(map[string]crawler.TDLibClient),
		inUseConns:     make(map[string]crawler.TDLibClient),
		maxSize:        maxSize,
		service:        &RealTelegramService{},
		storagePrefix:  storagePrefix,
		defaultConfig:  defaultConfig,
		connDirMap:     make(map[string]string),
	}

	// If there are pre-configured database URLs, initialize connections with them
	if len(defaultConfig.TDLibDatabaseURLs) > 0 {
		log.Info().Msgf("Initializing connection pool with %d pre-configured database URLs and max size %d",
			len(defaultConfig.TDLibDatabaseURLs), maxSize)
		pool.PreloadConnections(defaultConfig.TDLibDatabaseURLs)
	}

	return pool
}

// PreloadConnections initializes TDLib connections using the provided database URLs.
// This function pre-creates connections at startup time to minimize connection
// initialization delays during crawling operations. It handles loading multiple
// pre-authenticated TDLib database files from the specified URLs.
//
// Parameters:
//   - databaseURLs: A list of URLs pointing to pre-configured TDLib database archives
//
// The function will initialize up to maxSize connections (or the number of URLs provided,
// whichever is smaller) and add them to the pool's available connections map.
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

		// Calculate the directory hash the same way the client initialization does
		h := fnv.New32a()
		h.Write([]byte(connConfig.TDLibDatabaseURL))
		
		// Add unique components to ensure different processes get different folders
		// even if they share the same database URL
		uniqueComponent := fmt.Sprintf("%d_%d_%d", time.Now().UnixNano(), os.Getpid(), p.connectionCount)
		h.Write([]byte(uniqueComponent))
		
		dirName := fmt.Sprintf("conn_%d", h.Sum32())

		// Use the directory name as the connection ID for perfect matching
		p.connectionCount++
		connID := dirName

		// Store the mapping
		p.connDirMap[connID] = dirName

		// Add to available connections
		p.availableConns[connID] = client
		log.Info().Str("connectionID", connID).Str("databaseURL", databaseURLs[i]).Msg("Added connection to pool")
	}

	log.Info().Int("available", len(p.availableConns)).Int("maxSize", p.maxSize).Msg("Connection pool initialized")
}

// GetConnection acquires a connection from the pool or creates a new one if needed.
// It first attempts to reuse an existing available connection. If none are available
// and the pool size limit hasn't been reached, it creates a new connection.
// If all connections are in use and the pool is at capacity, it returns an error.
//
// Parameters:
//   - ctx: Context for potential cancellation or timeout of the connection acquisition
//
// Returns:
//   - A TDLib client connection ready for use
//   - A string identifier for the connection, needed when releasing it back to the pool
//   - An error if the pool is exhausted or connection creation fails
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

		// Calculate the directory hash the same way the client initialization does
		h := fnv.New32a()
		h.Write([]byte(connConfig.TDLibDatabaseURL))
		
		// Add unique components to ensure different processes get different folders
		// even if they share the same database URL
		uniqueComponent := fmt.Sprintf("%d_%d_%d", time.Now().UnixNano(), os.Getpid(), p.connectionCount)
		h.Write([]byte(uniqueComponent))
		
		dirName := fmt.Sprintf("conn_%d", h.Sum32())

		// Use the directory name as the connection ID for perfect matching
		p.connectionCount++
		connID := dirName

		// Store the mapping
		p.connDirMap[connID] = dirName
		p.inUseConns[connID] = client

		log.Info().Str("connectionID", connID).Msg("Created new connection in pool")
		return client, connID, nil
	}

	// If we get here, the pool is exhausted
	return nil, "", fmt.Errorf("connection pool exhausted (all %d connections in use)", p.maxSize)
}

// ReleaseConnection returns a connection to the pool, making it available
// for reuse. This should be called when a caller is finished with a connection
// acquired through GetConnection.
//
// Parameters:
//   - connID: The connection identifier that was returned by GetConnection
//
// If the connection ID doesn't exist in the in-use connections map, a warning
// is logged and no action is taken.
//
// The connection is fully disconnected, its directory is removed, and a
// fresh connection is created before being returned to the available pool.
func (p *ConnectionPool) ReleaseConnection(connID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Find the connection in the inUseConns map
	client, exists := p.inUseConns[connID]
	if !exists {
		log.Warn().Str("connectionID", connID).Msg("Attempted to release a connection that is not in the pool")
		return
	}

	// Remove the connection from inUse map
	delete(p.inUseConns, connID)

	// Close the existing connection
	log.Debug().Str("connectionID", connID).Msg("Closing connection before returning to pool")
	closeClientSafe(client)

	// The connection ID is already in the correct format for the directory name
	// Format: "conn_X" where X is a number
	connDirName := connID
	dirPath := filepath.Join(p.storagePrefix, "state", connDirName)

	// Remove the connection directory
	log.Debug().Str("connectionID", connID).Str("dirPath", dirPath).Msg("Removing connection directory")
	if err := os.RemoveAll(dirPath); err != nil {
		log.Warn().Err(err).Str("connectionID", connID).Str("dirPath", dirPath).Msg("Failed to remove connection directory")
	}

	// Create a fresh connection
	log.Debug().Str("connectionID", connID).Msg("Creating fresh connection to replace the closed one")

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

		log.Info().Str("databaseURL", databaseURL).Msg("Creating fresh connection with specified database URL")
	} else {
		// If no specific URLs provided, use the default config
		connConfig = p.defaultConfig
		log.Info().Msg("Creating fresh connection with default configuration")
	}

	// Create a new connection
	newClient, err := p.service.InitializeClientWithConfig(p.storagePrefix, connConfig)
	if err != nil {
		log.Error().Err(err).Str("connectionID", connID).Msg("Failed to create fresh connection, pool capacity reduced")
		return
	}

	// Add the fresh connection to available connections with same ID
	p.availableConns[connID] = newClient

	log.Info().Str("connectionID", connID).Msg("Fresh connection created and returned to pool")
}

// Close shuts down all connections in the pool and resets the pool to an empty state.
// This should be called when the pool is no longer needed to clean up resources properly.
// It closes both available and in-use connections.
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

// closeClientSafe safely closes a TDLib client connection with timeout protection.
// It handles the case where closing might hang or take too long by setting a timeout.
//
// Parameters:
//   - client: The TDLib client connection to close
//
// The function will attempt to close the connection gracefully and log any errors.
// If closing takes longer than 5 seconds, it will log a warning and continue.
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

// HandleConnectionError handles a connection that has experienced an error.
// It closes the faulty connection, cleans up its directory, and creates
// a fresh connection if possible. This ensures that connections are always
// clean and reliable.
//
// Parameters:
//   - connID: The connection identifier that encountered the error
//
// Returns:
//   - A new TDLib client connection or nil if creation failed
//   - A string identifier for the connection (same as input connID if successful)
//   - An error if the refresh operation fails
//
// This method should be called when a connection exhibits errors in normal operation
// to ensure the next use gets a clean state.
func (p *ConnectionPool) HandleConnectionError(ctx context.Context, connID string) (crawler.TDLibClient, string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	log.Warn().Str("connectionID", connID).Msg("Handling connection error by recreating connection")

	// First, check if the connection is in-use
	client, exists := p.inUseConns[connID]
	if !exists {
		// If it's not in use, check if it's in available connections
		client, exists = p.availableConns[connID]
		if !exists {
			return nil, "", fmt.Errorf("connection ID %s not found in pool", connID)
		}

		// Remove from available connections
		delete(p.availableConns, connID)
	} else {
		// Remove from in-use connections
		delete(p.inUseConns, connID)
	}

	// Close the faulty connection
	log.Debug().Str("connectionID", connID).Msg("Closing faulty connection")
	closeClientSafe(client)

	// The connection ID is already in the correct format for the directory name
	// Format: "conn_X" where X is a number
	connDirName := connID
	dirPath := filepath.Join(p.storagePrefix, "state", connDirName)

	// Remove the connection directory
	log.Debug().Str("connectionID", connID).Str("dirPath", dirPath).Msg("Removing connection directory due to error")
	if err := os.RemoveAll(dirPath); err != nil {
		log.Warn().Err(err).Str("connectionID", connID).Str("dirPath", dirPath).Msg("Failed to remove faulty connection directory")
	}

	// Create a fresh connection with the same ID
	var connConfig common.CrawlerConfig

	if len(p.defaultConfig.TDLibDatabaseURLs) > 0 {
		// Calculate which database URL to use
		urlIndex := p.connectionCount % len(p.defaultConfig.TDLibDatabaseURLs)
		databaseURL := p.defaultConfig.TDLibDatabaseURLs[urlIndex]

		connConfig = p.defaultConfig
		connConfig.TDLibDatabaseURL = databaseURL

		log.Info().Str("databaseURL", databaseURL).Msg("Creating fresh connection after error")
	} else {
		connConfig = p.defaultConfig
		log.Info().Msg("Creating fresh connection with default configuration after error")
	}

	// Create a new connection
	newClient, err := p.service.InitializeClientWithConfig(p.storagePrefix, connConfig)
	if err != nil {
		log.Error().Err(err).Str("connectionID", connID).Msg("Failed to create fresh connection after error")
		return nil, "", fmt.Errorf("failed to create fresh connection after error: %w", err)
	}

	// Put the new connection directly back into in-use
	p.inUseConns[connID] = newClient

	log.Info().Str("connectionID", connID).Msg("Successfully created fresh connection after error")
	return newClient, connID, nil
}

// Stats returns statistics about the current state of the connection pool,
// including the number of available connections, in-use connections, and
// the maximum pool size. This is useful for monitoring and debugging.
//
// Returns:
//   - A map with string keys and int values containing pool statistics:
//   - "available": Number of connections ready for use
//   - "inUse": Number of connections currently being used
//   - "maxSize": Maximum pool capacity
func (p *ConnectionPool) Stats() map[string]int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return map[string]int{
		"available": len(p.availableConns),
		"inUse":     len(p.inUseConns),
		"maxSize":   p.maxSize,
	}
}

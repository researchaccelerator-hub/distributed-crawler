# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A multi-platform social media crawler written in Go that supports scraping content from Telegram and YouTube. The crawler can run in multiple modes: standalone (single process), DAPR-enabled (with distributed state management), or fully distributed (orchestrator/worker architecture).

## Build & Run Commands

### Build
```bash
go build -o telegram-scraper
```

### Debug Build
```bash
go build -gcflags "all=-N -l" -o ./bin/app
```

### Docker Build
```bash
docker build -t telegram-scraper .
```

### Test Commands
```bash
# Run all tests
go test ./...

# Run specific test
go test ./path/to/package -run TestName

# Verbose output
go test -v ./...

# Test specific package
go test ./crawler/youtube -v
```

## Platform-Specific Setup

### macOS (Required for TDLib)
```bash
export CGO_CFLAGS=-I/opt/homebrew/include
export CGO_LDFLAGS=-L/opt/homebrew/lib -lssl -lcrypto
```

### Telegram Authentication
First-time setup requires generating authentication:
```bash
./telegram-scraper --generate-code
```

## Execution Modes

The crawler supports four distinct execution modes:

1. **Standalone** (`--mode=standalone` or no flags): Single process, local state, no DAPR
2. **DAPR Standalone** (`--mode=dapr-standalone` or `--dapr`): Single process with DAPR state management
3. **Orchestrator** (`--mode=orchestrator --dapr`): Distributed coordinator that publishes work via DAPR pubsub
4. **Worker** (`--mode=worker --worker-id=<id> --dapr`): Worker node that processes individual crawl tasks

### Example Commands

```bash
# Standalone Telegram crawl
./telegram-scraper --platform telegram --urls "channel1,channel2"

# YouTube crawl with API key
./telegram-scraper --platform youtube --youtube-api-key "KEY" --urls "UCxxx,UCyyy"

# Bluesky firehose crawl (full network, posts only, 5 minute window)
./telegram-scraper --platform bluesky --urls "firehose" --time-ago "5m" --max-posts 1000

# Bluesky specific user crawl
./telegram-scraper --platform bluesky --urls "did:plc:z72i7hdynmk6r22z27h6tvur" --time-ago "24h"

# Distributed orchestrator
./telegram-scraper --mode=orchestrator --dapr --urls="url1,url2"

# Distributed worker
./telegram-scraper --mode=worker --dapr --worker-id="worker-1"
```

## Architecture Overview

### Core Design Principles

- **Platform-agnostic interfaces**: The `crawler.Crawler` interface allows multiple platforms (Telegram, YouTube) with a unified API
- **Factory pattern**: `CrawlerFactory` and `StateManagerFactory` enable runtime selection of implementations
- **Layer-based crawling**: Pages are organized into depth-based layers, allowing breadth-first traversal
- **State persistence**: All crawl state (pages, messages, metadata) is persisted to enable resume functionality

### Key Packages

**`main.go`**
- Entry point with Cobra CLI setup
- Parses flags, initializes configuration
- Routes to appropriate execution mode (standalone, DAPR, orchestrator, worker)
- Handles time-based filtering (`--time-ago`, `--min-post-date`, `--date-between`)

**`crawler/`**
- Defines `Crawler` interface for platform-agnostic crawling
- `CrawlTarget`, `CrawlJob`, `CrawlResult` types used across platforms
- `TDLibClient` interface wraps Telegram TDLib client methods
- Factory pattern for creating platform-specific crawlers

**`crawler/telegram/`**
- `TelegramCrawler` implementation of the `Crawler` interface
- Uses TDLib for Telegram API interactions
- Handles Telegram-specific message types and metadata

**`crawler/youtube/`**
- `YouTubeCrawler` implementation for YouTube Data API v3
- `ClientAdapter` adapts the generic client to YouTube-specific interface
- Handles video metadata, comments, and channel information

**`crawler/bluesky/`**
- `BlueskyCrawler` implementation for Bluesky/AT Protocol firehose
- Uses WebSocket connection to JetStream for real-time event streaming
- Implements time-window buffering to match existing batch interface
- Handles Bluesky-specific post types, embeds, facets (mentions/links)
- `event_processor.go`: Parses JetStream events (posts, reposts, likes)

**`state/`**
- `StateManagementInterface`: Core interface for all state operations
- `Page`: Represents a crawl target (channel/page) with status tracking
- Multiple implementations: local filesystem, Azure Blob, DAPR state store
- State includes: pages (by layer/depth), messages, media cache, crawl metadata
- `StateManagerFactory`: Creates appropriate state manager based on config

**`standalone/runner.go`**
- Main crawl loop for non-distributed execution
- Processes layers sequentially (depth-first within layers)
- Handles page status transitions (unfetched → processing → fetched/error)
- Manages discovered pages and adds them as new layers
- Implements resume logic based on page status

**`orchestrator/orchestrator.go`**
- Central coordinator for distributed crawling
- Publishes work items to DAPR pubsub
- Tracks worker health and work completion
- Reassigns work from failed workers
- Processes results and discovered pages

**`worker/worker.go`**
- Subscribes to work items from orchestrator
- Executes crawl jobs independently
- Publishes results and status updates back to orchestrator

**`distributed/`**
- `WorkItem`: Serializable crawl task for a single page
- `ResultMessage`: Work completion report with discovered pages
- `PubSubClient`: Wraps DAPR pubsub for work distribution

**`client/`**
- Generic client factory pattern for different platforms
- `YouTubeClient` implementation wraps YouTube Data API
- Provides abstraction over platform-specific API clients

**`telegramhelper/`**
- TDLib connection pool management
- Connection setup, authentication helpers
- Telegram-specific utilities (file cleaning, message parsing)

**`model/`**
- Unified `Post` data structure for all platforms
- `ChannelData`, `EngagementData`, `Comment` types
- Standardized format for JSONL output

**`common/utils.go`**
- `CrawlerConfig`: Central configuration struct
- `GenerateCrawlID()`: Timestamp-based unique IDs
- URL file parsing utilities

### State Management & Resume Logic

**Page Status Flow:**
```
unfetched → processing → fetched (success)
                      → error (failure, can retry)
```

**Crawl Resumption:**
- When `--crawl-id` matches existing incomplete crawl: resumes from last execution ID
- Checks `FindIncompleteCrawl()` to locate execution ID
- Skips pages with status "fetched" when resuming same execution
- Re-processes "error" pages and continues from last unfetched page

**Layer Processing:**
- Pages organized by depth (0 = seed URLs, 1 = first hop, etc.)
- Each layer processed completely before moving to next depth
- New pages discovered during processing are added as new layer at depth+1
- `GetLayerByDepth()` retrieves all pages at specific depth
- Processing stops when no pages found at current depth and no deeper layers exist

**Media Cache:**
- `HasProcessedMedia()` prevents duplicate media downloads
- `MarkMediaAsProcessed()` records processed media by ID
- Cache persisted in state to survive restarts

### Connection Pooling (Telegram)

- `InitConnectionPool(poolSize, storageRoot, config)`: Creates pool of TDLib connections
- Pool size determined by: min(concurrency, len(TDLibDatabaseURLs))
- Each connection uses separate TDLib database for isolation
- `RunForChannelWithPool()`: Acquires connection from pool, processes channel, returns to pool
- Pre-seeded databases can be provided via `--tdlib-database-urls` for faster startup

### Platform Selection

Platform is specified via `--platform` flag (default: "telegram"):
- `telegram`: Uses TDLib and `TelegramCrawler`
- `youtube`: Uses YouTube Data API v3 and `YouTubeCrawler`
- `bluesky`: Uses Bluesky JetStream WebSocket firehose and `BlueskyCrawler`

YouTube requires `--youtube-api-key` parameter.
Bluesky requires no authentication - the firehose is publicly accessible.

## Code Style Guidelines

**Imports:**
- Group: standard library, project imports, third-party
- Use goimports for automatic formatting

**Error Handling:**
```go
if err != nil {
    return fmt.Errorf("context: %w", err)
}
```

**Logging:**
- Use zerolog with structured fields:
```go
log.Error().Err(err).Str("url", url).Msg("Failed to process")
```

**Naming:**
- Exported: PascalCase
- Unexported: camelCase
- Acronyms: All caps (e.g., `TDLibClient`, `YouTubeAPIKey`)

**Comments:**
- Document all exported functions:
```go
// FunctionName does X and returns Y.
// It handles Z by doing W.
func FunctionName() {}
```

**Receivers:**
- Use pointer receivers for methods that modify state
- Use value receivers for read-only methods on small structs

**Concurrency:**
- Always use `context.Context` for cancellation
- Use `sync.WaitGroup` for goroutine coordination
- Protect shared state with `sync.Mutex` or `sync.RWMutex`

## Testing Patterns

**Mock Interfaces:**
- See `crawl/mocks_test.go` for mock generation patterns
- Tests use interface-based mocking for TDLib client

**Table-Driven Tests:**
```go
tests := []struct {
    name    string
    input   X
    want    Y
    wantErr bool
}{
    {"case1", input1, expected1, false},
}
for _, tt := range tests {
    t.Run(tt.name, func(t *testing.T) {
        // test logic
    })
}
```

## Common Development Patterns

### Adding a New Platform

1. Create `crawler/<platform>/<platform>_crawler.go`
2. Implement `crawler.Crawler` interface
3. Create client adapter in `client/<platform>_client.go`
4. Add model types in `model/<platform>/types.go`
5. Register in `crawler/common/registrar.go`:
```go
factory.RegisterCrawler(crawler.PlatformNewPlatform, func() crawler.Crawler {
    return &newplatform.NewPlatformCrawler{}
})
```

### Adding State Storage Backend

1. Create implementation in `state/<backend>state.go`
2. Implement `StateManagementInterface`
3. Update `state/statefactory.go` to recognize new backend
4. Add config struct to `state/interface.go`

### Time Filtering Options

The crawler supports three time filtering approaches:

1. **`--min-post-date "YYYY-MM-DD"`**: Lower bound only
2. **`--time-ago "30d"`**: Relative time (30d, 6h, 2w, 1m, 1y)
3. **`--date-between "YYYY-MM-DD,YYYY-MM-DD"`**: Specific range with optional `--sample-size N` for random sampling

Implemented in `main.go` parseTimeAgo() and PersistentPreRunE.

### Bluesky-Specific Features

The Bluesky crawler uses a **streaming time-window approach** that differs from traditional batch crawling:

**Connection & Streaming:**
- Connects to Bluesky JetStream WebSocket: `wss://jetstream2.us-east.bsky.network/subscribe`
- Continuously streams events in real-time (posts, reposts, likes)
- Buffers events until time window completes or message limit reached
- No authentication required - fully public firehose

**Target ID Formats:**
- `"firehose"`: Full Bluesky network (all public posts)
- `"did:plc:..."`: Specific user DID
- `"@handle.bsky.social"` or `"handle.bsky.social"`: User handle

**Collection Filtering:**
- Default: `app.bsky.feed.post` (posts only)
- Configurable via metadata for reposts, likes, follows, etc.
- Filters applied as WebSocket query parameters

**Event Processing:**
- JetStream events are JSON with commit data
- Events include: DID, timestamp (microseconds), operation type, record data
- Record data contains post text, embeds, facets (mentions/links), timestamps
- Cursor tracking for resumption (time_us field)

**Data Mapping to Unified Post Format:**
- `DID` → `ChannelID`
- `RecordKey` (rkey) → Part of `PostUID`
- `text` field → `Description`, `SearchableText`, `AllText`
- `createdAt` → `PublishedAt`
- Reply structure → `RepliedID`
- Embed with record → `QuotedID` (quote posts)
- Repost events → `SharedID`
- Facets with mentions/links → `Outlinks`
- Embed images → `ThumbURL` (constructed from blob CID)
- Platform → `"Bluesky"`

**Time-Window Behavior:**
- Connects to WebSocket when `FetchMessages()` called
- Buffers events for duration of `FromTime` to `ToTime`
- Returns when: timeout reached, limit hit, or context cancelled
- Uses gorilla/websocket for robust connection handling

**Example Usage Patterns:**
```bash
# Monitor last 10 minutes of full network
./telegram-scraper --platform bluesky --urls "firehose" --time-ago "10m" --max-posts 5000

# Specific user, last 24 hours
./telegram-scraper --platform bluesky --urls "did:plc:xyz123" --time-ago "24h"

# Date range with sample
./telegram-scraper --platform bluesky --urls "firehose" \
  --date-between "2025-01-01,2025-01-02" --sample-size 1000
```

**Limitations:**
- No historical data - can only collect events from connection time forward
- No engagement counts (likes, reposts) directly from firehose - requires separate API calls
- Outlinks discovery doesn't trigger snowball crawling (not currently wired up for Bluesky)

## Go API Client Best Practices

Based on the YouTube InnerTube client implementation (see `client/youtube_innertube_*.go`), follow these patterns when building production-ready API clients:

### Phase 1: Critical Production Readiness

**Input Validation**
- Pre-compile regex patterns at package level (not in functions)
- Validate all inputs before making API calls
- Provide clear, actionable error messages

```go
// Pre-compile patterns
var channelIDPattern = regexp.MustCompile(`^UC[a-zA-Z0-9_-]{22}$`)

// Validate before API call
func (c *Client) GetData(id string) error {
    if err := validateID(id); err != nil {
        return fmt.Errorf("invalid ID: %w", err)
    }
    // ... make API call
}
```

**Connection State Management**
- Track connection state with thread-safe mutex
- Implement `ensureConnected()` helper
- Check connection before all operations

```go
type Client struct {
    client    *APIClient
    mu        sync.RWMutex  // Protects client and connected state
    connected bool
}

func (c *Client) ensureConnected() error {
    c.mu.RLock()
    defer c.mu.RUnlock()

    if !c.connected || c.client == nil {
        return fmt.Errorf("client not connected - call Connect() first")
    }
    return nil
}
```

**Resource Bounds**
- Use LRU caches instead of unbounded maps
- Define cache size limits at package level
- LRU caches are thread-safe (no manual locking needed)

```go
import lru "github.com/hashicorp/golang-lru/v2"

const defaultCacheSize = 1000

type Client struct {
    cache *lru.Cache[string, *Data]  // Not map[string]*Data
}

func NewClient() (*Client, error) {
    cache, err := lru.New[string, *Data](defaultCacheSize)
    if err != nil {
        return nil, err
    }
    return &Client{cache: cache}, nil
}
```

**API Timeouts**
- Configure timeouts (default 30 seconds)
- Use context.WithTimeout for all API calls
- Distinguish timeout errors from other errors

```go
func (c *Client) apiCall(ctx context.Context) error {
    apiCtx, cancel := context.WithTimeout(ctx, c.apiTimeout)
    defer cancel()

    data, err := c.client.Call(apiCtx)
    if err != nil {
        if apiCtx.Err() == context.DeadlineExceeded {
            log.Error().Dur("timeout", c.apiTimeout).Msg("API call timed out")
            return fmt.Errorf("timed out after %v: %w", c.apiTimeout, err)
        }
        return fmt.Errorf("API call failed: %w", err)
    }
    return nil
}
```

### Phase 2: Performance and Maintainability

**Regex Optimization**
- NEVER compile regex inside functions (especially in loops)
- Pre-compile at package level
- Reuse compiled patterns

```go
// ❌ Wrong - compiles on every call
func parse(text string) string {
    re := regexp.MustCompile(`\d+`)  // BAD
    return re.FindString(text)
}

// ✅ Correct - compile once at package init
var numericPattern = regexp.MustCompile(`\d+`)

func parse(text string) string {
    return numericPattern.FindString(text)
}
```

**Parser Refactoring**
- Keep functions under 100 lines
- Limit nesting depth to 3-4 levels
- Extract format-specific parsing into helpers
- Single responsibility per function

```go
// ❌ Wrong - 225 lines, 6-7 nesting levels
func parseChannel(data interface{}) (*Channel, error) {
    // Massive nested if statements
}

// ✅ Correct - delegate to focused helpers
func parseChannel(data interface{}) (*Channel, error) {
    // 30-40 lines
    if format1 { parseFormat1(data, channel) }  // 40 lines each
    if format2 { parseFormat2(data, channel) }
    if metadata { parseMetadata(data, channel) }
    return channel, nil
}
```

**Deduplication**
- Extract repeated logic into helper functions
- Use helper for common filtering/validation

```go
// ❌ Wrong - repeated 4 times
if !fromTime.IsZero() && item.Time.Before(fromTime) {
    continue
}
if !toTime.IsZero() && item.Time.After(toTime) {
    continue
}

// ✅ Correct - single function
func shouldInclude(item *Item, fromTime, toTime time.Time) bool {
    if !fromTime.IsZero() && item.Time.Before(fromTime) {
        return false
    }
    if !toTime.IsZero() && item.Time.After(toTime) {
        return false
    }
    return true
}
```

### Phase 3: Testing

**Unit Tests**
- Table-driven tests for validation
- 60+ test cases covering edge cases
- Test validation before testing API logic

**Integration Tests**
- Real API calls with `-short` skip flag
- Test with well-known public resources
- Validate caching behavior
- Test error conditions (invalid input, timeout, disconnected)
- Document in separate INTEGRATION_TESTS.md

```go
func TestIntegration(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping integration test in short mode")
    }
    // ... test with real API
}
```

### Production Readiness Checklist

Before deploying an API client:

- [ ] Input validation on all public methods
- [ ] Connection state tracking (thread-safe)
- [ ] Resource bounds (LRU caches)
- [ ] API timeouts (configurable, 30s default)
- [ ] Error wrapping with context
- [ ] Regex pre-compiled at package level
- [ ] Parser functions < 100 lines
- [ ] Nesting depth < 4 levels
- [ ] Unit tests (validation, parsing)
- [ ] Integration tests (real API)
- [ ] Documentation (how to run tests)

### Examples in Codebase

See `client/youtube_innertube_*.go` for complete implementation:
- `youtube_innertube_client.go` - Main client (production-ready)
- `youtube_innertube_validation.go` - Input validation patterns
- `youtube_innertube_validation_test.go` - 60+ validation tests
- `youtube_innertube_integration_test.go` - 11 integration tests
- `INTEGRATION_TESTS.md` - Test documentation

## Important Notes

- **macOS TDLib**: Always export CGO_CFLAGS and CGO_LDFLAGS before building
- **YouTube Quotas**: YouTube API has strict quota limits (10,000 units/day default). Use `--max-posts` to limit consumption
- **Bluesky Firehose**: No authentication needed, but only provides real-time data (not historical)
- **Graceful Shutdown**: SIGINT/SIGTERM handlers save state before exit (see `standalone/runner.go`)
- **State Persistence**: Always call `sm.Close()` to flush media cache and final state
- **Resume Logic**: Uses both `crawl_id` and `crawl_execution_id` - execution ID changes per run, crawl ID persists

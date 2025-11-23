# Bluesky Crawler Tests

This directory contains comprehensive tests for the Bluesky crawler implementation.

## Test Files

- `bluesky_crawler_test.go` - Tests for the main BlueskyCrawler implementation
- `event_processor_test.go` - Tests for Bluesky event processing logic

## Running Tests

### Prerequisites

On macOS, ensure TDLib dependencies are properly configured:

```bash
export CGO_CFLAGS=-I/opt/homebrew/include
export CGO_LDFLAGS=-L/opt/homebrew/lib -lssl -lcrypto
```

### Run All Bluesky Tests

```bash
go test ./crawler/bluesky/... -v
```

### Run Specific Test

```bash
go test ./crawler/bluesky -run TestBlueskyCrawlerInitialize -v
```

### Run with Coverage

```bash
go test ./crawler/bluesky/... -cover -coverprofile=coverage.out
go tool cover -html=coverage.out
```

## Test Coverage

### BlueskyCrawler Tests (`bluesky_crawler_test.go`)

**Initialization & Setup:**
- ✅ `TestNewBlueskyCrawler` - Verifies crawler creation
- ✅ `TestBlueskyCrawlerInitialize` - Tests successful initialization with valid config
- ✅ `TestBlueskyCrawlerInitializeErrors` - Tests error handling for invalid configurations

**Target Validation:**
- ✅ `TestValidateTarget` - Tests validation of various target formats (firehose, DIDs, handles)

**Channel Info:**
- ✅ `TestGetChannelInfo` - Tests fetching channel information for different target types

**Message Fetching:**
- ✅ `TestFetchMessages` - Tests complete message fetching workflow with state management

**Data Conversion:**
- ✅ `TestConvertMessageToPost` - Tests conversion from Bluesky message to unified Post model
- ✅ `TestConvertBlueskyPostToModelPost` - Tests detailed Bluesky post conversion with replies

**Embed Processing:**
- ✅ `TestProcessEmbed` - Tests handling of different embed types (images, links, quotes)

**Platform Type:**
- ✅ `TestGetPlatformType` - Verifies correct platform type returned

### Event Processor Tests (`event_processor_test.go`)

**Post Processing:**
- ✅ `TestProcessPostEvent` - Tests basic post event parsing
- ✅ `TestProcessPostEventWithReply` - Tests reply structure parsing
- ✅ `TestProcessPostEventWithEmbed` - Tests embed extraction from posts

**Repost Processing:**
- ✅ `TestProcessRepostEvent` - Tests repost event parsing

**Like Processing:**
- ✅ `TestProcessLikeEvent` - Tests like event parsing

**Facet Processing:**
- ✅ `TestParseFacets` - Tests parsing of facets (mentions, links, hashtags)
- ✅ `TestExtractOutlinks` - Tests outlink extraction from facets

**Utility Functions:**
- ✅ `TestExtractPostIDFromURI` - Tests AT URI parsing
- ✅ `TestParseEmbed` - Tests embed structure parsing

**Commit Event Processing:**
- ✅ `TestProcessCommitEvent` - Tests routing of different commit types

## Mock Objects

Tests use mock implementations to avoid external dependencies:

- `MockBlueskyClient` - Simulates the Bluesky WebSocket client
- `MockStateManager` - Simulates state persistence

## Test Data

Tests use realistic Bluesky event structures including:
- Post events with text, timestamps, and languages
- Reply events with parent/root references
- Embed events (images, external links, quote posts)
- Facet events (mentions, links, hashtags)
- Repost and like events

## Known Limitations

The tests currently don't cover:
- Actual WebSocket connection testing (would require integration tests)
- Time-window buffering behavior (would require goroutine timing tests)
- Cursor persistence and resumption (requires state manager integration)

These would be good candidates for integration tests in a future test suite.

## Adding New Tests

When adding new functionality to the Bluesky crawler:

1. Add unit tests to the appropriate test file
2. Use table-driven test pattern for multiple test cases
3. Use descriptive test names that explain what is being tested
4. Include both success and error cases
5. Update this README with new test coverage

Example table-driven test:

```go
func TestNewFeature(t *testing.T) {
    tests := []struct {
        name    string
        input   string
        want    string
        wantErr bool
    }{
        {"valid input", "test", "expected", false},
        {"invalid input", "", "", true},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            got, err := NewFeature(tt.input)
            if (err != nil) != tt.wantErr {
                t.Errorf("NewFeature() error = %v, wantErr %v", err, tt.wantErr)
                return
            }
            if got != tt.want {
                t.Errorf("NewFeature() = %v, want %v", got, tt.want)
            }
        })
    }
}
```

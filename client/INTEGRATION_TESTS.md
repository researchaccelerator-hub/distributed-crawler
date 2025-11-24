# YouTube InnerTube Integration Tests

This document describes the integration tests for the YouTube InnerTube client and how to run them.

## Overview

The integration tests in `youtube_innertube_integration_test.go` validate the InnerTube client against the real YouTube API. These tests:

- Make actual network requests to YouTube
- Validate parsing of real API responses
- Test error handling and edge cases
- Verify caching behavior
- Ensure timeout configuration works

## Running Integration Tests

### Run All Integration Tests

```bash
# Set CGO flags for macOS (required for TDLib dependency)
export CGO_CFLAGS=-I/opt/homebrew/include
export CGO_LDFLAGS="-L/opt/homebrew/lib -lssl -lcrypto"

# Run integration tests
go test -v -run Integration ./client/
```

### Run Specific Integration Test

```bash
# Run only channel info test
go test -v -run TestIntegrationGetChannelInfo ./client/

# Run only video fetching test
go test -v -run TestIntegrationGetVideosFromChannel ./client/

# Run only caching test
go test -v -run TestIntegrationGetChannelInfoCaching ./client/
```

### Skip Integration Tests (Default)

Integration tests are automatically skipped when running with `-short` flag:

```bash
# This skips all integration tests
go test -v -short ./client/
```

## Test Coverage

### 1. GetChannelInfo Tests

**TestIntegrationGetChannelInfo**
- Tests fetching channel info by channel ID
- Tests fetching channel info by @handle
- Validates: title, subscriber count, video count, thumbnails
- Uses: Google Developers channel (UC_x5XG1OV2P6uZZ5FSM9Ttw)

**TestIntegrationGetChannelInfoCaching**
- Verifies LRU cache improves performance
- Compares first call vs cached call timing
- Ensures data consistency between calls

### 2. GetVideosFromChannel Tests

**TestIntegrationGetVideosFromChannel**
- Fetches videos from a real channel
- Validates time range filtering (90 days)
- Verifies limit parameter works (10 videos max)
- Checks video metadata completeness

**TestIntegrationGetVideosWithDifferentLimits**
- Tests multiple limit values (5, 10, 20)
- Ensures limit is respected
- Validates across 1-year time range

### 3. GetVideosByIDs Tests

**TestIntegrationGetVideosByIDs**
- Tests fetching specific videos by ID
- Uses well-known video IDs (Rick Astley, PSY)
- Note: May fail if Player endpoint not implemented

### 4. Error Handling Tests

**TestIntegrationInvalidChannelID**
- Tests validation catches bad inputs before API calls
- Covers: empty, invalid format, too short, special chars

**TestIntegrationAPITimeout**
- Verifies timeout configuration works
- Uses impossibly short timeout (1ns) to trigger timeout
- Ensures timeout errors are properly reported

**TestIntegrationDisconnectedClient**
- Ensures operations fail gracefully when not connected
- Validates "not connected" error message

**TestIntegrationGetRandomVideos**
- Verifies GetRandomVideos returns "not supported" error
- Confirms clear error message for unsupported operation

## Expected Test Results

### Successful Tests
All tests should pass when:
- Network connectivity is available
- YouTube API is accessible
- Test channels still exist

### Acceptable Failures

**TestIntegrationGetVideosByIDs**
- May fail with TODO note about Player endpoint
- This is expected until Player endpoint is implemented

**Video Count Variations**
- Channels may have 0 videos in certain time ranges
- Tests handle this gracefully

## Network Requirements

Integration tests require:
- Internet connectivity
- Access to youtube.com
- No rate limiting from your IP
- No proxy blocking YouTube

## Timeout Configuration

Default timeout: **30 seconds**

Can be adjusted in test setup:
```go
config := &InnerTubeConfig{
    APITimeout: 60 * time.Second, // Increase if needed
}
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Integration Tests
on: [push, pull_request]
jobs:
  integration:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-go@v3
        with:
          go-version: '1.21'
      - name: Run Integration Tests
        run: |
          export CGO_CFLAGS=-I/usr/include
          export CGO_LDFLAGS="-L/usr/lib -lssl -lcrypto"
          go test -v -run Integration ./client/
        timeout-minutes: 10
```

### Run Integration Tests Nightly

```yaml
name: Nightly Integration Tests
on:
  schedule:
    - cron: '0 2 * * *'  # Run at 2 AM daily
```

## Troubleshooting

### Test Timeout
**Problem**: Tests timeout after 30 seconds

**Solutions**:
- Check network connectivity
- Verify YouTube is accessible
- Increase timeout in test config
- Check for rate limiting

### Connection Refused
**Problem**: "connection refused" error

**Solutions**:
- Check firewall settings
- Verify no proxy blocking
- Ensure DNS resolution works
- Try different network

### Parse Errors
**Problem**: Failed to parse channel/video data

**Solutions**:
- YouTube may have changed API format
- Check if parsers need updating
- Verify channel still exists
- Review error logs for details

### Cache Not Working
**Problem**: Cached calls same speed as first call

**Solutions**:
- Check LRU cache is initialized
- Verify cache key is correct
- Ensure cache not being invalidated
- Review cache configuration

## Test Maintenance

### When YouTube API Changes

If YouTube updates their InnerTube API format:

1. Run integration tests to identify failures
2. Examine actual API responses
3. Update parser functions in `youtube_innertube_client.go`
4. Update tests if necessary
5. Document changes in commit message

### Updating Test Channels

If test channels become unavailable:

1. Find replacement channels with similar characteristics
2. Update channel IDs in tests
3. Verify new channels have sufficient content
4. Document changes in test comments

### Adding New Tests

When adding new InnerTube functionality:

1. Add corresponding integration test
2. Follow existing test patterns
3. Handle network failures gracefully
4. Document expected behavior
5. Add to this document

## Best Practices

1. **Run Before Committing**: Always run integration tests before major commits
2. **Check CI Results**: Monitor CI for integration test failures
3. **Document Failures**: If tests fail, document why and how to fix
4. **Keep Tests Fast**: Use reasonable limits (10-20 videos max)
5. **Handle Flakiness**: Network tests can be flaky, implement retries if needed

## Performance Benchmarks

Typical execution times (with good network):

| Test | Duration | Notes |
|------|----------|-------|
| GetChannelInfo | 1-2s | First call |
| GetChannelInfo (cached) | <1ms | From LRU cache |
| GetVideos (10 limit) | 2-3s | Depends on channel size |
| GetVideos (20 limit) | 3-5s | More videos = more parsing |
| Invalid inputs | <100ms | Validation before API call |

## Related Documentation

- [../CLAUDE.md](../CLAUDE.md) - Main project documentation
- [youtube_innertube_client.go](youtube_innertube_client.go) - Client implementation
- [youtube_innertube_test.go](youtube_innertube_test.go) - Unit tests
- [youtube_innertube_validation.go](youtube_innertube_validation.go) - Input validation

## Contact

For issues with integration tests:
1. Check this document first
2. Review test output and logs
3. Examine actual API responses
4. Open issue with details if needed

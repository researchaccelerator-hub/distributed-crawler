---
layout: default
title: "API Reference"
---

# API Reference

Complete reference for all command-line options, configuration parameters, and environment variables.

## Command Line Interface

### Basic Syntax
```bash
./distributed-crawler [global-options] [command] [command-options]
```

### Global Options

#### Execution Mode

**`--mode`** *(string)*  
Execution mode for the crawler. Choose based on your deployment needs:
- `standalone` - Single process execution (default)
- `dapr-standalone` - Single process with DAPR integration  
- `orchestrator` - Distributes work to multiple workers
- `worker` - Processes jobs from orchestrator

*Example:* `--mode orchestrator`

**`--worker-id`** *(string)*  
Unique identifier for worker instances in distributed mode. Required when using `--mode worker`.

*Example:* `--worker-id "worker-01"`

---

#### Data Sources

**`--urls`** *(comma-separated list)*  
Channel URLs or IDs to scrape. Supports multiple formats per platform.

*Examples:*
- Telegram: `--urls "channel1,channel2,@username"`
- YouTube: `--urls "UCxxx123,UCyyy456,@handle"`

**`--url-file`** *(string)*  
Path to file containing URLs (one per line). Useful for large channel lists.

*Example:* `--url-file "channels.txt"`

**`--url-file-url`** *(string)*  
HTTP URL to a file containing URLs to crawl. Remote channel list support.

*Example:* `--url-file-url "https://example.com/channels.txt"`

---

#### Platform Configuration

**`--platform`** *(string)* • *Default: `telegram`*  
Target platform for crawling.

*Options:* `telegram` | `youtube`  
*Example:* `--platform youtube`

**`--youtube-api-key`** *(string)*  
API key for YouTube Data API v3. Required when using `--platform youtube`.  
Get your key at [Google Cloud Console](https://console.cloud.google.com/).

*Example:* `--youtube-api-key "AIza..."`

---

#### Crawling Limits

**`--max-posts`** *(integer)* • *Default: `-1` (unlimited)*  
Maximum posts to collect per channel. Use to limit resource usage.

*Examples:*
- `--max-posts 1000` - Limit to 1000 posts per channel
- `--max-posts -1` - No limit

**`--max-comments`** *(integer)* • *Default: `-1` (unlimited)*  
Maximum comments to collect per post.

*Example:* `--max-comments 500`

**`--max-depth`** *(integer)* • *Default: `-1` (unlimited)*  
Maximum crawl depth for discovery operations.

**`--max-pages`** *(integer)* • *Default: `108000`*  
Maximum number of pages/channels to process in total.

**`--min-users`** *(integer)* • *Default: `100`*  
Skip channels with fewer subscribers/members than this threshold.

*Example:* `--min-users 1000` - Only crawl channels with 1000+ members

---

#### Time Filtering

**`--time-ago`** *(duration)* • *Default: `1m`*  
Only collect posts newer than specified duration.

*Formats:* `30d` (days) | `6h` (hours) | `2w` (weeks) | `1m` (months) | `1y` (years)  
*Example:* `--time-ago "30d"` - Posts from last 30 days

**`--min-post-date`** *(date)*  
Minimum post date to crawl. Format: YYYY-MM-DD

*Example:* `--min-post-date "2023-01-01"`

**`--date-between`** *(date range)*  
Crawl posts within specific date range. Format: YYYY-MM-DD,YYYY-MM-DD

*Example:* `--date-between "2023-01-01,2023-12-31"`

**`--sample-size`** *(integer)* • *Default: `0` (no sampling)*  
Randomly sample this many posts when using `--date-between`.

*Example:* `--sample-size 5000` - Random sample of 5000 posts

---

#### Storage Configuration

**`--storage-root`** *(string)* • *Default: `/tmp/crawl`*  
Root directory for storing crawled data and progress files.

*Example:* `--storage-root "/data/crawls"`

**`--output`** *(string)* • *Default: `json`*  
Output format for crawled data.

*Options:* `json` | `csv`

**`--skip-media`** *(flag)*  
Skip downloading media files (images, videos, documents).

*Usage:* `--skip-media`

---

#### System Configuration

**`--concurrency`** *(integer)* • *Default: `1`*  
Number of concurrent crawler threads. Higher values increase speed but use more resources.

*Example:* `--concurrency 5`

**`--timeout`** *(integer)* • *Default: `30`*  
HTTP request timeout in seconds.

*Example:* `--timeout 60`

**`--log-level`** *(string)* • *Default: `debug`*  
Logging verbosity level.

*Options:* `trace` | `debug` | `info` | `warn` | `error` | `fatal` | `panic`  
*Example:* `--log-level info`

**`--config`** *(string)*  
Path to YAML configuration file for advanced settings.

*Example:* `--config "config.yaml"`

---

#### Advanced Options

**`--crawl-id`** *(string)*  
Custom identifier for this crawl operation. Auto-generated if not provided.

**`--crawl-label`** *(string)*  
Human-readable label for organizing crawl results.

*Example:* `--crawl-label "weekly-analysis"`

**`--user-agent`** *(string)* • *Default: `Mozilla/5.0 Crawler`*  
Custom User-Agent string for HTTP requests.

**`--dapr`** *(flag)*  
Enable DAPR integration for cloud-native deployments.

**`--dapr-port`** *(integer)* • *Default: `6481`*  
DAPR sidecar port for service communication.

**`--tdlib-verbosity`** *(integer)* • *Default: `1`*  
TDLib logging verbosity (0-10). Higher values provide more detailed Telegram client logs.

## Environment Variables

All environment variables are prefixed with `CRAWLER_` and override configuration file values.

### Telegram API Configuration
| Variable | Required | Description |
|----------|----------|-------------|
| `TG_API_ID` | Yes* | Telegram API ID from my.telegram.org |
| `TG_API_HASH` | Yes* | Telegram API Hash from my.telegram.org |
| `TG_PHONE_NUMBER` | Yes* | Phone number with country code (e.g., +12025551234) |
| `TG_PHONE_CODE` | Yes* | OTP sent to your Telegram app |

*Required only for Telegram platform

### Azure Blob Storage Configuration
| Variable | Required | Description |
|----------|----------|-------------|
| `CONTAINER_NAME` | No | Azure Blob Storage container name |
| `BLOB_NAME` | No | Blob path for storing data |
| `AZURE_STORAGE_ACCOUNT_URL` | No | Azure Storage account URL |

### System Configuration
| Variable | Default | Description |
|----------|---------|-------------|
| `CRAWLER_LOGGING_LEVEL` | `"debug"` | Override log level |
| `CRAWLER_STORAGE_ROOT` | `"/tmp/crawl"` | Override storage root |
| `CRAWLER_CONCURRENCY` | `1` | Override concurrency setting |

## Configuration File

### File Locations (searched in order)
1. Path specified by `--config` flag
2. `./config.yaml`
3. `$HOME/.crawler/config.yaml`
4. `/etc/crawler/config.yaml`

### Configuration File Format
```yaml
# Logging configuration
logging:
  level: "info"

# DAPR configuration
dapr:
  enabled: false
  port: 6481
  mode: "job"

# Crawler configuration
crawler:
  concurrency: 5
  timeout: 30
  useragent: "Custom Crawler 1.0"
  minusers: 100
  maxcomments: 1000
  maxposts: 5000
  maxdepth: 3
  maxpages: 10000
  minpostdate: "2023-01-01"
  timeago: "30d"
  datebetween: "2023-01-01,2023-12-31"
  samplesize: 1000
  crawlid: "custom-crawl-001"
  crawllabel: "research-project-q1"
  skipmedia: false
  platform: "telegram"

# Output configuration
output:
  format: "json"

# Storage configuration
storage:
  root: "/data/crawls"

# TDLib configuration
tdlib:
  database_url: ""
  database_urls: []
  verbosity: 1

# YouTube configuration
youtube:
  api_key: "your_api_key_here"

# Distributed mode configuration
distributed:
  mode: "standalone"
  worker_id: ""
```

## Data Models

### Post Structure
```json
{
  "post_uid": "string",
  "url": "string",
  "channel_id": "string",
  "channel_name": "string",
  "published_at": "2023-04-01T12:34:56Z",
  "description": "string",
  "thumb_url": "string",
  "media_url": "string",
  "views_count": 0,
  "comments_count": 0,
  "shares_count": 0,
  "likes_count": 0,
  "engagement": 0,
  "platform_name": "string",
  "post_type": ["string"],
  "outlinks": ["string"],
  "reactions": {"emoji": 0},
  "comments": [
    {
      "comment_id": "string",
      "author": "string",
      "text": "string",
      "created_at": "2023-04-01T12:34:56Z",
      "likes_count": 0,
      "reply_to_comment_id": "string"
    }
  ],
  "channel_data": {
    "channel_id": "string",
    "channel_name": "string",
    "channel_url_external": "string",
    "channel_description": "string",
    "channel_engagement_data": {
      "follower_count": 0,
      "post_count": 0,
      "video_count": 0,
      "views_count": 0
    }
  },
  "crawl_id": "string",
  "crawl_label": "string"
}
```

### Progress Structure
```json
{
  "crawl_id": "string",
  "started_at": "2023-04-01T12:34:56Z",
  "updated_at": "2023-04-01T12:34:56Z",
  "status": "running",
  "total_channels": 0,
  "completed_channels": 0,
  "total_posts": 0,
  "processed_posts": 0,
  "errors": [],
  "channels": {
    "channel_name": {
      "status": "completed",
      "posts_scraped": 0,
      "last_post_date": "2023-04-01T12:34:56Z",
      "error": null
    }
  }
}
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Configuration error |
| 3 | Authentication error |
| 4 | Network error |
| 5 | Storage error |
| 6 | API quota exceeded |

## Rate Limits and Quotas

### Telegram API
- **Message History**: ~20 requests/minute
- **Channel Info**: ~60 requests/minute
- **Media Downloads**: ~30 files/minute

The scraper implements exponential backoff and automatic retry for rate limit handling.

### YouTube Data API v3
- **Default Quota**: 10,000 units/day
- **Search Requests**: 100 units each
- **Video Details**: 1 unit each
- **Comment Threads**: 1 unit each

Use `--max-posts` to limit quota usage.

## Examples

### Basic Usage
```bash
# Scrape Telegram channels
./distributed-crawler --urls "channel1,channel2"

# Scrape YouTube channels
./distributed-crawler --platform youtube --youtube-api-key "KEY" --urls "UCxxx,UCyyy"
```

### Advanced Configuration
```bash
# Distributed mode with time filtering
./distributed-crawler --mode orchestrator --dapr \
  --urls "$(cat channels.txt | tr '\n' ',')" \
  --time-ago "30d" \
  --max-posts 1000 \
  --skip-media \
  --crawl-label "monthly-analysis"

# Worker node
./distributed-crawler --mode worker --dapr --worker-id "worker-01"
```

### Configuration File Usage
```bash
# Use custom config file
./distributed-crawler --config /path/to/config.yaml --urls "channel1,channel2"
```

---

For more examples and use cases, see the [Examples](examples/) section.
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
| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--mode` | string | `""` | Execution mode: `standalone`, `dapr-standalone`, `orchestrator`, `worker` |
| `--worker-id` | string | `""` | Worker identifier for distributed mode (required for worker mode) |

#### Data Sources
| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--urls` | []string | `[]` | Comma-separated list of channel URLs/IDs to scrape |
| `--url-file` | string | `""` | File containing URLs to crawl (one per line) |
| `--url-file-url` | string | `""` | URL to a file containing URLs to crawl |

#### Platform Configuration
| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--platform` | string | `"telegram"` | Platform to crawl: `telegram`, `youtube` |
| `--youtube-api-key` | string | `""` | API key for YouTube Data API (required for YouTube) |

#### Crawling Limits
| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--max-posts` | int | `-1` | Maximum number of posts to collect per channel (-1 = unlimited) |
| `--max-comments` | int | `-1` | Maximum number of comments to crawl per post (-1 = unlimited) |
| `--max-depth` | int | `-1` | Maximum depth of the crawl (-1 = unlimited) |
| `--max-pages` | int | `108000` | Maximum number of pages/channels to crawl |
| `--min-users` | int | `100` | Minimum number of users in a channel to crawl |

#### Time Filtering
| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--min-post-date` | string | `""` | Minimum post date to crawl (format: YYYY-MM-DD) |
| `--time-ago` | string | `"1m"` | Only posts newer than this (e.g., `30d`, `6h`, `2w`, `1m`, `1y`) |
| `--date-between` | string | `""` | Date range (format: YYYY-MM-DD,YYYY-MM-DD) |
| `--sample-size` | int | `0` | Random sample size when using date-between (0 = no sampling) |

#### Storage Configuration
| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--storage-root` | string | `"/tmp/crawl"` | Root directory for storing data |
| `--output` | string | `"json"` | Output format (`json`, `csv`) |
| `--skip-media` | bool | `false` | Skip downloading media files |

#### DAPR Configuration
| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--dapr` | bool | `false` | Enable DAPR integration |
| `--dapr-mode` | string | `"job"` | DAPR mode: `job` or `standalone` |
| `--dapr-port` | int | `6481` | DAPR sidecar port |

#### TDLib Configuration
| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--tdlib-database-url` | string | `""` | URL to pre-seeded TDLib database (deprecated) |
| `--tdlib-database-urls` | []string | `[]` | Multiple TDLib database URLs for connection pooling |
| `--tdlib-verbosity` | int | `1` | TDLib verbosity level (0-10, higher = more verbose) |

#### Crawl Management
| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--crawl-id` | string | `""` | Unique identifier for this crawl operation |
| `--crawl-label` | string | `""` | User-defined label for the crawl |
| `--generate-code` | bool | `false` | Run code generation after crawling |
| `--crawl-type` | string | `"focused"` | Crawl type: `focused` or `snowball` |

#### System Configuration
| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--concurrency` | int | `1` | Number of concurrent crawlers |
| `--timeout` | int | `30` | HTTP request timeout in seconds |
| `--user-agent` | string | `"Mozilla/5.0 Crawler"` | User agent string |
| `--log-level` | string | `"debug"` | Log level: `trace`, `debug`, `info`, `warn`, `error`, `fatal`, `panic` |
| `--config` | string | `""` | Path to configuration file |

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
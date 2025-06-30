---
layout: default
title: "Examples"
---

# Examples

Common use cases and configuration examples for the Distributed Crawler.

## Basic Usage Examples

### Simple Telegram Scraping
```bash
# Scrape a few Telegram channels
./distributed-crawler --urls "channel1,channel2,channel3"

# Scrape with time filtering (last 30 days)
./distributed-crawler --urls "news_channel,tech_channel" --time-ago "30d"

# Limit the number of posts per channel
./distributed-crawler --urls "busy_channel" --max-posts 1000
```

### YouTube Channel Scraping
```bash
# Basic YouTube scraping
./distributed-crawler --platform youtube \
  --youtube-api-key "YOUR_API_KEY" \
  --urls "UCxxx1,UCxxx2"

# YouTube with filtering and limits
./distributed-crawler --platform youtube \
  --youtube-api-key "YOUR_API_KEY" \
  --urls "@handle1,@handle2" \
  --time-ago "90d" \
  --max-posts 500
```

## Advanced Configuration Examples

### Large-Scale Distributed Scraping
```bash
# Start orchestrator
./distributed-crawler --mode orchestrator --dapr \
  --url-file channels_list.txt \
  --crawl-label "quarterly-analysis" \
  --time-ago "90d" \
  --max-posts 5000 \
  --skip-media

# Start multiple workers
./distributed-crawler --mode worker --dapr --worker-id "worker-01"
./distributed-crawler --mode worker --dapr --worker-id "worker-02"
./distributed-crawler --mode worker --dapr --worker-id "worker-03"
```

### Date Range Analysis
```bash
# Scrape posts between specific dates with sampling
./distributed-crawler --urls "channel1,channel2" \
  --date-between "2023-01-01,2023-12-31" \
  --sample-size 10000 \
  --crawl-label "2023-annual-analysis"
```

### Custom Storage Configuration
```bash
# Use custom storage directory
./distributed-crawler --urls "channel1,channel2" \
  --storage-root "/data/telegram-scrapes" \
  --crawl-id "custom-crawl-001"
```

## Configuration File Examples

### Basic Configuration File
Create `config.yaml`:

```yaml
# Basic configuration
crawler:
  concurrency: 3
  timeout: 45
  maxposts: 2000
  platform: "telegram"
  timeago: "30d"
  skipmedia: false

storage:
  root: "/data/crawls"

logging:
  level: "info"

output:
  format: "json"
```

Usage:
```bash
./distributed-crawler --config config.yaml --urls "channel1,channel2"
```

### Advanced Configuration File
Create `advanced-config.yaml`:

```yaml
# Advanced configuration for large-scale operations
logging:
  level: "info"

dapr:
  enabled: true
  port: 6481
  mode: "standalone"

crawler:
  concurrency: 10
  timeout: 60
  useragent: "Research Bot 1.0"
  minusers: 500
  maxcomments: 5000
  maxposts: 10000
  maxdepth: 5
  timeago: "60d"
  skipmedia: true
  platform: "telegram"

storage:
  root: "/mnt/research-data/crawls"

output:
  format: "json"

tdlib:
  verbosity: 2
  database_urls:
    - "https://storage.example.com/tdlib-db-1.tgz"
    - "https://storage.example.com/tdlib-db-2.tgz"
```

### YouTube-Specific Configuration
Create `youtube-config.yaml`:

```yaml
crawler:
  platform: "youtube"
  concurrency: 5
  maxposts: 1000
  maxcomments: 500
  timeago: "90d"

youtube:
  api_key: "YOUR_YOUTUBE_API_KEY"

storage:
  root: "/data/youtube-crawls"

logging:
  level: "info"
```

## Environment Setup Examples

### Development Environment
```bash
# Set up for local development
export TG_API_ID="12345678"
export TG_API_HASH="abcdef1234567890abcdef1234567890"
export TG_PHONE_NUMBER="+1234567890"
export CRAWLER_LOGGING_LEVEL="debug"
export CRAWLER_STORAGE_ROOT="./local-storage"

./distributed-crawler --urls "test_channel" --max-posts 100
```

### Production Environment
```bash
# Production setup with Azure storage
export TG_API_ID="12345678"
export TG_API_HASH="abcdef1234567890abcdef1234567890"
export TG_PHONE_NUMBER="+1234567890"
export CONTAINER_NAME="production-crawl-data"
export BLOB_NAME="crawls/"
export AZURE_STORAGE_ACCOUNT_URL="https://myaccount.blob.core.windows.net"
export CRAWLER_LOGGING_LEVEL="info"

./distributed-crawler --mode orchestrator --dapr \
  --url-file production-channels.txt \
  --crawl-label "production-weekly" \
  --config production-config.yaml
```

## URL File Examples

### Simple Channel List
Create `channels.txt`:
```
news_channel_1
tech_updates
crypto_news
@political_channel
science_daily
```

### YouTube Channels
Create `youtube-channels.txt`:
```
UCxxx1234567890
UCyyy0987654321
@techreviewer
@newsnetwork
@sciencechannel
```

### Mixed Format with Comments
Create `mixed-channels.txt`:
```
# Telegram channels
news_channel_1
tech_updates
@verified_channel

# YouTube channels (will be ignored if platform=telegram)
UCxxx1234567890
@youtuber_handle
```

## Docker Examples

### Dockerfile
```dockerfile
FROM golang:1.20-alpine AS builder

# Install TDLib dependencies
RUN apk add --no-cache \
    build-base \
    cmake \
    gperf \
    openssl-dev \
    zlib-dev \
    git

# Build TDLib
RUN git clone https://github.com/tdlib/td.git /tmp/td && \
    cd /tmp/td && \
    mkdir build && \
    cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release .. && \
    cmake --build . && \
    make install

# Build application
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o distributed-crawler

FROM alpine:latest
RUN apk add --no-cache ca-certificates openssl-dev
WORKDIR /root/
COPY --from=builder /app/distributed-crawler .
COPY --from=builder /usr/local/lib/libtd* /usr/local/lib/
COPY --from=builder /usr/local/include/td /usr/local/include/td
ENV LD_LIBRARY_PATH=/usr/local/lib
CMD ["./distributed-crawler"]
```

### Docker Compose for Distributed Setup
Create `docker-compose.yml`:
```yaml
version: '3.8'

services:
  orchestrator:
    build: .
    environment:
      - TG_API_ID=${TG_API_ID}
      - TG_API_HASH=${TG_API_HASH}
      - TG_PHONE_NUMBER=${TG_PHONE_NUMBER}
      - CRAWLER_LOGGING_LEVEL=info
    command: >
      ./distributed-crawler --mode orchestrator --dapr
      --url-file /data/channels.txt
      --crawl-label docker-crawl
    volumes:
      - ./data:/data
      - ./storage:/storage
    depends_on:
      - redis

  worker1:
    build: .
    environment:
      - TG_API_ID=${TG_API_ID}
      - TG_API_HASH=${TG_API_HASH}
      - TG_PHONE_NUMBER=${TG_PHONE_NUMBER}
      - CRAWLER_LOGGING_LEVEL=info
    command: >
      ./distributed-crawler --mode worker --dapr
      --worker-id worker-1
    volumes:
      - ./storage:/storage
    depends_on:
      - redis

  worker2:
    build: .
    environment:
      - TG_API_ID=${TG_API_ID}
      - TG_API_HASH=${TG_API_HASH}
      - TG_PHONE_NUMBER=${TG_PHONE_NUMBER}
      - CRAWLER_LOGGING_LEVEL=info
    command: >
      ./distributed-crawler --mode worker --dapr
      --worker-id worker-2
    volumes:
      - ./storage:/storage
    depends_on:
      - redis

  redis:
    image: redis:alpine
    ports:
      - "6379:6379"
```

## Script Examples

### Batch Processing Script
Create `batch-scrape.sh`:
```bash
#!/bin/bash

# Configuration
CHANNELS_DIR="./channel-lists"
OUTPUT_DIR="./batch-results"
SCRAPER="./distributed-crawler"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Process each channel list
for channel_file in "$CHANNELS_DIR"/*.txt; do
    if [ -f "$channel_file" ]; then
        filename=$(basename "$channel_file" .txt)
        echo "Processing $filename..."
        
        $SCRAPER \
            --url-file "$channel_file" \
            --storage-root "$OUTPUT_DIR/$filename" \
            --crawl-label "batch-$filename" \
            --time-ago "30d" \
            --max-posts 1000 \
            --log-level info
        
        echo "Completed $filename"
    fi
done

echo "Batch processing completed"
```

### Monitoring Script
Create `monitor-crawl.sh`:
```bash
#!/bin/bash

CRAWL_ID="$1"
STORAGE_ROOT="${2:-/tmp/crawl}"

if [ -z "$CRAWL_ID" ]; then
    echo "Usage: $0 <crawl-id> [storage-root]"
    exit 1
fi

PROGRESS_FILE="$STORAGE_ROOT/crawls/$CRAWL_ID/progress.json"

echo "Monitoring crawl: $CRAWL_ID"
echo "Progress file: $PROGRESS_FILE"
echo "================================"

while [ -f "$PROGRESS_FILE" ]; do
    if command -v jq &> /dev/null; then
        status=$(jq -r '.status' "$PROGRESS_FILE")
        completed=$(jq -r '.completed_channels' "$PROGRESS_FILE")
        total=$(jq -r '.total_channels' "$PROGRESS_FILE")
        posts=$(jq -r '.processed_posts' "$PROGRESS_FILE")
        
        echo "Status: $status | Channels: $completed/$total | Posts: $posts"
    else
        echo "Progress file exists (install jq for detailed info)"
    fi
    
    sleep 10
done

echo "Crawl completed or progress file not found"
```

## Troubleshooting Examples

### Debug Configuration
```bash
# Maximum verbosity for troubleshooting
./distributed-crawler \
    --urls "problematic_channel" \
    --log-level trace \
    --tdlib-verbosity 5 \
    --max-posts 10 \
    --timeout 120
```

### Test Connection
```bash
# Test with minimal configuration
./distributed-crawler \
    --urls "telegram" \
    --max-posts 1 \
    --skip-media \
    --log-level debug
```

---

These examples should cover most common use cases. For more specific scenarios, check the [API Reference](api-reference/) or [open an issue](https://github.com/researchaccelerator-hub/distributed-crawler/issues) on GitHub.
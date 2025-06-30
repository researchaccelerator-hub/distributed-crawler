---
layout: default
title: "Architecture"
---

# Architecture Overview

The Telegram Scraper is designed as a modular, scalable system that can operate in multiple execution modes depending on your needs. This guide explains the system architecture, components, and execution modes.

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Telegram Scraper                        │
├─────────────────────────────────────────────────────────────┤
│  CLI Interface (main.go)                                   │
│  ├── Configuration Management                              │
│  ├── Command Line Parsing                                  │
│  └── Execution Mode Selection                              │
├─────────────────────────────────────────────────────────────┤
│  Execution Modes                                           │
│  ├── Standalone Mode                                       │
│  ├── DAPR Standalone Mode                                  │
│  ├── Distributed Orchestrator Mode                        │
│  └── Distributed Worker Mode                              │
├─────────────────────────────────────────────────────────────┤
│  Core Components                                           │
│  ├── Platform Crawlers                                     │
│  │   ├── Telegram Crawler                                 │
│  │   └── YouTube Crawler                                  │
│  ├── State Management                                      │
│  │   ├── Local File System                                │
│  │   ├── Azure Blob Storage                               │
│  │   └── DAPR State Store                                 │
│  ├── Data Models                                           │
│  └── Utility Libraries                                     │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. CLI Interface (`main.go`)
The entry point handles:
- Command-line argument parsing using Cobra
- Configuration loading from files and environment variables
- Execution mode selection
- Logging configuration
- Signal handling for graceful shutdown

### 2. Platform Crawlers

#### Telegram Crawler (`crawler/telegram/`)
- **TDLib Integration**: Uses Telegram's official client library
- **Connection Pooling**: Manages multiple TDLib connections for efficiency
- **Authentication**: Handles phone number, OTP, and 2FA authentication
- **Message Processing**: Extracts text, media, reactions, and metadata
- **Rate Limiting**: Implements exponential backoff for API limits

#### YouTube Crawler (`crawler/youtube/`)
- **YouTube Data API**: Integrates with Google's official API
- **Quota Management**: Efficient API usage to stay within limits
- **Video Metadata**: Extracts titles, descriptions, engagement metrics
- **Comment Threading**: Retrieves comment threads with replies
- **Channel Information**: Fetches channel metadata and statistics

### 3. State Management (`state/`)
Handles persistence and progress tracking:

- **Local File System**: Default storage using JSON files
- **Azure Blob Storage**: Cloud storage for scalable deployments
- **DAPR State Store**: Distributed state management for cloud-native setups
- **Progress Tracking**: Resumable crawls with checkpoint saving
- **Seed List Management**: Manages target URLs and crawl queues

### 4. Data Models (`model/`)
Unified data structures for all platforms:

```go
type Post struct {
    PostUID        string    `json:"post_uid"`
    URL            string    `json:"url"`
    ChannelID      string    `json:"channel_id"`
    ChannelName    string    `json:"channel_name"`
    PublishedAt    time.Time `json:"published_at"`
    Description    string    `json:"description"`
    ViewsCount     int       `json:"views_count"`
    CommentsCount  int       `json:"comments_count"`
    Engagement     int       `json:"engagement"`
    PlatformName   string    `json:"platform_name"`
    PostType       []string  `json:"post_type"`
    Comments       []Comment `json:"comments"`
    ChannelData    Channel   `json:"channel_data"`
}
```

## Execution Modes

### 1. Standalone Mode
**Use case**: Simple, single-machine scraping

```bash
./telegram-scraper --mode=standalone --urls "channel1,channel2"
```

**Characteristics**:
- Single process execution
- Local file storage
- Direct platform API calls
- Best for small to medium datasets

### 2. DAPR Standalone Mode
**Use case**: Cloud-ready single instance with state management

```bash
./telegram-scraper --mode=dapr-standalone --dapr --urls "channel1,channel2"
```

**Characteristics**:
- Single process with DAPR integration
- Cloud storage capabilities
- State management via DAPR
- Improved reliability and observability

### 3. Distributed Orchestrator Mode
**Use case**: Large-scale scraping with job distribution

```bash
./telegram-scraper --mode=orchestrator --dapr --urls "channel1,channel2,..."
```

**Characteristics**:
- Coordinates multiple worker instances
- Distributes work via DAPR pub/sub
- Monitors worker health and progress
- Handles job retries and failures

### 4. Distributed Worker Mode
**Use case**: Scalable worker nodes for processing

```bash
./telegram-scraper --mode=worker --dapr --worker-id="worker-1"
```

**Characteristics**:
- Receives jobs from orchestrator
- Processes individual crawl tasks
- Reports progress and results
- Auto-scaling friendly

## Data Flow

### Standalone Mode Flow
```
URLs → Crawler → Platform APIs → Data Processing → Local Storage
```

### Distributed Mode Flow
```
URLs → Orchestrator → Job Queue → Workers → Platform APIs → Data Processing → Shared Storage
```

## Storage Architecture

### Local File System
```
storage/
└── crawls/
    └── {crawl-id}/
        ├── progress.json
        ├── seed_list.json
        ├── channel1/
        │   ├── data.jsonl
        │   └── media/
        └── channel2/
            ├── data.jsonl
            └── media/
```

### Cloud Storage (Azure Blob)
```
container/
└── crawls/
    └── {crawl-id}/
        ├── progress.json
        ├── seed_list.json
        └── data/
            ├── channel1.jsonl
            └── channel2.jsonl
```

## Configuration Management

### Configuration Sources (Priority Order)
1. Command-line flags
2. Environment variables (prefixed with `CRAWLER_`)
3. Configuration file (`config.yaml`)
4. Default values

### Example Configuration File
```yaml
# config.yaml
crawler:
  concurrency: 5
  timeout: 30
  maxposts: 10000
  platform: "telegram"

storage:
  root: "/data/crawls"

logging:
  level: "info"

dapr:
  enabled: true
  port: 6481

azure:
  storage_account_url: "https://youraccount.blob.core.windows.net"
  container_name: "crawl-data"
```

## Security Considerations

### API Credentials
- Telegram API credentials stored in environment variables
- YouTube API keys passed as command-line arguments
- Azure storage credentials via Azure SDK authentication

### Data Privacy
- No personal data stored unless explicitly in public messages
- Media downloads can be disabled with `--skip-media`
- Support for data retention policies

### Network Security
- HTTPS/TLS for all API communications
- Rate limiting to respect platform policies
- Configurable user agents and request headers

## Scalability Features

### Horizontal Scaling
- Orchestrator + multiple workers architecture
- DAPR-based service discovery and communication
- Stateless worker design for easy scaling

### Vertical Scaling
- Configurable concurrency levels
- Connection pooling for database clients
- Memory-efficient streaming processing

### Performance Optimizations
- Incremental crawling with progress checkpoints
- Concurrent processing of multiple channels
- Efficient JSON streaming for large datasets
- Media download optimization

## Monitoring and Observability

### Logging
- Structured logging with zerolog
- Configurable log levels
- Contextual logging with request IDs

### Metrics
- Built-in performance metrics
- DAPR integration for distributed tracing
- Progress tracking and reporting

### Health Checks
- Worker health monitoring
- Graceful shutdown handling
- Automatic retry mechanisms

## Development Architecture

### Package Structure
```
telegram-scraper/
├── main.go                 # Entry point
├── common/                 # Shared utilities
├── crawler/               # Platform crawlers
│   ├── telegram/         # Telegram implementation
│   └── youtube/          # YouTube implementation
├── state/                # State management
├── model/                # Data models
├── standalone/           # Standalone mode runner
├── dapr/                 # DAPR integration
├── orchestrator/         # Distributed orchestrator
├── worker/               # Distributed worker
└── telegramhelper/       # Telegram-specific utilities
```

### Key Interfaces
- `crawler.Crawler`: Platform-agnostic crawler interface
- `state.StateManagementInterface`: Storage abstraction
- `client.ClientInterface`: API client abstraction

## Next Steps

- **API Reference**: Detailed documentation of all configuration options
- **Examples**: Common deployment patterns and use cases
- **Contributing**: Guidelines for extending the system with new platforms

---

Understanding the architecture helps you choose the right execution mode and configuration for your use case.
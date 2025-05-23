# Telegram & YouTube Scraper

The **Scraper** is a Go-based application designed to scrape messages and metadata from both Telegram channels and YouTube channels. For Telegram, it uses the TDLib library to collect and process messages. For YouTube, it uses the YouTube Data API. Results are stored in either local files or Azure Blob Storage for further analysis.

## Features

- **Multi-platform Support:**
  - Telegram channels via the TDLib API
  - YouTube channels via the YouTube Data API
- Scrapes and processes messages, videos, metadata, and engagement statistics
- Supports various Telegram message types (e.g., text, video, photo)
- Captures YouTube video metadata including views, likes, and comments
- Saves data in JSONL format for easy integration with data pipelines
- Optionally integrates with Azure Blob Storage for scalable cloud storage
- Supports incremental crawling with progress tracking to resume interrupted crawls
- Skip media downloads option to save bandwidth and storage

---

## Requirements

1. **Go**: Ensure Go is installed on your system.
2. **TDLib**: The Telegram Database Library must be installed for Telegram scraping. See [TDLib Installation](#tdlib-installation) section.
3. **Telegram API Credentials** (for Telegram scraping):
   - `TG_API_ID`
   - `TG_API_HASH`
   - `TG_PHONE_NUMBER`
   - `TG_PHONE_CODE` (OTP sent to your phone)
4. **YouTube API Key** (for YouTube scraping):
   - Google Developer API key with YouTube Data API v3 enabled
5. **Environment Variables**: Set up the following based on your use case.

---

## TDLib Installation

### macOS
```bash
brew install tdlib
```

### Ubuntu/Debian
```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake gperf libssl-dev zlib1g-dev
git clone https://github.com/tdlib/td.git
cd td
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
cmake --build .
sudo make install
```

---

## Environment Variables

### Required for Telegram API
- **`TG_API_ID`**: Telegram API ID (obtained from https://my.telegram.org).
- **`TG_API_HASH`**: Telegram API Hash (obtained from https://my.telegram.org).
- **`TG_PHONE_NUMBER`**: Your Telegram phone number with country code (e.g., +12025551234).
- **`TG_PHONE_CODE`**: OTP sent to your phone by Telegram during authentication.

### Required for YouTube API
- No environment variable required, but you need to provide the YouTube API key via the `--youtube-api-key` parameter when running the scraper with `--platform youtube`.

### Optional for Azure Blob Storage
- **`CONTAINER_NAME`**: Name of the Azure Blob Storage container.
- **`BLOB_NAME`**: Name of the blob path to store scraped data.
- **`AZURE_STORAGE_ACCOUNT_URL`**: Azure Storage account URL.

### Optional for Custom Configuration
- **`SEED_LIST`**: Comma-separated list of channel usernames/IDs to scrape (or provide as a command-line argument with `--urls`).
- **`STORAGE_DIR`**: Custom directory path for local storage (default: `./storage`).
- **`MAX_MESSAGES`**: Maximum number of messages to scrape per channel (default: all messages).
- **`LOG_LEVEL`**: Logging level (default: "info", options: "debug", "info", "warn", "error").

---

## Directory Structure

- **`/storage`**: Default local directory for storing progress and scraped data.
   - `/crawls/{crawlid}/channel_name/data.jsonl`: Output for scraped channel data.
   - `/crawls/{crawlid}/progress.json`: Tracks crawling progress for resumption.

---

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/your-repo/telegram-scraper.git
   cd telegram-scraper
   ```
2. Install Dependencies:
    ```bash
   go mod tidy
   ```

3. Build the project:
   ```bash
   go build -o telegram-scraper
   ```

## Usage

### CLI Commands and Options

The scraper supports the following command-line arguments:

```
Usage: ./telegram-scraper [options]

Options:
  --urls string                  Comma-separated list of channel usernames/IDs to scrape
  --url-file string              File containing URLs to crawl (one per line)
  --crawl-id string              Specify a custom crawl ID for tracking (default: auto-generated)
  --crawl-label string           User-defined label for the crawl (e.g., "youtube-snowball")
  --storage-root string          Directory for storing data locally (default: "/tmp/crawl")
  --max-posts int                Maximum number of posts to collect per channel (default: all)
  --max-comments int             Maximum number of comments to crawl per post (default: all)
  --max-depth int                Maximum depth of the crawl (default: all)
  --min-post-date string         Minimum post date to crawl (format: YYYY-MM-DD)
  --time-ago string              Only consider posts newer than this time ago (e.g., '30d', '6h', '2w', '1m', '1y')
  --skip-media                   Skip downloading media files (thumbnails, videos, etc.)
  --platform string              Platform to crawl (telegram, youtube) (default: "telegram")
  --youtube-api-key string       API key for YouTube Data API (required for YouTube platform)
  --log-level string             Set logging level: trace, debug, info, warn, error (default: "debug")
  --dapr                         Run with DAPR enabled
  --help                         Display this help message
```

### Running the Scraper

#### Basic Telegram Usage

Run the scraper with a list of Telegram channel usernames:

```bash
./telegram-scraper --urls "channel1,channel2,channel3"
```

#### With Environment Setup on macOS

For macOS users, you need to set specific environment variables for the TDLib compilation:

```bash
export CGO_CFLAGS=-I/opt/homebrew/include
export CGO_LDFLAGS=-L/opt/homebrew/lib -lssl -lcrypto
./telegram-scraper --urls "channel1,channel2,channel3"
```

#### YouTube Scraping

To scrape YouTube channels, you need to provide your YouTube API key:

```bash
./telegram-scraper --platform youtube --youtube-api-key "YOUR_API_KEY" --urls "UCxxx,UCyyy"
```

Where:
- `UCxxx,UCyyy` are YouTube channel IDs (starting with UC)
- You can also use channel handles (starting with @) or custom URLs

#### Skipping Media Downloads

To save bandwidth and storage, you can skip media downloads:

```bash
./telegram-scraper --urls "channel1,channel2" --skip-media
```

#### Resuming a Crawl

To resume an interrupted crawl:

```bash
./telegram-scraper --urls "channel1,channel2" --crawl-id "your-previous-crawl-id"
```

#### Limiting Post Count

To limit the number of posts scraped per channel:

```bash
./telegram-scraper --urls "channel1,channel2" --max-posts 1000
```

#### Filtering by Date

To only scrape posts after a specific date:

```bash
./telegram-scraper --urls "channel1,channel2" --min-post-date "2023-01-01"
```

Or using relative time:

```bash
./telegram-scraper --urls "channel1,channel2" --time-ago "30d"
```

#### Custom Storage Directory

To specify a custom storage location:

```bash
./telegram-scraper --urls "channel1" --storage-root "/path/to/custom/dir"
```

### Azure Blob Storage Integration

If Azure Blob Storage is enabled via environment variables (CONTAINER_NAME, BLOB_NAME, and AZURE_STORAGE_ACCOUNT_URL), the scraper will automatically upload data to the specified container and blob.

To explicitly enable Azure upload:

```bash
./telegram-scraper --urls "channel1" --dapr
```

### Authentication Flow

When running the scraper for the first time:

1. Enter your phone number when prompted (with country code, e.g., +12025551234)
2. Enter the authentication code sent to your Telegram app
3. If you have Two-Factor Authentication enabled, enter your password when prompted

The auth session will be saved locally for future use.

## Architecture and Key Components

### Core Components

* `main.go`: Entry point for the application, CLI command setup, and configuration processing
* `crawl/`: Core crawling logic for Telegram
* `crawler/`: Platform-agnostic crawler interfaces and factories
  * `crawler/youtube/`: YouTube-specific crawler implementation
* `telegramhelper/`: Handles TDLib client connections and Telegram API interactions
* `state/`: Manages progress, seed list setup, and storage (local and Azure Blob)
* `model/`: Defines unified data structures for storing messages from all platforms
  * `model/youtube/`: YouTube-specific data models
* `common/`: Shared utilities, configuration structures, and helper functions
* `standalone/`: Runner implementation for standalone mode execution
* `dapr/`: DAPR integration for cloud-based operation

### Key Interfaces

* `crawler.Crawler`: Common interface for all platform crawlers
* `crawler.CrawlerFactory`: Factory for creating platform-specific crawlers
* `state.StateManagementInterface`: Interface for managing state across different storage backends
* `state.StateManagerFactory`: Factory for creating state managers based on configuration


## Examples

### Telegram Example

To scrape Telegram data for @examplechannel and @anotherchannel, storing results in Azure Blob Storage:

1. Set environment variables:
```bash
export TG_API_ID="your_api_id"
export TG_API_HASH="your_api_hash"
export TG_PHONE_NUMBER="your_phone_number"
export TG_PHONE_CODE="your_telegram_code"
export CONTAINER_NAME="your_container_name"
export BLOB_NAME="your_blob_path"
export AZURE_STORAGE_ACCOUNT_URL="https://youraccount.blob.core.windows.net"
```

2. Run the scraper:
```bash
./telegram-scraper --urls "examplechannel,anotherchannel"
```

### YouTube Example

To scrape YouTube videos from specific channels:

1. Set environment variables for storage (optional):
```bash
export CONTAINER_NAME="your_container_name" 
export BLOB_NAME="your_blob_path"
export AZURE_STORAGE_ACCOUNT_URL="https://youraccount.blob.core.windows.net"
```

2. Run the scraper with your YouTube API key:
```bash
./telegram-scraper --platform youtube --youtube-api-key "YOUR_API_KEY" --urls "UCxxx1,UCxxx2"
```

3. Run with additional parameters:
```bash
# Scrape only videos from the last 90 days with a custom label
./telegram-scraper --platform youtube --youtube-api-key "YOUR_API_KEY" --urls "UCxxx1,UCxxx2" --time-ago "90d" --crawl-label "tech-channels-q1"

# Limit to 100 videos per channel
./telegram-scraper --platform youtube --youtube-api-key "YOUR_API_KEY" --urls "UCxxx1,UCxxx2" --max-posts 100
```

## Data Storage Format

### Telegram Data Format

The scraper outputs Telegram data in JSONL format with the following structure:

```json
{
  "post_uid": "12345-examplechannel",
  "url": "https://t.me/examplechannel/12345",
  "channel_id": "1001234567890",
  "channel_name": "Example Channel",
  "published_at": "2023-04-01T12:34:56Z",
  "description": "Example message content",
  "thumb_url": "https://storage.example.com/thumbnails/abc123.jpg",
  "media_url": "https://storage.example.com/videos/def456.mp4",
  "views_count": 1000,
  "comments_count": 25,
  "shares_count": 50,
  "engagement": 1075,
  "platform_name": "Telegram",
  "post_type": ["video"],
  "outlinks": ["channel1", "channel2"],
  "reactions": {"👍": 20, "❤️": 15},
  "comments": [
    {
      "comment_id": "1234",
      "author": "User1",
      "text": "Great post!",
      "created_at": "2023-04-01T12:40:00Z"
    }
  ],
  "channel_data": {
    "channel_id": "1001234567890",
    "channel_name": "Example Channel",
    "channel_url_external": "https://t.me/c/examplechannel",
    "channel_engagement_data": {
      "follower_count": 50000,
      "post_count": 1500,
      "views_count": 2000000
    }
  }
}
```

### YouTube Data Format

The scraper outputs YouTube data in a similar JSONL format:

```json
{
  "post_uid": "video123-channel456",
  "url": "https://www.youtube.com/watch?v=video123",
  "channel_id": "UCchannel456",
  "channel_name": "Example YouTube Channel",
  "published_at": "2023-04-01T12:34:56Z",
  "description": "Example video description",
  "thumb_url": "https://i.ytimg.com/vi/video123/maxresdefault.jpg",
  "media_url": "",
  "views_count": 10000,
  "comments_count": 250,
  "likes_count": 500,
  "engagement": 10750,
  "platform_name": "YouTube",
  "post_type": ["video"],
  "outlinks": [],
  "comments": [
    {
      "comment_id": "comment789",
      "author": "YouTubeUser1",
      "text": "Great video!",
      "created_at": "2023-04-01T13:00:00Z"
    }
  ],
  "channel_data": {
    "channel_id": "UCchannel456",
    "channel_name": "Example YouTube Channel",
    "channel_url_external": "https://www.youtube.com/channel/UCchannel456",
    "channel_description": "This is an example YouTube channel",
    "channel_engagement_data": {
      "follower_count": 100000,
      "video_count": 500
    }
  },
  "crawl_label": "tech-channels-q1"
}
```

## Handling Rate Limits and Errors

### Telegram Rate Limits

The scraper implements exponential backoff for handling rate limits from the Telegram API. If you encounter persistent rate limiting:

- Reduce the number of channels in your seed list
- Increase delay between requests by modifying the code in `telegramhelper/client.go`
- Consider using a different Telegram account with fewer API calls

### YouTube API Quota Limits

YouTube Data API has strict quota limits (typically 10,000 units per day for a new API key):

- Each search request costs 100 units
- Each video details request costs 1 unit
- Each comment thread request costs 1 unit

To avoid quota exhaustion:
- Limit the number of channels you scrape in a single run
- Use the `--max-posts` parameter to limit videos per channel
- Consider using multiple API keys for larger scraping jobs

## Troubleshooting

- **Authentication Issues**: Ensure Telegram API credentials are correct. Delete the `.tdlib` directory to restart authentication.
- **TDLib Errors**: Check that TDLib is properly installed and accessible.
- **YouTube API Key Issues**: Verify your API key is valid and has YouTube Data API v3 enabled.
- **YouTube API Quota Exceeded**: Wait until your quota resets (usually at midnight Pacific Time) or use a different API key.
- **Storage Errors**: Verify write permissions to the storage directory.
- **Azure Upload Failures**: Confirm your Azure Blob Storage configuration and credentials.
- **macOS Compilation Errors**: Set the required CGO environment variables as described in the Usage section.
- **Log Analysis**: Set `--log-level debug` for more detailed logging information.

## License

This project is licensed under Apache 2.0 License.

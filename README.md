# Telegram Scraper

The **Telegram Scraper** is a Go-based application designed to scrape messages and metadata from Telegram channels using the TDLib library. It collects and processes messages, storing the results in either local files or Azure Blob Storage for further analysis.

## Features

- Connects to Telegram channels via the TDLib API.
- Scrapes and processes messages, metadata, and engagement statistics.
- Supports various message types (e.g., text, video, photo).
- Saves data in JSONL format for easy integration with data pipelines.
- Optionally integrates with Azure Blob Storage for scalable cloud storage.
- Supports incremental crawling with progress tracking to resume interrupted crawls.

---

## Requirements

1. **Go**: Ensure Go is installed on your system.
2. **TDLib**: The Telegram Database Library must be installed. See [TDLib Installation](#tdlib-installation) section.
3. **Telegram API Credentials**: You will need:
   - `TG_API_ID`
   - `TG_API_HASH`
   - `TG_PHONE_NUMBER`
   - `TG_PHONE_CODE` (OTP sent to your phone)

4. **Environment Variables**: Set up the following based on your use case.

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

### Optional for Azure Blob Storage
- **`CONTAINER_NAME`**: Name of the Azure Blob Storage container.
- **`BLOB_NAME`**: Name of the blob path to store scraped data.
- **`AZURE_STORAGE_ACCOUNT_URL`**: Azure Storage account URL.

### Optional for Custom Configuration
- **`SEED_LIST`**: Comma-separated list of Telegram channel usernames to scrape (or provide as a command-line argument with `-seed-list`).
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
  -seed-list string      Comma-separated list of Telegram channel usernames to scrape
  -crawl-id string       Specify a custom crawl ID for tracking (default: auto-generated UUID)
  -storage-dir string    Directory for storing data locally (default: "./storage")
  -max-messages int      Maximum number of messages to scrape per channel (default: all)
  -resume                Resume from the last successful crawl point (default: false)
  -azure                 Enable Azure Blob Storage upload (default: false if env vars not set)
  -log-level string      Set logging level: debug, info, warn, error (default: "info")
  -help                  Display this help message
```

### Running the Scraper


#### Basic Usage

Run the scraper with a seed list of channel usernames:

```bash
./telegram-scraper -seed-list "channel1,channel2,channel3"
```

#### With Environment Setup on macOS

For macOS users, you need to set specific environment variables for the TDLib compilation:

```bash
export CGO_CFLAGS=-I/opt/homebrew/include
export CGO_LDFLAGS=-L/opt/homebrew/lib -lssl -lcrypto
./telegram-scraper -seed-list "channel1,channel2,channel3"
```

#### Resuming a Crawl

To resume an interrupted crawl:

```bash
./telegram-scraper -resume -crawl-id "your-previous-crawl-id"
```

#### Limiting Message Count

To limit the number of messages scraped per channel:

```bash
./telegram-scraper -seed-list "channel1,channel2" -max-messages 1000
```

#### Custom Storage Directory

To specify a custom storage location:

```bash
./telegram-scraper -seed-list "channel1" -storage-dir "/path/to/custom/dir"
```

### Azure Blob Storage Integration

If Azure Blob Storage is enabled via environment variables (CONTAINER_NAME, BLOB_NAME, and AZURE_STORAGE_ACCOUNT_URL), the scraper will automatically upload data to the specified container and blob.

To explicitly enable Azure upload:

```bash
./telegram-scraper -seed-list "channel1" -azure
```

### Authentication Flow

When running the scraper for the first time:

1. Enter your phone number when prompted (with country code, e.g., +12025551234)
2. Enter the authentication code sent to your Telegram app
3. If you have Two-Factor Authentication enabled, enter your password when prompted

The auth session will be saved locally for future use.

## Key Files and Functions

* `main.go`: Entry point for the application, managing scraping and progress tracking.
* `telegramhelper/`: Handles TDLib client connections and Telegram API interactions.
* `state/`: Manages progress, seed list setup, and storage (local and Azure Blob).
* `models/`: Defines data structures for storing and processing Telegram messages.
* `storage/`: Implements storage adapters for both local and cloud storage.

## Data Output Format

The scraper outputs data in JSONL format with the following structure:

```json
{
  "message_id": 12345,
  "channel_id": 1001234567890,
  "channel_username": "example_channel",
  "date": "2023-04-01T12:34:56Z",
  "content": {
    "type": "text",
    "text": "Example message content",
    "media_url": "",
    "media_type": ""
  },
  "views": 1000,
  "forwards": 50,
  "replies": 25,
  "is_forwarded": false,
  "forward_source": "",
  "metadata": {
    "scraped_at": "2023-04-02T09:00:00Z",
    "crawl_id": "abc123def456"
  }
}
```

## Example

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
./telegram-scraper -seed-list "examplechannel,anotherchannel"
```

## Handling Rate Limits and Errors

The scraper implements exponential backoff for handling rate limits from the Telegram API. If you encounter persistent rate limiting:

- Reduce the number of channels in your seed list
- Increase delay between requests by modifying the code in `telegramhelper/client.go`
- Consider using a different Telegram account with fewer API calls

## Troubleshooting

- **Authentication Issues**: Ensure Telegram API credentials are correct. Delete the `./tdlib-db` directory to restart authentication.
- **TDLib Errors**: Check that TDLib is properly installed and accessible.
- **Storage Errors**: Verify write permissions to the storage directory.
- **Azure Upload Failures**: Confirm your Azure Blob Storage configuration and credentials.
- **macOS Compilation Errors**: Set the required CGO environment variables as described in the Usage section.
- **Log Analysis**: Set `-log-level debug` for more detailed logging information.

## License

This project is licensed under Apache 2.0 License.

---
layout: default
title: "Getting Started"
---

# Getting Started

This guide will help you set up and run the Telegram Scraper for the first time.

## Prerequisites

Before you begin, ensure you have the following installed:

- **Go 1.19+**: [Download and install Go](https://golang.org/dl/)
- **TDLib** (for Telegram scraping): See [TDLib Installation](#tdlib-installation)
- **Git**: For cloning the repository

## TDLib Installation

### macOS (Homebrew)
```bash
brew install tdlib
```

### Ubuntu/Debian
```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake gperf libssl-dev zlib1g-dev
git clone https://github.com/tdlib/td.git
cd td
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
cmake --build .
sudo make install
```

### Windows
See the [TDLib documentation](https://tdlib.github.io/td/build.html) for Windows installation instructions.

## Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/your-username/telegram-scraper.git
   cd telegram-scraper
   ```

2. **Install dependencies**:
   ```bash
   go mod tidy
   ```

3. **Build the application**:
   ```bash
   go build -o telegram-scraper
   ```

   For macOS users, you may need to set environment variables:
   ```bash
   export CGO_CFLAGS=-I/opt/homebrew/include
   export CGO_LDFLAGS=-L/opt/homebrew/lib -lssl -lcrypto
   go build -o telegram-scraper
   ```

## Configuration

### Environment Variables

#### For Telegram Scraping
Set these environment variables for Telegram API access:

```bash
export TG_API_ID="your_api_id"
export TG_API_HASH="your_api_hash"
export TG_PHONE_NUMBER="+1234567890"
export TG_PHONE_CODE="123456"  # OTP from Telegram
```

**Getting Telegram API credentials:**
1. Visit [my.telegram.org](https://my.telegram.org)
2. Log in with your phone number
3. Go to "API Development Tools"
4. Create a new application to get your API ID and Hash

#### For YouTube Scraping
No environment variables needed, but you'll need a YouTube API key:

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select existing one
3. Enable the YouTube Data API v3
4. Create credentials (API key)
5. Use the key with `--youtube-api-key` parameter

#### Optional: Azure Blob Storage
For cloud storage integration:

```bash
export CONTAINER_NAME="your_container_name"
export BLOB_NAME="your_blob_path"
export AZURE_STORAGE_ACCOUNT_URL="https://youraccount.blob.core.windows.net"
```

## First Run

### Telegram Example

1. **Basic scraping**:
   ```bash
   ./telegram-scraper --urls "channel1,channel2,channel3"
   ```

2. **With time filtering**:
   ```bash
   ./telegram-scraper --urls "channel1,channel2" --time-ago "30d"
   ```

3. **Limit posts per channel**:
   ```bash
   ./telegram-scraper --urls "channel1,channel2" --max-posts 1000
   ```

### YouTube Example

```bash
./telegram-scraper --platform youtube \
  --youtube-api-key "YOUR_API_KEY" \
  --urls "UCxxx1,UCxxx2" \
  --time-ago "90d"
```

## Authentication Flow

### Telegram
On first run, you'll be prompted to:

1. Enter your phone number (with country code)
2. Enter the verification code sent to your Telegram app
3. If 2FA is enabled, enter your password

The authentication session will be saved for future use.

### YouTube
No interactive authentication required - just provide your API key.

## Understanding the Output

The scraper creates a directory structure like this:

```
storage/
└── crawls/
    └── {crawl-id}/
        ├── progress.json
        ├── channel1/
        │   └── data.jsonl
        └── channel2/
            └── data.jsonl
```

Each `data.jsonl` file contains one JSON object per line with comprehensive message data.

## Command Line Options

| Option | Description | Example |
|--------|-------------|---------|
| `--urls` | Comma-separated list of channels | `"channel1,channel2"` |
| `--url-file` | File containing URLs (one per line) | `channels.txt` |
| `--platform` | Platform to scrape | `telegram` or `youtube` |
| `--max-posts` | Maximum posts per channel | `1000` |
| `--time-ago` | Only posts newer than this | `30d`, `6h`, `2w` |
| `--storage-root` | Custom storage directory | `/path/to/storage` |
| `--skip-media` | Skip downloading media files | (flag only) |
| `--log-level` | Logging verbosity | `debug`, `info`, `warn` |

## Troubleshooting

### Common Issues

**"TDLib not found" error**:
- Ensure TDLib is properly installed
- On macOS, verify Homebrew installation: `brew list tdlib`
- Check that CGO environment variables are set correctly

**Authentication failures**:
- Verify your API credentials are correct
- Delete the `.tdlib` directory to restart authentication
- Ensure your phone number includes the country code

**YouTube API quota exceeded**:
- Check your API usage in Google Cloud Console
- Wait for quota reset (usually midnight Pacific Time)
- Consider using multiple API keys for large jobs

**Permission denied errors**:
- Check write permissions to the storage directory
- Use `--storage-root` to specify a writable location

### Getting Help

- **Issues**: Report bugs on [GitHub Issues](https://github.com/your-username/telegram-scraper/issues)
- **Logs**: Use `--log-level debug` for detailed troubleshooting information
- **Documentation**: Check the [Architecture Guide](architecture/) for advanced configuration

## Next Steps

- Read the [Architecture Documentation](architecture/) to understand how the system works
- Explore the [API Reference](api-reference/) for advanced configuration options
- Check out [Examples](examples/) for common use cases and configuration templates

---

Need help? Check our [troubleshooting guide](troubleshooting/) or [open an issue](https://github.com/your-username/telegram-scraper/issues) on GitHub.
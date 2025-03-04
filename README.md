# Telegram Scraper

The **Telegram Scraper** is a Go-based application designed to scrape messages and metadata from Telegram channels using the TDLib library. It collects and processes messages, storing the results in either local files or Azure Blob Storage for further analysis.

## Features

- Connects to Telegram channels via the TDLib API.
- Scrapes and processes messages, metadata, and engagement statistics.
- Supports various message types (e.g., text, video, photo).
- Saves data in JSONL format for easy integration with data pipelines.
- Optionally integrates with Azure Blob Storage for scalable cloud storage.

---

## Requirements

1. **Go**: Ensure Go is installed on your system.
2. **Telegram API Credentials**: You will need:
    - `TG_API_ID`
    - `TG_API_HASH`
    - `TG_PHONE_NUMBER`
    - `TG_PHONE_CODE` (OTP sent to your phone)

3. **Environment Variables**: Set up the following based on your use case.

---

## Environment Variables

### Required for Telegram API
- **`TG_API_ID`**: Telegram API ID.
- **`TG_API_HASH`**: Telegram API Hash.
- **`TG_PHONE_NUMBER`**: Your Telegram phone number.
- **`TG_PHONE_CODE`**: OTP sent to your phone by Telegram.

### Optional for Azure Blob Storage
- **`CONTAINER_NAME`**: Name of the Azure Blob Storage container.
- **`BLOB_NAME`**: Name of the blob path to store scraped data.
- **`AZURE_STORAGE_ACCOUNT_URL`**: Azure Storage account URL.

### Optional for Custom Configuration
- **`SEED_LIST`**: Comma-separated list of Telegram channel usernames to scrape (or provide as a command-line argument with `-seed-list`).

---

## Directory Structure

- **`/storage`**: Default local directory for storing progress and scraped data.
    - `/crawls/{crawlid}/channel_name/data.jsonl`: Output for scraped channel data.

---

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/your-repo/tdlib-scraper.git
   cd tdlib-scraper
   ```
2. Install Dependencies:
    ```bash
   go mod tidy
   ```

3. Build the project:
   ```bash
   go build -o tdlib-scraper
   ```

## Usage

### Running the Scraper

Run the scraper with the necessary environment variables and seed list:

*Important*
For reasons I'm not quite sure of to run the app I need to set the following env vars on OSX:

```bash
export CGO_CFLAGS=-I/opt/homebrew/include
export CGO_LDFLAGS=-L/opt/homebrew/lib -lssl -lcrypto
```

```bash
./tdlib-scraper -seed-list "channel1,channel2,channel3"
```

### Azure Blob Storage Integration

If Azure Blob Storage is enabled via environment variables (CONTAINER_NAME, BLOB_NAME, and AZURE_STORAGE_ACCOUNT_URL), the scraper will automatically upload data to the specified container and blob.

### Local Storage

If Azure Blob Storage is not configured, data will be saved locally in the storage directory under a folder named with the generated crawl ID.

## Key Files and Functions
* main.go: Entry point for the application, managing scraping and progress tracking.
* telegramhelper/: Handles TDLib client connections and Telegram API interactions.
* state/: Manages progress, seed list setup, and storage (local and Azure Blob).


## Example

To scrape Telegram data for @examplechannel and store it in Azure Blob Storage:
1.	Set environment variables:
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
./tdlib-scraper -seed-list "examplechannel"
```

## Troubleshooting
•	Ensure Telegram API credentials are correct.
•	Check your Azure Blob Storage configuration if data is not uploading.
•	Review log messages (zerolog) for detailed error information.

## License

This project is licensed under Apache 2.0 License.

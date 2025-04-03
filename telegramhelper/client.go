package telegramhelper

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/rs/zerolog/log"
	"github.com/zelenin/go-tdlib/client"
	"hash/fnv"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// TelegramService defines an interface for interacting with the Telegram client.
// This abstraction allows for both real implementations with TDLib and mock
// implementations for testing.
//
// The interface provides methods for:
// - Initializing Telegram clients with different configurations
// - Retrieving authenticated user information
//
// By using this interface, components that need Telegram functionality can
// be tested with mock implementations, and the actual TDLib implementation
// can be easily swapped if needed.
type TelegramService interface {
	// InitializeClient creates and initializes a basic Telegram client with default settings.
	//
	// Parameters:
	//   - storagePrefix: Base directory for storing TDLib database and files
	//
	// Returns:
	//   - An initialized TDLib client
	//   - An error if initialization fails
	InitializeClient(storagePrefix string) (crawler.TDLibClient, error)
	
	// InitializeClientWithConfig creates and initializes a Telegram client with custom configuration.
	// This allows for more control over client behavior and storage options.
	//
	// Parameters:
	//   - storagePrefix: Base directory for storing TDLib database and files
	//   - cfg: Configuration options controlling client behavior and features
	//
	// Returns:
	//   - An initialized TDLib client
	//   - An error if initialization fails
	InitializeClientWithConfig(storagePrefix string, cfg common.CrawlerConfig) (crawler.TDLibClient, error)
	
	// GetMe retrieves information about the authenticated user.
	// This is typically used to verify successful authentication.
	//
	// Parameters:
	//   - libClient: An initialized TDLib client
	//
	// Returns:
	//   - User information for the authenticated user
	//   - An error if retrieval fails
	GetMe(libClient crawler.TDLibClient) (*client.User, error)
}

// RealTelegramService is the concrete implementation of the TelegramService interface
// that uses the TDLib library for authenticating and communicating with Telegram servers.
//
// This service handles:
// - Client initialization with proper credentials
// - Authentication flow management
// - Session management using local database storage
// - Loading pre-seeded TDLib databases for faster startup
//
// The implementation supports both development and production use cases,
// with options for configuring database locations and authentication methods.
type RealTelegramService struct{}

// InitializeClient sets up a real TDLib client
func (s *RealTelegramService) InitializeClient(storagePrefix string) (crawler.TDLibClient, error) {
	return s.InitializeClientWithConfig(storagePrefix, common.CrawlerConfig{})
}

// Credentials stores Telegram API authentication details necessary for
// connecting to the Telegram API. This structure is used for both reading
// credentials from a file and for in-memory storage during client initialization.
//
// The credentials include:
// - API ID and hash obtained from the Telegram developer portal
// - Phone number for account authentication
// - Phone code received via SMS or Telegram during authentication
//
// These credentials are sensitive and should be handled securely.
// The structure is designed to be serialized to/from JSON for persistent storage.
type Credentials struct {
	APIId       string `json:"api_id"`      // Telegram API ID obtained from developer portal
	APIHash     string `json:"api_hash"`    // Telegram API hash obtained from developer portal
	PhoneNumber string `json:"phone_number"` // User's phone number in international format
	PhoneCode   string `json:"phone_code"`  // One-time code received via SMS or Telegram
}

// readCredentials loads Telegram API authentication details from a JSON file.
// The credentials file provides a way to store and reuse authentication details
// without hardcoding them in the application or relying on environment variables.
//
// The function looks for a file named "credentials.json" in the ".tdlib" directory
// in the current working directory. This file should contain a JSON representation
// of the Credentials struct.
//
// Returns:
//   - A pointer to a Credentials struct containing the authentication details
//   - An error if the file doesn't exist, can't be read, or contains invalid JSON
//
// This function is used during client initialization to obtain the necessary
// credentials for authenticating with the Telegram API. If the file doesn't exist
// or contains invalid data, the function will return an error, prompting the
// application to fall back to environment variables for credentials.
func readCredentials() (*Credentials, error) {
	credsPath := filepath.Join(".tdlib", "credentials.json")

	// Check if credentials file exists
	if _, err := os.Stat(credsPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("credentials file not found at %s", credsPath)
	}

	// Read the file
	data, err := os.ReadFile(credsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read credentials file: %w", err)
	}

	// Parse the JSON
	var creds Credentials
	if err := json.Unmarshal(data, &creds); err != nil {
		return nil, fmt.Errorf("failed to parse credentials JSON: %w", err)
	}

	return &creds, nil
}

// SetupAuth prepares the environment for Telegram authentication by setting
// environment variables that will be used by the CLI interactor during the
// TDLib authentication flow.
//
// Parameters:
//   - phoneNumber: The phone number associated with the Telegram account
//   - phoneCode: The verification code received via SMS or Telegram during login
//
// The function uses environment variables as a simple and non-intrusive way to
// pass authentication information to the TDLib client library without requiring
// modifications to the library itself. It sets:
//   - TG_PHONE_NUMBER: The phone number in international format
//   - TG_PHONE_CODE: The one-time verification code
//
// These environment variables are later read by the standard CLI interactor
// provided by the TDLib client library when it needs to perform authentication steps.
// This approach avoids having to implement a custom interactor while still
// providing a way to automate the authentication process.
//
// Only non-empty values will be set as environment variables to avoid overriding
// existing values with empty ones.
func SetupAuth(phoneNumber, phoneCode string) {
	// Set environment variables for the CLI interactor to use
	if phoneNumber != "" {
		os.Setenv("TG_PHONE_NUMBER", phoneNumber)
		log.Debug().
			Str("phone_number_masked", maskPhoneNumber(phoneNumber)).
			Msg("Set TG_PHONE_NUMBER environment variable for authentication")
	} else {
		log.Debug().Msg("No phone number provided, will use existing TG_PHONE_NUMBER or prompt user")
	}
	
	if phoneCode != "" {
		os.Setenv("TG_PHONE_CODE", phoneCode)
		log.Debug().Msg("Set TG_PHONE_CODE environment variable for authentication")
	} else {
		log.Debug().Msg("No phone code provided, will use existing TG_PHONE_CODE or prompt user")
	}
}

// maskPhoneNumber hides most digits of a phone number for security in logs
func maskPhoneNumber(phoneNumber string) string {
	if len(phoneNumber) <= 4 {
		return "***" // Too short to mask meaningfully
	}
	
	// Keep country code (first few digits) and last 2 digits
	visiblePrefix := 3
	if len(phoneNumber) > 10 {
		visiblePrefix = 4 // For international format with + and country code
	}
	
	masked := phoneNumber[:visiblePrefix]
	for i := visiblePrefix; i < len(phoneNumber)-2; i++ {
		masked += "*"
	}
	masked += phoneNumber[len(phoneNumber)-2:]
	
	return masked
}

// InitializeClientWithConfig creates and initializes a TDLib client with custom configuration.
// This method handles the complex process of setting up a Telegram client, including
// authentication, database setup, and pre-seeding from existing database archives.
//
// Parameters:
//   - storagePrefix: Base directory for storing TDLib database and files
//   - cfg: Configuration containing options like database URL for pre-seeding
//
// Returns:
//   - An initialized and authenticated TDLib client ready for use
//   - An error if any step of the initialization process fails
//
// The function supports several advanced features:
//   - Loading pre-seeded TDLib databases from remote URLs for faster startup
//   - Reading authentication credentials from a credentials file or environment variables
//   - Creating unique database directories for each client instance
//   - Handling the complete Telegram authentication flow
//
// Connection timeouts ensure the process doesn't hang indefinitely if authentication
// or connection problems occur. If authentication requires user interaction for phone code,
// the function will prompt for input through the CLI interactor.
func (s *RealTelegramService) InitializeClientWithConfig(storagePrefix string, cfg common.CrawlerConfig) (crawler.TDLibClient, error) {
	authorizer := client.ClientAuthorizer()
	
	// We'll use the default CLI interactor but prepare environment variables
	// so we need to track the phoneCode for later

	// Generate a unique subfolder for this connection if a database URL is provided
	uniqueSubfolder := ""
	if cfg.TDLibDatabaseURL != "" {
		// Create a unique subfolder based on the URL hash
		h := fnv.New32a()
		h.Write([]byte(cfg.TDLibDatabaseURL))
		uniqueSubfolder = fmt.Sprintf("conn_%d", h.Sum32())

		// Create the full unique path
		uniquePath := filepath.Join(storagePrefix, "state", uniqueSubfolder)

		// Ensure the directory exists
		err := os.MkdirAll(uniquePath, 0755)
		if err != nil {
			log.Warn().Err(err).Msgf("Failed to create unique directory %s for database", uniquePath)
		}

		// Download and extract to the unique directory
		if err := downloadAndExtractTarball(cfg.TDLibDatabaseURL, uniquePath); err != nil {
			log.Warn().Err(err).Msg("Failed to download and extract pre-seeded TDLib database, proceeding with fresh database")
			// Continue with a fresh database even if download fails
		} else {
			log.Info().Msgf("Successfully downloaded and extracted pre-seeded TDLib database to %s", uniquePath)
		}
	}

	// Try to read credentials from file first
	var apiID int
	var apiHash string
	var phoneNumber, phoneCode string

	creds, err := readCredentials()
	if err == nil && creds != nil {
		log.Info().Msg("Using API credentials from stored file")
		apiID, err = strconv.Atoi(creds.APIId)
		if err != nil {
			log.Warn().Err(err).Msg("Invalid API ID in credentials file")
			return nil, fmt.Errorf("invalid API ID in credentials file: %w", err)
		}
		apiHash = creds.APIHash
		phoneNumber = creds.PhoneNumber
		phoneCode = creds.PhoneCode
	} else {
		// Fall back to environment variables if needed
		log.Info().Msg("Using API credentials from environment variables")
		apiIdStr := os.Getenv("TG_API_ID")
		apiID, err = strconv.Atoi(apiIdStr)
		if err != nil {
			log.Fatal().Err(err).Msg("Error converting TG_API_ID to int")
			return nil, err
		}
		apiHash = os.Getenv("TG_API_HASH")
		phoneNumber = os.Getenv("TG_PHONE_NUMBER")
		phoneCode = os.Getenv("TG_PHONE_CODE")
		
		// Create credentials object to use locally (don't save it)
		creds = &Credentials{
			APIId:       apiIdStr,
			APIHash:     apiHash,
			PhoneNumber: phoneNumber,
			PhoneCode:   phoneCode,
		}
	}

	// Determine database and files directory paths
	var dbDir, filesDir string

	if uniqueSubfolder != "" {
		// Use the unique subfolder path if we created one
		dbDir = filepath.Join(storagePrefix, "state", uniqueSubfolder, ".tdlib", "database")
		filesDir = filepath.Join(storagePrefix, "state", uniqueSubfolder, ".tdlib", "files")
	} else {
		// Use default paths
		dbDir = filepath.Join(storagePrefix, "state", ".tdlib", "database")
		filesDir = filepath.Join(storagePrefix, "state", ".tdlib", "files")
	}

	// Ensure directories exist
	os.MkdirAll(dbDir, 0755)
	os.MkdirAll(filesDir, 0755)

	log.Info().Msgf("Using TDLib database directory: %s", dbDir)

	authorizer.TdlibParameters <- &client.SetTdlibParametersRequest{
		UseTestDc:           false,
		DatabaseDirectory:   dbDir,
		FilesDirectory:      filesDir,
		UseFileDatabase:     true,
		UseChatInfoDatabase: true,
		UseMessageDatabase:  true,
		UseSecretChats:      false,
		ApiId:               int32(apiID),
		ApiHash:             apiHash,
		SystemLanguageCode:  "en",
		DeviceModel:         "Server",
		SystemVersion:       "1.0.0",
		ApplicationVersion:  "1.0.0",
	}

	log.Warn().Msg("ABOUT TO CONNECT TO TELEGRAM. IF YOUR PHONE CODE IS INVALID, YOU MUST RE-RUN WITH A VALID CODE.")

	// Set up authentication environment variables
	// The phone number will be picked up by the default CLI interactor
	SetupAuth(phoneNumber, phoneCode)
	
	// Use the default CLI interactor which will read the environment variables
	go client.CliInteractor(authorizer)

	clientReady := make(chan *client.Client)
	errChan := make(chan error)

	go func() {
		tdlibClient, err := client.NewClient(authorizer)

		verb := client.SetLogVerbosityLevelRequest{NewVerbosityLevel: 1}
		tdlibClient.SetLogVerbosityLevel(&verb)
		if err != nil {
			errChan <- fmt.Errorf("failed to initialize TDLib client: %w", err)
			return
		}
		clientReady <- tdlibClient
	}()

	select {
	case tdlibClient := <-clientReady:
		log.Info().Msg("Client initialized successfully")
		return tdlibClient, nil
	case err := <-errChan:
		log.Fatal().Err(err).Msg("Error initializing client")
		return nil, err
	case <-time.After(30 * time.Second):
		log.Warn().Msg("Timeout reached. Exiting application.")
		return nil, fmt.Errorf("timeout initializing TDLib client")
	}

}

// GetMe retrieves the authenticated Telegram user
func (t *RealTelegramService) GetMe(tdlibClient crawler.TDLibClient) (*client.User, error) {
	user, err := tdlibClient.GetMe()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to retrieve authenticated user")
		return nil, err
	}
	log.Info().Msgf("Logged in as: %s %s", user.FirstName, user.LastName)
	return user, nil
}

// GenCode initializes the TDLib client and retrieves the authenticated user
func GenCode(service TelegramService, storagePrefix string) {
	tdclient, err := service.InitializeClient(storagePrefix)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize TDLib client")
		return
	}
	defer func() {
		if tdclient != nil {
			tdclient.Close()
		}
	}()

	user, err := service.GetMe(tdclient)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to retrieve user information")
		return
	}

	log.Info().Msgf("Authenticated as: %s %s", user.FirstName, user.LastName)
}

// downloadAndExtractTarball downloads a pre-configured TDLib database archive from a URL
// and extracts it to the specified target directory. This is a key feature for performance
// optimization, allowing the application to start with a pre-authenticated TDLib session
// rather than going through the full authentication flow for each new instance.
//
// Parameters:
//   - url: The URL of the gzipped tarball containing a pre-configured TDLib database
//   - targetDir: The directory where the contents should be extracted
//
// Returns:
//   - An error if any step of the download or extraction process fails
//
// The function:
// 1. Downloads the tarball using a standard HTTP GET request with browser-like headers
// 2. Checks for successful HTTP status code (200)
// 3. Passes the response body to downloadAndExtractTarballFromReader for extraction
//
// This approach allows for rapid deployment of new crawler instances with pre-authenticated
// sessions, significantly reducing startup time and avoiding the need for repeated
// authentication steps. It's especially valuable in distributed or containerized environments.
func downloadAndExtractTarball(url, targetDir string) error {
	req, err := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
	req.Header.Set("Accept", "*/*")
	if err != nil {
		return err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("non-200 status returned: %v", resp.Status)
	}

	// Pass the response body to the extraction function
	return downloadAndExtractTarballFromReader(resp.Body, targetDir)
}

// downloadAndExtractTarballFromReader extracts files from a gzip-compressed tarball
// provided by the reader and writes them to the specified target directory.
// It handles directories and regular files, creating necessary directories
// and files as needed. Unknown file types are ignored. Returns an error if
// any operation fails.
func downloadAndExtractTarballFromReader(reader io.Reader, targetDir string) error {
	// Step 1: Decompress the gzip file
	gzReader, err := gzip.NewReader(reader)
	if err != nil {
		return err
	}
	defer gzReader.Close()

	// Step 2: Read the tarball contents
	tarReader := tar.NewReader(gzReader)

	// Step 3: Extract files
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break // End of tar archive
		}
		if err != nil {
			return err
		}

		// Determine target file path
		targetPath := filepath.Join(targetDir, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			err := os.MkdirAll(targetPath, os.ModePerm)
			if err != nil {
				return err
			}
		case tar.TypeReg:
			err := os.MkdirAll(filepath.Dir(targetPath), os.ModePerm)
			if err != nil {
				return err
			}
			file, err := os.Create(targetPath)
			if err != nil {
				return err
			}
			defer file.Close()

			_, err = io.Copy(file, tarReader)
			if err != nil {
				return err
			}
		default:
			log.Debug().Msgf("Ignoring unknown file type: %s\n", header.Name)
		}
	}

	return nil
}

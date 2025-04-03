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

// TelegramService defines an interface for interacting with the Telegram client
type TelegramService interface {
	InitializeClient(storagePrefix string) (crawler.TDLibClient, error)
	InitializeClientWithConfig(storagePrefix string, cfg common.CrawlerConfig) (crawler.TDLibClient, error)
	GetMe(libClient crawler.TDLibClient) (*client.User, error)
}

// RealTelegramService is the actual TDLib implementation
type RealTelegramService struct{}

// InitializeClient sets up a real TDLib client
func (s *RealTelegramService) InitializeClient(storagePrefix string) (crawler.TDLibClient, error) {
	return s.InitializeClientWithConfig(storagePrefix, common.CrawlerConfig{})
}

// Credentials stores Telegram API authentication details
type Credentials struct {
	APIId       string `json:"api_id"`
	APIHash     string `json:"api_hash"`
	PhoneNumber string `json:"phone_number"`
	PhoneCode   string `json:"phone_code"`
}

// readCredentials attempts to read stored credentials from .tdlib/credentials.json
// Returns the credentials if found, or nil if not found or there was an error
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

// CustomCliInteractor handles TDLib authentication flow with custom credentials
// We need a simpler approach without creating interface dependencies
func SetupAuth(phoneNumber, phoneCode string) {
	// Set environment variables for the CLI interactor to use
	if phoneNumber != "" {
		os.Setenv("TG_PHONE_NUMBER", phoneNumber)
	}
	
	if phoneCode != "" {
		os.Setenv("TG_PHONE_CODE", phoneCode)
	}
}

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

// downloadAndExtractTarball downloads a tarball from the specified URL and extracts its contents
// into the target directory. It handles HTTP requests, decompresses gzip files, and processes
// tar archives to create directories and files as needed. Returns an error if any step fails.
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

	// Pass the response body to the new function
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

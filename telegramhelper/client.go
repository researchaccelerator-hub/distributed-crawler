package telegramhelper

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/rs/zerolog/log"
	"github.com/zelenin/go-tdlib/client"
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

func (s *RealTelegramService) InitializeClientWithConfig(storagePrefix string, cfg common.CrawlerConfig) (crawler.TDLibClient, error) {
	authorizer := client.ClientAuthorizer()
	go client.CliInteractor(authorizer)

	if cfg.TDLibDatabaseURL != "" {
		if err := downloadAndExtractTarball(cfg.TDLibDatabaseURL, filepath.Join(storagePrefix, "state")); err != nil {
			log.Warn().Err(err).Msg("Failed to download and extract pre-seeded TDLib database, proceeding with fresh database")
			// Continue with a fresh database even if download fails
		} else {
			log.Info().Msg("Successfully downloaded and extracted pre-seeded TDLib database")
		}
	}
	apiID, err := strconv.Atoi(os.Getenv("TG_API_ID"))
	if err != nil {
		log.Fatal().Err(err).Msg("Error converting TG_API_ID to int")
		return nil, err
	}
	apiHash := os.Getenv("TG_API_HASH")

	authorizer.TdlibParameters <- &client.SetTdlibParametersRequest{
		UseTestDc:           false,
		DatabaseDirectory:   filepath.Join(storagePrefix+"/state", ".tdlib", "database"),
		FilesDirectory:      filepath.Join(storagePrefix+"/state", ".tdlib", "files"),
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

	log.Warn().Msg("ABOUT TO CONNECT TO TELEGRAM. IF YOUR TG_PHONE_CODE IS INVALID, YOU MUST RE-RUN WITH A VALID CODE.")
	// p := os.Getenv("TG_PHONE_NUMBER")
	// pc := os.Getenv("TG_PHONE_CODE")
	// os.Setenv("TG_PHONE_NUMBER", p)
	// os.Setenv("TG_PHONE_CODE", pc)
	// authorizer.PhoneNumber <- p
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

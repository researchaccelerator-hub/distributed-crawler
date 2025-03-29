package telegramhelper

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/researchaccelerator-hub/telegram-scraper/model"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/rs/zerolog/log"
	"github.com/zelenin/go-tdlib/client"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
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

		verb := client.SetLogVerbosityLevelRequest{NewVerbosityLevel: 4}
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

// removeMultimedia removes all files and subdirectories in the specified directory.
// If the directory does not exist, it does nothing.
//
// Parameters:
//   - filedir: The path to the directory whose contents are to be removed.
//
// Returns:
//   - An error if there is a failure during removal; otherwise, nil.
func removeMultimedia(filedir string) error {
	// Check if the directory exists
	info, err := os.Stat(filedir)
	if os.IsNotExist(err) {
		// Directory does not exist, nothing to do
		return nil
	}
	if err != nil {
		return err
	}

	// Ensure it is a directory
	if !info.IsDir() {
		return err
	}

	// Remove contents of the directory
	err = filepath.Walk(filedir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip the root directory itself
		if path == filedir {
			return nil
		}

		// Remove files and subdirectories
		if err := os.RemoveAll(path); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	log.Info().Msgf("Contents of directory %s removed successfully.\n", filedir)
	return nil
}

// processMessageSafely extracts and returns the thumbnail path, video path, and description
// from a given Telegram video message. It ensures the message structure is valid and not corrupt.
//
// Parameters:
// - mymsg: A pointer to a client.MessageVideo object containing the video message details.
// - tdlibClient: A pointer to a client.Client used for potential future operations.
//
// Returns:
// - thumbnailPath: The remote ID of the video's thumbnail.
// - videoPath: The remote ID of the video.
// - description: The text caption of the video.
// - err: An error if the message structure is invalid or corrupt.
func processMessageSafely(mymsg *client.MessageVideo) (thumbnailPath, videoPath, description string, err error) {
	if mymsg == nil || mymsg.Video == nil || mymsg.Video.Thumbnail == nil {
		return "", "", "", fmt.Errorf("invalid or corrupt message structure")
	}

	thumbnailPath = mymsg.Video.Thumbnail.File.Remote.Id
	videoPath = mymsg.Video.Video.Remote.Id
	description = mymsg.Caption.Text
	//thumbnailPath = fetch(tdlibClient, thumbnailPath)
	//videoPath = fetch(tdlibClient, videoPath)
	return thumbnailPath, videoPath, description, nil
}

// fetchAndUploadMedia fetches a media file from Telegram using the provided TDLibClient
// and uploads it to Azure blob storage via the StateManager. It requires the crawl ID,
// channel name, file ID, and post link as inputs. If the file ID is empty, it returns
// immediately with no error. The function returns the file ID upon successful upload,
// or an error if any step fails.
func fetchAndUploadMedia(tdlibClient crawler.TDLibClient, sm state.StateManagementInterface, crawlid, channelName, fileID, postLink string) (string, error) {
	if fileID == "" {
		return "", nil
	}

	path, remoteid, err := fetchfilefromtelegram(tdlibClient, sm, fileID)
	if err != nil {
		log.Error().Err(err).Str("fileID", fileID).Msg("Failed to fetch file from Telegram")
		return "", err
	}

	if path == "" {
		return "", fmt.Errorf("empty path returned from fetch operation")
	}

	fileInfo, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("File %s does not exist\n", path)
		} else {
			fmt.Printf("Error checking file: %v\n", err)
		}
		return "", err
	}

	// Get file size in bytes
	sizeInBytes := fileInfo.Size()

	// Convert to MB (1 MB = 1,048,576 bytes)
	sizeInMB := float64(sizeInBytes) / 1048576.0

	// Check if file is over 200MB
	const fileSizeLimit = 190.0 // MB
	isOverLimit := sizeInMB > fileSizeLimit
	if isOverLimit {
		return "", fmt.Errorf("file size is too large (%d MB)", sizeInMB)
	}
	_, err = sm.StoreFile(channelName, path, remoteid)
	e := os.Remove(path)
	if e != nil {
		log.Error().Err(e).Msg("Failed to remove file")
	}
	removeMultimedia(path)
	if err != nil {
		log.Error().Err(err).Str("path", path).Msg("Failed to upload file to blob storage")
		return "", err
	}

	err = sm.MarkMediaAsProcessed(remoteid)
	if err != nil {
		return "", err
	}

	return remoteid, nil
}

// ParseMessage processes a Telegram message and extracts relevant information to create a Post model.
//
// This function handles various message content types, including text, video, photo, animation, and more.
// It also recovers from potential panics during parsing to ensure the process continues smoothly.
//
// Parameters:
// - message: The Telegram message to be parsed.
// - mlr: The message link associated with the message.
// - chat: The chat information where the message was posted.
// - supergroup: The supergroup information related to the chat.
// - supergroupInfo: Full information about the supergroup.
// - postcount: The number of posts in the channel.
// - viewcount: The number of views for the channel.
// - channelName: The name of the channel.
// - tdlibClient: The Telegram client used for fetching additional data.
//
// Returns:
// - post: A Post model populated with the extracted data.
// - err: An error if the parsing fails.
// In telegramhelper package:
var ParseMessage = func(
	crawlid string,
	message *client.Message,
	mlr *client.MessageLink,
	chat *client.Chat,
	supergroup *client.Supergroup,
	supergroupInfo *client.SupergroupFullInfo,
	postcount int,
	viewcount int,
	channelName string,
	tdlibClient crawler.TDLibClient,
	sm state.StateManagementInterface,
	cfg common.CrawlerConfig,
) (post model.Post, err error) {
	// Defer to recover from panics and ensure the crawl continues
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack() // Get stack trace
			log.Error().
				Str("channel", channelName).
				Interface("panic", r).
				Str("stack", string(stack)).
				Msg("Recovered from panic while parsing message")
			err = fmt.Errorf("failed to parse message: %v", r)
		}
	}()

	// Validate required inputs
	if message == nil {
		return model.Post{}, fmt.Errorf("message is nil")
	}
	if mlr == nil {
		return model.Post{}, fmt.Errorf("message link is nil")
	}
	if chat == nil {
		return model.Post{}, fmt.Errorf("chat is nil")
	}

	publishedAt := time.Unix(int64(message.Date), 0)

	if !cfg.MinPostDate.IsZero() && publishedAt.Before(cfg.MinPostDate) {
		return model.Post{}, nil // Skip messages earlier than MinPostDate
	}

	var messageNumber string
	if mlr.Link != "" {
		linkParts := strings.Split(mlr.Link, "/")
		if len(linkParts) > 0 {
			messageNumber = linkParts[len(linkParts)-1]
		}
	}

	if messageNumber == "" {
		return model.Post{}, fmt.Errorf("could not determine message number")
	}

	// Initialize variables
	comments := make([]model.Comment, 0)
	description := ""
	thumbnailPath := ""
	videoPath := ""

	// Safely fetch comments if available
	if message.InteractionInfo != nil &&
		message.InteractionInfo.ReplyInfo != nil &&
		message.InteractionInfo.ReplyInfo.ReplyCount > 0 {
		fetchedComments, fetchErr := GetMessageComments(tdlibClient, chat.Id, message.Id, channelName, cfg.MaxComments)
		if fetchErr != nil {
			log.Error().Stack().Err(fetchErr).Msg("Failed to fetch comments")
		} else if len(fetchedComments) > 0 {
			comments = fetchedComments
		}
	}

	// Process based on message content type
	if message.Content != nil {
		switch content := message.Content.(type) {
		case *client.MessageText:
			if content != nil && content.Text != nil {
				description = content.Text.Text
			}

		case *client.MessageVideo:
			// Safe processing with nil checks
			if content != nil {
				thumbnailPath, videoPath, description, _ = processMessageSafely(content)

				if thumbnailPath != "" {
					thumbnailPath, _ = fetchAndUploadMedia(tdlibClient, sm, crawlid, channelName, thumbnailPath, mlr.Link)
				}

				if videoPath != "" {
					videoPath, _ = fetchAndUploadMedia(tdlibClient, sm, crawlid, channelName, videoPath, mlr.Link)
				}

				if content.Caption != nil {
					description = content.Caption.Text
				}
			}

		case *client.MessagePhoto:
			if content != nil {
				if content.Caption != nil {
					description = content.Caption.Text
				}

				if content.Photo != nil &&
					len(content.Photo.Sizes) > 0 &&
					content.Photo.Sizes[0].Photo != nil &&
					content.Photo.Sizes[0].Photo.Remote != nil {
					thumbnailPath = content.Photo.Sizes[0].Photo.Remote.Id
					if thumbnailPath != "" {
						thumbnailPath, _ = fetchAndUploadMedia(tdlibClient, sm, crawlid, channelName, thumbnailPath, mlr.Link)
					}
				}
			}

		case *client.MessageAnimation:
			if content != nil {
				if content.Caption != nil {
					description = content.Caption.Text
				}

				if content.Animation != nil &&
					content.Animation.Thumbnail != nil &&
					content.Animation.Thumbnail.File != nil &&
					content.Animation.Thumbnail.File.Remote != nil {
					thumbnailPath = content.Animation.Thumbnail.File.Remote.Id
					if thumbnailPath != "" {
						thumbnailPath, _ = fetchAndUploadMedia(tdlibClient, sm, crawlid, channelName, thumbnailPath, mlr.Link)
					}
				}
			}

		case *client.MessageAnimatedEmoji:
			if content != nil {
				description = content.Emoji
			}

		case *client.MessagePoll:
			if content != nil && content.Poll != nil && content.Poll.Question != nil {
				description = content.Poll.Question.Text
			}

		case *client.MessageGiveaway:
			if content != nil && content.Prize != nil {
				description = content.Prize.GiveawayPrizeType()
			}

		case *client.MessagePaidMedia:
			if content != nil && content.Caption != nil {
				description = content.Caption.Text
			}

		case *client.MessageSticker:
			if content != nil &&
				content.Sticker != nil &&
				content.Sticker.Sticker != nil &&
				content.Sticker.Sticker.Remote != nil {
				thumbnailPath = content.Sticker.Sticker.Remote.Id
				if thumbnailPath != "" {
					thumbnailPath, _ = fetchAndUploadMedia(tdlibClient, sm, crawlid, channelName, thumbnailPath, mlr.Link)
				}
			}

		case *client.MessageGiveawayWinners:
			log.Debug().Msgf("This message is a giveaway winner: %+v", content)

		case *client.MessageGiveawayCompleted:
			log.Debug().Msgf("This message is a giveaway completed: %+v", content)

		case *client.MessageVideoNote:
			if content != nil {
				if content.VideoNote != nil {
					if content.VideoNote.Thumbnail != nil &&
						content.VideoNote.Thumbnail.File != nil &&
						content.VideoNote.Thumbnail.File.Remote != nil {
						thumbnailPath = content.VideoNote.Thumbnail.File.Remote.Id
						if thumbnailPath != "" {
							thumbnailPath, _ = fetchAndUploadMedia(tdlibClient, sm, crawlid, channelName, thumbnailPath, mlr.Link)
						}
					}

					if content.VideoNote.Video != nil &&
						content.VideoNote.Video.Remote != nil {
						videoPath = content.VideoNote.Video.Remote.Id
						if videoPath != "" {
							videoPath, _ = fetchAndUploadMedia(tdlibClient, sm, crawlid, channelName, videoPath, mlr.Link)
						}
					}
				}
			}

		case *client.MessageDocument:
			if content != nil {
				if content.Document != nil {
					description = content.Document.FileName

					if content.Document.Thumbnail != nil &&
						content.Document.Thumbnail.File != nil &&
						content.Document.Thumbnail.File.Remote != nil {
						thumbnailPath = content.Document.Thumbnail.File.Remote.Id
						if thumbnailPath != "" {
							thumbnailPath, _ = fetchAndUploadMedia(tdlibClient, sm, crawlid, channelName, thumbnailPath, mlr.Link)
						}
					}

					if content.Document.Document != nil &&
						content.Document.Document.Remote != nil {
						videoPath = content.Document.Document.Remote.Id
						if videoPath != "" {
							videoPath, _ = fetchAndUploadMedia(tdlibClient, sm, crawlid, channelName, videoPath, mlr.Link)
						}
					}
				}
			}

		default:
			log.Debug().Str("type", fmt.Sprintf("%T", content)).Msg("Unknown message content type")
		}
	}

	// Safely extract outlinks and reactions
	outlinks := extractChannelLinksFromMessage(message)
	reactions := make(map[string]int)

	if message.InteractionInfo != nil &&
		message.InteractionInfo.Reactions != nil &&
		len(message.InteractionInfo.Reactions.Reactions) > 0 {
		for _, reaction := range message.InteractionInfo.Reactions.Reactions {
			if reaction.Type != nil {
				if emojiReaction, ok := reaction.Type.(*client.ReactionTypeEmoji); ok && emojiReaction != nil {
					reactions[emojiReaction.Emoji] = int(reaction.TotalCount)
				}
			}
		}
	}

	// Build the post
	posttype := []string{"unknown"}
	if message.Content != nil {
		posttype = []string{message.Content.MessageContentType()}
	}

	createdAt := time.Now()
	if message.EditDate > 0 {
		createdAt = time.Unix(int64(message.EditDate), 0)
	}

	vc := GetViewCount(message, channelName)
	postUid := fmt.Sprintf("%s-%s", messageNumber, channelName)
	var sharecount int = 0

	// Safely get share count
	if tdlibClient != nil {
		sharecount, _ = GetMessageShareCount(tdlibClient, chat.Id, message.Id, channelName)
	}

	username := GetPoster(tdlibClient, message)

	// Safely get supergroup info
	memberCount := 0
	if supergroupInfo != nil {
		memberCount = int(supergroupInfo.MemberCount)
	}

	post = model.Post{
		PostLink:       mlr.Link,
		ChannelID:      message.ChatId,
		PostUID:        postUid,
		URL:            mlr.Link,
		PublishedAt:    publishedAt,
		CreatedAt:      createdAt,
		LanguageCode:   "",
		Engagement:     vc,
		ViewCount:      vc,
		LikeCount:      0,
		ShareCount:     sharecount,
		CommentCount:   len(comments),
		ChannelName:    chat.Title,
		Description:    description,
		IsAd:           false,
		PostType:       posttype,
		TranscriptText: "",
		ImageText:      "",
		PlatformName:   "Telegram",
		LikesCount:     0,
		SharesCount:    sharecount,
		CommentsCount:  len(comments),
		ViewsCount:     vc,
		SearchableText: "",
		AllText:        "",
		ThumbURL:       thumbnailPath,
		MediaURL:       videoPath,
		Outlinks:       outlinks,
		CaptureTime:    time.Now(),
		ChannelData: model.ChannelData{
			ChannelID:           message.ChatId,
			ChannelName:         chat.Title,
			ChannelProfileImage: "",
			ChannelEngagementData: model.EngagementData{
				FollowerCount:  memberCount,
				FollowingCount: 0,
				LikeCount:      0,
				PostCount:      postcount,
				ViewsCount:     viewcount,
				CommentCount:   0,
				ShareCount:     0,
			},
			ChannelURLExternal: fmt.Sprintf("https://t.me/c/%s", channelName),
			ChannelURL:         "",
		},
		Comments:  comments,
		Reactions: reactions,
		Handle:    username,
	}

	// Store the post but don't return an error if storage fails
	if sm != nil {
		storeErr := sm.StorePost(channelName, post)
		if storeErr != nil {
			log.Error().Err(storeErr).Msg("Failed to store data")
		}
	}

	return post, nil
}

func checkFileCache(sm state.StateManagementInterface, uniqueid string) (string, bool, error) {
	exists, err := sm.HasProcessedMedia(uniqueid)
	return "", exists, err
}

// fetchfilefromtelegram retrieves and downloads a file from Telegram using the provided tdlib client and download ID.
//
// Parameters:
//   - tdlibClient: A pointer to the tdlib client used for interacting with Telegram.
//   - downloadid: A string representing the ID of the file to be downloaded.
//
// Returns:
//   - A string containing the local path of the downloaded file. Returns an empty string if an error occurs during fetching or downloading.
//
// The function includes error handling and logs relevant information, including any panics that are recovered.
func fetchfilefromtelegram(tdlibClient crawler.TDLibClient, sm state.StateManagementInterface, downloadid string) (string, string, error) {
	defer func() {
		if r := recover(); r != nil {
			log.Info().Msgf("Recovered from panic: %v\n", r)
		}
	}()

	// Fetch the remote file
	f, err := tdlibClient.GetRemoteFile(&client.GetRemoteFileRequest{
		RemoteFileId: downloadid,
	})

	if err != nil {
		log.Error().Err(err).Stack().Msgf("Error fetching remote file: %v\n", downloadid)
		return "", "", err
	}
	if existingPath, exists, err := checkFileCache(sm, f.Remote.UniqueId); err != nil {
		log.Error().Err(err).Stack().Msgf("Error checking file cache: %v\n", downloadid)
	} else if exists {
		log.Info().Msgf("File already downloaded at %s, skipping duplicate", existingPath)
		return "", "", nil
	}

	// Download the file
	downloadedFile, err := tdlibClient.DownloadFile(&client.DownloadFileRequest{
		FileId:      f.Id,
		Priority:    1,
		Offset:      0,
		Limit:       0,
		Synchronous: true,
	})
	if err != nil {
		log.Error().Stack().Err(err).Msgf("Error downloading file: %v\n", f.Id)
		return "", "", err
	}

	// Ensure the file path is valid
	if downloadedFile.Local.Path == "" {
		log.Debug().Msg("Downloaded file path is empty.")
		return "", "", err
	}

	log.Info().Msgf("Downloaded File Path: %s\n", downloadedFile.Local.Path)
	return downloadedFile.Local.Path, f.Remote.UniqueId, nil
}

func extractChannelLinksFromMessage(message *client.Message) []string {
	// Hold unique channel names
	channelNamesMap := make(map[string]bool)

	// Regex to identify Telegram channel links in text
	channelLinkRegex := regexp.MustCompile(`(https?://)?t\.me/([a-zA-Z0-9_]+)`)

	// Check if it's a text message
	var messageText *client.MessageText

	// Type assertion with proper type checking
	switch content := message.Content.(type) {
	case *client.MessageText:
		messageText = content
	default:
		return []string{} // Not a text message
	}

	// Process text entities if available
	if messageText != nil && messageText.Text != nil && messageText.Text.Entities != nil {
		for _, entity := range messageText.Text.Entities {
			switch entityType := entity.Type.(type) {
			case *client.TextEntityTypeTextUrl:
				// Extract URL from text link
				url := entityType.Url
				if matches := channelLinkRegex.FindStringSubmatch(url); len(matches) > 0 {
					// Extract just the channel name (group 2 from regex)
					channelName := matches[2]
					channelNamesMap[channelName] = true
				}

			case *client.TextEntityTypeMention:
				// Extract mention
				offset := entity.Offset
				length := entity.Length
				if int(offset+length) <= len(messageText.Text.Text) {
					mention := messageText.Text.Text[offset : offset+length]
					if strings.HasPrefix(mention, "@") {
						// Remove the @ prefix
						channelNamesMap[mention[1:]] = true
					}
				}

			case *client.TextEntityTypeUrl:
				// Extract URL directly from text
				offset := entity.Offset
				length := entity.Length
				if int(offset+length) <= len(messageText.Text.Text) {
					url := messageText.Text.Text[offset : offset+length]
					if matches := channelLinkRegex.FindStringSubmatch(url); len(matches) > 0 {
						// Extract just the channel name (group 2 from regex)
						channelName := matches[2]
						channelNamesMap[channelName] = true
					}
				}
			}
		}
	}

	// Also check the plain text for channel links using regex
	if messageText != nil && messageText.Text != nil {
		matches := channelLinkRegex.FindAllStringSubmatch(messageText.Text.Text, -1)
		for _, match := range matches {
			if len(match) >= 3 {
				// Extract just the channel name (group 2 from regex)
				channelName := match[2]
				channelNamesMap[channelName] = true
			}
		}
	}

	// Convert map to slice
	var channelNames []string
	for name := range channelNamesMap {
		channelNames = append(channelNames, name)
	}

	return channelNames
}

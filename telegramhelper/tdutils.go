package telegramhelper

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/zelenin/go-tdlib/client"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"tdlib-scraper/model"
	"tdlib-scraper/state"
	"time"
)

func GenCode() {
	authorizer := client.ClientAuthorizer()
	go client.CliInteractor(authorizer)

	apiId := os.Getenv("TG_API_ID")
	intValue, err := strconv.Atoi(apiId)
	if err != nil {
		log.Fatal().Err(err).Msg("Error converting string to int\n")

	}
	workingDir, err := os.Getwd()
	if err != nil {
		log.Error().Err(err).Stack().Msg("Failed to get working directory")
		return
	}
	tempDir, err := os.MkdirTemp(workingDir, "tempdir-*")
	log.Info().Msgf("Temporary directory: %s", tempDir)

	if err != nil {
		log.Error().Err(err).Stack().Msg("Failed to create temporary directory")
		return
	}
	int32Value := int32(intValue)
	apiHash := os.Getenv("TG_API_HASH")
	filedir := filepath.Join(".tdlib", "files")
	if err != nil {
		log.Error().Err(err).Msg("SetLogVerbosityLevel error")
	}
	authorizer.TdlibParameters <- &client.SetTdlibParametersRequest{
		UseTestDc:           false,
		DatabaseDirectory:   filepath.Join(tempDir, ".tdlib", "database"),
		FilesDirectory:      filedir,
		UseFileDatabase:     true,
		UseChatInfoDatabase: true,
		UseMessageDatabase:  true,
		UseSecretChats:      false,
		ApiId:               int32Value,
		ApiHash:             apiHash,
		SystemLanguageCode:  "en",
		DeviceModel:         "Server",
		SystemVersion:       "1.0.0",
		ApplicationVersion:  "1.0.0",
	}

	//authorizer.PhoneNumber <- os.Getenv("TG_PHONE_NUMBER")

	_, err = client.SetLogVerbosityLevel(&client.SetLogVerbosityLevelRequest{
		NewVerbosityLevel: 5,
	})

	if err != nil {
		log.Error().Err(err).Msg("SetLogVerbosityLevel error")
	}
	clientReady := make(chan *client.Client)
	errChan := make(chan error)
	go func() {
		log.Info().Msg("Starting client initialization...")
		tdlibClient, err := client.NewClient(authorizer)
		if err != nil {
			errChan <- fmt.Errorf("failed to initialize TDLib client: %w", err)
			return
		}
		clientReady <- tdlibClient
	}()

	select {
	case tdlibClient := <-clientReady:
		log.Info().Msg("Client initialized successfully")
		defer tdlibClient.Close()
	case err = <-errChan:
		log.Fatal().Err(err).Msg("Error initializing client")
	case <-time.After(30 * time.Second):
		log.Warn().Msg("Timeout reached. Exiting application.")
	}
}

// TdConnect initializes and connects a new TDLib client instance.
// It sets the necessary TDLib parameters, including API ID and hash,
// database and file directories, and logging verbosity level. The function
// returns the connected client instance or an error if the connection fails.
func TdConnect(storageprefix string) (*client.Client, error) {
	authorizer := client.ClientAuthorizer()

	go client.CliInteractor(authorizer)
	fn := "https://tomb218.sg-host.com/tdlib.tgz"
	targetDir := storageprefix + "/state"

	err := downloadAndExtractTarball(fn, targetDir)
	if err != nil {
		log.Error().Err(err).Stack().Msg("Error extracting tarball")
	} else {
		log.Info().Msg("Extraction completed successfully!")
	}
	apiId := os.Getenv("TG_API_ID")
	intValue, err := strconv.Atoi(apiId)
	if err != nil {
		log.Fatal().Err(err).Msg("Error converting string to int\n")

	}

	// Cast int to int32
	int32Value := int32(intValue)
	apiHash := os.Getenv("TG_API_HASH")
	filedir := filepath.Join(storageprefix+"/state", ".tdlib", "files")
	err = removeMultimedia(filedir)
	if err != nil {
		log.Error().Err(err).Msg("SetLogVerbosityLevel error")
	}
	authorizer.TdlibParameters <- &client.SetTdlibParametersRequest{
		UseTestDc:           false,
		DatabaseDirectory:   filepath.Join(storageprefix+"/state", ".tdlib", "database"),
		FilesDirectory:      filedir,
		UseFileDatabase:     true,
		UseChatInfoDatabase: true,
		UseMessageDatabase:  true,
		UseSecretChats:      false,
		ApiId:               int32Value,
		ApiHash:             apiHash,
		SystemLanguageCode:  "en",
		DeviceModel:         "Server",
		SystemVersion:       "1.0.0",
		ApplicationVersion:  "1.0.0",
	}

	//authorizer.PhoneNumber <- os.Getenv("TG_PHONE_NUMBER")
	//if os.Getenv("TG_PHONE_CODE") == "" {
	//	log.Fatal().Msg("TG_PHONE_CODE environment variable is not set")
	//}
	//authorizer.Code <- os.Getenv("TG_PHONE_CODE")

	_, err = client.SetLogVerbosityLevel(&client.SetLogVerbosityLevelRequest{
		NewVerbosityLevel: 1,
	})
	if err != nil {
		log.Error().Err(err).Msg("SetLogVerbosityLevel error")
	}

	log.Warn().Msg("ABOUT TO CONNECT TO TELEGRAM IF YOUR TG_PHONE_CODE IS NOT VALID, YOU WILL NEED TO RE-RUN THE PROGRAM WITH A VALID TG_PHONE_CODE. THIS WILL HANG WHILST IT TRIES TO CONNECT")
	tdlibClient, err := client.NewClient(authorizer)
	if err != nil {
		log.Fatal().Err(err).Msg("NewClient error")
	}

	optionValue, err := client.GetOption(&client.GetOptionRequest{
		Name: "version",
	})
	if err != nil {
		log.Fatal().Err(err).Msg("GetOption error")
	}

	log.Printf("TDLib version: %s", optionValue.(*client.OptionValueString).Value)

	me, err := tdlibClient.GetMe()
	if err != nil {
		log.Fatal().Err(err).Msg("GetMe error: %s")
	}

	log.Info().Msgf("Logged in as: %s %s", me.FirstName, me.LastName)

	return tdlibClient, err
}
func downloadAndExtractTarball(url, targetDir string) error {
	// Step 1: Download the tarball
	req, err := http.NewRequest("GET", url, nil)

	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.5")
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
		return err
	}

	// Step 2: Decompress the gzip file
	gzReader, err := gzip.NewReader(resp.Body)
	if err != nil {
		return err
	}
	defer gzReader.Close()

	// Step 3: Read the tarball contents
	tarReader := tar.NewReader(gzReader)

	// Step 4: Extract files
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
			// Create directory
			err := os.MkdirAll(targetPath, os.ModePerm)
			if err != nil {
				return err
			}
		case tar.TypeReg:
			// Create file
			err := os.MkdirAll(filepath.Dir(targetPath), os.ModePerm)
			if err != nil {
				return err
			}
			file, err := os.Create(targetPath)
			if err != nil {
				return err
			}
			defer file.Close()

			// Copy file contents
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
func processMessageSafely(mymsg *client.MessageVideo, tdlibClient *client.Client) (thumbnailPath, videoPath, description string, err error) {
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
func ParseMessage(crawlid string, message *client.Message, mlr *client.MessageLink, chat *client.Chat, supergroup *client.Supergroup, supergroupInfo *client.SupergroupFullInfo, postcount int, viewcount int, channelName string, tdlibClient *client.Client, sm state.StateManager) (post model.Post, err error) {
	// Defer to recover from panics and ensure the crawl continues
	defer func() {
		if r := recover(); r != nil {
			// Log the panic and set a default error
			log.Info().Msgf("Recovered from panic while parsing message for channel %s: %v\n", channelName, r)
			err = fmt.Errorf("failed to parse message")
		}
	}()

	publishedAt := time.Unix(int64(message.Date), 0)
	if publishedAt.Year() < 2018 {
		return model.Post{}, nil // Skip messages not from earlier than 2018
	}

	var messageNumber string
	linkParts := strings.Split(mlr.Link, "/")
	if len(linkParts) > 0 {
		messageNumber = linkParts[len(linkParts)-1]
	} else {
		return model.Post{}, nil // Skip if message number cannot be determined
	}

	comments := make([]model.Comment, 0)
	if message.InteractionInfo != nil && message.InteractionInfo.ReplyInfo != nil {
		defer func() {
			if r := recover(); r != nil {
				log.Info().Msgf("Recovered from panic while fetching comments for channel %s: %v\n", channelName, r)
			}
		}()
		if message.InteractionInfo.ReplyInfo.ReplyCount > 0 {
			comments, err = GetMessageComments(tdlibClient, chat.Id, message.Id, channelName)
			if err != nil {
				log.Error().Stack().Err(err).Msg("Fetch message error")
			}
		}
	}

	description := ""
	thumbnailPath := ""
	videoPath := ""
	switch content := message.Content.(type) {
	case *client.MessageText:
		description = content.Text.Text
	case *client.MessageVideo:
		thumbnailPath, videoPath, description, _ = processMessageSafely(content, tdlibClient)
		path := fetchfilefromtelegram(tdlibClient, thumbnailPath)
		err = sm.UploadBlobFileAndDelete(crawlid, channelName, mlr.Link, path)
		path = fetchfilefromtelegram(tdlibClient, videoPath)
		err = sm.UploadBlobFileAndDelete(crawlid, channelName, mlr.Link, path)
	case *client.MessagePhoto:
		description = content.Caption.Text
		thumbnailPath = content.Photo.Sizes[0].Photo.Remote.Id
		path := fetchfilefromtelegram(tdlibClient, thumbnailPath)
		err = sm.UploadBlobFileAndDelete(crawlid, channelName, mlr.Link, path)
		if err != nil {
			log.Error().Err(err).Msg("UploadBlobFileAndDelete error")
		}
		//thumbnailPath = fetch(tdlibClient, content.Photo.Sizes[0].Photo.Remote.Id)
	case *client.MessageAnimation:
		description = content.Caption.Text
		thumbnailPath = content.Animation.Thumbnail.File.Remote.Id
		path := fetchfilefromtelegram(tdlibClient, thumbnailPath)
		err = sm.UploadBlobFileAndDelete(crawlid, channelName, mlr.Link, path)
		if err != nil {
			log.Error().Err(err).Msg("UploadBlobFileAndDelete error")
		}
	case *client.MessageAnimatedEmoji:
		description = content.Emoji
	case *client.MessagePoll:
		description = content.Poll.Question.Text
	case *client.MessageGiveaway:
		description = content.Prize.GiveawayPrizeType()
	case *client.MessagePaidMedia:
		description = content.Caption.Text
	case *client.MessageSticker:
		thumbnailPath = content.Sticker.Sticker.Remote.Id
		//thumbnailPath = Fetch(tdlibClient, content.Sticker.Sticker.Remote.Id)
		path := fetchfilefromtelegram(tdlibClient, thumbnailPath)
		err = sm.UploadBlobFileAndDelete(crawlid, channelName, mlr.Link, path)
		if err != nil {
			log.Error().Err(err).Msg("UploadBlobFileAndDelete error")
		}
	case *client.MessageGiveawayWinners:
		log.Debug().Msgf("This message is a giveaway winner:", content)
	case *client.MessageGiveawayCompleted:
		log.Debug().Msgf("This message is a giveaway completed:", content)
	case *client.MessageVideoNote:
		thumbnailPath = content.VideoNote.Thumbnail.File.Remote.Id
		path := fetchfilefromtelegram(tdlibClient, thumbnailPath)
		err = sm.UploadBlobFileAndDelete(crawlid, channelName, mlr.Link, path)
		if err != nil {
			log.Error().Err(err).Msg("UploadBlobFileAndDelete error")
		}
		videoPath = content.VideoNote.Video.Remote.Id
		path = fetchfilefromtelegram(tdlibClient, thumbnailPath)
		err = sm.UploadBlobFileAndDelete(crawlid, channelName, mlr.Link, path)
		if err != nil {
			log.Error().Err(err).Msg("UploadBlobFileAndDelete error")
		}
		//thumbnailPath = fetch(tdlibClient, thumbnailPath)
		//videoPath = fetch(tdlibClient, videoPath)
	case *client.MessageDocument:
		description = content.Document.FileName
		thumbnailPath = content.Document.Thumbnail.File.Remote.Id
		path := fetchfilefromtelegram(tdlibClient, thumbnailPath)
		err = sm.UploadBlobFileAndDelete(crawlid, channelName, mlr.Link, path)
		if err != nil {
			log.Error().Err(err).Msg("UploadBlobFileAndDelete error for video")
		}
		videoPath = content.Document.Document.Remote.Id
		path = fetchfilefromtelegram(tdlibClient, thumbnailPath)
		err = sm.UploadBlobFileAndDelete(crawlid, channelName, mlr.Link, path)
		if err != nil {
			log.Error().Err(err).Msg("UploadBlobFileAndDelete error for video")
		}
		//thumbnailPath = fetch(tdlibClient, thumbnailPath)
		//videoPath = fetch(tdlibClient, videoPath)
	default:
		log.Debug().Msg("Unknown message content type")
	}

	reactions := make(map[string]int)
	if message.InteractionInfo != nil && message.InteractionInfo.Reactions != nil && len(message.InteractionInfo.Reactions.Reactions) > 0 {
		defer func() {
			if r := recover(); r != nil {
				log.Info().Msgf("Recovered from panic while processing reactions: %v\n", r)
			}
		}()
		for _, reaction := range message.InteractionInfo.Reactions.Reactions {
			if reaction.Type != nil {
				if emojiReaction, ok := reaction.Type.(*client.ReactionTypeEmoji); ok {
					reactions[emojiReaction.Emoji] = int(reaction.TotalCount)
				}
			}
		}
	}

	posttype := []string{message.Content.MessageContentType()}
	createdAt := time.Unix(int64(message.EditDate), 0)
	vc := GetViewCount(message, channelName)
	postUid := fmt.Sprintf("%s-%s", messageNumber, channelName)
	sharecount, _ := GetMessageShareCount(tdlibClient, chat.Id, message.Id, channelName)

	post = model.Post{
		PostLink:       mlr.Link,
		ChannelID:      message.ChatId,
		PostUID:        postUid,
		URL:            mlr.Link,
		PublishedAt:    publishedAt,
		CreatedAt:      createdAt,
		LanguageCode:   "RU",
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
		ChannelData: model.ChannelData{
			ChannelID:           message.ChatId,
			ChannelName:         chat.Title,
			ChannelProfileImage: "",
			ChannelEngagementData: model.EngagementData{
				FollowerCount:  int(supergroupInfo.MemberCount),
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
	}
	err = sm.StoreData(crawlid, channelName, post)
	if err != nil {
		log.Error().Err(err).Msg("StoreData error")
	}
	return post, nil
}
func fetchfilefromtelegram(tdlibClient *client.Client, downloadid string) string {
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
		return ""
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
		return ""
	}

	// Ensure the file path is valid
	if downloadedFile.Local.Path == "" {
		log.Debug().Msg("Downloaded file path is empty.")
		return ""
	}

	log.Info().Msgf("Downloaded File Path: %s\n", downloadedFile.Local.Path)
	return downloadedFile.Local.Path
}

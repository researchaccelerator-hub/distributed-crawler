package telegramhelper

import (
	"fmt"
	"os"
	"regexp"
	"runtime/debug"
	"strings"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/researchaccelerator-hub/telegram-scraper/model"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/rs/zerolog/log"
	"github.com/zelenin/go-tdlib/client"
)

//// removeMultimedia removes all files and subdirectories in the specified directory.
//// If the directory does not exist, it does nothing.
////
//// Parameters:
////   - filedir: The path to the directory whose contents are to be removed.
////
//// Returns:
////   - An error if there is a failure during removal; otherwise, nil.
//func removeMultimedia(filedir string) error {
//	log.Debug().Str("directory", filedir).Msg("Attempting to remove multimedia directory contents")
//
//	// Check if the directory exists
//	info, err := os.Stat(filedir)
//	if os.IsNotExist(err) {
//		// Directory does not exist, nothing to do
//		log.Debug().Str("directory", filedir).Msg("Directory does not exist, nothing to remove")
//		return nil
//	}
//	if err != nil {
//		log.Error().Err(err).Str("directory", filedir).Msg("Failed to check directory status")
//		return err
//	}
//
//	// Ensure it is a directory
//	if !info.IsDir() {
//		log.Error().Str("path", filedir).Msg("Path is not a directory")
//		return fmt.Errorf("path %s is not a directory", filedir)
//	}
//
//	// Get file count before removal for logging
//	var fileCount int
//	filepath.Walk(filedir, func(path string, info os.FileInfo, err error) error {
//		if err == nil && path != filedir {
//			fileCount++
//		}
//		return nil
//	})
//
//	log.Debug().
//		Str("directory", filedir).
//		Int("file_count", fileCount).
//		Msg("Removing files and subdirectories")
//
//	// Remove contents of the directory
//	err = filepath.Walk(filedir, func(path string, info os.FileInfo, err error) error {
//		if err != nil {
//			log.Warn().Err(err).Str("path", path).Msg("Error accessing path during cleanup")
//			return err
//		}
//
//		// Skip the root directory itself
//		if path == filedir {
//			return nil
//		}
//
//		// Remove files and subdirectories
//		if err := os.RemoveAll(path); err != nil {
//			log.Error().Err(err).Str("path", path).Msg("Failed to remove path")
//			return err
//		}
//
//		log.Debug().Str("path", path).Msg("Removed path successfully")
//		return nil
//	})
//
//	if err != nil {
//		log.Error().Err(err).Str("directory", filedir).Msg("Error removing directory contents")
//		return err
//	}
//
//	log.Debug().
//		Str("directory", filedir).
//		Int("files_removed", fileCount).
//		Msg("Directory contents removed successfully")
//	return nil
//}

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
func processMessageSafely(mymsg *client.MessageVideo) (thumbnailPath, videoPath, description string, videofileid int32, thumbnailfileid int32, err error) {
	if mymsg == nil || mymsg.Video == nil || mymsg.Video.Thumbnail == nil {
		return "", "", "", 0, 0, fmt.Errorf("invalid or corrupt message structure")
	}

	thumbnailPath = mymsg.Video.Thumbnail.File.Remote.Id
	thumbnailfileid = mymsg.Video.Thumbnail.File.Id
	videoPath = mymsg.Video.Video.Remote.Id
	videofileid = mymsg.Video.Video.Id
	description = mymsg.Caption.Text
	return thumbnailPath, videoPath, description, videofileid, thumbnailfileid, nil
}

// fetchAndUploadMedia fetches a media file from Telegram using the provided TDLibClient
// and uploads it to storage via the StateManager. It requires the crawl ID,
// channel name, file ID, and post link as inputs.
//
// Parameters:
//   - tdlibClient: An initialized TDLib client connection
//   - sm: State manager interface for storing the file
//   - crawlid: Unique identifier for the current crawl
//   - channelName: Name of the channel from which the file originates
//   - fileID: Telegram's identifier for the file to download
//   - postLink: Link to the post containing the media
//   - cfg: CrawlerConfig containing runtime configuration options
//
// Returns:
//   - The unique remote ID of the file for future reference if successful
//   - An error if any step in the process fails
//
// The function follows these steps:
// 1. Check if fileID is empty (nothing to download)
// 2. Check if media downloads should be skipped based on configuration
// 3. Download the file from Telegram (if not skipped)
// 4. Verify file existence and size limits
// 5. Store the file via the state manager
// 6. Clean up the local file
// 7. Mark the media as processed to prevent redundant downloads
func fetchAndUploadMedia(tdlibClient crawler.TDLibClient, sm state.StateManagementInterface, crawlid, channelName, fileID, postLink string, cfid int32, cfg common.CrawlerConfig) (string, error) {
	if fileID == "" {
		log.Debug().Msg("Empty file ID provided, nothing to fetch")
		return "", nil
	}

	// Check if media downloads should be skipped
	if cfg.SkipMediaDownload {
		log.Debug().
			Str("file_id", fileID).
			Str("channel", channelName).
			Msg("Skipping media download as per configuration")
		return "", nil
	}

	log.Debug().
		Str("file_id", fileID).
		Str("channel", channelName).
		Str("crawl_id", crawlid).
		Str("post_link", postLink).
		Msg("Fetching and uploading media file")

	path, remoteid, err := fetchfilefromtelegram(tdlibClient, sm, fileID)
	if err != nil {
		log.Error().
			Err(err).
			Str("file_id", fileID).
			Str("channel", channelName).
			Msg("Failed to fetch file from Telegram")
		return "", err
	}

	if path == "" {
		log.Debug().Str("file_id", fileID).Msg("Empty path returned from fetch operation, file likely already processed")
		return "", nil // Not a real error if we already processed it
	}

	fileInfo, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			log.Error().
				Str("path", path).
				Str("file_id", fileID).
				Msg("Downloaded file does not exist")
		} else {
			log.Error().
				Err(err).
				Str("path", path).
				Str("file_id", fileID).
				Msg("Error checking downloaded file")
		}
		return "", err
	}

	// Get file size in bytes
	sizeInBytes := fileInfo.Size()

	// Convert to MB (1 MB = 1,048,576 bytes)
	sizeInMB := float64(sizeInBytes) / 1048576.0

	log.Debug().
		Str("path", path).
		Str("remote_id", remoteid).
		Float64("size_mb", sizeInMB).
		Msg("File downloaded and ready for storage")

	// Check if file is over size limit
	const fileSizeLimit = 150.0 // MB
	isOverLimit := sizeInMB > fileSizeLimit
	if isOverLimit {
		log.Warn().
			Float64("size_mb", sizeInMB).
			Float64("limit_mb", fileSizeLimit).
			Str("file_id", fileID).
			Msg("File exceeds size limit, skipping storage")

		// Clean up the file even though we're not storing it
		if e := os.Remove(path); e != nil {
			log.Warn().Err(e).Str("path", path).Msg("Failed to remove oversized file")
		}

		deleteFileReq := client.DeleteFileRequest{FileId: cfid}
		_, err := tdlibClient.DeleteFile(&deleteFileReq)
		if err != nil {
			return "", err
		}
		return "", fmt.Errorf("file size is too large (%.2f MB)", sizeInMB)
	}

	// Store the file
	storageLocation, filep, err := sm.StoreFile(channelName, path, remoteid)
	if err != nil {
		log.Error().
			Err(err).
			Str("path", path).
			Str("channel", channelName).
			Str("remote_id", remoteid).
			Msg("Failed to store file")
	} else {
		log.Debug().
			Str("storage_location", storageLocation).
			Str("channel", channelName).
			Float64("size_mb", sizeInMB).
			Msg("File stored successfully")
	}

	// Delete original file after successful upload
	err = os.Remove(filep)
	if err != nil {
		log.Warn().Err(err).Str("path", storageLocation).Msg("Failed to delete source file after upload")
	}
	deleteFileReq := client.DeleteFileRequest{FileId: cfid}
	ok, err := tdlibClient.DeleteFile(&deleteFileReq)
	if err != nil {
		log.Error().Err(err).Msg("Failed to delete file from Telegram")
	}
	log.Debug().Msgf("Response from TD for file deletion: %v", ok)
	// Mark as processed to avoid future downloads
	if err := sm.MarkMediaAsProcessed(remoteid); err != nil {
		log.Error().
			Err(err).
			Str("remote_id", remoteid).
			Msg("Failed to mark media as processed")
		return "", err
	}

	log.Debug().
		Str("remote_id", remoteid).
		Str("channel", channelName).
		Msg("Media processing complete")

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
	//videofileid := int32(0)
	thumbnailfileid := int32(0)
	// Safely fetch comments if available
	if message.InteractionInfo != nil &&
		message.InteractionInfo.ReplyInfo != nil &&
		message.InteractionInfo.ReplyInfo.ReplyCount > 0 {
		fetchedComments, fetchErr := GetMessageComments(tdlibClient, chat.Id, message.Id, channelName, cfg.MaxComments, int(message.InteractionInfo.ReplyInfo.ReplyCount))
		if fetchErr != nil {
			log.Error().Stack().Err(fetchErr).Msg("Failed to fetch comments")
		}
		comments = fetchedComments

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
				thumbnailPath, videoPath, description, _, thumbnailfileid, err = processMessageSafely(content)

				if thumbnailPath != "" {
					thumbnailPath, _ = fetchAndUploadMedia(tdlibClient, sm, crawlid, channelName, thumbnailPath, mlr.Link, thumbnailfileid, cfg)
				}

				//if videoPath != "" {
				//	videoPath, _ = fetchAndUploadMedia(tdlibClient, sm, crawlid, channelName, videoPath, mlr.Link, videofileid, cfg)
				//}

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
						thumbnailPath, _ = fetchAndUploadMedia(tdlibClient, sm, crawlid, channelName, thumbnailPath, mlr.Link, thumbnailfileid, cfg)
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
						thumbnailPath, _ = fetchAndUploadMedia(tdlibClient, sm, crawlid, channelName, thumbnailPath, mlr.Link, thumbnailfileid, cfg)
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
					thumbnailPath, _ = fetchAndUploadMedia(tdlibClient, sm, crawlid, channelName, thumbnailPath, mlr.Link, thumbnailfileid, cfg)
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
							thumbnailPath, _ = fetchAndUploadMedia(tdlibClient, sm, crawlid, channelName, thumbnailPath, mlr.Link, thumbnailfileid, cfg)
						}
					}

					if content.VideoNote.Video != nil &&
						content.VideoNote.Video.Remote != nil {
						videoPath = content.VideoNote.Video.Remote.Id
						//if videoPath != "" {
						//	videoPath, _ = fetchAndUploadMedia(tdlibClient, sm, crawlid, channelName, videoPath, mlr.Link, thumbnailfileid, cfg)
						//}
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
							thumbnailPath, _ = fetchAndUploadMedia(tdlibClient, sm, crawlid, channelName, thumbnailPath, mlr.Link, thumbnailfileid, cfg)
						}
					}

					if content.Document.Document != nil &&
						content.Document.Document.Remote != nil {
						videoPath = content.Document.Document.Remote.Id
						//if videoPath != "" {
						//	videoPath, _ = fetchAndUploadMedia(tdlibClient, sm, crawlid, channelName, videoPath, mlr.Link, videofileid, cfg)
						//}
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
		ChannelID:      fmt.Sprintf("%d", message.ChatId), // Convert int64 to string
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
			ChannelID:           fmt.Sprintf("%d", message.ChatId), // Convert int64 to string
			ChannelName:         chat.Title,
			ChannelDescription:  "", // Empty description for Telegram channels
			ChannelProfileImage: "",
			CountryCode:         "", // No country data available for Telegram channels
			// PublishedAt field is intentionally omitted for Telegram channels
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

// checkFileCache checks if a media file with the given unique ID has already
// been processed and stored, avoiding redundant downloads and processing.
//
// Parameters:
//   - sm: The state manager interface for querying media processing status
//   - uniqueid: The unique identifier of the media file to check
//
// Returns:
//   - A string containing the path where the file is stored (currently always empty)
//   - A boolean indicating whether the file has already been processed
//   - An error if the cache query operation fails
func checkFileCache(sm state.StateManagementInterface, uniqueid string) (string, bool, error) {
	log.Debug().Str("unique_id", uniqueid).Msg("Checking if media file already processed")
	exists, err := sm.HasProcessedMedia(uniqueid)

	if err != nil {
		log.Error().
			Err(err).
			Str("unique_id", uniqueid).
			Msg("Error checking if media file is in cache")
	} else if exists {
		log.Debug().
			Str("unique_id", uniqueid).
			Msg("Media file found in cache")
	} else {
		log.Debug().
			Str("unique_id", uniqueid).
			Msg("Media file not found in cache, needs processing")
	}

	return "", exists, err
}

// fetchfilefromtelegram retrieves and downloads a file from Telegram using the provided tdlib client and download ID.
//
// Parameters:
//   - tdlibClient: A pointer to the tdlib client used for interacting with Telegram.
//   - sm: State manager interface for checking if the file has already been processed
//   - downloadid: A string representing the ID of the file to be downloaded.
//
// Returns:
//   - A string containing the local path of the downloaded file. Returns an empty string if an error occurs during fetching or downloading.
//   - A string containing the unique ID of the remote file
//   - An error if any of the steps fail
//
// The function includes error handling and logs relevant information, including any panics that are recovered.
func fetchfilefromtelegram(tdlibClient crawler.TDLibClient, sm state.StateManagementInterface, downloadid string) (string, string, error) {
	log.Debug().Str("download_id", downloadid).Msg("Fetching file from Telegram")

	defer func() {
		if r := recover(); r != nil {
			stack := string(debug.Stack())
			log.Error().
				Interface("panic", r).
				Str("stack", stack).
				Str("download_id", downloadid).
				Msg("Recovered from panic in file download")
		}
	}()

	// Fetch the remote file
	f, err := tdlibClient.GetRemoteFile(&client.GetRemoteFileRequest{
		RemoteFileId: downloadid,
	})

	if err != nil {
		log.Error().
			Err(err).
			Stack().
			Str("download_id", downloadid).
			Msg("Failed to get remote file information")
		return "", "", err
	}

	log.Debug().
		Str("download_id", downloadid).
		Str("file_id", fmt.Sprintf("%d", f.Id)).
		Str("unique_id", f.Remote.UniqueId).
		Int32("size", int32(f.Size)).
		Msg("Retrieved remote file information")

	// Check if we've already processed this file
	if existingPath, exists, err := checkFileCache(sm, f.Remote.UniqueId); err != nil {
		log.Error().
			Err(err).
			Stack().
			Str("download_id", downloadid).
			Str("unique_id", f.Remote.UniqueId).
			Msg("Error checking file cache")
	} else if exists {
		log.Debug().
			Str("path", existingPath).
			Str("unique_id", f.Remote.UniqueId).
			Msg("File already processed, skipping download")
		return "", "", nil
	}

	// Download the file
	log.Debug().
		Str("download_id", downloadid).
		Str("file_id", fmt.Sprintf("%d", f.Id)).
		Msg("Downloading file from Telegram")

	downloadedFile, err := tdlibClient.DownloadFile(&client.DownloadFileRequest{
		FileId:      f.Id,
		Priority:    1,
		Offset:      0,
		Limit:       0,
		Synchronous: true,
	})
	if err != nil {
		log.Error().
			Err(err).
			Stack().
			Str("download_id", downloadid).
			Str("file_id", fmt.Sprintf("%d", f.Id)).
			Msg("Error downloading file")
		return "", "", err
	}

	// Ensure the file path is valid
	if downloadedFile.Local.Path == "" {
		log.Warn().
			Str("download_id", downloadid).
			Str("file_id", fmt.Sprintf("%d", f.Id)).
			Msg("Downloaded file path is empty")
		return "", "", fmt.Errorf("empty file path received from TDLib")
	}

	log.Debug().
		Str("path", downloadedFile.Local.Path).
		Str("unique_id", f.Remote.UniqueId).
		Int32("downloaded_size", int32(downloadedFile.Size)).
		Bool("downloaded_from_memory", downloadedFile.Local.IsDownloadingCompleted).
		Msg("File downloaded successfully")

	return downloadedFile.Local.Path, f.Remote.UniqueId, nil
}

// extractChannelLinksFromMessage extracts all unique Telegram channel links and mentions
// from a message. This is a critical function for the crawler's discovery mechanism,
// allowing it to find new channels to crawl based on links in messages.
//
// Parameters:
//   - message: The Telegram message to analyze for channel links
//
// Returns:
//   - A slice of unique channel names (without the @ prefix or t.me/ domain)
//     that were found in the message
//
// The function searches for channels in three ways:
//  1. TextEntityTypeTextUrl entities - formatted links with custom text
//  2. TextEntityTypeMention entities - @username mentions
//  3. TextEntityTypeUrl entities - plain URLs in text
//  4. Plain text regex matching for t.me links that might not be formatted as entities
//
// Each channel name is deduplicated using a map before being returned,
// ensuring that the same channel isn't added multiple times even if referenced
// multiple ways in the same message.
func extractChannelLinksFromMessage(message *client.Message) []string {
	// Hold unique channel names
	channelNamesMap := make(map[string]bool)

	// Regex to identify Telegram channel links in text
	channelLinkRegex := regexp.MustCompile(`(https?://)?t\.me/([a-zA-Z0-9_]{4,32})`)

	// Regex to identify names that fit username requirements
	usernameRegex := regexp.MustCompile(`(@)?([a-zA-Z0-9_]{4,32})`)

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
					log.Info().Str("url", url).Str("channel_name", channelName).Msg("random-walk: adding TextEntityTypeTextUrl")
					channelNamesMap[channelName] = true
				}

			case *client.TextEntityTypeMention:
				// Extract mention
				offset := entity.Offset
				length := entity.Length
				if int(offset+length) <= len(messageText.Text.Text) {
					mention := messageText.Text.Text[offset : offset+length]
					if matches := usernameRegex.FindStringSubmatch(mention); len(matches) > 0 {
						if strings.HasPrefix(mention, "@") {
							// Remove the @ prefix
							log.Info().Str("mention", mention).Msg("random-walk: adding TextEntityTypeMention")
							channelNamesMap[mention[1:]] = true
						}
					} else {
						log.Info().Str("mention", mention).Msg("random-walk: skipping TextEntityTypeMention for not matching regex")
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
						log.Info().Str("url", url).Str("channel_name", channelName).Msg("random-walk: adding TextEntityTypeUrl")
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
				log.Info().Str("channel_name", channelName).Msg("random-walk: adding messageText.Text.Text")
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

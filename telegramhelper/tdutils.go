package telegramhelper

import (
	"fmt"
	"github.com/zelenin/go-tdlib/client"
	"log"
	"path/filepath"
	"strings"
	"tdlib-scraper/model"
	"time"
)

// TdConnect initializes and connects a new TDLib client instance.
// It sets the necessary TDLib parameters, including API ID and hash,
// database and file directories, and logging verbosity level. The function
// returns the connected client instance or an error if the connection fails.
func TdConnect() (*client.Client, error) {
	authorizer := client.ClientAuthorizer()
	go client.CliInteractor(authorizer)

	const (
		apiId   = 26120733
		apiHash = "a2c2eabafa983f16d0a9304bc91d272c"
	)

	authorizer.TdlibParameters <- &client.SetTdlibParametersRequest{
		UseTestDc:           false,
		DatabaseDirectory:   filepath.Join(".tdlib", "database"),
		FilesDirectory:      filepath.Join(".tdlib", "files"),
		UseFileDatabase:     true,
		UseChatInfoDatabase: true,
		UseMessageDatabase:  true,
		UseSecretChats:      false,
		ApiId:               apiId,
		ApiHash:             apiHash,
		SystemLanguageCode:  "en",
		DeviceModel:         "Server",
		SystemVersion:       "1.0.0",
		ApplicationVersion:  "1.0.0",
	}

	_, err := client.SetLogVerbosityLevel(&client.SetLogVerbosityLevelRequest{
		NewVerbosityLevel: 1,
	})
	if err != nil {
		log.Fatalf("SetLogVerbosityLevel error: %s", err)
	}

	tdlibClient, err := client.NewClient(authorizer)
	if err != nil {
		log.Fatalf("NewClient error: %s", err)
	}

	optionValue, err := client.GetOption(&client.GetOptionRequest{
		Name: "version",
	})
	if err != nil {
		log.Fatalf("GetOption error: %s", err)
	}

	log.Printf("TDLib version: %s", optionValue.(*client.OptionValueString).Value)

	me, err := tdlibClient.GetMe()
	if err != nil {
		log.Fatalf("GetMe error: %s", err)
	}

	log.Printf("Me: %s %s", me.FirstName, me.LastName)

	return tdlibClient, err
}

// Fetch retrieves and downloads a remote file using the provided tdlib client.
// It takes a tdlib client and a download ID as parameters, and returns the local
// file path as a string. If an error occurs during fetching or downloading, or if
// the local path is empty, it returns an empty string. The function recovers from
// any panics, logging the error and ensuring an empty string is returned.
func Fetch(tdlibClient *client.Client, downloadid string) string {
	defer func() {
		if r := recover(); r != nil {
			// Log the panic and ensure an empty string is returned
			fmt.Printf("Recovered from panic: %v\n", r)
		}
	}()

	// Attempt to fetch the remote file
	f, err := tdlibClient.GetRemoteFile(&client.GetRemoteFileRequest{
		RemoteFileId: downloadid,
	})
	if err != nil {
		fmt.Printf("Error fetching remote file: %v\n", err)
		return ""
	}

	// Attempt to download the file
	downloadedFile, err := tdlibClient.DownloadFile(&client.DownloadFileRequest{
		FileId:      f.Id, // Use the File.Id from the GetRemoteFile response
		Priority:    1,    // Download priority (1 = high)
		Offset:      0,    // Start downloading from the beginning
		Limit:       0,    // Download the entire file
		Synchronous: true,
	})
	if err != nil {
		fmt.Printf("Error downloading file: %v\n", err)
		return ""
	}

	// Check if the local path exists
	if downloadedFile.Local.Path == "" {
		fmt.Println("Downloaded file path is empty")
		return ""
	}

	fmt.Printf("Downloaded File Path: %s\n", downloadedFile.Local.Path)
	return downloadedFile.Local.Path
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
func ParseMessage(message *client.Message, mlr *client.MessageLink, chat *client.Chat, supergroup *client.Supergroup, supergroupInfo *client.SupergroupFullInfo, postcount int, viewcount int, channelName string, tdlibClient *client.Client) (post model.Post, err error) {
	// Defer to recover from panics and ensure the crawl continues
	defer func() {
		if r := recover(); r != nil {
			// Log the panic and set a default error
			fmt.Printf("Recovered from panic while parsing message: %v\n", r)
			err = fmt.Errorf("failed to parse message")
		}
	}()

	publishedAt := time.Unix(int64(message.Date), 0)
	if publishedAt.Year() != 2024 {
		return model.Post{}, nil // Skip messages not from 2024
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
				fmt.Printf("Recovered from panic while fetching comments: %v\n", r)
			}
		}()
		if message.InteractionInfo.ReplyInfo.ReplyCount > 0 {
			var err error
			comments, err = GetMessageComments(tdlibClient, chat.Id, message.Id)
			if err != nil {
				fmt.Printf("Fetch message error: %s\n", err)
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
	case *client.MessagePhoto:
		description = content.Caption.Text
		thumbnailPath = content.Photo.Sizes[0].Photo.Remote.Id
		//thumbnailPath = fetch(tdlibClient, content.Photo.Sizes[0].Photo.Remote.Id)
	case *client.MessageAnimation:
		description = content.Caption.Text
		thumbnailPath = content.Animation.Thumbnail.File.Remote.Id
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
		thumbnailPath = Fetch(tdlibClient, content.Sticker.Sticker.Remote.Id)
	case *client.MessageGiveawayWinners:
		fmt.Println("This message is a giveaway winner:", content)
	case *client.MessageGiveawayCompleted:
		fmt.Println("This message is a giveaway completed:", content)
	case *client.MessageVideoNote:
		thumbnailPath = content.VideoNote.Thumbnail.File.Remote.Id
		videoPath = content.VideoNote.Video.Remote.Id
		//thumbnailPath = fetch(tdlibClient, thumbnailPath)
		//videoPath = fetch(tdlibClient, videoPath)
	case *client.MessageDocument:
		description = content.Document.FileName
		thumbnailPath = content.Document.Thumbnail.File.Remote.Id
		videoPath = content.Document.Document.Remote.Id
		//thumbnailPath = fetch(tdlibClient, thumbnailPath)
		//videoPath = fetch(tdlibClient, videoPath)
	default:
		fmt.Println("Unknown message content type")
	}

	reactions := make(map[string]int)
	if message.InteractionInfo != nil && message.InteractionInfo.Reactions != nil && len(message.InteractionInfo.Reactions.Reactions) > 0 {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("Recovered from panic while processing reactions: %v\n", r)
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
	vc := GetViewCount(message)
	postUid := fmt.Sprintf("%s-%s", messageNumber, channelName)
	sharecount, _ := GetMessageShareCount(tdlibClient, chat.Id, message.Id)

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
	return post, nil
}

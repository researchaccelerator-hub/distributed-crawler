package main

import (
	"flag"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/zelenin/go-tdlib/client"
	"strings"
	"tdlib-scraper/state"
	"tdlib-scraper/telegramhelper"
	"time"
)

func main() {
	crawlid := generateCrawlID()
	log.Info().Msgf("Starting scraper for crawl: %s", crawlid)
	storageRoot := "/Users/tombarber/scraper"
	sm := state.NewStateManager(storageRoot, crawlid)
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	list, err := sm.SeedSetup(parseSeedList())
	// Load progress
	progress, err := sm.LoadProgress()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load progress")
	}
	// Process remaining items
	for i := progress; i < len(list); i++ {
		item := list[i]
		log.Info().Msgf("Processing item: %s", item)

		if err = run(item, storageRoot, *sm); err != nil {
			log.Error().Stack().Err(err).Msgf("Error processing item %s", item)
		}

		// Update progress
		progress = i + 1
		if err = sm.SaveProgress(progress); err != nil {
			log.Fatal().Err(err).Msgf("Failed to save progress: %v", err)
		}
	}

	log.Info().Msg("All items processed successfully.")

}

// parseSeedList parses a command-line flag "seed-list" into a slice of strings.
// The flag is expected to be a comma-separated list of seed channels.
// If the flag is not provided, it logs an informational message and returns an empty slice.
func parseSeedList() []string {
	var stringList string
	flag.StringVar(&stringList, "seed-list", "", "Comma-separated list of seed channels")
	flag.Parse()

	if stringList == "" {
		log.Info().Msg("seed-list argument is not provided")
		return []string{}
	}

	// Split the string into a slice
	values := strings.Split(stringList, ",")
	return values
}

// generateCrawlID generates a unique identifier based on the current timestamp.
// The identifier is formatted as a string in the "YYYYMMDDHHMMSS" format.
func generateCrawlID() string {
	// Get the current timestamp
	currentTime := time.Now()

	// Format the timestamp to a string (e.g., "20060102150405" for YYYYMMDDHHMMSS)
	crawlID := currentTime.Format("20060102150405")

	return crawlID
}

// run connects to a Telegram channel using the provided username and crawl ID,
// retrieves messages and metadata, and writes them to a JSONL file. It handles
// errors during connection, file operations, and message parsing, ensuring
// resources are properly closed. The function returns an error if any operation
// fails.
func run(channelUsername string, storageprefix string, sm state.StateManager) error {
	tdlibClient, err := telegramhelper.TdConnect(storageprefix)
	// Ensure tdlibClient is closed after the function finishes
	defer func() {
		if tdlibClient != nil {
			fmt.Println("Closing tdlibClient...")
			_, err := tdlibClient.Close()
			if err != nil {
				log.Printf("Error closing tdlibClient: %v", err)
			} else {
				log.Info().Msg("tdlibClient closed successfully.")
			}
		}
	}()

	// Search for the channel
	chat, err := tdlibClient.SearchPublicChat(&client.SearchPublicChatRequest{
		Username: channelUsername,
	})

	if err != nil {
		log.Error().Err(err).Stack().Msgf("Failed to find channel: %v", channelUsername)
		return err
	}

	chatdet, err := tdlibClient.GetChat(&client.GetChatRequest{
		ChatId: chat.Id,
	})

	var fromMessageId int64 = 0
	vc, err := telegramhelper.GetTotalChannelViews(tdlibClient, chat.Id)
	pc3, err := telegramhelper.GetMessageCount(tdlibClient, chat.Id)
	sgid := int64(0)
	if chat.Type != nil {
		if supergroupType, ok := chat.Type.(*client.ChatTypeSupergroup); ok {
			sgid = supergroupType.SupergroupId
		}
	}
	supergroup, _ := tdlibClient.GetSupergroup(&client.GetSupergroupRequest{
		SupergroupId: sgid,
	})
	supergroupInfo, err := tdlibClient.GetSupergroupFullInfo(&client.GetSupergroupFullInfoRequest{
		SupergroupId: supergroup.Id,
	})
	for {
		// Get the latest message
		log.Info().Msgf("Fetching from message id %d\n", fromMessageId)
		chatHistory, err := tdlibClient.GetChatHistory(&client.GetChatHistoryRequest{
			ChatId:        chat.Id,
			Limit:         100,
			FromMessageId: fromMessageId,
		})
		if err != nil {
			log.Error().Stack().Err(err).Msg("Failed to get chat history")
			break
		}

		if len(chatHistory.Messages) == 0 {
			log.Info().Msg("No messages found in the channel")
			return nil
		}

		for _, message := range chatHistory.Messages {
			latestMessage := message
			m, err := tdlibClient.GetMessage(&client.GetMessageRequest{
				MessageId: latestMessage.Id,
				ChatId:    chat.Id,
			})

			messageLinkResponse, err := tdlibClient.GetMessageLink(&client.GetMessageLinkRequest{
				ChatId:    message.ChatId,
				MessageId: message.Id,
			})
			if err != nil {
				log.Error().Stack().Err(err).Msg("Failed to get message link")
			}

			post, err := telegramhelper.ParseMessage(m, messageLinkResponse, chatdet, supergroup, supergroupInfo, pc3, vc, channelUsername, tdlibClient)
			if err != nil {
				log.Error().Stack().Err(err).Msg("Failed to parse message")
				break
			}
			if post.PostUID != "" {
				err = sm.StoreData(channelUsername, post)
				if err != nil {
					log.Fatal().Err(err).Stack().Msg("failed to write to file: %w")
					return err
				}
			}
		}
		fromMessageId = chatHistory.Messages[len(chatHistory.Messages)-1].Id

	}
	return nil
}

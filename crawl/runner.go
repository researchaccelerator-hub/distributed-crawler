package crawl

import (
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/researchaccelerator-hub/telegram-scraper/telegramhelper"
	"github.com/rs/zerolog/log"
	"github.com/zelenin/go-tdlib/client"
)

// Run connects to a Telegram channel using the provided username and crawl ID,
// retrieves messages and metadata, and writes them to a JSONL file. It handles
// errors during connection, file operations, and message parsing, ensuring
// resources are properly closed. The function returns an error if any operation
// fails.
func Run(crawlid, channelUsername string, storageprefix string, sm state.StateManager) error {
	tdlibClient, err := telegramhelper.TdConnect(storageprefix)
	// Ensure tdlibClient is closed after the function finishes
	defer func() {
		if tdlibClient != nil {
			log.Debug().Msg("Closing tdlibClient...")
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
	vc, err := telegramhelper.GetTotalChannelViews(tdlibClient, chat.Id, channelUsername)
	pc3, err := telegramhelper.GetMessageCount(tdlibClient, chat.Id, channelUsername)
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
			log.Info().Msgf("No messages found in the channel %s", channelUsername)
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

			_, err = telegramhelper.ParseMessage(crawlid, m, messageLinkResponse, chatdet, supergroup, supergroupInfo, pc3, vc, channelUsername, tdlibClient, sm)
			if err != nil {
				log.Error().Stack().Err(err).Msg("Failed to parse message")
				break
			}
		}
		fromMessageId = chatHistory.Messages[len(chatHistory.Messages)-1].Id

	}
	return nil
}

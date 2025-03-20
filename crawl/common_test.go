// Package crawl provides functionality to crawl Telegram channels and extract data.
package crawl

import (
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/stretchr/testify/mock"
	"github.com/zelenin/go-tdlib/client"
)

// TestFixtures contains common test data used across multiple tests
type TestFixtures struct {
	ChatID        int64
	SupegroupID   int64
	CrawlID       string
	ChannelName   string
	Messages      []*client.Message
	Channel       *channelInfo
	StateManager  *state.StateManager
	TempDirectory string
}

// NewTestFixtures creates a new set of test fixtures with default values
func NewTestFixtures(t *testing.T) *TestFixtures {
	t.Helper()

	// Create a state manager
	stateManager, tmpDir, err := createTempStateManager()
	if err != nil {
		t.Fatalf("Failed to create temp state manager: %v", err)
	}

	chatID := int64(100)
	supergroupID := int64(500)

	// Basic message content
	messages := []*client.Message{
		{
			Id:     1,
			ChatId: chatID,
			Content: &client.MessageText{
				Text: &client.FormattedText{Text: "Message 1"},
			},
		},
		{
			Id:     2,
			ChatId: chatID,
			Content: &client.MessageText{
				Text: &client.FormattedText{Text: "Message 2"},
			},
		},
	}

	// Channel info
	channel := &channelInfo{
		chat: &client.Chat{
			Id: chatID,
			Type: &client.ChatTypeSupergroup{
				SupergroupId: supergroupID,
				IsChannel:    true,
			},
			Title: "Test Channel",
		},
		chatDetails: &client.Chat{
			Id:    chatID,
			Title: "Test Channel Details",
		},
		supergroup: &client.Supergroup{
			Id: supergroupID,
			Usernames: &client.Usernames{
				ActiveUsernames:   []string{"testchannel"},
				DisabledUsernames: []string{},
				EditableUsername:  "testchannel",
			},
		},
		supergroupInfo: &client.SupergroupFullInfo{
			Description: "Test channel description",
			MemberCount: 1000,
		},
		messageCount: 10,
		totalViews:   100,
	}

	return &TestFixtures{
		ChatID:        chatID,
		SupegroupID:   supergroupID,
		CrawlID:       "test-crawl-id",
		ChannelName:   "testchannel",
		Messages:      messages,
		Channel:       channel,
		StateManager:  stateManager,
		TempDirectory: tmpDir,
	}
}

// Cleanup removes any resources created by the fixtures
func (f *TestFixtures) Cleanup() {
	os.RemoveAll(f.TempDirectory)
}

// createTempStateManager creates a temporary StateManager for testing purposes.
// It creates a temporary directory and initializes a StateManager with a unique crawl ID.
// The caller is responsible for cleaning up the temporary directory when done.
func createTempStateManager() (*state.StateManager, string, error) {
	// Create a temporary directory
	tmpDir, err := os.MkdirTemp("", "test-state-")
	if err != nil {
		return nil, "", err
	}

	// Create a state manager with the temp directory
	crawlID := "test-crawl-" + uuid.New().String()
	cfg := state.Config{
		StorageRoot:      "",
		ContainerName:    "",
		BlobNameRoot:     "",
		JobID:            "",
		CrawlID:          crawlID,
		CrawlExecutionID: "",
		DAPREnabled:      false,
		MaxLayers:        0,
	}
	stateManager, err := state.NewStateManager(cfg)

	return stateManager, tmpDir, nil
}

//// CreatePost creates a model.Post with default values for testing
//func CreatePost(postID string) model.Post {
//	return model.Post{
//		PostID:   postID,
//		PostLink: "https://t.me/channel/" + postID,
//		Channel:  "testchannel",
//		PostDate: time.Now(),
//	}
//}

// CreateClientMessage creates a client.Message with the given ID and text
func CreateClientMessage(id int64, text string, chatID int64) *client.Message {
	return &client.Message{
		Id:     id,
		ChatId: chatID,
		Content: &client.MessageText{
			Text: &client.FormattedText{Text: text},
		},
		Date: int32(time.Now().Unix()),
	}
}

// AssertCalled is a helper to assert that a mock was called with specific arguments
func AssertCalled(t *testing.T, mockObj *mock.Mock, methodName string, arguments ...interface{}) {
	t.Helper()
	mockObj.AssertCalled(t, methodName, arguments...)
}

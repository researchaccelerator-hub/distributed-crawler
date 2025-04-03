// Package crawl provides functionality to crawl Telegram channels and extract data.
package crawl

import (
	"errors"
	"github.com/google/uuid"
	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/zelenin/go-tdlib/client"
)

func TestGetChannelInfo(t *testing.T) {
	// Create fixtures
	fixtures := NewTestFixtures(t)
	defer fixtures.Cleanup()

	// Create a page for testing
	testPage := &state.Page{
		ID:       uuid.New().String(),
		URL:      "testchannel",
		Status:   "unfetched",
		Depth:    0,
		Messages: []state.Message{},
	}

	// Create a config for testing
	testConfig := common.CrawlerConfig{
		MinPostDate: time.Now().Add(-30 * 24 * time.Hour), // 30 days ago
		MaxPosts:    100,
	}

	// Table driven tests for different scenarios
	testCases := []struct {
		name                string
		setupMocks          func(*MockTDLibClient)
		getTotalViewsFn     TotalViewsGetter
		getMessageCountFn   MessageCountGetter
		getMemberCountFn    MemberCountGetter
		expectedError       bool
		expectedErrorMsg    string
		expectedChannelInfo func() *channelInfo
	}{
		{
			name: "SearchPublicChatError",
			setupMocks: func(m *MockTDLibClient) {
				m.On("SearchPublicChat", &client.SearchPublicChatRequest{
					Username: "testchannel",
				}).Return(nil, errors.New("channel not found"))
			},
			getTotalViewsFn: func(client crawler.TDLibClient, messages []*client.Message, chatID int64, channelUsername string) (int, error) {
				return 0, nil
			},
			getMessageCountFn: func(client crawler.TDLibClient, messages []*client.Message, chatID int64, channelUsername string) (int, error) {
				return 0, nil
			},
			getMemberCountFn: func(client crawler.TDLibClient, channelUsername string) (int, error) {
				return 0, nil
			},
			expectedError:    true,
			expectedErrorMsg: "channel not found",
			expectedChannelInfo: func() *channelInfo {
				return nil
			},
		},
		{
			name: "Success",
			setupMocks: func(m *MockTDLibClient) {
				chatID := int64(12345)
				supergroupID := int64(67890)

				chat := &client.Chat{
					Id: chatID,
					Type: &client.ChatTypeSupergroup{
						SupergroupId: supergroupID,
						IsChannel:    true,
					},
					Title: "Test Channel",
				}

				chatDetails := &client.Chat{
					Id:    chatID,
					Title: "Test Channel Details",
				}

				supergroup := &client.Supergroup{
					Id: supergroupID,
					Usernames: &client.Usernames{
						ActiveUsernames:   []string{"testchannel"},
						DisabledUsernames: []string{},
						EditableUsername:  "testchannel",
					},
					MemberCount:       1000,
					Date:              123456789,
					HasLinkedChat:     true,
					HasLocation:       false,
					SignMessages:      true,
					IsVerified:        false,
					RestrictionReason: "",
				}

				supergroupInfo := &client.SupergroupFullInfo{
					Description:        "Test channel description",
					MemberCount:        1000,
					AdministratorCount: 5,
					RestrictedCount:    0,
					BannedCount:        0,
					LinkedChatId:       54321,
					CanGetMembers:      true,
					CanSetStickerSet:   true,
					StickerSetId:       0,
				}

				m.On("SearchPublicChat", &client.SearchPublicChatRequest{
					Username: "testchannel",
				}).Return(chat, nil)

				m.On("GetChat", &client.GetChatRequest{
					ChatId: chatID,
				}).Return(chatDetails, nil)

				m.On("GetSupergroup", &client.GetSupergroupRequest{
					SupergroupId: supergroupID,
				}).Return(supergroup, nil)

				m.On("GetSupergroupFullInfo", &client.GetSupergroupFullInfoRequest{
					SupergroupId: supergroupID,
				}).Return(supergroupInfo, nil)

				// Mock telegramhelper.FetchChannelMessages
				messages := []*client.Message{
					{Id: 1, ChatId: chatID},
					{Id: 2, ChatId: chatID},
					{Id: 3, ChatId: chatID},
				}
				
				// We need to mock the message fetching calls
				// This is a simplification since the actual function might make multiple calls
				m.On("GetChatHistory", mock.Anything).Return(&client.Messages{
					TotalCount: 3,
					Messages:   messages,
				}, nil)
			},
			getTotalViewsFn: func(client crawler.TDLibClient, messages []*client.Message, chatID int64, channelUsername string) (int, error) {
				return 5000, nil
			},
			getMessageCountFn: func(client crawler.TDLibClient, messages []*client.Message, chatID int64, channelUsername string) (int, error) {
				return 100, nil
			},
			getMemberCountFn: func(client crawler.TDLibClient, channelUsername string) (int, error) {
				return 1000, nil
			},
			expectedError: false,
			expectedChannelInfo: func() *channelInfo {
				chatID := int64(12345)
				supergroupID := int64(67890)

				return &channelInfo{
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
						MemberCount:       1000,
						Date:              123456789,
						HasLinkedChat:     true,
						HasLocation:       false,
						SignMessages:      true,
						IsVerified:        false,
						RestrictionReason: "",
					},
					supergroupInfo: &client.SupergroupFullInfo{
						Description:        "Test channel description",
						MemberCount:        1000,
						AdministratorCount: 5,
						RestrictedCount:    0,
						BannedCount:        0,
						LinkedChatId:       54321,
						CanGetMembers:      true,
						CanSetStickerSet:   true,
						StickerSetId:       0,
					},
					totalViews:   5000,
					messageCount: 100,
					memberCount:  1000,
				}
			},
		},
	}

	// Run each test case
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup mocks
			mockClient := new(MockTDLibClient)
			tc.setupMocks(mockClient)

			// Call the function with mock helpers
			info, messages, err := getChannelInfoWithDeps(
				mockClient,
				testPage,
				tc.getTotalViewsFn,
				tc.getMessageCountFn,
				tc.getMemberCountFn,
				testConfig,
			)

			// Assert results
			if tc.expectedError {
				assert.Error(t, err)
				if tc.expectedErrorMsg != "" {
					assert.Contains(t, err.Error(), tc.expectedErrorMsg)
				}
				assert.Nil(t, info)
				assert.Nil(t, messages)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, info)

				expectedInfo := tc.expectedChannelInfo()
				if expectedInfo != nil {
					// Compare chat
					assert.Equal(t, expectedInfo.chat.Id, info.chat.Id)
					assert.Equal(t, expectedInfo.chat.Title, info.chat.Title)

					// Compare chat details
					assert.Equal(t, expectedInfo.chatDetails.Id, info.chatDetails.Id)
					assert.Equal(t, expectedInfo.chatDetails.Title, info.chatDetails.Title)

					// Compare other fields
					if expectedInfo.supergroup == nil {
						assert.Nil(t, info.supergroup)
					} else {
						assert.NotNil(t, info.supergroup)
						assert.Equal(t, expectedInfo.supergroup.Id, info.supergroup.Id)
					}

					if expectedInfo.supergroupInfo == nil {
						assert.Nil(t, info.supergroupInfo)
					} else {
						assert.NotNil(t, info.supergroupInfo)
						assert.Equal(t, expectedInfo.supergroupInfo.Description, info.supergroupInfo.Description)
					}

					assert.Equal(t, expectedInfo.totalViews, info.totalViews)
					assert.Equal(t, expectedInfo.messageCount, info.messageCount)
					assert.Equal(t, expectedInfo.memberCount, info.memberCount)
				}
			}

			// Verify all expected calls were made
			mockClient.AssertExpectations(t)
		})
	}
}

// // Package crawl provides functionality to crawl Telegram channels and extract data.
package crawl

//
//import (
//	"errors"
//	"testing"
//
//	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
//	"github.com/stretchr/testify/assert"
//	"github.com/zelenin/go-tdlib/client"
//)
//
//func TestGetChannelInfo(t *testing.T) {
//	// Create fixtures
//	fixtures := NewTestFixtures(t)
//	defer fixtures.Cleanup()
//
//	// Table driven tests for different scenarios
//	testCases := []struct {
//		name                string
//		channelUsername     string
//		setupMocks          func(*MockTDLibClient)
//		getTotalViewsFn     func(crawler.TDLibClient, int64, string) (int, error)
//		getMessageCountFn   func(crawler.TDLibClient, int64, string) (int, error)
//		getMemberCountFn    func(client crawler.TDLibClient, channelUsername string) (int, error)
//		expectedError       bool
//		expectedErrorMsg    string
//		expectedChannelInfo func() *channelInfo
//	}{
//		{
//			name:            "SearchPublicChatError",
//			channelUsername: "testchannel",
//			setupMocks: func(m *MockTDLibClient) {
//				m.On("SearchPublicChat", &client.SearchPublicChatRequest{
//					Username: "testchannel",
//				}).Return(nil, errors.New("channel not found"))
//			},
//			getTotalViewsFn: func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error) {
//				return 0, nil
//			},
//			getMessageCountFn: func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error) {
//				return 0, nil
//			},
//			getMemberCountFn: func(client crawler.TDLibClient, channelUsername string) (int, error) {
//				return 0, nil
//			},
//			expectedError:    true,
//			expectedErrorMsg: "channel not found",
//			expectedChannelInfo: func() *channelInfo {
//				return nil
//			},
//		},
//		{
//			name:            "Success",
//			channelUsername: "testchannel",
//			setupMocks: func(m *MockTDLibClient) {
//				chatID := int64(12345)
//				supergroupID := int64(67890)
//
//				chat := &client.Chat{
//					Id: chatID,
//					Type: &client.ChatTypeSupergroup{
//						SupergroupId: supergroupID,
//						IsChannel:    true,
//					},
//					Title: "Test Channel",
//				}
//
//				chatDetails := &client.Chat{
//					Id:    chatID,
//					Title: "Test Channel Details",
//				}
//
//				supergroup := &client.Supergroup{
//					Id: supergroupID,
//					Usernames: &client.Usernames{
//						ActiveUsernames:   []string{"testchannel"},
//						DisabledUsernames: []string{},
//						EditableUsername:  "testchannel",
//					},
//					MemberCount:       1000,
//					Date:              123456789,
//					HasLinkedChat:     true,
//					HasLocation:       false,
//					SignMessages:      true,
//					IsVerified:        false,
//					RestrictionReason: "",
//				}
//
//				supergroupInfo := &client.SupergroupFullInfo{
//					Description:        "Test channel description",
//					MemberCount:        1000,
//					AdministratorCount: 5,
//					RestrictedCount:    0,
//					BannedCount:        0,
//					LinkedChatId:       54321,
//					CanGetMembers:      true,
//					CanSetStickerSet:   true,
//					StickerSetId:       0,
//				}
//
//				m.On("SearchPublicChat", &client.SearchPublicChatRequest{
//					Username: "testchannel",
//				}).Return(chat, nil)
//
//				m.On("GetChat", &client.GetChatRequest{
//					ChatId: chatID,
//				}).Return(chatDetails, nil)
//
//				m.On("GetSupergroup", &client.GetSupergroupRequest{
//					SupergroupId: supergroupID,
//				}).Return(supergroup, nil)
//
//				m.On("GetSupergroupFullInfo", &client.GetSupergroupFullInfoRequest{
//					SupergroupId: supergroupID,
//				}).Return(supergroupInfo, nil)
//			},
//			getTotalViewsFn: func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error) {
//				return 5000, nil
//			},
//			getMessageCountFn: func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error) {
//				return 100, nil
//			},
//			expectedError: false,
//			expectedChannelInfo: func() *channelInfo {
//				chatID := int64(12345)
//				supergroupID := int64(67890)
//
//				return &channelInfo{
//					chat: &client.Chat{
//						Id: chatID,
//						Type: &client.ChatTypeSupergroup{
//							SupergroupId: supergroupID,
//							IsChannel:    true,
//						},
//						Title: "Test Channel",
//					},
//					chatDetails: &client.Chat{
//						Id:    chatID,
//						Title: "Test Channel Details",
//					},
//					supergroup: &client.Supergroup{
//						Id: supergroupID,
//						Usernames: &client.Usernames{
//							ActiveUsernames:   []string{"testchannel"},
//							DisabledUsernames: []string{},
//							EditableUsername:  "testchannel",
//						},
//						MemberCount:       1000,
//						Date:              123456789,
//						HasLinkedChat:     true,
//						HasLocation:       false,
//						SignMessages:      true,
//						IsVerified:        false,
//						RestrictionReason: "",
//					},
//					supergroupInfo: &client.SupergroupFullInfo{
//						Description:        "Test channel description",
//						MemberCount:        1000,
//						AdministratorCount: 5,
//						RestrictedCount:    0,
//						BannedCount:        0,
//						LinkedChatId:       54321,
//						CanGetMembers:      true,
//						CanSetStickerSet:   true,
//						StickerSetId:       0,
//					},
//					totalViews:   5000,
//					messageCount: 100,
//				}
//			},
//		},
//		{
//			name:            "NonSupergroupChannel",
//			channelUsername: "testchannel",
//			setupMocks: func(m *MockTDLibClient) {
//				chatID := int64(12345)
//
//				chat := &client.Chat{
//					Id: chatID,
//					Type: &client.ChatTypePrivate{
//						UserId: 987654,
//					},
//					Title: "Private Chat",
//				}
//
//				chatDetails := &client.Chat{
//					Id:    chatID,
//					Title: "Private Chat Details",
//				}
//
//				m.On("SearchPublicChat", &client.SearchPublicChatRequest{
//					Username: "testchannel",
//				}).Return(chat, nil)
//
//				m.On("GetChat", &client.GetChatRequest{
//					ChatId: chatID,
//				}).Return(chatDetails, nil)
//			},
//			getTotalViewsFn: func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error) {
//				return 0, nil
//			},
//			getMessageCountFn: func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error) {
//				return 50, nil
//			},
//			expectedError: false,
//			expectedChannelInfo: func() *channelInfo {
//				chatID := int64(12345)
//
//				return &channelInfo{
//					chat: &client.Chat{
//						Id: chatID,
//						Type: &client.ChatTypePrivate{
//							UserId: 987654,
//						},
//						Title: "Private Chat",
//					},
//					chatDetails: &client.Chat{
//						Id:    chatID,
//						Title: "Private Chat Details",
//					},
//					supergroup:     nil,
//					supergroupInfo: nil,
//					totalViews:     0,
//					messageCount:   50,
//				}
//			},
//		},
//		{
//			name:            "TotalViewsError",
//			channelUsername: "testchannel",
//			setupMocks: func(m *MockTDLibClient) {
//				chatID := int64(12345)
//
//				chat := &client.Chat{
//					Id: chatID,
//					Type: &client.ChatTypeBasicGroup{
//						BasicGroupId: 54321,
//					},
//					Title: "Basic Group",
//				}
//
//				chatDetails := &client.Chat{
//					Id:    chatID,
//					Title: "Basic Group Details",
//				}
//
//				m.On("SearchPublicChat", &client.SearchPublicChatRequest{
//					Username: "testchannel",
//				}).Return(chat, nil)
//
//				m.On("GetChat", &client.GetChatRequest{
//					ChatId: chatID,
//				}).Return(chatDetails, nil)
//			},
//			getTotalViewsFn: func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error) {
//				return 0, errors.New("views count error")
//			},
//			getMessageCountFn: func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error) {
//				return 200, nil
//			},
//			expectedError: false,
//			expectedChannelInfo: func() *channelInfo {
//				chatID := int64(12345)
//
//				return &channelInfo{
//					chat: &client.Chat{
//						Id: chatID,
//						Type: &client.ChatTypeBasicGroup{
//							BasicGroupId: 54321,
//						},
//						Title: "Basic Group",
//					},
//					chatDetails: &client.Chat{
//						Id:    chatID,
//						Title: "Basic Group Details",
//					},
//					supergroup:     nil,
//					supergroupInfo: nil,
//					totalViews:     0, // Default when error occurs
//					messageCount:   200,
//				}
//			},
//		},
//		{
//			name:            "MessageCountError",
//			channelUsername: "testchannel",
//			setupMocks: func(m *MockTDLibClient) {
//				chatID := int64(12345)
//
//				chat := &client.Chat{
//					Id: chatID,
//					Type: &client.ChatTypeBasicGroup{
//						BasicGroupId: 54321,
//					},
//					Title: "Basic Group",
//				}
//
//				chatDetails := &client.Chat{
//					Id:    chatID,
//					Title: "Basic Group Details",
//				}
//
//				m.On("SearchPublicChat", &client.SearchPublicChatRequest{
//					Username: "testchannel",
//				}).Return(chat, nil)
//
//				m.On("GetChat", &client.GetChatRequest{
//					ChatId: chatID,
//				}).Return(chatDetails, nil)
//			},
//			getTotalViewsFn: func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error) {
//				return 4000, nil
//			},
//			getMessageCountFn: func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error) {
//				return 0, errors.New("message count error")
//			},
//			expectedError: false,
//			expectedChannelInfo: func() *channelInfo {
//				chatID := int64(12345)
//
//				return &channelInfo{
//					chat: &client.Chat{
//						Id: chatID,
//						Type: &client.ChatTypeBasicGroup{
//							BasicGroupId: 54321,
//						},
//						Title: "Basic Group",
//					},
//					chatDetails: &client.Chat{
//						Id:    chatID,
//						Title: "Basic Group Details",
//					},
//					supergroup:     nil,
//					supergroupInfo: nil,
//					totalViews:     4000,
//					messageCount:   0, // Default when error occurs
//				}
//			},
//		},
//		{
//			name:            "AllNonCriticalErrorsTogether",
//			channelUsername: "testchannel",
//			setupMocks: func(m *MockTDLibClient) {
//				chatID := int64(12345)
//				supergroupID := int64(67890)
//
//				chat := &client.Chat{
//					Id: chatID,
//					Type: &client.ChatTypeSupergroup{
//						SupergroupId: supergroupID,
//						IsChannel:    true,
//					},
//					Title: "Test Channel",
//				}
//
//				chatDetails := &client.Chat{
//					Id:    chatID,
//					Title: "Test Channel Details",
//				}
//
//				m.On("SearchPublicChat", &client.SearchPublicChatRequest{
//					Username: "testchannel",
//				}).Return(chat, nil)
//
//				m.On("GetChat", &client.GetChatRequest{
//					ChatId: chatID,
//				}).Return(chatDetails, nil)
//
//				m.On("GetSupergroup", &client.GetSupergroupRequest{
//					SupergroupId: supergroupID,
//				}).Return(nil, errors.New("supergroup fetch error"))
//			},
//			getTotalViewsFn: func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error) {
//				return 0, errors.New("views count error")
//			},
//			getMessageCountFn: func(client crawler.TDLibClient, chatID int64, channelUsername string) (int, error) {
//				return 0, errors.New("message count error")
//			},
//			expectedError: false,
//			expectedChannelInfo: func() *channelInfo {
//				chatID := int64(12345)
//				supergroupID := int64(67890)
//
//				return &channelInfo{
//					chat: &client.Chat{
//						Id: chatID,
//						Type: &client.ChatTypeSupergroup{
//							SupergroupId: supergroupID,
//							IsChannel:    true,
//						},
//						Title: "Test Channel",
//					},
//					chatDetails: &client.Chat{
//						Id:    chatID,
//						Title: "Test Channel Details",
//					},
//					supergroup:     nil,
//					supergroupInfo: nil,
//					totalViews:     0,
//					messageCount:   0,
//				}
//			},
//		},
//	}
//
//	// Run each test case
//	for _, tc := range testCases {
//		t.Run(tc.name, func(t *testing.T) {
//			// Setup mocks
//			mockClient := new(MockTDLibClient)
//			tc.setupMocks(mockClient)
//
//			// Call the function with mock helpers
//			info, err := getChannelInfoWithDeps(mockClient, tc.channelUsername, tc.getTotalViewsFn, tc.getMessageCountFn, tc.getMemberCountFn)
//
//			// Assert results
//			if tc.expectedError {
//				assert.Error(t, err)
//				if tc.expectedErrorMsg != "" {
//					assert.Contains(t, err.Error(), tc.expectedErrorMsg)
//				}
//				assert.Nil(t, info)
//			} else {
//				assert.NoError(t, err)
//				assert.NotNil(t, info)
//
//				expectedInfo := tc.expectedChannelInfo()
//
//				// Compare chat
//				if expectedInfo.chat != nil {
//					assert.Equal(t, expectedInfo.chat.Id, info.chat.Id)
//					assert.Equal(t, expectedInfo.chat.Title, info.chat.Title)
//				}
//
//				// Compare chat details
//				if expectedInfo.chatDetails != nil {
//					assert.Equal(t, expectedInfo.chatDetails.Id, info.chatDetails.Id)
//					assert.Equal(t, expectedInfo.chatDetails.Title, info.chatDetails.Title)
//				}
//
//				// Compare other fields
//				if expectedInfo.supergroup == nil {
//					assert.Nil(t, info.supergroup)
//				} else {
//					assert.NotNil(t, info.supergroup)
//					assert.Equal(t, expectedInfo.supergroup.Id, info.supergroup.Id)
//				}
//
//				if expectedInfo.supergroupInfo == nil {
//					assert.Nil(t, info.supergroupInfo)
//				} else {
//					assert.NotNil(t, info.supergroupInfo)
//					assert.Equal(t, expectedInfo.supergroupInfo.Description, info.supergroupInfo.Description)
//				}
//
//				assert.Equal(t, expectedInfo.totalViews, info.totalViews)
//				assert.Equal(t, expectedInfo.messageCount, info.messageCount)
//			}
//
//			// Verify all expected calls were made
//			mockClient.AssertExpectations(t)
//		})
//	}
//}

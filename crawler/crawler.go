package crawler

import "github.com/zelenin/go-tdlib/client"

type TDLibClient interface {
	GetMessage(req *client.GetMessageRequest) (*client.Message, error)
	GetMessageLink(req *client.GetMessageLinkRequest) (*client.MessageLink, error)
	GetMessageThreadHistory(req *client.GetMessageThreadHistoryRequest) (*client.Messages, error)
	GetRemoteFile(req *client.GetRemoteFileRequest) (*client.File, error)
	DownloadFile(req *client.DownloadFileRequest) (*client.File, error)
	GetChatHistory(req *client.GetChatHistoryRequest) (*client.Messages, error)
	SearchPublicChat(req *client.SearchPublicChatRequest) (*client.Chat, error)
	GetChat(req *client.GetChatRequest) (*client.Chat, error)
	GetSupergroup(req *client.GetSupergroupRequest) (*client.Supergroup, error)
	GetSupergroupFullInfo(req *client.GetSupergroupFullInfoRequest) (*client.SupergroupFullInfo, error)
	Close() (*client.Ok, error)
	GetMe() (*client.User, error)
	GetBasicGroupFullInfo(req *client.GetBasicGroupFullInfoRequest) (*client.BasicGroupFullInfo, error)
}

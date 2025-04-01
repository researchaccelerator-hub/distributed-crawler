package crawler

import "github.com/zelenin/go-tdlib/client"

// TDLibClient interface defines the methods we need from the TDLib client
type TDLibClient interface {
	GetMessage(req *client.GetMessageRequest) (*client.Message, error)
	GetMessageLink(req *client.GetMessageLinkRequest) (*client.MessageLink, error)
	GetMessageThreadHistory(req *client.GetMessageThreadHistoryRequest) (*client.Messages, error)
	GetMessageThread(req *client.GetMessageThreadRequest) (*client.MessageThreadInfo, error)
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
	GetUser(*client.GetUserRequest) (*client.User, error)
}

// CrawlerOptions holds configuration options for a crawler
type CrawlerOptions struct {
	StoragePrefix string
	Config        interface{}
	// Add other options as needed
}

// Option is a function that sets an option on a crawler
type Option func(*CrawlerOptions)

// WithStoragePrefix sets the storage prefix for the crawler
func WithStoragePrefix(prefix string) Option {
	return func(o *CrawlerOptions) {
		o.StoragePrefix = prefix
	}
}

// WithConfig sets the configuration for the crawler
func WithConfig(config interface{}) Option {
	return func(o *CrawlerOptions) {
		o.Config = config
	}
}
package telegramhelper

import (
	"context"
	"fmt"
	"testing"

	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/crawler"
	"github.com/zelenin/go-tdlib/client"
)

// MockTDLibClient is a mock implementation for testing
type MockTDLibClient struct {
	ID     string
	Closed bool
}

func (m *MockTDLibClient) Close() (*client.Ok, error) {
	m.Closed = true
	return nil, nil
}

// Implement other required methods (stub implementations for testing)
func (m *MockTDLibClient) GetMessage(req *client.GetMessageRequest) (*client.Message, error) { return nil, nil }
func (m *MockTDLibClient) GetMessageLink(req *client.GetMessageLinkRequest) (*client.MessageLink, error) { return nil, nil }
func (m *MockTDLibClient) GetMessageThreadHistory(req *client.GetMessageThreadHistoryRequest) (*client.Messages, error) { return nil, nil }
func (m *MockTDLibClient) GetMessageThread(req *client.GetMessageThreadRequest) (*client.MessageThreadInfo, error) { return nil, nil }
func (m *MockTDLibClient) GetRemoteFile(req *client.GetRemoteFileRequest) (*client.File, error) { return nil, nil }
func (m *MockTDLibClient) DownloadFile(req *client.DownloadFileRequest) (*client.File, error) { return nil, nil }
func (m *MockTDLibClient) GetChatHistory(req *client.GetChatHistoryRequest) (*client.Messages, error) { return nil, nil }
func (m *MockTDLibClient) SearchPublicChat(req *client.SearchPublicChatRequest) (*client.Chat, error) { return nil, nil }
func (m *MockTDLibClient) GetChat(req *client.GetChatRequest) (*client.Chat, error) { return nil, nil }
func (m *MockTDLibClient) GetSupergroup(req *client.GetSupergroupRequest) (*client.Supergroup, error) { return nil, nil }
func (m *MockTDLibClient) GetSupergroupFullInfo(req *client.GetSupergroupFullInfoRequest) (*client.SupergroupFullInfo, error) { return nil, nil }
func (m *MockTDLibClient) GetMe() (*client.User, error) { return nil, nil }
func (m *MockTDLibClient) GetBasicGroupFullInfo(req *client.GetBasicGroupFullInfoRequest) (*client.BasicGroupFullInfo, error) { return nil, nil }
func (m *MockTDLibClient) GetUser(*client.GetUserRequest) (*client.User, error) { return nil, nil }
func (m *MockTDLibClient) DeleteFile(req *client.DeleteFileRequest) (*client.Ok, error) { return nil, nil }

// MockPoolTelegramService is a mock service that creates mock clients for pool testing
type MockPoolTelegramService struct {
	CreatedClients []*MockTDLibClient
	CreateError    error
}

func (m *MockPoolTelegramService) InitializeClient(storagePrefix string) (crawler.TDLibClient, error) {
	return m.InitializeClientWithConfig(storagePrefix, common.CrawlerConfig{})
}

func (m *MockPoolTelegramService) InitializeClientWithConfig(storagePrefix string, config common.CrawlerConfig) (crawler.TDLibClient, error) {
	if m.CreateError != nil {
		return nil, m.CreateError
	}
	
	client := &MockTDLibClient{
		ID: fmt.Sprintf("mock-client-%d", len(m.CreatedClients)),
	}
	m.CreatedClients = append(m.CreatedClients, client)
	return client, nil
}

func (m *MockPoolTelegramService) GetMe(libClient crawler.TDLibClient) (*client.User, error) {
	return nil, nil
}

func TestConnectionPoolReuseWithoutDisconnect(t *testing.T) {
	// Create a pool with mock service
	mockService := &MockPoolTelegramService{}
	
	pool := &ConnectionPool{
		availableConns: make(map[string]crawler.TDLibClient),
		inUseConns:     make(map[string]crawler.TDLibClient),
		maxSize:        2,
		service:        mockService,
		storagePrefix:  "test",
		defaultConfig:  common.CrawlerConfig{},
		connDirMap:     make(map[string]string),
	}

	ctx := context.Background()

	// Get a connection from the pool
	client1, connID1, err := pool.GetConnection(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}

	if len(mockService.CreatedClients) != 1 {
		t.Errorf("Expected 1 client to be created, got %d", len(mockService.CreatedClients))
	}

	// Cast to mock client to check state
	mockClient1, ok := client1.(*MockTDLibClient)
	if !ok {
		t.Fatalf("Expected MockTDLibClient, got %T", client1)
	}

	// Verify the client is not closed initially
	if mockClient1.Closed {
		t.Error("Client should not be closed initially")
	}

	// Release the connection back to the pool
	pool.ReleaseConnection(connID1)

	// Verify the client was NOT closed during release
	if mockClient1.Closed {
		t.Error("Client should NOT be closed when released to pool (to avoid rate limiting)")
	}

	// Get a connection again - should reuse the same one
	client2, connID2, err := pool.GetConnection(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection again: %v", err)
	}

	// Should be the same client instance
	if client1 != client2 {
		t.Error("Expected to reuse the same client instance")
	}

	// Should be the same connection ID
	if connID1 != connID2 {
		t.Error("Expected to reuse the same connection ID")
	}

	// Should still not have created additional clients
	if len(mockService.CreatedClients) != 1 {
		t.Errorf("Expected still only 1 client created, got %d", len(mockService.CreatedClients))
	}

	// Verify pool stats
	stats := pool.Stats()
	expectedStats := map[string]int{
		"available": 0, // Client is in use
		"inUse":     1,
		"maxSize":   2,
	}

	for key, expected := range expectedStats {
		if stats[key] != expected {
			t.Errorf("Stats[%s]: expected %d, got %d", key, expected, stats[key])
		}
	}
}

func TestConnectionPoolErrorHandling(t *testing.T) {
	// Create a pool with mock service
	mockService := &MockPoolTelegramService{}
	
	pool := &ConnectionPool{
		availableConns: make(map[string]crawler.TDLibClient),
		inUseConns:     make(map[string]crawler.TDLibClient),
		maxSize:        2,
		service:        mockService,
		storagePrefix:  "test",
		defaultConfig:  common.CrawlerConfig{},
		connDirMap:     make(map[string]string),
	}

	ctx := context.Background()

	// Get a connection from the pool
	client1, connID1, err := pool.GetConnection(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}

	// Cast to mock client to check state
	mockClient1, ok := client1.(*MockTDLibClient)
	if !ok {
		t.Fatalf("Expected MockTDLibClient, got %T", client1)
	}

	// Handle connection error - this SHOULD disconnect and create a new one
	client2, connID2, err := pool.HandleConnectionError(ctx, connID1)
	if err != nil {
		t.Fatalf("Failed to handle connection error: %v", err)
	}

	// Original client should be closed
	if !mockClient1.Closed {
		t.Error("Original client should be closed when handling error")
	}

	// Should have created a new client
	if len(mockService.CreatedClients) != 2 {
		t.Errorf("Expected 2 clients to be created after error handling, got %d", len(mockService.CreatedClients))
	}

	// Should be a different client instance
	if client1 == client2 {
		t.Error("Expected a new client instance after error handling")
	}

	// Connection ID should be the same (reusing the slot)
	if connID1 != connID2 {
		t.Error("Expected the same connection ID after error handling")
	}
}
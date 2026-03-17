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

	// Cast to mock client to check state (GetConnection wraps with RateLimitedTDLibClient)
	wrapped1, ok := client1.(*RateLimitedTDLibClient)
	if !ok {
		t.Fatalf("Expected *RateLimitedTDLibClient, got %T", client1)
	}
	mockClient1, ok := wrapped1.inner.(*MockTDLibClient)
	if !ok {
		t.Fatalf("Expected inner MockTDLibClient, got %T", wrapped1.inner)
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

func TestRetireConnection_RemovesAndClosesClient(t *testing.T) {
	mockService := &MockPoolTelegramService{}

	pool := &ConnectionPool{
		availableConns: make(map[string]crawler.TDLibClient),
		inUseConns:     make(map[string]crawler.TDLibClient),
		maxSize:        3,
		service:        mockService,
		storagePrefix:  "test",
		defaultConfig:  common.CrawlerConfig{},
		connDirMap:     make(map[string]string),
	}

	ctx := context.Background()

	// Acquire two connections.
	_, connID1, err := pool.GetConnection(ctx)
	if err != nil {
		t.Fatalf("GetConnection 1: %v", err)
	}
	_, connID2, err := pool.GetConnection(ctx)
	if err != nil {
		t.Fatalf("GetConnection 2: %v", err)
	}

	// Retire the first connection.
	pool.RetireConnection(connID1)

	// The retired client must be closed.
	retired := mockService.CreatedClients[0]
	if !retired.Closed {
		t.Error("retired client should be closed")
	}

	// Pool total size should now be 1 (connID2 still in use).
	stats := pool.Stats()
	total := stats["available"] + stats["inUse"]
	if total != 1 {
		t.Errorf("expected total pool size 1 after retirement, got %d", total)
	}

	// The retired connID must no longer be findable.
	pool.mu.Lock()
	_, inUse := pool.inUseConns[connID1]
	_, avail := pool.availableConns[connID1]
	pool.mu.Unlock()
	if inUse || avail {
		t.Error("retired connection should not appear in either pool map")
	}

	// The surviving connection is unaffected.
	pool.ReleaseConnection(connID2)
	stats = pool.Stats()
	if stats["available"] != 1 {
		t.Errorf("surviving connection should be available after release, got available=%d", stats["available"])
	}
}

func TestRetireConnection_UnknownID_NoOp(t *testing.T) {
	pool := &ConnectionPool{
		availableConns: make(map[string]crawler.TDLibClient),
		inUseConns:     make(map[string]crawler.TDLibClient),
		maxSize:        2,
		service:        &MockPoolTelegramService{},
		storagePrefix:  "test",
		defaultConfig:  common.CrawlerConfig{},
		connDirMap:     make(map[string]string),
	}

	// Should not panic for an unknown ID.
	pool.RetireConnection("does-not-exist")

	stats := pool.Stats()
	if stats["available"] != 0 || stats["inUse"] != 0 {
		t.Error("pool should remain empty after no-op retirement")
	}
}

func TestRetireConnection_PoolDrainedToZero(t *testing.T) {
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

	_, id1, _ := pool.GetConnection(ctx)
	_, id2, _ := pool.GetConnection(ctx)

	pool.RetireConnection(id1)
	pool.RetireConnection(id2)

	stats := pool.Stats()
	total := stats["available"] + stats["inUse"]
	if total != 0 {
		t.Errorf("pool should be empty after retiring all connections, got total=%d", total)
	}

	// All created clients should be closed.
	for i, c := range mockService.CreatedClients {
		if !c.Closed {
			t.Errorf("client[%d] should be closed after retirement", i)
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

	// Cast to mock client to check state (GetConnection wraps with RateLimitedTDLibClient)
	wrapped1, ok := client1.(*RateLimitedTDLibClient)
	if !ok {
		t.Fatalf("Expected *RateLimitedTDLibClient, got %T", client1)
	}
	mockClient1, ok := wrapped1.inner.(*MockTDLibClient)
	if !ok {
		t.Fatalf("Expected inner MockTDLibClient, got %T", wrapped1.inner)
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
package state

import (
	"context"
	"io"
	"os"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

// TestAzureClient implements a mock Azure client for testing
type TestAzureClient struct {
	mu    sync.Mutex
	blobs map[string][]byte // key is "container/blob"
}

// NewTestAzureClient creates a new test Azure client
func NewTestAzureClient() *TestAzureClient {
	return &TestAzureClient{
		blobs: make(map[string][]byte),
	}
}

func (c *TestAzureClient) getKey(containerName, blobName string) string {
	return containerName + "/" + blobName
}

// UploadStream simulates uploading a stream to Azure
func (c *TestAzureClient) UploadStream(ctx context.Context, containerName, blobName string, body io.Reader, options *azblob.UploadStreamOptions) (azblob.UploadStreamResponse, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return azblob.UploadStreamResponse{}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	key := c.getKey(containerName, blobName)
	c.blobs[key] = data

	return azblob.UploadStreamResponse{}, nil
}

// DownloadFile simulates downloading a file from Azure
func (c *TestAzureClient) DownloadFile(ctx context.Context, containerName, blobName string, file *os.File, options *azblob.DownloadFileOptions) (int64, error) {
	c.mu.Lock()
	key := c.getKey(containerName, blobName)
	data, ok := c.blobs[key]
	c.mu.Unlock()

	if !ok {
		return 0, os.ErrNotExist
	}

	n, err := file.Write(data)
	if err != nil {
		return 0, err
	}

	if _, err := file.Seek(0, 0); err != nil {
		return 0, err
	}

	return int64(n), nil
}

// UploadFile simulates uploading a file to Azure
func (c *TestAzureClient) UploadFile(ctx context.Context, containerName, blobName string, file *os.File, options *azblob.UploadFileOptions) (azblob.UploadFileResponse, error) {
	data, err := io.ReadAll(file)
	if err != nil {
		return azblob.UploadFileResponse{}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	key := c.getKey(containerName, blobName)
	c.blobs[key] = data

	return azblob.UploadFileResponse{}, nil
}

// ServiceClient returns a mock service client with container client methods
func (c *TestAzureClient) ServiceClient() *TestServiceClient {
	return &TestServiceClient{
		azureClient: c,
	}
}

// TestServiceClient is a mock for the Azure service client
type TestServiceClient struct {
	azureClient *TestAzureClient
}

// NewContainerClient returns a mock container client
func (s *TestServiceClient) NewContainerClient(containerName string) *TestContainerClient {
	return &TestContainerClient{
		azureClient:   s.azureClient,
		containerName: containerName,
	}
}

// TestContainerClient is a mock for the Azure container client
type TestContainerClient struct {
	azureClient   *TestAzureClient
	containerName string
}

// NewBlobClient returns a mock blob client
func (c *TestContainerClient) NewBlobClient(blobName string) *TestBlobClient {
	return &TestBlobClient{
		azureClient:   c.azureClient,
		containerName: c.containerName,
		blobName:      blobName,
	}
}

// NewAppendBlobClient returns a mock append blob client
func (c *TestContainerClient) NewAppendBlobClient(blobName string) *TestAppendBlobClient {
	return &TestAppendBlobClient{
		azureClient:   c.azureClient,
		containerName: c.containerName,
		blobName:      blobName,
	}
}

// TestBlobClient is a mock for the Azure blob client
type TestBlobClient struct {
	azureClient   *TestAzureClient
	containerName string
	blobName      string
}

// GetProperties simulates checking if a blob exists
func (c *TestBlobClient) GetProperties(ctx context.Context, options interface{}) (interface{}, error) {
	c.azureClient.mu.Lock()
	defer c.azureClient.mu.Unlock()

	key := c.azureClient.getKey(c.containerName, c.blobName)
	_, ok := c.azureClient.blobs[key]
	if !ok {
		return nil, os.ErrNotExist
	}

	return struct{}{}, nil
}

// TestAppendBlobClient is a mock for the Azure append blob client
type TestAppendBlobClient struct {
	azureClient   *TestAzureClient
	containerName string
	blobName      string
}

// Create simulates creating an append blob
func (c *TestAppendBlobClient) Create(ctx context.Context, options interface{}) (interface{}, error) {
	c.azureClient.mu.Lock()
	defer c.azureClient.mu.Unlock()

	key := c.azureClient.getKey(c.containerName, c.blobName)
	c.azureClient.blobs[key] = []byte{}

	return struct{}{}, nil
}

// AppendBlock simulates appending a block to an append blob
func (c *TestAppendBlobClient) AppendBlock(ctx context.Context, body io.ReadSeekCloser, options interface{}) (interface{}, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}

	c.azureClient.mu.Lock()
	defer c.azureClient.mu.Unlock()

	key := c.azureClient.getKey(c.containerName, c.blobName)
	if existing, ok := c.azureClient.blobs[key]; ok {
		c.azureClient.blobs[key] = append(existing, data...)
	} else {
		c.azureClient.blobs[key] = data
	}

	return struct{}{}, nil
}

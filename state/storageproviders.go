package state

//
//import (
//	"bytes"
//	"context"
//	"fmt"
//	"io"
//	"os"
//	"path/filepath"
//	"strings"
//
//	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
//	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
//)
//
//// Local file system implementation of StorageProvider
//type LocalStorageProvider struct{}
//
//func NewLocalStorageProvider() *LocalStorageProvider {
//	return &LocalStorageProvider{}
//}
//
//func (p *LocalStorageProvider) ReadFile(path string) ([]byte, error) {
//	return os.ReadFile(path)
//}
//
//func (p *LocalStorageProvider) WriteFile(path string, data []byte) error {
//	dir := filepath.Dir(path)
//	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
//		return err
//	}
//	return os.WriteFile(path, data, 0644)
//}
//
//func (p *LocalStorageProvider) AppendToFile(path string, data []byte) error {
//	dir := filepath.Dir(path)
//	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
//		return err
//	}
//
//	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
//	if err != nil {
//		return err
//	}
//	defer file.Close()
//
//	_, err = file.Write(data)
//	return err
//}
//
//func (p *LocalStorageProvider) CreateDir(path string) error {
//	return os.MkdirAll(path, os.ModePerm)
//}
//
//func (p *LocalStorageProvider) FileExists(path string) (bool, error) {
//	_, err := os.Stat(path)
//	if err == nil {
//		return true, nil
//	}
//	if os.IsNotExist(err) {
//		return false, nil
//	}
//	return false, err
//}
//
//func (p *LocalStorageProvider) DeleteFile(path string) error {
//	return os.Remove(path)
//}
//
//// Azure Blob Storage implementation of BlobStorageProvider
//type AzureBlobProvider struct {
//	client *azblob.Client
//}
//
//func NewAzureBlobProvider(accountURL string) (*AzureBlobProvider, error) {
//	cred, err := azidentity.NewDefaultAzureCredential(nil)
//	if err != nil {
//		return nil, fmt.Errorf("failed to create Azure credential: %w", err)
//	}
//
//	client, err := azblob.NewClient(accountURL, cred, nil)
//	if err != nil {
//		return nil, fmt.Errorf("failed to create Azure Blob Storage client: %w", err)
//	}
//
//	return &AzureBlobProvider{
//		client: client,
//	}, nil
//}
//
//func (p *AzureBlobProvider) UploadStream(ctx context.Context, containerName, blobName string, body io.Reader, options *azblob.UploadStreamOptions) (interface{}, error) {
//	return p.client.UploadStream(ctx, containerName, blobName, body, options)
//}
//
//func (p *AzureBlobProvider) DownloadFile(ctx context.Context, containerName, blobName string, file *os.File, options *azblob.DownloadFileOptions) (int64, error) {
//	return p.client.DownloadFile(ctx, containerName, blobName, file, options)
//}
//
//func (p *AzureBlobProvider) UploadFile(ctx context.Context, containerName, blobName string, file *os.File, options *azblob.UploadFileOptions) (interface{}, error) {
//	return p.client.UploadFile(ctx, containerName, blobName, file, options)
//}
//
//func (p *AzureBlobProvider) BlobExists(containerName, blobName string) (bool, error) {
//	blobClient := p.client.ServiceClient().NewContainerClient(containerName).NewBlobClient(blobName)
//	_, err := blobClient.GetProperties(context.Background(), nil)
//	if err == nil {
//		return true, nil
//	}
//	if strings.Contains(err.Error(), "404") {
//		return false, nil
//	}
//	return false, err
//}
//
//func (p *AzureBlobProvider) AppendToBlob(containerName, blobName string, data []byte) error {
//	containerClient := p.client.ServiceClient().NewContainerClient(containerName)
//	appendBlobClient := containerClient.NewAppendBlobClient(blobName)
//
//	// Check if blob exists, if not create it
//	exists, err := p.BlobExists(containerName, blobName)
//	if err != nil {
//		return fmt.Errorf("error checking blob existence: %w", err)
//	}
//
//	if !exists {
//		// Create the append blob
//		_, err = appendBlobClient.Create(context.Background(), nil)
//		if err != nil {
//			return fmt.Errorf("failed to create append blob: %w", err)
//		}
//	}
//
//	// Append the data
//	reader := bytes.NewReader(data)
//	readSeekCloser := readSeekCloserWrapper{reader}
//
//	_, err = appendBlobClient.AppendBlock(context.Background(), readSeekCloser, nil)
//	if err != nil {
//		return fmt.Errorf("failed to append block: %w", err)
//	}
//
//	return nil
//}
//
////// Factory function to create StateManager with proper configuration
////func NewStateManagerFromEnv(crawlid string) (*StateManager, error) {
////	storageRoot := os.Getenv("STORAGE_ROOT")
////	if storageRoot == "" {
////		storageRoot = "./storage" // Default value
////	}
////
////	containerName := os.Getenv("CONTAINER_NAME")
////	blobNameRoot := os.Getenv("BLOB_NAME")
////	jobID := os.Getenv("JOB_UID")
////	accountURL := os.Getenv("AZURE_STORAGE_ACCOUNT_URL")
////
////	config := Config{
////		StorageRoot:   storageRoot,
////		ContainerName: containerName,
////		BlobNameRoot:  blobNameRoot,
////		JobID:         jobID,
////		CrawlID:       crawlid,
////	}
////
////	storageProvider := NewLocalStorageProvider()
////
////	var blobProvider BlobStorageProvider
////	if containerName != "" && blobNameRoot != "" && accountURL != "" {
////		var err error
////		blobProvider, err = NewAzureBlobProvider(accountURL)
////		if err != nil {
////			return nil, fmt.Errorf("failed to create Azure Blob provider: %w", err)
////		}
////	}
////
////	return NewStateManager(config, storageProvider, blobProvider), nil
////}

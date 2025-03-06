package state

import (
	"context"
	"fmt"
	daprc "github.com/dapr/go-sdk/client"
	"os"
	"path/filepath"
)

func checkComponentType(ctx context.Context, client daprc.Client, componentName string) (string, error) {

	// Get metadata for the specified component
	metadata, err := client.GetMetadata(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get metadata: %w", err)
	}

	// Look for the component in the registered bindings
	for _, binding := range metadata.RegisteredComponents {
		if binding.Name == componentName {
			// Return the binding type
			return binding.Type, nil
		}
	}

	// If not found in bindings, check other component types as needed
	// For example, check state stores
	for _, stateStore := range metadata.RegisteredComponents {
		if stateStore.Name == componentName {
			return stateStore.Type, nil
		}
	}

	return "", fmt.Errorf("component %s not found", componentName)
}

func fetchFileNamingComponent(client daprc.Client, componentName string) (string, error) {
	ctx := context.Background()
	componentType, err := checkComponentType(ctx, client, componentName)
	if err != nil {
		return "", err
	}

	if componentType == "bindings.localstorage" {
		return "fileName", nil
	} else if componentType == "bindings.azure.blobstorage" {
		return "blobName", nil
	} else {
		return "unknown", nil
	}
}

func generateStandardStorageLocation(storageroot string, crawlid string, crawlexecutionid string, channelname string, postid string, local bool) (string, error) {
	if local {
		path := filepath.Join(storageroot, crawlid, crawlexecutionid, channelname)
		if err := os.MkdirAll(path, 0755); err != nil {
			return "", fmt.Errorf("failed to create media directory: %w", err)
		}

		return filepath.Join(storageroot, crawlid, crawlexecutionid, channelname, postid), nil
	}
	return storageroot + "/" + crawlid + "/" + crawlexecutionid + "/" + channelname + "/" + postid, nil
}

func generateStandardStorageLocationForChannels(storageroot string, crawlid string, crawlexecutionid string, channelname string, local bool) (string, error) {
	if local {
		path := filepath.Join(storageroot, crawlid, crawlexecutionid, channelname)
		if err := os.MkdirAll(path, 0755); err != nil {
			return "", fmt.Errorf("failed to create media directory: %w", err)
		}

		return filepath.Join(storageroot, crawlid, crawlexecutionid, channelname), nil
	}
	return storageroot + "/" + crawlid + "/" + crawlexecutionid + "/" + channelname, nil
}

package state

import (
	"context"
	"fmt"
	daprc "github.com/dapr/go-sdk/client"
	"github.com/rs/zerolog/log"
	"os"
	"path/filepath"
)

// checkComponentType determines the type of a DAPR component by name
// by querying the component metadata.
func checkComponentType(ctx context.Context, client daprc.Client, componentName string) (string, error) {
	log.Debug().Str("component_name", componentName).Msg("Checking DAPR component type")

	// Get metadata for the specified component
	metadata, err := client.GetMetadata(ctx)
	if err != nil {
		log.Error().
			Err(err).
			Str("component_name", componentName).
			Msg("Failed to get DAPR metadata")
		return "", fmt.Errorf("failed to get metadata: %w", err)
	}

	log.Debug().
		Int("registered_components", len(metadata.RegisteredComponents)).
		Msg("Retrieved DAPR metadata")

	// Look for the component in the registered bindings
	for _, binding := range metadata.RegisteredComponents {
		if binding.Name == componentName {
			// Return the binding type
			log.Debug().
				Str("component_name", componentName).
				Str("component_type", binding.Type).
				Msg("Found component in registered bindings")
			return binding.Type, nil
		}
	}

	// If not found in bindings, check other component types as needed
	// For example, check state stores
	for _, stateStore := range metadata.RegisteredComponents {
		if stateStore.Name == componentName {
			log.Debug().
				Str("component_name", componentName).
				Str("component_type", stateStore.Type).
				Msg("Found component in state stores")
			return stateStore.Type, nil
		}
	}

	log.Warn().
		Str("component_name", componentName).
		Msg("Component not found in any registered DAPR components")
	return "", fmt.Errorf("component %s not found", componentName)
}

// fetchFileNamingComponent determines the appropriate file naming parameter
// to use with different storage components in DAPR.
func fetchFileNamingComponent(client daprc.Client, componentName string) (string, error) {
	log.Debug().Str("component_name", componentName).Msg("Determining file naming parameter for component")
	
	ctx := context.Background()
	componentType, err := checkComponentType(ctx, client, componentName)
	if err != nil {
		log.Error().
			Err(err).
			Str("component_name", componentName).
			Msg("Failed to get component type")
		return "", err
	}

	var paramName string
	if componentType == "bindings.localstorage" {
		paramName = "fileName"
		log.Debug().
			Str("component_type", componentType).
			Str("param_name", paramName).
			Msg("Using fileName parameter for local storage binding")
	} else if componentType == "bindings.azure.blobstorage" {
		paramName = "blobName"
		log.Debug().
			Str("component_type", componentType).
			Str("param_name", paramName).
			Msg("Using blobName parameter for Azure Blob storage binding")
	} else {
		paramName = "unknown"
		log.Warn().
			Str("component_type", componentType).
			Msg("Unknown component type, using generic 'unknown' parameter name")
	}
	
	return paramName, nil
}

// generateStandardSharedStorageLocation creates a standardized path for shared storage
// based on the storage root, crawl ID, folder name, and filename.
func generateStandardSharedStorageLocation(storageroot string, crawlid string, foldername string, filename string, local bool) (string, error) {
	log.Debug().
		Str("storage_root", storageroot).
		Str("crawl_id", crawlid).
		Str("folder_name", foldername).
		Str("filename", filename).
		Bool("local", local).
		Msg("Generating standard shared storage location")

	if local {
		path := filepath.Join(storageroot, crawlid, foldername)
		if err := os.MkdirAll(path, 0755); err != nil {
			log.Error().
				Err(err).
				Str("path", path).
				Msg("Failed to create media directory")
			return "", fmt.Errorf("failed to create media directory: %w", err)
		}

		result := filepath.Join(storageroot, crawlid)
		log.Debug().
			Str("path", result).
			Msg("Created local shared storage location")
		return result, nil
	}
	
	result := storageroot + "/" + crawlid + "/" + foldername + "/" + filename
	log.Debug().
		Str("path", result).
		Msg("Generated remote shared storage location")
	return result, nil
}
// generateStandardStorageLocation creates a standardized path for storing post data
// based on storage root, crawl ID, execution ID, channel name, and post ID.
func generateStandardStorageLocation(storageroot string, crawlid string, crawlexecutionid string, channelname string, postid string, local bool) (string, error) {
	log.Debug().
		Str("storage_root", storageroot).
		Str("crawl_id", crawlid).
		Str("crawl_execution_id", crawlexecutionid).
		Str("channel_name", channelname).
		Str("post_id", postid).
		Bool("local", local).
		Msg("Generating standard post storage location")

	if local {
		path := filepath.Join(storageroot, crawlid, crawlexecutionid, channelname)
		if err := os.MkdirAll(path, 0755); err != nil {
			log.Error().
				Err(err).
				Str("path", path).
				Msg("Failed to create media directory for post")
			return "", fmt.Errorf("failed to create media directory: %w", err)
		}

		result := filepath.Join(storageroot, crawlid, crawlexecutionid, channelname, postid)
		log.Debug().
			Str("path", result).
			Msg("Created local post storage location")
		return result, nil
	}
	
	result := storageroot + "/" + crawlid + "/" + crawlexecutionid + "/" + channelname + "/" + postid
	log.Debug().
		Str("path", result).
		Msg("Generated remote post storage location")
	return result, nil
}

// generateStandardStorageLocationForChannels creates a standardized path for storing channel data
// based on storage root, crawl ID, execution ID, and channel name.
func generateStandardStorageLocationForChannels(storageroot string, crawlid string, crawlexecutionid string, channelname string, local bool) (string, error) {
	log.Debug().
		Str("storage_root", storageroot).
		Str("crawl_id", crawlid).
		Str("crawl_execution_id", crawlexecutionid).
		Str("channel_name", channelname).
		Bool("local", local).
		Msg("Generating standard channel storage location")

	if local {
		path := filepath.Join(storageroot, crawlid, crawlexecutionid, channelname)
		if err := os.MkdirAll(path, 0755); err != nil {
			log.Error().
				Err(err).
				Str("path", path).
				Msg("Failed to create media directory for channel")
			return "", fmt.Errorf("failed to create media directory: %w", err)
		}

		result := filepath.Join(storageroot, crawlid, crawlexecutionid, channelname)
		log.Debug().
			Str("path", result).
			Msg("Created local channel storage location")
		return result, nil
	}
	
	result := storageroot + "/" + crawlid + "/" + crawlexecutionid + "/" + channelname
	log.Debug().
		Str("path", result).
		Msg("Generated remote channel storage location")
	return result, nil
}

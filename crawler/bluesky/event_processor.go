package bluesky

import (
	"fmt"
	"strings"
	"time"

	blueskymodel "github.com/researchaccelerator-hub/telegram-scraper/model/bluesky"
	"github.com/rs/zerolog/log"
)

// processCommitEvent processes a commit event and extracts relevant data.
func processCommitEvent(event *blueskymodel.BlueskyEvent) (interface{}, error) {
	if event.Commit == nil {
		return nil, fmt.Errorf("no commit data in event")
	}

	// Only process create operations
	if event.Commit.Operation != blueskymodel.OperationCreate {
		return nil, fmt.Errorf("not a create operation: %s", event.Commit.Operation)
	}

	// Route based on collection type
	switch event.Commit.Collection {
	case blueskymodel.CollectionPost:
		return processPostEvent(event)
	case blueskymodel.CollectionRepost:
		return processRepostEvent(event)
	case blueskymodel.CollectionLike:
		return processLikeEvent(event)
	default:
		return nil, fmt.Errorf("unsupported collection type: %s", event.Commit.Collection)
	}
}

// processPostEvent processes a post creation event.
func processPostEvent(event *blueskymodel.BlueskyEvent) (*blueskymodel.BlueskyPost, error) {
	record := event.Commit.Record
	if record == nil {
		return nil, fmt.Errorf("no record in commit")
	}

	post := &blueskymodel.BlueskyPost{
		DID:       event.DID,
		RecordKey: event.Commit.RKey,
		CID:       event.Commit.CID,
	}

	// Extract text
	if text, ok := record["text"].(string); ok {
		post.Text = text
	}

	// Extract creation time
	if createdAt, ok := record["createdAt"].(string); ok {
		if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
			post.CreatedAt = t
		} else {
			log.Warn().Err(err).Str("created_at", createdAt).Msg("Failed to parse createdAt")
		}
	}

	// Extract languages
	if langs, ok := record["langs"].([]interface{}); ok {
		post.Languages = make([]string, 0, len(langs))
		for _, lang := range langs {
			if langStr, ok := lang.(string); ok {
				post.Languages = append(post.Languages, langStr)
			}
		}
	}

	// Extract reply reference
	if reply, ok := record["reply"].(map[string]interface{}); ok {
		post.ReplyTo = parseReplyReference(reply)
	}

	// Extract embed
	if embed, ok := record["embed"].(map[string]interface{}); ok {
		post.Embed = parseEmbed(embed)

		// Check if this is a quote post
		if post.Embed != nil && post.Embed.Record != nil {
			post.QuoteOf = post.Embed.Record.Record
		}
	}

	// Extract facets (mentions, links, hashtags)
	if facets, ok := record["facets"].([]interface{}); ok {
		post.Facets = parseFacets(facets)
	}

	return post, nil
}

// processRepostEvent processes a repost (share) event.
func processRepostEvent(event *blueskymodel.BlueskyEvent) (*blueskymodel.BlueskyRepost, error) {
	record := event.Commit.Record
	if record == nil {
		return nil, fmt.Errorf("no record in commit")
	}

	repost := &blueskymodel.BlueskyRepost{
		DID:       event.DID,
		RecordKey: event.Commit.RKey,
		CID:       event.Commit.CID,
	}

	// Extract creation time
	if createdAt, ok := record["createdAt"].(string); ok {
		if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
			repost.CreatedAt = t
		}
	}

	// Extract subject (the post being reposted)
	if subject, ok := record["subject"].(map[string]interface{}); ok {
		repost.Subject = parseStrongRef(subject)
	}

	return repost, nil
}

// processLikeEvent processes a like event.
func processLikeEvent(event *blueskymodel.BlueskyEvent) (*blueskymodel.BlueskyLike, error) {
	record := event.Commit.Record
	if record == nil {
		return nil, fmt.Errorf("no record in commit")
	}

	like := &blueskymodel.BlueskyLike{
		DID:       event.DID,
		RecordKey: event.Commit.RKey,
		CID:       event.Commit.CID,
	}

	// Extract creation time
	if createdAt, ok := record["createdAt"].(string); ok {
		if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
			like.CreatedAt = t
		}
	}

	// Extract subject (the post being liked)
	if subject, ok := record["subject"].(map[string]interface{}); ok {
		like.Subject = parseStrongRef(subject)
	}

	return like, nil
}

// parseReplyReference extracts reply reference from record data.
func parseReplyReference(reply map[string]interface{}) *blueskymodel.ReplyReference {
	ref := &blueskymodel.ReplyReference{}

	if root, ok := reply["root"].(map[string]interface{}); ok {
		ref.Root = parseStrongRef(root)
	}

	if parent, ok := reply["parent"].(map[string]interface{}); ok {
		ref.Parent = parseStrongRef(parent)
	}

	return ref
}

// parseStrongRef extracts a strong reference from record data.
func parseStrongRef(data map[string]interface{}) *blueskymodel.StrongRef {
	ref := &blueskymodel.StrongRef{}

	if uri, ok := data["uri"].(string); ok {
		ref.URI = uri
	}

	if cid, ok := data["cid"].(string); ok {
		ref.CID = cid
	}

	return ref
}

// parseEmbed extracts embed information from record data.
func parseEmbed(embedData map[string]interface{}) *blueskymodel.BlueskyEmbed {
	embed := &blueskymodel.BlueskyEmbed{
		RawData: embedData,
	}

	// Extract type
	if embedType, ok := embedData["$type"].(string); ok {
		embed.Type = embedType
	}

	switch embed.Type {
	case blueskymodel.EmbedTypeImages:
		if images, ok := embedData["images"].([]interface{}); ok {
			embed.Images = parseEmbedImages(images)
		}

	case blueskymodel.EmbedTypeExternal:
		if external, ok := embedData["external"].(map[string]interface{}); ok {
			embed.External = parseEmbedExternal(external)
		}

	case blueskymodel.EmbedTypeRecord:
		if record, ok := embedData["record"].(map[string]interface{}); ok {
			embed.Record = &blueskymodel.EmbedRecord{
				Record: parseStrongRef(record),
			}
		}

	case blueskymodel.EmbedTypeVideo:
		if video, ok := embedData["video"].(map[string]interface{}); ok {
			embed.Video = parseEmbedVideo(video)
		}
	}

	return embed
}

// parseEmbedImages extracts image embeds.
func parseEmbedImages(images []interface{}) []blueskymodel.EmbedImage {
	result := make([]blueskymodel.EmbedImage, 0, len(images))

	for _, img := range images {
		if imgMap, ok := img.(map[string]interface{}); ok {
			embedImg := blueskymodel.EmbedImage{}

			if alt, ok := imgMap["alt"].(string); ok {
				embedImg.Alt = alt
			}

			if image, ok := imgMap["image"].(map[string]interface{}); ok {
				embedImg.Image = parseBlobRef(image)
			}

			result = append(result, embedImg)
		}
	}

	return result
}

// parseEmbedExternal extracts external link embed.
func parseEmbedExternal(external map[string]interface{}) *blueskymodel.EmbedExternal {
	ext := &blueskymodel.EmbedExternal{}

	if uri, ok := external["uri"].(string); ok {
		ext.URI = uri
	}

	if title, ok := external["title"].(string); ok {
		ext.Title = title
	}

	if description, ok := external["description"].(string); ok {
		ext.Description = description
	}

	if thumb, ok := external["thumb"].(map[string]interface{}); ok {
		ext.Thumb = parseBlobRef(thumb)
	}

	return ext
}

// parseEmbedVideo extracts video embed.
func parseEmbedVideo(video map[string]interface{}) *blueskymodel.EmbedVideo {
	vid := &blueskymodel.EmbedVideo{}

	if alt, ok := video["alt"].(string); ok {
		vid.Alt = alt
	}

	if videoBlob, ok := video["video"].(map[string]interface{}); ok {
		vid.Video = parseBlobRef(videoBlob)
	}

	if captions, ok := video["captions"].([]interface{}); ok {
		vid.Captions = make([]string, 0, len(captions))
		for _, cap := range captions {
			if capStr, ok := cap.(string); ok {
				vid.Captions = append(vid.Captions, capStr)
			}
		}
	}

	return vid
}

// parseBlobRef extracts blob reference data.
func parseBlobRef(blob map[string]interface{}) *blueskymodel.BlobRef {
	blobRef := &blueskymodel.BlobRef{}

	if blobType, ok := blob["$type"].(string); ok {
		blobRef.Type = blobType
	}

	if mimeType, ok := blob["mimeType"].(string); ok {
		blobRef.MimeType = mimeType
	}

	if size, ok := blob["size"].(float64); ok {
		blobRef.Size = int(size)
	} else if size, ok := blob["size"].(int); ok {
		blobRef.Size = size
	}

	if ref, ok := blob["ref"].(map[string]interface{}); ok {
		if link, ok := ref["$link"].(string); ok {
			blobRef.Ref = &blueskymodel.BlobReference{
				Link: link,
			}
		}
	}

	return blobRef
}

// parseFacets extracts facets (mentions, links, hashtags) from record data.
func parseFacets(facetsData []interface{}) []blueskymodel.BlueskyFacet {
	facets := make([]blueskymodel.BlueskyFacet, 0, len(facetsData))

	for _, facetData := range facetsData {
		if facetMap, ok := facetData.(map[string]interface{}); ok {
			facet := blueskymodel.BlueskyFacet{
				RawData: facetMap,
			}

			// Extract index
			if index, ok := facetMap["index"].(map[string]interface{}); ok {
				facet.Index = parseFacetIndex(index)
			}

			// Extract features
			if features, ok := facetMap["features"].([]interface{}); ok {
				facet.Features = parseFacetFeatures(features)
			}

			facets = append(facets, facet)
		}
	}

	return facets
}

// parseFacetIndex extracts facet index (character range).
func parseFacetIndex(index map[string]interface{}) *blueskymodel.FacetIndex {
	idx := &blueskymodel.FacetIndex{}

	if byteStart, ok := index["byteStart"].(float64); ok {
		idx.ByteStart = int(byteStart)
	} else if byteStart, ok := index["byteStart"].(int); ok {
		idx.ByteStart = byteStart
	}

	if byteEnd, ok := index["byteEnd"].(float64); ok {
		idx.ByteEnd = int(byteEnd)
	} else if byteEnd, ok := index["byteEnd"].(int); ok {
		idx.ByteEnd = byteEnd
	}

	return idx
}

// parseFacetFeatures extracts facet features.
func parseFacetFeatures(featuresData []interface{}) []blueskymodel.FacetFeature {
	features := make([]blueskymodel.FacetFeature, 0, len(featuresData))

	for _, featureData := range featuresData {
		if featureMap, ok := featureData.(map[string]interface{}); ok {
			feature := blueskymodel.FacetFeature{
				RawData: featureMap,
			}

			if featureType, ok := featureMap["$type"].(string); ok {
				feature.Type = featureType
			}

			// Extract based on feature type
			switch feature.Type {
			case blueskymodel.FacetMention:
				if did, ok := featureMap["did"].(string); ok {
					feature.DID = did
				}
			case blueskymodel.FacetLink:
				if uri, ok := featureMap["uri"].(string); ok {
					feature.URI = uri
				}
			case blueskymodel.FacetTag:
				if tag, ok := featureMap["tag"].(string); ok {
					feature.Tag = tag
				}
			}

			features = append(features, feature)
		}
	}

	return features
}

// extractOutlinks extracts outlinks (mentions and URLs) from a Bluesky post.
func extractOutlinks(post *blueskymodel.BlueskyPost) []string {
	outlinks := make([]string, 0)
	seen := make(map[string]bool)

	for _, facet := range post.Facets {
		for _, feature := range facet.Features {
			var link string

			switch feature.Type {
			case blueskymodel.FacetMention:
				// Convert DID to handle if possible, or use DID
				if feature.DID != "" {
					link = feature.DID
				}
			case blueskymodel.FacetLink:
				if feature.URI != "" {
					link = feature.URI
				}
			}

			// Add if not already seen
			if link != "" && !seen[link] {
				outlinks = append(outlinks, link)
				seen[link] = true
			}
		}
	}

	return outlinks
}

// extractPostURIfromStrongRef extracts the post ID from an AT URI.
// AT URIs look like: at://did:plc:xyz/app.bsky.feed.post/abc123
func extractPostIDFromURI(uri string) string {
	parts := strings.Split(uri, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return ""
}

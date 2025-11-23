package client

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	innertubego "github.com/nezbut/innertube-go"
	youtubemodel "github.com/researchaccelerator-hub/telegram-scraper/model/youtube"
	"github.com/rs/zerolog/log"
)

// YouTubeInnerTubeClient implements the YouTubeClient interface using the InnerTube API
// This provides an alternative to the YouTube Data API that doesn't require API keys
// or have quota limitations, but requires more data parsing.
type YouTubeInnerTubeClient struct {
	client *innertubego.InnerTube

	// Caching system similar to YouTubeDataClient
	channelCache         map[string]*youtubemodel.YouTubeChannel
	uploadsPlaylistCache map[string]string
	videoStatsCache      map[string]*youtubemodel.YouTubeVideo
	cacheMutex           sync.RWMutex

	// Configuration
	clientType    string // "WEB", "ANDROID", "IOS", etc.
	clientVersion string
}

// InnerTubeConfig contains configuration for the InnerTube client
type InnerTubeConfig struct {
	ClientType    string // Default: "WEB"
	ClientVersion string // Default: "2.20230728.00.00"
}

// NewYouTubeInnerTubeClient creates a new YouTube client using the InnerTube API
func NewYouTubeInnerTubeClient(config *InnerTubeConfig) (*YouTubeInnerTubeClient, error) {
	// Set defaults
	if config == nil {
		config = &InnerTubeConfig{
			ClientType:    "WEB",
			ClientVersion: "2.20230728.00.00",
		}
	}

	if config.ClientType == "" {
		config.ClientType = "WEB"
	}

	if config.ClientVersion == "" {
		config.ClientVersion = "2.20230728.00.00"
	}

	log.Info().
		Str("client_type", config.ClientType).
		Str("client_version", config.ClientVersion).
		Msg("Creating YouTube InnerTube client")

	return &YouTubeInnerTubeClient{
		clientType:           config.ClientType,
		clientVersion:        config.ClientVersion,
		channelCache:         make(map[string]*youtubemodel.YouTubeChannel),
		uploadsPlaylistCache: make(map[string]string),
		videoStatsCache:      make(map[string]*youtubemodel.YouTubeVideo),
	}, nil
}

// Connect establishes a connection to the InnerTube API
func (c *YouTubeInnerTubeClient) Connect(ctx context.Context) error {
	log.Info().Msg("Connecting to YouTube InnerTube API")

	// Create InnerTube client
	// Parameters: config, clientType, clientVersion, apiKey, accessToken, refreshToken, httpClient, debug
	client, err := innertubego.NewInnerTube(
		nil,           // config (will use defaults)
		c.clientType,  // clientType
		c.clientVersion, // clientVersion
		"",            // apiKey (not needed for unauthenticated access)
		"",            // accessToken (not implemented yet)
		"",            // refreshToken (not implemented yet)
		nil,           // httpClient (will use default)
		true,          // debug mode
	)

	if err != nil {
		log.Error().Err(err).Msg("Failed to create InnerTube client")
		return fmt.Errorf("failed to create InnerTube client: %w", err)
	}

	c.client = client
	log.Info().Msg("Successfully connected to YouTube InnerTube API")
	return nil
}

// Disconnect closes the connection to the InnerTube API
func (c *YouTubeInnerTubeClient) Disconnect(ctx context.Context) error {
	log.Info().Msg("Disconnecting from YouTube InnerTube API")
	c.client = nil
	return nil
}

// GetChannelInfo retrieves information about a YouTube channel using InnerTube
func (c *YouTubeInnerTubeClient) GetChannelInfo(ctx context.Context, channelID string) (*youtubemodel.YouTubeChannel, error) {
	if c.client == nil {
		return nil, fmt.Errorf("InnerTube client not connected")
	}

	// Check cache first
	c.cacheMutex.RLock()
	cachedChannel, exists := c.channelCache[channelID]
	c.cacheMutex.RUnlock()

	if exists {
		log.Debug().
			Str("channel_id", channelID).
			Str("title", cachedChannel.Title).
			Msg("Using cached channel info")
		return cachedChannel, nil
	}

	log.Info().Str("channel_id", channelID).Msg("Fetching YouTube channel info via InnerTube")

	// Use the browse endpoint to get channel information
	// InnerTube API requires a browse ID which is typically the channel ID
	browseID := channelID

	// Call the browse endpoint
	// Parameters: context, browseID, params, continuation
	data, err := c.client.Browse(ctx, &browseID, nil, nil)
	if err != nil {
		log.Error().Err(err).Str("channel_id", channelID).Msg("Failed to browse channel")
		return nil, fmt.Errorf("failed to browse channel: %w", err)
	}

	// Parse the response to extract channel information
	// Note: InnerTube responses are complex nested structures that need careful parsing
	channel, err := c.parseChannelFromBrowse(data, channelID)
	if err != nil {
		log.Error().Err(err).Str("channel_id", channelID).Msg("Failed to parse channel data")
		return nil, fmt.Errorf("failed to parse channel data: %w", err)
	}

	// Cache the result
	c.cacheMutex.Lock()
	c.channelCache[channelID] = channel

	// Cache under both input ID and actual ID if different
	if channel.ID != channelID {
		c.channelCache[channel.ID] = channel
		log.Debug().
			Str("input_channel_id", channelID).
			Str("actual_channel_id", channel.ID).
			Msg("Cached channel under both IDs")
	}
	c.cacheMutex.Unlock()

	log.Info().
		Str("channel_id", channel.ID).
		Str("title", channel.Title).
		Int64("subscribers", channel.SubscriberCount).
		Int64("video_count", channel.VideoCount).
		Msg("YouTube channel info retrieved via InnerTube")

	return channel, nil
}

// GetVideos retrieves videos from a YouTube channel using InnerTube
func (c *YouTubeInnerTubeClient) GetVideos(ctx context.Context, channelID string, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	return c.GetVideosFromChannel(ctx, channelID, fromTime, toTime, limit)
}

// GetVideosFromChannel retrieves videos from a specific YouTube channel using InnerTube
func (c *YouTubeInnerTubeClient) GetVideosFromChannel(ctx context.Context, channelID string, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	if c.client == nil {
		return nil, fmt.Errorf("InnerTube client not connected")
	}

	log.Info().
		Str("channel_id", channelID).
		Time("from_time", fromTime).
		Time("to_time", toTime).
		Int("limit", limit).
		Msg("Fetching videos from YouTube channel via InnerTube")

	// Use current time as default if toTime is zero
	effectiveToTime := toTime
	if effectiveToTime.IsZero() {
		effectiveToTime = time.Now()
	}

	// Handle negative or zero limit as "unlimited"
	effectiveLimit := limit
	if effectiveLimit <= 0 {
		effectiveLimit = 1000000
	}

	// Browse the channel to get videos
	browseID := channelID
	// Parameters: context, browseID, params, continuation
	data, err := c.client.Browse(ctx, &browseID, nil, nil)
	if err != nil {
		log.Error().Err(err).Str("channel_id", channelID).Msg("Failed to browse channel for videos")
		return nil, fmt.Errorf("failed to browse channel: %w", err)
	}

	// Parse videos from browse response
	videos, err := c.parseVideosFromBrowse(data, channelID, fromTime, effectiveToTime, effectiveLimit)
	if err != nil {
		log.Error().Err(err).Str("channel_id", channelID).Msg("Failed to parse videos from browse data")
		return nil, fmt.Errorf("failed to parse videos: %w", err)
	}

	log.Info().
		Str("channel_id", channelID).
		Int("video_count", len(videos)).
		Msg("Retrieved videos from YouTube channel via InnerTube")

	return videos, nil
}

// parseChannelFromBrowse extracts channel information from InnerTube browse response
func (c *YouTubeInnerTubeClient) parseChannelFromBrowse(data interface{}, channelID string) (*youtubemodel.YouTubeChannel, error) {
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid data type for channel browse response")
	}

	channel := &youtubemodel.YouTubeChannel{
		ID:          channelID,
		Thumbnails:  make(map[string]string),
		PublishedAt: time.Now(), // Default to now, may be updated if we find creation date
	}

	var headerParsed bool

	// Try extracting from header (multiple formats due to YouTube A/B testing)
	if header, ok := dataMap["header"].(map[string]interface{}); ok {
		// Try legacy c4TabbedHeaderRenderer format
		if c4Header, ok := header["c4TabbedHeaderRenderer"].(map[string]interface{}); ok {
			headerParsed = true
			log.Debug().Msg("Using c4TabbedHeaderRenderer format")

			// Extract title
			if title, ok := c4Header["title"].(string); ok {
				channel.Title = title
			}

			// Extract channel ID (may differ from input if we got a handle)
			if cID, ok := c4Header["channelId"].(string); ok {
				channel.ID = cID
			}

			// Extract subscriber count
			if subCountObj, ok := c4Header["subscriberCountText"].(map[string]interface{}); ok {
				channel.SubscriberCount = parseCount(subCountObj)
			}

			// Extract video count
			if videoCountObj, ok := c4Header["videosCountText"].(map[string]interface{}); ok {
				channel.VideoCount = parseCount(videoCountObj)
			}

			// Extract view count (if available)
			if viewCountObj, ok := c4Header["viewCountText"].(map[string]interface{}); ok {
				channel.ViewCount = parseCount(viewCountObj)
			}

			// Extract avatar/thumbnails
			if avatar, ok := c4Header["avatar"].(map[string]interface{}); ok {
				if thumbs, ok := avatar["thumbnails"].([]interface{}); ok {
					for _, thumb := range thumbs {
						if t, ok := thumb.(map[string]interface{}); ok {
							if url, ok := t["url"].(string); ok {
								// Categorize by size
								if width, ok := t["width"].(float64); ok {
									if width >= 800 {
										channel.Thumbnails["high"] = url
									} else if width >= 240 {
										channel.Thumbnails["medium"] = url
									} else {
										channel.Thumbnails["default"] = url
									}
								}
							}
						}
					}
				}
			}

			// Extract banner (optional)
			if banner, ok := c4Header["banner"].(map[string]interface{}); ok {
				if thumbs, ok := banner["thumbnails"].([]interface{}); ok {
					for _, thumb := range thumbs {
						if t, ok := thumb.(map[string]interface{}); ok {
							if url, ok := t["url"].(string); ok {
								channel.Thumbnails["banner"] = url
								break // Just take first banner
							}
						}
					}
				}
			}
		}

		// Try new pageHeaderViewModel format (YouTube's newer A/B test)
		if pageHeader, ok := header["pageHeaderViewModel"].(map[string]interface{}); ok {
			if !headerParsed {
				log.Debug().Msg("Using pageHeaderViewModel format")
			}
			headerParsed = true

			// Extract title from nested structure
			if titleObj, ok := pageHeader["title"].(map[string]interface{}); ok {
				if dynText, ok := titleObj["dynamicTextViewModel"].(map[string]interface{}); ok {
					if textObj, ok := dynText["text"].(map[string]interface{}); ok {
						if content, ok := textObj["content"].(string); ok {
							channel.Title = content
						}
					}
				}
			}

			// Extract metadata (subscriber count, video count, etc.)
			if metadataObj, ok := pageHeader["metadata"].(map[string]interface{}); ok {
				if contentMeta, ok := metadataObj["contentMetadataViewModel"].(map[string]interface{}); ok {
					if rows, ok := contentMeta["metadataRows"].([]interface{}); ok {
						for _, row := range rows {
							if rowMap, ok := row.(map[string]interface{}); ok {
								if parts, ok := rowMap["metadataParts"].([]interface{}); ok {
									for _, part := range parts {
										if partMap, ok := part.(map[string]interface{}); ok {
											if text, ok := partMap["text"].(map[string]interface{}); ok {
												if content, ok := text["content"].(string); ok {
													// Try to identify what this metadata is
													lowerContent := strings.ToLower(content)
													if strings.Contains(lowerContent, "subscriber") {
														channel.SubscriberCount = parseCountFromText(content)
													} else if strings.Contains(lowerContent, "video") {
														channel.VideoCount = parseCountFromText(content)
													} else if strings.Contains(lowerContent, "view") {
														channel.ViewCount = parseCountFromText(content)
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}

			// Extract banner image
			if bannerObj, ok := pageHeader["banner"].(map[string]interface{}); ok {
				if imageBanner, ok := bannerObj["imageBannerViewModel"].(map[string]interface{}); ok {
					if image, ok := imageBanner["image"].(map[string]interface{}); ok {
						if sources, ok := image["sources"].([]interface{}); ok {
							for _, source := range sources {
								if s, ok := source.(map[string]interface{}); ok {
									if url, ok := s["url"].(string); ok {
										channel.Thumbnails["banner"] = url
										break
									}
								}
							}
						}
					}
				}
			}

			// Extract avatar from image
			if imageObj, ok := pageHeader["image"].(map[string]interface{}); ok {
				if decoratedAvatar, ok := imageObj["decoratedAvatarViewModel"].(map[string]interface{}); ok {
					if avatar, ok := decoratedAvatar["avatar"].(map[string]interface{}); ok {
						if avatarImg, ok := avatar["avatarViewModel"].(map[string]interface{}); ok {
							if img, ok := avatarImg["image"].(map[string]interface{}); ok {
								if sources, ok := img["sources"].([]interface{}); ok {
									for _, source := range sources {
										if s, ok := source.(map[string]interface{}); ok {
											if url, ok := s["url"].(string); ok {
												if width, ok := s["width"].(float64); ok {
													if width >= 800 {
														channel.Thumbnails["high"] = url
													} else if width >= 240 {
														channel.Thumbnails["medium"] = url
													} else {
														channel.Thumbnails["default"] = url
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}

	// Try extracting from metadata section
	if metadata, ok := dataMap["metadata"].(map[string]interface{}); ok {
		if channelMeta, ok := metadata["channelMetadataRenderer"].(map[string]interface{}); ok {
			// Extract description
			if description, ok := channelMeta["description"].(string); ok {
				channel.Description = description
			}

			// Extract channel ID if we don't have it yet
			if externalID, ok := channelMeta["externalId"].(string); ok {
				if channel.ID == "" || channel.ID == channelID {
					channel.ID = externalID
				}
			}

			// Extract country
			if country, ok := channelMeta["country"].(string); ok {
				channel.Country = country
			}

			// Extract title as fallback
			if channel.Title == "" {
				if title, ok := channelMeta["title"].(string); ok {
					channel.Title = title
				}
			}
		}
	}

	if !headerParsed {
		log.Warn().
			Str("channel_id", channelID).
			Msg("Could not find recognized header format in browse response")
	}

	log.Debug().
		Str("channel_id", channel.ID).
		Str("title", channel.Title).
		Int64("subscribers", channel.SubscriberCount).
		Int64("videos", channel.VideoCount).
		Msg("Parsed channel from InnerTube")

	return channel, nil
}

// parseVideosFromBrowse extracts video information from InnerTube browse response
// Returns the parsed videos and a continuation token for pagination (if available)
func (c *YouTubeInnerTubeClient) parseVideosFromBrowse(data interface{}, channelID string, fromTime, toTime time.Time, limit int) ([]*youtubemodel.YouTubeVideo, error) {
	dataMap, ok := data.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid data type for video browse response")
	}

	videos := make([]*youtubemodel.YouTubeVideo, 0)

	// Navigate to tabs in the response
	contents, ok := dataMap["contents"].(map[string]interface{})
	if !ok {
		log.Warn().Msg("No contents found in browse response")
		return videos, nil
	}

	twoCol, ok := contents["twoColumnBrowseResultsRenderer"].(map[string]interface{})
	if !ok {
		log.Warn().Msg("No twoColumnBrowseResultsRenderer found")
		return videos, nil
	}

	tabs, ok := twoCol["tabs"].([]interface{})
	if !ok {
		log.Warn().Msg("No tabs found in response")
		return videos, nil
	}

	// Find the videos tab (usually index 1, but we'll check all)
	for tabIdx, tab := range tabs {
		tabMap, ok := tab.(map[string]interface{})
		if !ok {
			continue
		}

		tabRenderer, ok := tabMap["tabRenderer"].(map[string]interface{})
		if !ok {
			continue
		}

		content, ok := tabRenderer["content"].(map[string]interface{})
		if !ok {
			continue
		}

		// Try richGridRenderer (new layout)
		if richGrid, ok := content["richGridRenderer"].(map[string]interface{}); ok {
			log.Debug().Int("tab_index", tabIdx).Msg("Using richGridRenderer layout")

			if items, ok := richGrid["contents"].([]interface{}); ok {
				for _, item := range items {
					itemMap, ok := item.(map[string]interface{})
					if !ok {
						continue
					}

					// Check for richItemRenderer (contains video)
					if richItem, ok := itemMap["richItemRenderer"].(map[string]interface{}); ok {
						if contentObj, ok := richItem["content"].(map[string]interface{}); ok {
							// Try videoRenderer
							if videoRenderer, ok := contentObj["videoRenderer"].(map[string]interface{}); ok {
								video := parseVideoRenderer(videoRenderer, channelID)
								if video != nil {
									// Apply time filtering
									if !fromTime.IsZero() && video.PublishedAt.Before(fromTime) {
										continue
									}
									if !toTime.IsZero() && video.PublishedAt.After(toTime) {
										continue
									}
									videos = append(videos, video)

									// Check limit
									if limit > 0 && len(videos) >= limit {
										return videos, nil
									}
								}
							}
						}
					}
				}
			}
		}

		// Try sectionListRenderer (older layout)
		if sectionList, ok := content["sectionListRenderer"].(map[string]interface{}); ok {
			log.Debug().Int("tab_index", tabIdx).Msg("Using sectionListRenderer layout")

			if sectionContents, ok := sectionList["contents"].([]interface{}); ok {
				for _, sectionItem := range sectionContents {
					sectionMap, ok := sectionItem.(map[string]interface{})
					if !ok {
						continue
					}

					// Check for itemSectionRenderer
					if itemSection, ok := sectionMap["itemSectionRenderer"].(map[string]interface{}); ok {
						if itemContents, ok := itemSection["contents"].([]interface{}); ok {
							for _, contentItem := range itemContents {
								contentMap, ok := contentItem.(map[string]interface{})
								if !ok {
									continue
								}

								// Check for gridRenderer
								if gridRenderer, ok := contentMap["gridRenderer"].(map[string]interface{}); ok {
									if gridItems, ok := gridRenderer["items"].([]interface{}); ok {
										for _, gridItem := range gridItems {
											gridItemMap, ok := gridItem.(map[string]interface{})
											if !ok {
												continue
											}

											// Try gridVideoRenderer
											if gridVideoRenderer, ok := gridItemMap["gridVideoRenderer"].(map[string]interface{}); ok {
												video := parseVideoRenderer(gridVideoRenderer, channelID)
												if video != nil {
													// Apply time filtering
													if !fromTime.IsZero() && video.PublishedAt.Before(fromTime) {
														continue
													}
													if !toTime.IsZero() && video.PublishedAt.After(toTime) {
														continue
													}
													videos = append(videos, video)

													// Check limit
													if limit > 0 && len(videos) >= limit {
														return videos, nil
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}

		// If we found videos in this tab, we're done
		if len(videos) > 0 {
			break
		}
	}

	log.Debug().
		Str("channel_id", channelID).
		Int("videos_parsed", len(videos)).
		Msg("Parsed videos from InnerTube")

	return videos, nil
}

// getCachedVideoStats checks if video statistics are already cached
func (c *YouTubeInnerTubeClient) getCachedVideoStats(videoID string) (*youtubemodel.YouTubeVideo, bool) {
	c.cacheMutex.RLock()
	defer c.cacheMutex.RUnlock()
	video, exists := c.videoStatsCache[videoID]
	return video, exists
}

// cacheVideoStats stores video statistics in cache
func (c *YouTubeInnerTubeClient) cacheVideoStats(video *youtubemodel.YouTubeVideo) {
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()
	c.videoStatsCache[video.ID] = video
}

// Helper functions for parsing InnerTube responses

// parseCountFromText converts formatted text like "43.2M subscribers" or "1,234 videos" to int64
func parseCountFromText(text string) int64 {
	// Remove leading/trailing whitespace
	text = strings.TrimSpace(text)
	text = strings.ReplaceAll(text, ",", "")

	// Determine multiplier based on suffix
	var multiplier int64 = 1
	upperText := strings.ToUpper(text)
	if strings.Contains(upperText, "K") {
		multiplier = 1000
		text = strings.ReplaceAll(strings.ReplaceAll(text, "K", ""), "k", "")
	} else if strings.Contains(upperText, "M") {
		multiplier = 1000000
		text = strings.ReplaceAll(strings.ReplaceAll(text, "M", ""), "m", "")
	} else if strings.Contains(upperText, "B") {
		multiplier = 1000000000
		text = strings.ReplaceAll(strings.ReplaceAll(text, "B", ""), "b", "")
	}

	// Extract numeric part using regex
	re := regexp.MustCompile(`[\d.]+`)
	numStr := re.FindString(text)
	if numStr == "" {
		return 0
	}

	// Parse the number
	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0
	}

	return int64(num * float64(multiplier))
}

// parseCount extracts count from InnerTube text object (simpleText or runs format)
func parseCount(countObj map[string]interface{}) int64 {
	// Try simpleText format
	if simpleText, ok := countObj["simpleText"].(string); ok {
		return parseCountFromText(simpleText)
	}

	// Try runs array format
	if runs, ok := countObj["runs"].([]interface{}); ok {
		var fullText string
		for _, run := range runs {
			if runMap, ok := run.(map[string]interface{}); ok {
				if text, ok := runMap["text"].(string); ok {
					fullText += text
				}
			}
		}
		return parseCountFromText(fullText)
	}

	return 0
}

// extractText extracts text from InnerTube text object (simpleText or runs format)
func extractText(textObj interface{}) string {
	if textObj == nil {
		return ""
	}

	// Handle direct string
	if str, ok := textObj.(string); ok {
		return str
	}

	// Handle map with simpleText
	if textMap, ok := textObj.(map[string]interface{}); ok {
		if simpleText, ok := textMap["simpleText"].(string); ok {
			return simpleText
		}

		// Handle runs array
		if runs, ok := textMap["runs"].([]interface{}); ok {
			var parts []string
			for _, run := range runs {
				if runMap, ok := run.(map[string]interface{}); ok {
					if text, ok := runMap["text"].(string); ok {
						parts = append(parts, text)
					}
				}
			}
			return strings.Join(parts, "")
		}
	}

	return ""
}

// parseRelativeTime converts relative time text like "5 days ago" to absolute timestamp
func parseRelativeTime(relativeTime string) time.Time {
	now := time.Now()

	// Parse patterns like "X days ago", "X weeks ago", etc.
	re := regexp.MustCompile(`(\d+)\s+(second|minute|hour|day|week|month|year)s?\s+ago`)
	matches := re.FindStringSubmatch(relativeTime)

	if len(matches) < 3 {
		// If we can't parse, return current time
		return now
	}

	value, err := strconv.Atoi(matches[1])
	if err != nil {
		return now
	}

	unit := matches[2]

	switch unit {
	case "second":
		return now.Add(-time.Duration(value) * time.Second)
	case "minute":
		return now.Add(-time.Duration(value) * time.Minute)
	case "hour":
		return now.Add(-time.Duration(value) * time.Hour)
	case "day":
		return now.AddDate(0, 0, -value)
	case "week":
		return now.AddDate(0, 0, -value*7)
	case "month":
		return now.AddDate(0, -value, 0)
	case "year":
		return now.AddDate(-value, 0, 0)
	}

	return now
}

// parseVideoRenderer extracts video data from a videoRenderer or gridVideoRenderer object
func parseVideoRenderer(renderer map[string]interface{}, channelID string) *youtubemodel.YouTubeVideo {
	video := &youtubemodel.YouTubeVideo{
		ChannelID:  channelID,
		Thumbnails: make(map[string]string),
	}

	// Extract video ID (required)
	if videoID, ok := renderer["videoId"].(string); ok {
		video.ID = videoID
	} else {
		// No video ID, can't process this video
		return nil
	}

	// Extract title
	if titleObj, ok := renderer["title"]; ok {
		video.Title = extractText(titleObj)
	}

	// Extract description (if available - not always present in browse responses)
	if descObj, ok := renderer["description"]; ok {
		video.Description = extractText(descObj)
	} else if descSnippet, ok := renderer["descriptionSnippet"]; ok {
		video.Description = extractText(descSnippet)
	}

	// Extract thumbnails
	if thumbObj, ok := renderer["thumbnail"].(map[string]interface{}); ok {
		if thumbs, ok := thumbObj["thumbnails"].([]interface{}); ok {
			for _, thumb := range thumbs {
				if t, ok := thumb.(map[string]interface{}); ok {
					if url, ok := t["url"].(string); ok {
						// Categorize by width
						if width, ok := t["width"].(float64); ok {
							if width >= 640 {
								video.Thumbnails["high"] = url
							} else if width >= 320 {
								video.Thumbnails["medium"] = url
							} else {
								video.Thumbnails["default"] = url
							}
						} else {
							// No width info, use as default
							if video.Thumbnails["default"] == "" {
								video.Thumbnails["default"] = url
							}
						}
					}
				}
			}
		}
	}

	// Extract published time (relative format like "5 days ago")
	if publishedObj, ok := renderer["publishedTimeText"]; ok {
		relativeTime := extractText(publishedObj)
		if relativeTime != "" {
			video.PublishedAt = parseRelativeTime(relativeTime)
		}
	}

	// Extract duration/length
	if lengthObj, ok := renderer["lengthText"]; ok {
		video.Duration = extractText(lengthObj)
	}

	// Extract view count
	if viewCountObj, ok := renderer["viewCountText"]; ok {
		viewText := extractText(viewCountObj)
		if viewText != "" {
			video.ViewCount = parseCountFromText(viewText)
		}
	}

	// Note: Like count and comment count are not available in browse responses
	// They require calling the Player or Next endpoint for each video

	// Check for badges (live, membership, etc.)
	if badges, ok := renderer["badges"].([]interface{}); ok {
		for _, badge := range badges {
			if badgeMap, ok := badge.(map[string]interface{}); ok {
				if metaBadge, ok := badgeMap["metadataBadgeRenderer"].(map[string]interface{}); ok {
					if style, ok := metaBadge["style"].(string); ok {
						log.Debug().
							Str("video_id", video.ID).
							Str("badge_style", style).
							Msg("Video has badge")
						// Could set flags based on badge style if needed
					}
				}
			}
		}
	}

	return video
}

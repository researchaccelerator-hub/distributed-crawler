package youtube

import (
	"testing"
	
	youtubemodel "github.com/researchaccelerator-hub/telegram-scraper/model/youtube"
)

// TestClientAdapterImplementsInterface tests that ClientAdapter implements the YouTubeClient interface
func TestClientAdapterImplementsInterface(t *testing.T) {
	// Type assertion check - this will fail at compile time if ClientAdapter doesn't implement YouTubeClient
	var _ youtubemodel.YouTubeClient = (*ClientAdapter)(nil)
}
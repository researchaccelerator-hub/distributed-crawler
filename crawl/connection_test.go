package crawl

import (
	"fmt"
	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"testing"
	"time"
)

func TestConnectionCycle(t *testing.T) {
	cfg := common.CrawlerConfig{
		DaprMode:         false,
		DaprPort:         0,
		Concurrency:      0,
		Timeout:          0,
		UserAgent:        "",
		OutputFormat:     "",
		StorageRoot:      "",
		TDLibDatabaseURL: "",
		MinPostDate:      time.Time{},
		PostRecency:      time.Time{},
		DaprJobMode:      false,
		MinUsers:         0,
		CrawlID:          "",
		MaxComments:      0,
		MaxPosts:         0,
		MaxDepth:         0,
	}
	client, err := Connect("/tmp/crawl/", cfg)

	client.GetMe()

	client.Close()

	client, err = Connect("/tmp/crawl/", cfg)

	o, err := client.GetMe()
	if err != nil {
		return
	}

	fmt.Printf("%+v\n", o)

}

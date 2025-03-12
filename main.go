package main

import (
	"fmt"
	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/dapr"
	"github.com/researchaccelerator-hub/telegram-scraper/standalone"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile      string
	crawlerCfg   common.CrawlerConfig
	urlList      []string
	urlFile      string
	generateCode bool
	crawlType    string
	minPostDate  string
	daprMode     string
	minUsers     int
	crawlID      string
	timeAgo      string // Add this line for the new parameter
)

func main() {
	// Initialize and execute the root command
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// parseTimeAgo parses a time duration string and returns a time.Time cutoff point
func parseTimeAgo(timeAgoStr string) (time.Time, error) {
	if timeAgoStr == "" {
		return time.Time{}, nil
	}

	// Get the last character which should be the unit
	unit := timeAgoStr[len(timeAgoStr)-1:]
	// Get the value without the unit
	valueStr := timeAgoStr[:len(timeAgoStr)-1]

	var value int
	if _, err := fmt.Sscanf(valueStr, "%d", &value); err != nil {
		return time.Time{}, fmt.Errorf("invalid time-ago format, must be a number followed by a unit (h,d,w,m,y): %v", err)
	}

	now := time.Now()
	var cutoffTime time.Time

	switch unit {
	case "h": // hours
		cutoffTime = now.Add(time.Duration(-value) * time.Hour)
	case "d": // days
		cutoffTime = now.AddDate(0, 0, -value)
	case "w": // weeks
		cutoffTime = now.AddDate(0, 0, -value*7)
	case "m": // months
		cutoffTime = now.AddDate(0, -value, 0)
	case "y": // years
		cutoffTime = now.AddDate(-value, 0, 0)
	default:
		return time.Time{}, fmt.Errorf("invalid time unit '%s', must be h (hours), d (days), w (weeks), m (months), or y (years)", unit)
	}

	return cutoffTime, nil
}

// Root command setup
var rootCmd = &cobra.Command{
	Use:   "crawler",
	Short: "A flexible web crawler that can run as a DAPR job or standalone",
	Long: `A web crawler application that can run in three modes:
1. As a DAPR job - waiting for job requests
2. As a DAPR standalone - processing URLs directly but using DAPR
3. As a regular standalone application - processing URLs without DAPR`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// Load configuration file if specified
		if cfgFile != "" {
			viper.SetConfigFile(cfgFile)
		} else {
			// Search for config in default locations
			viper.AddConfigPath(".")
			viper.AddConfigPath("$HOME/.crawler")
			viper.AddConfigPath("/etc/crawler")
			viper.SetConfigName("config")
			viper.SetConfigType("yaml")
		}

		// Read environment variables prefixed with CRAWLER_
		viper.SetEnvPrefix("CRAWLER")
		viper.AutomaticEnv()
		viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))

		// Load the configuration
		if err := viper.ReadInConfig(); err != nil {
			// It's okay if there is no config file
			if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
				return err
			}
		}

		// Bind configuration to structure
		crawlerCfg.DaprMode = viper.GetBool("dapr.enabled")
		crawlerCfg.DaprPort = viper.GetInt("dapr.port")
		crawlerCfg.DaprJobMode = viper.GetString("dapr.mode") == "job"
		crawlerCfg.Concurrency = viper.GetInt("crawler.concurrency")
		crawlerCfg.Timeout = viper.GetInt("crawler.timeout")
		crawlerCfg.UserAgent = viper.GetString("crawler.useragent")
		crawlerCfg.OutputFormat = viper.GetString("output.format")
		crawlerCfg.StorageRoot = viper.GetString("storage.root")
		crawlerCfg.TDLibDatabaseURL = viper.GetString("tdlib.database_url")
		crawlerCfg.MinUsers = viper.GetInt("crawler.minusers")
		crawlerCfg.CrawlID = viper.GetString("crawler.crawlid")

		// Parse min post date from string to time.Time if provided
		minPostDateStr := viper.GetString("crawler.minpostdate")
		if minPostDateStr != "" {
			parsedTime, err := time.Parse("2006-01-02", minPostDateStr)
			if err != nil {
				return fmt.Errorf("invalid min-post-date format, must be YYYY-MM-DD: %v", err)
			}
			crawlerCfg.MinPostDate = parsedTime
		} else {
			// Set to zero time if not specified
			crawlerCfg.MinPostDate = time.Time{}
		}

		// Check if time-ago is provided and use it if min-post-date isn't set
		timeAgoStr := viper.GetString("crawler.timeago")
		if timeAgoStr != "" {
			// Only use time-ago if min-post-date isn't explicitly set
			cutoffTime, err := parseTimeAgo(timeAgoStr)
			if err != nil {
				return err
			}
			// Only set if valid
			if !cutoffTime.IsZero() {
				crawlerCfg.PostRecency = cutoffTime
				fmt.Printf("Using cutoff date: %s based on time-ago: %s\n",
					cutoffTime.Format("2006-01-02 15:04:05"),
					timeAgoStr)

			} else {
				fmt.Println("Warning: Both min-post-date and time-ago specified; using min-post-date")
			}
		}

		// Override with command line flags if provided
		if cmd.Flags().Changed("dapr-mode") {
			crawlerCfg.DaprJobMode = daprMode == "job"
		}

		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		// If no specific subcommand is invoked, show help
		if len(args) == 0 && !crawlerCfg.DaprMode && len(urlList) == 0 && urlFile == "" {
			cmd.Help()
			return
		}

		// Start in appropriate mode
		if crawlerCfg.DaprMode {
			if crawlerCfg.DaprJobMode {
				fmt.Println("Starting in DAPR job mode...")
				dapr.StartDaprMode(crawlerCfg)
			} else {
				fmt.Println("Starting in DAPR standalone mode...")
				dapr.StartDaprStandaloneMode(urlList, urlFile, crawlerCfg, generateCode)
			}
		} else {
			fmt.Println("Starting in regular standalone mode...")
			standalone.StartStandaloneMode(urlList, urlFile, crawlerCfg, generateCode)
		}
	},
}

// Initialize cobra command
func init() {
	// Global flags
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./config.yaml)")
	rootCmd.PersistentFlags().BoolVar(&crawlerCfg.DaprMode, "dapr", false, "run with DAPR enabled")
	rootCmd.PersistentFlags().StringVar(&daprMode, "dapr-mode", "job", "DAPR mode to use ('job' or 'standalone')")
	rootCmd.PersistentFlags().IntVar(&crawlerCfg.DaprPort, "dapr-port", 6400, "DAPR port to use")
	rootCmd.PersistentFlags().IntVar(&crawlerCfg.Concurrency, "concurrency", 1, "number of concurrent crawlers")
	rootCmd.PersistentFlags().IntVar(&crawlerCfg.Timeout, "timeout", 30, "HTTP request timeout in seconds")
	rootCmd.PersistentFlags().StringVar(&crawlerCfg.UserAgent, "user-agent", "Mozilla/5.0 Crawler", "User agent to use")
	rootCmd.PersistentFlags().StringVar(&crawlerCfg.OutputFormat, "output", "json", "Output format (json, csv, etc.)")
	rootCmd.PersistentFlags().StringVar(&crawlerCfg.StorageRoot, "storage-root", "/tmp/crawl", "Storage root directory")
	rootCmd.PersistentFlags().StringVar(&minPostDate, "min-post-date", "", "Minimum post date to crawl (format: YYYY-MM-DD)")
	rootCmd.PersistentFlags().StringVar(&timeAgo, "time-ago", "1m", "Only consider posts newer than this time ago (e.g., '30d' for 30 days, '6h' for 6 hours, '2w' for 2 weeks, '1m' for 1 month, '1y' for 1 year)")
	rootCmd.PersistentFlags().StringVar(&crawlerCfg.TDLibDatabaseURL, "tdlib-database-url", "", "URL to a pre-seeded TDLib database archive")
	rootCmd.PersistentFlags().IntVar(&minUsers, "min-users", 100, "Minimum number of users in a channel to crawl")
	rootCmd.PersistentFlags().StringVar(&crawlID, "crawl-id", "", "Unique identifier for this crawl operation")

	// Standalone mode specific flags
	rootCmd.Flags().StringSliceVar(&urlList, "urls", []string{}, "comma-separated list of URLs to crawl")
	rootCmd.Flags().StringVar(&urlFile, "url-file", "", "file containing URLs to crawl (one per line)")
	rootCmd.Flags().BoolVar(&generateCode, "generate-code", false, "run code generation after crawling")
	rootCmd.Flags().StringVar(&crawlType, "crawl-type", "focused", "Select between focused(default) and snowball")

	// Bind flags to viper
	viper.BindPFlag("dapr.enabled", rootCmd.PersistentFlags().Lookup("dapr"))
	viper.BindPFlag("dapr.mode", rootCmd.PersistentFlags().Lookup("dapr-mode"))
	viper.BindPFlag("dapr.port", rootCmd.PersistentFlags().Lookup("dapr-port"))
	viper.BindPFlag("crawler.concurrency", rootCmd.PersistentFlags().Lookup("concurrency"))
	viper.BindPFlag("crawler.timeout", rootCmd.PersistentFlags().Lookup("timeout"))
	viper.BindPFlag("crawler.useragent", rootCmd.PersistentFlags().Lookup("user-agent"))
	viper.BindPFlag("output.format", rootCmd.PersistentFlags().Lookup("output"))
	viper.BindPFlag("storage.root", rootCmd.PersistentFlags().Lookup("storage-root"))
	viper.BindPFlag("crawler.minpostdate", rootCmd.PersistentFlags().Lookup("min-post-date"))
	viper.BindPFlag("crawler.timeago", rootCmd.PersistentFlags().Lookup("time-ago"))
	viper.BindPFlag("tdlib.database_url", rootCmd.PersistentFlags().Lookup("tdlib-database-url"))
	viper.BindPFlag("crawler.minusers", rootCmd.PersistentFlags().Lookup("min-users"))
	viper.BindPFlag("crawler.crawlid", rootCmd.PersistentFlags().Lookup("crawl-id"))

	// Add subcommands
	rootCmd.AddCommand(versionCmd)
}

// Version command
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Crawler v1.0")
	},
}

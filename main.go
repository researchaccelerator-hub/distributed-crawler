package main

import (
	"fmt"
	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/dapr"
	"github.com/researchaccelerator-hub/telegram-scraper/standalone"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile           string
	crawlerCfg        common.CrawlerConfig
	urlList           []string
	urlFile           string
	generateCode      bool
	crawlType         string
	minPostDate       string
	daprMode          string
	minUsers          int
	crawlID           string
	timeAgo           string // Time ago parameter
	tdlibDatabaseURLs []string // Multiple TDLib database URLs
	logLevel          string // Logging level
)

func main() {
	// Set up the logger with a default level
	// The actual level will be configured in PersistentPreRunE
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	
	// Initialize and execute the root command
	if err := rootCmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("Failed to execute root command")
	}
}

// parseTimeAgo parses a time duration string and returns a time.Time cutoff point
// It accepts formats like "30d" (30 days), "6h" (6 hours), "2w" (2 weeks),
// "1m" (1 month), or "1y" (1 year).
func parseTimeAgo(timeAgoStr string) (time.Time, error) {
	log.Debug().Str("time_ago", timeAgoStr).Msg("Parsing time-ago parameter")
	
	if timeAgoStr == "" {
		log.Debug().Msg("Empty time-ago string provided, returning zero time")
		return time.Time{}, nil
	}

	// Get the last character which should be the unit
	unit := timeAgoStr[len(timeAgoStr)-1:]
	// Get the value without the unit
	valueStr := timeAgoStr[:len(timeAgoStr)-1]

	var value int
	if _, err := fmt.Sscanf(valueStr, "%d", &value); err != nil {
		log.Error().
			Err(err).
			Str("time_ago", timeAgoStr).
			Str("value_part", valueStr).
			Msg("Failed to parse numeric part of time-ago")
		return time.Time{}, fmt.Errorf("invalid time-ago format, must be a number followed by a unit (h,d,w,m,y): %v", err)
	}

	now := time.Now()
	var cutoffTime time.Time

	switch unit {
	case "h": // hours
		cutoffTime = now.Add(time.Duration(-value) * time.Hour)
		log.Debug().Int("hours", value).Time("cutoff", cutoffTime).Msg("Computed hour-based cutoff time")
	case "d": // days
		cutoffTime = now.AddDate(0, 0, -value)
		log.Debug().Int("days", value).Time("cutoff", cutoffTime).Msg("Computed day-based cutoff time")
	case "w": // weeks
		cutoffTime = now.AddDate(0, 0, -value*7)
		log.Debug().Int("weeks", value).Time("cutoff", cutoffTime).Msg("Computed week-based cutoff time")
	case "m": // months
		cutoffTime = now.AddDate(0, -value, 0)
		log.Debug().Int("months", value).Time("cutoff", cutoffTime).Msg("Computed month-based cutoff time")
	case "y": // years
		cutoffTime = now.AddDate(-value, 0, 0)
		log.Debug().Int("years", value).Time("cutoff", cutoffTime).Msg("Computed year-based cutoff time")
	default:
		log.Error().
			Str("unit", unit).
			Str("time_ago", timeAgoStr).
			Msg("Invalid time unit provided")
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
		// Configure logging level
		logLevelStr := viper.GetString("logging.level")
		if cmd.Flags().Changed("log-level") {
			logLevelStr = logLevel
		}
		
		// Set log level based on configuration
		level, err := zerolog.ParseLevel(logLevelStr)
		if err != nil {
			// Default to info if invalid level
			level = zerolog.InfoLevel
			log.Warn().Str("provided_level", logLevelStr).Msg("Invalid log level provided, defaulting to info")
		}
		zerolog.SetGlobalLevel(level)
		log.Info().Str("log_level", level.String()).Msg("Logger initialized")
		
		// Load configuration file if specified
		if cfgFile != "" {
			viper.SetConfigFile(cfgFile)
			log.Debug().Str("config_file", cfgFile).Msg("Using specified config file")
		} else {
			// Search for config in default locations
			viper.AddConfigPath(".")
			viper.AddConfigPath("$HOME/.crawler")
			viper.AddConfigPath("/etc/crawler")
			viper.SetConfigName("config")
			viper.SetConfigType("yaml")
			log.Debug().Msg("Searching for config in default locations")
		}

		// Read environment variables prefixed with CRAWLER_
		viper.SetEnvPrefix("CRAWLER")
		viper.AutomaticEnv()
		viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
		log.Debug().Msg("Environment variables with CRAWLER_ prefix will override config")

		// Load the configuration
		if err := viper.ReadInConfig(); err != nil {
			// It's okay if there is no config file
			if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
				log.Error().Err(err).Msg("Error reading config file")
				return err
			}
			log.Debug().Msg("No config file found, using defaults and environment variables")
		} else {
			log.Info().Str("config_file", viper.ConfigFileUsed()).Msg("Config file loaded")
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
		
		log.Debug().
			Bool("dapr_mode", crawlerCfg.DaprMode).
			Int("dapr_port", crawlerCfg.DaprPort).
			Bool("dapr_job_mode", crawlerCfg.DaprJobMode).
			Int("concurrency", crawlerCfg.Concurrency).
			Int("timeout", crawlerCfg.Timeout).
			Str("output_format", crawlerCfg.OutputFormat).
			Str("storage_root", crawlerCfg.StorageRoot).
			Msg("Base configuration loaded")
		
		// Get the multiple TDLib database URLs
		tdlibDatabaseURLs = viper.GetStringSlice("tdlib.database_urls")
		crawlerCfg.TDLibDatabaseURLs = tdlibDatabaseURLs
		
		// If no multiple URLs are provided but the single URL is, add it to the list
		if len(crawlerCfg.TDLibDatabaseURLs) == 0 && crawlerCfg.TDLibDatabaseURL != "" {
			crawlerCfg.TDLibDatabaseURLs = append(crawlerCfg.TDLibDatabaseURLs, crawlerCfg.TDLibDatabaseURL)
			log.Debug().
				Str("tdlib_database_url", crawlerCfg.TDLibDatabaseURL).
				Msg("Using single database URL as no multiple URLs provided")
		}
		
		if len(crawlerCfg.TDLibDatabaseURLs) > 0 {
			log.Info().
				Int("url_count", len(crawlerCfg.TDLibDatabaseURLs)).
				Msg("Configured with multiple TDLib database URLs")
			
			for i, url := range crawlerCfg.TDLibDatabaseURLs {
				log.Debug().Int("index", i).Str("url", url).Msg("TDLib database URL")
			}
		}
		
		crawlerCfg.MinUsers = viper.GetInt("crawler.minusers")
		crawlerCfg.CrawlID = viper.GetString("crawler.crawlid")
		crawlerCfg.MaxComments = viper.GetInt("crawler.maxcomments")
		crawlerCfg.MaxPosts = viper.GetInt("crawler.maxposts")
		crawlerCfg.MaxDepth = viper.GetInt("crawler.maxdepth")
		crawlerCfg.MaxPages = viper.GetInt("crawler.maxpages")

		log.Debug().
			Int("min_users", crawlerCfg.MinUsers).
			Str("crawl_id", crawlerCfg.CrawlID).
			Int("max_comments", crawlerCfg.MaxComments).
			Int("max_posts", crawlerCfg.MaxPosts).
			Int("max_depth", crawlerCfg.MaxDepth).
			Int("max_pages", crawlerCfg.MaxPages).
			Msg("Crawler limits configured")

		// Parse min post date from string to time.Time if provided
		minPostDateStr := viper.GetString("crawler.minpostdate")
		if minPostDateStr != "" {
			parsedTime, err := time.Parse("2006-01-02", minPostDateStr)
			if err != nil {
				log.Error().Err(err).Str("date_string", minPostDateStr).Msg("Invalid min-post-date format")
				return fmt.Errorf("invalid min-post-date format, must be YYYY-MM-DD: %v", err)
			}
			crawlerCfg.MinPostDate = parsedTime
			log.Info().Time("min_post_date", parsedTime).Msg("Min post date configured")
		} else {
			// Set to zero time if not specified
			crawlerCfg.MinPostDate = time.Time{}
			log.Debug().Msg("No minimum post date specified")
		}

		// Check if time-ago is provided and use it if min-post-date isn't set
		timeAgoStr := viper.GetString("crawler.timeago")
		if timeAgoStr != "" {
			log.Debug().Str("time_ago", timeAgoStr).Msg("Processing time-ago parameter")
			
			// Only use time-ago if min-post-date isn't explicitly set
			cutoffTime, err := parseTimeAgo(timeAgoStr)
			if err != nil {
				log.Error().Err(err).Str("time_ago", timeAgoStr).Msg("Failed to parse time-ago parameter")
				return err
			}
			
			// Only set if valid
			if !cutoffTime.IsZero() {
				crawlerCfg.PostRecency = cutoffTime
				log.Info().
					Time("cutoff_time", cutoffTime).
					Str("time_ago", timeAgoStr).
					Msg("Using relative time cutoff for post recency")
			} else if !crawlerCfg.MinPostDate.IsZero() {
				log.Warn().
					Time("min_post_date", crawlerCfg.MinPostDate).
					Str("time_ago", timeAgoStr).
					Msg("Both min-post-date and time-ago specified; using min-post-date")
			}
		}

		// Override with command line flags if provided
		if cmd.Flags().Changed("dapr-mode") {
			crawlerCfg.DaprJobMode = daprMode == "job"
			log.Debug().
				Bool("dapr_job_mode", crawlerCfg.DaprJobMode).
				Str("dapr_mode", daprMode).
				Msg("DAPR mode overridden by command line flag")
		}

		log.Info().Msg("Configuration loaded successfully")
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		// If no specific subcommand is invoked, show help
		if !generateCode && len(args) == 0 && !crawlerCfg.DaprMode && len(urlList) == 0 && urlFile == "" {
			log.Info().Msg("No arguments provided, showing help")
			cmd.Help()
			return
		}

		// Log url information if available
		if len(urlList) > 0 {
			log.Info().
				Strs("urls", urlList).
				Int("url_count", len(urlList)).
				Msg("URLs provided via command line")
		}
		
		if urlFile != "" {
			log.Info().Str("url_file", urlFile).Msg("URL file provided")
		}

		// Start in appropriate mode
		if crawlerCfg.DaprMode {
			if crawlerCfg.DaprJobMode {
				log.Info().
					Int("dapr_port", crawlerCfg.DaprPort).
					Msg("Starting in DAPR job mode")
				dapr.StartDaprMode(crawlerCfg)
			} else {
				log.Info().
					Int("dapr_port", crawlerCfg.DaprPort).
					Int("url_count", len(urlList)).
					Bool("generate_code", generateCode).
					Msg("Starting in DAPR standalone mode")
				dapr.StartDaprStandaloneMode(urlList, urlFile, crawlerCfg, generateCode)
			}
		} else {
			log.Info().
				Int("url_count", len(urlList)).
				Bool("generate_code", generateCode).
				Msg("Starting in regular standalone mode")
			standalone.StartStandaloneMode(urlList, urlFile, crawlerCfg, generateCode)
		}
	},
}

// Initialize cobra command
func init() {
	// Global flags
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./config.yaml)")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "Log level (trace, debug, info, warn, error, fatal, panic)")
	rootCmd.PersistentFlags().BoolVar(&crawlerCfg.DaprMode, "dapr", false, "run with DAPR enabled")
	rootCmd.PersistentFlags().StringVar(&daprMode, "dapr-mode", "job", "DAPR mode to use ('job' or 'standalone')")
	rootCmd.PersistentFlags().IntVar(&crawlerCfg.DaprPort, "dapr-port", 6481, "DAPR port to use")
	rootCmd.PersistentFlags().IntVar(&crawlerCfg.Concurrency, "concurrency", 1, "number of concurrent crawlers")
	rootCmd.PersistentFlags().IntVar(&crawlerCfg.Timeout, "timeout", 30, "HTTP request timeout in seconds")
	rootCmd.PersistentFlags().StringVar(&crawlerCfg.UserAgent, "user-agent", "Mozilla/5.0 Crawler", "User agent to use")
	rootCmd.PersistentFlags().StringVar(&crawlerCfg.OutputFormat, "output", "json", "Output format (json, csv, etc.)")
	rootCmd.PersistentFlags().StringVar(&crawlerCfg.StorageRoot, "storage-root", "/tmp/crawl", "Storage root directory")
	rootCmd.PersistentFlags().StringVar(&minPostDate, "min-post-date", "", "Minimum post date to crawl (format: YYYY-MM-DD)")
	rootCmd.PersistentFlags().StringVar(&timeAgo, "time-ago", "1m", "Only consider posts newer than this time ago (e.g., '30d' for 30 days, '6h' for 6 hours, '2w' for 2 weeks, '1m' for 1 month, '1y' for 1 year)")
	rootCmd.PersistentFlags().StringVar(&crawlerCfg.TDLibDatabaseURL, "tdlib-database-url", "", "URL to a pre-seeded TDLib database archive (deprecated, use --tdlib-database-urls)")
	rootCmd.PersistentFlags().StringSliceVar(&tdlibDatabaseURLs, "tdlib-database-urls", []string{}, "Comma-separated list of URLs to pre-seeded TDLib database archives for connection pooling")
	rootCmd.PersistentFlags().IntVar(&minUsers, "min-users", 100, "Minimum number of users in a channel to crawl")
	rootCmd.PersistentFlags().StringVar(&crawlID, "crawl-id", "", "Unique identifier for this crawl operation")
	rootCmd.PersistentFlags().IntVar(&crawlerCfg.MaxComments, "max-comments", -1, "The maximum number of comments to crawl")
	rootCmd.PersistentFlags().IntVar(&crawlerCfg.MaxDepth, "max-depth", -1, "The maximum depth of the crawl")
	rootCmd.PersistentFlags().IntVar(&crawlerCfg.MaxPosts, "max-posts", -1, "The maximum posts to collect")
	rootCmd.PersistentFlags().IntVar(&crawlerCfg.MaxPages, "max-pages", 108000, "The maximum number of pages/channels to crawl")

	// Standalone mode specific flags
	rootCmd.Flags().StringSliceVar(&urlList, "urls", []string{}, "comma-separated list of URLs to crawl")
	rootCmd.Flags().StringVar(&urlFile, "url-file", "", "file containing URLs to crawl (one per line)")
	rootCmd.Flags().BoolVar(&generateCode, "generate-code", false, "run code generation after crawling")
	rootCmd.Flags().StringVar(&crawlType, "crawl-type", "focused", "Select between focused(default) and snowball")

	// Bind flags to viper
	viper.BindPFlag("logging.level", rootCmd.PersistentFlags().Lookup("log-level"))
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
	viper.BindPFlag("tdlib.database_urls", rootCmd.PersistentFlags().Lookup("tdlib-database-urls"))
	viper.BindPFlag("crawler.minusers", rootCmd.PersistentFlags().Lookup("min-users"))
	viper.BindPFlag("crawler.crawlid", rootCmd.PersistentFlags().Lookup("crawl-id"))
	viper.BindPFlag("crawler.maxcomments", rootCmd.PersistentFlags().Lookup("max-comments"))
	viper.BindPFlag("crawler.maxposts", rootCmd.PersistentFlags().Lookup("max-posts"))
	viper.BindPFlag("crawler.maxdepth", rootCmd.PersistentFlags().Lookup("max-depth"))
	viper.BindPFlag("crawler.maxpages", rootCmd.PersistentFlags().Lookup("max-pages"))

	// Add subcommands
	rootCmd.AddCommand(versionCmd)
}

// Version command
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number",
	Run: func(cmd *cobra.Command, args []string) {
		version := "Crawler v1.0"
		fmt.Println(version)
		log.Info().Str("version", version).Msg("Version information requested")
	},
}
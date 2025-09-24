package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/dapr"
	"github.com/researchaccelerator-hub/telegram-scraper/orchestrator"
	"github.com/researchaccelerator-hub/telegram-scraper/standalone"
	"github.com/researchaccelerator-hub/telegram-scraper/worker"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile           string
	crawlerCfg        common.CrawlerConfig
	urlList           []string
	urlFile           string
	urlFileURL        string
	generateCode      bool
	minPostDate       string
	daprMode          string
	mode              string // New: execution mode (standalone, dapr-standalone, orchestrator, worker)
	workerID          string // New: worker identifier for distributed mode
	minUsers          int
	crawlID           string
	crawlLabel        string   // User-provided label for the crawl
	timeAgo           string   // Time ago parameter
	dateBetween       string   // Date range in format "YYYY-MM-DD,YYYY-MM-DD"
	sampleSize        int      // Number of posts to randomly sample when using date-between
	tdlibDatabaseURLs []string // Multiple TDLib database URLs
	logLevel          string   // Logging level
	tdlibVerbosity    int      // TDLib verbosity level
	skipMediaDownload bool     // Flag to skip media downloads
)

func main() {
	// Set up the logger with a default level
	// The actual level will be configured in PersistentPreRunE
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	// TODO: Remove after identifying memory leak

	mux := http.NewServeMux()

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	log.Info().Msg("Starting server on :6060")
	log.Info().Msg("Profiling endpoints are available at http://localhost:6060/debug/pprof/")

	server := &http.Server{
		Addr:    ":6060",
		Handler: mux,
	}

	// Start the HTTP server.
	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.Error().Msgf("Could not start server: %s\n", err)
		}
	}()

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
	Short: "A flexible web crawler that can run in multiple execution modes",
	Long: `A web crawler application that can run in multiple execution modes:

Distributed modes (Phase 2+):
  --mode=orchestrator    - Central coordinator that distributes work via Dapr pubsub
  --mode=worker          - Worker node that processes individual crawl tasks

Direct execution modes:
  --mode=standalone      - Single process crawling without Dapr
  --mode=dapr-standalone - Single process crawling with Dapr state management
  --mode=dapr-job        - Dapr job mode for scheduled crawling tasks (uses CLI config + job data)

Legacy modes (for backward compatibility):
  --dapr --dapr-mode=job - Traditional Dapr job mode
  --dapr                 - Traditional Dapr standalone mode
  (no flags)             - Traditional standalone mode

Configuration Priority:
  - All CLI arguments are accepted and used as defaults
  - dapr-job mode: job data overrides CLI arguments when provided
  - URLs: job data takes precedence, CLI URLs used if job has none
  - YouTube random sampling doesn't require URLs (discovers content randomly)

Examples:
  # Distributed orchestrator (Phase 2+)
  crawler --mode=orchestrator --dapr --urls="url1,url2"
  
  # Distributed worker (Phase 2+)  
  crawler --mode=worker --dapr --worker-id="worker-1"
  
  # Explicit standalone
  crawler --mode=standalone --urls="url1,url2"
  
  # DAPR job mode (all CLI args accepted, job data can override)
  crawler --mode=dapr-job --dapr-port=6481 --platform=youtube --max-posts=1000
  
  # Legacy compatibility
  crawler --urls="url1,url2"`,
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

		// Check YouTube API key if platform is YouTube (skip for dapr-job mode as API key may come from job data)
		if crawlerCfg.Platform == "youtube" && mode != "dapr-job" {
			if crawlerCfg.YouTubeAPIKey == "" {
				fmt.Println("Error: When using --platform youtube, you must provide a valid YouTube API key with --youtube-api-key")
				log.Error().Msg("YouTube API key is required but was not provided")
				return fmt.Errorf("YouTube API key is required for YouTube platform")
			} else {
				log.Info().Str("api_key_status", "provided").Str("api_key_length", fmt.Sprintf("%d chars", len(crawlerCfg.YouTubeAPIKey))).Msg("Using YouTube API key")
			}
		} else if crawlerCfg.Platform == "youtube" && mode == "dapr-job" {
			log.Debug().Msg("YouTube platform selected for dapr-job mode; API key validation will occur when job data is processed")
		}

		// TODO: update to just pass the whole crawl config to validate sampling method
		// Validate sampling method combinations (skip URL validation for dapr-job mode)
		if crawlerCfg.SamplingMethod == "random-walk" {
			// xor operation to confirm only one of the two options is provided
			if (len(urlList) > 0) != (crawlerCfg.SeedSize > 0) {
			} else if (len(urlList) > 0) && (crawlerCfg.SeedSize > 0) {
				return fmt.Errorf("Cannot provide seed urls and seed size in random-walk crawl")
			}
		} else if err := validateSamplingMethod(crawlerCfg.Platform, crawlerCfg.SamplingMethod, urlList, urlFile, mode); err != nil {
			return err
		}

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
		crawlerCfg.CrawlLabel = viper.GetString("crawler.crawllabel")
		crawlerCfg.MaxComments = viper.GetInt("crawler.maxcomments")
		crawlerCfg.MaxPosts = viper.GetInt("crawler.maxposts")
		crawlerCfg.MaxDepth = viper.GetInt("crawler.maxdepth")
		crawlerCfg.MaxPages = viper.GetInt("crawler.maxpages")

		// Set TDLib verbosity level
		if cmd.Flags().Changed("tdlib-verbosity") {
			crawlerCfg.TDLibVerbosity = tdlibVerbosity
		} else {
			crawlerCfg.TDLibVerbosity = viper.GetInt("tdlib.verbosity")
			if crawlerCfg.TDLibVerbosity == 0 {
				// Default to 1 if not specified in config
				crawlerCfg.TDLibVerbosity = 1
			}
		}
		// Set skip media download flag
		if cmd.Flags().Changed("skip-media") {
			crawlerCfg.SkipMediaDownload = skipMediaDownload
		} else {
			crawlerCfg.SkipMediaDownload = viper.GetBool("crawler.skipmedia")
		}

		// Load sampling method configuration
		crawlerCfg.SamplingMethod = viper.GetString("crawler.sampling")
		if crawlerCfg.SamplingMethod == "" {
			crawlerCfg.SamplingMethod = "channel" // Default value
		}

		// Load minimum channel videos configuration
		crawlerCfg.MinChannelVideos = viper.GetInt64("crawler.min_channel_videos")
		if crawlerCfg.MinChannelVideos == 0 {
			crawlerCfg.MinChannelVideos = 10 // Default value
		}

		log.Debug().
			Int("min_users", crawlerCfg.MinUsers).
			Str("crawl_id", crawlerCfg.CrawlID).
			Str("crawl_label", crawlerCfg.CrawlLabel).
			Int("max_comments", crawlerCfg.MaxComments).
			Int("max_posts", crawlerCfg.MaxPosts).
			Int("max_depth", crawlerCfg.MaxDepth).
			Int("max_pages", crawlerCfg.MaxPages).
			Int("tdlib_verbosity", crawlerCfg.TDLibVerbosity).
			Bool("skip_media_download", crawlerCfg.SkipMediaDownload).
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

		// Parse date-between parameter if provided
		dateBetweenStr := viper.GetString("crawler.datebetween")
		if dateBetweenStr != "" {
			log.Debug().Str("date_between", dateBetweenStr).Msg("Processing date-between parameter")

			// Parse date range in format "YYYY-MM-DD,YYYY-MM-DD"
			dates := strings.Split(dateBetweenStr, ",")
			if len(dates) != 2 {
				log.Error().Str("date_between", dateBetweenStr).Msg("Invalid date-between format")
				return fmt.Errorf("invalid date-between format, must be 'YYYY-MM-DD,YYYY-MM-DD'")
			}

			minDate, err := time.Parse("2006-01-02", strings.TrimSpace(dates[0]))
			if err != nil {
				log.Error().Err(err).Str("min_date", dates[0]).Msg("Invalid min date in date-between")
				return fmt.Errorf("invalid min date in date-between format, must be YYYY-MM-DD: %v", err)
			}

			maxDate, err := time.Parse("2006-01-02", strings.TrimSpace(dates[1]))
			if err != nil {
				log.Error().Err(err).Str("max_date", dates[1]).Msg("Invalid max date in date-between")
				return fmt.Errorf("invalid max date in date-between format, must be YYYY-MM-DD: %v", err)
			}

			// Validate that min date is before max date
			if minDate.After(maxDate) {
				log.Error().
					Time("min_date", minDate).
					Time("max_date", maxDate).
					Msg("Min date is after max date in date-between")
				return fmt.Errorf("min date must be before max date in date-between")
			}

			crawlerCfg.DateBetweenMin = minDate
			crawlerCfg.DateBetweenMax = maxDate
			log.Info().
				Time("min_date", minDate).
				Time("max_date", maxDate).
				Msg("Date range configured for date-between filtering")
		}

		// Parse sample-size parameter if provided
		sampleSizeValue := viper.GetInt("crawler.samplesize")
		if sampleSizeValue > 0 {
			crawlerCfg.SampleSize = sampleSizeValue
			log.Info().
				Int("sample_size", sampleSizeValue).
				Msg("Random sampling configured for date-between filtering")
		}

		// Override with command line flags if provided
		if cmd.Flags().Changed("dapr-mode") {
			crawlerCfg.DaprJobMode = daprMode == "job"
			log.Debug().
				Bool("dapr_job_mode", crawlerCfg.DaprJobMode).
				Str("dapr_mode", daprMode).
				Msg("DAPR mode overridden by command line flag")
		}

		// Validate distributed mode configuration
		if mode == "worker" && workerID == "" {
			return fmt.Errorf("worker mode requires --worker-id to be specified")
		}

		// Log the execution mode
		if mode != "" {
			log.Info().Str("execution_mode", mode).Msg("Execution mode explicitly set")
			if mode == "worker" {
				log.Info().Str("worker_id", workerID).Msg("Worker ID configured")
			}
		} else {
			log.Debug().Msg("No explicit execution mode set, will use legacy auto-detection")
		}

		log.Info().Msg("Configuration loaded successfully")
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		// If no specific subcommand is invoked, show help
		// Skip URL requirement check for dapr-job mode since URLs come from job data
		if !generateCode && len(args) == 0 && !crawlerCfg.DaprMode && len(urlList) == 0 && urlFile == "" && urlFileURL == "" && mode == "" {
			log.Info().Msg("No arguments provided, showing help")
			cmd.Help()
			return
		}

		// Handle URL file from URL if provided
		var downloadedFile string
		if urlFileURL != "" {
			log.Info().Str("url_file_url", urlFileURL).Msg("URL file URL provided")

			var err error
			downloadedFile, err = common.DownloadURLFile(urlFileURL)
			if err != nil {
				log.Fatal().Err(err).Str("url", urlFileURL).Msg("Failed to download URL file")
				return
			}

			// Set urlFile to the downloaded file path if no local urlFile was specified
			if urlFile == "" {
				urlFile = downloadedFile
				log.Info().Str("downloaded_file", urlFile).Msg("Using downloaded URL file")
			} else {
				log.Warn().
					Str("url_file", urlFile).
					Str("downloaded_file", downloadedFile).
					Msg("Both local file and URL provided, using local file")
			}
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

		// Determine execution mode - new distributed modes take precedence
		switch mode {
		case "orchestrator":
			log.Info().Str("mode", mode).Msg("Starting in orchestrator mode")
			startOrchestratorMode(urlList, urlFile, crawlerCfg, generateCode)
		case "worker":
			log.Info().Str("mode", mode).Str("worker_id", workerID).Msg("Starting in worker mode")
			startWorkerMode(workerID, crawlerCfg)
		case "standalone":
			log.Info().Str("mode", mode).Msg("Starting in explicit standalone mode")
			standalone.StartStandaloneMode(urlList, urlFile, crawlerCfg, generateCode)
		case "dapr-standalone":
			log.Info().Str("mode", mode).Msg("Starting in explicit DAPR standalone mode")
			dapr.StartDaprStandaloneMode(urlList, urlFile, crawlerCfg, generateCode)
		case "dapr-job":
			log.Info().Str("mode", mode).Msg("Starting in DAPR job mode")
			// URLs not required for dapr-job mode as they come from job data
			dapr.StartDaprMode(crawlerCfg)
		case "":
			// Legacy mode detection for backward compatibility
			if crawlerCfg.DaprMode {
				if crawlerCfg.DaprJobMode {
					log.Info().
						Int("dapr_port", crawlerCfg.DaprPort).
						Msg("Starting in DAPR job mode (legacy)")
					dapr.StartDaprMode(crawlerCfg)
				} else {
					log.Info().
						Int("dapr_port", crawlerCfg.DaprPort).
						Int("url_count", len(urlList)).
						Bool("generate_code", generateCode).
						Msg("Starting in DAPR standalone mode (legacy)")
					dapr.StartDaprStandaloneMode(urlList, urlFile, crawlerCfg, generateCode)
				}
			} else {
				log.Info().
					Int("url_count", len(urlList)).
					Bool("generate_code", generateCode).
					Msg("Starting in regular standalone mode (legacy)")
				standalone.StartStandaloneMode(urlList, urlFile, crawlerCfg, generateCode)
			}
		default:
			log.Fatal().Str("mode", mode).Msg("Unknown execution mode")
		}

		// Clean up downloaded file if needed
		if downloadedFile != "" && downloadedFile == urlFile {
			// Only delete if we're using the downloaded file and not a user-provided one
			log.Debug().Str("file", downloadedFile).Msg("Cleaning up downloaded URL file")
			if err := os.Remove(downloadedFile); err != nil {
				log.Warn().Err(err).Str("file", downloadedFile).Msg("Failed to clean up downloaded URL file")
			}
		}
	},
}

// startOrchestratorMode starts the distributed orchestrator
func startOrchestratorMode(urlList []string, urlFile string, crawlerCfg common.CrawlerConfig, generateCode bool) {
	log.Info().Msg("Starting orchestrator mode (Phase 1 - basic structure)")

	// Collect URLs from command line arguments or file
	var urls []string
	if len(urlList) > 0 {
		urls = append(urls, urlList...)
	}

	if urlFile != "" {
		fileURLs, err := common.ReadURLsFromFile(urlFile)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to read URLs from file")
		}
		urls = append(urls, fileURLs...)
	}

	// For random sampling, URLs are not required since we discover content randomly
	if len(urls) == 0 && !(crawlerCfg.Platform == "youtube" && crawlerCfg.SamplingMethod == "random") {
		log.Fatal().Msg("No URLs provided. Use --urls or --url-file to specify URLs to crawl")
	}

	// Generate crawl ID if not provided
	if crawlerCfg.CrawlID == "" {
		crawlerCfg.CrawlID = common.GenerateCrawlID()
	}

	// Create orchestrator instance
	orch, err := orchestrator.NewOrchestrator(crawlerCfg.CrawlID, crawlerCfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create orchestrator")
	}

	// Set up signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start orchestrator
	err = orch.Start(ctx, urls)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to start orchestrator")
	}

	log.Info().Str("crawl_id", crawlerCfg.CrawlID).Int("url_count", len(urls)).Msg("Orchestrator started, waiting for signal to stop")

	// Wait for shutdown signal
	<-sigChan
	log.Info().Msg("Received shutdown signal, stopping orchestrator")

	// Stop orchestrator
	if err := orch.Stop(ctx); err != nil {
		log.Error().Err(err).Msg("Error stopping orchestrator")
	} else {
		log.Info().Msg("Orchestrator stopped gracefully")
	}
}

// startWorkerMode starts the distributed worker
func startWorkerMode(workerID string, crawlerCfg common.CrawlerConfig) {
	log.Info().Str("worker_id", workerID).Msg("Starting worker mode (Phase 1 - basic structure)")

	// Create worker instance
	w, err := worker.NewWorker(workerID, crawlerCfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create worker")
	}

	// Set up signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start worker
	err = w.Start(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to start worker")
	}

	log.Info().Str("worker_id", workerID).Msg("Worker started, waiting for signal to stop")

	// Wait for shutdown signal
	<-sigChan
	log.Info().Msg("Received shutdown signal, stopping worker")

	// Stop worker
	if err := w.Stop(ctx); err != nil {
		log.Error().Err(err).Msg("Error stopping worker")
	} else {
		log.Info().Msg("Worker stopped gracefully")
	}
}

// validateSamplingMethod validates that the platform supports the specified sampling method
func validateSamplingMethod(platform, samplingMethod string, urlList []string, urlFile string, mode string) error {
	// Valid sampling methods per platform
	validMethods := map[string][]string{
		"telegram": {"channel", "snowball", "random-walk"},
		"youtube":  {"channel", "random", "snowball"},
	}

	// Check if platform is supported
	supportedMethods, exists := validMethods[platform]
	if !exists {
		return fmt.Errorf("unsupported platform: %s", platform)
	}

	// Check if sampling method is valid for this platform
	isSupported := false
	for _, method := range supportedMethods {
		if method == samplingMethod {
			isSupported = true
			break
		}
	}

	if !isSupported {
		return fmt.Errorf("sampling method '%s' is not supported for platform '%s'. Supported methods: %v",
			samplingMethod, platform, supportedMethods)
	}

	// For random sampling, no URLs/channels are required
	if samplingMethod == "random" || samplingMethod == "random-walk" {
		return nil
	}

	// For channel and snowball sampling, validate that URLs are provided
	// Skip URL validation for dapr-job mode since jobs provide URLs through job data
	if (samplingMethod == "channel" || samplingMethod == "snowball" || samplingMethod == "random-walk") && len(urlList) == 0 && urlFile == "" && mode != "dapr-job" {
		return fmt.Errorf("%s sampling requires URLs to be provided. Use --urls or --url-file to specify them", samplingMethod)
	}

	return nil
}

// Initialize cobra command
func init() {
	// Global flags
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./config.yaml)")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "debug", "Log level (trace, debug, info, warn, error, fatal, panic)")
	rootCmd.PersistentFlags().BoolVar(&crawlerCfg.DaprMode, "dapr", false, "run with DAPR enabled")
	rootCmd.PersistentFlags().StringVar(&daprMode, "dapr-mode", "job", "DAPR mode to use ('job' or 'standalone')")
	rootCmd.PersistentFlags().IntVar(&crawlerCfg.DaprPort, "dapr-port", 6481, "DAPR port to use")
	rootCmd.PersistentFlags().IntVar(&crawlerCfg.Concurrency, "concurrency", 1, "number of concurrent crawlers")
	rootCmd.PersistentFlags().IntVar(&crawlerCfg.Timeout, "timeout", 30, "HTTP request timeout in seconds")
	rootCmd.PersistentFlags().StringVar(&crawlerCfg.UserAgent, "user-agent", "Mozilla/5.0 Crawler", "User agent to use")
	rootCmd.PersistentFlags().StringVar(&crawlerCfg.OutputFormat, "output", "json", "Output format (json, csv, etc.)")
	rootCmd.PersistentFlags().StringVar(&crawlerCfg.StorageRoot, "storage-root", "/tmp/crawl", "Storage root directory")
	rootCmd.PersistentFlags().StringVar(&minPostDate, "min-post-date", "", "Minimum post date to crawl (format: YYYY-MM-DD)")
	rootCmd.PersistentFlags().StringVar(&timeAgo, "time-ago", "", "Only consider posts newer than this time ago (e.g., '30d' for 30 days, '6h' for 6 hours, '2w' for 2 weeks, '1m' for 1 month, '1y' for 1 year)")
	rootCmd.PersistentFlags().StringVar(&dateBetween, "date-between", "", "Date range to crawl posts between (format: YYYY-MM-DD,YYYY-MM-DD)")
	rootCmd.PersistentFlags().IntVar(&sampleSize, "sample-size", 0, "Number of posts to randomly sample when using date-between (0 means no sampling)")
	rootCmd.PersistentFlags().StringVar(&crawlerCfg.TDLibDatabaseURL, "tdlib-database-url", "", "URL to a pre-seeded TDLib database archive (deprecated, use --tdlib-database-urls)")
	rootCmd.PersistentFlags().StringSliceVar(&tdlibDatabaseURLs, "tdlib-database-urls", []string{}, "Comma-separated list of URLs to pre-seeded TDLib database archives for connection pooling")
	rootCmd.PersistentFlags().IntVar(&minUsers, "min-users", 100, "Minimum number of users in a channel to crawl")
	rootCmd.PersistentFlags().StringVar(&crawlID, "crawl-id", "", "Unique identifier for this crawl operation")
	rootCmd.PersistentFlags().StringVar(&crawlLabel, "crawl-label", "", "User-defined label for the crawl (e.g., 'youtube-snowball')")
	rootCmd.PersistentFlags().IntVar(&crawlerCfg.MaxComments, "max-comments", -1, "The maximum number of comments to crawl")
	rootCmd.PersistentFlags().IntVar(&crawlerCfg.MaxDepth, "max-depth", -1, "The maximum depth of the crawl")
	rootCmd.PersistentFlags().IntVar(&crawlerCfg.MaxPosts, "max-posts", -1, "The maximum posts to collect")
	rootCmd.PersistentFlags().IntVar(&crawlerCfg.MaxPages, "max-pages", 108000, "The maximum number of pages/channels to crawl")
	rootCmd.PersistentFlags().IntVar(&tdlibVerbosity, "tdlib-verbosity", 1, "TDLib verbosity level (0-10, where 10 is most verbose)")
	rootCmd.PersistentFlags().BoolVar(&skipMediaDownload, "skip-media", false, "Skip downloading media files (thumbnails, videos, etc.)")
	rootCmd.PersistentFlags().StringVar(&crawlerCfg.YouTubeAPIKey, "youtube-api-key", "", "API key for YouTube Data API")
	rootCmd.PersistentFlags().StringVar(&crawlerCfg.Platform, "platform", "telegram", "Platform to crawl (telegram, youtube)")
	rootCmd.PersistentFlags().StringVar(&crawlerCfg.SamplingMethod, "sampling", "channel", "Sampling method: channel, random, random-walk, snowball")
	rootCmd.PersistentFlags().IntVar(&crawlerCfg.SeedSize, "seed-size", 0, "Number of discovered channels to randomly select as seed channels")
	rootCmd.PersistentFlags().IntVar(&crawlerCfg.WalkbackRate, "walkback-rate", 15, "The rate at which to perform walkbacks when using random-walk sampling")
	rootCmd.PersistentFlags().Int64Var(&crawlerCfg.MinChannelVideos, "min-channel-videos", 10, "Minimum videos per channel for inclusion")

	// New distributed mode flags
	rootCmd.PersistentFlags().StringVar(&mode, "mode", "", "Execution mode: standalone, dapr-standalone, orchestrator, worker (empty for legacy auto-detection)")
	rootCmd.PersistentFlags().StringVar(&workerID, "worker-id", "", "Worker identifier for distributed mode (required for worker mode)")

	// Standalone mode specific flags
	rootCmd.Flags().StringSliceVar(&urlList, "urls", []string{}, "comma-separated list of URLs to crawl")
	rootCmd.Flags().StringVar(&urlFile, "url-file", "", "file containing URLs to crawl (one per line)")
	rootCmd.Flags().StringVar(&urlFileURL, "url-file-url", "", "URL to a file containing URLs to crawl (one per line)")
	rootCmd.Flags().BoolVar(&generateCode, "generate-code", false, "run code generation after crawling")

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
	viper.BindPFlag("crawler.datebetween", rootCmd.PersistentFlags().Lookup("date-between"))
	viper.BindPFlag("crawler.samplesize", rootCmd.PersistentFlags().Lookup("sample-size"))
	viper.BindPFlag("tdlib.database_url", rootCmd.PersistentFlags().Lookup("tdlib-database-url"))
	viper.BindPFlag("tdlib.database_urls", rootCmd.PersistentFlags().Lookup("tdlib-database-urls"))
	viper.BindPFlag("tdlib.verbosity", rootCmd.PersistentFlags().Lookup("tdlib-verbosity"))
	viper.BindPFlag("crawler.minusers", rootCmd.PersistentFlags().Lookup("min-users"))
	viper.BindPFlag("crawler.crawlid", rootCmd.PersistentFlags().Lookup("crawl-id"))
	viper.BindPFlag("crawler.crawllabel", rootCmd.PersistentFlags().Lookup("crawl-label"))
	viper.BindPFlag("crawler.maxcomments", rootCmd.PersistentFlags().Lookup("max-comments"))
	viper.BindPFlag("crawler.maxposts", rootCmd.PersistentFlags().Lookup("max-posts"))
	viper.BindPFlag("crawler.maxdepth", rootCmd.PersistentFlags().Lookup("max-depth"))
	viper.BindPFlag("crawler.maxpages", rootCmd.PersistentFlags().Lookup("max-pages"))
	viper.BindPFlag("crawler.skipmedia", rootCmd.PersistentFlags().Lookup("skip-media"))
	viper.BindPFlag("youtube.api_key", rootCmd.PersistentFlags().Lookup("youtube-api-key"))
	viper.BindPFlag("crawler.platform", rootCmd.PersistentFlags().Lookup("platform"))
	viper.BindPFlag("crawler.sampling", rootCmd.PersistentFlags().Lookup("sampling"))
	viper.BindPFlag("crawler.seedsize", rootCmd.PersistentFlags().Lookup("seed-size"))
	viper.BindPFlag("crawler.walkback_rate", rootCmd.PersistentFlags().Lookup("walkback-rate"))
	viper.BindPFlag("crawler.min_channel_videos", rootCmd.PersistentFlags().Lookup("min-channel-videos"))
	viper.BindPFlag("distributed.mode", rootCmd.PersistentFlags().Lookup("mode"))
	viper.BindPFlag("distributed.worker_id", rootCmd.PersistentFlags().Lookup("worker-id"))

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

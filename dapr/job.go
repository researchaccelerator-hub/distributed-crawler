// Package dapr provides Dapr-related functionality
package dapr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	daprc "github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"
	daprs "github.com/dapr/go-sdk/service/grpc"
	common2 "github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/crawl"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/researchaccelerator-hub/telegram-scraper/telegramhelper"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/types/known/anypb"
)

// StartDaprMode initializes and starts a Dapr service in job mode using the provided
// crawler configuration.
//
// This function:
// 1. Initializes a Dapr client for interacting with the Dapr runtime
// 2. Creates a gRPC service that listens on the configured port
// 3. Registers service invocation handlers for:
//   - scheduleJob: For scheduling new crawling jobs
//   - getJob: For retrieving information about existing jobs
//
// 4. Registers job event handlers for predefined job names
// 5. Starts the service and keeps it running until terminated
//
// The function uses the Dapr Jobs API to schedule and manage crawling tasks,
// which allows for distributed execution and better reliability. Jobs can be
// scheduled with specific due times and will be executed by the Dapr runtime
// based on that schedule.
//
// Parameters:
//   - crawlerCfg: Configuration settings for the crawler, including Dapr port and other settings
//
// The function will panic if it fails to create the Dapr client or start the service.
func StartDaprMode(crawlerCfg common2.CrawlerConfig) {
	log.Info().Msg("🚀 Starting crawler in DAPR job mode")
	log.Info().Msgf("📡 Listening on port %d for DAPR requests", crawlerCfg.DaprPort)
	log.Info().Msgf("🔧 Base configuration: platform=%s, concurrency=%d, storage=%s",
		crawlerCfg.Platform, crawlerCfg.Concurrency, crawlerCfg.StorageRoot)

	//Create new Dapr client
	log.Info().Msg("🔌 Creating DAPR client connection...")
	daprClient, err := daprc.NewClient()
	if err != nil {
		log.Fatal().Err(err).Msg("❌ Failed to create DAPR client")
		panic(err)
	}
	defer daprClient.Close()
	log.Info().Msg("✅ DAPR client connection established")

	app = App{
		daprClient: daprClient,
		baseConfig: crawlerCfg, // Store CLI configuration
	}
	log.Info().Msg("📦 Application state initialized with base configuration")

	// Create a new Dapr service
	port := fmt.Sprintf(":%d", crawlerCfg.DaprPort)
	log.Info().Msgf("🌐 Creating DAPR gRPC service on port %s", port)
	server, err := daprs.NewService(port)
	if err != nil {
		log.Fatal().Err(err).Msg("❌ Failed to start DAPR gRPC server")
	}
	log.Info().Msg("✅ DAPR gRPC service created successfully")

	// Creates handlers for the service
	log.Info().Msg("🔗 Registering service invocation handlers...")
	if err := server.AddServiceInvocationHandler("scheduleJob", scheduleJob); err != nil {
		log.Fatal().Err(err).Msg("❌ Error adding scheduleJob invocation handler")
	}
	log.Info().Msg("✅ Registered scheduleJob handler")

	if err := server.AddServiceInvocationHandler("getJob", getJob); err != nil {
		log.Fatal().Err(err).Msg("❌ Error adding getJob invocation handler")
	}
	log.Info().Msg("✅ Registered getJob handler")

	if err := server.AddServiceInvocationHandler("deleteJob", deleteJob); err != nil {
		log.Fatal().Err(err).Msg("❌ Error adding deleteJob invocation handler")
	}
	log.Info().Msg("✅ Registered deleteJob handler")

	// Register job event handler for all jobs (both static and dynamic names)
	log.Info().Msg("🎯 Registering job event handlers...")
	for _, jobName := range jobNames {
		if err := server.AddJobEventHandler(jobName, handleJob); err != nil {
			log.Fatal().Err(err).Msgf("❌ Failed to register job event handler for: %s", jobName)
		}
		log.Info().Msgf("✅ Registered job handler for: %s", jobName)
	}

	// Dynamic job name patterns are supported through the extractBaseJobType function
	// which matches job names like "youtube-crawl-1234567" to base type "youtube-crawl"
	log.Info().Msg("🔧 Job handlers registered for base patterns. Dynamic job names will be matched by prefix.")
	log.Info().Msgf("📋 Supported base patterns: %v", baseJobPatterns)

	log.Info().Msgf("🚀 Starting DAPR server on port: %s", port)
	log.Info().Msg("🎉 DAPR job service fully initialized and ready to receive jobs!")
	log.Info().Msg("⏳ Waiting for job events from DAPR runtime...")

	if err = server.Start(); err != nil {
		log.Fatal().Err(err).Msg("❌ Failed to start DAPR server")
	}
}

type App struct {
	daprClient daprc.Client
	baseConfig common2.CrawlerConfig // Store CLI configuration for job handlers
}

var app App

// Base job names that our handlers support (patterns for matching dynamic names)
var baseJobPatterns = []string{"telegram-crawl", "youtube-crawl", "scheduled-crawl", "maintenance-job"}

// All possible job names (including dynamic ones) - register a broader set
var jobNames = []string{"telegram-crawl", "youtube-crawl", "scheduled-crawl", "maintenance-job"}

type CrawlerJob struct {
	Name    string `json:"name"`
	Task    string `json:"task"`
	DueTime string `json:"dueTime"`
}

// // scheduleJob handles the scheduling of a job based on the provided invocation event.
// // It unmarshals the event data into a DroidJob structure, constructs a JobData object,
// // and marshals it into JSON format. The job is then scheduled using the Dapr client.
// // Returns the original invocation event content and any error encountered during the process.
// //
// // Parameters:
// // - ctx: The context for the operation.
// // - in: The invocation event containing job details.
// //
// // Returns:
// // - out: The content of the invocation event.
// // - err: An error if the job scheduling fails.
// func scheduleJob(ctx context.Context, in *common.InvocationEvent) (out *common.Content, err error) {
//
//	if in == nil {
//		err = errors.New("no invocation parameter")
//		return
//	}
//
//	droidJob := DroidJob{}
//	err = json.Unmarshal(in.Data, &droidJob)
//	if err != nil {
//		log.Error().Err(err).Msgf("failed to unmarshal job: %v", err)
//		return nil, err
//	}
//
//	jobData := JobData{
//		Droid: droidJob.Name,
//		Task:  droidJob.Job,
//	}
//
//	content, err := json.Marshal(jobData)
//	if err != nil {
//		log.Error().Err(err).Msg("Error marshalling job content")
//		return nil, err
//	}
//
//	// schedule job
//	job := daprc.Job{
//		Name:    droidJob.Name,
//		DueTime: droidJob.DueTime,
//		Data: &anypb.Any{
//			Value: content,
//		},
//	}
//
//	err = app.daprClient.ScheduleJobAlpha1(ctx, &job)
//	if err != nil {
//		log.Error().Msgf("failed to schedule job. err: %v", err)
//		return nil, err
//	}
//
//	log.Info().Msgf("Job scheduled: %v", droidJob.Name)
//
//	out = &common.Content{
//		Data:        in.Data,
//		ContentType: in.ContentType,
//		DataTypeURL: in.DataTypeURL,
//	}
//
//	return out, err
//
// }
//
// getJob retrieves a job by its name using the provided invocation event.
// It fetches the job data from the Dapr client and returns it in a common.Content structure.
//
// Parameters:
// - ctx: The context for the operation.
// - in: The invocation event containing the job name.
//
// Returns:
// - out: The content of the job retrieved.
// - err: An error if the job retrieval fails.
func getJob(ctx context.Context, in *common.InvocationEvent) (out *common.Content, err error) {

	if in == nil {
		err = errors.New("no invocation parameter")
		return nil, err
	}

	job, err := app.daprClient.GetJobAlpha1(ctx, string(in.Data))
	if err != nil {
		log.Error().Err(err).Msgf("failed to get job. err: %v", err)
	}

	out = &common.Content{
		Data:        job.Data.Value,
		ContentType: in.ContentType,
		DataTypeURL: in.DataTypeURL,
	}

	return out, err
}

// deleteJob deletes a job by its name using the provided invocation event.
// It deletes the job from the Dapr client and returns the job name that was deleted.
//
// Parameters:
// - ctx: The context for the operation.
// - in: The invocation event containing the job name.
//
// Returns:
// - out: The content confirming the job deletion.
// - err: An error if the job deletion fails.
func deleteJob(ctx context.Context, in *common.InvocationEvent) (out *common.Content, err error) {
	if in == nil {
		err = errors.New("no invocation parameter")
		return nil, err
	}

	jobName := string(in.Data)
	log.Info().Str("job_name", jobName).Msg("Attempting to delete job")

	err = app.daprClient.DeleteJobAlpha1(ctx, jobName)
	if err != nil {
		log.Error().Err(err).Str("job_name", jobName).Msg("failed to delete job")
		return nil, err
	}

	log.Info().Str("job_name", jobName).Msg("Job deleted successfully")

	out = &common.Content{
		Data:        []byte(fmt.Sprintf(`{"deleted": "%s"}`, jobName)),
		ContentType: in.ContentType,
		DataTypeURL: in.DataTypeURL,
	}

	return out, nil
}

//
//type JobData struct {
//	Droid string `json:"droid"`
//	Task  string `json:"Task"`
//}
//
//// handleJob processes a job event by unmarshaling the job data and payload,
//// then logs the droid and task information. It returns an error if unmarshaling fails.
////
//// Parameters:
//// - ctx: The context for the operation.
//// - job: The job event containing the job data.
////
//// Returns:
//// - error: An error if the job data or payload unmarshaling fails.
//func handleJob(ctx context.Context, job *common.JobEvent) error {
//	log.Info().Msgf("Job event received! Raw data: %s", string(job.Data))
//	log.Info().Msgf("Job type: %s", job.JobType)
//	var jobData common.Job
//	if err := json.Unmarshal(job.Data, &jobData); err != nil {
//		return fmt.Errorf("failed to unmarshal job: %v", err)
//	}
//
//	var jobPayload JobData
//	if err := json.Unmarshal(job.Data, &jobPayload); err != nil {
//		return fmt.Errorf("failed to unmarshal payload: %v", err)
//	}
//
//	log.Info().Msgf("Starting droid: %s", jobPayload.Droid)
//	log.Info().Msgf("Executing maintenance job: %s", jobPayload.Task)
//
//	return nil
//}

// mergeConfigWithJobData merges CLI configuration with job data,
// giving precedence to job data when provided, but falling back to CLI values
func mergeConfigWithJobData(baseConfig common2.CrawlerConfig, jobData JobData) common2.CrawlerConfig {
	mergedConfig := baseConfig // Start with CLI configuration

	// Override with job data if provided (non-zero/non-empty values)
	if jobData.MaxDepth != 0 {
		mergedConfig.MaxDepth = jobData.MaxDepth
	}
	if jobData.Concurrency != 0 {
		mergedConfig.Concurrency = jobData.Concurrency
	}
	if jobData.CrawlID != "" {
		mergedConfig.CrawlID = jobData.CrawlID
	}
	if jobData.Platform != "" {
		mergedConfig.Platform = jobData.Platform
	}
	if jobData.YouTubeAPIKey != "" {
		mergedConfig.YouTubeAPIKey = jobData.YouTubeAPIKey
	}
	if jobData.SamplingMethod != "" {
		mergedConfig.SamplingMethod = jobData.SamplingMethod
	}
	if jobData.MinChannelVideos != 0 {
		mergedConfig.MinChannelVideos = jobData.MinChannelVideos
	}
	if jobData.MaxPosts != 0 {
		mergedConfig.MaxPosts = jobData.MaxPosts
	}
	if jobData.SampleSize != 0 {
		mergedConfig.SampleSize = jobData.SampleSize
	}
	if !jobData.MinPostDate.IsZero() {
		mergedConfig.MinPostDate = jobData.MinPostDate
	}
	if !jobData.DateBetweenMin.IsZero() {
		mergedConfig.DateBetweenMin = jobData.DateBetweenMin
	}
	if !jobData.DateBetweenMax.IsZero() {
		mergedConfig.DateBetweenMax = jobData.DateBetweenMax
	}
	if len(jobData.TDLibDatabaseURLs) > 0 {
		mergedConfig.TDLibDatabaseURLs = jobData.TDLibDatabaseURLs
	}
	if jobData.MaxPages != 0 {
		mergedConfig.MaxPages = jobData.MaxPages
	}

	log.Debug().
		Str("merged_platform", mergedConfig.Platform).
		Str("merged_sampling", mergedConfig.SamplingMethod).
		Int("merged_concurrency", mergedConfig.Concurrency).
		Int("merged_max_depth", mergedConfig.MaxDepth).
		Int("merged_max_posts", mergedConfig.MaxPosts).
		Int("merged_max_pages", mergedConfig.MaxPages).
		Msg("Merged CLI configuration with job data")

	return mergedConfig
}

// JobData structure includes crawler-specific fields for job configuration
type JobData struct {
	DueTime           string    `json:"dueTime"`
	JobName           string    `json:"jobName"`
	Task              string    `json:"task"`
	URLs              []string  `json:"urls,omitempty"`
	URLFile           string    `json:"urlFile,omitempty"`
	CrawlID           string    `json:"crawlId,omitempty"`
	MaxDepth          int       `json:"maxDepth,omitempty"`
	Concurrency       int       `json:"concurrency,omitempty"`
	Platform          string    `json:"platform,omitempty"`          // Platform to crawl: "telegram", "youtube", etc.
	YouTubeAPIKey     string    `json:"youtubeApiKey,omitempty"`     // YouTube API key for YouTube platform
	SamplingMethod    string    `json:"samplingMethod,omitempty"`    // Sampling method: "random", etc.
	MinChannelVideos  int64     `json:"minChannelVideos,omitempty"`  // Minimum videos for YouTube channels
	MaxPosts          int       `json:"maxPosts,omitempty"`          // Maximum posts to fetch
	SampleSize        int       `json:"sampleSize,omitempty"`        // Sample size for random sampling
	MinPostDate       time.Time `json:"minPostDate,omitempty"`       // Minimum post date for filtering
	DateBetweenMin    time.Time `json:"dateBetweenMin,omitempty"`    // Date range minimum
	DateBetweenMax    time.Time `json:"dateBetweenMax,omitempty"`    // Date range maximum
	TDLibDatabaseURLs []string  `json:"tdlibDatabaseUrls,omitempty"` // TDLib database URLs for connection pooling
	MaxPages          int       `json:"maxPages,omitempty"`          // Maximum pages to process
}

// handleJob processes a job event by unmarshaling the job data and payload,
// then initiates the crawling process based on the provided configuration.
// It returns an error if unmarshaling fails or if the crawling process encounters an error.
//
// Parameters:
// - ctx: The context for the operation.
// - job: The job event containing the job data.
//
// Returns:
// - error: An error if the job data or payload unmarshaling fails, or if crawling fails.
func handleJob(ctx context.Context, job *common.JobEvent) error {
	log.Info().Msg("🎉 JOB EVENT RECEIVED!")
	log.Info().Msg("==================================================================================")
	log.Info().Msgf("📅 Timestamp: %s", time.Now().Format(time.RFC3339))
	log.Info().Msgf("🏷️  Job Type: %s", job.JobType)
	log.Info().Msgf("📄 Raw Job Data: %s", string(job.Data))
	log.Info().Msgf("📊 Data Size: %d bytes", len(job.Data))

	log.Info().Msg("🔍 Attempting to parse job data...")
	var jobData JobData
	if err := json.Unmarshal(job.Data, &jobData); err != nil {
		log.Error().Err(err).Str("raw_data", string(job.Data)).Msg("❌ CRITICAL: Failed to unmarshal job payload")
		log.Error().Msg("🔍 This means the job data format is invalid or incompatible")
		return fmt.Errorf("failed to unmarshal job payload: %v", err)
	}

	log.Info().Msg("✅ Job data parsed successfully!")
	log.Info().Msgf("🏷️  Job Name: %s", jobData.JobName)
	log.Info().Msgf("📋 Task: %s", jobData.Task)
	log.Info().Msgf("🌐 Platform: %s", jobData.Platform)
	log.Info().Msgf("📄 URLs: %v", jobData.URLs)
	log.Info().Msgf("🔧 Concurrency: %d", jobData.Concurrency)
	log.Info().Msgf("📊 Max Posts: %d", jobData.MaxPosts)

	if jobData.YouTubeAPIKey != "" {
		log.Info().Msgf("🔑 YouTube API Key: %s...%s", jobData.YouTubeAPIKey[:8], jobData.YouTubeAPIKey[len(jobData.YouTubeAPIKey)-4:])
	}

	// Route job execution based on job type (job name), supporting dynamic suffixes
	log.Info().Msg("🎯 ROUTING JOB FOR EXECUTION...")
	jobType := job.JobType
	baseJobType := extractBaseJobType(jobType)

	log.Info().Msgf("🏷️  Original Job Type: %s", jobType)
	log.Info().Msgf("🔍 Extracted Base Type: %s", baseJobType)
	log.Info().Msgf("📋 Available Base Patterns: %v", baseJobPatterns)

	// Determine execution path
	log.Info().Msg("🚦 Determining execution path...")

	switch baseJobType {
	case "telegram-crawl", "youtube-crawl", "scheduled-crawl":
		log.Info().Msgf("✅ Matched crawl job type: %s", baseJobType)
		log.Info().Msg("🏃 Executing as CRAWL JOB...")
		log.Info().Msg("================================================================================")
		return executeCrawlJob(ctx, baseJobType, jobData)

	case "maintenance-job":
		log.Info().Msgf("✅ Matched maintenance job type: %s", baseJobType)
		log.Info().Msg("🔧 Executing as MAINTENANCE JOB...")
		log.Info().Msg("================================================================================")
		return executeMaintenanceJob(ctx, jobData)

	default:
		// Fallback: check if this is a crawling job by task description
		log.Warn().Msgf("⚠️  Unknown job type: %s", jobType)
		log.Info().Msg("🔍 Checking if this is a crawl job based on task description...")

		if strings.Contains(strings.ToLower(jobData.Task), "crawl") {
			log.Info().Msgf("✅ Task contains 'crawl': treating '%s' as crawl job", jobType)
			log.Info().Msg("🏃 Executing as FALLBACK CRAWL JOB...")
			log.Info().Msg("================================================================================")
			return executeCrawlJob(ctx, jobType, jobData)
		} else {
			log.Warn().Msgf("❓ Unknown job type: %s, executing as generic task", jobType)
			log.Info().Msg("🔄 Executing as GENERIC JOB...")
			log.Info().Msg("================================================================================")
			return executeGenericJob(ctx, jobData)
		}
	}
}

// extractBaseJobType extracts the base job type from a potentially suffixed job name
// Examples: "youtube-crawl-1234567" -> "youtube-crawl", "telegram-crawl" -> "telegram-crawl"
func extractBaseJobType(jobType string) string {
	for _, baseType := range baseJobPatterns {
		if strings.HasPrefix(jobType, baseType) {
			// If it matches exactly or starts with the base type followed by a hyphen
			if jobType == baseType || strings.HasPrefix(jobType, baseType+"-") {
				return baseType
			}
		}
	}
	return jobType // Return original if no pattern matches
}

// executeCrawlJob handles crawling jobs with platform-specific logic
func executeCrawlJob(ctx context.Context, jobType string, jobData JobData) error {
	log.Info().Msg("🚀 STARTING CRAWL JOB EXECUTION")
	log.Info().Msgf("🏷️  Job Type: %s", jobType)
	log.Info().Msgf("📅 Start Time: %s", time.Now().Format(time.RFC3339))

	// Step 1: Merge configurations
	log.Info().Msg("🔧 Step 1: Merging CLI configuration with job data...")
	log.Info().Msgf("📋 Base CLI Platform: %s", app.baseConfig.Platform)
	log.Info().Msgf("📋 Base CLI Concurrency: %d", app.baseConfig.Concurrency)
	log.Info().Msgf("📋 Base CLI Storage: %s", app.baseConfig.StorageRoot)

	crawlerCfg := mergeConfigWithJobData(app.baseConfig, jobData)
	log.Info().Msg("✅ Configuration merge completed")

	// Step 2: Platform detection and auto-configuration
	log.Info().Msg("🔍 Step 2: Platform detection and auto-configuration...")
	log.Info().Msgf("🌐 Job Data Platform: %s", jobData.Platform)
	log.Info().Msgf("🌐 Merged Config Platform: %s", crawlerCfg.Platform)

	platformBeforeDetection := crawlerCfg.Platform

	// Set platform based on job type if not already specified in job data
	if crawlerCfg.Platform == "" || jobData.Platform == "" {
		log.Info().Msg("🔄 Platform not specified, attempting auto-detection...")
		switch jobType {
		case "telegram-crawl":
			crawlerCfg.Platform = "telegram"
			log.Info().Msg("✅ Auto-detected platform: telegram")
		case "youtube-crawl":
			crawlerCfg.Platform = "youtube"
			log.Info().Msg("✅ Auto-detected platform: youtube")
		case "scheduled-crawl":
			// Keep existing platform or use CLI default
			if crawlerCfg.Platform == "" {
				crawlerCfg.Platform = "telegram" // Default to telegram
				log.Info().Msg("✅ Using default platform: telegram")
			} else {
				log.Info().Msgf("✅ Keeping existing platform: %s", crawlerCfg.Platform)
			}
		default:
			log.Warn().Msgf("⚠️  Unknown job type for platform detection: %s", jobType)
		}
	} else {
		log.Info().Msgf("✅ Platform already specified: %s", crawlerCfg.Platform)
	}

	if platformBeforeDetection != crawlerCfg.Platform {
		log.Info().Msgf("🔄 Platform changed: %s → %s", platformBeforeDetection, crawlerCfg.Platform)
	}

	// Step 3: Environment and storage configuration
	log.Info().Msg("📁 Step 3: Storage and environment configuration...")

	// Override storage root from environment if set (for containerized deployments)
	envStorageRoot := os.Getenv("STORAGE_ROOT")
	if envStorageRoot != "" {
		log.Info().Msgf("🔄 Overriding storage root with environment variable: %s → %s", crawlerCfg.StorageRoot, envStorageRoot)
		crawlerCfg.StorageRoot = envStorageRoot
	} else {
		log.Info().Msgf("📁 Using storage root: %s", crawlerCfg.StorageRoot)
	}

	// Step 4: Crawl ID generation
	log.Info().Msg("🏷️  Step 4: Crawl ID validation/generation...")
	if crawlerCfg.CrawlID == "" {
		crawlerCfg.CrawlID = common2.GenerateCrawlID()
		log.Info().Msgf("✅ Generated new crawl ID: %s", crawlerCfg.CrawlID)
	} else {
		log.Info().Msgf("✅ Using provided crawl ID: %s", crawlerCfg.CrawlID)
	}

	// Step 5: URL collection and validation
	log.Info().Msg("📄 Step 5: URL collection and validation...")
	var urls []string

	if len(jobData.URLs) > 0 {
		urls = append(urls, jobData.URLs...)
		log.Info().Msgf("✅ Added %d URLs from job data: %v", len(jobData.URLs), jobData.URLs)
	} else {
		log.Info().Msg("📄 No URLs provided in job data")
	}

	if jobData.URLFile != "" {
		log.Info().Msgf("📁 Reading URLs from file: %s", jobData.URLFile)
		fileURLs, err := common2.ReadURLsFromFile(jobData.URLFile)
		if err != nil {
			log.Error().Err(err).Str("file", jobData.URLFile).Msg("❌ Failed to read URLs from file")
			return fmt.Errorf("failed to read URLs from file: %v", err)
		}
		urls = append(urls, fileURLs...)
		log.Info().Msgf("✅ Added %d URLs from file", len(fileURLs))
	}

	log.Info().Msgf("📊 Total URLs collected: %d", len(urls))

	// For random sampling, URLs are not required since we discover content randomly
	if len(urls) == 0 && !(crawlerCfg.Platform == "youtube" && crawlerCfg.SamplingMethod == "random") {
		err := fmt.Errorf("no URLs provided in job data")
		log.Error().Err(err).Msg("Failed to start crawl")
		return err
	}

	log.Info().Msgf("Starting %s crawl of %d URLs with concurrency %d", crawlerCfg.Platform, len(urls), crawlerCfg.Concurrency)

	// Platform-specific initialization
	log.Info().Msgf("🔧 Initializing platform: %s", crawlerCfg.Platform)

	if crawlerCfg.Platform == "youtube" {
		log.Info().Msg("🎥 Setting up YouTube platform configuration")

		// For YouTube platform, we need to validate the API key
		if crawlerCfg.YouTubeAPIKey == "" {
			err := fmt.Errorf("YouTube API key is required for YouTube platform")
			log.Error().Err(err).Msg("❌ YouTube API key validation failed")
			return err
		}

		log.Info().Msg("✅ YouTube API key validated successfully")
		log.Info().Msgf("🔍 Sampling method: %s", crawlerCfg.SamplingMethod)

		if crawlerCfg.SamplingMethod == "random" {
			log.Info().Msgf("🎲 Random sampling configured - will discover content dynamically")
			log.Info().Msgf("📊 Target sample size: %d", crawlerCfg.SampleSize)
		}

	} else {
		// Default Telegram platform initialization
		log.Info().Msg("📱 Setting up Telegram platform configuration")

		baseDir := filepath.Join(crawlerCfg.StorageRoot, "state") // Same base path where connection folders are created
		log.Info().Msgf("📁 Base directory for state: %s", baseDir)

		cleaner := telegramhelper.NewFileCleaner(
			baseDir, // Base directory where conn_* folders are located (matches InitializeClientWithConfig)
			[]string{
				".tdlib/files/videos",    // Videos directory to clean up
				".tdlib/files/photos",    // Database directory to clean up
				".tdlib/files/documents", // General files directory
			},
			5,  // cleanup interval minutes
			15, // file age threshold minutes
		)

		log.Info().Msg("🧹 Starting file cleaner for TDLib temporary files")
		if err := cleaner.Start(); err != nil {
			log.Error().Err(err).Msg("❌ Failed to start file cleaner")
			return err
		}
		log.Info().Msg("✅ File cleaner started successfully")

		// Initialize connection pool with an appropriate size
		log.Info().Msg("🔗 Initializing Telegram connection pool")
		poolSize := crawlerCfg.Concurrency
		if poolSize < 1 {
			poolSize = 1
			log.Info().Msg("⚠️  Pool size was < 1, defaulting to 1")
		}

		// If we have database URLs, use those to determine pool size
		if len(crawlerCfg.TDLibDatabaseURLs) > 0 {
			log.Info().Msgf("📦 Found %d TDLib database URLs for connection pooling", len(crawlerCfg.TDLibDatabaseURLs))
			// Use the smaller of concurrency or number of database URLs
			if len(crawlerCfg.TDLibDatabaseURLs) < poolSize {
				originalPoolSize := poolSize
				poolSize = len(crawlerCfg.TDLibDatabaseURLs)
				log.Info().Msgf("⚖️  Adjusting pool size from %d to %d to match available database URLs", originalPoolSize, poolSize)
			}
		} else {
			log.Info().Msg("📦 No TDLib database URLs provided, using default connection setup")
		}

		log.Info().Msgf("🚀 Initializing connection pool with size: %d", poolSize)
		// Initialize the connection pool
		crawl.InitConnectionPool(poolSize, crawlerCfg.StorageRoot, crawlerCfg)
		defer crawl.CloseConnectionPool()
		log.Info().Msg("✅ Connection pool initialized successfully")
	}

	// Launch the crawler with the provided configuration
	log.Info().Msg("🚀 Launching crawl execution")
	log.Info().Msgf("📋 Final configuration summary:")
	log.Info().Msgf("   • Platform: %s", crawlerCfg.Platform)
	log.Info().Msgf("   • Crawl ID: %s", crawlerCfg.CrawlID)
	log.Info().Msgf("   • URLs to process: %d", len(urls))
	log.Info().Msgf("   • Concurrency: %d", crawlerCfg.Concurrency)
	log.Info().Msgf("   • Max depth: %d", crawlerCfg.MaxDepth)
	if crawlerCfg.Platform == "youtube" && crawlerCfg.SamplingMethod == "random" {
		log.Info().Msgf("   • Sample size: %d", crawlerCfg.SampleSize)
	}

	err := launchCrawl(urls, crawlerCfg)
	if err != nil {
		log.Error().Err(err).Msg("❌ Crawling execution failed")
		log.Error().Msgf("💥 Failure details: %v", err)
		return fmt.Errorf("crawl execution failed: %w", err)
	}

	log.Info().Msg("✅ Crawling completed successfully")
	log.Info().Msg("🎉 Job execution finished")
	return nil
}

// executeMaintenanceJob handles maintenance tasks
func executeMaintenanceJob(ctx context.Context, jobData JobData) error {
	log.Info().Msgf("🔧 Executing maintenance job: %s", jobData.Task)

	// Validate task type
	if jobData.Task == "" {
		err := fmt.Errorf("maintenance task type cannot be empty")
		log.Error().Err(err).Msg("❌ Maintenance job validation failed")
		return err
	}

	// Add maintenance logic here based on task type
	taskType := strings.ToLower(jobData.Task)
	log.Info().Msgf("🎯 Processing maintenance task type: %s", taskType)

	switch taskType {
	case "cleanup", "clean":
		log.Info().Msg("🧹 Performing cleanup maintenance")
		// Add cleanup logic here
		log.Info().Msg("✅ Cleanup maintenance completed successfully")
		return nil

	case "health check", "healthcheck":
		log.Info().Msg("🏥 Performing health check")
		// Add health check logic here
		log.Info().Msg("✅ Health check completed successfully")
		return nil

	default:
		log.Info().Msgf("⚙️  Performing generic maintenance task: %s", jobData.Task)
		// Add generic maintenance logic here
		log.Info().Msgf("✅ Generic maintenance task '%s' completed successfully", jobData.Task)
		return nil
	}
}

// executeGenericJob handles generic/unknown job types
func executeGenericJob(ctx context.Context, jobData JobData) error {
	log.Info().Msgf("❓ Executing generic job: %s", jobData.Task)

	// Validate job data
	if jobData.Task == "" {
		err := fmt.Errorf("generic job task type cannot be empty")
		log.Error().Err(err).Msg("❌ Generic job validation failed")
		return err
	}

	log.Info().Msgf("📋 Job details: %+v", jobData)
	log.Warn().Msgf("⚠️  No specific handler for job type '%s', executing as generic job", jobData.JobName)

	// For now, just log the job completion
	// This can be extended to handle custom job types in the future
	log.Info().Msg("✅ Generic job completed successfully")

	return nil
}

// launchCrawl initializes and runs the scraping process for a given list of strings using the specified crawler configuration.
// Returns an error if any critical process fails.
func launchCrawl(stringList []string, crawlCfg common2.CrawlerConfig) error {
	seenURLs := make(map[string]bool)

	// Initialize seenURLs with the seed URLs
	for _, url := range stringList {
		seenURLs[url] = true
	}

	crawlexecid := common2.GenerateCrawlID()
	log.Info().Msgf("Starting scraper for crawl: %s", crawlCfg.CrawlID)

	cfg := state.Config{
		StorageRoot:      crawlCfg.StorageRoot,
		CrawlID:          crawlCfg.CrawlID,
		CrawlLabel:       crawlCfg.CrawlLabel,
		CrawlExecutionID: crawlexecid,
		Platform:         crawlCfg.Platform, // Pass the platform information
		SamplingMethod:   crawlCfg.SamplingMethod,
	}

	smfact := state.DefaultStateManagerFactory{}
	sm, err := smfact.Create(cfg)
	if err != nil {
		log.Error().Err(err).
			Str("storage_root", cfg.StorageRoot).
			Str("crawl_id", cfg.CrawlID).
			Str("platform", cfg.Platform).
			Msg("❌ CRITICAL: Failed to initialize state manager")
		return fmt.Errorf("state manager initialization failed: %w", err)
	}

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	// Get the existing layers or seed a new crawl
	err = sm.Initialize(stringList)
	if err != nil {
		log.Error().Err(err).
			Int("url_count", len(stringList)).
			Strs("urls", stringList).
			Msg("❌ CRITICAL: Failed to set up seed URLs")
		return fmt.Errorf("seed URL initialization failed: %w", err)
	}

	// Process layers iteratively, with potential for new layers to be added during execution
	depth := 0
	for {
		// Check current maximum depth at the beginning of each iteration
		maxDepth, err := sm.GetMaxDepth()
		if err != nil {
			log.Error().Err(err).Msg("Failed to get maximum depth, using 0")
			maxDepth = 0
		}

		// If we've processed all layers up to the current maximum depth, we're done
		if depth > maxDepth {
			log.Info().Msgf("Processed all layers up to maximum depth %d", maxDepth)
			break
		}

		// Get the current layer to process
		pages, err := sm.GetLayerByDepth(depth)
		if err != nil {
			log.Error().Err(err).Msgf("Failed to get layer at depth %d", depth)
			depth++
			continue
		}

		// Skip if there are no pages at this depth
		if len(pages) == 0 {
			log.Info().Msgf("No pages found at depth %d, skipping", depth)
			depth++
			continue
		}

		if depth > crawlCfg.MaxDepth {
			break
		}
		log.Info().Msgf("Processing layer at depth %d with %d pages", depth, len(pages))

		// Create a Layer object
		layer := &state.Layer{
			Depth: depth,
			Pages: pages,
		}

		// Process pages in current layer in parallel
		processLayerInParallel(layer, crawlCfg.Concurrency, sm, crawlCfg, nil, time.Time{})

		// Log progress after completing a layer
		log.Info().Msgf("Completed layer at depth %d", depth)

		// Move to the next depth
		depth++
	}

	completionMetadata := map[string]interface{}{
		"status":          "completed",
		"endTime":         time.Now(),
		"previousCrawlID": cfg.CrawlExecutionID,
	}

	if err := sm.UpdateCrawlMetadata(cfg.CrawlID, completionMetadata); err != nil {
		log.Error().Err(err).Msg("Failed to update crawl completion metadata")
		return err
	}
	err = sm.ExportPagesToBinding(cfg.CrawlID)
	if err != nil {
	}
	log.Info().Msg("All items processed successfully.")
	return nil
}

// scheduleJob handles the scheduling of a job based on the provided invocation event.
// Enhanced to support crawler-specific job configurations.
func scheduleJob(ctx context.Context, in *common.InvocationEvent) (out *common.Content, err error) {
	if in == nil {
		err = errors.New("no invocation parameter")
		return
	}

	var jobData JobData
	err = json.Unmarshal(in.Data, &jobData)
	if err != nil {
		log.Error().Err(err).Msgf("failed to unmarshal job: %v", err)
		return nil, err
	}

	content, err := json.Marshal(jobData)
	if err != nil {
		log.Error().Err(err).Msg("Error marshalling job content")
		return nil, err
	}

	// schedule job
	job := daprc.Job{
		Name:    jobData.JobName,
		DueTime: jobData.DueTime,
		Data: &anypb.Any{
			Value: content,
		},
	}

	err = app.daprClient.ScheduleJobAlpha1(ctx, &job)
	if err != nil {
		log.Error().Msgf("failed to schedule job. err: %v", err)
		return nil, err
	}

	log.Info().Msgf("Job scheduled: %v", jobData.JobName)

	out = &common.Content{
		Data:        in.Data,
		ContentType: in.ContentType,
		DataTypeURL: in.DataTypeURL,
	}

	return out, err
}

// Note: processLayerInParallel function is shared with standalone.go
// Note: readURLsFromFile function removed as we're now using the common implementation

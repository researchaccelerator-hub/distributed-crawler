package dapr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	daprc "github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"
	daprs "github.com/dapr/go-sdk/service/grpc"
	common2 "github.com/researchaccelerator-hub/telegram-scraper/common"
	"github.com/researchaccelerator-hub/telegram-scraper/state"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/types/known/anypb"
	"os"
	"strings"
	"time"
)

// StartDaprMode initializes and starts a Dapr service in job mode using the provided
// crawler configuration. It sets up service invocation handlers for scheduling and
// retrieving jobs, and registers job event handlers for predefined job names. The
// service listens for Dapr requests on the specified port and logs relevant information
// and errors during the process.
func StartDaprMode(crawlerCfg common2.CrawlerConfig) {
	log.Info().Msg("Starting crawler in DAPR job mode")
	log.Printf("Listening on port %d for DAPR requests", crawlerCfg.DaprPort)

	//Create new Dapr client
	daprClient, err := daprc.NewClient()
	if err != nil {
		panic(err)
	}
	defer daprClient.Close()

	app = App{
		daprClient: daprClient,
	}

	// Create a new Dapr service
	port := fmt.Sprintf(":%d", crawlerCfg.DaprPort)
	server, err := daprs.NewService(port)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to start server: %v")
	}

	// Creates handlers for the service
	if err := server.AddServiceInvocationHandler("scheduleJob", scheduleJob); err != nil {
		log.Fatal().Err(err).Msg("error adding invocation handler")
	}

	if err := server.AddServiceInvocationHandler("getJob", getJob); err != nil {
		log.Fatal().Err(err).Msg("error adding invocation handler")
	}

	//if err := server.AddServiceInvocationHandler("deleteJob", deleteJob); err != nil {
	//	log.Fatal().Err(err).Msg("error adding invocation handler: %v", err)
	//}

	// Register job event handler for all jobs
	for _, jobName := range jobNames {
		if err := server.AddJobEventHandler(jobName, handleJob); err != nil {
			log.Fatal().Err(err).Msg("failed to register job event handler")
		}
		log.Info().Msgf("Registered job handler for: %s", jobName)
	}

	log.Info().Msgf("Starting server on port: %s", port)
	if err = server.Start(); err != nil {
		log.Fatal().Err(err).Msg("failed to start server")
	}
}

type App struct {
	daprClient daprc.Client
}

var app App

var jobNames = []string{"R2-D2", "C-3PO", "BB-8", "my-scheduled-job"}

type DroidJob struct {
	Name    string `json:"name"`
	Job     string `json:"job"`
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

// Modified JobData structure to include crawler-specific fields
type JobData struct {
	DueTime     string   `json:"dueTime"`
	Droid       string   `json:"droid"`
	Task        string   `json:"task"`
	URLs        []string `json:"urls,omitempty"`
	URLFile     string   `json:"urlFile,omitempty"`
	CrawlID     string   `json:"crawlId,omitempty"`
	MaxDepth    int      `json:"maxDepth,omitempty"`
	Concurrency int      `json:"concurrency,omitempty"`
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
	log.Info().Msgf("Job event received! Raw data: %s", string(job.Data))
	log.Info().Msgf("Job type: %s", job.JobType)

	var jobData JobData
	if err := json.Unmarshal(job.Data, &jobData); err != nil {
		return fmt.Errorf("failed to unmarshal job payload: %v", err)
	}

	log.Info().Msgf("Starting droid: %s", jobData.Droid)
	log.Info().Msgf("Executing task: %s", jobData.Task)

	// Check if this is a crawling job
	if strings.Contains(strings.ToLower(jobData.Task), "crawl") {
		// Set up crawler configuration using job data
		crawlerCfg := common2.CrawlerConfig{
			StorageRoot: os.Getenv("STORAGE_ROOT"), // Get from environment or job data
			MaxDepth:    jobData.MaxDepth,
			Concurrency: jobData.Concurrency,
			CrawlID:     jobData.CrawlID,
		}

		// If CrawlID not provided, generate one
		if crawlerCfg.CrawlID == "" {
			crawlerCfg.CrawlID = common2.GenerateCrawlID()
		}

		// Collect URLs from job data or file
		var urls []string

		if len(jobData.URLs) > 0 {
			urls = append(urls, jobData.URLs...)
		}

		if jobData.URLFile != "" {
			fileURLs, err := readURLsFromFile(jobData.URLFile)
			if err != nil {
				log.Error().Err(err).Msg("Failed to read URLs from file")
				return err
			}
			urls = append(urls, fileURLs...)
		}

		if len(urls) == 0 {
			err := fmt.Errorf("no URLs provided in job data")
			log.Error().Err(err).Msg("Failed to start crawl")
			return err
		}

		log.Info().Msgf("Starting crawl of %d URLs with concurrency %d", len(urls), crawlerCfg.Concurrency)

		// Launch the crawler with the provided configuration
		err := launchCrawl(urls, crawlerCfg)
		if err != nil {
			log.Error().Err(err).Msg("Crawling failed")
			return err
		}

		log.Info().Msg("Crawling completed successfully")
		return nil
	}

	log.Info().Msgf("Executed job: %s / %s", jobData.Droid, jobData.Task)
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
		CrawlExecutionID: crawlexecid,
	}

	smfact := state.DefaultStateManagerFactory{}
	sm, err := smfact.Create(cfg)
	if err != nil {
		log.Error().Err(err).Msg("Failed to initialize state manager")
		return err
	}

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	// Get the existing layers or seed a new crawl
	err = sm.Initialize(stringList)
	if err != nil {
		log.Error().Err(err).Msg("Failed to set up seed URLs")
		return err
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
		processLayerInParallel(layer, crawlCfg.Concurrency, sm, crawlCfg)

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
		Name:    jobData.Droid,
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

	log.Info().Msgf("Job scheduled: %v", jobData.Droid)

	out = &common.Content{
		Data:        in.Data,
		ContentType: in.ContentType,
		DataTypeURL: in.DataTypeURL,
	}

	return out, err
}

//
//// Helper function to read URLs from a file
//func readURLsFromFile(filename string) ([]string, error) {
//	data, err := os.ReadFile(filename)
//	if err != nil {
//		return nil, err
//	}
//
//	lines := strings.Split(string(data), "\n")
//	var urls []string
//
//	for _, line := range lines {
//		line = strings.TrimSpace(line)
//		if line != "" && !strings.HasPrefix(line, "#") {
//			urls = append(urls, line)
//		}
//	}
//
//	return urls, nil
//}

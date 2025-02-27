package dapr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	daprc "github.com/dapr/go-sdk/client"
	"github.com/dapr/go-sdk/service/common"
	daprs "github.com/dapr/go-sdk/service/grpc"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/types/known/anypb"
	common2 "tdlib-scraper/common"
)

// Start the crawler in DAPR job mode
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
	server, err := daprs.NewService(":" + port)
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
		fmt.Println("Registered job handler for: ", jobName)
	}

	fmt.Println("Starting server on port: " + port)
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

func scheduleJob(ctx context.Context, in *common.InvocationEvent) (out *common.Content, err error) {

	if in == nil {
		err = errors.New("no invocation parameter")
		return
	}

	droidJob := DroidJob{}
	err = json.Unmarshal(in.Data, &droidJob)
	if err != nil {
		fmt.Println("failed to unmarshal job: ", err)
		return nil, err
	}

	jobData := JobData{
		Droid: droidJob.Name,
		Task:  droidJob.Job,
	}

	content, err := json.Marshal(jobData)
	if err != nil {
		fmt.Printf("Error marshalling job content")
		return nil, err
	}

	// schedule job
	job := daprc.Job{
		Name:    droidJob.Name,
		DueTime: droidJob.DueTime,
		Data: &anypb.Any{
			Value: content,
		},
	}

	err = app.daprClient.ScheduleJobAlpha1(ctx, &job)
	if err != nil {
		fmt.Println("failed to schedule job. err: ", err)
		return nil, err
	}

	fmt.Println("Job scheduled: ", droidJob.Name)

	out = &common.Content{
		Data:        in.Data,
		ContentType: in.ContentType,
		DataTypeURL: in.DataTypeURL,
	}

	return out, err

}

// Handler that gets a job by name
func getJob(ctx context.Context, in *common.InvocationEvent) (out *common.Content, err error) {

	if in == nil {
		err = errors.New("no invocation parameter")
		return nil, err
	}

	job, err := app.daprClient.GetJobAlpha1(ctx, string(in.Data))
	if err != nil {
		fmt.Println("failed to get job. err: ", err)
	}

	out = &common.Content{
		Data:        job.Data.Value,
		ContentType: in.ContentType,
		DataTypeURL: in.DataTypeURL,
	}

	return out, err
}

type JobData struct {
	Droid string `json:"droid"`
	Task  string `json:"Task"`
}

func handleJob(ctx context.Context, job *common.JobEvent) error {
	fmt.Println("Job event received! Raw data:", string(job.Data))
	fmt.Println("Job type:", job.JobType)
	var jobData common.Job
	if err := json.Unmarshal(job.Data, &jobData); err != nil {
		return fmt.Errorf("failed to unmarshal job: %v", err)
	}

	var jobPayload JobData
	if err := json.Unmarshal(job.Data, &jobPayload); err != nil {
		return fmt.Errorf("failed to unmarshal payload: %v", err)
	}

	fmt.Println("Starting droid:", jobPayload.Droid)
	fmt.Println("Executing maintenance job:", jobPayload.Task)

	return nil
}

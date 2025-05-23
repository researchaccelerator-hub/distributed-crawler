package dapr

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

// TestJobHandling tests that job data is properly parsed and validated
func TestJobHandling(t *testing.T) {
	// Test proper job data
	t.Run("Valid Job Data", func(t *testing.T) {
		jobData := JobData{
			DueTime:     "1h",
			Droid:       "R2-D2",
			Task:        "crawl",
			URLs:        []string{"https://t.me/channel1"},
			CrawlID:     "test-crawl-id",
			MaxDepth:    2,
			Concurrency: 3,
		}

		jobDataBytes, err := json.Marshal(jobData)
		assert.NoError(t, err, "Should marshal job data without error")
		assert.NotNil(t, jobDataBytes, "Serialized job data should not be nil")

		// Unmarshal to verify roundtrip
		var parsedJobData JobData
		err = json.Unmarshal(jobDataBytes, &parsedJobData)
		assert.NoError(t, err, "Should unmarshal job data without error")
		
		// Verify all fields are preserved
		assert.Equal(t, jobData.DueTime, parsedJobData.DueTime)
		assert.Equal(t, jobData.Droid, parsedJobData.Droid)
		assert.Equal(t, jobData.Task, parsedJobData.Task)
		assert.Equal(t, jobData.URLs, parsedJobData.URLs)
		assert.Equal(t, jobData.CrawlID, parsedJobData.CrawlID)
		assert.Equal(t, jobData.MaxDepth, parsedJobData.MaxDepth)
		assert.Equal(t, jobData.Concurrency, parsedJobData.Concurrency)
	})

	// Test with URL file instead of direct URLs
	t.Run("With URL File", func(t *testing.T) {
		jobData := JobData{
			DueTime:     "1h", 
			Droid:       "R2-D2",
			Task:        "crawl",
			URLFile:     "/tmp/test-urls.txt",
			CrawlID:     "test-crawl-id",
			MaxDepth:    2,
			Concurrency: 3,
		}

		jobDataBytes, err := json.Marshal(jobData)
		assert.NoError(t, err, "Should marshal job data without error")
		assert.NotNil(t, jobDataBytes, "Serialized job data should not be nil")
		
		// Unmarshal to verify roundtrip
		var parsedJobData JobData
		err = json.Unmarshal(jobDataBytes, &parsedJobData)
		assert.NoError(t, err, "Should unmarshal job data without error")
		
		// Verify URL file field is preserved
		assert.Equal(t, jobData.URLFile, parsedJobData.URLFile)
	})
}
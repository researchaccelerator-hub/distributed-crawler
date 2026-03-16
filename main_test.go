package main

import (
	"testing"

	"github.com/researchaccelerator-hub/telegram-scraper/common"
)

func TestValidateSamplingMethod(t *testing.T) {
	tests := []struct {
		name          string
		in            common.SamplingValidationInput
		expectError   bool
		errorContains string
	}{
		{
			name:        "telegram channel sampling with URLs",
			in:          common.SamplingValidationInput{Platform: "telegram", SamplingMethod: "channel", URLList: []string{"https://t.me/test"}},
			expectError: false,
		},
		{
			name:        "telegram snowball sampling with URLs",
			in:          common.SamplingValidationInput{Platform: "telegram", SamplingMethod: "snowball", URLList: []string{"https://t.me/test1", "https://t.me/test2"}},
			expectError: false,
		},
		{
			name:          "telegram random sampling - unsupported",
			in:            common.SamplingValidationInput{Platform: "telegram", SamplingMethod: "random"},
			expectError:   true,
			errorContains: "not supported for platform 'telegram'",
		},
		{
			name:        "youtube channel sampling with URLs",
			in:          common.SamplingValidationInput{Platform: "youtube", SamplingMethod: "channel", URLList: []string{"https://youtube.com/c/test"}},
			expectError: false,
		},
		{
			name:        "youtube random sampling without URLs",
			in:          common.SamplingValidationInput{Platform: "youtube", SamplingMethod: "random"},
			expectError: false,
		},
		{
			name:        "youtube snowball sampling with URLs",
			in:          common.SamplingValidationInput{Platform: "youtube", SamplingMethod: "snowball", URLList: []string{"https://youtube.com/c/seed"}},
			expectError: false,
		},
		{
			name:          "channel sampling without URLs",
			in:            common.SamplingValidationInput{Platform: "youtube", SamplingMethod: "channel"},
			expectError:   true,
			errorContains: "channel sampling requires URLs",
		},
		{
			name:          "snowball sampling without URLs",
			in:            common.SamplingValidationInput{Platform: "telegram", SamplingMethod: "snowball"},
			expectError:   true,
			errorContains: "snowball sampling requires URLs",
		},
		{
			name:        "channel sampling with URL file",
			in:          common.SamplingValidationInput{Platform: "youtube", SamplingMethod: "channel", URLFile: "/path/to/urls.txt"},
			expectError: false,
		},
		{
			name:          "unsupported platform",
			in:            common.SamplingValidationInput{Platform: "unsupported", SamplingMethod: "channel", URLList: []string{"https://example.com"}},
			expectError:   true,
			errorContains: "unsupported platform",
		},
		{
			name:          "invalid sampling method",
			in:            common.SamplingValidationInput{Platform: "youtube", SamplingMethod: "invalid", URLList: []string{"https://youtube.com/c/test"}},
			expectError:   true,
			errorContains: "not supported for platform 'youtube'",
		},
		// random-walk cases
		{
			name:        "random-walk with seed size only",
			in:          common.SamplingValidationInput{Platform: "telegram", SamplingMethod: "random-walk", SeedSize: 100, CrawlID: "my-crawl"},
			expectError: false,
		},
		{
			name:        "random-walk with URL list only",
			in:          common.SamplingValidationInput{Platform: "telegram", SamplingMethod: "random-walk", URLList: []string{"chan1", "chan2"}, CrawlID: "my-crawl"},
			expectError: false,
		},
		{
			name:        "random-walk with url-file-url only",
			in:          common.SamplingValidationInput{Platform: "telegram", SamplingMethod: "random-walk", URLFileURL: "https://example.com/seeds.txt", CrawlID: "my-crawl"},
			expectError: false,
		},
		{
			name:          "random-walk with neither URLs nor seed size",
			in:            common.SamplingValidationInput{Platform: "telegram", SamplingMethod: "random-walk", CrawlID: "my-crawl"},
			expectError:   true,
			errorContains: "must provide either seed urls or seed size",
		},
		{
			name:          "random-walk with both URLs and seed size",
			in:            common.SamplingValidationInput{Platform: "telegram", SamplingMethod: "random-walk", URLList: []string{"chan1"}, SeedSize: 100, CrawlID: "my-crawl"},
			expectError:   true,
			errorContains: "must provide either seed urls or seed size",
		},
		{
			name:          "random-walk crawl ID too long",
			in:            common.SamplingValidationInput{Platform: "telegram", SamplingMethod: "random-walk", SeedSize: 100, CrawlID: "this-crawl-id-is-way-too-long-and-exceeds-32-chars"},
			expectError:   true,
			errorContains: "crawl IDs cannot exceed 32 characters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSamplingMethod(tt.in)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
					return
				}
				if tt.errorContains != "" && !containsString(err.Error(), tt.errorContains) {
					t.Errorf("Expected error to contain '%s', but got: %s", tt.errorContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error but got: %s", err.Error())
				}
			}
		})
	}
}

func TestValidateSamplingMethodSupportedMethods(t *testing.T) {
	telegramMethods := []string{"channel", "snowball", "random-walk"}
	youtubeMethods := []string{"channel", "random", "snowball"}

	for _, method := range telegramMethods {
		in := common.SamplingValidationInput{Platform: "telegram", SamplingMethod: method}
		switch method {
		case "channel", "snowball":
			in.URLList = []string{"https://t.me/test"}
		case "random-walk":
			in.SeedSize = 100
		}
		if err := validateSamplingMethod(in); err != nil {
			t.Errorf("Telegram should support method '%s', but got error: %s", method, err.Error())
		}
	}

	for _, method := range youtubeMethods {
		in := common.SamplingValidationInput{Platform: "youtube", SamplingMethod: method}
		if method != "random" {
			in.URLList = []string{"https://youtube.com/c/test"}
		}
		if err := validateSamplingMethod(in); err != nil {
			t.Errorf("YouTube should support method '%s', but got error: %s", method, err.Error())
		}
	}
}

func TestValidateSamplingMethodDaprJobMode(t *testing.T) {
	err := validateSamplingMethod(common.SamplingValidationInput{Platform: "telegram", SamplingMethod: "channel", Mode: "dapr-job"})
	if err != nil {
		t.Errorf("Expected no error for channel sampling without URLs in dapr-job mode, got: %s", err.Error())
	}

	err = validateSamplingMethod(common.SamplingValidationInput{Platform: "youtube", SamplingMethod: "snowball", Mode: "dapr-job"})
	if err != nil {
		t.Errorf("Expected no error for snowball sampling without URLs in dapr-job mode, got: %s", err.Error())
	}

	err = validateSamplingMethod(common.SamplingValidationInput{Platform: "telegram", SamplingMethod: "channel", Mode: "standalone"})
	if err == nil {
		t.Errorf("Expected error for channel sampling without URLs in standalone mode")
	}

	err = validateSamplingMethod(common.SamplingValidationInput{Platform: "telegram", SamplingMethod: "channel"})
	if err == nil {
		t.Errorf("Expected error for channel sampling without URLs in empty mode")
	}
}

func containsString(s, substr string) bool {
	return len(substr) == 0 || (len(s) >= len(substr) && s[:len(substr)] == substr) ||
		(len(s) > len(substr) && containsStringHelper(s, substr))
}

func containsStringHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

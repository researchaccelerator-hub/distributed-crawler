package main

import (
	"testing"
)

func TestValidateSamplingMethod(t *testing.T) {
	tests := []struct {
		name           string
		platform       string
		samplingMethod string
		urlList        []string
		urlFile        string
		expectError    bool
		errorContains  string
	}{
		{
			name:           "telegram channel sampling with URLs",
			platform:       "telegram",
			samplingMethod: "channel",
			urlList:        []string{"https://t.me/test"},
			urlFile:        "",
			expectError:    false,
		},
		{
			name:           "telegram snowball sampling with URLs",
			platform:       "telegram",
			samplingMethod: "snowball",
			urlList:        []string{"https://t.me/test1", "https://t.me/test2"},
			urlFile:        "",
			expectError:    false,
		},
		{
			name:           "telegram random sampling - unsupported",
			platform:       "telegram",
			samplingMethod: "random",
			urlList:        []string{},
			urlFile:        "",
			expectError:    true,
			errorContains:  "not supported for platform 'telegram'",
		},
		{
			name:           "youtube channel sampling with URLs",
			platform:       "youtube",
			samplingMethod: "channel",
			urlList:        []string{"https://youtube.com/c/test"},
			urlFile:        "",
			expectError:    false,
		},
		{
			name:           "youtube random sampling without URLs",
			platform:       "youtube",
			samplingMethod: "random",
			urlList:        []string{},
			urlFile:        "",
			expectError:    false,
		},
		{
			name:           "youtube snowball sampling with URLs",
			platform:       "youtube",
			samplingMethod: "snowball",
			urlList:        []string{"https://youtube.com/c/seed"},
			urlFile:        "",
			expectError:    false,
		},
		{
			name:           "channel sampling without URLs",
			platform:       "youtube",
			samplingMethod: "channel",
			urlList:        []string{},
			urlFile:        "",
			expectError:    true,
			errorContains:  "channel sampling requires URLs",
		},
		{
			name:           "snowball sampling without URLs",
			platform:       "telegram",
			samplingMethod: "snowball",
			urlList:        []string{},
			urlFile:        "",
			expectError:    true,
			errorContains:  "snowball sampling requires URLs",
		},
		{
			name:           "channel sampling with URL file",
			platform:       "youtube",
			samplingMethod: "channel",
			urlList:        []string{},
			urlFile:        "/path/to/urls.txt",
			expectError:    false,
		},
		{
			name:           "unsupported platform",
			platform:       "unsupported",
			samplingMethod: "channel",
			urlList:        []string{"https://example.com"},
			urlFile:        "",
			expectError:    true,
			errorContains:  "unsupported platform",
		},
		{
			name:           "invalid sampling method",
			platform:       "youtube",
			samplingMethod: "invalid",
			urlList:        []string{"https://youtube.com/c/test"},
			urlFile:        "",
			expectError:    true,
			errorContains:  "not supported for platform 'youtube'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSamplingMethod(tt.platform, tt.samplingMethod, tt.urlList, tt.urlFile)
			
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
	// Test that the correct methods are supported for each platform
	telegramMethods := []string{"channel", "snowball"}
	youtubeMethods := []string{"channel", "random", "snowball"}
	
	// Test Telegram supported methods
	for _, method := range telegramMethods {
		err := validateSamplingMethod("telegram", method, []string{"https://t.me/test"}, "")
		if err != nil {
			t.Errorf("Telegram should support method '%s', but got error: %s", method, err.Error())
		}
	}
	
	// Test YouTube supported methods
	for _, method := range youtubeMethods {
		var urlList []string
		if method != "random" {
			urlList = []string{"https://youtube.com/c/test"}
		}
		err := validateSamplingMethod("youtube", method, urlList, "")
		if err != nil {
			t.Errorf("YouTube should support method '%s', but got error: %s", method, err.Error())
		}
	}
}

// Helper function to check if a string contains a substring
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
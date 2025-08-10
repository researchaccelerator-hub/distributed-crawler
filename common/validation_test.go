package common

import (
	"fmt"
	"strings"
	"testing"
)

// validateSamplingMethod is a copy of the validation function for testing
// This allows us to test the logic without TDLib dependencies
func validateSamplingMethod(platform, samplingMethod string, urlList []string, urlFile string, mode string) error {
	// Valid sampling methods per platform
	validMethods := map[string][]string{
		"telegram": {"channel", "snowball"},
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
	if samplingMethod == "random" {
		return nil
	}

	// For channel and snowball sampling, validate that URLs are provided
	// Skip URL validation for dapr-job mode since jobs provide URLs through job data
	if (samplingMethod == "channel" || samplingMethod == "snowball") && len(urlList) == 0 && urlFile == "" && mode != "dapr-job" {
		return fmt.Errorf("%s sampling requires URLs to be provided. Use --urls or --url-file to specify them", samplingMethod)
	}

	return nil
}

// TestValidateSamplingMethodDaprJobMode tests that URL validation is skipped in dapr-job mode
func TestValidateSamplingMethodDaprJobMode(t *testing.T) {
	// Test that channel sampling without URLs is allowed in dapr-job mode
	err := validateSamplingMethod("telegram", "channel", []string{}, "", "dapr-job")
	if err != nil {
		t.Errorf("Expected no error for channel sampling without URLs in dapr-job mode, got: %s", err.Error())
	}

	// Test that snowball sampling without URLs is allowed in dapr-job mode
	err = validateSamplingMethod("youtube", "snowball", []string{}, "", "dapr-job")
	if err != nil {
		t.Errorf("Expected no error for snowball sampling without URLs in dapr-job mode, got: %s", err.Error())
	}

	// Test that other modes still require URLs
	err = validateSamplingMethod("telegram", "channel", []string{}, "", "standalone")
	if err == nil {
		t.Errorf("Expected error for channel sampling without URLs in standalone mode")
	}

	// Test that an empty mode still requires URLs (backwards compatibility)
	err = validateSamplingMethod("telegram", "channel", []string{}, "", "")
	if err == nil {
		t.Errorf("Expected error for channel sampling without URLs in empty mode")
	}
}

func TestValidateSamplingMethodComprehensive(t *testing.T) {
	tests := []struct {
		name           string
		platform       string
		samplingMethod string
		urlList        []string
		urlFile        string
		expectError    bool
		errorContains  string
	}{
		// Valid configurations
		{
			name:           "telegram channel with URL list",
			platform:       "telegram",
			samplingMethod: "channel",
			urlList:        []string{"https://t.me/test"},
			urlFile:        "",
			expectError:    false,
		},
		{
			name:           "telegram channel with URL file",
			platform:       "telegram",
			samplingMethod: "channel",
			urlList:        []string{},
			urlFile:        "/path/to/urls.txt",
			expectError:    false,
		},
		{
			name:           "telegram channel with both URL list and file",
			platform:       "telegram",
			samplingMethod: "channel",
			urlList:        []string{"https://t.me/test1", "https://t.me/test2"},
			urlFile:        "/path/to/urls.txt",
			expectError:    false,
		},
		{
			name:           "telegram snowball with multiple URLs",
			platform:       "telegram",
			samplingMethod: "snowball",
			urlList:        []string{"https://t.me/seed1", "https://t.me/seed2", "https://t.me/seed3"},
			urlFile:        "",
			expectError:    false,
		},
		{
			name:           "youtube channel with URL",
			platform:       "youtube",
			samplingMethod: "channel",
			urlList:        []string{"https://youtube.com/c/test"},
			urlFile:        "",
			expectError:    false,
		},
		{
			name:           "youtube random without URLs",
			platform:       "youtube",
			samplingMethod: "random",
			urlList:        []string{},
			urlFile:        "",
			expectError:    false,
		},
		{
			name:           "youtube random with URLs (should still work)",
			platform:       "youtube",
			samplingMethod: "random",
			urlList:        []string{"https://youtube.com/c/ignored"},
			urlFile:        "",
			expectError:    false,
		},
		{
			name:           "youtube snowball with URL file",
			platform:       "youtube",
			samplingMethod: "snowball",
			urlList:        []string{},
			urlFile:        "/path/to/youtube-seeds.txt",
			expectError:    false,
		},

		// Invalid platform configurations
		{
			name:           "telegram random - unsupported",
			platform:       "telegram",
			samplingMethod: "random",
			urlList:        []string{},
			urlFile:        "",
			expectError:    true,
			errorContains:  "not supported for platform 'telegram'",
		},
		{
			name:           "unsupported platform",
			platform:       "facebook",
			samplingMethod: "channel",
			urlList:        []string{"https://facebook.com/test"},
			urlFile:        "",
			expectError:    true,
			errorContains:  "unsupported platform: facebook",
		},
		{
			name:           "empty platform",
			platform:       "",
			samplingMethod: "channel",
			urlList:        []string{"https://example.com"},
			urlFile:        "",
			expectError:    true,
			errorContains:  "unsupported platform:",
		},

		// Invalid sampling method configurations
		{
			name:           "invalid sampling method for telegram",
			platform:       "telegram",
			samplingMethod: "invalid",
			urlList:        []string{"https://t.me/test"},
			urlFile:        "",
			expectError:    true,
			errorContains:  "not supported for platform 'telegram'",
		},
		{
			name:           "invalid sampling method for youtube",
			platform:       "youtube",
			samplingMethod: "breadth-first",
			urlList:        []string{"https://youtube.com/c/test"},
			urlFile:        "",
			expectError:    true,
			errorContains:  "not supported for platform 'youtube'",
		},
		{
			name:           "empty sampling method",
			platform:       "youtube",
			samplingMethod: "",
			urlList:        []string{"https://youtube.com/c/test"},
			urlFile:        "",
			expectError:    true,
			errorContains:  "not supported for platform 'youtube'",
		},

		// Missing URL configurations
		{
			name:           "telegram channel without URLs",
			platform:       "telegram",
			samplingMethod: "channel",
			urlList:        []string{},
			urlFile:        "",
			expectError:    true,
			errorContains:  "channel sampling requires URLs",
		},
		{
			name:           "telegram snowball without URLs",
			platform:       "telegram",
			samplingMethod: "snowball",
			urlList:        []string{},
			urlFile:        "",
			expectError:    true,
			errorContains:  "snowball sampling requires URLs",
		},
		{
			name:           "youtube channel without URLs",
			platform:       "youtube",
			samplingMethod: "channel",
			urlList:        []string{},
			urlFile:        "",
			expectError:    true,
			errorContains:  "channel sampling requires URLs",
		},
		{
			name:           "youtube snowball without URLs",
			platform:       "youtube",
			samplingMethod: "snowball",
			urlList:        []string{},
			urlFile:        "",
			expectError:    true,
			errorContains:  "snowball sampling requires URLs",
		},

		// Edge cases
		{
			name:           "case sensitivity - uppercase platform",
			platform:       "TELEGRAM",
			samplingMethod: "channel",
			urlList:        []string{"https://t.me/test"},
			urlFile:        "",
			expectError:    true,
			errorContains:  "unsupported platform: TELEGRAM",
		},
		{
			name:           "case sensitivity - uppercase sampling method",
			platform:       "telegram",
			samplingMethod: "CHANNEL",
			urlList:        []string{"https://t.me/test"},
			urlFile:        "",
			expectError:    true,
			errorContains:  "not supported for platform 'telegram'",
		},
		{
			name:           "whitespace in platform",
			platform:       " telegram ",
			samplingMethod: "channel",
			urlList:        []string{"https://t.me/test"},
			urlFile:        "",
			expectError:    true,
			errorContains:  "unsupported platform:  telegram ",
		},
		{
			name:           "whitespace in sampling method",
			platform:       "telegram",
			samplingMethod: " channel ",
			urlList:        []string{"https://t.me/test"},
			urlFile:        "",
			expectError:    true,
			errorContains:  "not supported for platform 'telegram'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSamplingMethod(tt.platform, tt.samplingMethod, tt.urlList, tt.urlFile, "")
			
			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
					return
				}
				if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
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
	platforms := map[string][]string{
		"telegram": {"channel", "snowball"},
		"youtube":  {"channel", "random", "snowball"},
	}
	
	for platform, methods := range platforms {
		for _, method := range methods {
			t.Run(fmt.Sprintf("%s_%s", platform, method), func(t *testing.T) {
				var urlList []string
				if method != "random" {
					urlList = []string{"https://example.com/test"}
				}
				
				err := validateSamplingMethod(platform, method, urlList, "", "")
				if err != nil {
					t.Errorf("Platform '%s' should support method '%s', but got error: %s", platform, method, err.Error())
				}
			})
		}
	}
}

func TestValidateSamplingMethodErrorMessages(t *testing.T) {
	tests := []struct {
		name           string
		platform       string
		samplingMethod string
		urlList        []string
		urlFile        string
		expectedError  string
	}{
		{
			name:           "unsupported platform error format",
			platform:       "twitter",
			samplingMethod: "channel",
			urlList:        []string{"https://twitter.com/test"},
			urlFile:        "",
			expectedError:  "unsupported platform: twitter",
		},
		{
			name:           "unsupported method error format includes supported methods",
			platform:       "telegram",
			samplingMethod: "random",
			urlList:        []string{"https://t.me/test"},
			urlFile:        "",
			expectedError:  "sampling method 'random' is not supported for platform 'telegram'. Supported methods: [channel snowball]",
		},
		{
			name:           "missing URLs error format",
			platform:       "youtube",
			samplingMethod: "channel",
			urlList:        []string{},
			urlFile:        "",
			expectedError:  "channel sampling requires URLs to be provided. Use --urls or --url-file to specify them",
		},
		{
			name:           "missing URLs for snowball",
			platform:       "youtube",
			samplingMethod: "snowball",
			urlList:        []string{},
			urlFile:        "",
			expectedError:  "snowball sampling requires URLs to be provided. Use --urls or --url-file to specify them",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSamplingMethod(tt.platform, tt.samplingMethod, tt.urlList, tt.urlFile, "")
			
			if err == nil {
				t.Errorf("Expected error but got none")
				return
			}
			
			if err.Error() != tt.expectedError {
				t.Errorf("Expected exact error '%s', but got: '%s'", tt.expectedError, err.Error())
			}
		})
	}
}

func TestValidateSamplingMethodRandomSpecialCase(t *testing.T) {
	// Test that random sampling works regardless of URL configuration
	testCases := []struct {
		name    string
		urlList []string
		urlFile string
	}{
		{
			name:    "no URLs",
			urlList: []string{},
			urlFile: "",
		},
		{
			name:    "with URLs",
			urlList: []string{"https://youtube.com/c/test1", "https://youtube.com/c/test2"},
			urlFile: "",
		},
		{
			name:    "with URL file",
			urlList: []string{},
			urlFile: "/path/to/urls.txt",
		},
		{
			name:    "with both URLs and file",
			urlList: []string{"https://youtube.com/c/test"},
			urlFile: "/path/to/urls.txt",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSamplingMethod("youtube", "random", tc.urlList, tc.urlFile, "")
			if err != nil {
				t.Errorf("Random sampling should work with any URL configuration, but got error: %s", err.Error())
			}
		})
	}
}

func TestValidateSamplingMethodBoundaryConditions(t *testing.T) {
	t.Run("empty URL list vs nil URL list", func(t *testing.T) {
		// Test with empty slice
		err1 := validateSamplingMethod("youtube", "channel", []string{}, "", "")
		
		// Test with nil slice  
		err2 := validateSamplingMethod("youtube", "channel", nil, "", "")
		
		// Both should produce the same error
		if (err1 == nil) != (err2 == nil) {
			t.Errorf("Empty slice and nil slice should behave the same")
		}
		
		if err1 != nil && err2 != nil && err1.Error() != err2.Error() {
			t.Errorf("Empty slice and nil slice should produce the same error message")
		}
	})

	t.Run("very long URL list", func(t *testing.T) {
		// Test with a very large URL list
		urlList := make([]string, 1000)
		for i := 0; i < 1000; i++ {
			urlList[i] = fmt.Sprintf("https://example.com/channel%d", i)
		}
		
		err := validateSamplingMethod("youtube", "channel", urlList, "", "")
		if err != nil {
			t.Errorf("Large URL list should be accepted, but got error: %s", err.Error())
		}
	})

	t.Run("special characters in file path", func(t *testing.T) {
		specialPaths := []string{
			"/path/with spaces/urls.txt",
			"/path/with-dashes/urls.txt",
			"/path/with_underscores/urls.txt",
			"/path/with.dots/urls.txt",
			"/path/with(parentheses)/urls.txt",
			"/path/with[brackets]/urls.txt",
		}
		
		for _, path := range specialPaths {
			err := validateSamplingMethod("youtube", "channel", []string{}, path, "")
			if err != nil {
				t.Errorf("File path with special characters should be accepted: %s, but got error: %s", path, err.Error())
			}
		}
	})
}
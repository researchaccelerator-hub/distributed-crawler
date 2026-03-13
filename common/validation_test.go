package common

import (
	"fmt"
	"strings"
	"testing"
)

func TestValidateSamplingMethodDaprJobMode(t *testing.T) {
	err := ValidateSamplingMethod(SamplingValidationInput{Platform: "telegram", SamplingMethod: "channel", Mode: "dapr-job"})
	if err != nil {
		t.Errorf("Expected no error for channel sampling without URLs in dapr-job mode, got: %s", err.Error())
	}

	err = ValidateSamplingMethod(SamplingValidationInput{Platform: "youtube", SamplingMethod: "snowball", Mode: "dapr-job"})
	if err != nil {
		t.Errorf("Expected no error for snowball sampling without URLs in dapr-job mode, got: %s", err.Error())
	}

	err = ValidateSamplingMethod(SamplingValidationInput{Platform: "telegram", SamplingMethod: "channel", Mode: "standalone"})
	if err == nil {
		t.Errorf("Expected error for channel sampling without URLs in standalone mode")
	}

	err = ValidateSamplingMethod(SamplingValidationInput{Platform: "telegram", SamplingMethod: "channel"})
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
		{
			name:           "telegram channel with URL list",
			platform:       "telegram",
			samplingMethod: "channel",
			urlList:        []string{"https://t.me/test"},
			expectError:    false,
		},
		{
			name:           "telegram channel with URL file",
			platform:       "telegram",
			samplingMethod: "channel",
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
			urlList:        []string{"https://t.me/seed1", "https://t.me/seed2"},
			expectError:    false,
		},
		{
			name:           "youtube channel with URL",
			platform:       "youtube",
			samplingMethod: "channel",
			urlList:        []string{"https://youtube.com/c/test"},
			expectError:    false,
		},
		{
			name:           "youtube random without URLs",
			platform:       "youtube",
			samplingMethod: "random",
			expectError:    false,
		},
		{
			name:           "youtube random with URLs (should still work)",
			platform:       "youtube",
			samplingMethod: "random",
			urlList:        []string{"https://youtube.com/c/ignored"},
			expectError:    false,
		},
		{
			name:           "youtube snowball with URL file",
			platform:       "youtube",
			samplingMethod: "snowball",
			urlFile:        "/path/to/youtube-seeds.txt",
			expectError:    false,
		},
		// random-walk
		{
			name:           "telegram random-walk with seed URLs",
			platform:       "telegram",
			samplingMethod: "random-walk",
			urlList:        []string{"https://t.me/seed1"},
			expectError:    false,
		},
		{
			name:           "telegram random-walk with seed size",
			platform:       "telegram",
			samplingMethod: "random-walk",
			expectError:    false,
		},
		{
			name:           "telegram random-walk with both URLs and seed size - error",
			platform:       "telegram",
			samplingMethod: "random-walk",
			urlList:        []string{"https://t.me/seed1"},
			expectError:    true,
			errorContains:  "not both or neither",
		},
		{
			name:           "telegram random-walk with neither - error",
			platform:       "telegram",
			samplingMethod: "random-walk",
			expectError:    true,
			errorContains:  "not both or neither",
		},
		// Invalid platforms
		{
			name:           "telegram random - unsupported",
			platform:       "telegram",
			samplingMethod: "random",
			expectError:    true,
			errorContains:  "not supported for platform 'telegram'",
		},
		{
			name:           "unsupported platform",
			platform:       "facebook",
			samplingMethod: "channel",
			urlList:        []string{"https://facebook.com/test"},
			expectError:    true,
			errorContains:  "unsupported platform: facebook",
		},
		{
			name:           "empty platform",
			platform:       "",
			samplingMethod: "channel",
			urlList:        []string{"https://example.com"},
			expectError:    true,
			errorContains:  "unsupported platform:",
		},
		// Invalid sampling methods
		{
			name:           "invalid sampling method for telegram",
			platform:       "telegram",
			samplingMethod: "invalid",
			urlList:        []string{"https://t.me/test"},
			expectError:    true,
			errorContains:  "not supported for platform 'telegram'",
		},
		{
			name:           "invalid sampling method for youtube",
			platform:       "youtube",
			samplingMethod: "breadth-first",
			urlList:        []string{"https://youtube.com/c/test"},
			expectError:    true,
			errorContains:  "not supported for platform 'youtube'",
		},
		{
			name:           "empty sampling method",
			platform:       "youtube",
			samplingMethod: "",
			urlList:        []string{"https://youtube.com/c/test"},
			expectError:    true,
			errorContains:  "not supported for platform 'youtube'",
		},
		// Missing URLs
		{
			name:           "telegram channel without URLs",
			platform:       "telegram",
			samplingMethod: "channel",
			expectError:    true,
			errorContains:  "channel sampling requires URLs",
		},
		{
			name:           "telegram snowball without URLs",
			platform:       "telegram",
			samplingMethod: "snowball",
			expectError:    true,
			errorContains:  "snowball sampling requires URLs",
		},
		{
			name:           "youtube channel without URLs",
			platform:       "youtube",
			samplingMethod: "channel",
			expectError:    true,
			errorContains:  "channel sampling requires URLs",
		},
		{
			name:           "youtube snowball without URLs",
			platform:       "youtube",
			samplingMethod: "snowball",
			expectError:    true,
			errorContains:  "snowball sampling requires URLs",
		},
		// Case sensitivity
		{
			name:           "uppercase platform rejected",
			platform:       "TELEGRAM",
			samplingMethod: "channel",
			urlList:        []string{"https://t.me/test"},
			expectError:    true,
			errorContains:  "unsupported platform: TELEGRAM",
		},
		{
			name:           "uppercase sampling method rejected",
			platform:       "telegram",
			samplingMethod: "CHANNEL",
			urlList:        []string{"https://t.me/test"},
			expectError:    true,
			errorContains:  "not supported for platform 'telegram'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// random-walk with seed size > 0 needs SeedSize set
			seedSize := 0
			if tt.samplingMethod == "random-walk" && len(tt.urlList) == 0 && tt.urlFile == "" && !tt.expectError {
				seedSize = 100
			}
			err := ValidateSamplingMethod(SamplingValidationInput{
				Platform:       tt.platform,
				SamplingMethod: tt.samplingMethod,
				URLList:        tt.urlList,
				URLFile:        tt.urlFile,
				SeedSize:       seedSize,
			})
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
	platforms := map[string][]string{
		"telegram": {"channel", "snowball", "random-walk"},
		"youtube":  {"channel", "random", "snowball"},
	}

	for platform, methods := range platforms {
		for _, method := range methods {
			t.Run(fmt.Sprintf("%s_%s", platform, method), func(t *testing.T) {
				in := SamplingValidationInput{Platform: platform, SamplingMethod: method}
				switch method {
				case "random":
					// no URLs needed
				case "random-walk":
					in.SeedSize = 100
				default:
					in.URLList = []string{"https://example.com/test"}
				}
				err := ValidateSamplingMethod(in)
				if err != nil {
					t.Errorf("Platform '%s' should support method '%s', but got error: %s", platform, method, err.Error())
				}
			})
		}
	}
}

func TestValidateSamplingMethodErrorMessages(t *testing.T) {
	tests := []struct {
		name          string
		in            SamplingValidationInput
		expectedError string
	}{
		{
			name:          "unsupported platform error format",
			in:            SamplingValidationInput{Platform: "twitter", SamplingMethod: "channel", URLList: []string{"https://twitter.com/test"}},
			expectedError: "unsupported platform: twitter",
		},
		{
			name:          "unsupported method error format includes supported methods",
			in:            SamplingValidationInput{Platform: "telegram", SamplingMethod: "random", URLList: []string{"https://t.me/test"}},
			expectedError: "sampling method 'random' is not supported for platform 'telegram'. Supported methods: [channel snowball random-walk]",
		},
		{
			name:          "missing URLs error format",
			in:            SamplingValidationInput{Platform: "youtube", SamplingMethod: "channel"},
			expectedError: "channel sampling requires URLs to be provided. Use --urls or --url-file to specify them",
		},
		{
			name:          "missing URLs for snowball",
			in:            SamplingValidationInput{Platform: "youtube", SamplingMethod: "snowball"},
			expectedError: "snowball sampling requires URLs to be provided. Use --urls or --url-file to specify them",
		},
		{
			name:          "random-walk crawl ID too long",
			in:            SamplingValidationInput{Platform: "telegram", SamplingMethod: "random-walk", SeedSize: 100, CrawlID: strings.Repeat("x", 33)},
			expectedError: "crawl IDs cannot exceed 32 characters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSamplingMethod(tt.in)
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
	testCases := []struct {
		name    string
		urlList []string
		urlFile string
	}{
		{name: "no URLs"},
		{name: "with URLs", urlList: []string{"https://youtube.com/c/test1"}},
		{name: "with URL file", urlFile: "/path/to/urls.txt"},
		{name: "with both", urlList: []string{"https://youtube.com/c/test"}, urlFile: "/path/to/urls.txt"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateSamplingMethod(SamplingValidationInput{Platform: "youtube", SamplingMethod: "random", URLList: tc.urlList, URLFile: tc.urlFile})
			if err != nil {
				t.Errorf("Random sampling should work with any URL configuration, but got error: %s", err.Error())
			}
		})
	}
}

func TestValidateSamplingMethodRandomWalk(t *testing.T) {
	t.Run("seed URLs accepted", func(t *testing.T) {
		err := ValidateSamplingMethod(SamplingValidationInput{
			Platform: "telegram", SamplingMethod: "random-walk",
			URLList: []string{"https://t.me/channel1"},
		})
		if err != nil {
			t.Errorf("Unexpected error: %s", err)
		}
	})

	t.Run("seed size accepted", func(t *testing.T) {
		err := ValidateSamplingMethod(SamplingValidationInput{
			Platform: "telegram", SamplingMethod: "random-walk",
			SeedSize: 500,
		})
		if err != nil {
			t.Errorf("Unexpected error: %s", err)
		}
	})

	t.Run("url-file-url accepted as URL source", func(t *testing.T) {
		err := ValidateSamplingMethod(SamplingValidationInput{
			Platform: "telegram", SamplingMethod: "random-walk",
			URLFileURL: "https://example.com/seeds.txt",
		})
		if err != nil {
			t.Errorf("Unexpected error: %s", err)
		}
	})

	t.Run("both URL source and seed size rejected", func(t *testing.T) {
		err := ValidateSamplingMethod(SamplingValidationInput{
			Platform: "telegram", SamplingMethod: "random-walk",
			URLList: []string{"https://t.me/channel1"}, SeedSize: 100,
		})
		if err == nil || !strings.Contains(err.Error(), "not both or neither") {
			t.Errorf("Expected XOR error, got: %v", err)
		}
	})

	t.Run("neither URL source nor seed size rejected", func(t *testing.T) {
		err := ValidateSamplingMethod(SamplingValidationInput{
			Platform: "telegram", SamplingMethod: "random-walk",
		})
		if err == nil || !strings.Contains(err.Error(), "not both or neither") {
			t.Errorf("Expected XOR error, got: %v", err)
		}
	})

	t.Run("crawl ID exactly 32 chars accepted", func(t *testing.T) {
		err := ValidateSamplingMethod(SamplingValidationInput{
			Platform: "telegram", SamplingMethod: "random-walk",
			SeedSize: 100, CrawlID: strings.Repeat("x", 32),
		})
		if err != nil {
			t.Errorf("Unexpected error: %s", err)
		}
	})

	t.Run("crawl ID 33 chars rejected", func(t *testing.T) {
		err := ValidateSamplingMethod(SamplingValidationInput{
			Platform: "telegram", SamplingMethod: "random-walk",
			SeedSize: 100, CrawlID: strings.Repeat("x", 33),
		})
		if err == nil || !strings.Contains(err.Error(), "cannot exceed 32 characters") {
			t.Errorf("Expected crawl ID length error, got: %v", err)
		}
	})
}

func TestValidateSamplingMethodBoundaryConditions(t *testing.T) {
	t.Run("empty URL list vs nil URL list behave the same", func(t *testing.T) {
		err1 := ValidateSamplingMethod(SamplingValidationInput{Platform: "youtube", SamplingMethod: "channel", URLList: []string{}})
		err2 := ValidateSamplingMethod(SamplingValidationInput{Platform: "youtube", SamplingMethod: "channel", URLList: nil})
		if (err1 == nil) != (err2 == nil) {
			t.Errorf("Empty slice and nil slice should behave the same")
		}
		if err1 != nil && err2 != nil && err1.Error() != err2.Error() {
			t.Errorf("Empty slice and nil slice should produce the same error message")
		}
	})

	t.Run("very long URL list", func(t *testing.T) {
		urlList := make([]string, 1000)
		for i := range urlList {
			urlList[i] = fmt.Sprintf("https://example.com/channel%d", i)
		}
		err := ValidateSamplingMethod(SamplingValidationInput{Platform: "youtube", SamplingMethod: "channel", URLList: urlList})
		if err != nil {
			t.Errorf("Large URL list should be accepted, but got error: %s", err.Error())
		}
	})
}

package common

import "fmt"

// SamplingValidationInput holds all inputs needed to validate a sampling method configuration.
type SamplingValidationInput struct {
	Platform       string
	SamplingMethod string
	URLList        []string
	URLFile        string
	URLFileURL     string // may not yet be resolved to URLFile at validation time
	Mode           string
	SeedSize       int    // random-walk: mutually exclusive with URL sources
	CrawlID        string // random-walk: must not exceed 32 chars
}

// ValidateSamplingMethod validates that the platform supports the specified sampling method
// and that the required URL / seed inputs are present.
func ValidateSamplingMethod(in SamplingValidationInput) error {
	validMethods := map[string][]string{
		"telegram": {"channel", "snowball", "random-walk"},
		"youtube":  {"channel", "random", "snowball"},
	}

	supportedMethods, exists := validMethods[in.Platform]
	if !exists {
		return fmt.Errorf("unsupported platform: %s", in.Platform)
	}

	isSupported := false
	for _, method := range supportedMethods {
		if method == in.SamplingMethod {
			isSupported = true
			break
		}
	}
	if !isSupported {
		return fmt.Errorf("sampling method '%s' is not supported for platform '%s'. Supported methods: %v",
			in.SamplingMethod, in.Platform, supportedMethods)
	}

	hasURLSource := len(in.URLList) > 0 || in.URLFile != "" || in.URLFileURL != ""

	if in.SamplingMethod == "random-walk" {
		// Exactly one of (URL sources / seed size) must be provided.
		if hasURLSource == (in.SeedSize > 0) {
			return fmt.Errorf("must provide either seed urls or seed size in random-walk crawl, not both or neither")
		}
		if len(in.CrawlID) > 32 {
			return fmt.Errorf("crawl IDs cannot exceed 32 characters")
		}
		return nil
	}

	// For random (YouTube), no URLs required.
	if in.SamplingMethod == "random" {
		return nil
	}

	// For channel and snowball: URLs required unless dapr-job mode (job data provides them).
	if !hasURLSource && in.Mode != "dapr-job" {
		return fmt.Errorf("%s sampling requires URLs to be provided. Use --urls or --url-file to specify them", in.SamplingMethod)
	}

	return nil
}

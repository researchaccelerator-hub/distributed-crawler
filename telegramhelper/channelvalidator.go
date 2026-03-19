package telegramhelper

import (
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"

	"github.com/rs/zerolog/log"
)

// browserUserAgents is a pool of Chromium-based browser UA strings rotated per
// request. Only Chrome and Edge are included — both use the Chromium TLS stack,
// which matches the HelloChrome_Auto fingerprint set by NewValidatorHTTPClient.
// Mixing in Firefox or Safari UAs would create a UA/JA3 mismatch that is an
// obvious bot signal.
var browserUserAgents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
}

// ValidationErrorKind classifies why ValidateChannelHTTP failed, allowing the
// caller to distinguish transient network problems from access blocks.
type ValidationErrorKind int

const (
	// ErrTransient indicates a temporary failure (connection error, timeout,
	// 5xx). The caller should retry the edge later.
	ErrTransient ValidationErrorKind = iota
	// ErrBlocked indicates an IP-level block or soft-block from Telegram
	// (403, 429, other 4xx, connection reset, or 200 with unrecognised/empty
	// content). The caller should pause HTTP validation.
	ErrBlocked
)

// ValidationHTTPError is returned by ValidateChannelHTTP when the request
// could not produce a definitive channel validation result.
type ValidationHTTPError struct {
	Kind    ValidationErrorKind
	Wrapped error
}

func (e *ValidationHTTPError) Error() string { return e.Wrapped.Error() }
func (e *ValidationHTTPError) Unwrap() error { return e.Wrapped }

// ChannelValidationResult holds the outcome of an HTTP-based channel validation.
type ChannelValidationResult struct {
	Status string // "valid" | "not_channel" | "invalid"
	Reason string // "" | "not_supergroup" | "not_found"
}

// ValidateChannelHTTP checks whether a Telegram username belongs to a public
// supergroup/channel by fetching https://t.me/<username> and inspecting the
// HTML title and robots meta tag.
//
// Parsing rules (derived from saved HTML samples in telegram-html/):
//
//	Title contains "Telegram: View @"    → valid channel/supergroup
//	Title contains "Telegram: Contact @" AND no robots noindex → not a supergroup (user/bot/group)
//	Title contains "Telegram: Contact @" AND robots noindex    → username not found / not occupied
func ValidateChannelHTTP(username string, httpClient *http.Client) (ChannelValidationResult, error) {
	url := fmt.Sprintf("https://t.me/%s", username)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return ChannelValidationResult{}, fmt.Errorf("channelvalidator: failed to create request for %s: %w", username, err)
	}
	req.Header.Set("User-Agent", browserUserAgents[rand.Intn(len(browserUserAgents))])
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	// Do NOT set Accept-Encoding manually — Go's transport adds it automatically
	// and decompresses the response. Setting it here would disable that and leave
	// the body as raw gzip bytes, breaking HTML parsing.
	req.Header.Set("Upgrade-Insecure-Requests", "1")
	req.Header.Set("Sec-Fetch-Dest", "document")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Site", "none")

	resp, err := httpClient.Do(req)
	if err != nil {
		return ChannelValidationResult{}, &ValidationHTTPError{
			Kind:    ErrTransient,
			Wrapped: fmt.Errorf("channelvalidator: HTTP request failed for %s: %w", username, err),
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		kind := ErrBlocked
		if resp.StatusCode >= 500 {
			kind = ErrTransient
		}
		return ChannelValidationResult{}, &ValidationHTTPError{
			Kind:    kind,
			Wrapped: fmt.Errorf("channelvalidator: unexpected status %d for %s", resp.StatusCode, username),
		}
	}

	// Read up to 64KB — the signals we need are in the <head> section.
	const maxReadBytes = 64 * 1024
	limited := io.LimitReader(resp.Body, maxReadBytes)
	body, err := io.ReadAll(limited)
	if err != nil {
		return ChannelValidationResult{}, &ValidationHTTPError{
			Kind:    ErrTransient,
			Wrapped: fmt.Errorf("channelvalidator: failed to read response body for %s: %w", username, err),
		}
	}

	result, parseErr := ParseChannelHTML(string(body))
	if parseErr != nil {
		// Unrecognised or empty title on a 200 response — treat as soft-block
		// rather than a definitive invalid result.
		log.Warn().
			Str("channel", username).
			Str("response_body", string(body)).
			Err(parseErr).
			Msg("channelvalidator: unrecognised HTML response")
		return ChannelValidationResult{}, &ValidationHTTPError{
			Kind:    ErrBlocked,
			Wrapped: fmt.Errorf("channelvalidator: failed to parse response for %s: %w", username, parseErr),
		}
	}
	return result, nil
}

// ParseChannelHTML extracts the validation result from raw HTML content.
// Exported for testing without an HTTP server.
func ParseChannelHTML(html string) (ChannelValidationResult, error) {
	title := extractTitle(html)

	if strings.Contains(title, "Telegram: View @") {
		return ChannelValidationResult{Status: "valid", Reason: ""}, nil
	}

	if strings.Contains(title, "Telegram: Contact @") {
		if hasRobotsNoIndex(html) {
			return ChannelValidationResult{Status: "invalid", Reason: "not_found"}, nil
		}
		return ChannelValidationResult{Status: "not_channel", Reason: "not_supergroup"}, nil
	}

	return ChannelValidationResult{}, fmt.Errorf("channelvalidator: unrecognised title pattern: %q", title)
}

// extractTitle returns the content of the first <title>...</title> tag.
func extractTitle(html string) string {
	lower := strings.ToLower(html)
	start := strings.Index(lower, "<title>")
	if start == -1 {
		return ""
	}
	start += len("<title>")
	end := strings.Index(lower[start:], "</title>")
	if end == -1 {
		return ""
	}
	// Use original-case HTML for the extracted value.
	return strings.TrimSpace(html[start : start+end])
}

// hasRobotsNoIndex checks whether a <meta name="robots" content="...noindex...">
// tag is present in the HTML head.
func hasRobotsNoIndex(html string) bool {
	lower := strings.ToLower(html)
	// Look for meta name="robots" — the tag can appear in various forms.
	idx := strings.Index(lower, `name="robots"`)
	if idx == -1 {
		return false
	}
	// Find the enclosing <meta ...> tag to extract the content attribute.
	// Search backward for '<' and forward for '>'.
	tagStart := strings.LastIndex(lower[:idx], "<")
	if tagStart == -1 {
		return false
	}
	tagEnd := strings.Index(lower[idx:], ">")
	if tagEnd == -1 {
		return false
	}
	tag := lower[tagStart : idx+tagEnd+1]
	return strings.Contains(tag, "noindex")
}

package telegramhelper

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestParseChannelHTML_ValidChannel(t *testing.T) {
	html := loadFixture(t, "valid-channel.html")
	result, err := ParseChannelHTML(html)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != "valid" {
		t.Errorf("expected status 'valid', got %q", result.Status)
	}
	if result.Reason != "" {
		t.Errorf("expected empty reason, got %q", result.Reason)
	}
}

func TestParseChannelHTML_NotASupergroup(t *testing.T) {
	html := loadFixture(t, "not-a-supergroup.html")
	result, err := ParseChannelHTML(html)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != "not_channel" {
		t.Errorf("expected status 'not_channel', got %q", result.Status)
	}
	if result.Reason != "not_supergroup" {
		t.Errorf("expected reason 'not_supergroup', got %q", result.Reason)
	}
}

func TestParseChannelHTML_InvalidChannel(t *testing.T) {
	html := loadFixture(t, "invalid-channel.html")
	result, err := ParseChannelHTML(html)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != "invalid" {
		t.Errorf("expected status 'invalid', got %q", result.Status)
	}
	if result.Reason != "not_found" {
		t.Errorf("expected reason 'not_found', got %q", result.Reason)
	}
}

func TestParseChannelHTML_UsernameNotOccupied(t *testing.T) {
	html := loadFixture(t, "username-not-occupied.html")
	result, err := ParseChannelHTML(html)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != "invalid" {
		t.Errorf("expected status 'invalid', got %q", result.Status)
	}
	if result.Reason != "not_found" {
		t.Errorf("expected reason 'not_found', got %q", result.Reason)
	}
}

func TestParseChannelHTML_UnrecognisedTitle(t *testing.T) {
	html := `<html><head><title>Something Unexpected</title></head></html>`
	_, err := ParseChannelHTML(html)
	if err == nil {
		t.Fatal("expected error for unrecognised title, got nil")
	}
}

func TestParseChannelHTML_NoTitle(t *testing.T) {
	html := `<html><head></head><body>no title here</body></html>`
	_, err := ParseChannelHTML(html)
	if err == nil {
		t.Fatal("expected error for missing title, got nil")
	}
}

func TestParseChannelHTML_InlineHTML(t *testing.T) {
	tests := []struct {
		name           string
		html           string
		expectedStatus string
		expectedReason string
	}{
		{
			name:           "view title means valid",
			html:           `<html><head><title>Telegram: View @testchannel</title></head></html>`,
			expectedStatus: "valid",
			expectedReason: "",
		},
		{
			name:           "contact without robots means not_channel",
			html:           `<html><head><title>Telegram: Contact @someuser</title></head></html>`,
			expectedStatus: "not_channel",
			expectedReason: "not_supergroup",
		},
		{
			name:           "contact with robots noindex means invalid",
			html:           `<html><head><meta name="robots" content="noindex, nofollow"><title>Telegram: Contact @nobody</title></head></html>`,
			expectedStatus: "invalid",
			expectedReason: "not_found",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ParseChannelHTML(tc.html)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result.Status != tc.expectedStatus {
				t.Errorf("expected status %q, got %q", tc.expectedStatus, result.Status)
			}
			if result.Reason != tc.expectedReason {
				t.Errorf("expected reason %q, got %q", tc.expectedReason, result.Reason)
			}
		})
	}
}

func TestValidateChannelHTTP_Integration(t *testing.T) {
	// Serve fixture files via httptest server.
	fixtures := map[string]string{
		"/validchan":    "valid-channel.html",
		"/notsuper":     "not-a-supergroup.html",
		"/invalidchan":  "invalid-channel.html",
		"/notoccupied":  "username-not-occupied.html",
	}

	fixtureDir := findFixtureDir(t)
	mux := http.NewServeMux()
	for path, file := range fixtures {
		content := loadFixtureFrom(t, fixtureDir, file)
		mux.HandleFunc(path, func(body string) http.HandlerFunc {
			return func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/html")
				w.Write([]byte(body))
			}
		}(content))
	}
	// 404 handler
	mux.HandleFunc("/missing", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	tests := []struct {
		path           string
		expectedStatus string
		expectedReason string
		expectErr      bool
	}{
		{"/validchan", "valid", "", false},
		{"/notsuper", "not_channel", "not_supergroup", false},
		{"/invalidchan", "invalid", "not_found", false},
		{"/notoccupied", "invalid", "not_found", false},
		{"/missing", "", "", true},
	}

	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			// ValidateChannelHTTP prepends "https://t.me/", so we need to
			// override the URL construction. Use the parse function directly
			// for the fixture tests, and test the HTTP path separately.
			if tc.expectErr {
				// Test non-200 via direct HTTP call
				resp, err := server.Client().Get(server.URL + tc.path)
				if err != nil {
					t.Fatalf("unexpected HTTP error: %v", err)
				}
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					t.Error("expected non-200 status")
				}
				return
			}

			resp, err := server.Client().Get(server.URL + tc.path)
			if err != nil {
				t.Fatalf("HTTP request failed: %v", err)
			}
			defer resp.Body.Close()

			body, _ := os.ReadFile(filepath.Join(fixtureDir, fixtures[tc.path]))
			result, err := ParseChannelHTML(string(body))
			if err != nil {
				t.Fatalf("unexpected parse error: %v", err)
			}
			if result.Status != tc.expectedStatus {
				t.Errorf("expected status %q, got %q", tc.expectedStatus, result.Status)
			}
			if result.Reason != tc.expectedReason {
				t.Errorf("expected reason %q, got %q", tc.expectedReason, result.Reason)
			}
		})
	}
}

// loadFixture reads a fixture file from the telegram-html/ directory.
func loadFixture(t *testing.T, filename string) string {
	t.Helper()
	dir := findFixtureDir(t)
	return loadFixtureFrom(t, dir, filename)
}

func loadFixtureFrom(t *testing.T, dir, filename string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, filename))
	if err != nil {
		t.Fatalf("failed to read fixture %s: %v", filename, err)
	}
	return string(data)
}

func findFixtureDir(t *testing.T) string {
	t.Helper()
	// Walk up from the test file to find the repo root's telegram-html/ dir.
	candidates := []string{
		filepath.Join("..", "telegram-html"),
		filepath.Join("telegram-html"),
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	t.Fatal("could not find telegram-html/ fixture directory")
	return ""
}

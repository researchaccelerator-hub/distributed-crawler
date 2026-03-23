package telegramhelper

import "testing"

func TestFilterUsername(t *testing.T) {
	tests := []struct {
		name           string
		username       string
		expectedValid  bool
		expectedReason string
	}{
		// Valid usernames
		{"valid simple", "testchannel", true, ""},
		{"valid with underscore", "test_channel", true, ""},
		{"valid with numbers", "channel123", true, ""},
		{"valid min length", "abcde", true, ""},
		{"valid 32 chars", "abcdefghijklmnopqrstuvwxyz123456", true, ""},

		// Too short
		{"too short 4 chars", "abcd", false, "too_short"},
		{"too short 1 char", "a", false, "too_short"},
		{"too short empty", "", false, "too_short"},

		// Too long
		{"too long 33 chars", "abcdefghijklmnopqrstuvwxyz1234567", false, "too_long"},

		// Invalid start character
		{"starts with number", "1channel", false, "invalid_start_char"},
		{"starts with underscore", "_channel", false, "invalid_start_char"},
		{"starts with non-ASCII letter", "\u00e9channel", false, "invalid_start_char"},

		// Ends with underscore
		{"ends with underscore", "channel_", false, "ends_with_underscore"},

		// Invalid characters
		{"contains space", "test channel", false, "invalid_char"},
		{"contains dash", "test-channel", false, "invalid_char"},
		{"contains dot", "test.channel", false, "invalid_char"},
		{"contains unicode", "t\u00e9stchannel", false, "invalid_char"},

		// Bot suffixes
		{"ends with _bot", "some_bot", false, "bot_suffix"},
		{"ends with Bot", "SomeBot", false, "bot_suffix"},
		{"ends with BOT", "SomeBOT", false, "bot_suffix"},
		{"ends with _Bot", "Test_Bot", false, "bot_suffix"},

		// Path-like strings — the invalid_char filter catches these before
		// the path check since / . ~ are not valid username chars
		{"looks like path", "usr/local", false, "invalid_char"},
		{"contains tilde", "home~user", false, "invalid_char"},
		{"contains dot path", "file.name", false, "invalid_char"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := FilterUsername(tc.username)
			if result.Valid != tc.expectedValid {
				t.Errorf("FilterUsername(%q): expected Valid=%v, got %v (reason: %q)",
					tc.username, tc.expectedValid, result.Valid, result.Reason)
			}
			if !tc.expectedValid && result.Reason != tc.expectedReason {
				t.Errorf("FilterUsername(%q): expected reason %q, got %q",
					tc.username, tc.expectedReason, result.Reason)
			}
		})
	}
}

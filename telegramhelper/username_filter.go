package telegramhelper

import (
	"strings"
	"unicode"
)

// UsernameFilterResult holds the outcome of a username pre-filter check.
type UsernameFilterResult struct {
	Valid  bool
	Reason string // empty if valid; otherwise a short human-readable reason
}

// FilterUsername checks whether a string looks like a plausible Telegram channel
// username, based on Telegram's documented rules and known false-positive patterns.
//
// Telegram username rules:
//   - 5–32 characters
//   - Alphanumeric (ASCII) plus underscore
//   - Must start with a letter
//   - Must not end with underscore
//
// Additional heuristic filters:
//   - Known bot suffixes (_bot, Bot) — bots are never supergroups
//   - Strings that look like file paths (contain /, ., ~)
func FilterUsername(username string) UsernameFilterResult {
	if len(username) < 5 {
		return UsernameFilterResult{Valid: false, Reason: "too_short"}
	}
	if len(username) > 32 {
		return UsernameFilterResult{Valid: false, Reason: "too_long"}
	}

	// Must start with a letter.
	first := rune(username[0])
	if !unicode.IsLetter(first) || first > 127 {
		// Telegram only allows ASCII letters as the first character.
		return UsernameFilterResult{Valid: false, Reason: "invalid_start_char"}
	}

	// Must not end with underscore.
	if username[len(username)-1] == '_' {
		return UsernameFilterResult{Valid: false, Reason: "ends_with_underscore"}
	}

	// Check all characters are valid (ASCII alphanumeric + underscore).
	for _, ch := range username {
		if !isValidUsernameChar(ch) {
			return UsernameFilterResult{Valid: false, Reason: "invalid_char"}
		}
	}

	// Reject strings that look like file paths or system strings.
	if strings.ContainsAny(username, "/\\~.") {
		return UsernameFilterResult{Valid: false, Reason: "looks_like_path"}
	}

	// Reject known bot suffixes — bots are never supergroups.
	lower := strings.ToLower(username)
	if strings.HasSuffix(lower, "_bot") || strings.HasSuffix(lower, "bot") {
		// Exception: the suffix "bot" alone shouldn't reject "SomeRobot" (7 chars)
		// but "SomeBot" or "some_bot" are clearly bots.
		// Heuristic: if it ends with exactly "bot" or "_bot" (case-insensitive), reject.
		return UsernameFilterResult{Valid: false, Reason: "bot_suffix"}
	}

	return UsernameFilterResult{Valid: true}
}

func isValidUsernameChar(ch rune) bool {
	if ch >= 'a' && ch <= 'z' {
		return true
	}
	if ch >= 'A' && ch <= 'Z' {
		return true
	}
	if ch >= '0' && ch <= '9' {
		return true
	}
	return ch == '_'
}

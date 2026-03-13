package telegramhelper

// Tests for extractChannelLinksFromMessage and extractLinksFromFormattedText.
//
// Key regression coverage:
//   - UTF-16 offset correctness: Cyrillic and emoji prefixes must not corrupt byte slices
//   - Media captions (Photo, Video, Document, Audio, Animation, VoiceNote) are processed
//   - Reserved t.me paths (joinchat, share, etc.) are filtered out
//   - Deduplication: same channel mentioned multiple ways yields one entry
//   - Unknown / nil content returns an empty slice without panicking

import (
	"sort"
	"testing"

	"github.com/zelenin/go-tdlib/client"
)

// helper to sort and compare string slices regardless of order
func sortedEqual(t *testing.T, want, got []string) {
	t.Helper()
	sort.Strings(want)
	sort.Strings(got)
	if len(want) != len(got) {
		t.Fatalf("length mismatch: want %v, got %v", want, got)
	}
	for i := range want {
		if want[i] != got[i] {
			t.Fatalf("element %d: want %q, got %q (full: %v vs %v)", i, want[i], got[i], want, got)
		}
	}
}

func msgText(ft *client.FormattedText) *client.Message {
	return &client.Message{Content: &client.MessageText{Text: ft}}
}

func msgPhoto(ft *client.FormattedText) *client.Message {
	return &client.Message{Content: &client.MessagePhoto{Caption: ft}}
}

func msgVideo(ft *client.FormattedText) *client.Message {
	return &client.Message{Content: &client.MessageVideo{Caption: ft}}
}

func msgDocument(ft *client.FormattedText) *client.Message {
	return &client.Message{Content: &client.MessageDocument{Caption: ft}}
}

func msgAnimation(ft *client.FormattedText) *client.Message {
	return &client.Message{Content: &client.MessageAnimation{Caption: ft}}
}

func msgAudio(ft *client.FormattedText) *client.Message {
	return &client.Message{Content: &client.MessageAudio{Caption: ft}}
}

func msgVoiceNote(ft *client.FormattedText) *client.Message {
	return &client.Message{Content: &client.MessageVoiceNote{Caption: ft}}
}

// --- Plain-text regex scan ---

func TestExtractChannelLinks_PlainTextTmeLink(t *testing.T) {
	msg := msgText(&client.FormattedText{Text: "Check out https://t.me/channelname for news"})
	got := extractChannelLinksFromMessage(msg)
	sortedEqual(t, []string{"channelname"}, got)
}

func TestExtractChannelLinks_PlainTextTmeLinkNoScheme(t *testing.T) {
	msg := msgText(&client.FormattedText{Text: "Visit t.me/somechan today"})
	got := extractChannelLinksFromMessage(msg)
	sortedEqual(t, []string{"somechan"}, got)
}

func TestExtractChannelLinks_PlainTextMultipleLinks(t *testing.T) {
	msg := msgText(&client.FormattedText{Text: "t.me/chanone and t.me/chantwo"})
	got := extractChannelLinksFromMessage(msg)
	sortedEqual(t, []string{"chanone", "chantwo"}, got)
}

// --- TextEntityTypeTextUrl ---

func TestExtractChannelLinks_TextEntityTypeTextUrl(t *testing.T) {
	ft := &client.FormattedText{
		Text: "click here",
		Entities: []*client.TextEntity{
			{
				Offset: 0, Length: 10,
				Type: &client.TextEntityTypeTextUrl{Url: "https://t.me/linkedchan"},
			},
		},
	}
	got := extractChannelLinksFromMessage(msgText(ft))
	sortedEqual(t, []string{"linkedchan"}, got)
}

func TestExtractChannelLinks_TextEntityTypeTextUrl_NonTme(t *testing.T) {
	ft := &client.FormattedText{
		Text: "link",
		Entities: []*client.TextEntity{
			{
				Offset: 0, Length: 4,
				Type: &client.TextEntityTypeTextUrl{Url: "https://example.com/page"},
			},
		},
	}
	got := extractChannelLinksFromMessage(msgText(ft))
	if len(got) != 0 {
		t.Fatalf("expected no channels from non-t.me URL, got %v", got)
	}
}

// --- TextEntityTypeMention (UTF-16 offset correctness) ---

func TestExtractChannelLinks_Mention_ASCII(t *testing.T) {
	// "Hello @testchan!"
	// @testchan starts at byte/UTF-16 offset 6, length 9
	text := "Hello @testchan!"
	ft := &client.FormattedText{
		Text: text,
		Entities: []*client.TextEntity{
			{Offset: 6, Length: 9, Type: &client.TextEntityTypeMention{}},
		},
	}
	got := extractChannelLinksFromMessage(msgText(ft))
	sortedEqual(t, []string{"testchan"}, got)
}

// Regression test for the P0 UTF-16 offset bug.
// "Привет @testchan" — each Cyrillic letter is 1 UTF-16 code unit but 2 UTF-8 bytes.
// If the code used raw byte offsets instead of utf16OffsetToBytes it would slice
// into the middle of a multi-byte rune and produce garbage / an incorrect channel name.
func TestExtractChannelLinks_Mention_CyrillicPrefix_UTF16Regression(t *testing.T) {
	// "Привет " = 7 UTF-16 code units (6 Cyrillic + 1 space)
	// "@testchan" starts at UTF-16 offset 7, length 9
	text := "Привет @testchan"
	ft := &client.FormattedText{
		Text: text,
		Entities: []*client.TextEntity{
			{Offset: 7, Length: 9, Type: &client.TextEntityTypeMention{}},
		},
	}
	got := extractChannelLinksFromMessage(msgText(ft))
	sortedEqual(t, []string{"testchan"}, got)
}

// Emoji test: U+1F600 (😀) occupies 2 UTF-16 code units and 4 UTF-8 bytes.
// "😀 @testchan" — @testchan starts at UTF-16 offset 3 (2 for emoji + 1 for space).
func TestExtractChannelLinks_Mention_EmojiPrefix_UTF16Regression(t *testing.T) {
	text := "😀 @testchan"
	ft := &client.FormattedText{
		Text: text,
		Entities: []*client.TextEntity{
			{Offset: 3, Length: 9, Type: &client.TextEntityTypeMention{}},
		},
	}
	got := extractChannelLinksFromMessage(msgText(ft))
	sortedEqual(t, []string{"testchan"}, got)
}

func TestExtractChannelLinks_Mention_ArabicPrefix_UTF16Regression(t *testing.T) {
	// Arabic letters are BMP (1 UTF-16 CU, 2 UTF-8 bytes each)
	// "مرحبا @testchan" — 5 Arabic chars + 1 space = UTF-16 offset 6 for @
	text := "مرحبا @testchan"
	ft := &client.FormattedText{
		Text: text,
		Entities: []*client.TextEntity{
			{Offset: 6, Length: 9, Type: &client.TextEntityTypeMention{}},
		},
	}
	got := extractChannelLinksFromMessage(msgText(ft))
	sortedEqual(t, []string{"testchan"}, got)
}

// --- TextEntityTypeUrl ---

func TestExtractChannelLinks_TextEntityTypeUrl_TmeLink(t *testing.T) {
	text := "See https://t.me/urlchan for details"
	// "https://t.me/urlchan" starts at offset 4, length 20
	ft := &client.FormattedText{
		Text: text,
		Entities: []*client.TextEntity{
			{Offset: 4, Length: 20, Type: &client.TextEntityTypeUrl{}},
		},
	}
	got := extractChannelLinksFromMessage(msgText(ft))
	sortedEqual(t, []string{"urlchan"}, got)
}

// --- Media captions ---

func TestExtractChannelLinks_PhotoCaption(t *testing.T) {
	ft := &client.FormattedText{Text: "t.me/photochan"}
	got := extractChannelLinksFromMessage(msgPhoto(ft))
	sortedEqual(t, []string{"photochan"}, got)
}

func TestExtractChannelLinks_VideoCaption(t *testing.T) {
	ft := &client.FormattedText{Text: "t.me/videochan"}
	got := extractChannelLinksFromMessage(msgVideo(ft))
	sortedEqual(t, []string{"videochan"}, got)
}

func TestExtractChannelLinks_DocumentCaption(t *testing.T) {
	ft := &client.FormattedText{Text: "t.me/docchan"}
	got := extractChannelLinksFromMessage(msgDocument(ft))
	sortedEqual(t, []string{"docchan"}, got)
}

func TestExtractChannelLinks_AnimationCaption(t *testing.T) {
	ft := &client.FormattedText{Text: "t.me/animchan"}
	got := extractChannelLinksFromMessage(msgAnimation(ft))
	sortedEqual(t, []string{"animchan"}, got)
}

func TestExtractChannelLinks_AudioCaption(t *testing.T) {
	ft := &client.FormattedText{Text: "t.me/audiochan"}
	got := extractChannelLinksFromMessage(msgAudio(ft))
	sortedEqual(t, []string{"audiochan"}, got)
}

func TestExtractChannelLinks_VoiceNoteCaption(t *testing.T) {
	ft := &client.FormattedText{Text: "t.me/voicechan"}
	got := extractChannelLinksFromMessage(msgVoiceNote(ft))
	sortedEqual(t, []string{"voicechan"}, got)
}

// --- Reserved paths ---

func TestExtractChannelLinks_ReservedPath_Joinchat(t *testing.T) {
	msg := msgText(&client.FormattedText{Text: "https://t.me/joinchat/abc123"})
	got := extractChannelLinksFromMessage(msg)
	if len(got) != 0 {
		t.Fatalf("expected joinchat to be filtered, got %v", got)
	}
}

func TestExtractChannelLinks_ReservedPath_Share(t *testing.T) {
	msg := msgText(&client.FormattedText{Text: "https://t.me/share/url?url=x"})
	got := extractChannelLinksFromMessage(msg)
	if len(got) != 0 {
		t.Fatalf("expected share to be filtered, got %v", got)
	}
}

func TestExtractChannelLinks_ReservedPath_Proxy(t *testing.T) {
	msg := msgText(&client.FormattedText{Text: "https://t.me/proxy?server=x"})
	got := extractChannelLinksFromMessage(msg)
	if len(got) != 0 {
		t.Fatalf("expected proxy to be filtered, got %v", got)
	}
}

// --- Deduplication ---

func TestExtractChannelLinks_Deduplication(t *testing.T) {
	// Same channel mentioned as plain URL and as a TextUrl entity
	ft := &client.FormattedText{
		Text: "Check t.me/samechan",
		Entities: []*client.TextEntity{
			{
				Offset: 6, Length: 13,
				Type: &client.TextEntityTypeTextUrl{Url: "https://t.me/samechan"},
			},
		},
	}
	got := extractChannelLinksFromMessage(msgText(ft))
	if len(got) != 1 {
		t.Fatalf("expected 1 unique channel, got %v", got)
	}
	if got[0] != "samechan" {
		t.Fatalf("expected 'samechan', got %q", got[0])
	}
}

// --- Case normalization ---

func TestExtractChannelLinks_CaseNormalization(t *testing.T) {
	msg := msgText(&client.FormattedText{Text: "t.me/MixedCase"})
	got := extractChannelLinksFromMessage(msg)
	sortedEqual(t, []string{"mixedcase"}, got)
}

// --- Unknown / unsupported content types ---

func TestExtractChannelLinks_UnknownContentType(t *testing.T) {
	// MessageSticker has no FormattedText caption — should return empty slice
	msg := &client.Message{Content: &client.MessageSticker{}}
	got := extractChannelLinksFromMessage(msg)
	if len(got) != 0 {
		t.Fatalf("expected empty slice for unsupported content type, got %v", got)
	}
}

// --- Channel name too short (< 4 chars) filtered by regex ---

func TestExtractChannelLinks_TooShortName(t *testing.T) {
	// t.me/abc — only 3 chars, doesn't match [a-zA-Z0-9_]{4,32}
	msg := msgText(&client.FormattedText{Text: "t.me/abc"})
	got := extractChannelLinksFromMessage(msg)
	if len(got) != 0 {
		t.Fatalf("expected short name to be filtered, got %v", got)
	}
}

// --- Mention at end of string (no trailing character) ---

func TestExtractChannelLinks_Mention_AtEndOfString(t *testing.T) {
	text := "@endchan"
	ft := &client.FormattedText{
		Text: text,
		Entities: []*client.TextEntity{
			{Offset: 0, Length: 8, Type: &client.TextEntityTypeMention{}},
		},
	}
	got := extractChannelLinksFromMessage(msgText(ft))
	sortedEqual(t, []string{"endchan"}, got)
}

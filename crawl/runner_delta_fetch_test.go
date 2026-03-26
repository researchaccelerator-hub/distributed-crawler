package crawl

import (
	"testing"
	"time"

	"github.com/zelenin/go-tdlib/client"
)

func TestMaxMessageDate_ReturnsNewest(t *testing.T) {
	msgs := []*client.Message{
		{Date: 1711440600}, // 2024-03-26 10:30:00 UTC
		{Date: 1711500000}, // 2024-03-27 03:00:00 UTC
		{Date: 1711400000}, // 2024-03-25 23:13:20 UTC
	}
	got := maxMessageDate(msgs)
	expected := time.Unix(1711500000, 0)
	if !got.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, got)
	}
}

func TestMaxMessageDate_EmptySlice(t *testing.T) {
	got := maxMessageDate(nil)
	if !got.IsZero() {
		t.Fatalf("expected zero time for nil slice, got %v", got)
	}
}

func TestMaxMessageDate_SkipsNilMessages(t *testing.T) {
	msgs := []*client.Message{
		nil,
		{Date: 1711440600},
		nil,
	}
	got := maxMessageDate(msgs)
	expected := time.Unix(1711440600, 0)
	if !got.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, got)
	}
}

func TestMaxMessageDate_SkipsZeroDate(t *testing.T) {
	msgs := []*client.Message{
		{Date: 0},
		{Date: 1711440600},
	}
	got := maxMessageDate(msgs)
	expected := time.Unix(1711440600, 0)
	if !got.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, got)
	}
}

func TestMaxMessageDate_ReturnsUTC(t *testing.T) {
	msgs := []*client.Message{
		{Date: 1711440600},
	}
	got := maxMessageDate(msgs)
	if got.Location() != time.UTC {
		t.Fatalf("expected UTC location, got %v", got.Location())
	}
}

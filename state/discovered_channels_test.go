package state

import (
	"errors"
	"testing"
)

func TestDiscoveredChannels_Add(t *testing.T) {
	dc := NewDiscoveredChannels()

	// First add should succeed.
	if err := dc.Add("telegram"); err != nil {
		t.Fatalf("first Add returned unexpected error: %v", err)
	}

	// Second add of the same channel should return ErrChannelExists.
	err := dc.Add("telegram")
	if err == nil {
		t.Fatal("expected error on duplicate Add, got nil")
	}
	if !errors.Is(err, ErrChannelExists) {
		t.Fatalf("expected ErrChannelExists, got: %v", err)
	}

	// Different channel should succeed.
	if err := dc.Add("durov"); err != nil {
		t.Fatalf("Add of different channel returned unexpected error: %v", err)
	}
}

func TestDiscoveredChannels_Contains(t *testing.T) {
	dc := NewDiscoveredChannels()

	if dc.Contains("telegram") {
		t.Fatal("expected Contains to return false for missing channel")
	}

	dc.Add("telegram")

	if !dc.Contains("telegram") {
		t.Fatal("expected Contains to return true after Add")
	}

	if dc.Contains("durov") {
		t.Fatal("expected Contains to return false for different channel")
	}
}

func TestDiscoveredChannels_DuplicatePreservesState(t *testing.T) {
	dc := NewDiscoveredChannels()
	dc.Add("ch1")
	dc.Add("ch2")
	dc.Add("ch3")

	// Duplicate add should not alter the set.
	dc.Add("ch2")

	// All originals should still be present.
	for _, ch := range []string{"ch1", "ch2", "ch3"} {
		if !dc.Contains(ch) {
			t.Fatalf("expected %q to be present after duplicate add", ch)
		}
	}

	// A genuinely new channel should still work after duplicates.
	if err := dc.Add("ch4"); err != nil {
		t.Fatalf("Add after duplicate returned unexpected error: %v", err)
	}
}

func TestErrChannelExists_Wrapping(t *testing.T) {
	dc := NewDiscoveredChannels()
	dc.Add("test")

	err := dc.Add("test")
	if err == nil {
		t.Fatal("expected error")
	}

	// Error message should contain the channel name.
	if !errors.Is(err, ErrChannelExists) {
		t.Fatalf("error should wrap ErrChannelExists: %v", err)
	}
	if err.Error() != "test: channel already exists" {
		t.Fatalf("unexpected error message: %q", err.Error())
	}
}

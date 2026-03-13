package dapr

import (
	"testing"
	"time"

	"github.com/researchaccelerator-hub/telegram-scraper/common"
)

// TestCalculateDateFilters_DateBetween verifies that when both DateBetweenMin and
// DateBetweenMax are set they take precedence over all other date fields.
func TestCalculateDateFilters_DateBetween(t *testing.T) {
	minDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	maxDate := time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)

	cfg := common.CrawlerConfig{
		DateBetweenMin: minDate,
		DateBetweenMax: maxDate,
		PostRecency:    time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC), // should be ignored
		MinPostDate:    time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC), // should be ignored
	}

	from, to := CalculateDateFilters(cfg)

	if !from.Equal(minDate) {
		t.Errorf("fromTime: expected %v, got %v", minDate, from)
	}
	if !to.Equal(maxDate) {
		t.Errorf("toTime: expected %v, got %v", maxDate, to)
	}
}

// TestCalculateDateFilters_DateBetween_OnlyMinSet verifies that when only
// DateBetweenMin is set (without DateBetweenMax) it does not activate the
// date-between branch — PostRecency should be used instead.
func TestCalculateDateFilters_DateBetween_OnlyMinSet(t *testing.T) {
	minDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	recency := time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC)

	cfg := common.CrawlerConfig{
		DateBetweenMin: minDate,
		// DateBetweenMax is zero
		PostRecency: recency,
	}

	from, _ := CalculateDateFilters(cfg)

	if !from.Equal(recency) {
		t.Errorf("expected PostRecency to be used when DateBetweenMax is zero; fromTime=%v", from)
	}
}

// TestCalculateDateFilters_PostRecency verifies that when DateBetween is not set
// but PostRecency is set, fromTime = PostRecency and toTime ≈ now.
func TestCalculateDateFilters_PostRecency(t *testing.T) {
	recency := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)

	cfg := common.CrawlerConfig{
		PostRecency: recency,
		MinPostDate: time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC), // should be ignored
	}

	before := time.Now()
	from, to := CalculateDateFilters(cfg)
	after := time.Now()

	if !from.Equal(recency) {
		t.Errorf("fromTime: expected %v, got %v", recency, from)
	}
	if to.Before(before) || to.After(after) {
		t.Errorf("toTime should be ≈ now; before=%v, to=%v, after=%v", before, to, after)
	}
}

// TestCalculateDateFilters_MinPostDate verifies that when neither DateBetween nor
// PostRecency is set, fromTime = MinPostDate and toTime ≈ now.
func TestCalculateDateFilters_MinPostDate(t *testing.T) {
	minDate := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)

	cfg := common.CrawlerConfig{
		MinPostDate: minDate,
	}

	before := time.Now()
	from, to := CalculateDateFilters(cfg)
	after := time.Now()

	if !from.Equal(minDate) {
		t.Errorf("fromTime: expected %v, got %v", minDate, from)
	}
	if to.Before(before) || to.After(after) {
		t.Errorf("toTime should be ≈ now; before=%v, to=%v, after=%v", before, to, after)
	}
}

// TestCalculateDateFilters_AllZero verifies that with all date fields zero,
// fromTime is the zero time and toTime ≈ now.
func TestCalculateDateFilters_AllZero(t *testing.T) {
	cfg := common.CrawlerConfig{}

	before := time.Now()
	from, to := CalculateDateFilters(cfg)
	after := time.Now()

	if !from.IsZero() {
		t.Errorf("expected zero fromTime, got %v", from)
	}
	if to.Before(before) || to.After(after) {
		t.Errorf("toTime should be ≈ now; before=%v, to=%v, after=%v", before, to, after)
	}
}

package cron

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// newYork returns the America/New_York location or fails the test.
func newYork(t *testing.T) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load location: %v", err)
	}
	return loc
}

// allDays returns the bit mask for every day of the month plus the star bit.
func allDays() uint64 {
	var bits uint64 = starBit
	for d := 1; d <= 31; d++ {
		bits |= 1 << uint(d)
	}
	return bits
}

// allMonths returns the bit mask for every month plus the star bit.
func allMonths() uint64 {
	var bits uint64 = starBit
	for m := 1; m <= 12; m++ {
		bits |= 1 << uint(m)
	}
	return bits
}

// allWeekdays returns the bit mask for every day of the week plus the star bit.
func allWeekdays() uint64 {
	var bits uint64 = starBit
	for d := 0; d <= 6; d++ {
		bits |= 1 << uint(d)
	}
	return bits
}

// allHours returns the bit mask for every hour plus the star bit.
func allHours() uint64 {
	var bits uint64 = starBit
	for h := 0; h <= 23; h++ {
		bits |= 1 << uint(h)
	}
	return bits
}

// dailyAt returns a SpecSchedule firing at the given wall-clock time every
// day in loc.
func dailyAt(hour, minute int, loc *time.Location) *SpecSchedule {
	return &SpecSchedule{
		Second:   1 << 0,
		Minute:   1 << uint(minute),
		Hour:     1 << uint(hour),
		Dom:      allDays(),
		Month:    allMonths(),
		Dow:      allWeekdays(),
		Location: loc,
	}
}

// every30 returns a SpecSchedule firing every 30 minutes in loc.
func every30(loc *time.Location) *SpecSchedule {
	return &SpecSchedule{
		Second:   1 << 0,
		Minute:   1<<0 | 1<<30,
		Hour:     allHours(),
		Dom:      allDays(),
		Month:    allMonths(),
		Dow:      allWeekdays(),
		Location: loc,
	}
}

// TestMomentReportFallBack verifies that during a fall-back (DST ends), both
// local 01:30 activations are retained with different UTC identities.
func TestMomentReportFallBack(t *testing.T) {
	loc := newYork(t)
	sched := dailyAt(1, 30, loc)

	// 2023-11-05 is the fall-back day in America/New_York (02:00 EDT -> 01:00 EST).
	start := time.Date(2023, 11, 5, 0, 0, 0, 0, time.UTC)
	end := time.Date(2023, 11, 6, 0, 0, 0, 0, time.UTC)

	report, err := MomentReport(sched, start, end, loc)
	if err != nil {
		t.Fatalf("MomentReport: %v", err)
	}

	if len(report.Moments) != 2 {
		t.Fatalf("expected 2 moments, got %d: %+v", len(report.Moments), report.Moments)
	}

	first := report.Moments[0]
	second := report.Moments[1]

	// Both have wall-clock 01:30.
	if first.Wall.Format("15:04") != "01:30" {
		t.Errorf("first wall = %s, want 01:30", first.Wall.Format("15:04"))
	}
	if second.Wall.Format("15:04") != "01:30" {
		t.Errorf("second wall = %s, want 01:30", second.Wall.Format("15:04"))
	}

	// UTC identities differ.
	if !first.UTC.Before(second.UTC) {
		t.Errorf("first UTC %s should be before second UTC %s", first.UTC, second.UTC)
	}

	// First is EDT (UTC-4), second is EST (UTC-5).
	if first.Offset != -4*3600 {
		t.Errorf("first offset = %d, want -14400 (EDT)", first.Offset)
	}
	if second.Offset != -5*3600 {
		t.Errorf("second offset = %d, want -18000 (EST)", second.Offset)
	}

	// The second 01:30 is the duplicate.
	if !second.Duplicate {
		t.Errorf("second 01:30 should be marked duplicate")
	}
	if first.Duplicate {
		t.Errorf("first 01:30 should not be marked duplicate")
	}

	// Neither is a jump (fall-back is not a spring-forward jump).
	if first.Jump || second.Jump {
		t.Errorf("fall-back moments should not be marked jump")
	}
}

// TestMomentReportSpringForward verifies that during a spring-forward the
// wall-clock jump is marked.
func TestMomentReportSpringForward(t *testing.T) {
	loc := newYork(t)
	sched := every30(loc)

	// 2023-03-12 is the spring-forward day (02:00 EST -> 03:00 EDT).
	start := time.Date(2023, 3, 12, 6, 0, 0, 0, time.UTC)
	end := time.Date(2023, 3, 12, 8, 0, 0, 0, time.UTC)

	report, err := MomentReport(sched, start, end, loc)
	if err != nil {
		t.Fatalf("MomentReport: %v", err)
	}

	// Expect 06:30 (01:30 EST), 07:00 (03:00 EDT), 07:30 (03:30 EDT).
	if len(report.Moments) != 3 {
		t.Fatalf("expected 3 moments, got %d: %+v", len(report.Moments), report.Moments)
	}

	// The 07:00 UTC moment (03:00 EDT) is after the jump.
	jumpIdx := -1
	for i, m := range report.Moments {
		if m.Jump {
			if jumpIdx != -1 {
				t.Errorf("multiple jumps marked")
			}
			jumpIdx = i
		}
	}
	if jumpIdx == -1 {
		t.Fatalf("expected a jump to be marked, got none")
	}
	if report.Moments[jumpIdx].UTC.Format("15:04") != "07:00" {
		t.Errorf("jump at UTC %s, want 07:00", report.Moments[jumpIdx].UTC.Format("15:04"))
	}
	if report.Moments[jumpIdx].Wall.Format("15:04") != "03:00" {
		t.Errorf("jump wall = %s, want 03:00", report.Moments[jumpIdx].Wall.Format("15:04"))
	}
}

// sameSchedule is a bad Schedule that always returns the same time.
type sameSchedule struct {
	t time.Time
}

func (s sameSchedule) Next(time.Time) time.Time {
	return s.t
}

// TestMomentReportBadSchedule verifies that a Schedule returning the same
// moment stops the report with an error.
func TestMomentReportBadSchedule(t *testing.T) {
	loc := time.UTC
	fixed := time.Date(2023, 11, 5, 1, 30, 0, 0, time.UTC)
	sched := sameSchedule{t: fixed}

	start := time.Date(2023, 11, 5, 0, 0, 0, 0, time.UTC)
	end := time.Date(2023, 11, 6, 0, 0, 0, 0, time.UTC)

	report, err := MomentReport(sched, start, end, loc)
	if err == nil {
		t.Fatalf("expected error for bad schedule, got report: %+v", report)
	}
	if report != nil {
		t.Errorf("expected nil report on error, got %+v", report)
	}
	if !strings.Contains(err.Error(), "did not advance") {
		t.Errorf("error = %q, want it to mention 'did not advance'", err.Error())
	}
}

// backwardSchedule is a bad Schedule that returns a time before the previous.
type backwardSchedule struct {
	t time.Time
	n int
}

func (s *backwardSchedule) Next(prev time.Time) time.Time {
	s.n++
	if s.n == 1 {
		return s.t
	}
	return prev.Add(-1 * time.Hour)
}

// TestMomentReportBackwardSchedule verifies that a Schedule returning a
// moment before the previous stops with an error.
func TestMomentReportBackwardSchedule(t *testing.T) {
	loc := time.UTC
	fixed := time.Date(2023, 11, 5, 1, 30, 0, 0, time.UTC)
	sched := &backwardSchedule{t: fixed}

	start := time.Date(2023, 11, 5, 0, 0, 0, 0, time.UTC)
	end := time.Date(2023, 11, 6, 0, 0, 0, 0, time.UTC)

	_, err := MomentReport(sched, start, end, loc)
	if err == nil {
		t.Fatalf("expected error for backward schedule, got nil")
	}
	if !strings.Contains(err.Error(), "did not advance") {
		t.Errorf("error = %q, want it to mention 'did not advance'", err.Error())
	}
}

// TestMomentReportEndpoint verifies that the endpoint is not listed as an
// extra item (half-open range).
func TestMomentReportEndpoint(t *testing.T) {
	loc := time.UTC
	// Every hour on the hour.
	sched := &SpecSchedule{
		Second:   1 << 0,
		Minute:   1 << 0,
		Hour:     allHours(),
		Dom:      allDays(),
		Month:    allMonths(),
		Dow:      allWeekdays(),
		Location: loc,
	}

	// Range [10:00, 12:00). Next(10:00) = 11:00, Next(11:00) = 12:00 (excluded).
	start := time.Date(2023, 11, 5, 10, 0, 0, 0, time.UTC)
	end := time.Date(2023, 11, 5, 12, 0, 0, 0, time.UTC)

	report, err := MomentReport(sched, start, end, loc)
	if err != nil {
		t.Fatalf("MomentReport: %v", err)
	}

	// Only 11:00 is in (10:00, 12:00). 12:00 is the endpoint and excluded.
	if len(report.Moments) != 1 {
		t.Fatalf("expected 1 moment (endpoint excluded), got %d: %+v", len(report.Moments), report.Moments)
	}
	if report.Moments[0].UTC.Format("15:04") != "11:00" {
		t.Errorf("moment = %s, want 11:00", report.Moments[0].UTC.Format("15:04"))
	}
}

// TestMomentReportInvalidRange verifies that start >= end returns an error.
func TestMomentReportInvalidRange(t *testing.T) {
	sched := dailyAt(1, 30, time.UTC)
	start := time.Date(2023, 11, 6, 0, 0, 0, 0, time.UTC)
	end := time.Date(2023, 11, 5, 0, 0, 0, 0, time.UTC)

	_, err := MomentReport(sched, start, end, time.UTC)
	if err == nil {
		t.Fatalf("expected error for invalid range, got nil")
	}
}

// TestMomentReportNilSchedule verifies that a nil schedule returns an error.
func TestMomentReportNilSchedule(t *testing.T) {
	start := time.Date(2023, 11, 5, 0, 0, 0, 0, time.UTC)
	end := time.Date(2023, 11, 6, 0, 0, 0, 0, time.UTC)

	_, err := MomentReport(nil, start, end, time.UTC)
	if err == nil {
		t.Fatalf("expected error for nil schedule, got nil")
	}
}

// ExampleMomentReport shows a report over a fall-back range in
// America/New_York. The two local 01:30 activations are retained with
// different UTC identities; the second is marked as a duplicate.
func ExampleMomentReport() {
	loc, _ := time.LoadLocation("America/New_York")
	sched, _ := ParseStandard("CRON_TZ=America/New_York 30 1 * * *")

	start := time.Date(2023, 11, 5, 0, 0, 0, 0, time.UTC)
	end := time.Date(2023, 11, 6, 0, 0, 0, 0, time.UTC)

	report, err := MomentReport(sched, start, end, loc)
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	for _, m := range report.Moments {
		flags := ""
		if m.Duplicate {
			flags += " duplicate"
		}
		if m.Jump {
			flags += " jump"
		}
		fmt.Printf("UTC=%s wall=%s offset=%d%s\n",
			m.UTC.Format("15:04"), m.Wall.Format("15:04"), m.Offset, flags)
	}

	// Output:
	// UTC=05:30 wall=01:30 offset=-14400
	// UTC=06:30 wall=01:30 offset=-18000 duplicate
}
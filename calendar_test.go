package cron

import (
	"fmt"
	"sync"
	"testing"
	"time"
	_ "time/tzdata"
)

func utcTime(y int, mon time.Month, d, h, min int) time.Time {
	return time.Date(y, mon, d, h, min, 0, 0, time.UTC)
}

func mustParseStandard(t *testing.T, spec string) Schedule {
	t.Helper()
	schedule, err := ParseStandard(spec)
	if err != nil {
		t.Fatalf("parse %q: %v", spec, err)
	}
	return schedule
}

func mustParse(t *testing.T, spec string) Schedule {
	t.Helper()
	schedule, err := NewParser(Minute | Hour | Dom | Month | Dow | Descriptor).Parse(spec)
	if err != nil {
		t.Fatalf("parse %q: %v", spec, err)
	}
	return schedule
}

func mustCalendar(t *testing.T, locationName string, dates []DisabledDate, weekdays []time.Weekday, intervals []TimeInterval) *ExceptionCalendar {
	t.Helper()
	calendar, err := NewCalendar(locationName, dates, weekdays, intervals)
	if err != nil {
		t.Fatalf("build calendar: %v", err)
	}
	return calendar
}

func mustLocation(t *testing.T, name string) *time.Location {
	t.Helper()
	location, err := time.LoadLocation(name)
	if err != nil {
		t.Fatal(err)
	}
	return location
}

// Acceptance: every minute in UTC; disabling [10:01,10:03) makes the first
// activation after 10:00 land exactly on 10:03 (the interval end is allowed).
// Also disabling that whole date pushes the result into the next legal date.
func TestExceptionCalendarUTCAcceptance(t *testing.T) {
	schedule := mustParseStandard(t, "* * * * *")
	start := utcTime(2023, time.January, 1, 10, 0)

	intervalCalendar := mustCalendar(t, "UTC", nil, nil, []TimeInterval{
		{utcTime(2023, time.January, 1, 10, 1), utcTime(2023, time.January, 1, 10, 3)},
	})
	if got := intervalCalendar.Wrap(schedule).Next(start); !got.Equal(utcTime(2023, time.January, 1, 10, 3)) {
		t.Fatalf("interval end activation lost: got %v want 10:03", got)
	}

	dayCalendar := mustCalendar(t, "UTC", []DisabledDate{{2023, time.January, 1}}, nil, []TimeInterval{
		{utcTime(2023, time.January, 1, 10, 1), utcTime(2023, time.January, 1, 10, 3)},
	})
	want := utcTime(2023, time.January, 2, 0, 0)
	if got := dayCalendar.Wrap(schedule).Next(start); !got.Equal(want) {
		t.Fatalf("got %v want next legal date %v", got, want)
	}
}

// Spring forward in New York: 2023-03-12 has only 23 local hours. Blocking the
// date skips to midnight of the next calendar date using calendar arithmetic,
// not a fixed 24-hour offset.
func TestExceptionCalendarNewYorkSpringForward(t *testing.T) {
	schedule := mustParse(t, "CRON_TZ=America/New_York * * * * *")
	calendar := mustCalendar(t, "America/New_York", []DisabledDate{{2023, time.March, 12}}, nil, nil)

	newYork := mustLocation(t, "America/New_York")
	start := time.Date(2023, time.March, 12, 0, 0, 0, 0, newYork)
	want := time.Date(2023, time.March, 13, 0, 0, 0, 0, newYork)
	if got := calendar.Wrap(schedule).Next(start); !got.Equal(want) {
		t.Fatalf("spring forward day: got %v want %v", got, want)
	}
}

// Fall back in New York: local 01:30 happens twice (05:30Z and 06:30Z). The
// two UTC instants behind the repeated local time are judged independently.
func TestExceptionCalendarNewYorkFallBackRepeatedHour(t *testing.T) {
	schedule := mustParse(t, "CRON_TZ=America/New_York * * * * *")
	calendar := mustCalendar(t, "America/New_York", nil, nil, []TimeInterval{{
		utcTime(2023, time.November, 5, 5, 0),
		utcTime(2023, time.November, 5, 6, 30),
	}})

	start := utcTime(2023, time.November, 5, 5, 0)
	want := utcTime(2023, time.November, 5, 6, 30)
	if got := calendar.Wrap(schedule).Next(start); !got.Equal(want) {
		t.Fatalf("repeated local hour instants not distinguished: got %v want %v", got, want)
	}

	// Blocking the whole date forbids both occurrences and reaches the next day.
	dayCalendar := mustCalendar(t, "America/New_York", []DisabledDate{{2023, time.November, 5}}, nil, nil)
	wantDay := time.Date(2023, time.November, 6, 0, 0, 0, 0, mustLocation(t, "America/New_York"))
	if got := dayCalendar.Wrap(schedule).Next(utcTime(2023, time.November, 5, 4, 0)); !got.Equal(wantDay) {
		t.Fatalf("fall back day blocked: got %v want %v", got, wantDay)
	}
}

// Leap day: 2024-02-29 exists and is blocked; the search resumes on 03-01.
// Feb 29 in a non-leap year is rejected while building the calendar.
func TestExceptionCalendarLeapDay(t *testing.T) {
	schedule := mustParseStandard(t, "* * * * *")
	calendar := mustCalendar(t, "UTC", []DisabledDate{{2024, time.February, 29}}, nil, nil)
	start := utcTime(2024, time.February, 29, 12, 0)
	want := utcTime(2024, time.March, 1, 0, 0)
	if got := calendar.Wrap(schedule).Next(start); !got.Equal(want) {
		t.Fatalf("leap day: got %v want %v", got, want)
	}

	if _, err := NewCalendar("UTC", []DisabledDate{{2023, time.February, 29}}, nil, nil); err == nil {
		t.Fatal("expected error for Feb 29 in non-leap year")
	}
}

// Overlapping and adjacent UTC intervals are merged; legal instants at
// touching ends survive.
func TestExceptionCalendarMergedIntervals(t *testing.T) {
	schedule := mustParseStandard(t, "* * * * *")
	calendar := mustCalendar(t, "UTC", nil, nil, []TimeInterval{
		{utcTime(2023, time.January, 1, 10, 5), utcTime(2023, time.January, 1, 10, 9)},
		{utcTime(2023, time.January, 1, 10, 2), utcTime(2023, time.January, 1, 10, 5)},
		{utcTime(2023, time.January, 1, 10, 0), utcTime(2023, time.January, 1, 10, 2)},
	})
	if len(calendar.intervals) != 1 {
		t.Fatalf("intervals not merged: %d intervals", len(calendar.intervals))
	}
	merged := calendar.intervals[0]
	if !merged.Start.Equal(utcTime(2023, time.January, 1, 10, 0)) || !merged.End.Equal(utcTime(2023, time.January, 1, 10, 9)) {
		t.Fatalf("unexpected merged interval %v", merged)
	}
	if got := calendar.Wrap(schedule).Next(utcTime(2023, time.January, 1, 9, 59)); !got.Equal(utcTime(2023, time.January, 1, 10, 9)) {
		t.Fatalf("touching end lost: got %v", got)
	}
}

// Next is strictly later than t even when t itself is legal.
func TestCalendarScheduleNextStrictlyLater(t *testing.T) {
	schedule := mustParseStandard(t, "* * * * *")
	wrapped := mustCalendar(t, "UTC", nil, nil, nil).Wrap(schedule)
	t0 := utcTime(2023, time.January, 1, 10, 0)
	if got := wrapped.Next(t0); !got.Equal(utcTime(2023, time.January, 1, 10, 1)) {
		t.Fatalf("got %v want 10:01", got)
	}
}

// A candidate exactly on the cutoff is allowed; beyond it yields zero time.
func TestCalendarScheduleDeadlineBoundary(t *testing.T) {
	schedule := mustParseStandard(t, "* * * * *")
	wrapped := mustCalendar(t, "UTC", []DisabledDate{{2023, time.January, 1}}, nil, nil).Wrap(schedule)
	start := utcTime(2023, time.January, 1, 23, 58)
	deadline := utcTime(2023, time.January, 2, 0, 0)
	if got := wrapped.NextUntil(start, deadline); !got.Equal(deadline) {
		t.Fatalf("deadline instant should be allowed: got %v", got)
	}
	if got := wrapped.NextUntil(start, deadline.Add(-1)); !got.IsZero() {
		t.Fatalf("got %v want zero past deadline", got)
	}
}

// With every weekday disabled there is no legal date. The search stops at the
// cutoff instead of continuing past it: the underlying Schedule is not
// consulted again once its candidate passes the cutoff.
func TestCalendarSchedulePermanentlyBlockedStopsAtCutoff(t *testing.T) {
	schedule := &recordingSchedule{schedule: mustParseStandard(t, "* * * * *")}
	allWeekdays := []time.Weekday{
		time.Sunday, time.Monday, time.Tuesday, time.Wednesday,
		time.Thursday, time.Friday, time.Saturday,
	}
	calendar := mustCalendar(t, "UTC", nil, allWeekdays, nil)
	start := utcTime(2023, time.January, 1, 0, 0)
	deadline := start.Add(90 * time.Second)
	if got := calendar.Wrap(schedule).NextUntil(start, deadline); !got.IsZero() {
		t.Fatalf("got %v want zero before cutoff", got)
	}
	if schedule.calls > 3 {
		t.Fatalf("search continued past cutoff: underlying Next called %d times", schedule.calls)
	}
}

// When the underlying Schedule has no later activation, the result is zero.
func TestCalendarScheduleNoUnderlyingNext(t *testing.T) {
	wrapped := mustCalendar(t, "UTC", nil, nil, nil).Wrap(&zeroSchedule{})
	if got := wrapped.Next(utcTime(2023, time.January, 1, 0, 0)); !got.IsZero() {
		t.Fatalf("got %v want zero", got)
	}
}

// A Schedule failing to advance must not cause an infinite loop.
func TestCalendarScheduleUnderlyingDoesNotAdvance(t *testing.T) {
	stuck := utcTime(2023, time.January, 2, 0, 0)
	wrapped := mustCalendar(t, "UTC", []DisabledDate{{2023, time.January, 2}}, nil, nil).Wrap(&fixedSchedule{at: stuck})
	if got := wrapped.Next(stuck.Add(-time.Minute)); !got.IsZero() {
		t.Fatalf("got %v want zero for non-advancing schedule", got)
	}
}

// CRON_TZ of the wrapped schedule is preserved; disabled dates are judged in
// the calendar's own location, which may differ from the schedule's.
func TestExceptionCalendarTimeZoneIndependence(t *testing.T) {
	schedule := mustParse(t, "CRON_TZ=Asia/Shanghai * * * * *")
	start := utcTime(2023, time.January, 1, 15, 59)

	// A UTC calendar disables the UTC date Jan 1: 16:00Z is already Jan 2 in
	// Shanghai yet still Jan 1 in UTC, so it must be blocked.
	utcCalendar := mustCalendar(t, "UTC", []DisabledDate{{2023, time.January, 1}}, nil, nil)
	if got := utcCalendar.Wrap(schedule).Next(start); !got.Equal(utcTime(2023, time.January, 2, 0, 0)) {
		t.Fatalf("UTC calendar: got %v want 2023-01-02T00:00Z", got)
	}

	// A Shanghai calendar instead ends the disabled day at 16:00Z.
	shanghaiCalendar := mustCalendar(t, "Asia/Shanghai", []DisabledDate{{2023, time.January, 1}}, nil, nil)
	if got := shanghaiCalendar.Wrap(schedule).Next(start); !got.Equal(utcTime(2023, time.January, 1, 16, 0)) {
		t.Fatalf("Shanghai calendar: got %v want 2023-01-01T16:00Z", got)
	}
}

func TestNewCalendarRejectsBadInput(t *testing.T) {
	if _, err := NewCalendar("Mars/Olympus", nil, nil, nil); err == nil {
		t.Fatal("expected error for unknown time zone")
	}
	if _, err := NewCalendarWithLocation(nil, nil, nil, nil); err == nil {
		t.Fatal("expected error for nil location")
	}
	bad := []TimeInterval{{
		utcTime(2023, time.January, 1, 10, 3),
		utcTime(2023, time.January, 1, 10, 1),
	}}
	if _, err := NewCalendar("UTC", nil, nil, bad); err == nil {
		t.Fatal("expected error for reversed interval")
	}
	if _, err := NewCalendar("UTC", nil, []time.Weekday{7}, nil); err == nil {
		t.Fatal("expected error for weekday 7")
	}
}

// Mutating the input slices after construction must not affect the calendar.
func TestNewCalendarCopiesInputs(t *testing.T) {
	dates := []DisabledDate{{2023, time.January, 1}}
	intervals := []TimeInterval{{
		utcTime(2023, time.January, 1, 10, 0),
		utcTime(2023, time.January, 1, 11, 0),
	}}
	calendar := mustCalendar(t, "UTC", dates, nil, intervals)

	dates[0] = DisabledDate{2023, time.January, 2}
	intervals[0].Start = utcTime(2023, time.February, 1, 0, 0)
	intervals[0].End = utcTime(2023, time.February, 2, 0, 0)

	if _, blocked := calendar.dayReason(utcTime(2023, time.January, 1, 0, 0)); !blocked {
		t.Fatal("January 1 date rule lost after mutating input slice")
	}
	if _, blocked := calendar.containingInterval(utcTime(2023, time.January, 1, 10, 30)); !blocked {
		t.Fatal("January interval lost after mutating input slice")
	}
	if _, blocked := calendar.dayReason(utcTime(2023, time.January, 2, 0, 0)); blocked {
		t.Fatal("calendar unexpectedly gained January 2 rule")
	}
}

// A built calendar may be read concurrently with its Next searches.
func TestExceptionCalendarConcurrentReads(t *testing.T) {
	calendar := mustCalendar(t, "UTC", []DisabledDate{{2023, time.January, 1}}, nil, []TimeInterval{{
		utcTime(2023, time.January, 2, 10, 0),
		utcTime(2023, time.January, 2, 11, 0),
	}})
	schedule := mustParseStandard(t, "* * * * *")

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				wrapped := calendar.Wrap(schedule)
				wrapped.Next(utcTime(2023, time.January, 1, 12, 0))
				calendar.dayReason(utcTime(2023, time.January, 1, 12, 0))
				calendar.containingInterval(utcTime(2023, time.January, 2, 10, 30))
			}
		}()
	}
	wg.Wait()
}

// recordingSchedule counts calls to Next and delegates to an inner schedule.
type recordingSchedule struct {
	schedule Schedule
	mu       sync.Mutex
	calls    int
}

func (r *recordingSchedule) Next(t time.Time) time.Time {
	r.mu.Lock()
	r.calls++
	r.mu.Unlock()
	return r.schedule.Next(t)
}

// zeroSchedule never has a next activation.
type zeroSchedule struct{}

func (zeroSchedule) Next(time.Time) time.Time { return time.Time{} }

// fixedSchedule always returns the same activation.
type fixedSchedule struct{ at time.Time }

func (s fixedSchedule) Next(t time.Time) time.Time {
	if s.at.After(t) {
		return s.at
	}
	return time.Time{}
}

// ExampleExceptionCalendar exercises the acceptance scenario over a 90-second
// window, listing every skipped candidate with its reason and the final time.
func ExampleExceptionCalendar() {
	schedule, _ := ParseStandard("* * * * *")
	start := time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC)
	maintenance := []TimeInterval{{
		start.Add(1 * time.Minute),
		start.Add(3 * time.Minute),
	}}

	// Search with a 90-second cutoff: the merged interval jumps the search to
	// 10:03, which is already past the cutoff, so the result is zero.
	calendar, _ := NewCalendar("UTC", nil, nil, maintenance)
	wrapped := calendar.Wrap(schedule)
	_, skipped := wrapped.NextWithLog(start, start.Add(90*time.Second))
	fmt.Println("bounded 90s search from", start.Format("15:04:05"))
	for _, event := range skipped {
		fmt.Println("skip", event.Time.UTC().Format(time.RFC3339), "->", event.Reason)
	}
	if got := wrapped.NextUntil(start, start.Add(90*time.Second)); got.IsZero() {
		fmt.Println("result: none (past cutoff)")
	}

	// The same calendar without a cutoff lands exactly on the interval end.
	if got := wrapped.Next(start); got.Equal(start.Add(3 * time.Minute)) {
		fmt.Println("unbounded result: 10:03")
	}

	// When the whole date is also disabled, the search uses calendar
	// arithmetic and reaches the next legal date.
	fullDay, _ := NewCalendar("UTC", []DisabledDate{{Year: 2023, Month: time.January, Day: 1}}, nil, maintenance)
	result := fullDay.Wrap(schedule).Next(start)
	fmt.Println("date-disabled result:", result.UTC().Format("2006-01-02T15:04:05Z07:00"))

	// Output:
	// bounded 90s search from 10:00:00
	// skip 2023-01-01T10:01:00Z -> UTC disabled interval [2023-01-01T10:01:00Z, 2023-01-01T10:03:00Z)
	// result: none (past cutoff)
	// unbounded result: 10:03
	// date-disabled result: 2023-01-02T00:00:00Z
}

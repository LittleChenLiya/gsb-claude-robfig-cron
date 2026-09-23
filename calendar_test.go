package cron

import (
	"sync"
	"testing"
	"time"

	_ "time/tzdata"
)

func mustParse(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse("2006-01-02 15:04:05", value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func mustSchedule(t *testing.T, spec string) Schedule {
	t.Helper()
	parsed, err := NewParser(Minute | Hour | Dom | Month | Dow | Descriptor).Parse(spec)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func mustCalendar(t *testing.T, options ...CalendarOption) *Calendar {
	t.Helper()
	calendar, err := NewCalendar(options...)
	if err != nil {
		t.Fatal(err)
	}
	return calendar
}

// UTC every minute, disabling [10:01, 10:03) moves 10:00 to 10:03; disabling
// the date as well must move on to the next legal date.
func TestIntervalThenDate(t *testing.T) {
	schedule := mustSchedule(t, "* * * * *")
	ten := mustParse(t, "2025-01-15 10:00:00")

	interval := Interval{
		Start: mustParse(t, "2025-01-15 10:01:00"),
		End:   mustParse(t, "2025-01-15 10:03:00"),
	}
	wrapped, err := NewCalendarSchedule(schedule, mustCalendar(t, WithDisabledIntervals(interval)))
	if err != nil {
		t.Fatal(err)
	}
	if got := wrapped.Next(ten); !got.Equal(mustParse(t, "2025-01-15 10:03:00")) {
		t.Fatalf("interval end = %s, want 2025-01-15 10:03:00", got)
	}

	wrapped, err = NewCalendarSchedule(schedule, mustCalendar(t,
		WithDisabledIntervals(interval),
		WithDisabledDates(mustParse(t, "2025-01-15 00:00:00")),
	))
	if err != nil {
		t.Fatal(err)
	}
	want := mustParse(t, "2025-01-16 00:00:00")
	if got := wrapped.Next(ten); !got.Equal(want) {
		t.Fatalf("disabled date = %s, want %s", got, want)
	}
}

// A disabled date spanning New York's spring-forward is 23 UTC hours; date
// arithmetic must advance by calendar day, not 24 fixed hours, and the endpoint
// itself must be eligible.
func TestSpringForwardDate(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	schedule := mustSchedule(t, "* * * * *")
	calendar := mustCalendar(t,
		WithCalendarLocation(ny),
		WithDisabledDates(time.Date(2025, 3, 9, 12, 0, 0, 0, ny)),
	)
	wrapped, err := NewCalendarSchedule(schedule, calendar)
	if err != nil {
		t.Fatal(err)
	}

	// 2025-03-09 05:00 UTC is the first minute of the disabled local date.
	from := time.Date(2025, 3, 9, 4, 59, 0, 0, time.UTC)
	want := time.Date(2025, 3, 10, 4, 0, 0, 0, time.UTC) // 2025-03-10 00:00 EDT
	got := wrapped.Next(from)
	if !got.Equal(want) {
		t.Fatalf("spring-forward date = %s, want %s", got, want)
	}
}

// During the fall-back the local hour 01 repeats: 01:30 EDT and 01:30 EST are
// distinct UTC instants. With only the first hour blocked the second one must
// run; with the whole local date blocked search advances by 25 UTC hours.
func TestFallBackRepeatedHour(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	schedule := mustSchedule(t, "CRON_TZ=America/New_York 30 1 * * *")

	firstRepeat := Interval{
		Start: time.Date(2025, 11, 2, 5, 0, 0, 0, time.UTC), // 01:00 EDT
		End:   time.Date(2025, 11, 2, 6, 0, 0, 0, time.UTC), // 01:00 EST
	}
	wrapped, err := NewCalendarSchedule(schedule, mustCalendar(t, WithDisabledIntervals(firstRepeat)))
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2025, 11, 2, 4, 0, 0, 0, time.UTC)  // 00:00 EDT
	want := time.Date(2025, 11, 2, 6, 30, 0, 0, time.UTC) // 01:30 EST, second repeat
	if got := wrapped.Next(from); !got.Equal(want) {
		t.Fatalf("second repeated 01:30 = %s, want %s", got, want)
	}

	// Blocking the whole calendar date must skip both repeats and land on the
	// next local matching time; Nov 3 01:30 EST follows the 25-hour local date.
	calendar := mustCalendar(t,
		WithCalendarLocation(ny),
		WithDisabledDates(time.Date(2025, 11, 2, 12, 0, 0, 0, ny)),
	)
	wrapped, err = NewCalendarSchedule(schedule, calendar)
	if err != nil {
		t.Fatal(err)
	}
	want = time.Date(2025, 11, 3, 6, 30, 0, 0, time.UTC) // 2025-11-03 01:30 EST
	if got := wrapped.Next(from); !got.Equal(want) {
		t.Fatalf("fall-back date = %s, want %s", got, want)
	}
}

// An every-hour schedule on the spring-forward day: 02:30 does not exist and is
// skipped by the schedule itself.
func TestSpringForwardMissingHour(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	schedule := mustSchedule(t, "CRON_TZ=America/New_York 30 * * * *")
	wrapped, err := NewCalendarSchedule(schedule, mustCalendar(t, WithCalendarLocation(ny)))
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2025, 3, 9, 6, 30, 0, 0, time.UTC) // 01:30 EST
	want := time.Date(2025, 3, 9, 7, 30, 0, 0, time.UTC) // 03:30 EDT (02:30 never occurs)
	if got := wrapped.Next(from); !got.Equal(want) {
		t.Fatalf("missing hour = %s, want %s", got, want)
	}
}

// Leap day is judged in the calendar's location: disabling 2024-02-29 skips
// the entire date, and Feb 29 in a non-leap year simply never matches.
func TestLeapDay(t *testing.T) {
	schedule := mustSchedule(t, "0 0 * * *")
	calendar := mustCalendar(t, WithDisabledDates(mustParse(t, "2024-02-29 12:00:00")))
	wrapped, err := NewCalendarSchedule(schedule, calendar)
	if err != nil {
		t.Fatal(err)
	}
	from := mustParse(t, "2024-02-28 00:00:00")
	want := mustParse(t, "2024-03-01 00:00:00")
	if got := wrapped.Next(from); !got.Equal(want) {
		t.Fatalf("leap day = %s, want %s", got, want)
	}

	from = mustParse(t, "2025-02-28 00:00:00")
	want = mustParse(t, "2025-02-28 00:00:00").AddDate(0, 0, 1) // 2025-03-01
	if got := wrapped.Next(from); !got.Equal(want) {
		t.Fatalf("non-leap year = %s, want %s", got, want)
	}
}

// Overlapping and adjacent intervals are merged, and the merged half-open end
// remains a legal activation instant.
func TestMergeOverlappingIntervals(t *testing.T) {
	schedule := mustSchedule(t, "* * * * *")
	intervals := []Interval{
		{mustParse(t, "2025-01-15 10:05:00"), mustParse(t, "2025-01-15 10:10:00")},
		{mustParse(t, "2025-01-15 10:01:00"), mustParse(t, "2025-01-15 10:06:00")},
		{mustParse(t, "2025-01-15 10:10:00"), mustParse(t, "2025-01-15 10:12:00")},
	}
	calendar := mustCalendar(t, WithDisabledIntervals(intervals...))
	if len(calendar.intervals) != 1 {
		t.Fatalf("merged intervals = %d, want 1 (%v)", len(calendar.intervals), calendar.intervals)
	}
	merged := calendar.intervals[0]
	if !merged.Start.Equal(mustParse(t, "2025-01-15 10:01:00")) ||
		!merged.End.Equal(mustParse(t, "2025-01-15 10:12:00")) {
		t.Fatalf("merged span = %v", merged)
	}
	wrapped, err := NewCalendarSchedule(schedule, calendar)
	if err != nil {
		t.Fatal(err)
	}
	want := mustParse(t, "2025-01-15 10:12:00")
	if got := wrapped.Next(mustParse(t, "2025-01-15 10:00:00")); !got.Equal(want) {
		t.Fatalf("merged end = %s, want %s", got, want)
	}
}

// A candidate exactly at the deadline is eligible; the next minute is not.
func TestDeadlineBoundary(t *testing.T) {
	schedule := mustSchedule(t, "* * * * *")
	interval := Interval{
		Start: mustParse(t, "2025-01-15 10:01:00"),
		End:   mustParse(t, "2025-01-15 10:03:00"),
	}
	wrapped, err := NewCalendarSchedule(schedule, mustCalendar(t, WithDisabledIntervals(interval)))
	if err != nil {
		t.Fatal(err)
	}
	from := mustParse(t, "2025-01-15 10:00:00")

	deadline := mustParse(t, "2025-01-15 10:03:00")
	if got := wrapped.NextUntil(from, deadline); !got.Equal(deadline) {
		t.Fatalf("deadline-equal = %s, want %s", got, deadline)
	}

	before := mustParse(t, "2025-01-15 10:02:59")
	if got := wrapped.NextUntil(from, before); !got.IsZero() {
		t.Fatalf("deadline exceeded = %s, want zero", got)
	}
}

type countingSchedule struct {
	Schedule
	nextCalls int
}

func (s *countingSchedule) Next(t time.Time) time.Time {
	s.nextCalls++
	return s.Schedule.Next(t)
}

// With a permanently disabled calendar (all weekdays off) the search must stop
// at the deadline instead of looping forever, with one bounded probe per date.
func TestPermanentlyDisabledStopsAtDeadline(t *testing.T) {
	counter := &countingSchedule{Schedule: mustSchedule(t, "* * * * *")}
	calendar := mustCalendar(t,
		WithDisabledWeekdays(time.Sunday, time.Monday, time.Tuesday, time.Wednesday,
			time.Thursday, time.Friday, time.Saturday),
	)
	wrapped, err := NewCalendarSchedule(counter, calendar)
	if err != nil {
		t.Fatal(err)
	}
	from := mustParse(t, "2025-01-15 10:00:00") // Wednesday
	deadline := from.AddDate(0, 0, 90)
	if got := wrapped.NextUntil(from, deadline); !got.IsZero() {
		t.Fatalf("permanent calendar = %s, want zero", got)
	}
	if counter.nextCalls > 92 {
		t.Fatalf("inner Next called %d times, expected bounded date-at-a-time probes", counter.nextCalls)
	}
}

// The deadline lying inside a never-ending disabled interval stops the search
// at the deadline without probing beyond it.
func TestPermanentIntervalStopsAtDeadline(t *testing.T) {
	counter := &countingSchedule{Schedule: mustSchedule(t, "* * * * *")}
	from := mustParse(t, "2025-01-15 10:00:00")
	interval := Interval{
		Start: mustParse(t, "2025-01-15 10:01:00"),
		End:   mustParse(t, "2099-01-01 00:00:00"),
	}
	wrapped, err := NewCalendarSchedule(counter, mustCalendar(t, WithDisabledIntervals(interval)))
	if err != nil {
		t.Fatal(err)
	}
	deadline := from.Add(90 * time.Second)
	if got := wrapped.NextUntil(from, deadline); !got.IsZero() {
		t.Fatalf("permanent interval = %s, want zero", got)
	}
	if counter.nextCalls != 1 {
		t.Fatalf("inner Next called %d times, want exactly 1", counter.nextCalls)
	}
}

// With no explicit deadline the rolling five-year window stops a permanently
// disabled search as well.
func TestPermanentlyDisabledDefaultWindow(t *testing.T) {
	counter := &countingSchedule{Schedule: mustSchedule(t, "* * * * *")}
	calendar := mustCalendar(t,
		WithDisabledWeekdays(time.Sunday, time.Monday, time.Tuesday, time.Wednesday,
			time.Thursday, time.Friday, time.Saturday),
	)
	wrapped, err := NewCalendarSchedule(counter, calendar)
	if err != nil {
		t.Fatal(err)
	}
	if got := wrapped.Next(mustParse(t, "2025-01-15 10:00:00")); !got.IsZero() {
		t.Fatalf("permanent calendar default window = %s, want zero", got)
	}
}

// Next must be strictly greater than t: a legal activation at t itself is not
// returned.
func TestNextStrictlyAfter(t *testing.T) {
	schedule := mustSchedule(t, "* * * * *")
	wrapped, err := NewCalendarSchedule(schedule, mustCalendar(t))
	if err != nil {
		t.Fatal(err)
	}
	at := mustParse(t, "2025-01-15 10:00:00")
	if got := wrapped.Next(at); !got.Equal(at.Add(time.Minute)) {
		t.Fatalf("Next(at) = %s, want %s", got, at.Add(time.Minute))
	}
}

// When the underlying schedule has no next activation, the wrapper returns
// zero.
func TestInnerScheduleExhausted(t *testing.T) {
	wrapped, err := NewCalendarSchedule(
		exhaustedSchedule{},
		mustCalendar(t, WithDisabledDates(mustParse(t, "2025-01-15 00:00:00"))),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := wrapped.Next(mustParse(t, "2025-01-15 10:00:00")); !got.IsZero() {
		t.Fatalf("exhausted inner = %s, want zero", got)
	}
}

type exhaustedSchedule struct{}

func (exhaustedSchedule) Next(time.Time) time.Time { return time.Time{} }

// Mutating slices or times passed to the options after construction must not
// affect the built calendar.
func TestInputsCopied(t *testing.T) {
	dates := []time.Time{mustParse(t, "2025-01-15 00:00:00")}
	weekdays := []time.Weekday{time.Monday}
	intervals := []Interval{{
		Start: mustParse(t, "2025-01-16 10:00:00"),
		End:   mustParse(t, "2025-01-16 11:00:00"),
	}}
	calendar, err := NewCalendar(
		WithDisabledDates(dates...),
		WithDisabledWeekdays(weekdays...),
		WithDisabledIntervals(intervals...),
	)
	if err != nil {
		t.Fatal(err)
	}

	dates[0] = mustParse(t, "2025-01-17 00:00:00")
	weekdays[0] = time.Tuesday
	intervals[0].Start = mustParse(t, "2025-02-01 10:00:00")
	intervals[0].End = mustParse(t, "2025-02-01 11:00:00")

	for _, at := range []time.Time{
		mustParse(t, "2025-01-15 10:00:00"), // originally disabled date
		mustParse(t, "2025-01-20 10:00:00"), // Monday
	} {
		if calendar.IsAllowed(at) {
			t.Errorf("rule lost after input mutation at %s", at)
		}
	}
	if blocked, ok := calendar.intervalContaining(mustParse(t, "2025-01-16 10:30:00")); !ok {
		t.Errorf("interval lost after input mutation: %v", blocked)
	}
}

// Rule evaluation must be safe for concurrent reads after construction.
func TestConcurrentReads(t *testing.T) {
	calendar := mustCalendar(t,
		WithCalendarLocation(time.UTC),
		WithDisabledDates(mustParse(t, "2025-01-15 00:00:00")),
		WithDisabledWeekdays(time.Saturday, time.Sunday),
		WithDisabledIntervals(Interval{
			Start: mustParse(t, "2025-02-01 10:00:00"),
			End:   mustParse(t, "2025-02-01 11:00:00"),
		}),
	)
	wrapped, err := NewCalendarSchedule(mustSchedule(t, "* * * * *"), calendar)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			from := mustParse(t, "2025-01-15 00:00:00").Add(time.Duration(i) * time.Minute)
			deadline := from.AddDate(0, 0, 60)
			_, _ = wrapped.NextWithReasons(from, deadline, nil)
			_ = calendar.IsAllowed(from)
		}(i)
	}
	wg.Wait()
}

func TestConstructionErrors(t *testing.T) {
	if _, err := NewCalendar(WithCalendarLocation(nil)); err == nil {
		t.Error("nil location accepted")
	}
	if _, err := NewCalendar(WithTimezone("Not/A/Zone")); err == nil {
		t.Error("invalid timezone accepted")
	}
	if _, err := NewCalendar(WithDisabledWeekdays(time.Weekday(7))); err == nil {
		t.Error("invalid weekday accepted")
	}
	start := mustParse(t, "2025-01-15 10:00:00")
	end := mustParse(t, "2025-01-15 09:00:00")
	if _, err := NewCalendar(WithDisabledIntervals(Interval{start, end})); err == nil {
		t.Error("reversed interval accepted")
	}
	if _, err := NewCalendar(WithDisabledIntervals(Interval{start, start})); err == nil {
		t.Error("zero-length interval accepted")
	}
	validCalendar, err := NewCalendar()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewCalendarSchedule(nil, validCalendar); err == nil {
		t.Error("nil inner schedule accepted")
	}
	if _, err := NewCalendarSchedule(mustSchedule(t, "* * * * *"), nil); err == nil {
		t.Error("nil calendar accepted")
	}
}

// The wrapped CRON_TZ schedule keeps evaluating in its own timezone while
// disabled dates are evaluated in the calendar's timezone.
func TestCRONTZDifferentFromCalendarLocation(t *testing.T) {
	tokyo, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		t.Fatal(err)
	}
	schedule := mustSchedule(t, "CRON_TZ=Asia/Tokyo 0 9 * * *") // 09:00 JST daily
	calendar := mustCalendar(t,
		WithCalendarLocation(tokyo),
		WithDisabledDates(time.Date(2025, 1, 16, 0, 0, 0, 0, tokyo)),
	)
	wrapped, err := NewCalendarSchedule(schedule, calendar)
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2025, 1, 16, 0, 0, 0, 0, time.UTC) // 09:00 JST on the disabled date
	want := time.Date(2025, 1, 17, 0, 0, 0, 0, time.UTC) // 09:00 JST the next day
	if got := wrapped.Next(from); !got.Equal(want) {
		t.Fatalf("CRON_TZ vs calendar loc = %s, want %s", got, want)
	}
}

// WithDeadline fixes the cutoff for plain Next calls.
func TestConfiguredDeadline(t *testing.T) {
	schedule := mustSchedule(t, "* * * * *")
	wrapped, err := NewCalendarSchedule(
		schedule,
		mustCalendar(t),
		WithDeadline(mustParse(t, "2025-01-15 10:02:00")),
	)
	if err != nil {
		t.Fatal(err)
	}
	from := mustParse(t, "2025-01-15 10:00:00")
	if got := wrapped.Next(from); !got.Equal(mustParse(t, "2025-01-15 10:01:00")) {
		t.Fatalf("configured deadline = %s, want 10:01", got)
	}
	wrapped2, err := NewCalendarSchedule(
		schedule,
		mustCalendar(t, WithDisabledIntervals(Interval{
			Start: mustParse(t, "2025-01-15 10:01:00"),
			End:   mustParse(t, "2099-01-01 00:00:00"),
		})),
		WithDeadline(mustParse(t, "2025-01-15 10:02:00")),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := wrapped2.Next(from); !got.IsZero() {
		t.Fatalf("configured deadline within interval = %s, want zero", got)
	}
}

// Reasons are reported independently when multiple rules match at once.
func TestSkipReasonsCombined(t *testing.T) {
	schedule := mustSchedule(t, "* * * * *")
	wednesday := mustParse(t, "2025-01-15 00:00:00") // Wednesday
	calendar := mustCalendar(t,
		WithDisabledDates(wednesday),
		WithDisabledWeekdays(time.Wednesday),
		WithDisabledIntervals(Interval{
			Start: mustParse(t, "2025-01-15 10:01:00"),
			End:   mustParse(t, "2025-01-15 10:03:00"),
		}),
	)
	wrapped, err := NewCalendarSchedule(schedule, calendar)
	if err != nil {
		t.Fatal(err)
	}
	_, events := wrapped.NextWithReasons(
		mustParse(t, "2025-01-15 10:00:00"),
		mustParse(t, "2025-01-15 10:02:00"),
		nil,
	)
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1: %+v", len(events), events)
	}
	want := SkipDate | SkipWeekday | SkipInterval
	if events[0].Reasons != want {
		t.Fatalf("reasons = %b (%s), want %b (%s)", events[0].Reasons, events[0].Reasons, want, want)
	}
}

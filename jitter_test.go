package cron

import (
	"errors"
	"testing"
	"time"
	_ "time/tzdata"
)

const (
	jitterTestKey    = "reports-rollup"
	jitterTestMax    = 20 * time.Second
	jitterTestLayout = "2006-01-02T15:04:05.000000000Z07:00"
)

func mustParseUTC(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(jitterTestLayout, value)
	if err != nil {
		t.Fatalf("parse %q: %v", value, err)
	}
	return parsed
}

func minuteScheduleInUTC(t *testing.T) Schedule {
	t.Helper()
	schedule, err := secondParser.Parse("0 * * * * *")
	if err != nil {
		t.Fatalf("parse schedule: %v", err)
	}
	return schedule
}

// fixedSchedule returns a constant list of activation times, ending forever
// once the list is exhausted.
type fixedSchedule struct {
	times []time.Time
}

func (s *fixedSchedule) Next(t time.Time) time.Time {
	for _, fire := range s.times {
		if fire.After(t) {
			return fire
		}
	}
	return time.Time{}
}

// Golden values produced by the fixed HMAC-SHA-256 algorithm for key
// "reports-rollup" and a 20 second window. They must never change.
func TestJitterDeterministic(t *testing.T) {
	schedule := minuteScheduleInUTC(t)
	start := mustParseUTC(t, "2023-03-12T06:58:00.000000000Z")
	jitter, err := NewJitterSchedule(schedule, jitterTestKey, jitterTestMax, start)
	if err != nil {
		t.Fatalf("new jitter schedule: %v", err)
	}

	want := []struct {
		original, delayed string
	}{
		{"2023-03-12T06:59:00.000000000Z", "2023-03-12T06:59:14.797561135Z"},
		{"2023-03-12T07:00:00.000000000Z", "2023-03-12T07:00:06.336027863Z"},
		{"2023-03-12T07:01:00.000000000Z", "2023-03-12T07:01:03.216500872Z"},
		{"2023-03-12T07:02:00.000000000Z", "2023-03-12T07:02:15.556782822Z"},
		{"2023-03-12T07:03:00.000000000Z", "2023-03-12T07:03:02.958747355Z"},
	}

	// Walking the schedule from just before the first golden firing yields each
	// delayed firing strictly after the query time and in increasing order.
	query := mustParseUTC(t, want[0].original).Add(-time.Nanosecond)
	for i, tc := range want {
		original := mustParseUTC(t, tc.original)
		expected := mustParseUTC(t, tc.delayed)
		if got := schedule.Next(original.Add(-time.Nanosecond)); !got.Equal(original) {
			t.Fatalf("fire %d: underlying schedule changed, got %v want %v", i, got, original)
		}
		got := jitter.Next(query)
		if !got.Equal(expected) {
			t.Errorf("fire %d: got %v want %v", i, got, expected)
		}
		if !got.After(query) {
			t.Errorf("fire %d: %v is not strictly after %v", i, got, query)
		}
		query = got
	}

	// The same query asked repeatedly always returns the identical time.
	repeatedAt := mustParseUTC(t, "2023-03-12T07:00:00.000000000Z")
	first := jitter.Next(repeatedAt)
	for i := 0; i < 25; i++ {
		if got := jitter.Next(repeatedAt); !got.Equal(first) {
			t.Fatalf("repeat %d: got %v want %v", i, got, first)
		}
	}
	expected := mustParseUTC(t, "2023-03-12T07:00:06.336027863Z")
	if !first.Equal(expected) {
		t.Fatalf("query between original and delayed firing: got %v want %v", first, expected)
	}

	// A freshly constructed wrapper with the same inputs agrees exactly.
	rebuilt, err := NewJitterSchedule(minuteScheduleInUTC(t), jitterTestKey, jitterTestMax, start)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	for _, tc := range want {
		original := mustParseUTC(t, tc.original)
		wantDelayed := mustParseUTC(t, tc.delayed)
		if got := rebuilt.Next(original.Add(-time.Nanosecond)); !got.Equal(wantDelayed) {
			t.Errorf("rebuilt wrapper at %v: got %v want %v", original, got, wantDelayed)
		}
	}
}

// Querying exactly between an original firing and its delayed firing must still
// surface that delayed firing instead of skipping it.
func TestJitterPreservesPendingFiring(t *testing.T) {
	schedule := minuteScheduleInUTC(t)
	start := mustParseUTC(t, "2023-03-12T06:00:00.000000000Z")
	jitter, err := NewJitterSchedule(schedule, jitterTestKey, jitterTestMax, start)
	if err != nil {
		t.Fatalf("new jitter schedule: %v", err)
	}

	original := mustParseUTC(t, "2023-03-12T07:00:00.000000000Z")
	delayed := mustParseUTC(t, "2023-03-12T07:00:06.336027863Z")
	for _, query := range []time.Time{
		original.Add(-time.Nanosecond),
		original,
		delayed.Add(-time.Nanosecond),
	} {
		if got := jitter.Next(query); !got.Equal(delayed) {
			t.Errorf("query %v: got %v want pending firing %v", query, got, delayed)
		}
	}

	// Querying at or beyond the delayed firing advances to the next one.
	nextDelayed := mustParseUTC(t, "2023-03-12T07:01:03.216500872Z")
	if got := jitter.Next(delayed); !got.Equal(nextDelayed) {
		t.Errorf("query at delayed firing: got %v want %v", got, nextDelayed)
	}
}

// Every delayed firing stays inside the window between adjacent original firings
// and never exceeds the configured maximum delay.
func TestJitterWindows(t *testing.T) {
	schedule := minuteScheduleInUTC(t)
	start := mustParseUTC(t, "2023-03-12T00:00:00.000000000Z")
	jitter, err := NewJitterSchedule(schedule, jitterTestKey, jitterTestMax, start)
	if err != nil {
		t.Fatalf("new jitter schedule: %v", err)
	}

	query := start
	var previous time.Time
	for i := 0; i < 1000; i++ {
		delayed := jitter.Next(query)
		original := schedule.Next(delayed.Add(-jitterTestMax - time.Nanosecond))
		offset := delayed.Sub(original)
		if offset < 0 || offset > jitterTestMax {
			t.Fatalf("fire %d: offset %v out of [0,%v] (orig %v delayed %v)", i, offset, jitterTestMax, original, delayed)
		}
		nextOriginal := schedule.Next(original)
		if !delayed.Before(nextOriginal) {
			t.Fatalf("fire %d: delayed %v crossed next original firing %v", i, delayed, nextOriginal)
		}
		if !delayed.After(query) {
			t.Fatalf("fire %d: delayed %v not after query %v", i, delayed, query)
		}
		if !previous.IsZero() && !delayed.After(previous) {
			t.Fatalf("fire %d: sequence not increasing: %v after %v", i, delayed, previous)
		}
		previous = delayed
		query = delayed
	}
}

// When the computed offset would cross the next original firing, the delayed
// firing is clamped to one nanosecond before it.
func TestJitterClampsToNextFiring(t *testing.T) {
	base := mustParseUTC(t, "2023-03-12T08:00:00.000000000Z")
	schedule := &fixedSchedule{times: []time.Time{
		base,
		base.Add(10 * time.Nanosecond),
		base.Add(20 * time.Nanosecond),
	}}
	maxDelay := time.Second
	start := base.Add(-time.Hour)
	jitter, err := NewJitterSchedule(schedule, "clamp", maxDelay, start)
	if err != nil {
		t.Fatalf("new jitter schedule: %v", err)
	}

	first := jitter.Next(start)
	if want := base.Add(9 * time.Nanosecond); !first.Equal(want) {
		t.Errorf("first: got %v want clamped %v", first, want)
	}
	second := jitter.Next(first)
	if want := base.Add(19 * time.Nanosecond); !second.Equal(want) {
		t.Errorf("second: got %v want clamped %v", second, want)
	}
	// The final firing has no successor and keeps its computed offset.
	third := jitter.Next(second)
	offset := third.Sub(base.Add(20 * time.Nanosecond))
	if offset < 0 || offset > maxDelay {
		t.Errorf("third: offset %v out of bounds", offset)
	}
	if !third.After(second) {
		t.Errorf("third: %v not after %v", third, second)
	}
	if next := jitter.Next(third); !next.IsZero() {
		t.Errorf("exhausted schedule: got %v want zero time", next)
	}
}

// A single firing that is overdue at the original instant but whose delayed
// instant has not arrived is not dropped; afterwards the schedule ends.
func TestJitterSingleFiringAndExhaustion(t *testing.T) {
	fire := mustParseUTC(t, "2023-03-12T09:00:00.000000000Z")
	schedule := &fixedSchedule{times: []time.Time{fire}}
	start := fire.Add(-time.Hour)
	jitter, err := NewJitterSchedule(schedule, "once", 20*time.Second, start)
	if err != nil {
		t.Fatalf("new jitter schedule: %v", err)
	}
	delayed := fire.Add(jitter.offset(fire))

	for _, query := range []time.Time{start, fire, fire.Add(-time.Nanosecond), delayed.Add(-time.Nanosecond)} {
		got, err := jitter.NextAfter(query)
		if err != nil {
			t.Fatalf("query %v: %v", query, err)
		}
		if !got.Equal(delayed) {
			t.Errorf("query %v: got %v want %v", query, got, delayed)
		}
	}

	got, err := jitter.NextAfter(delayed)
	if err != nil {
		t.Fatalf("query at delayed firing: %v", err)
	}
	if !got.IsZero() {
		t.Errorf("after final firing: got %v want zero time", got)
	}

	// An immediately exhausted underlying schedule reports the zero time.
	empty, err := NewJitterSchedule(&fixedSchedule{}, "empty", time.Second, start)
	if err != nil {
		t.Fatalf("empty jitter schedule: %v", err)
	}
	if next, err := empty.NextAfter(start); err != nil || !next.IsZero() {
		t.Errorf("empty: got %v, %v want zero time, nil", next, err)
	}
}

// collectFirings returns all activations of a schedule in [from, until],
// starting the underlying Next walk at initialQuery (which may be before from).
func collectFirings(t *testing.T, schedule Schedule, initialQuery, from, until time.Time) []time.Time {
	t.Helper()
	var firings []time.Time
	query := initialQuery
	for {
		next := schedule.Next(query)
		if next.IsZero() || next.After(until) {
			return firings
		}
		if next.Before(from) {
			query = next
			continue
		}
		firings = append(firings, next)
		query = next
	}
}

// Delayed firings stay ordered and inside their windows across daylight-saving
// transitions, including the skipped spring-forward hour and the repeated
// fall-back hour.
func TestJitterDaylightSaving(t *testing.T) {
	tests := []struct {
		name        string
		from, until string
		want        int
	}{
		{
			name:  "spring forward skips 02:00",
			from:  "2023-03-12T05:00:00.000000000Z",
			until: "2023-03-12T09:30:00.000000000Z",
			want:  5, // 00, 01, 03, 04, 05 wall time; 02 does not exist
		},
		{
			name:  "fall back repeats 01:00",
			from:  "2023-11-05T04:00:00.000000000Z",
			until: "2023-11-05T09:30:00.000000000Z",
			want:  6, // 00, 01 EDT, 01 EST, 02, 03, 04
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			schedule, err := secondParser.Parse("CRON_TZ=America/New_York 0 0 * * * *")
			if err != nil {
				t.Fatalf("parse schedule: %v", err)
			}
			from := mustParseUTC(t, tc.from)
			until := mustParseUTC(t, tc.until)
			jitter, err := NewJitterSchedule(schedule, "dst", 20*time.Minute, from)
			if err != nil {
				t.Fatalf("new jitter schedule: %v", err)
			}

			original := collectFirings(t, schedule, from.Add(-time.Nanosecond), from, until)
			delayed := collectFirings(t, jitter, from, from, until)
			if len(original) != tc.want {
				t.Fatalf("original firings: got %d want %d", len(original), tc.want)
			}
			if len(delayed) != len(original) {
				t.Fatalf("delayed firings: got %d want %d", len(delayed), len(original))
			}

			for i, firing := range delayed {
				offset := firing.Sub(original[i])
				if offset < 0 || offset > 20*time.Minute {
					t.Errorf("fire %d: offset %v out of [0,20m]", i, offset)
				}
				if i+1 < len(original) && !firing.Before(original[i+1]) {
					t.Errorf("fire %d: delayed %v crossed next original %v", i, firing, original[i+1])
				}
				if i > 0 && !firing.After(delayed[i-1]) {
					t.Errorf("fire %d: delayed sequence not increasing (%v after %v)", i, firing, delayed[i-1])
				}
			}
		})
	}
}

// Queries before the explicit Start time are rejected.
func TestJitterStartGuard(t *testing.T) {
	schedule := minuteScheduleInUTC(t)
	start := mustParseUTC(t, "2023-03-12T10:00:00.000000000Z")
	jitter, err := NewJitterSchedule(schedule, jitterTestKey, jitterTestMax, start)
	if err != nil {
		t.Fatalf("new jitter schedule: %v", err)
	}

	for _, query := range []time.Time{
		start.Add(-time.Nanosecond),
		start.Add(-time.Hour),
		time.Time{},
	} {
		if got, err := jitter.NextAfter(query); !errors.Is(err, ErrBeforeStart) {
			t.Errorf("query %v: got %v, %v want ErrBeforeStart", query, got, err)
		}
		func() {
			defer func() {
				recovered := recover()
				if recovered != ErrBeforeStart {
					t.Errorf("query %v: recovered %v want ErrBeforeStart panic", query, recovered)
				}
			}()
			jitter.Next(query)
		}()
	}

	// Querying exactly at Start is allowed.
	want := mustParseUTC(t, "2023-03-12T10:00:00.000000000Z").Add(jitter.offset(mustParseUTC(t, "2023-03-12T10:00:00.000000000Z")))
	if got, err := jitter.NextAfter(start); err != nil || !got.Equal(want) {
		t.Errorf("query at start: got %v, %v want %v, nil", got, err, want)
	}
}

func TestNewJitterScheduleValidation(t *testing.T) {
	schedule := minuteScheduleInUTC(t)
	start := mustParseUTC(t, "2023-03-12T10:00:00.000000000Z")
	if _, err := NewJitterSchedule(nil, "k", time.Second, start); err == nil {
		t.Error("nil schedule: expected error")
	}
	if _, err := NewJitterSchedule(schedule, "k", -time.Nanosecond, start); err == nil {
		t.Error("negative maxDelay: expected error")
	}
	maxDuration := time.Duration(1<<63 - 1)
	if _, err := NewJitterSchedule(schedule, "k", maxDuration, start); err == nil {
		t.Error("maximum maxDelay: expected error")
	}
	if _, err := NewJitterSchedule(schedule, "k", time.Second, time.Time{}); err == nil {
		t.Error("zero start: expected error")
	}
	if _, err := NewJitterSchedule(schedule, "k", 0, start); err != nil {
		t.Errorf("zero maxDelay: unexpected error %v", err)
	}

	// The zero value is not usable.
	var zero JitterSchedule
	if _, err := zero.NextAfter(start); err == nil {
		t.Error("zero JitterSchedule: expected error")
	}
}

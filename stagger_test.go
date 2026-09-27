package cron

import (
	"errors"
	"testing"
	"time"
)

func utc(y int, mo time.Month, d, h, mi, s, ns int) time.Time {
	return time.Date(y, mo, d, h, mi, s, ns, time.UTC)
}

// fixedSchedule is a deterministic finite Schedule used by the tests.
type fixedSchedule struct {
	instants []time.Time
}

func (f *fixedSchedule) Next(t time.Time) time.Time {
	for _, instant := range f.instants {
		if instant.After(t) {
			return instant
		}
	}
	return time.Time{}
}

func mustParseCron(t *testing.T, spec string) Schedule {
	t.Helper()
	schedule, err := ParseStandard(spec)
	if err != nil {
		t.Fatalf("ParseStandard(%q): %v", spec, err)
	}
	return schedule
}

func mustStagger(t *testing.T, schedule Schedule, key string, maxDelay time.Duration, start time.Time) *StaggerSchedule {
	t.Helper()
	staggered, err := NewStaggerSchedule(schedule, key, maxDelay, start)
	if err != nil {
		t.Fatalf("NewStaggerSchedule returned error: %v", err)
	}
	return staggered
}

func TestStaggerDeterminism(t *testing.T) {
	const key = "tenant-42"
	maxDelay := 20 * time.Second
	start := utc(2024, 1, 1, 0, 0, 0, 0)

	wrapped := func() *StaggerSchedule {
		return mustStagger(t, mustParseCron(t, "* * * * *"), key, maxDelay, start)
	}

	queries := []time.Time{
		start,
		utc(2024, 1, 1, 0, 0, 30, 0),
		utc(2024, 1, 1, 0, 0, 45, 123456789),
		utc(2024, 6, 15, 12, 30, 15, 0),
	}

	first := make([]time.Time, len(queries))
	for i, q := range queries {
		first[i] = wrapped().Next(q)
	}

	// Repeated queries, on both the same instance and freshly constructed
	// wrappers, must return byte-identical instants.
	for iteration := 0; iteration < 5; iteration++ {
		schedule := wrapped()
		for i, q := range queries {
			got := schedule.Next(q)
			if !got.Equal(first[i]) {
				t.Fatalf("iteration %d query %s: got %s, want %s", iteration, q, got, first[i])
			}
		}
	}

	for _, next := range first {
		t.Logf("next: %s", next.Format(time.RFC3339Nano))
	}

	// Golden values pin the documented FNV-1a encoding; changing the fixed
	// algorithm should be a deliberate, documented break.
	want := []string{
		"2024-01-01T00:00:13.915756871Z",
		"2024-01-01T00:01:15.612256081Z",
		"2024-01-01T00:01:15.612256081Z",
		"2024-06-15T12:31:00.005711532Z",
	}
	for i := range queries {
		if got := first[i].Format(time.RFC3339Nano); got != want[i] {
			t.Errorf("query %s: got %s, want %s", queries[i].Format(time.RFC3339Nano), got, want[i])
		}
	}
}

func TestStaggerOffsetRangeAndEncoding(t *testing.T) {
	start := utc(2024, 1, 1, 0, 0, 0, 0)
	schedule := mustStagger(t, mustParseCron(t, "* * * * *"), "k", 20*time.Second, start)

	// Every offset for a day of per-minute activations stays in [0, maxDelay).
	orig := start
	for i := 0; i < 24*60; i++ {
		offset := schedule.staggerOffset(orig)
		if offset < 0 || offset >= schedule.MaxDelay {
			t.Fatalf("offset %s for %s out of [0,%s)", offset, orig, schedule.MaxDelay)
		}
		orig = orig.Add(time.Minute)
	}

	// The same UTC instant expressed in different locations produces the same
	// offset; the hash input is the UTC instant, not wall-clock text.
	loc := time.FixedZone("UTC+5", 5*3600)
	inUTC := utc(2024, 3, 31, 1, 30, 0, 0)
	if got, want := schedule.staggerOffset(inUTC), schedule.staggerOffset(inUTC.In(loc)); got != want {
		t.Fatalf("offset changed by location: %s vs %s", got, want)
	}
}

func TestStaggerClampedBeforeNextOriginal(t *testing.T) {
	start := utc(2024, 1, 1, 0, 0, 0, 0)
	first := start
	second := start.Add(5 * time.Nanosecond)
	third := start.Add(10*time.Second + 5)
	underlying := &fixedSchedule{instants: []time.Time{first, second, third}}
	schedule := mustStagger(t, underlying, "crowded", 20*time.Second, start)

	// No matter what the hash says, a 5ns gap between original instants means
	// the delayed first activation is clamped to second-1ns.
	got := schedule.delayed(first, second)
	want := second.Add(-time.Nanosecond)
	if !got.Equal(want) {
		t.Fatalf("delayed first: got %s, want %s", got, want)
	}

	got = schedule.delayed(second, third)
	if !got.Before(third) {
		t.Fatalf("delayed activation %s crossed next original %s", got, third)
	}
	if got.Before(second) {
		t.Fatalf("delayed activation %s precedes its original %s", got, second)
	}

	// Walking Next over the crowded schedule stays strictly increasing.
	cur := start
	for i := 0; i < 3; i++ {
		next := schedule.Next(cur)
		if next.IsZero() {
			break
		}
		if !next.After(cur) {
			t.Fatalf("activation %d not strictly after %s: %s", i, cur, next)
		}
		cur = next
	}
}

func TestStaggerPendingActivation(t *testing.T) {
	start := utc(2024, 1, 1, 0, 0, 0, 0)
	orig := start.Add(time.Minute)
	underlying := &fixedSchedule{instants: []time.Time{
		orig,
		orig.Add(time.Minute),
		orig.Add(2 * time.Minute),
	}}
	schedule := mustStagger(t, underlying, "keep-pending", 20*time.Second, start)
	offset := schedule.staggerOffset(orig)
	if offset <= 0 {
		t.Skip("chosen key does not produce a positive offset for this instant")
	}
	delayed := orig.Add(offset)
	t.Logf("original %s, delayed %s", orig, delayed)

	// Querying exactly at the original instant returns the not-yet-due
	// delayed instant rather than skipping to the following activation.
	if got := schedule.Next(orig); !got.Equal(delayed) {
		t.Fatalf("at original instant: got %s, want %s", got, delayed)
	}

	// Querying strictly between original and delayed keeps the same instant.
	midway := orig.Add(offset / 2)
	if got := schedule.Next(midway); !got.Equal(delayed) {
		t.Fatalf("midway: got %s, want %s", got, delayed)
	}

	// One nanosecond before the delayed instant still returns it.
	if got := schedule.Next(delayed.Add(-time.Nanosecond)); !got.Equal(delayed) {
		t.Fatalf("1ns before: got %s, want %s", got, delayed)
	}

	// At the delayed instant itself that activation is due, so the next query
	// moves on to the following delayed activation.
	secondDelayed := schedule.delayed(orig.Add(time.Minute), orig.Add(2*time.Minute))
	if got := schedule.Next(delayed); !got.Equal(secondDelayed) {
		t.Fatalf("at delayed instant: got %s, want %s", got, secondDelayed)
	}
}

func TestStaggerChainIncreasingAndBounded(t *testing.T) {
	start := utc(2024, 1, 1, 0, 0, 0, 0)
	schedule := mustStagger(t, mustParseCron(t, "* * * * *"), "chain-walk", 20*time.Second, start)

	// Independently derive the expected delayed sequence from the wrapped
	// schedule and compare it with repeated Next() calls.
	cur := start
	orig := schedule.Schedule.Next(start.Add(-time.Nanosecond))
	for i := 0; i < 1000; i++ {
		nextOrig := schedule.Schedule.Next(orig)
		want := schedule.delayed(orig, nextOrig)

		got := schedule.Next(cur)
		if !got.Equal(want) {
			t.Fatalf("step %d query %s: got %s, want %s", i, cur, got, want)
		}
		if !got.After(cur) {
			t.Fatalf("step %d: %s is not strictly after %s", i, got, cur)
		}
		if got.Before(orig) {
			t.Fatalf("step %d: delayed %s precedes original %s", i, got, orig)
		}
		if !nextOrig.IsZero() && !got.Before(nextOrig) {
			t.Fatalf("step %d: delayed %s reached next original %s", i, got, nextOrig)
		}

		cur = got
		orig = nextOrig
	}
}

func TestStaggerDSTTransitions(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("time zone database unavailable: %v", err)
	}

	cases := []struct {
		name  string
		start time.Time
	}{
		// Spring forward: 2024-03-10 02:00 jumps to 03:00 local time.
		{"spring-forward", time.Date(2024, 3, 10, 0, 0, 0, 0, loc)},
		// Fall back: 2024-11-03 02:00 repeats as 01:00 local time.
		{"fall-back", time.Date(2024, 11, 3, 0, 0, 0, 0, loc)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schedule := mustStagger(t, mustParseCron(t, "* * * * *"), "dst-key", 20*time.Second, tc.start)
			end := tc.start.Add(4 * time.Hour)

			// Every delayed instant stays at or after its original and before
			// the following original instant across the transition.
			orig := schedule.Schedule.Next(tc.start.Add(-time.Nanosecond))
			for orig.Before(end) {
				nextOrig := schedule.Schedule.Next(orig)
				delayed := schedule.delayed(orig, nextOrig)
				if delayed.Before(orig) {
					t.Fatalf("delayed %s before original %s", delayed, orig)
				}
				if !nextOrig.IsZero() && !delayed.Before(nextOrig) {
					t.Fatalf("delayed %s crossed next original %s", delayed, nextOrig)
				}
				orig = nextOrig
			}

			// Chained Next() results are strictly increasing through the gap.
			cur := tc.start
			for cur.Before(end) {
				next := schedule.Next(cur)
				if next.IsZero() {
					t.Fatalf("schedule exhausted during %s at %s", tc.name, cur)
				}
				if !next.After(cur) {
					t.Fatalf("%s: %s not strictly after %s", tc.name, next, cur)
				}
				cur = next
			}
		})
	}
}

func TestStaggerExhaustedUnderlyingSchedule(t *testing.T) {
	start := utc(2024, 1, 1, 0, 0, 0, 0)
	last := start.Add(time.Minute)
	underlying := &fixedSchedule{instants: []time.Time{last}}
	schedule := mustStagger(t, underlying, "finite", 20*time.Second, start)

	// Once the only delayed activation is past, the wrapper reports no further
	// activation just like the underlying schedule.
	query := last.Add(21 * time.Second)
	if got := schedule.Next(query); !got.IsZero() {
		t.Fatalf("exhausted schedule: got %s, want zero time", got)
	}

	// Querying between the last original and its delayed instant keeps it.
	offset := schedule.staggerOffset(last)
	if offset > 0 {
		want := last.Add(offset)
		if got := schedule.Next(last.Add(offset / 2)); !got.Equal(want) {
			t.Fatalf("pending last activation: got %s, want %s", got, want)
		}
	}

	// A schedule whose only activation precedes the start time is empty.
	early := &fixedSchedule{instants: []time.Time{start.Add(-time.Second)}}
	empty := mustStagger(t, early, "too-early", 20*time.Second, start)
	if got := empty.Next(start); !got.IsZero() {
		t.Fatalf("activation before start should be dropped, got %s", got)
	}
}

func TestStaggerBeforeStart(t *testing.T) {
	start := utc(2024, 1, 1, 0, 0, 0, 0)
	schedule := mustStagger(t, mustParseCron(t, "* * * * *"), "bounded", 20*time.Second, start)

	for _, query := range []time.Time{
		start.Add(-time.Nanosecond),
		start.Add(-time.Hour),
		time.Date(2023, 12, 31, 23, 59, 59, 0, time.UTC),
	} {
		got, err := schedule.NextAfter(query)
		if !errors.Is(err, ErrStaggerBeforeStart) {
			t.Fatalf("NextAfter(%s): err = %v, want ErrStaggerBeforeStart", query, err)
		}
		if !got.IsZero() {
			t.Fatalf("NextAfter(%s): got %s, want zero time", query, got)
		}

		var panicked bool
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					err, _ := recovered.(error)
					if !errors.Is(err, ErrStaggerBeforeStart) {
						t.Errorf("Next(%s) panicked with %v, want ErrStaggerBeforeStart", query, recovered)
					}
					panicked = true
				}
			}()
			schedule.Next(query)
		}()
		if !panicked {
			t.Fatalf("Next(%s) did not panic", query)
		}
	}

	// Querying exactly at the start time is allowed.
	if got, err := schedule.NextAfter(start); err != nil || got.IsZero() {
		t.Fatalf("NextAfter(start): got %s, err %v", got, err)
	}
}

func TestNewStaggerScheduleValidation(t *testing.T) {
	start := utc(2024, 1, 1, 0, 0, 0, 0)
	valid := mustParseCron(t, "* * * * *")

	cases := []struct {
		name     string
		schedule Schedule
		key      string
		maxDelay time.Duration
		start    time.Time
	}{
		{"nil schedule", nil, "k", time.Second, start},
		{"nil key with NUL byte", valid, "a\x00b", time.Second, start},
		{"zero max delay", valid, "k", 0, start},
		{"negative max delay", valid, "k", -time.Second, start},
		{"zero start", valid, "k", time.Second, time.Time{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := NewStaggerSchedule(tc.schedule, tc.key, tc.maxDelay, tc.start); err == nil {
				t.Fatalf("expected error, got schedule %#v", got)
			}
		})
	}
}

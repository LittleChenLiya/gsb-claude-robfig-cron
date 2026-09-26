package cron

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

// nonMonotonicSchedule violates the Schedule contract by returning its
// input unchanged.
type nonMonotonicSchedule struct{}

func (nonMonotonicSchedule) Next(t time.Time) time.Time { return t }

// finiteSchedule activates at the given fixed times, in order, and then
// reports exhaustion by returning the zero time.
type finiteSchedule struct {
	times []time.Time
}

func (s *finiteSchedule) Next(t time.Time) time.Time {
	for _, ft := range s.times {
		if ft.After(t) {
			return ft
		}
	}
	return time.Time{}
}

func mustParseStandard(t *testing.T, spec string) Schedule {
	t.Helper()
	s, err := ParseStandard(spec)
	if err != nil {
		t.Fatalf("ParseStandard(%q): %v", spec, err)
	}
	return s
}

func atMinutes(base time.Time, mins ...int) []time.Time {
	out := make([]time.Time, len(mins))
	for i, m := range mins {
		out[i] = base.Add(time.Duration(m) * time.Minute)
	}
	return out
}

// Acceptance: the common activations of "every 2 minutes" and "every
// 3 minutes" within a 12 minute window are minutes 6 and 12.
func TestQueryWindowIntersectionEvery2And3Minutes(t *testing.T) {
	start := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	end := start.Add(12 * time.Minute)

	res, err := QueryWindow([]Schedule{Every(2 * time.Minute), Every(3 * time.Minute)},
		start, end, Intersection, WindowOptions{})
	if err != nil {
		t.Fatalf("QueryWindow: %v", err)
	}
	if !res.Complete {
		t.Fatal("expected complete result without a budget")
	}
	want := atMinutes(start, 6, 12)
	if !reflect.DeepEqual(res.Times, want) {
		t.Fatalf("intersection = %v, want %v", res.Times, want)
	}
}

func TestQueryWindowUnionDeduplicates(t *testing.T) {
	start := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	end := start.Add(12 * time.Minute)

	// Both schedules fire at minutes 4, 8 and 12; each instant must be
	// delivered exactly once, in increasing order.
	res, err := QueryWindow([]Schedule{Every(2 * time.Minute), Every(4 * time.Minute)},
		start, end, Union, WindowOptions{})
	if err != nil {
		t.Fatalf("QueryWindow: %v", err)
	}
	want := atMinutes(start, 2, 4, 6, 8, 10, 12)
	if !reflect.DeepEqual(res.Times, want) {
		t.Fatalf("union = %v, want %v", res.Times, want)
	}
	for i := 1; i < len(res.Times); i++ {
		if !res.Times[i].After(res.Times[i-1]) {
			t.Fatalf("times not strictly increasing at %d: %v", i, res.Times)
		}
	}
}

func TestQueryWindowIntersectionDisjoint(t *testing.T) {
	start := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	end := start.Add(30 * time.Minute)

	// Fires only at odd minutes; never coincides with every-2-minutes.
	odd := &finiteSchedule{times: atMinutes(start, 1, 3, 5, 7, 9, 11, 13, 15)}
	res, err := QueryWindow([]Schedule{Every(2 * time.Minute), odd},
		start, end, Intersection, WindowOptions{})
	if err != nil {
		t.Fatalf("QueryWindow: %v", err)
	}
	if !res.Complete {
		t.Fatal("expected complete result")
	}
	if len(res.Times) != 0 {
		t.Fatalf("expected no common activations, got %v", res.Times)
	}
}

func TestQueryWindowScheduleEnds(t *testing.T) {
	start := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	end := start.Add(30 * time.Minute)

	// A schedule reporting the zero time has ended: the intersection ends
	// with it, while the union keeps the remaining schedule.
	short := &finiteSchedule{times: atMinutes(start, 5)}
	res, err := QueryWindow([]Schedule{Every(5 * time.Minute), short},
		start, end, Intersection, WindowOptions{})
	if err != nil {
		t.Fatalf("QueryWindow: %v", err)
	}
	if want := atMinutes(start, 5); !reflect.DeepEqual(res.Times, want) {
		t.Fatalf("intersection = %v, want %v", res.Times, want)
	}

	res, err = QueryWindow([]Schedule{Every(10 * time.Minute), short},
		start, end, Union, WindowOptions{})
	if err != nil {
		t.Fatalf("QueryWindow: %v", err)
	}
	if want := atMinutes(start, 5, 10, 20, 30); !reflect.DeepEqual(res.Times, want) {
		t.Fatalf("union = %v, want %v", res.Times, want)
	}
}

func TestQueryWindowBounds(t *testing.T) {
	start := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	end := start.Add(3 * time.Minute)

	// The window is (start, end]: a firing exactly at start is excluded,
	// one exactly at end is included.
	res, err := QueryWindow([]Schedule{Every(time.Minute)}, start, end, Union, WindowOptions{})
	if err != nil {
		t.Fatalf("QueryWindow: %v", err)
	}
	if want := atMinutes(start, 1, 2, 3); !reflect.DeepEqual(res.Times, want) {
		t.Fatalf("times = %v, want %v", res.Times, want)
	}

	// An empty window delivers nothing.
	res, err = QueryWindow([]Schedule{Every(time.Minute)}, start, start, Union, WindowOptions{})
	if err != nil {
		t.Fatalf("QueryWindow: %v", err)
	}
	if !res.Complete || len(res.Times) != 0 {
		t.Fatalf("empty window = %+v, want complete empty result", res)
	}

	// An inverted window is an error.
	if _, err = QueryWindow([]Schedule{Every(time.Minute)}, end, start, Union, WindowOptions{}); err == nil {
		t.Fatal("expected error for end before start")
	}
}

func TestQueryWindowDST(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("time zone database unavailable: %v", err)
	}
	// 2026-03-08 is the spring-forward day in America/New_York.
	sched := mustParseStandard(t, "CRON_TZ=America/New_York 30 9 * * *")
	start := time.Date(2026, 3, 7, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)

	res, err := QueryWindow([]Schedule{sched}, start, end, Union, WindowOptions{})
	if err != nil {
		t.Fatalf("QueryWindow: %v", err)
	}
	// 09:30 local every day; the UTC offset changes across the DST jump.
	// Compare instants, since the schedule reports times in the window's
	// own location.
	want := []time.Time{
		time.Date(2026, 3, 7, 9, 30, 0, 0, loc),
		time.Date(2026, 3, 8, 9, 30, 0, 0, loc),
		time.Date(2026, 3, 9, 9, 30, 0, 0, loc),
	}
	if len(res.Times) != len(want) {
		t.Fatalf("times = %v, want %v", res.Times, want)
	}
	for i := range want {
		if !res.Times[i].Equal(want[i]) {
			t.Fatalf("times = %v, want %v", res.Times, want)
		}
	}
	for _, ts := range res.Times {
		if got := ts.In(loc).Hour()*60 + ts.In(loc).Minute(); got != 9*60+30 {
			t.Fatalf("activation %v is not 09:30 in New York", ts)
		}
	}
}

func TestQueryWindowBadSchedule(t *testing.T) {
	start := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	// A schedule whose Next does not increase must fail loudly, in both
	// modes, rather than looping forever.
	for _, mode := range []WindowMode{Union, Intersection} {
		_, err := QueryWindow([]Schedule{nonMonotonicSchedule{}}, start, end, mode, WindowOptions{})
		if !errors.Is(err, ErrNonMonotonicSchedule) {
			t.Fatalf("%v mode: err = %v, want %v", mode, err, ErrNonMonotonicSchedule)
		}
		_, err = QueryWindow([]Schedule{Every(time.Minute), nonMonotonicSchedule{}},
			start, end, mode, WindowOptions{})
		if !errors.Is(err, ErrNonMonotonicSchedule) {
			t.Fatalf("%v mode mixed: err = %v, want %v", mode, err, ErrNonMonotonicSchedule)
		}
	}

	if _, err := QueryWindow(nil, start, end, Union, WindowOptions{}); err == nil {
		t.Fatal("expected error for no schedules")
	}
	if _, err := QueryWindow([]Schedule{nil}, start, end, Union, WindowOptions{}); err == nil {
		t.Fatal("expected error for nil schedule")
	}
	if _, err := QueryWindow([]Schedule{Every(time.Minute)}, start, end, WindowMode(42), WindowOptions{}); err == nil {
		t.Fatal("expected error for unknown mode")
	}
}

// runWindowed executes a query in budget-limited segments, resuming from
// the cursor until the result is complete, and returns all delivered times.
func runWindowed(t *testing.T, schedules []Schedule, start, end time.Time, mode WindowMode, budget int) []time.Time {
	t.Helper()
	opts := WindowOptions{MaxNextCalls: budget}
	var all []time.Time
	for i := 0; i < 10000; i++ {
		res, err := QueryWindow(schedules, start, end, mode, opts)
		if err != nil {
			t.Fatalf("QueryWindow: %v", err)
		}
		// A resumed segment must never repeat the last delivered instant.
		if len(all) > 0 && len(res.Times) > 0 && !res.Times[0].After(all[len(all)-1]) {
			t.Fatalf("segment repeats previous instant: %v after %v", res.Times, all)
		}
		all = append(all, res.Times...)
		if res.Complete {
			return all
		}
		if res.Cursor == nil {
			t.Fatal("incomplete result without a cursor")
		}
		opts.Cursor = res.Cursor
	}
	t.Fatal("query did not terminate within 10000 segments")
	return nil
}

func TestQueryWindowBudgetResume(t *testing.T) {
	start := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	end := start.Add(60 * time.Minute)

	cases := []struct {
		name      string
		schedules []Schedule
		mode      WindowMode
	}{
		{"union", []Schedule{Every(2 * time.Minute), Every(3 * time.Minute)}, Union},
		{"intersection", []Schedule{Every(2 * time.Minute), Every(3 * time.Minute)}, Intersection},
		{"union finite", []Schedule{Every(7 * time.Minute), &finiteSchedule{times: atMinutes(start, 4, 9, 33)}}, Union},
		{"intersection finite", []Schedule{Every(5 * time.Minute), &finiteSchedule{times: atMinutes(start, 10, 25, 90)}}, Intersection},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want, err := QueryWindow(tc.schedules, start, end, tc.mode, WindowOptions{})
			if err != nil {
				t.Fatalf("QueryWindow: %v", err)
			}
			if !want.Complete {
				t.Fatal("unlimited query must be complete")
			}
			// Budgets of 1, 3 and 7 force many interruptions, including
			// interruptions mid-initialization and mid-step.
			for _, budget := range []int{1, 3, 7} {
				got := runWindowed(t, tc.schedules, start, end, tc.mode, budget)
				if !reflect.DeepEqual(got, want.Times) {
					t.Fatalf("budget %d: segmented = %v, want %v", budget, got, want.Times)
				}
			}
		})
	}
}

func TestQueryWindowCursorValidation(t *testing.T) {
	start := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	end := start.Add(10 * time.Minute)

	res, err := QueryWindow([]Schedule{Every(time.Minute)}, start, end, Union,
		WindowOptions{MaxNextCalls: 1})
	if err != nil {
		t.Fatalf("QueryWindow: %v", err)
	}
	if res.Complete || res.Cursor == nil {
		t.Fatal("expected interrupted query with a cursor")
	}

	// A cursor built for a different number of schedules is rejected.
	if _, err := QueryWindow([]Schedule{Every(time.Minute), Every(2 * time.Minute)},
		start, end, Union, WindowOptions{Cursor: res.Cursor}); err == nil {
		t.Fatal("expected error for cursor/schedule count mismatch")
	}
}

// ExampleQueryWindow prints, without waiting for real time, the instants in
// a fixed 12 minute window where both an every-2-minutes and an
// every-3-minutes schedule would fire.
func ExampleQueryWindow() {
	start := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	res, err := QueryWindow(
		[]Schedule{Every(2 * time.Minute), Every(3 * time.Minute)},
		start, start.Add(12*time.Minute), Intersection, WindowOptions{})
	if err != nil {
		panic(err)
	}
	for _, ts := range res.Times {
		fmt.Println(ts.Format("15:04:05"))
	}
	// Output:
	// 10:06:00
	// 10:12:00
}

// ExampleQueryWindow_union prints every instant at which either of two
// schedules fires within a fixed window, deduplicated and in order.
func ExampleQueryWindow_union() {
	start := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	res, err := QueryWindow(
		[]Schedule{Every(2 * time.Minute), Every(3 * time.Minute)},
		start, start.Add(6*time.Minute), Union, WindowOptions{})
	if err != nil {
		panic(err)
	}
	for _, ts := range res.Times {
		fmt.Println(ts.Format("15:04:05"))
	}
	// Output:
	// 10:02:00
	// 10:03:00
	// 10:04:00
	// 10:06:00
}

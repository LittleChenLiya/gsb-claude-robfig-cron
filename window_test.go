package cron

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// listSchedule activates at the given fixed times, in order, and returns the
// zero time once they are exhausted.
type listSchedule struct{ times []time.Time }

func (s listSchedule) Next(t time.Time) time.Time {
	for _, next := range s.times {
		if next.After(t) {
			return next
		}
	}
	return time.Time{}
}

// stuckSchedule violates the Schedule contract by never advancing.
type stuckSchedule struct{}

func (stuckSchedule) Next(t time.Time) time.Time { return t }

// emptySchedule is always exhausted.
type emptySchedule struct{}

func (emptySchedule) Next(time.Time) time.Time { return time.Time{} }

var windowStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func at(minutes ...int) []time.Time {
	times := make([]time.Time, len(minutes))
	for i, m := range minutes {
		times[i] = windowStart.Add(time.Duration(m) * time.Minute)
	}
	return times
}

func assertTimes(t *testing.T, got, want []time.Time) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d times %v, want %d times %v", len(got), got, len(want), want)
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestIntersectionEvery2And3Minutes(t *testing.T) {
	res, err := WindowQuery{
		Schedules: []Schedule{Every(2 * time.Minute), Every(3 * time.Minute)},
		Start:     windowStart,
		End:       windowStart.Add(12 * time.Minute),
		Mode:      Intersection,
	}.Query(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Complete {
		t.Fatal("expected complete result without a budget")
	}
	assertTimes(t, res.Times, at(6, 12))
}

func TestUnionDeduplicates(t *testing.T) {
	// Two identical schedules must not produce duplicate times, and
	// coincident activations of different schedules collapse into one.
	res, err := WindowQuery{
		Schedules: []Schedule{
			Every(2 * time.Minute),
			Every(2 * time.Minute),
			Every(5 * time.Minute),
		},
		Start: windowStart,
		End:   windowStart.Add(10 * time.Minute),
		Mode:  Union,
	}.Query(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertTimes(t, res.Times, at(2, 4, 5, 6, 8, 10))
}

func TestIntersectionDisjoint(t *testing.T) {
	// Activations at minutes 2,4,6 and 3,5,7 never coincide.
	res, err := WindowQuery{
		Schedules: []Schedule{
			listSchedule{at(2, 4, 6)},
			listSchedule{at(3, 5, 7)},
		},
		Start: windowStart,
		End:   windowStart.Add(10 * time.Minute),
		Mode:  Intersection,
	}.Query(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Complete {
		t.Fatal("expected complete result")
	}
	if len(res.Times) != 0 {
		t.Fatalf("expected no common times, got %v", res.Times)
	}
}

func TestWindowBounds(t *testing.T) {
	// The window is (start, end]: an activation exactly at start is
	// excluded, one exactly at end is included.
	for _, mode := range []CombineMode{Union, Intersection} {
		res, err := WindowQuery{
			Schedules: []Schedule{Every(time.Minute), Every(time.Minute)},
			Start:     windowStart.Add(2 * time.Minute),
			End:       windowStart.Add(5 * time.Minute),
			Mode:      mode,
		}.Query(nil)
		if err != nil {
			t.Fatalf("mode %v: unexpected error: %v", mode, err)
		}
		assertTimes(t, res.Times, at(3, 4, 5))
	}
}

func TestWindowQueryDST(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("time zone database unavailable: %v", err)
	}

	morning, err := ParseStandard("30 9 * * *")
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	morning.(*SpecSchedule).Location = loc
	evening, err := ParseStandard("30 21 * * *")
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	evening.(*SpecSchedule).Location = loc

	// The window spans the 2026-03-08 spring-forward transition.
	start := time.Date(2026, 3, 7, 0, 0, 0, 0, loc)
	end := time.Date(2026, 3, 10, 0, 0, 0, 0, loc)

	res, err := WindowQuery{
		Schedules: []Schedule{morning, evening},
		Start:     start,
		End:       end,
		Mode:      Union,
	}.Query(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var want []time.Time
	for day := 7; day <= 9; day++ {
		want = append(want,
			time.Date(2026, 3, day, 9, 30, 0, 0, loc),
			time.Date(2026, 3, day, 21, 30, 0, 0, loc),
		)
	}
	assertTimes(t, res.Times, want)

	// Intersection of the two distinct schedules is empty, and the query
	// must terminate across the DST gap.
	res, err = WindowQuery{
		Schedules: []Schedule{morning, evening},
		Start:     start,
		End:       end,
		Mode:      Intersection,
	}.Query(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res.Times) != 0 {
		t.Fatalf("expected no common times, got %v", res.Times)
	}
}

func TestWindowQueryNonMonotonic(t *testing.T) {
	for _, mode := range []CombineMode{Union, Intersection} {
		_, err := WindowQuery{
			Schedules: []Schedule{Every(time.Minute), stuckSchedule{}},
			Start:     windowStart,
			End:       windowStart.Add(time.Hour),
			Mode:      mode,
		}.Query(nil)
		if !errors.Is(err, ErrNonMonotonic) {
			t.Fatalf("mode %v: got %v, want ErrNonMonotonic", mode, err)
		}
	}
}

func TestWindowQueryExhaustedSchedule(t *testing.T) {
	// Union: an exhausted schedule simply contributes nothing.
	res, err := WindowQuery{
		Schedules: []Schedule{Every(2 * time.Minute), emptySchedule{}},
		Start:     windowStart,
		End:       windowStart.Add(6 * time.Minute),
		Mode:      Union,
	}.Query(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertTimes(t, res.Times, at(2, 4, 6))

	// Intersection: an exhausted schedule ends the query.
	res, err = WindowQuery{
		Schedules: []Schedule{Every(2 * time.Minute), emptySchedule{}},
		Start:     windowStart,
		End:       windowStart.Add(6 * time.Minute),
		Mode:      Intersection,
	}.Query(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.Complete || len(res.Times) != 0 {
		t.Fatalf("got %+v, want complete empty result", res)
	}
}

// runSegmented executes the query with the given Next-call budget, resuming
// from the cursor until the result is complete, and returns the concatenated
// times of all segments.
func runSegmented(t *testing.T, q WindowQuery, budget int) []time.Time {
	t.Helper()
	q.MaxNextCalls = budget
	var all []time.Time
	var cursor *WindowCursor
	for i := 0; i < 100000; i++ {
		res, err := q.Query(cursor)
		if err != nil {
			t.Fatalf("budget %d: unexpected error: %v", budget, err)
		}
		all = append(all, res.Times...)
		if res.Complete {
			return all
		}
		if res.Cursor == nil {
			t.Fatalf("budget %d: incomplete result without cursor", budget)
		}
		cursor = res.Cursor
	}
	t.Fatalf("budget %d: query did not terminate", budget)
	return nil
}

func TestWindowQueryBudgetResume(t *testing.T) {
	queries := map[string]WindowQuery{
		"union": {
			Schedules: []Schedule{
				Every(2 * time.Minute),
				Every(3 * time.Minute),
				Every(5 * time.Minute),
			},
			Start: windowStart,
			End:   windowStart.Add(time.Hour),
			Mode:  Union,
		},
		"intersection": {
			Schedules: []Schedule{Every(2 * time.Minute), Every(3 * time.Minute)},
			Start:     windowStart,
			End:       windowStart.Add(time.Hour),
			Mode:      Intersection,
		},
	}

	for name, q := range queries {
		reference := runSegmented(t, q, 0) // unlimited budget
		if len(reference) == 0 {
			t.Fatalf("%s: reference result is empty", name)
		}
		for _, budget := range []int{1, 2, 3, 5, 8, 13} {
			got := runSegmented(t, q, budget)
			assertTimes(t, got, reference)
		}
		// Segments stitched together must be strictly increasing, i.e. the
		// cursor never re-delivers the last reported time.
		for i := 1; i < len(reference); i++ {
			if !reference[i].After(reference[i-1]) {
				t.Fatalf("%s: times not strictly increasing: %v", name, reference)
			}
		}
	}
}

func TestWindowQueryBudgetInterruptsInitialization(t *testing.T) {
	// A budget smaller than the schedule count interrupts the query before
	// initialization finishes; resuming still yields the full result.
	q := WindowQuery{
		Schedules: []Schedule{Every(2 * time.Minute), Every(3 * time.Minute)},
		Start:     windowStart,
		End:       windowStart.Add(12 * time.Minute),
		Mode:      Intersection,
	}
	assertTimes(t, runSegmented(t, q, 1), at(6, 12))
}

func TestWindowQueryValidation(t *testing.T) {
	valid := WindowQuery{
		Schedules: []Schedule{Every(time.Minute)},
		Start:     windowStart,
		End:       windowStart.Add(time.Hour),
		Mode:      Union,
	}

	q := valid
	q.Schedules = nil
	if _, err := q.Query(nil); err == nil {
		t.Fatal("expected error for empty schedules")
	}

	q = valid
	q.End = windowStart.Add(-time.Hour)
	if _, err := q.Query(nil); err == nil {
		t.Fatal("expected error for end before start")
	}

	q = valid
	q.Mode = CombineMode(42)
	if _, err := q.Query(nil); err == nil {
		t.Fatal("expected error for unknown mode")
	}

	// A cursor from an interrupted union query must not resume an
	// intersection query, nor one with a different schedule count.
	q = valid
	q.Schedules = []Schedule{Every(time.Minute), Every(2 * time.Minute)}
	q.MaxNextCalls = 3
	res, err := q.Query(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Complete {
		t.Fatal("expected incomplete result")
	}

	other := valid
	other.Mode = Intersection
	if _, err := other.Query(res.Cursor); err == nil {
		t.Fatal("expected error for mode mismatch")
	}

	other = valid
	other.Schedules = []Schedule{Every(time.Minute)}
	if _, err := other.Query(res.Cursor); err == nil {
		t.Fatal("expected error for schedule count mismatch")
	}
}

// ExampleWindowQuery prints the common activation times of an every-2-minutes
// and an every-3-minutes schedule inside a fixed 12-minute window, without
// waiting for anything to actually trigger.
func ExampleWindowQuery() {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	res, err := WindowQuery{
		Schedules: []Schedule{Every(2 * time.Minute), Every(3 * time.Minute)},
		Start:     start,
		End:       start.Add(12 * time.Minute),
		Mode:      Intersection,
	}.Query(nil)
	if err != nil {
		panic(err)
	}
	for _, tm := range res.Times {
		fmt.Println(tm.Sub(start), "after window start")
	}
	// Output:
	// 6m0s after window start
	// 12m0s after window start
}

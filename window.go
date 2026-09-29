package cron

import (
	"errors"
	"fmt"
	"time"
)

// CombineMode determines how the activation times of multiple schedules are
// combined by a window query.
type CombineMode int

const (
	// Union reports every time at which any of the schedules activates.
	Union CombineMode = iota
	// Intersection reports only the times at which all of the schedules
	// activate simultaneously.
	Intersection
)

// ErrNonMonotonic is returned when a schedule's Next method returns a
// non-zero time that does not come after its argument. Schedules are
// required to be strictly increasing; violating ones cannot be queried.
var ErrNonMonotonic = errors.New("cron: schedule Next returned a non-increasing time")

// errBudget is an internal sentinel signaling that the Next-call budget of a
// window query has been exhausted.
var errBudget = errors.New("cron: next-call budget exhausted")

// WindowQuery describes a query for the activation times of one or more
// schedules inside the half-open window (Start, End]. The result is reported
// in increasing absolute time, with duplicates removed.
type WindowQuery struct {
	// Schedules are the schedules to combine. At least one is required.
	Schedules []Schedule

	// Start and End bound the query window. Times strictly after Start and
	// up to and including End are reported.
	Start time.Time
	End   time.Time

	// Mode selects Union or Intersection semantics.
	Mode CombineMode

	// MaxNextCalls bounds the number of underlying Schedule.Next calls a
	// single Query invocation may make. Zero or negative means unlimited.
	// When the budget is exhausted, the returned result is incomplete and
	// carries a cursor that can be used to resume the query.
	MaxNextCalls int
}

// WindowCursor captures the resumable state of an incomplete window query.
// Passing it back to WindowQuery.Query continues the query where it left
// off, without re-delivering the last reported time.
type WindowCursor struct {
	mode CombineMode
	// frontier holds, per schedule, the earliest activation time not yet
	// considered (zero if the schedule is exhausted).
	frontier []time.Time
	// initialized counts how many frontier entries were seeded with the
	// schedule's first activation after Start; the rest is still pending.
	initialized int
	// pending, when hasPending is set, is an intersection time that was
	// already delivered but whose per-schedule advance was interrupted.
	pending    time.Time
	hasPending bool
}

// WindowResult is the outcome of a window query.
type WindowResult struct {
	// Times are the activation times found, in increasing absolute time and
	// free of duplicates.
	Times []time.Time

	// Complete reports whether the whole window was scanned. If false, the
	// Next-call budget was exhausted and Cursor holds the resumable state.
	Complete bool

	// Cursor is set when Complete is false. Hand it to WindowQuery.Query to
	// resume; resuming never re-delivers an already reported time.
	Cursor *WindowCursor
}

// Query runs the window query. Pass a nil cursor to start a fresh query, or
// the cursor of a previous incomplete result to resume it.
func (q WindowQuery) Query(cursor *WindowCursor) (WindowResult, error) {
	if len(q.Schedules) == 0 {
		return WindowResult{}, errors.New("cron: window query requires at least one schedule")
	}
	if q.End.Before(q.Start) {
		return WindowResult{}, errors.New("cron: window query end is before start")
	}
	if q.Mode != Union && q.Mode != Intersection {
		return WindowResult{}, fmt.Errorf("cron: unknown combine mode %d", q.Mode)
	}

	it := &windowIterator{
		schedules: q.Schedules,
		end:       q.End,
		mode:      q.Mode,
		remaining: q.MaxNextCalls,
		frontier:  make([]time.Time, len(q.Schedules)),
	}
	if it.remaining <= 0 {
		it.remaining = -1 // unlimited
	}

	if cursor != nil {
		if cursor.mode != q.Mode {
			return WindowResult{}, errors.New("cron: cursor combine mode does not match query")
		}
		if cursor.frontier != nil && len(cursor.frontier) != len(q.Schedules) {
			return WindowResult{}, errors.New("cron: cursor schedule count does not match query")
		}
		copy(it.frontier, cursor.frontier)
		it.initialized = cursor.initialized
		it.pending = cursor.pending
		it.hasPending = cursor.hasPending
	}

	// Initialization: seed each schedule's frontier with its first
	// activation after Start. A small budget may interrupt this partway;
	// the cursor then carries the partial progress.
	for ; it.initialized < len(q.Schedules); it.initialized++ {
		next, err := it.next(it.initialized, q.Start)
		if err == errBudget {
			return it.incomplete(nil), nil
		}
		if err != nil {
			return WindowResult{}, err
		}
		it.frontier[it.initialized] = next
	}

	if q.Mode == Union {
		return it.runUnion(nil)
	}
	return it.runIntersection(nil)
}

// windowIterator carries the mutable state of a single Query invocation.
type windowIterator struct {
	schedules   []Schedule
	end         time.Time
	mode        CombineMode
	remaining   int // -1 means unlimited
	frontier    []time.Time
	initialized int
	pending     time.Time
	hasPending  bool
}

// next advances schedule i past from, enforcing the call budget and the
// strictly-increasing contract of Schedule.Next.
func (it *windowIterator) next(i int, from time.Time) (time.Time, error) {
	if it.remaining == 0 {
		return time.Time{}, errBudget
	}
	t := it.schedules[i].Next(from)
	if it.remaining > 0 {
		it.remaining--
	}
	if !t.IsZero() && !t.After(from) {
		return time.Time{}, fmt.Errorf("%w (schedule %d)", ErrNonMonotonic, i)
	}
	return t, nil
}

// incomplete builds an incomplete result carrying a cursor over the current
// frontier.
func (it *windowIterator) incomplete(times []time.Time) WindowResult {
	frontier := make([]time.Time, len(it.frontier))
	copy(frontier, it.frontier)
	return WindowResult{
		Times:    times,
		Complete: false,
		Cursor: &WindowCursor{
			mode:        it.mode,
			frontier:    frontier,
			initialized: it.initialized,
			pending:     it.pending,
			hasPending:  it.hasPending,
		},
	}
}

// runUnion merges the per-schedule activation streams. Each iteration
// delivers the smallest pending frontier and advances every schedule whose
// frontier equals it, which deduplicates coincident activations.
func (it *windowIterator) runUnion(times []time.Time) (WindowResult, error) {
	for {
		minIdx := -1
		var min time.Time
		for i, f := range it.frontier {
			if f.IsZero() {
				continue
			}
			if minIdx == -1 || f.Before(min) {
				min, minIdx = f, i
			}
		}
		if minIdx == -1 || min.After(it.end) {
			return WindowResult{Times: times, Complete: true}, nil
		}

		// Advance every schedule sitting on min. Each advance is committed
		// immediately: delivery itself needs no budget, and a partially
		// advanced frontier still leaves at least one schedule on min, so
		// resuming delivers it exactly once.
		for i := range it.frontier {
			if !it.frontier[i].Equal(min) {
				continue
			}
			next, err := it.next(i, min)
			if err == errBudget {
				return it.incomplete(times), nil
			}
			if err != nil {
				return WindowResult{}, err
			}
			it.frontier[i] = next
		}
		times = append(times, min)
	}
}

// runIntersection finds the times at which all schedules activate. Rather
// than probing the window instant by instant, a schedule lagging behind the
// current candidate is advanced activation-by-activation until it catches
// up, so each schedule stays on its own activation grid (relative schedules
// such as ConstantDelaySchedule depend on being called from their previous
// activation).
func (it *windowIterator) runIntersection(times []time.Time) (WindowResult, error) {
	if it.hasPending {
		// A previous invocation delivered pending but was interrupted
		// while advancing the schedules past it; finish that advance
		// before looking for further common times.
		for i := range it.frontier {
			if !it.frontier[i].Equal(it.pending) {
				continue
			}
			next, err := it.next(i, it.pending)
			if err == errBudget {
				return it.incomplete(times), nil
			}
			if err != nil {
				return WindowResult{}, err
			}
			it.frontier[i] = next
		}
		it.hasPending = false
	}

	for {
		candidate, ok := it.candidate()
		if !ok || candidate.After(it.end) {
			return WindowResult{Times: times, Complete: true}, nil
		}

		// Advance every schedule lagging behind the candidate until it
		// reaches or passes it.
		lagging := false
		for i, f := range it.frontier {
			for !f.IsZero() && f.Before(candidate) {
				lagging = true
				next, err := it.next(i, f)
				if err == errBudget {
					it.frontier[i] = f
					return it.incomplete(times), nil
				}
				if err != nil {
					return WindowResult{}, err
				}
				f = next
			}
			it.frontier[i] = f
		}
		if lagging {
			// A schedule overtook the candidate; recompute and realign.
			continue
		}

		// All schedules agree on candidate: deliver it, then advance every
		// schedule past it. If the budget stops the advance midway, the
		// cursor remembers the pending delivery so resuming finishes the
		// advance without delivering candidate a second time.
		times = append(times, candidate)
		it.pending = candidate
		it.hasPending = true
		for i := range it.frontier {
			next, err := it.next(i, candidate)
			if err == errBudget {
				return it.incomplete(times), nil
			}
			if err != nil {
				return WindowResult{}, err
			}
			it.frontier[i] = next
		}
		it.hasPending = false
	}
}

// candidate returns the largest pending frontier time, which is the earliest
// time at which all schedules could next agree. ok is false when any
// schedule is exhausted, which ends the intersection.
func (it *windowIterator) candidate() (candidate time.Time, ok bool) {
	candidate = it.frontier[0]
	for _, f := range it.frontier {
		if f.IsZero() {
			return time.Time{}, false
		}
		if f.After(candidate) {
			candidate = f
		}
	}
	return candidate, true
}

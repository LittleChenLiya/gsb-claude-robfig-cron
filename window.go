package cron

import (
	"errors"
	"fmt"
	"time"
)

// WindowMode selects how the activation times of multiple schedules are
// combined within a queried window.
type WindowMode int

const (
	// Union emits every time at which any of the schedules activates.
	Union WindowMode = iota
	// Intersection emits only the times at which all of the schedules
	// activate simultaneously.
	Intersection
)

// String returns a human-readable name for the mode.
func (m WindowMode) String() string {
	switch m {
	case Union:
		return "union"
	case Intersection:
		return "intersection"
	}
	return "unknown"
}

// ErrNonMonotonicSchedule is returned when a schedule's Next method returns
// a time that is not strictly later than the time it was queried with. Such
// a schedule violates the Schedule contract and cannot be combined safely.
var ErrNonMonotonicSchedule = errors.New("cron: schedule returned a non-increasing activation time")

// WindowOptions tunes a QueryWindow call.
type WindowOptions struct {
	// MaxNextCalls bounds the total number of Schedule.Next calls made
	// during the query. Values <= 0 mean no limit. When the limit is
	// reached, the result is marked incomplete and carries a Cursor that
	// can be handed back to QueryWindow to resume where the query stopped.
	MaxNextCalls int

	// Cursor resumes a previously interrupted query. It must come from the
	// Cursor field of an incomplete WindowResult produced with the same
	// schedules, mode and window end. The window start is ignored when a
	// cursor is supplied; enumeration continues strictly after the last
	// delivered time, so no instant is delivered twice.
	Cursor *WindowCursor
}

// WindowCursor captures the resumable state of an interrupted window query.
// Its fields are exported so it can be serialized by callers (e.g. as JSON)
// and stored between QueryWindow calls.
type WindowCursor struct {
	// Last is the last activation time delivered to the caller. Enumeration
	// resumes strictly after this instant.
	Last time.Time
	// Initialized reports whether every entry of Nexts holds a computed
	// candidate. While false, zero entries are schedules whose first
	// candidate has not been computed yet; once true, a zero entry means
	// the schedule is exhausted and must not be queried again.
	Initialized bool
	// Nexts holds, per schedule, the next candidate activation strictly
	// after Last. A zero entry means the schedule has no further
	// activations, or that its first candidate has not been computed yet.
	Nexts []time.Time
}

// WindowResult is the outcome of a QueryWindow call.
type WindowResult struct {
	// Times are the combined activation times in (start, end], in
	// increasing absolute order, with duplicates removed.
	Times []time.Time
	// Complete is true when the whole window was enumerated. It is false
	// only when the MaxNextCalls budget was exhausted; in that case Cursor
	// is non-nil and can be used to resume the query.
	Complete bool
	// Cursor is the resumable state of an incomplete query.
	Cursor *WindowCursor
}

// QueryWindow enumerates the activation times of the given schedules,
// combined according to mode, within the absolute window (start, end]: the
// start is exclusive, the end is inclusive. Results are delivered in
// increasing absolute order and deduplicated.
//
// The enumeration never waits for real time; it only calls Schedule.Next.
// A schedule reports exhaustion by returning the zero time from Next. If
// any schedule's Next returns a time that is not strictly later than its
// input, QueryWindow fails with ErrNonMonotonicSchedule.
//
// Intersection mode does not probe time nanosecond by nanosecond: schedules
// lagging behind the newest candidate are advanced along their own
// activations directly to that candidate.
func QueryWindow(schedules []Schedule, start, end time.Time, mode WindowMode, opts WindowOptions) (WindowResult, error) {
	if len(schedules) == 0 {
		return WindowResult{}, errors.New("cron: QueryWindow requires at least one schedule")
	}
	for i, s := range schedules {
		if s == nil {
			return WindowResult{}, fmt.Errorf("cron: schedule %d is nil", i)
		}
	}
	if end.Before(start) {
		return WindowResult{}, errors.New("cron: window end is before start")
	}
	if mode != Union && mode != Intersection {
		return WindowResult{}, fmt.Errorf("cron: unknown window mode %d", int(mode))
	}

	last := start
	nexts := make([]time.Time, len(schedules))
	if opts.Cursor != nil {
		if len(opts.Cursor.Nexts) != len(schedules) {
			return WindowResult{}, fmt.Errorf("cron: cursor tracks %d schedules, got %d",
				len(opts.Cursor.Nexts), len(schedules))
		}
		last = opts.Cursor.Last
		copy(nexts, opts.Cursor.Nexts)
	}

	budget := opts.MaxNextCalls
	if budget <= 0 {
		budget = -1 // unlimited
	}
	q := &windowQuery{
		schedules: schedules,
		end:       end,
		budget:    budget,
		last:      last,
		nexts:     nexts,
	}

	// Fill in any candidate not computed yet. This covers both a fresh
	// query (all entries zero) and a resumed one whose budget ran out
	// mid-initialization. Once initialization finished, zero entries are
	// exhausted schedules and are left alone.
	if opts.Cursor == nil || !opts.Cursor.Initialized {
		for i := range q.nexts {
			if !q.nexts[i].IsZero() {
				continue
			}
			n, ok := q.advance(i, q.last)
			if !ok {
				return q.incomplete(), nil
			}
			if err := checkNext(q.last, n); err != nil {
				return WindowResult{}, err
			}
			q.nexts[i] = n
		}
	}
	q.initialized = true

	var err error
	switch mode {
	case Union:
		err = q.runUnion()
	case Intersection:
		err = q.runIntersection()
	}
	if err != nil {
		return WindowResult{}, err
	}
	if q.exhausted() {
		return q.incomplete(), nil
	}
	return WindowResult{Times: q.times, Complete: true}, nil
}

// windowQuery holds the mutable state of a single QueryWindow call.
type windowQuery struct {
	schedules   []Schedule
	end         time.Time
	budget      int // remaining Next calls; negative means unlimited
	last        time.Time
	initialized bool
	nexts       []time.Time
	times       []time.Time
}

// advance calls Next on schedule i with argument t, enforcing the budget
// and the monotonicity contract. It reports false when the budget is
// exhausted before the call can be made.
func (q *windowQuery) advance(i int, t time.Time) (time.Time, bool) {
	if q.budget == 0 {
		return time.Time{}, false
	}
	if q.budget > 0 {
		q.budget--
	}
	return q.schedules[i].Next(t), true
}

// exhausted reports whether the Next-call budget ran out.
func (q *windowQuery) exhausted() bool {
	return q.budget == 0
}

func (q *windowQuery) incomplete() WindowResult {
	nexts := make([]time.Time, len(q.nexts))
	copy(nexts, q.nexts)
	return WindowResult{
		Times:    q.times,
		Complete: false,
		Cursor:   &WindowCursor{Last: q.last, Initialized: q.initialized, Nexts: nexts},
	}
}

// emit records a combined activation time, skipping anything not strictly
// after the last delivered time so resumed queries never repeat an instant.
func (q *windowQuery) emit(t time.Time) {
	if !t.After(q.last) {
		return
	}
	q.times = append(q.times, t)
	q.last = t
}

// checkNext validates that n is a legal successor of prev.
func checkNext(prev, n time.Time) error {
	if !n.IsZero() && !n.After(prev) {
		return ErrNonMonotonicSchedule
	}
	return nil
}

// runUnion merges the activation streams of all schedules.
func (q *windowQuery) runUnion() error {
	for {
		// The next delivered time is the smallest pending candidate.
		m := time.Time{}
		for _, n := range q.nexts {
			if n.IsZero() {
				continue
			}
			if m.IsZero() || n.Before(m) {
				m = n
			}
		}
		if m.IsZero() || m.After(q.end) {
			return nil
		}
		q.emit(m)
		// Advance every schedule that fired at m, deduplicating the
		// instant across schedules.
		for i := range q.nexts {
			if !q.nexts[i].Equal(m) {
				continue
			}
			n, ok := q.advance(i, m)
			if !ok {
				return nil
			}
			if err := checkNext(m, n); err != nil {
				return err
			}
			q.nexts[i] = n
		}
	}
}

// runIntersection emits only instants where every schedule activates.
// Schedules lagging behind the newest candidate are advanced directly to
// their first activation at or after that candidate by chaining their own
// Next steps, rather than probing time nanosecond by nanosecond. Stepping
// from a schedule's own last activation (instead of an arbitrary instant)
// also preserves the phase of relative schedules such as
// ConstantDelaySchedule, whose Next result depends on the query time.
func (q *windowQuery) runIntersection() error {
	for {
		// A schedule reporting the zero time has ended, and with it the
		// intersection.
		min, max := q.nexts[0], q.nexts[0]
		for _, n := range q.nexts[1:] {
			if n.Before(min) {
				min = n
			}
			if n.After(max) {
				max = n
			}
		}
		if min.IsZero() || min.After(q.end) {
			return nil
		}
		if min.Equal(max) {
			// All schedules agree: deliver the instant and step every
			// schedule past it.
			q.emit(min)
			for i := range q.nexts {
				n, ok := q.advance(i, min)
				if !ok {
					return nil
				}
				if err := checkNext(min, n); err != nil {
					return err
				}
				q.nexts[i] = n
			}
			continue
		}
		// Push each lagging schedule forward to the vicinity of the newest
		// candidate.
		for i := range q.nexts {
			for !q.nexts[i].IsZero() && q.nexts[i].Before(max) {
				prev := q.nexts[i]
				n, ok := q.advance(i, prev)
				if !ok {
					return nil
				}
				if err := checkNext(prev, n); err != nil {
					return err
				}
				q.nexts[i] = n
			}
		}
	}
}

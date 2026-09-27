package cron

import (
	"errors"
	"hash/fnv"
	"strconv"
	"strings"
	"time"
)

// ErrStaggerBeforeStart is returned by StaggerSchedule.NextAfter when it is
// queried with a time earlier than the explicit start time fixed at wrapping
// time. StaggerSchedule.Next reports the same condition as a panic.
var ErrStaggerBeforeStart = errors.New("cron: stagger schedule queried before its explicit start time")

// StaggerSchedule wraps another Schedule and shifts every one of its
// activation instants by a deterministic, key-dependent delay. The same key
// always produces the same delay for the same original activation instant, so
// many jobs staggering from the same instant (e.g. every minute on the
// minute) are spread out repeatably instead of colliding.
//
// A delayed activation never crosses the following original activation
// instant: when the computed delay would reach or pass it, the activation is
// clamped to one nanosecond before that next original instant.
//
// Start is the explicit origin of the wrapper. Original activations earlier
// than Start are never exposed, and querying Next/NextAfter with a time before
// Start fails. Querying at or after Start still returns the delayed instant
// of an original activation that has already passed but whose delayed instant
// is still in the future.
type StaggerSchedule struct {
	// Schedule is the wrapped schedule providing the original activations.
	Schedule Schedule

	// Key is the stable stagger key; the same key and original instant always
	// yield the same delay. It must not contain a NUL byte.
	Key string

	// MaxDelay bounds the stagger window. Computed delays are in
	// [0, MaxDelay) nanoseconds and must be positive.
	MaxDelay time.Duration

	// Start is the explicit origin at which the wrapper takes effect.
	Start time.Time
}

// NewStaggerSchedule wraps schedule with a deterministic stagger window of up
// to maxDelay nanoseconds, keyed by key, taking effect at start. See the
// StaggerSchedule documentation and the README for the fixed delay algorithm.
//
// It returns an error if schedule is nil, if key contains a NUL byte, if
// maxDelay is not positive, or if start is the zero time.
func NewStaggerSchedule(schedule Schedule, key string, maxDelay time.Duration, start time.Time) (*StaggerSchedule, error) {
	if schedule == nil {
		return nil, errors.New("cron: cannot stagger a nil Schedule")
	}
	if strings.IndexByte(key, 0) >= 0 {
		return nil, errors.New("cron: stagger key must not contain a NUL byte")
	}
	if maxDelay <= 0 {
		return nil, errors.New("cron: stagger MaxDelay must be greater than zero")
	}
	if start.IsZero() {
		return nil, errors.New("cron: stagger Start must not be the zero time")
	}
	return &StaggerSchedule{
		Schedule: schedule,
		Key:      key,
		MaxDelay: maxDelay,
		Start:    start,
	}, nil
}

// Next returns the earliest delayed activation strictly later than t.
//
// Next panics with ErrStaggerBeforeStart if t precedes the explicit start
// time; use NextAfter to handle that error explicitly.
func (s *StaggerSchedule) Next(t time.Time) time.Time {
	next, err := s.NextAfter(t)
	if err != nil {
		panic(err)
	}
	return next
}

// NextAfter is Next without the panic: it returns ErrStaggerBeforeStart when t
// is earlier than the wrapper's start time. A zero time.Time with a nil error
// means the wrapped schedule has no further activation.
func (s *StaggerSchedule) NextAfter(t time.Time) (time.Time, error) {
	if t.Before(s.Start) {
		return time.Time{}, ErrStaggerBeforeStart
	}

	// Only an original activation within MaxDelay before t can still have its
	// delayed instant in the future, and activations earlier than Start never
	// take effect. Scan that bounded window for a pending delayed activation
	// (the "original time already past, delayed time not yet reached" case).
	windowStart := t.Add(-s.MaxDelay)
	if s.Start.After(windowStart) {
		windowStart = s.Start
	}
	orig := s.Schedule.Next(windowStart.Add(-time.Nanosecond))
	var pending time.Time
	for !orig.IsZero() && !orig.After(t) {
		nextOrig := s.Schedule.Next(orig)
		delayed := s.delayed(orig, nextOrig)
		if delayed.After(t) {
			pending = delayed
		}
		orig = nextOrig
	}
	if !pending.IsZero() {
		return pending, nil
	}

	// orig is now the first original activation strictly later than t, or the
	// zero time if the wrapped schedule is exhausted.
	if orig.IsZero() {
		return time.Time{}, nil
	}
	return s.delayed(orig, s.Schedule.Next(orig)), nil
}

// delayed returns the actual activation for original instant orig, given the
// following original instant nextOrig (zero if there is none).
func (s *StaggerSchedule) delayed(orig, nextOrig time.Time) time.Time {
	delayed := orig.Add(s.staggerOffset(orig))
	if !nextOrig.IsZero() && !delayed.Before(nextOrig) {
		return nextOrig.Add(-time.Nanosecond)
	}
	return delayed
}

// staggerOffset computes the deterministic delay for one original activation
// instant. The encoding and algorithm are part of the package's fixed
// contract and must not change:
//
//  1. Represent the original instant as its UTC Unix time in nanoseconds,
//     formatted as a decimal ASCII integer.
//  2. Form the message as UTF-8 bytes of Key, a single NUL separator byte
//     (0x00), and the decimal instant bytes.
//  3. Hash the message with 64-bit FNV-1a (hash/fnv).
//  4. Reduce modulo MaxDelay in nanoseconds, giving a delay in
//     [0, MaxDelay) nanoseconds.
//
// Because the input is the UTC instant (not a wall-clock rendering) the
// result is unaffected by time zones or DST, and identical inputs produce
// identical delays on every platform and process.
func (s *StaggerSchedule) staggerOffset(orig time.Time) time.Duration {
	h := fnv.New64a()
	h.Write([]byte(s.Key))
	h.Write([]byte{0})
	h.Write([]byte(strconv.FormatInt(orig.UTC().UnixNano(), 10)))
	return time.Duration(h.Sum64() % uint64(s.MaxDelay))
}

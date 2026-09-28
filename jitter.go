package cron

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"time"
)

// ErrBeforeStart is returned by JitterSchedule.NextAfter (and raised as a
// panic by JitterSchedule.Next) when queried for a time strictly before the
// schedule's explicit Start time.
var ErrBeforeStart = errors.New("cron: Next called with a time before the jitter schedule start")

// JitterSchedule wraps another Schedule and deterministically delays each of
// its activation times by a small, repeatable offset. This spreads jobs whose
// underlying schedules fire at the same instant (e.g. many "@hourly" jobs on
// the hour) across a fixed window, while guaranteeing that a delayed firing
// never crosses the next original activation time.
//
// The delay is a pure function of Key and the original activation time: the
// same pair always produces the same offset. See the "Deterministic jitter"
// section of the package documentation for the fixed algorithm and encoding.
//
// Use NewJitterSchedule to construct one; the zero value is not valid.
type JitterSchedule struct {
	// Schedule is the wrapped schedule whose activation times are delayed.
	Schedule Schedule

	// Key identifies the logical job. The same key always produces the same
	// offset for a given activation time; distinct keys spread related jobs.
	Key string

	// MaxDelay bounds the delay applied to every activation time. It must be
	// non-negative and smaller than the maximum representable duration.
	MaxDelay time.Duration

	// Start is the explicit time from which queries are answered. It must be
	// set to the time at which the wrapper is created (typically time.Now).
	// Calling Next for an earlier time reports ErrBeforeStart, because fires
	// prior to Start were never observed and cannot be reconstructed.
	Start time.Time
}

// NewJitterSchedule returns a JitterSchedule that delays every activation of
// schedule by a deterministic offset in [0, maxDelay] derived from key and the
// original activation time.
//
// start is the explicit reference time from which Next/NextAfter may be
// queried; queries earlier than start report ErrBeforeStart. Pass time.Now()
// when attaching the schedule to a live cron.
func NewJitterSchedule(schedule Schedule, key string, maxDelay time.Duration, start time.Time) (*JitterSchedule, error) {
	if schedule == nil {
		return nil, errors.New("cron: jitter requires a non-nil schedule")
	}
	if maxDelay < 0 {
		return nil, errors.New("cron: jitter maxDelay must not be negative")
	}
	if int64(maxDelay) >= math.MaxInt64 {
		return nil, errors.New("cron: jitter maxDelay must be less than the maximum duration")
	}
	if start.IsZero() {
		return nil, errors.New("cron: jitter requires an explicit, non-zero start time")
	}
	return &JitterSchedule{
		Schedule: schedule,
		Key:      key,
		MaxDelay: maxDelay,
		Start:    start,
	}, nil
}

// Next returns the earliest delayed activation time strictly after t.
//
// It panics with ErrBeforeStart if t is before the schedule's Start time. To
// inspect the error, use NextAfter.
func (s *JitterSchedule) Next(t time.Time) time.Time {
	next, err := s.NextAfter(t)
	if err != nil {
		panic(err)
	}
	return next
}

// NextAfter returns the earliest delayed activation time strictly after t and
// ErrBeforeStart when t is before the schedule's Start time. As with the
// wrapped Schedule, the zero time is returned when no future activation exists.
func (s *JitterSchedule) NextAfter(t time.Time) (time.Time, error) {
	if s.Schedule == nil {
		return time.Time{}, errors.New("cron: JitterSchedule must be created with NewJitterSchedule")
	}
	if t.Before(s.Start) {
		return time.Time{}, ErrBeforeStart
	}

	// A fire only remains in scope if its delayed instant can exceed t. Since
	// the delay is at most MaxDelay, scanning from t-MaxDelay-1 is sufficient.
	// Scanning starts no earlier than Start-1 so the origin of the scan is well
	// defined.
	scanFrom := t.Add(-s.MaxDelay - 1)
	if startBefore := s.Start.Add(-1); startBefore.After(scanFrom) {
		scanFrom = startBefore
	}

	orig := s.Schedule.Next(scanFrom)
	if orig.IsZero() {
		return time.Time{}, nil
	}

	for {
		origAfter := orig
		orig = s.Schedule.Next(origAfter)

		delayed := origAfter.Add(s.offset(origAfter))
		// A delayed firing must never reach the next original activation time.
		if !orig.IsZero() && !delayed.Before(orig) {
			delayed = orig.Add(-time.Nanosecond)
		}

		if delayed.After(t) {
			return delayed, nil
		}

		if orig.IsZero() {
			return time.Time{}, nil
		}
	}
}

// offset computes the deterministic delay in [0, MaxDelay] for an original
// activation time. The algorithm and encoding are part of the package's public
// contract and must not change.
func (s *JitterSchedule) offset(orig time.Time) time.Duration {
	// 1. Express the original activation time in UTC.
	utc := orig.UTC()

	// 2. Encode it canonically as RFC 3339 with nanosecond precision.
	m := utc.Format(time.RFC3339Nano)

	// 3. Keyed-hash the encoded time with HMAC-SHA-256.
	h := hmac.New(sha256.New, []byte(s.Key))
	h.Write([]byte(m))
	digest := h.Sum(nil)

	// 4. Read the first 8 bytes as a big-endian unsigned integer.
	v := binary.BigEndian.Uint64(digest[:8])

	// 5. Map it uniformly into [0, MaxDelay] with a modular reduction.
	if s.MaxDelay == 0 {
		return 0
	}
	return time.Duration(v % (uint64(s.MaxDelay) + 1))
}

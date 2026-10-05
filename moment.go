package cron

import (
	"errors"
	"fmt"
	"time"
)

// Moment is a single schedule activation within a Report.
type Moment struct {
	// UTC is the activation instant in Coordinated Universal Time.
	UTC time.Time
	// Wall is the local wall-clock reading in the schedule's location.
	Wall time.Time
	// Offset is the zone offset in seconds east of UTC.
	Offset int
	// Duplicate is true when this wall-clock reading repeats (fall-back):
	// the same local time name occurs twice with different UTC identities.
	Duplicate bool
	// Jump is true when the wall-clock skips forward (spring-forward):
	// the local time jumps over a non-existent gap.
	Jump bool
}

// Report is a read-only listing of a Schedule's activations over the
// half-open UTC range [Start, End). It calls Schedule.Next directly and
// never starts or stops any Job.
type Report struct {
	// Schedule is the schedule this report was generated from.
	Schedule Schedule
	// Location is the location used for wall-clock readings.
	Location *time.Location
	// Start is the inclusive lower bound of the UTC range.
	Start time.Time
	// End is the exclusive upper bound of the UTC range.
	End time.Time
	// Moments are the activations in (Start, End), in UTC order.
	Moments []Moment
}

// MomentReport returns a read-only report of s's activations over the
// half-open UTC range [start, end). Each activation is reported with its
// UTC instant, local wall-clock reading (in loc), and zone offset.
//
// The report calls Schedule.Next directly and never starts a Job. It marks
// Duplicate wall-clock readings (fall-back, e.g. 01:30 occurring twice with
// different UTC identities) and Jump transitions (spring-forward, where the
// wall-clock skips over a gap). It returns an error if Next fails to advance
// strictly (a "bad Schedule" returning the same or an earlier moment) or if
// the range is invalid.
//
// If loc is nil, the schedule's own Location is used when available,
// otherwise time.Local.
func MomentReport(s Schedule, start, end time.Time, loc *time.Location) (*Report, error) {
	if s == nil {
		return nil, errors.New("cron: report requires a non-nil schedule")
	}
	start = start.UTC()
	end = end.UTC()
	if !start.Before(end) {
		return nil, errors.New("cron: report range requires start < end")
	}
	if loc == nil {
		if ss, ok := s.(*SpecSchedule); ok && ss.Location != nil {
			loc = ss.Location
		} else {
			loc = time.Local
		}
	}

	r := &Report{
		Schedule: s,
		Location: loc,
		Start:    start,
		End:      end,
	}

	seen := make(map[string]bool) // wall-clock date+time -> seen
	var prev time.Time            // previous UTC moment
	var prevOff int
	havePrev := false

	for t := s.Next(start); !t.IsZero() && t.Before(end); t = s.Next(t) {
		// Strict advancement check: Next must return a time strictly after
		// the previous one (and after start for the first call).
		if !t.After(start) {
			return nil, fmt.Errorf("cron: bad schedule: Next(%s) = %s is not after start", start, t)
		}
		if havePrev && !t.After(prev) {
			return nil, fmt.Errorf("cron: bad schedule: Next(%s) = %s did not advance", prev, t)
		}

		wall := t.In(loc)
		_, off := wall.Zone()

		m := Moment{
			UTC:    t.UTC(),
			Wall:   wall,
			Offset: off,
		}

		// Duplicate wall-clock detection (fall-back): the same local
		// date+time name occurs more than once with different UTC identities.
		key := wall.Format("2006-01-02 15:04:05")
		if seen[key] {
			m.Duplicate = true
		}
		seen[key] = true

		// Jump detection (spring-forward): the zone offset increased between
		// consecutive moments, meaning the wall-clock skipped forward.
		if havePrev && off > prevOff {
			m.Jump = true
		}

		r.Moments = append(r.Moments, m)
		prev = t
		prevOff = off
		havePrev = true
	}

	return r, nil
}

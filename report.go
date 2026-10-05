package cron

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// ErrBadSchedule is returned by InspectSchedule when a Schedule violates its
// contract: Next must return an instant strictly later than its argument.
// The report walk stops immediately when this is observed.
var ErrBadSchedule = errors.New("cron: bad schedule: Next did not return a strictly later time")

// maxReportActivations caps how many Next results InspectSchedule collects, so
// a pathological Schedule cannot make the read-only walk run without end.
const maxReportActivations = 1000000

// Activation is a single scheduled instant recorded in a Report.
//
// All fields are derived from values returned by Schedule.Next; an Activation
// never causes a Job to run.
type Activation struct {
	// Time is the activation instant exactly as returned by Schedule.Next.
	Time time.Time

	// UTC is the same instant expressed in UTC.
	UTC time.Time

	// Local is the wall-clock time displayed in Time's own location.
	Local time.Time

	// Zone is the abbreviated zone name in effect at Local (e.g. "EST").
	Zone string

	// Offset is the east-of-UTC offset in seconds at Local.
	Offset int

	// RepeatedWall marks a local wall-clock time that an earlier activation
	// already produced. This happens during the autumn DST "fall back": the
	// repeated hour is walked twice, so the same wall-clock label exists at
	// two distinct UTC instants and both activations are marked.
	RepeatedWall bool

	// Gap marks the first activation reached after the local wall clock
	// skipped a range of times, i.e. the "spring forward" DST transition.
	// The wall clock jumped forward between this activation and the previous
	// one, so some wall-clock labels have no UTC instant at all.
	Gap bool
}

// Report is the read-only result of InspectSchedule.
//
// Start and End are the bounded UTC window; the window is half-open:
// activations are reported for Start <= Next() < End. Activations are in the
// strict ascending UTC order in which Schedule.Next produced them.
type Report struct {
	Start       time.Time
	End         time.Time
	Activations []Activation
}

// InspectSchedule produces a read-only report of s's Next results inside the
// half-open UTC window [start, end).
//
// It walks the Schedule by repeatedly calling the original Next method (it
// never re-implements or second-guesses the schedule's calendar) and records,
// for every result, the UTC instant, the local wall-clock time and the zone
// offset. Repeated local wall-clock times (the autumn DST fallback hour) keep
// their distinct UTC identities and are flagged RepeatedWall; a forward jump
// of the wall clock (the spring DST gap) is flagged Gap on the first
// activation after the jump.
//
// The walk only advances. If Next ever returns the zero time the window ends;
// if Next returns an instant that is not strictly later than its argument the
// Schedule is broken and ErrBadSchedule is returned and the walk stops. The
// end instant itself is not reported.
//
// InspectSchedule never starts, schedules or runs any Job; it only calls
// Schedule.Next and is safe to use on a Cron that has not been started.
func InspectSchedule(s Schedule, start, end time.Time) (*Report, error) {
	if s == nil {
		return nil, errors.New("cron: cannot inspect a nil Schedule")
	}
	if !end.After(start) {
		return nil, fmt.Errorf("cron: report window must have end after start (start=%s, end=%s)",
			start.UTC().Format(time.RFC3339), end.UTC().Format(time.RFC3339))
	}

	report := &Report{Start: start.UTC(), End: end.UTC()}

	// Index in report.Activations of the first time a wall-clock label was
	// seen, so the earlier member of a repeated pair can be flagged too.
	firstSeen := make(map[wallKey]int)

	from := start
	var prev time.Time
	for {
		next := s.Next(from)

		// The schedule has no further activations.
		if next.IsZero() {
			return report, nil
		}

		// Schedule contract: every result must be strictly later than the
		// argument. Stop and report the broken schedule otherwise.
		if !next.After(from) {
			return nil, fmt.Errorf("%w: Next(%s) returned %s",
				ErrBadSchedule,
				from.UTC().Format(time.RFC3339),
				next.UTC().Format(time.RFC3339))
		}

		// Half-open window: the end instant itself is not listed, and Next
		// never needs to be called past it.
		if !next.UTC().Before(report.End) {
			return report, nil
		}

		if len(report.Activations) >= maxReportActivations {
			return nil, fmt.Errorf("cron: more than %d activations between %s and %s; refusing to report more",
				maxReportActivations, report.Start.Format(time.RFC3339), report.End.Format(time.RFC3339))
		}

		local := next.In(next.Location())
		zone, offset := next.Zone()
		activation := Activation{
			Time:   next,
			UTC:    next.UTC(),
			Local:  local,
			Zone:   zone,
			Offset: offset,
		}

		// A larger offset than the previous activation means the local wall
		// clock has just jumped forward (e.g. EST -> EDT): wall-clock labels
		// between the two activations simply do not exist.
		if !prev.IsZero() {
			if _, prevOffset := prev.Zone(); offset > prevOffset {
				activation.Gap = true
			}
		}

		// Flag both occurrences when the same local wall-clock label comes
		// around again at a different UTC instant (the fall-back repeat).
		key := wallKeyOf(local)
		if idx, ok := firstSeen[key]; ok {
			activation.RepeatedWall = true
			report.Activations[idx].RepeatedWall = true
		} else {
			firstSeen[key] = len(report.Activations)
		}

		report.Activations = append(report.Activations, activation)
		prev = next
		from = next
	}
}

// wallKey identifies a local wall-clock label, ignoring which zone/offset
// presented it.
type wallKey struct {
	year                int
	month               time.Month
	day, hour, min, sec int
}

func wallKeyOf(t time.Time) wallKey {
	return wallKey{
		year:  t.Year(),
		month: t.Month(),
		day:   t.Day(),
		hour:  t.Hour(),
		min:   t.Minute(),
		sec:   t.Second(),
	}
}

// String renders the report as a plain-text table.
func (r *Report) String() string {
	var b strings.Builder
	_, _ = r.WriteTo(&b)
	return b.String()
}

// WriteTo writes the plain-text rendering of the report to w.
func (r *Report) WriteTo(w io.Writer) (int64, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "schedule report: %s <= Next() < %s (%d activation(s))\n",
		r.Start.Format(time.RFC3339), r.End.Format(time.RFC3339), len(r.Activations))
	fmt.Fprintf(&b, "%-20s  %-21s %-5s %-7s %s\n", "UTC", "WALL CLOCK", "ZONE", "OFFSET", "NOTES")
	for i := range r.Activations {
		a := &r.Activations[i]
		fmt.Fprintf(&b, "%-20s  %-21s %-5s %-7s %s\n",
			a.UTC.Format("2006-01-02T15:04:05Z07:00"),
			a.Local.Format("2006-01-02 15:04:05"),
			a.Zone,
			formatOffset(a.Offset),
			activationNotes(a))
	}
	n, err := io.WriteString(w, b.String())
	return int64(n), err
}

func activationNotes(a *Activation) string {
	var notes []string
	if a.RepeatedWall {
		notes = append(notes, "repeated local wall-clock time (DST fall back)")
	}
	if a.Gap {
		notes = append(notes, "wall clock skipped forward (DST gap)")
	}
	return strings.Join(notes, "; ")
}

// formatOffset renders an east-of-UTC offset in seconds as "+hh:mm" or
// "-hh:mm", using "Z" only for UTC itself.
func formatOffset(seconds int) string {
	if seconds == 0 {
		return "Z"
	}
	sign := "+"
	if seconds < 0 {
		sign = "-"
		seconds = -seconds
	}
	h := seconds / 3600
	m := (seconds % 3600) / 60
	return fmt.Sprintf("%s%02d:%02d", sign, h, m)
}

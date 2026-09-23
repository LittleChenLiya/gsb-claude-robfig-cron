package cron

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// DisabledDate is a whole calendar day (year, month, day) interpreted in the
// exception calendar's own time zone.
type DisabledDate struct {
	Year  int
	Month time.Month
	Day   int
}

// TimeInterval is a half-open UTC interval [Start, End). Times may be supplied
// in any location; they are converted to UTC when the calendar is built.
type TimeInterval struct {
	Start time.Time
	End   time.Time
}

// SkipEvent records one candidate activation that was rejected by the
// exception calendar, together with the rule that rejected it.
type SkipEvent struct {
	Time   time.Time
	Reason string
}

// ExceptionCalendar is a reusable set of exception rules evaluated in a single
// time zone. A time instant is forbidden when it falls in any disabled UTC
// interval, or when its local date in the calendar's location is disabled, or
// when its local weekday in the calendar's location is disabled.
//
// An ExceptionCalendar is immutable after construction and safe for concurrent
// use. It decides only which instants are forbidden; the wrapped Schedule
// still decides which instants could ever activate.
type ExceptionCalendar struct {
	location *time.Location

	// dates holds disabled local dates keyed by their calendar components.
	dates map[DisabledDate]struct{}

	// weekdays holds disabled local weekdays.
	weekdays map[time.Weekday]struct{}

	// intervals holds disabled UTC intervals, sorted by Start and merged so
	// that overlapping or adjacent intervals form a single interval.
	intervals []TimeInterval
}

// NewCalendar builds an ExceptionCalendar using the IANA time zone with the
// given name. An empty name means UTC. An unknown time zone, a reversed
// interval, an impossible date, or an out-of-range weekday is rejected.
//
// The input slices are copied: mutating them after NewCalendar returns has no
// effect on the calendar.
func NewCalendar(locationName string, dates []DisabledDate, weekdays []time.Weekday, intervals []TimeInterval) (*ExceptionCalendar, error) {
	locationName = strings.TrimSpace(locationName)
	if locationName == "" {
		locationName = "UTC"
	}
	location, err := time.LoadLocation(locationName)
	if err != nil {
		return nil, fmt.Errorf("cron: invalid exception calendar time zone %q: %w", locationName, err)
	}
	return NewCalendarWithLocation(location, dates, weekdays, intervals)
}

// NewCalendarWithLocation builds an ExceptionCalendar using the given
// time.Location. It behaves like NewCalendar otherwise.
func NewCalendarWithLocation(location *time.Location, dates []DisabledDate, weekdays []time.Weekday, intervals []TimeInterval) (*ExceptionCalendar, error) {
	if location == nil {
		return nil, fmt.Errorf("cron: exception calendar time zone must not be nil")
	}

	calendar := &ExceptionCalendar{
		location: location,
		dates:    make(map[DisabledDate]struct{}, len(dates)),
		weekdays: make(map[time.Weekday]struct{}, len(weekdays)),
	}

	for _, date := range dates {
		// Round-trip through time.Date at noon (noon avoids any DST midnight
		// normalization) to reject impossible dates such as February 30.
		canonical := time.Date(date.Year, date.Month, date.Day, 12, 0, 0, 0, location)
		if canonical.Year() != date.Year || canonical.Month() != date.Month || canonical.Day() != date.Day {
			return nil, fmt.Errorf("cron: invalid disabled date %04d-%02d-%02d in %s", date.Year, date.Month, date.Day, location)
		}
		calendar.dates[date] = struct{}{}
	}

	for _, weekday := range weekdays {
		if weekday < time.Sunday || weekday > time.Saturday {
			return nil, fmt.Errorf("cron: invalid disabled weekday %d in %s", weekday, location)
		}
		calendar.weekdays[weekday] = struct{}{}
	}

	merged := make([]TimeInterval, 0, len(intervals))
	for _, interval := range intervals {
		interval.Start = interval.Start.UTC()
		interval.End = interval.End.UTC()
		if interval.Start.After(interval.End) {
			return nil, fmt.Errorf("cron: reversed UTC disabled interval [%s, %s)", interval.Start.Format(time.RFC3339), interval.End.Format(time.RFC3339))
		}
		merged = append(merged, interval)
	}
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Start.Before(merged[j].Start)
	})

	// Merge overlapping intervals. Adjacent intervals (the next Start equals the
	// previous End) are also merged: their half-open union is one contiguous
	// interval and the touching instant is already included by the second one.
	for i := 1; i < len(merged); i++ {
		last := &merged[i-1]
		if !merged[i].Start.After(last.End) {
			if merged[i].End.After(last.End) {
				last.End = merged[i].End
			}
			merged = append(merged[:i], merged[i+1:]...)
			i--
		}
	}
	calendar.intervals = merged

	return calendar, nil
}

// Location returns the time zone in which disabled dates and weekdays are
// interpreted.
func (c *ExceptionCalendar) Location() *time.Location {
	return c.location
}

// Wrap returns a Schedule that yields the next activation of schedule which is
// not forbidden by the calendar. The returned *CalendarSchedule adds NextUntil
// for searches with an explicit cutoff.
func (c *ExceptionCalendar) Wrap(schedule Schedule) *CalendarSchedule {
	return &CalendarSchedule{
		schedule: schedule,
		calendar: c,
	}
}

// CalendarSchedule wraps a Schedule with an ExceptionCalendar. It implements
// Schedule itself, so it can be used anywhere the underlying Schedule could.
type CalendarSchedule struct {
	schedule Schedule
	calendar *ExceptionCalendar
}

// defaultSearchYears bounds searches without an explicit cutoff so that a
// permanently blocking calendar cannot loop forever. It mirrors the five-year
// horizon used by SpecSchedule.Next.
const defaultSearchYears = 5

// Next returns the next activation strictly after t that is allowed by both
// the wrapped schedule and the exception calendar. It returns the zero time if
// no such time exists within five years. The zero time is also returned when
// the wrapped schedule has no later activation.
func (s *CalendarSchedule) Next(t time.Time) time.Time {
	return s.next(t, t.AddDate(defaultSearchYears, 0, 0), nil)
}

// NextUntil is like Next, but the search stops at deadline: an activation
// exactly at deadline is allowed, while any later activation yields the zero
// time.
func (s *CalendarSchedule) NextUntil(t, deadline time.Time) time.Time {
	return s.next(t, deadline, nil)
}

// NextWithLog is like NextUntil but also reports every rejected candidate and
// the reason it was rejected.
func (s *CalendarSchedule) NextWithLog(t, deadline time.Time) (time.Time, []SkipEvent) {
	var skipped []SkipEvent
	result := s.next(t, deadline, &skipped)
	return result, skipped
}

// next drives the wrapped Schedule forward, applying the calendar rules.
func (s *CalendarSchedule) next(t, deadline time.Time, skipped *[]SkipEvent) time.Time {
	from := t
	for {
		candidate := s.schedule.Next(from)
		if candidate.IsZero() {
			return time.Time{}
		}
		// Defend against a Schedule that does not return a strictly later
		// activation, which would otherwise loop forever.
		if !candidate.After(from) {
			return time.Time{}
		}
		if candidate.After(deadline) {
			return time.Time{}
		}

		// Disabled UTC intervals take priority. Consecutive intervals are
		// already merged, so a hit jumps straight to the interval end; the
		// wrapped schedule is asked for instants strictly before the end so an
		// activation exactly at the end is still considered.
		if interval, ok := s.calendar.containingInterval(candidate); ok {
			s.record(candidate, intervalReason(interval), skipped)
			from = interval.End.Add(-1)
			continue
		}

		// Disabled dates and weekdays are evaluated in the calendar's own
		// location. Jumps use calendar arithmetic (AddDate), never a fixed
		// 24-hour offset, so DST transition days are handled correctly.
		if reason, blocked := s.calendar.dayReason(candidate); blocked {
			s.record(candidate, reason, skipped)
			local := candidate.In(s.calendar.location)
			nextDay := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, s.calendar.location).AddDate(0, 0, 1)
			from = nextDay.Add(-1)
			continue
		}

		return candidate
	}
}

func (s *CalendarSchedule) record(t time.Time, reason string, skipped *[]SkipEvent) {
	if skipped != nil {
		*skipped = append(*skipped, SkipEvent{Time: t, Reason: reason})
	}
}

// containingInterval returns the merged half-open interval containing t, if
// any. Intervals are stored in UTC regardless of t's location.
func (c *ExceptionCalendar) containingInterval(t time.Time) (TimeInterval, bool) {
	u := t.UTC()
	// First interval whose Start is strictly after u; the interval just before
	// it is the only one that can contain u.
	i := sort.Search(len(c.intervals), func(i int) bool {
		return c.intervals[i].Start.After(u)
	}) - 1
	if i >= 0 && u.Before(c.intervals[i].End) {
		return c.intervals[i], true
	}
	return TimeInterval{}, false
}

// dayReason reports why the local day of t in the calendar's location is
// disabled. Both a disabled date and a disabled weekday may apply.
func (c *ExceptionCalendar) dayReason(t time.Time) (string, bool) {
	local := t.In(c.location)
	date := DisabledDate{local.Year(), local.Month(), local.Day()}
	_, dateBlocked := c.dates[date]
	_, weekdayBlocked := c.weekdays[local.Weekday()]
	if !dateBlocked && !weekdayBlocked {
		return "", false
	}

	var parts []string
	if dateBlocked {
		parts = append(parts, fmt.Sprintf("disabled date %04d-%02d-%02d in %s", date.Year, date.Month, date.Day, c.location))
	}
	if weekdayBlocked {
		parts = append(parts, fmt.Sprintf("disabled weekday %s in %s", local.Weekday(), c.location))
	}
	return strings.Join(parts, "; "), true
}

func intervalReason(interval TimeInterval) string {
	return fmt.Sprintf("UTC disabled interval [%s, %s)", interval.Start.Format(time.RFC3339), interval.End.Format(time.RFC3339))
}

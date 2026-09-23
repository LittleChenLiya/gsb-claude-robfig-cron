package cron

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Interval is a half-open UTC time interval [Start, End). A trigger at exactly
// End is not contained in the interval and remains allowed.
type Interval struct {
	Start time.Time
	End   time.Time
}

// SkipReason is a bit mask describing which calendar rule rejected a candidate
// activation time. A candidate may match more than one rule at once.
type SkipReason uint8

const (
	// SkipDate means the candidate's calendar date is disabled in the
	// calendar's location.
	SkipDate SkipReason = 1 << iota
	// SkipWeekday means the candidate's weekday is disabled in the calendar's
	// location.
	SkipWeekday
	// SkipInterval means the candidate falls inside a disabled UTC interval.
	SkipInterval
)

// String describes the matched rules.
func (r SkipReason) String() string {
	var parts []string
	if r&SkipDate != 0 {
		parts = append(parts, "disabled date")
	}
	if r&SkipWeekday != 0 {
		parts = append(parts, "disabled weekday")
	}
	if r&SkipInterval != 0 {
		parts = append(parts, "disabled UTC interval")
	}
	return strings.Join(parts, ", ")
}

// SkipEvent records a candidate activation time that the calendar rejected.
type SkipEvent struct {
	// At is the rejected candidate time, in the same location as returned by
	// the underlying schedule.
	At time.Time
	// Reasons is the bit mask of rules that matched At.
	Reasons SkipReason
	// Interval is the disabled UTC interval containing At. It is the zero
	// Interval when Reasons does not include SkipInterval.
	Interval Interval
}

// Calendar is an immutable, reusable set of exception rules. Rule evaluation is
// safe for concurrent use after NewCalendar returns.
type Calendar struct {
	loc       *time.Location
	dates     map[dateKey]struct{}
	weekdays  [7]bool
	intervals []Interval // sorted by Start, merged (overlapping or adjacent), UTC
}

type dateKey [3]int // year, month, day in the calendar's location

// CalendarOption configures a Calendar at construction time.
type CalendarOption func(*calendarConfig) error

type calendarConfig struct {
	loc       *time.Location
	dates     []time.Time
	weekdays  []time.Weekday
	intervals []Interval
}

// WithCalendarLocation sets the calendar's location. Disabled dates and weekdays are
// always evaluated in this location, independently of the wrapped schedule's
// own location (for example CRON_TZ).
func WithCalendarLocation(loc *time.Location) CalendarOption {
	return func(c *calendarConfig) error {
		if loc == nil {
			return errors.New("cron: calendar location must not be nil")
		}
		c.loc = loc
		return nil
	}
}

// WithTimezone sets the calendar's location from an IANA timezone name, for
// example "America/New_York". An unknown name is rejected at construction.
func WithTimezone(name string) CalendarOption {
	return func(c *calendarConfig) error {
		loc, err := time.LoadLocation(name)
		if err != nil {
			return fmt.Errorf("cron: invalid calendar timezone %q: %w", name, err)
		}
		c.loc = loc
		return nil
	}
}

// WithDisabledDates disables whole calendar dates. Only the date of each time
// in the calendar's location matters; its clock time is ignored.
func WithDisabledDates(dates ...time.Time) CalendarOption {
	return func(c *calendarConfig) error {
		c.dates = append(c.dates, dates...)
		return nil
	}
}

// WithDisabledWeekdays disables the given weekdays (time.Sunday..time.Saturday)
// in the calendar's location.
func WithDisabledWeekdays(days ...time.Weekday) CalendarOption {
	return func(c *calendarConfig) error {
		for _, day := range days {
			if day < time.Sunday || day > time.Saturday {
				return fmt.Errorf("cron: invalid disabled weekday %d", day)
			}
		}
		c.weekdays = append(c.weekdays, days...)
		return nil
	}
}

// WithDisabledIntervals adds half-open UTC intervals [Start, End) during which
// no activation is allowed. Times may be supplied in any location; they are
// converted to UTC. A zero-length or reversed interval is rejected.
func WithDisabledIntervals(intervals ...Interval) CalendarOption {
	return func(c *calendarConfig) error {
		c.intervals = append(c.intervals, intervals...)
		return nil
	}
}

// NewCalendar builds an immutable exception calendar. Inputs are copied, so
// later mutation of slices or times passed to the options has no effect. The
// default location (when neither WithCalendarLocation nor WithTimezone is given) is UTC.
func NewCalendar(options ...CalendarOption) (*Calendar, error) {
	cfg := calendarConfig{loc: time.UTC}
	for _, option := range options {
		if err := option(&cfg); err != nil {
			return nil, err
		}
	}

	calendar := &Calendar{
		loc:   cfg.loc,
		dates: make(map[dateKey]struct{}, len(cfg.dates)),
	}

	for _, date := range cfg.dates {
		local := date.In(cfg.loc)
		key := dateKey{local.Year(), int(local.Month()), local.Day()}
		calendar.dates[key] = struct{}{}
	}

	for _, day := range cfg.weekdays {
		calendar.weekdays[int(day)] = true
	}

	intervals := make([]Interval, len(cfg.intervals))
	for i, interval := range cfg.intervals {
		start := interval.Start.UTC()
		end := interval.End.UTC()
		if !start.Before(end) {
			return nil, fmt.Errorf("cron: disabled interval must be half-open with Start before End: %s not before %s", start, end)
		}
		intervals[i] = Interval{start, end}
	}
	sort.Slice(intervals, func(i, j int) bool {
		return intervals[i].Start.Before(intervals[j].Start)
	})
	calendar.intervals = mergeIntervals(intervals)

	return calendar, nil
}

// mergeIntervals merges intervals that overlap or are adjacent, so consecutive
// disabled spans are searched as a single interval.
func mergeIntervals(sorted []Interval) []Interval {
	merged := make([]Interval, 0, len(sorted))
	for _, interval := range sorted {
		last := len(merged) - 1
		if last >= 0 && !interval.Start.After(merged[last].End) {
			if interval.End.After(merged[last].End) {
				merged[last].End = interval.End
			}
			continue
		}
		merged = append(merged, interval)
	}
	return merged
}

// Location returns the location in which disabled dates and weekdays are
// evaluated.
func (c *Calendar) Location() *time.Location {
	return c.loc
}

// IsAllowed reports whether t is not rejected by any rule.
func (c *Calendar) IsAllowed(t time.Time) bool {
	reasons, _ := c.reasons(t)
	return reasons == 0
}

// reasons evaluates every rule against the single UTC instant represented by t.
// Repeated local clock times (for example the 01:00 hour repeated during a
// fall-back DST transition) are separate UTC instants and are judged
// separately.
func (c *Calendar) reasons(t time.Time) (SkipReason, Interval) {
	var reasons SkipReason
	local := t.In(c.loc)
	if _, ok := c.dates[dateKey{local.Year(), int(local.Month()), local.Day()}]; ok {
		reasons |= SkipDate
	}
	if c.weekdays[int(local.Weekday())] {
		reasons |= SkipWeekday
	}
	if interval, ok := c.intervalContaining(t.UTC()); ok {
		reasons |= SkipInterval
		return reasons, interval
	}
	return reasons, Interval{}
}

// intervalContaining returns the merged half-open interval containing t, if
// any. A time equal to an interval End is not contained.
func (c *Calendar) intervalContaining(t time.Time) (Interval, bool) {
	idx := sort.Search(len(c.intervals), func(i int) bool {
		return c.intervals[i].Start.After(t)
	}) - 1
	if idx < 0 {
		return Interval{}, false
	}
	interval := c.intervals[idx]
	if t.Before(interval.End) {
		return interval, true
	}
	return Interval{}, false
}

// ScheduleOption configures a CalendarSchedule at construction time.
type ScheduleOption func(*CalendarSchedule)

// WithDeadline sets an absolute cutoff for Next. Candidate activation times at
// or before the deadline are still considered; later searches use NextUntil
// with an explicit deadline instead.
func WithDeadline(deadline time.Time) ScheduleOption {
	return func(s *CalendarSchedule) {
		s.deadline = deadline
	}
}

// CalendarSchedule wraps a Schedule and skips every activation time rejected by
// the exception calendar. The wrapped schedule alone decides which moments
// would otherwise be runnable.
type CalendarSchedule struct {
	inner    Schedule
	calendar *Calendar
	// deadline is the optional fixed cutoff for calls to Next. Its zero value
	// means a rolling five-year window is used, mirroring SpecSchedule.
	deadline time.Time
}

// NewCalendarSchedule wraps inner with the given exception calendar.
func NewCalendarSchedule(inner Schedule, calendar *Calendar, options ...ScheduleOption) (*CalendarSchedule, error) {
	if inner == nil {
		return nil, errors.New("cron: wrapped schedule must not be nil")
	}
	if calendar == nil {
		return nil, errors.New("cron: exception calendar must not be nil")
	}
	s := &CalendarSchedule{inner: inner, calendar: calendar}
	for _, option := range options {
		option(s)
	}
	return s, nil
}

// Next returns the next activation strictly after t that the underlying
// schedule produces and the calendar allows. It returns the zero time when the
// underlying schedule has no next activation, or none exists before the
// deadline (the configured WithDeadline cutoff, otherwise five years after t).
func (s *CalendarSchedule) Next(t time.Time) time.Time {
	next, _ := s.search(t, s.resolveDeadline(t), nil)
	return next
}

// NextUntil behaves like Next but never returns a time after deadline. A time
// exactly equal to deadline is eligible. Pass the zero time as deadline to use
// the default five-year window.
func (s *CalendarSchedule) NextUntil(t, deadline time.Time) time.Time {
	next, _ := s.search(t, s.resolveDeadline(t, deadline), nil)
	return next
}

// NextWithReasons is NextUntil and additionally records every rejected
// candidate in search order, including which rules matched. Pass a nil events
// slice to allocate one. It terminates even when the calendar permanently
// disables every candidate.
func (s *CalendarSchedule) NextWithReasons(t, deadline time.Time, events []SkipEvent) (time.Time, []SkipEvent) {
	return s.search(t, s.resolveDeadline(t, deadline), events)
}

// resolveDeadline prefers an explicit per-call deadline, then a configured
// fixed deadline, and finally a five-year window computed with calendar
// arithmetic (so DST transitions do not make the boundary drift by an hour).
func (s *CalendarSchedule) resolveDeadline(t time.Time, deadlines ...time.Time) time.Time {
	for _, deadline := range deadlines {
		if !deadline.IsZero() {
			return deadline
		}
	}
	if !s.deadline.IsZero() {
		return s.deadline
	}
	return t.AddDate(5, 0, 0)
}

// search repeatedly asks the underlying schedule for candidates and jumps over
// rejected spans. Each jump lands exactly on the first potentially legal
// instant, so an activation exactly at the end of a disabled span is not
// missed.
func (s *CalendarSchedule) search(t, deadline time.Time, events []SkipEvent) (time.Time, []SkipEvent) {
	candidate := s.inner.Next(t)
	for {
		// An activation exactly at the deadline is eligible; only times after
		// it, or the underlying schedule giving up, end the search.
		if candidate.IsZero() || candidate.After(deadline) {
			return time.Time{}, events
		}

		reasons, blockedBy := s.calendar.reasons(candidate)
		if reasons == 0 {
			return candidate, events
		}
		events = append(events, SkipEvent{
			At:       candidate,
			Reasons:  reasons,
			Interval: blockedBy,
		})

		var jump time.Time
		if reasons&SkipInterval != 0 {
			// Jump to the interval's half-open end, which may itself be legal.
			jump = blockedBy.End
		} else {
			// Advance to the first instant of the next local calendar date using
			// calendar arithmetic rather than a fixed 24-hour offset. time.Date
			// normalizes dates where midnight does not exist because of a DST
			// spring-forward.
			local := candidate.In(s.calendar.loc)
			jump = time.Date(local.Year(), local.Month(), local.Day()+1, 0, 0, 0, 0, s.calendar.loc)
		}

		if jump.After(deadline) {
			// The whole remaining span up to the deadline is disabled.
			return time.Time{}, events
		}

		// Ask the underlying schedule from one nanosecond before the boundary so
		// that an activation landing exactly on jump is still considered.
		candidate = s.inner.Next(jump.Add(-time.Nanosecond))
	}
}

package cron_test

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// ExampleCalendarSchedule searches within a 90-second window starting at
// 10:00:00. The first query merges two consecutive UTC intervals into
// [10:00:01, 10:00:03) and jumps straight to the legal activation at its
// half-open end. The second query additionally disables the date and weekday,
// so the search stops at the deadline with a zero time. Every probed candidate
// is reported together with the rules that rejected it.
func ExampleCalendarSchedule() {
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	everySecond, err := parser.Parse("* * * * * *")
	if err != nil {
		panic(err)
	}

	from := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC) // Wednesday
	deadline := from.Add(90 * time.Second)

	// Consecutive half-open intervals [01,02) and [02,03) merge into [01,03).
	intervalCalendar, err := cron.NewCalendar(cron.WithDisabledIntervals(
		cron.Interval{
			Start: time.Date(2025, 1, 15, 10, 0, 1, 0, time.UTC),
			End:   time.Date(2025, 1, 15, 10, 0, 2, 0, time.UTC),
		},
		cron.Interval{
			Start: time.Date(2025, 1, 15, 10, 0, 2, 0, time.UTC),
			End:   time.Date(2025, 1, 15, 10, 0, 3, 0, time.UTC),
		},
	))
	if err != nil {
		panic(err)
	}
	intervalSchedule, err := cron.NewCalendarSchedule(everySecond, intervalCalendar)
	if err != nil {
		panic(err)
	}
	next, events := intervalSchedule.NextWithReasons(from, deadline, nil)
	for _, event := range events {
		fmt.Printf("skip %s: %s\n", event.At.Format("15:04:05"), event.Reasons)
	}
	fmt.Printf("final: %s\n", next.Format("15:04:05"))

	// The same window with the whole date and every Wednesday disabled: the
	// deadline lies inside a permanently disabled local date, so the result is
	// zero rather than a date beyond the window.
	permanentCalendar, err := cron.NewCalendar(
		cron.WithDisabledDates(time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)),
		cron.WithDisabledWeekdays(time.Wednesday),
	)
	if err != nil {
		panic(err)
	}
	permanentSchedule, err := cron.NewCalendarSchedule(everySecond, permanentCalendar)
	if err != nil {
		panic(err)
	}
	next, events = permanentSchedule.NextWithReasons(from, deadline, nil)
	for _, event := range events {
		fmt.Printf("skip %s: %s\n", event.At.Format("15:04:05"), event.Reasons)
	}
	if next.IsZero() {
		fmt.Println("final: none before deadline")
	}

	// Output:
	// skip 10:00:01: disabled UTC interval
	// final: 10:00:03
	// skip 10:00:01: disabled date, disabled weekday
	// final: none before deadline
}

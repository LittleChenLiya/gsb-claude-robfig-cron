package cron_test

import (
	"fmt"
	"os"
	"time"

	"github.com/robfig/cron/v3"
)

// ExampleInspectSchedule produces a read-only report of a Schedule inside a
// bounded UTC window. The injected schedule emits the two local 01:30
// instants of the 2021 autumn DST fall-back in New York (EDT then EST). No
// Job is started.
func ExampleInspectSchedule() {
	sched := &scriptedSchedule{times: []time.Time{
		time.Date(2021, 11, 7, 1, 30, 0, 0, edt), // first 01:30, still EDT
		time.Date(2021, 11, 7, 1, 30, 0, 0, est), // second 01:30, now EST
	}}
	start := time.Date(2021, 11, 7, 5, 0, 0, 0, time.UTC)
	end := time.Date(2021, 11, 7, 7, 0, 0, 0, time.UTC)

	report, err := cron.InspectSchedule(sched, start, end)
	if err != nil {
		fmt.Fprintln(os.Stdout, err)
		return
	}
	_, _ = report.WriteTo(os.Stdout)
	// Output:
	// schedule report: 2021-11-07T05:00:00Z <= Next() < 2021-11-07T07:00:00Z (2 activation(s))
	// UTC                   WALL CLOCK            ZONE  OFFSET  NOTES
	// 2021-11-07T05:30:00Z  2021-11-07 01:30:00   EDT   -04:00  repeated local wall-clock time (DST fall back)
	// 2021-11-07T06:30:00Z  2021-11-07 01:30:00   EST   -05:00  repeated local wall-clock time (DST fall back)
}

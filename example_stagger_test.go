package cron_test

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// ExampleStaggerSchedule prints the original per-minute plan next to the
// deterministic staggered plan for the same fixed query time. No real waiting
// is involved: every result is computed from fixed timestamps.
func ExampleStaggerSchedule() {
	original, err := cron.ParseStandard("* * * * *")
	if err != nil {
		panic(err)
	}

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	staggered, err := cron.NewStaggerSchedule(original, "tenant-42", 20*time.Second, start)
	if err != nil {
		panic(err)
	}

	const layout = "15:04:05.000000000"
	query := start.Add(30 * time.Second)

	fmt.Println("original -> staggered (UTC)")
	orig := original.Next(start.Add(-time.Nanosecond))
	for i := 0; i < 5; i++ {
		delayed, err := staggered.NextAfter(orig)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s -> %s\n", orig.UTC().Format(layout), delayed.UTC().Format(layout))
		orig = original.Next(orig)
	}

	// Querying between an original instant and its delayed instant keeps the
	// pending activation instead of skipping it.
	fmt.Printf("NextAfter(%s) = %s\n", query.UTC().Format(layout),
		func() string {
			n, err := staggered.NextAfter(query)
			if err != nil {
				panic(err)
			}
			return n.UTC().Format(layout)
		}())

	// Output:
	// original -> staggered (UTC)
	// 00:00:00.000000000 -> 00:00:13.915756871
	// 00:01:00.000000000 -> 00:01:15.612256081
	// 00:02:00.000000000 -> 00:02:05.267967872
	// 00:03:00.000000000 -> 00:03:13.816103406
	// 00:04:00.000000000 -> 00:04:06.938568621
	// NextAfter(00:00:30.000000000) = 00:01:15.612256081
}

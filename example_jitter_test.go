package cron_test

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// ExampleNewJitterSchedule prints the original minute-aligned plan next to the
// deterministic, keyed jitter plan. All inputs are fixed, so the output never
// changes and no real waiting is involved.
func ExampleNewJitterSchedule() {
	parser := cron.NewParser(
		cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.DowOptional | cron.Descriptor,
	)
	minutely, err := parser.Parse("0 * * * * *")
	if err != nil {
		panic(err)
	}

	// Attach the wrapper at a fixed reference instant instead of time.Now().
	start := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
	jittered, err := cron.NewJitterSchedule(minutely, "billing-report", 20*time.Second, start)
	if err != nil {
		panic(err)
	}

	fmt.Println("original                 jittered")
	query := start
	for i := 0; i < 6; i++ {
		original := minutely.Next(query.Add(-time.Nanosecond))
		delayed := jittered.Next(query)
		fmt.Printf("%s  %s (+%s)\n",
			original.UTC().Format("15:04:05.000000000"),
			delayed.UTC().Format("15:04:05.000000000"),
			delayed.Sub(original),
		)
		query = delayed
	}

	// Output:
	// original                 jittered
	// 00:00:00.000000000  00:00:04.493536099 (+4.493536099s)
	// 00:01:00.000000000  00:01:11.736629597 (+11.736629597s)
	// 00:02:00.000000000  00:02:11.634838095 (+11.634838095s)
	// 00:03:00.000000000  00:03:19.076916958 (+19.076916958s)
	// 00:04:00.000000000  00:04:10.926329489 (+10.926329489s)
	// 00:05:00.000000000  00:05:14.300262096 (+14.300262096s)
}

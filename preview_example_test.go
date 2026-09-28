package cron

import (
	"fmt"
	"time"
)

func ExampleCron_Preview() {
	c := New()
	c.Schedule(Every(30*time.Second), FuncJob(func() {}))
	c.Schedule(Every(time.Minute), FuncJob(func() {}))

	start := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	end := start.Add(90 * time.Second)
	items, err := c.Preview(start, end, 10)
	if err != nil {
		panic(err)
	}
	for _, item := range items {
		fmt.Printf("%s entry %d\n", item.Time.Format("15:04:05"), item.EntryID)
	}

	// Output:
	// 12:00:30 entry 1
	// 12:01:00 entry 1
	// 12:01:00 entry 2
	// 12:01:30 entry 1
}

package cron

import (
	"fmt"
	"time"
)

func ExampleCron_Preview() {
	c := New()
	first, _ := c.AddFunc("@every 30s", func() {})
	second, _ := c.AddFunc("@every 45s", func() {})
	start := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	preview, err := c.Preview(start, start.Add(90*time.Second), 4)
	if err != nil {
		panic(err)
	}
	for _, activation := range preview {
		fmt.Printf("%s entry %d\n", activation.Time.Format(time.RFC3339), activation.EntryID)
	}
	_ = first
	_ = second
	// Output:
	// 2026-09-28T12:00:30Z entry 1
	// 2026-09-28T12:00:45Z entry 2
	// 2026-09-28T12:01:00Z entry 1
	// 2026-09-28T12:01:30Z entry 1
}

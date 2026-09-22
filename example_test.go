package cron

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Pause an every-minute job at 00:00:30 and resume it at 00:04:10 with a
// catch-up policy capped at 2: the four missed activations (01/02/03/04)
// are computed once from the single pause and resume times, the two oldest
// are dropped and reported, and the two most recent are replayed in their
// original scheduled order. A fake clock keeps the example instantaneous.
func ExampleCron_Pause() {
	clock := newFakeClock(time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC))
	c := New(WithClock(clock), WithSeconds(), WithLogger(DiscardLogger))

	var n int32
	done := make(chan struct{}, 2)
	id, _ := c.AddFunc("0 * * * * *", func() {
		fmt.Printf("run #%d actually executed at %s\n", atomic.AddInt32(&n, 1), clock.Now().Format("15:04:05"))
		done <- struct{}{}
	})
	c.SetMisfirePolicy(id, MisfireCatchUp, 2)

	c.Start()
	if !clock.waitTimer() {
		panic("scheduler did not arm a timer")
	}
	clock.AdvanceTo(time.Date(2026, 1, 15, 0, 0, 30, 0, time.UTC))
	c.Pause()
	pauseTime := clock.Now()
	clock.AdvanceTo(time.Date(2026, 1, 15, 0, 4, 10, 0, time.UTC))
	resumeTime := clock.Now()

	sched, _ := NewParser(Second | Minute | Hour | Dom | Month | Dow | Descriptor).Parse("0 * * * * *")
	missed, _ := missedActivations(sched, pauseTime, resumeTime)
	for i, m := range missed {
		fate := "replayed"
		if i < len(missed)-2 {
			fate = "dropped"
		}
		fmt.Printf("scheduled %s -> %s\n", m.Format("15:04:05"), fate)
	}

	c.Resume()
	<-done
	<-done
	fmt.Printf("dropped total: %d\n", c.Entry(id).DroppedMisses)
	c.Stop()

	// Output:
	// scheduled 00:01:00 -> dropped
	// scheduled 00:02:00 -> dropped
	// scheduled 00:03:00 -> replayed
	// scheduled 00:04:00 -> replayed
	// run #1 actually executed at 00:04:10
	// run #2 actually executed at 00:04:10
	// dropped total: 2
}

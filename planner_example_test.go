package cron

import (
	"fmt"
	"sync/atomic"
	"time"
)

// ExampleConfigPlanner shows how to pre-parse a batch of schedules before
// swapping a running Cron over to a brand-new instance: the plan is printed,
// the new instance is created but not started, and one planned job is then
// triggered manually.
func ExampleConfigPlanner() {
	var helloRuns, byeRuns int64

	// The currently running Cron keeps working untouched.
	running := New(WithLogger(DiscardLogger))
	running.AddFunc("@every 1h", FuncJob(func() {}))
	running.Start()
	defer running.Stop()

	// Stage the new configuration up front. CRON_TZ=UTC keeps the printed
	// next activation deterministic regardless of the host time zone.
	planner := NewConfigPlanner()
	planner.Add("hello", "@every 1m", newCountingJob("hello", &helloRuns))
	planner.Add("bye", "CRON_TZ=UTC 30 0 * * *", newCountingJob("bye", &byeRuns))

	// Parse every expression and check the names as one atomic batch.
	plan, err := planner.Prepare()
	if err != nil {
		fmt.Println("prepare error:", err)
		return
	}

	// Print the planned labels together with their parsed next activation.
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	for _, e := range plan.Entries() {
		fmt.Printf("plan: %-5s next=%s\n", e.Name,
			e.Schedule.Next(now).In(time.UTC).Format("15:04"))
	}

	// Apply creates a new, not-started Cron; nothing runs and the old
	// Cron is left alone.
	fresh := plan.Apply()
	fmt.Println("entries on new cron:", len(fresh.Entries()))
	fmt.Println("entries on old cron:", len(running.Entries()))
	fmt.Println("new cron started:", fresh.running)
	fmt.Println("hello runs:", atomic.LoadInt64(&helloRuns))

	// Trigger one planned job by hand, without starting the scheduler.
	plan.Entries()[0].Job.Run()
	fmt.Println("hello runs after manual trigger:", atomic.LoadInt64(&helloRuns))

	// Output:
	// plan: hello next=00:01
	// plan: bye   next=00:30
	// entries on new cron: 2
	// entries on old cron: 1
	// new cron started: false
	// hello runs: 0
	// hello runs after manual trigger: 1
}

package cron_test

import (
	"fmt"

	"github.com/robfig/cron/v3"
)

// Build a plan, print it, create a new Cron from it (without starting it),
// and manually trigger one of the planned jobs exactly once.
func ExamplePlan() {
	// Pre-parse the whole batch up front. A duplicate name or an invalid
	// spec rejects the entire plan (nil plan + error).
	plan, err := cron.NewPlan(
		cron.PlanFunc("heartbeat", "@every 1s", func() { fmt.Println("heartbeat") }),
		cron.PlanFunc("report", "0 * * * *", func() { fmt.Println("report") }),
	)
	if err != nil {
		fmt.Println("plan error:", err)
		return
	}

	// Print the plan.
	fmt.Print(plan)

	// Create a new Cron from the plan but do not start it: nothing runs yet.
	newCron := cron.New()
	ids := newCron.SchedulePlan(plan)

	// Manually trigger the first planned job exactly once, without starting
	// the scheduler.
	newCron.Entry(ids[0]).Job.Run()

	// Output:
	// Plan with 2 entries:
	//   1. heartbeat -> @every 1s
	//   2. report -> 0 * * * *
	// heartbeat
}

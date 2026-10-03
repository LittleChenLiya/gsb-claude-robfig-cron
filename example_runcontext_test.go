package cron_test

import (
	"context"
	"fmt"

	"github.com/robfig/cron/v3"
)

// ExampleWithRunContext shows how to give every run of a job its own identity
// and schedule tags, and how the end-of-run notification observes the same
// identity for both normal completions and panics.
//
// The ID generator is supplied by the caller, so tests can use a deterministic
// counter instead of randomness.
func ExampleWithRunContext() {
	var n int
	wrapper := cron.WithRunContext(cron.RunContextOptions{
		NewID: func() string {
			n++
			return fmt.Sprintf("run-%d", n)
		},
		Tags: map[string]string{"schedule": "every-minute"},
		Notify: func(info cron.RunInfo, event cron.RunEvent, _ interface{}) {
			fmt.Printf("finished %s (%s) schedule=%s\n", info.ID, event, info.Tag("schedule"))
		},
	})

	job := cron.ContextFunc(func(ctx context.Context) {
		info, _ := cron.RunInfoFromContext(ctx)
		fmt.Printf("running %s\n", info.ID)
	})
	panicky := cron.ContextFunc(func(ctx context.Context) {
		info, _ := cron.RunInfoFromContext(ctx)
		fmt.Printf("running %s\n", info.ID)
		panic("boom")
	})

	wrapped := cron.NewChain(wrapper).Then(job)
	wrapped.Run()
	wrapped.Run()

	// A panicking run is notified with the same identity before the panic
	// propagates.
	wrappedPanic := cron.NewChain(wrapper).Then(panicky)
	func() {
		defer func() { _ = recover() }()
		wrappedPanic.Run()
	}()

	// Output:
	// running run-1
	// finished run-1 (completed) schedule=every-minute
	// running run-2
	// finished run-2 (completed) schedule=every-minute
	// running run-3
	// finished run-3 (panicked) schedule=every-minute
}

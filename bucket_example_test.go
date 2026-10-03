package cron

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ExampleSkipDuplicateInBucket demonstrates a short count: ten goroutines fire
// the same bucket concurrently, but the underlying job runs exactly once.
func ExampleSkipDuplicateInBucket() {
	keyFunc := func(time.Time) string { return "A" }
	var calls int32
	job := FuncJob(func() {
		atomic.AddInt32(&calls, 1)
	})
	wrapped := SkipDuplicateInBucket(DiscardLogger, 2, keyFunc, nil)(job)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wrapped.Run()
		}()
	}
	wg.Wait()

	fmt.Println(calls)
	// Output: 1
}

// ExampleSkipDuplicateInBucket_panicRetry demonstrates that a failed bucket
// is retried on the next activation.
func ExampleSkipDuplicateInBucket_panicRetry() {
	keyFunc := func(time.Time) string { return "A" }
	var calls int32
	job := FuncJob(func() {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			panic("boom")
		}
	})
	var errs int32
	wrapped := SkipDuplicateInBucket(DiscardLogger, 2, keyFunc, func(error) {
		atomic.AddInt32(&errs, 1)
	})(job)

	wrapped.Run() // panics, recovered, onError called
	wrapped.Run() // retried, succeeds

	fmt.Println(calls, errs)
	// Output: 2 1
}

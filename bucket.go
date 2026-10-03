package cron

import (
	"fmt"
	"sync"
	"time"
)

// BucketKeyFunc returns the bucket key for the given activation time.
//
// The key function is caller-supplied: the wrapper never guesses a bucket
// from real sleep duration. Instead, the caller decides how to map a time to
// a bucket (e.g. truncate to the minute, hour, or day). Two activations that
// map to the same key are considered the same bucket.
type BucketKeyFunc func(time.Time) string

// SkipDuplicateInBucket returns a JobWrapper that deduplicates runs falling in
// the same time bucket.
//
// Behavior:
//
//   - Two activations in the same bucket (as computed by keyFunc) are
//     considered duplicates. While a bucket is running, additional activations
//     are skipped. Once a bucket has completed successfully, later activations
//     in the same bucket are also skipped.
//   - If the underlying job panics, the bucket is marked as failed and a later
//     activation in the same bucket is allowed to run again. The panic is
//     recovered and reported to onError (if non-nil) and to the logger at
//     Error level.
//   - At most size completed buckets are retained. When a new bucket is added
//     and the cache is full, the oldest completed bucket is evicted. Running
//     buckets are never evicted, so an in-flight bucket is always allowed to
//     finish.
//
// size must be positive; keyFunc must be non-nil.
//
// SkipDuplicateInBucket must be the innermost wrapper that handles panics
// (or used alone). If a Recover wrapper is installed inside it, the inner
// Recover will swallow panics before this wrapper can detect them, and a
// failed bucket will be incorrectly marked as completed.
func SkipDuplicateInBucket(logger Logger, size int, keyFunc BucketKeyFunc, onError func(error)) JobWrapper {
	if size <= 0 {
		panic("cron: SkipDuplicateInBucket: size must be positive")
	}
	if keyFunc == nil {
		panic("cron: SkipDuplicateInBucket: keyFunc must not be nil")
	}
	if logger == nil {
		logger = DiscardLogger
	}
	return func(j Job) Job {
		d := &bucketDeduper{
			size:    size,
			keyFunc: keyFunc,
			onError: onError,
			logger:  logger,
			job:     j,
			buckets: make(map[string]*bucketRun),
		}
		return FuncJob(d.run)
	}
}

// bucketRun tracks the state of a single bucket.
type bucketRun struct {
	running bool
	done    bool
}

// bucketDeduper holds the state for deduplicating runs in time buckets.
type bucketDeduper struct {
	mu      sync.Mutex
	size    int
	keyFunc BucketKeyFunc
	onError func(error)
	logger  Logger
	job     Job
	buckets map[string]*bucketRun
	order   []string // insertion order, oldest first
}

// run executes the job for the current bucket, deduplicating concurrent and
// repeated activations in the same bucket.
func (d *bucketDeduper) run() {
	key := d.keyFunc(time.Now())

	d.mu.Lock()
	if r, ok := d.buckets[key]; ok {
		if r.running || r.done {
			d.mu.Unlock()
			d.logger.Info("skip duplicate in bucket", "bucket", key)
			return
		}
		// Bucket exists but is neither running nor done: a previous attempt
		// failed and was removed, so this is a fresh retry. Fall through.
	} else {
		// Make room: evict oldest completed buckets, never running ones.
		for len(d.order) >= d.size {
			idx := -1
			for i, k := range d.order {
				if br := d.buckets[k]; br != nil && br.done {
					idx = i
					break
				}
			}
			if idx < 0 {
				break // all buckets are running; cannot evict
			}
			delete(d.buckets, d.order[idx])
			d.order = append(d.order[:idx], d.order[idx+1:]...)
		}
		d.buckets[key] = &bucketRun{}
		d.order = append(d.order, key)
	}
	d.buckets[key].running = true
	d.mu.Unlock()

	// Run the underlying job, recovering panics so a failure frees the
	// bucket for a later retry.
	var runErr error
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				runErr = toError(rec)
			}
		}()
		d.job.Run()
	}()

	d.mu.Lock()
	if runErr != nil {
		// Failed: remove the bucket so a later activation can retry.
		delete(d.buckets, key)
		removeOrder(&d.order, key)
		d.mu.Unlock()
		d.logger.Error(runErr, "panic in bucket", "bucket", key)
		if d.onError != nil {
			d.onError(runErr)
		}
		return
	}
	// Succeeded: mark done and evict oldest completed buckets to keep the
	// completed-bucket count bounded.
	if r, ok := d.buckets[key]; ok {
		r.running = false
		r.done = true
	}
	for {
		doneCount := 0
		oldestDone := -1
		for i, k := range d.order {
			if br := d.buckets[k]; br != nil && br.done {
				doneCount++
				if oldestDone < 0 {
					oldestDone = i
				}
			}
		}
		if doneCount <= d.size || oldestDone < 0 {
			break
		}
		delete(d.buckets, d.order[oldestDone])
		d.order = append(d.order[:oldestDone], d.order[oldestDone+1:]...)
	}
	d.mu.Unlock()
}

// keys returns the current bucket keys in insertion order (for testing).
func (d *bucketDeduper) keys() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	keys := make([]string, len(d.order))
	copy(keys, d.order)
	return keys
}

// toError converts a recovered panic value to an error.
func toError(rec interface{}) error {
	if err, ok := rec.(error); ok {
		return err
	}
	return fmt.Errorf("%v", rec)
}

// removeOrder removes the first occurrence of key from order.
func removeOrder(order *[]string, key string) {
	for i, k := range *order {
		if k == key {
			*order = append((*order)[:i], (*order)[i+1:]...)
			return
		}
	}
}

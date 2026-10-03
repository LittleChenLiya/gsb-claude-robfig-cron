package cron

import (
	"fmt"
	"sync"
)

// BucketKeyFunc returns the time-bucket (deduplication) key for a job
// invocation. The key is supplied explicitly by the caller, for example by
// truncating a schedule activation time to the scheduling interval; the
// wrapper never infers a bucket from wall-clock sleep durations. Two
// invocations that report the same key belong to the same bucket.
//
// The key function is called outside of the wrapper's internal lock, so it
// must be safe to invoke concurrently.
type BucketKeyFunc func() string

// BucketErrorHandler is invoked when a job run panics, after the bucket has
// already been marked as eligible for retry. It is called while the wrapper's
// internal state lock is held, so the handler must not re-enter the wrapped
// job's Run method (that would deadlock).
type BucketErrorHandler func(bucket string, err error)

// BucketOption customizes a SkipByBucket wrapper.
type BucketOption func(*bucketConfig)

type bucketConfig struct {
	limit   int
	onError BucketErrorHandler
}

// WithCompletedBucketLimit bounds the number of successfully completed buckets
// that the wrapper remembers. It must be at least 1. When a new bucket
// completes successfully and the limit is exceeded, the oldest completed
// bucket is forgotten. Buckets that are currently running are never evicted:
// only completed buckets are counted and removed.
//
// The default limit is DefaultCompletedBucketLimit.
func WithCompletedBucketLimit(limit int) BucketOption {
	return func(c *bucketConfig) {
		if limit < 1 {
			panic("cron: completed bucket limit must be at least 1")
		}
		c.limit = limit
	}
}

// WithBucketErrorHandler installs a callback for job runs that panic. A
// recovered panic is reported as an error. After the callback returns, the
// bucket is forgotten and its next invocation is allowed to run again.
func WithBucketErrorHandler(handler BucketErrorHandler) BucketOption {
	return func(c *bucketConfig) {
		c.onError = handler
	}
}

// DefaultCompletedBucketLimit is the default bound on remembered completed
// buckets.
const DefaultCompletedBucketLimit = 1000

// SkipByBucket is shorthand for SkipByBucketWith(DiscardLogger, bucketKey,
// opts...): duplicate runs of j are merged by the caller-supplied bucket key
// and skips are not logged. See SkipByBucketWith for the full semantics.
func SkipByBucket(bucketKey BucketKeyFunc, opts ...BucketOption) JobWrapper {
	return SkipByBucketWith(DiscardLogger, bucketKey, opts...)
}

// SkipByBucketWith returns a JobWrapper that merges duplicate runs of the
// wrapped job by the time-bucket key returned by bucketKey. Skips are logged
// to logger at Info level.
//
// Semantics (independent of any real sleep duration; the bucket is taken from
// bucketKey, never inferred):
//
//   - If a run for the bucket is currently in progress, the invocation is
//     skipped.
//   - If a run for the bucket has already completed successfully, the
//     invocation is skipped until the completed bucket is evicted because the
//     bounded cache is full.
//   - If a run panics, the panic is recovered, reported to the error handler
//     (if any), and the bucket is forgotten so that the same bucket may be
//     retried. The panic is deliberately not re-raised by this wrapper;
//     compose Recover in a Chain if other jobs need panic logging.
//   - At most a bounded number of successfully completed buckets are
//     remembered. Eviction removes only completed buckets; a bucket whose run
//     is in flight is never evicted.
//
// This wrapper does not change the behavior of SkipIfStillRunning, which is
// keyless and skips based solely on whether any previous invocation is still
// running.
//
// Each wrapped job keeps its own independent bucket state, just like
// SkipIfStillRunning.
func SkipByBucketWith(logger Logger, bucketKey BucketKeyFunc, opts ...BucketOption) JobWrapper {
	if logger == nil {
		logger = DefaultLogger
	}
	cfg := bucketConfig{limit: DefaultCompletedBucketLimit}
	for _, opt := range opts {
		opt(&cfg)
	}
	return func(j Job) Job {
		g := &bucketGuard{
			job:       j,
			logger:    logger,
			keyFunc:   bucketKey,
			limit:     cfg.limit,
			onError:   cfg.onError,
			running:   make(map[string]struct{}),
			completed: make(map[string]struct{}),
		}
		return FuncJob(g.run)
	}
}

// bucketGuard holds the per-wrapped-job deduplication state.
type bucketGuard struct {
	job     Job
	logger  Logger
	keyFunc BucketKeyFunc
	onError BucketErrorHandler

	mu        sync.Mutex
	running   map[string]struct{}
	completed map[string]struct{}
	// order records completed buckets in FIFO order, mirroring completed: a
	// bucket is added to both together when its run succeeds and removed from
	// both on eviction. A bucket is never in running while it is in these
	// structures, which is why eviction cannot affect in-flight buckets.
	order []string
	limit int
}

// run executes one invocation of the wrapped job for the caller's bucket.
func (g *bucketGuard) run() {
	bucket := g.keyFunc()

	g.mu.Lock()
	if _, ok := g.running[bucket]; ok {
		g.mu.Unlock()
		g.logger.Info("skip", "bucket", bucket, "reason", "running")
		return
	}
	if _, ok := g.completed[bucket]; ok {
		g.mu.Unlock()
		g.logger.Info("skip", "bucket", bucket, "reason", "completed")
		return
	}
	g.running[bucket] = struct{}{}
	g.mu.Unlock()

	var panicked interface{}
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = r
			}
		}()
		g.job.Run()
	}()

	g.mu.Lock()
	delete(g.running, bucket)
	if panicked != nil {
		// Panic: the bucket was never recorded as completed, so it can be
		// retried immediately.
		if g.onError != nil {
			err, ok := panicked.(error)
			if !ok {
				err = fmt.Errorf("%v", panicked)
			}
			g.onError(bucket, err)
		}
		g.mu.Unlock()
		return
	}
	// Success: remember the completed bucket, bounded by the FIFO limit.
	if _, ok := g.completed[bucket]; !ok {
		g.completed[bucket] = struct{}{}
		g.order = append(g.order, bucket)
		g.evictCompletedLocked()
	}
	g.mu.Unlock()
}

// evictCompletedLocked forgets the oldest completed buckets beyond the limit.
// The caller must hold g.mu.
//
// In-flight buckets cannot be evicted: a bucket enters order/completed only
// after its single allowed run finishes, and it is removed from both when
// forgotten. So while a bucket is in g.running it is never present in
// g.order or g.completed, and FIFO eviction of completed entries can never
// touch it.
func (g *bucketGuard) evictCompletedLocked() {
	for len(g.completed) > g.limit {
		oldest := g.order[0]
		g.order = g.order[1:]
		delete(g.completed, oldest)
	}
}

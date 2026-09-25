package cron

import (
	"sync"
	"time"
)

// OverflowPolicy decides what a QueuedJob does with a trigger that arrives
// while its waiting queue is already full.
type OverflowPolicy int

const (
	// DropOldest evicts the oldest waiting item from the queue to make room
	// for the incoming trigger. The evicted trigger never runs.
	DropOldest OverflowPolicy = iota

	// RejectNewest refuses the incoming trigger when the queue is full. The
	// rejected trigger never runs, but its planned time is kept in the
	// statistics (see QueuedJob.Dropped).
	RejectNewest
)

// String returns a human-readable name for the policy.
func (p OverflowPolicy) String() string {
	switch p {
	case DropOldest:
		return "DropOldest"
	case RejectNewest:
		return "RejectNewest"
	default:
		return "Unknown"
	}
}

// DropReason explains why a trigger never ran.
type DropReason int

const (
	// DropReasonOverflow indicates the trigger was evicted from a full queue
	// by a newer trigger (DropOldest policy).
	DropReasonOverflow DropReason = iota

	// DropReasonRejected indicates the trigger was refused because the queue
	// was full (RejectNewest policy).
	DropReasonRejected

	// DropReasonClosed indicates the trigger arrived after Close was called.
	DropReasonClosed
)

// String returns a human-readable name for the reason.
func (r DropReason) String() string {
	switch r {
	case DropReasonOverflow:
		return "Overflow"
	case DropReasonRejected:
		return "Rejected"
	case DropReasonClosed:
		return "Closed"
	default:
		return "Unknown"
	}
}

// DropRecord describes a single trigger that never ran. The planned time of
// the trigger is always preserved, including for RejectNewest overflows, so
// that callers can build statistics over dropped activations.
type DropRecord struct {
	// Planned is the time the trigger was originally scheduled for.
	Planned time.Time

	// Reason is why the trigger was dropped.
	Reason DropReason
}

// RunRecord describes a single actual execution of the inner job.
type RunRecord struct {
	// Planned is the time the trigger was originally scheduled for.
	Planned time.Time

	// Started is the time the inner job actually started running.
	Started time.Time
}

// QueueStats is a point-in-time snapshot of QueuedJob counters.
type QueueStats struct {
	// Triggers is the total number of times the job was triggered.
	Triggers int64

	// Enqueued is the number of triggers accepted into the queue.
	Enqueued int64

	// Started is the number of triggers whose execution has begun.
	Started int64

	// Completed is the number of triggers whose execution has finished.
	Completed int64

	// DroppedOldest is the number of waiting triggers evicted from a full
	// queue to make room for newer ones (DropOldest policy).
	DroppedOldest int64

	// RejectedNewest is the number of triggers refused because the queue was
	// full (RejectNewest policy, or a zero-capacity queue).
	RejectedNewest int64

	// RejectedClosed is the number of triggers refused because Close had
	// already been called.
	RejectedClosed int64
}

// queueItem is one pending invocation of the inner job.
type queueItem struct {
	planned time.Time
}

// QueuedJob is a Job (and the state behind the QueueIfStillRunning job
// wrapper) that allows at most one instance of the inner job to run at a
// time. Triggers that arrive while the inner job is running are placed in a
// bounded FIFO waiting queue; the queue capacity does NOT include the
// currently running instance. When the queue is full, the configured
// OverflowPolicy decides whether the oldest waiting trigger is evicted
// (DropOldest) or the incoming trigger is refused (RejectNewest).
//
// Each QueuedJob has its own queue and dispatcher goroutine, so distinct
// jobs never block each other.
//
// Panics are not intercepted by QueuedJob itself: the inner job runs
// synchronously on the dispatcher goroutine, so wrap the inner job with
// Recover (i.e. place QueueIfStillRunning before Recover in a Chain, or wrap
// with NewChain(Recover(logger)).Then(inner) before NewQueuedJob) and panics
// are handled by the existing Recover chain while the execution slot is
// released for the next queued trigger.
type QueuedJob struct {
	inner    Job
	logger   Logger
	policy   OverflowPolicy
	capacity int

	mu      sync.Mutex
	queue   []queueItem
	running bool
	closed  bool
	stats   QueueStats
	drops   []DropRecord
	history []RunRecord

	wake    chan struct{}
	closeCh chan struct{}
	done    chan struct{}
}

// NewQueuedJob returns a QueuedJob wrapping inner. Capacity is the maximum
// number of triggers allowed to wait in the queue and does not include the
// currently running instance; a negative capacity is treated as zero. The
// returned job's dispatcher goroutine runs until Close is called and the
// queue has drained.
func NewQueuedJob(inner Job, capacity int, policy OverflowPolicy, logger Logger) *QueuedJob {
	if capacity < 0 {
		capacity = 0
	}
	if logger == nil {
		logger = DiscardLogger
	}
	q := &QueuedJob{
		inner:    inner,
		logger:   logger,
		policy:   policy,
		capacity: capacity,
		wake:     make(chan struct{}, 1),
		closeCh:  make(chan struct{}),
		done:     make(chan struct{}),
	}
	go q.loop()
	return q
}

// QueueIfStillRunning returns a JobWrapper that gives every wrapped job its
// own QueuedJob: at most one instance of the job runs at a time, extra
// triggers wait in a bounded FIFO queue of the given capacity, and the
// overflow policy is applied when the queue is full. Jobs wrapped separately
// never block each other.
//
// To have panics handled by Recover and still release the execution slot,
// place this wrapper before Recover in the chain:
//
//	cron.New(cron.WithChain(
//		cron.QueueIfStillRunning(2, cron.RejectNewest, logger),
//		cron.Recover(logger),
//	))
//
// Use NewQueuedJob directly when you need access to the statistics and the
// Close/Wait lifecycle of the returned job.
func QueueIfStillRunning(capacity int, policy OverflowPolicy, logger Logger) JobWrapper {
	return func(j Job) Job {
		return NewQueuedJob(j, capacity, policy, logger)
	}
}

// Run implements Job. It records the trigger with the current time as its
// planned time and returns immediately; the inner job runs asynchronously.
func (q *QueuedJob) Run() {
	q.EnqueueAt(time.Now())
}

// EnqueueAt records a trigger planned for the given time. It reports whether
// the trigger was accepted into the queue. Triggers arriving after Close, or
// refused by a full RejectNewest queue, return false but are still counted in
// the statistics with their planned time preserved.
func (q *QueuedJob) EnqueueAt(planned time.Time) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stats.Triggers++
	if q.closed {
		q.stats.RejectedClosed++
		q.drops = append(q.drops, DropRecord{Planned: planned, Reason: DropReasonClosed})
		q.logger.Info("queue closed, trigger refused", "planned", planned)
		return false
	}
	// A trigger that can start immediately (the execution slot is free and
	// nothing is waiting) is always accepted, even with a zero-capacity
	// queue: the capacity bounds only the waiting queue, not the slot.
	canStartNow := !q.running && len(q.queue) == 0
	if !canStartNow && len(q.queue) >= q.capacity {
		if q.policy == DropOldest && len(q.queue) > 0 {
			dropped := q.queue[0]
			q.queue = append(q.queue[1:], queueItem{planned: planned})
			q.stats.DroppedOldest++
			q.stats.Enqueued++
			q.drops = append(q.drops, DropRecord{Planned: dropped.planned, Reason: DropReasonOverflow})
			q.logger.Info("queue full, oldest trigger dropped",
				"planned", dropped.planned, "replacedBy", planned)
			q.wakeLocked()
			return true
		}
		// RejectNewest policy (or a zero-capacity queue, which has no oldest
		// item to evict): refuse the incoming trigger, keeping its planned
		// time for statistics.
		q.stats.RejectedNewest++
		q.drops = append(q.drops, DropRecord{Planned: planned, Reason: DropReasonRejected})
		q.logger.Info("queue full, newest trigger rejected", "planned", planned)
		return false
	}
	q.queue = append(q.queue, queueItem{planned: planned})
	q.stats.Enqueued++
	q.wakeLocked()
	return true
}

// Close stops the intake of new triggers: subsequent Run/EnqueueAt calls are
// refused and counted as RejectedClosed. The currently running instance is
// NOT interrupted, and triggers already waiting in the queue still run to
// completion. Close is idempotent; calling it more than once is a no-op.
// Use Wait to block until the queue has drained.
func (q *QueuedJob) Close() {
	q.mu.Lock()
	already := q.closed
	q.closed = true
	q.mu.Unlock()
	if !already {
		close(q.closeCh)
	}
}

// Wait blocks until Close has been called and every queued trigger (plus the
// currently running instance, if any) has finished. It is safe to call Wait
// multiple times and from multiple goroutines; once the queue has drained,
// subsequent calls return immediately with the same drained result.
func (q *QueuedJob) Wait() {
	<-q.done
}

// Stats returns a snapshot of the job's counters.
func (q *QueuedJob) Stats() QueueStats {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.stats
}

// Dropped returns the records of all triggers that never ran, in the order
// they were dropped. The planned time of each dropped trigger is preserved,
// including triggers refused by the RejectNewest policy.
func (q *QueuedJob) Dropped() []DropRecord {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]DropRecord, len(q.drops))
	copy(out, q.drops)
	return out
}

// History returns the records of all executions started so far, in start
// order, each carrying its planned and actual start times.
func (q *QueuedJob) History() []RunRecord {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]RunRecord, len(q.history))
	copy(out, q.history)
	return out
}

// Pending returns the number of triggers currently waiting in the queue. The
// currently running instance is not counted.
func (q *QueuedJob) Pending() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.queue)
}

// wakeLocked notifies the dispatcher that work is available. q.mu must be
// held; the notification is best-effort because the dispatcher re-checks the
// queue under the lock anyway.
func (q *QueuedJob) wakeLocked() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// loop is the per-job dispatcher. It runs queued triggers one at a time, so
// at most one instance of the inner job is ever executing, and exits once the
// job is closed and the queue has drained.
func (q *QueuedJob) loop() {
	defer close(q.done)
	for {
		q.mu.Lock()
		if len(q.queue) == 0 {
			if q.closed {
				q.mu.Unlock()
				return
			}
			q.mu.Unlock()
			select {
			case <-q.wake:
			case <-q.closeCh:
			}
			continue
		}
		item := q.queue[0]
		q.queue[0] = queueItem{}
		q.queue = q.queue[1:]
		q.running = true
		started := time.Now()
		q.stats.Started++
		q.history = append(q.history, RunRecord{Planned: item.planned, Started: started})
		q.logger.Info("queue start", "planned", item.planned, "started", started,
			"pending", len(q.queue))
		q.mu.Unlock()

		// The inner job runs synchronously on this goroutine. If it panics,
		// a Recover wrapper inside the chain handles it and Run returns
		// normally, releasing the slot for the next queued trigger.
		q.inner.Run()

		q.mu.Lock()
		q.running = false
		q.stats.Completed++
		q.mu.Unlock()
	}
}

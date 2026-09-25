package cron

import (
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// seqJob is a controllable job for tests: it records the order in which its
// invocations start, reports each start on the started channel, and blocks
// its first invocation until the gate channel is closed.
type seqJob struct {
	mu      sync.Mutex
	order   []int
	started chan int
	gate    chan struct{}
}

func newSeqJob() *seqJob {
	return &seqJob{
		started: make(chan int, 32),
		gate:    make(chan struct{}),
	}
}

func (j *seqJob) Run() {
	j.mu.Lock()
	id := len(j.order) + 1
	j.order = append(j.order, id)
	j.mu.Unlock()
	j.started <- id
	if id == 1 && j.gate != nil {
		<-j.gate
	}
}

func (j *seqJob) runs() []int {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]int, len(j.order))
	copy(out, j.order)
	return out
}

// captureLogger records Error calls so tests can verify Recover fired.
type captureLogger struct {
	mu     sync.Mutex
	errors int
}

func (l *captureLogger) Info(msg string, keysAndValues ...interface{}) {}

func (l *captureLogger) Error(err error, msg string, keysAndValues ...interface{}) {
	l.mu.Lock()
	l.errors++
	l.mu.Unlock()
}

func (l *captureLogger) errorCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.errors
}

func plannedTimes(base time.Time, n int) []time.Time {
	out := make([]time.Time, n)
	for i := range out {
		out[i] = base.Add(time.Duration(i) * 30 * time.Second)
	}
	return out
}

func historyPlanned(h []RunRecord) []time.Time {
	out := make([]time.Time, len(h))
	for i, r := range h {
		out[i] = r.Planned
	}
	return out
}

// With capacity 2 and the first trigger blocked, four triggers under the
// DropOldest policy must execute t0, t2, t3: t1 is evicted from the full
// queue by t3.
func TestQueuedJobDropOldestSequence(t *testing.T) {
	job := newSeqJob()
	q := NewQueuedJob(job, 2, DropOldest, DiscardLogger)
	planned := plannedTimes(time.Now(), 4)

	q.EnqueueAt(planned[0])
	<-job.started // first trigger is now running (blocked on the gate)

	q.EnqueueAt(planned[1])
	q.EnqueueAt(planned[2])
	if p := q.Pending(); p != 2 {
		t.Fatalf("expected 2 pending triggers (running one not counted), got %d", p)
	}
	if s := q.Stats(); s.DroppedOldest != 0 {
		t.Fatalf("no drops expected before the queue is full, got %d", s.DroppedOldest)
	}

	q.EnqueueAt(planned[3]) // queue full: evicts planned[1]
	if s := q.Stats(); s.DroppedOldest != 1 {
		t.Fatalf("expected 1 dropped-oldest trigger, got %d", s.DroppedOldest)
	}
	drops := q.Dropped()
	if len(drops) != 1 || !drops[0].Planned.Equal(planned[1]) || drops[0].Reason != DropReasonOverflow {
		t.Fatalf("expected drop record for planned[1], got %+v", drops)
	}

	close(job.gate)
	q.Close()
	q.Wait()
	q.Wait() // the drained result must remain observable on repeated waits

	if got, want := historyPlanned(q.History()), []time.Time{planned[0], planned[2], planned[3]}; !reflect.DeepEqual(got, want) {
		t.Errorf("unexpected execution sequence: got %v, want %v", got, want)
	}
	if got := len(job.runs()); got != 3 {
		t.Errorf("expected 3 executions, got %d", got)
	}
	s := q.Stats()
	if s.Triggers != 4 || s.Enqueued != 4 || s.Started != 3 || s.Completed != 3 {
		t.Errorf("unexpected stats: %+v", s)
	}
}

// With capacity 2 and the first trigger blocked, four triggers under the
// RejectNewest policy must execute t0, t1, t2: t3 is refused, but its planned
// time is still recorded for statistics.
func TestQueuedJobRejectNewestSequence(t *testing.T) {
	job := newSeqJob()
	q := NewQueuedJob(job, 2, RejectNewest, DiscardLogger)
	planned := plannedTimes(time.Now(), 4)

	q.EnqueueAt(planned[0])
	<-job.started // first trigger is now running (blocked on the gate)

	q.EnqueueAt(planned[1])
	q.EnqueueAt(planned[2])
	if p := q.Pending(); p != 2 {
		t.Fatalf("expected 2 pending triggers (running one not counted), got %d", p)
	}

	if ok := q.EnqueueAt(planned[3]); ok {
		t.Fatal("expected the fourth trigger to be rejected by a full queue")
	}
	s := q.Stats()
	if s.RejectedNewest != 1 || s.DroppedOldest != 0 {
		t.Fatalf("expected 1 rejected-newest trigger, got %+v", s)
	}
	// The rejected trigger's planned time must be preserved for statistics.
	drops := q.Dropped()
	if len(drops) != 1 || !drops[0].Planned.Equal(planned[3]) || drops[0].Reason != DropReasonRejected {
		t.Fatalf("expected reject record carrying planned[3], got %+v", drops)
	}

	close(job.gate)
	q.Close()
	q.Wait()

	if got, want := historyPlanned(q.History()), []time.Time{planned[0], planned[1], planned[2]}; !reflect.DeepEqual(got, want) {
		t.Errorf("unexpected execution sequence: got %v, want %v", got, want)
	}
	if s := q.Stats(); s.Triggers != 4 || s.Enqueued != 3 || s.Started != 3 || s.Completed != 3 {
		t.Errorf("unexpected stats: %+v", s)
	}
}

// A zero-capacity queue accepts nothing while a run is in progress, even
// under DropOldest (there is no oldest waiting item to evict).
func TestQueuedJobZeroCapacity(t *testing.T) {
	job := newSeqJob()
	q := NewQueuedJob(job, 0, DropOldest, DiscardLogger)

	q.Run()
	<-job.started
	if ok := q.EnqueueAt(time.Now()); ok {
		t.Fatal("expected trigger to be refused by a zero-capacity queue")
	}
	if s := q.Stats(); s.RejectedNewest != 1 {
		t.Fatalf("expected 1 rejected trigger, got %+v", s)
	}
	close(job.gate)
	q.Close()
	q.Wait()
	if got := len(job.runs()); got != 1 {
		t.Errorf("expected 1 execution, got %d", got)
	}
}

// Close stops the intake but lets the queue drain; the running instance is
// not killed, and Wait can be awaited repeatedly.
func TestQueuedJobCloseDrainsAndWaitIsRepeatable(t *testing.T) {
	job := newSeqJob()
	q := NewQueuedJob(job, 5, RejectNewest, DiscardLogger)

	q.Run()
	<-job.started // blocked on the gate
	q.Run()
	q.Run()

	q.Close()
	q.Close() // repeated Close must be a no-op

	if ok := q.EnqueueAt(time.Now()); ok {
		t.Fatal("expected trigger after Close to be refused")
	}
	done := make(chan struct{})
	go func() {
		q.Wait()
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("Wait returned before the queue drained")
	case <-time.After(20 * time.Millisecond):
	}

	close(job.gate) // release the running instance; queued triggers drain
	<-done
	q.Wait() // repeated Wait returns immediately with the same drained result

	if got := len(job.runs()); got != 3 {
		t.Errorf("expected all 3 queued triggers to run after Close, got %d", got)
	}
	s := q.Stats()
	if s.RejectedClosed != 1 || s.Completed != 3 {
		t.Errorf("unexpected stats: %+v", s)
	}
	drops := q.Dropped()
	if len(drops) != 1 || drops[0].Reason != DropReasonClosed {
		t.Errorf("expected a closed-intake drop record, got %+v", drops)
	}
}

// A panic in the inner job must flow through the existing Recover chain and
// the execution slot must be released so the next queued trigger runs.
func TestQueuedJobPanicRecoversAndReleasesSlot(t *testing.T) {
	logger := &captureLogger{}
	var runs int64
	invoked := make(chan struct{}, 2)
	inner := FuncJob(func() {
		invoked <- struct{}{}
		if atomic.AddInt64(&runs, 1) == 1 {
			panic("boom")
		}
	})
	// The queue wrapper sits outside Recover: the dispatcher invokes the
	// Recover-wrapped job, so the panic is recovered and the loop continues.
	q := NewQueuedJob(NewChain(Recover(logger)).Then(inner), 1, RejectNewest, DiscardLogger)

	q.Run()
	<-invoked // wait until the panicking run actually holds the slot
	q.Run()
	q.Close()
	q.Wait()

	if got := atomic.LoadInt64(&runs); got != 2 {
		t.Errorf("expected the trigger after the panic to run, got %d runs", got)
	}
	if logger.errorCount() != 1 {
		t.Errorf("expected Recover chain to log 1 panic, got %d", logger.errorCount())
	}
	if s := q.Stats(); s.Completed != 2 {
		t.Errorf("expected both triggers completed (slot released), got %+v", s)
	}
}

// QueueIfStillRunning must compose with Recover in a Chain: with the queue
// wrapper listed first, panics are recovered and the queue keeps flowing.
func TestQueueIfStillRunningWrapperOrder(t *testing.T) {
	logger := &captureLogger{}
	var runs int64
	invoked := make(chan struct{}, 2)
	inner := FuncJob(func() {
		invoked <- struct{}{}
		if atomic.AddInt64(&runs, 1) == 1 {
			panic("boom")
		}
	})
	wrapped := NewChain(
		QueueIfStillRunning(1, RejectNewest, DiscardLogger),
		Recover(logger),
	).Then(inner)

	q, ok := wrapped.(*QueuedJob)
	if !ok {
		t.Fatalf("expected QueueIfStillRunning to produce a *QueuedJob, got %T", wrapped)
	}
	q.Run()
	<-invoked // wait until the panicking run actually holds the slot
	q.Run()
	q.Close()
	q.Wait()

	if got := atomic.LoadInt64(&runs); got != 2 {
		t.Errorf("expected 2 runs, got %d", got)
	}
	if logger.errorCount() != 1 {
		t.Errorf("expected Recover chain to log 1 panic, got %d", logger.errorCount())
	}
}

// Distinct jobs each get their own queue and dispatcher: a blocked job must
// not block another job.
func TestQueuedJobJobsAreIndependent(t *testing.T) {
	blocked := newSeqJob()
	free := newSeqJob()
	close(free.gate) // the second job never blocks

	q1 := NewQueuedJob(blocked, 1, RejectNewest, DiscardLogger)
	q2 := NewQueuedJob(free, 1, RejectNewest, DiscardLogger)

	q1.Run()
	<-blocked.started // q1 is now stuck on its gate

	q2.Run()
	select {
	case <-free.started:
	case <-time.After(time.Second):
		t.Fatal("second job blocked by the first job's queue")
	}

	close(blocked.gate)
	q1.Close()
	q2.Close()
	q1.Wait()
	q2.Wait()
}

// Run, Close, Stats and Wait may be called concurrently; run with -race.
func TestQueuedJobCloseTriggerRace(t *testing.T) {
	q := NewQueuedJob(FuncJob(func() { time.Sleep(time.Millisecond) }), 2, RejectNewest, DiscardLogger)

	const producers = 8
	const triggersEach = 50
	var wg sync.WaitGroup
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < triggersEach; j++ {
				q.Run()
			}
		}()
	}
	go q.Stats()
	go q.Close()
	wg.Wait()
	q.Close() // idempotent, even racing with the earlier Close
	q.Wait()
	q.Wait()

	s := q.Stats()
	if want := int64(producers * triggersEach); s.Triggers != want {
		t.Errorf("expected %d triggers, got %d", want, s.Triggers)
	}
	if s.Triggers != s.Enqueued+s.RejectedNewest+s.RejectedClosed {
		t.Errorf("inconsistent stats: %+v", s)
	}
	if s.Started != s.Completed {
		t.Errorf("expected all started runs completed after drain, got %+v", s)
	}
}

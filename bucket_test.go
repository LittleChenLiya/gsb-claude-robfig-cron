package cron

import (
	"fmt"
	"io/ioutil"
	"log"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---- test plumbing: a per-goroutine bucket so several buckets can be in
// flight concurrently in the same process.

var goroutineBuckets sync.Map // goroutine id -> string

// goID returns the calling goroutine's numeric id (test-only).
func goID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	s := strings.TrimPrefix(string(buf[:n]), "goroutine ")
	if i := strings.IndexByte(s, ' '); i >= 0 {
		s = s[:i]
	}
	id, _ := strconv.ParseInt(s, 10, 64)
	return id
}

// runAs invokes fn with bucket associated with the current goroutine. The
// wrapper's key function and the recorder job both read that association.
func runAs(bucket string, fn func()) {
	id := goID()
	goroutineBuckets.Store(id, bucket)
	defer goroutineBuckets.Delete(id)
	fn()
}

func goroutineBucket() string {
	if v, ok := goroutineBuckets.Load(goID()); ok {
		return v.(string)
	}
	return ""
}

func staticKey(key string) BucketKeyFunc {
	return func() string { return key }
}

// waitFor waits for ch to be signaled for up to a second, failing the test on
// timeout.
func waitFor(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// recorderJob counts runs per bucket and can block or panic on demand.
type recorderJob struct {
	mu      sync.Mutex
	runs    map[string]int
	panics  map[string]int // bucket -> number of initial runs that panic
	entered chan string
	signal  chan struct{} // closed once on the first run entry
	once    sync.Once
	release map[string]chan struct{}
}

func newRecorderJob() *recorderJob {
	return &recorderJob{
		runs:    map[string]int{},
		panics:  map[string]int{},
		entered: make(chan string, 128),
		signal:  make(chan struct{}),
		release: map[string]chan struct{}{},
	}
}

func (j *recorderJob) block(bucket string) chan struct{} {
	ch := make(chan struct{})
	j.release[bucket] = ch
	return ch
}

func (j *recorderJob) failFirst(bucket string, n int) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.panics[bucket] = n
}

func (j *recorderJob) Run() {
	bucket := goroutineBucket()

	j.mu.Lock()
	j.runs[bucket]++
	run := j.runs[bucket]
	panicCount := j.panics[bucket]
	j.mu.Unlock()

	select {
	case j.entered <- bucket:
	default:
	}
	j.once.Do(func() { close(j.signal) })

	if ch, ok := j.release[bucket]; ok && ch != nil {
		<-ch
	}

	if run <= panicCount {
		panic(fmt.Sprintf("forced panic in bucket %s run %d", bucket, run))
	}
}

// drainEntered discards buffered entry notifications.
func drainEntered(ch <-chan string) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// waitEnter waits for a run of bucket to be entered.
func waitEnter(t *testing.T, ch <-chan string, bucket string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != bucket {
			t.Fatalf("expected bucket %s to enter, got %s", bucket, got)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for bucket %s to enter", bucket)
	}
}

func (j *recorderJob) runsOf(bucket string) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.runs[bucket]
}

// eventLogger records Info calls for ordering assertions.
type eventLogger struct {
	mu    sync.Mutex
	infos []string
}

func (l *eventLogger) Info(msg string, kv ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.infos = append(l.infos, fmt.Sprintf("%s %v", msg, kv))
}

func (l *eventLogger) Error(err error, msg string, kv ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.infos = append(l.infos, fmt.Sprintf("error %s %v %v", msg, err, kv))
}

func (l *eventLogger) infosCopy() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.infos))
	copy(out, l.infos)
	return out
}

// Acceptance: two goroutines released by a barrier both trigger bucket A, and
// the underlying job runs exactly once.
func TestSkipByBucketBarrierSameBucketRunsOnce(t *testing.T) {
	job := newRecorderJob()
	wrapped := NewChain(SkipByBucket(goroutineBucket)).Then(job)
	block := job.block("A")

	start := make(chan struct{})
	returned := make(chan struct{}, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			runAs("A", wrapped.Run)
			returned <- struct{}{}
		}()
	}
	close(start)

	waitFor(t, job.signal, "job to enter once")
	waitFor(t, returned, "losing goroutine to be skipped and return")

	if got := job.runsOf("A"); got != 1 {
		t.Fatalf("expected underlying job to run once, got %d", got)
	}

	close(block)
	wg.Wait()

	// After the successful run, another A is a completed-bucket skip.
	runAs("A", wrapped.Run)
	if got := job.runsOf("A"); got != 1 {
		t.Fatalf("expected A to stay at one run after completion, got %d", got)
	}
}

// Different buckets are independent and both run.
func TestSkipByBucketDifferentBucketsIndependent(t *testing.T) {
	job := newRecorderJob()
	wrapped := NewChain(SkipByBucket(goroutineBucket)).Then(job)
	runAs("A", wrapped.Run)
	runAs("B", wrapped.Run)
	if got := job.runsOf("A"); got != 1 {
		t.Errorf("A runs = %d, want 1", got)
	}
	if got := job.runsOf("B"); got != 1 {
		t.Errorf("B runs = %d, want 1", got)
	}
}

// Acceptance: after the first run of A panics, the error callback fires and
// the second A is allowed to run (and succeeds). Callback ordering is fixed:
// the error handler is invoked before the failed invocation returns.
func TestSkipByBucketRetriesAfterPanic(t *testing.T) {
	job := newRecorderJob()
	job.failFirst("A", 1)

	var (
		mu     sync.Mutex
		events []string
	)
	wrapped := NewChain(SkipByBucket(goroutineBucket,
		WithBucketErrorHandler(func(bucket string, err error) {
			mu.Lock()
			events = append(events, "error:"+bucket+":"+err.Error())
			mu.Unlock()
		}),
	)).Then(job)

	runAs("A", func() {
		wrapped.Run() // panic recovered, bucket forgotten
		mu.Lock()
		events = append(events, "after-first")
		mu.Unlock()
		wrapped.Run() // retry allowed
	})
	mu.Lock()
	events = append(events, "done")
	mu.Unlock()

	if got := job.runsOf("A"); got != 2 {
		t.Fatalf("expected A to run twice after panic retry, got %d", got)
	}
	want := []string{
		"error:A:forced panic in bucket A run 1",
		"after-first",
		"done",
	}
	mu.Lock()
	defer mu.Unlock()
	if len(events) != len(want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("event %d = %q, want %q (full: %v)", i, events[i], want[i], events)
		}
	}
}

// Skips observe a defined reason order: "running" while in flight, then
// "completed" after success.
func TestSkipByBucketSkipReasonOrder(t *testing.T) {
	job := newRecorderJob()
	logger := &eventLogger{}
	wrapped := NewChain(SkipByBucketWith(logger, goroutineBucket)).Then(job)
	block := job.block("A")

	go runAs("A", wrapped.Run)
	waitEnter(t, job.entered, "A")

	runAs("A", wrapped.Run) // skipped: running
	close(block)

	// Poll until the successful run is observed as a completed skip.
	completed := "skip [bucket A reason completed]"
	deadline := time.Now().Add(time.Second)
	for {
		runAs("A", wrapped.Run)
		infos := logger.infosCopy()
		if len(infos) > 0 && infos[len(infos)-1] == completed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected a completed skip, got %v", logger.infosCopy())
		}
		time.Sleep(time.Millisecond)
	}

	infos := logger.infosCopy()
	running := "skip [bucket A reason running]"
	if infos[0] != running {
		t.Fatalf("first skip = %q, want %q (full: %v)", infos[0], running, infos)
	}
	found := false
	for _, info := range infos {
		if info == completed {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("no completed skip observed: %v", infos)
	}
	if got := job.runsOf("A"); got != 1 {
		t.Fatalf("A runs = %d, want 1", got)
	}
}

// Acceptance: with a completed-bucket quota of 2, an old completed bucket is
// evicted while the in-flight bucket is never evicted and deduplicates once
// it completes.
func TestSkipByBucketEvictionKeepsInFlight(t *testing.T) {
	job := newRecorderJob()
	wrapped := NewChain(SkipByBucketWith(DiscardLogger, goroutineBucket,
		WithCompletedBucketLimit(2),
	)).Then(job)

	// Fill the quota with A and B.
	runAs("A", wrapped.Run)
	runAs("B", wrapped.Run)

	// C starts and stays in flight.
	drainEntered(job.entered)
	cBlock := job.block("C")
	go runAs("C", wrapped.Run)
	waitEnter(t, job.entered, "C")

	// D completes while C is in flight: the oldest completed bucket (A) is
	// evicted; in-flight C is untouched.
	runAs("D", wrapped.Run)

	// Re-running evicted A runs again; in-flight C is unaffected.
	runAs("A", wrapped.Run)
	if got := job.runsOf("A"); got != 2 {
		t.Fatalf("evicted A should run again, got %d runs", got)
	}
	if got := job.runsOf("C"); got != 1 {
		t.Fatalf("in-flight C should have exactly one run, got %d", got)
	}

	// Let C finish; it must be remembered as a completed bucket and skipped.
	close(cBlock)
	deadline := time.Now().Add(time.Second)
	for {
		runAs("C", wrapped.Run)
		if got := job.runsOf("C"); got == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("C not deduped after completion: %d runs", job.runsOf("C"))
		}
		time.Sleep(time.Millisecond)
	}
	if got := job.runsOf("C"); got != 1 {
		t.Fatalf("completed C should skip, got %d runs", got)
	}
}

// Invalid limits are rejected.
func TestSkipByBucketRejectsBadLimit(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for zero limit")
		}
	}()
	SkipByBucketWith(DiscardLogger, staticKey("A"), WithCompletedBucketLimit(0))
}

// Race test: a barrier burst of goroutines over several buckets runs the
// underlying job exactly once per bucket. Intended to run with -race.
func TestSkipByBucketConcurrentRace(t *testing.T) {
	job := newRecorderJob()
	wrapped := NewChain(SkipByBucketWith(DiscardLogger, goroutineBucket)).Then(job)

	const perBucket = 16
	buckets := []string{"A", "B", "C"}
	blocks := map[string]chan struct{}{}
	for _, b := range buckets {
		ch := make(chan struct{})
		blocks[b] = ch
		job.release[b] = ch
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, b := range buckets {
		for i := 0; i < perBucket; i++ {
			wg.Add(1)
			go func(bucket string) {
				defer wg.Done()
				<-start
				runAs(bucket, wrapped.Run)
			}(b)
		}
	}
	close(start)

	got := map[string]bool{}
	for i := 0; i < len(buckets); i++ {
		select {
		case b := <-job.entered:
			got[b] = true
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for a bucket to enter; got %v", got)
		}
	}
	for _, ch := range blocks {
		close(ch)
	}
	wg.Wait()

	for _, b := range buckets {
		if n := job.runsOf(b); n != 1 {
			t.Errorf("bucket %s ran %d times, want 1", b, n)
		}
	}
}

// ExampleSkipByBucketWith is a short counting demo: the first invocation for a
// caller-supplied bucket runs; duplicate invocations of that bucket are
// skipped. Buckets are provided explicitly, never inferred from sleep.
func ExampleSkipByBucketWith() {
	var runs int
	bucket := "2026-10-03T10:00"
	base := FuncJob(func() { runs++ })
	wrapped := NewChain(SkipByBucketWith(
		PrintfLogger(log.New(ioutil.Discard, "", 0)),
		func() string { return bucket },
	)).Then(base)

	wrapped.Run() // first trigger for the bucket: runs
	wrapped.Run() // duplicate trigger for the same bucket: skipped
	fmt.Println("underlying runs:", runs)
	// Output: underlying runs: 1
}

package cron

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// blockingJob counts invocations and blocks each one on its own release
// channel, recording the maximum concurrency observed.
type blockingJob struct {
	mu      sync.Mutex
	started int
	done    int
	cur     int
	maxConc int
	release chan struct{}
}

func (j *blockingJob) Run() {
	rel := make(chan struct{})
	j.mu.Lock()
	j.started++
	j.cur++
	if j.cur > j.maxConc {
		j.maxConc = j.cur
	}
	j.release = rel
	j.mu.Unlock()

	<-rel

	j.mu.Lock()
	j.release = nil
	j.cur--
	j.done++
	j.mu.Unlock()
}

// Release unblocks the currently running invocation. It is a no-op if no
// invocation is running.
func (j *blockingJob) Release() {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.release != nil {
		close(j.release)
	}
}

func (j *blockingJob) Started() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.started
}

func (j *blockingJob) Done() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.done
}

func (j *blockingJob) MaxConcurrency() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.maxConc
}

// newCoalesce returns the wrapper asserted to the exported CoalescedJob
// interface so tests can call Close.
func newCoalesce(j Job) CoalescedJob {
	return CoalesceIfStillRunning(DiscardLogger)(j).(CoalescedJob)
}

// waitStarted waits until the job has started at least n times.
func waitStarted(t *testing.T, j *blockingJob, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if j.Started() >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d starts, got %d", n, j.Started())
}

func TestCoalesceIfStillRunning(t *testing.T) {
	t.Run("runs immediately", func(t *testing.T) {
		var j countJob
		wrappedJob := NewChain(CoalesceIfStillRunning(DiscardLogger)).Then(&j)
		go wrappedJob.Run()
		time.Sleep(2 * time.Millisecond)
		if c := j.Done(); c != 1 {
			t.Errorf("expected job run once, immediately, got %d", c)
		}
	})

	t.Run("second run immediate if first done", func(t *testing.T) {
		var j countJob
		wrappedJob := NewChain(CoalesceIfStillRunning(DiscardLogger)).Then(&j)
		go func() {
			go wrappedJob.Run()
			time.Sleep(time.Millisecond)
			go wrappedJob.Run()
		}()
		time.Sleep(3 * time.Millisecond)
		if c := j.Done(); c != 2 {
			t.Errorf("expected job run twice, immediately, got %d", c)
		}
	})

	t.Run("coalesces concurrent triggers into one follow-up", func(t *testing.T) {
		j := &blockingJob{}
		cj := newCoalesce(j)
		done := make(chan struct{})
		go func() { cj.Run(); close(done) }()
		waitStarted(t, j, 1)

		// Five triggers while the first invocation is running are coalesced.
		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); cj.Run() }()
		}
		wg.Wait()

		// Release the first invocation: exactly one follow-up should run.
		j.Release()
		waitStarted(t, j, 2)
		j.Release()
		<-done

		if got := j.Done(); got != 2 {
			t.Errorf("expected 2 total runs, got %d", got)
		}
		if got := j.MaxConcurrency(); got != 1 {
			t.Errorf("expected max concurrency 1, got %d", got)
		}
	})

	t.Run("coalesces again during follow-up", func(t *testing.T) {
		j := &blockingJob{}
		cj := newCoalesce(j)
		done := make(chan struct{})
		go func() { cj.Run(); close(done) }()
		waitStarted(t, j, 1)

		// Five triggers during the first invocation.
		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); cj.Run() }()
		}
		wg.Wait()

		// Release the first invocation and wait for the follow-up to start.
		j.Release()
		waitStarted(t, j, 2)

		// One more trigger during the follow-up coalesces into another run.
		var wg2 sync.WaitGroup
		wg2.Add(1)
		go func() { defer wg2.Done(); cj.Run() }()
		wg2.Wait()

		// The follow-up and the second follow-up both need releasing.
		j.Release()
		waitStarted(t, j, 3)
		j.Release()
		<-done

		if got := j.Done(); got != 3 {
			t.Errorf("expected 3 total runs, got %d", got)
		}
		if got := j.MaxConcurrency(); got != 1 {
			t.Errorf("expected max concurrency 1, got %d", got)
		}
	})

	t.Run("different jobs independent", func(t *testing.T) {
		var j1, j2 countJob
		j1.delay = 10 * time.Millisecond
		j2.delay = 10 * time.Millisecond
		chain := NewChain(CoalesceIfStillRunning(DiscardLogger))
		wrappedJob1 := chain.Then(&j1)
		wrappedJob2 := chain.Then(&j2)
		for i := 0; i < 11; i++ {
			go wrappedJob1.Run()
			go wrappedJob2.Run()
		}
		time.Sleep(100 * time.Millisecond)
		var (
			done1 = j1.Done()
			done2 = j2.Done()
		)
		if done1 != 2 || done2 != 2 {
			t.Error("expected both jobs executed twice, got", done1, "and", done2)
		}
	})
}

func TestCoalesceClose(t *testing.T) {
	t.Run("rejects new triggers", func(t *testing.T) {
		var runs int32
		job := FuncJob(func() { atomic.AddInt32(&runs, 1) })
		cj := newCoalesce(job)
		cj.Close(true)
		cj.Run()
		cj.Run()
		if got := atomic.LoadInt32(&runs); got != 0 {
			t.Errorf("expected 0 runs after close, got %d", got)
		}
	})

	t.Run("waits for current and coalesced invocations", func(t *testing.T) {
		j := &blockingJob{}
		cj := newCoalesce(j)
		done := make(chan struct{})
		go func() { cj.Run(); close(done) }()
		waitStarted(t, j, 1)

		// A coalesced follow-up is pending.
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); cj.Run() }()
		wg.Wait()

		closed := make(chan struct{})
		go func() { cj.Close(true); close(closed) }()
		time.Sleep(5 * time.Millisecond)
		select {
		case <-closed:
			t.Fatal("Close returned before the running job finished")
		default:
		}

		// Release the first invocation; the follow-up starts and must also
		// be allowed to finish before Close returns.
		j.Release()
		waitStarted(t, j, 2)
		j.Release()
		<-closed
		<-done

		if got := j.Done(); got != 2 {
			t.Errorf("expected 2 runs, got %d", got)
		}
	})

	t.Run("returns immediately when not waiting", func(t *testing.T) {
		j := &blockingJob{}
		cj := newCoalesce(j)
		done := make(chan struct{})
		go func() { cj.Run(); close(done) }()
		waitStarted(t, j, 1)
		closed := make(chan struct{})
		go func() { cj.Close(false); close(closed) }()
		select {
		case <-closed:
		case <-time.After(time.Second):
			t.Fatal("Close(false) blocked")
		}
		// The running job still finishes on its own.
		j.Release()
		<-done
	})

	t.Run("idempotent", func(t *testing.T) {
		var runs int32
		job := FuncJob(func() { atomic.AddInt32(&runs, 1) })
		cj := newCoalesce(job)
		cj.Close(true)
		cj.Close(true)
		cj.Close(false)
		cj.Close(false)
		cj.Run()
		if got := atomic.LoadInt32(&runs); got != 0 {
			t.Errorf("expected 0 runs, got %d", got)
		}
	})
}

// TestCoalesceRecoverComposition verifies that panics are handled by the
// existing Recover composition and do not leave the running flag stuck.
func TestCoalesceRecoverComposition(t *testing.T) {
	t.Run("Recover outside Coalesce", func(t *testing.T) {
		var runs int32
		job := FuncJob(func() {
			atomic.AddInt32(&runs, 1)
			panic("boom")
		})
		cj := newCoalesce(job)
		wrapped := NewChain(Recover(DiscardLogger)).Then(cj)
		for i := 0; i < 3; i++ {
			wrapped.Run()
		}
		if got := atomic.LoadInt32(&runs); got != 3 {
			t.Errorf("expected 3 runs (panic must not stick the running flag), got %d", got)
		}
	})

	t.Run("Recover inside Coalesce", func(t *testing.T) {
		var runs int32
		job := FuncJob(func() {
			atomic.AddInt32(&runs, 1)
			panic("boom")
		})
		cj := newCoalesce(NewChain(Recover(DiscardLogger)).Then(job))
		for i := 0; i < 3; i++ {
			cj.Run()
		}
		if got := atomic.LoadInt32(&runs); got != 3 {
			t.Errorf("expected 3 runs, got %d", got)
		}
	})

	t.Run("accepts new triggers after panic", func(t *testing.T) {
		var runs int32
		panicking := FuncJob(func() {
			atomic.AddInt32(&runs, 1)
			panic("boom")
		})
		cj := newCoalesce(panicking)
		wrapped := NewChain(Recover(DiscardLogger)).Then(cj)
		wrapped.Run()
		wrapped.Run()
		if got := atomic.LoadInt32(&runs); got != 2 {
			t.Errorf("expected 2 runs after panic, got %d", got)
		}
	})
}

// TestCoalesceCloseRace hammers Close and Run concurrently; run with -race.
func TestCoalesceCloseRace(t *testing.T) {
	j := &blockingJob{}
	cj := newCoalesce(j)
	go cj.Run()
	waitStarted(t, j, 1)

	// Wait for all triggers to register before closing, so the coalesced
	// follow-up is guaranteed to happen.
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); cj.Run() }()
	}
	wg.Wait()

	wg.Add(1)
	go func() { defer wg.Done(); cj.Close(true) }()
	time.Sleep(5 * time.Millisecond)
	j.Release() // release the first invocation
	waitStarted(t, j, 2)
	j.Release() // release the coalesced follow-up
	wg.Wait()

	before := j.Started()
	cj.Run()
	cj.Close(true)
	if got := j.Started(); got != before {
		t.Error("Run after close must be ignored")
	}
}

// ExampleCoalesceIfStillRunning demonstrates coalescing: five triggers while
// the first invocation is running merge into one follow-up, and a trigger
// during that follow-up merges into yet another one.
func ExampleCoalesceIfStillRunning() {
	var runs int32
	started := make(chan struct{}, 16)
	release := make(chan struct{})
	job := FuncJob(func() {
		atomic.AddInt32(&runs, 1)
		started <- struct{}{}
		<-release
	})
	cj := CoalesceIfStillRunning(DiscardLogger)(job)
	done := make(chan struct{})
	go func() { cj.Run(); close(done) }()
	<-started
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); cj.Run() }()
	}
	wg.Wait()
	release <- struct{}{}
	<-started
	var wg2 sync.WaitGroup
	wg2.Add(1)
	go func() { defer wg2.Done(); cj.Run() }()
	wg2.Wait()
	release <- struct{}{}
	<-started
	release <- struct{}{}
	<-done
	fmt.Println(atomic.LoadInt32(&runs))
	// Output: 3
}

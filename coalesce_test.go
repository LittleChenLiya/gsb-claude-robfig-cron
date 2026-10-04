package cron

import (
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// gatedJob counts runs and current/max concurrency, and lets each run be
// blocked on an individual gate channel.
type gatedJob struct {
	mu     sync.Mutex
	runs   int
	cur    int
	maxCur int
	gates  map[int]chan struct{} // run n (1-based) blocks until gates[n] is closed
	notify map[int]chan struct{} // notify[n] is closed when run n starts
}

func (j *gatedJob) Run() {
	j.mu.Lock()
	j.runs++
	j.cur++
	if j.cur > j.maxCur {
		j.maxCur = j.cur
	}
	n := j.runs
	if ch, ok := j.notify[n]; ok {
		close(ch)
	}
	j.mu.Unlock()

	if gate, ok := j.gates[n]; ok {
		<-gate
	}

	j.mu.Lock()
	j.cur--
	j.mu.Unlock()
}

func (j *gatedJob) stats() (runs, maxCur int) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.runs, j.maxCur
}

func TestCoalesceMergesTriggers(t *testing.T) {
	c := NewCoalescer(DiscardLogger)
	j := &gatedJob{
		gates:  map[int]chan struct{}{1: make(chan struct{})},
		notify: map[int]chan struct{}{1: make(chan struct{}), 2: make(chan struct{})},
	}
	wrapped := c.Wrap(j)

	go wrapped.Run()
	<-j.notify[1] // first run is now blocked on its gate

	// Five triggers while the first run is blocked: all coalesce into one
	// pending follow-up run instead of queueing or running concurrently.
	for i := 0; i < 5; i++ {
		wrapped.Run()
	}
	close(j.gates[1]) // release the first run

	<-j.notify[2] // the single coalesced follow-up run started
	c.Stop(true)  // wait for it to finish

	runs, maxCur := j.stats()
	if runs != 2 {
		t.Errorf("expected 2 runs (initial + 1 coalesced), got %d", runs)
	}
	if maxCur != 1 {
		t.Errorf("expected max concurrency 1, got %d", maxCur)
	}
}

func TestCoalesceTriggerDuringFollowUp(t *testing.T) {
	c := NewCoalescer(DiscardLogger)
	j := &gatedJob{
		gates: map[int]chan struct{}{
			1: make(chan struct{}),
			2: make(chan struct{}),
		},
		notify: map[int]chan struct{}{
			1: make(chan struct{}),
			2: make(chan struct{}),
			3: make(chan struct{}),
		},
	}
	wrapped := c.Wrap(j)

	go wrapped.Run()
	<-j.notify[1]

	// Triggers during the first run coalesce into one follow-up run.
	for i := 0; i < 3; i++ {
		wrapped.Run()
	}
	close(j.gates[1])

	<-j.notify[2] // coalesced follow-up run is now blocked on its gate

	// A trigger during the follow-up run coalesces into yet another run.
	wrapped.Run()
	close(j.gates[2])

	<-j.notify[3]
	c.Stop(true)

	runs, maxCur := j.stats()
	if runs != 3 {
		t.Errorf("expected 3 runs, got %d", runs)
	}
	if maxCur != 1 {
		t.Errorf("expected max concurrency 1, got %d", maxCur)
	}
}

func TestCoalescePanic(t *testing.T) {
	logger := PrintfLogger(log.New(ioutil.Discard, "", 0))

	t.Run("state not stuck after panic, new triggers accepted", func(t *testing.T) {
		c := NewCoalescer(DiscardLogger)
		var mu sync.Mutex
		var runs int
		job := FuncJob(func() {
			mu.Lock()
			runs++
			n := runs
			mu.Unlock()
			if n == 1 {
				panic("boom")
			}
		})
		// Compose with the existing Recover wrapper, as documented.
		wrapped := NewChain(Recover(logger), c.Wrap).Then(job)

		wrapped.Run() // panics; recovered by Recover
		mu.Lock()
		if runs != 1 {
			t.Fatalf("expected 1 run, got %d", runs)
		}
		mu.Unlock()

		// The running mark must not be stuck: a new trigger runs the job.
		wrapped.Run()
		mu.Lock()
		if runs != 2 {
			t.Errorf("expected job to run again after panic, got %d runs", runs)
		}
		mu.Unlock()
	})

	t.Run("pending trigger dropped on panic, later triggers still work", func(t *testing.T) {
		c := NewCoalescer(DiscardLogger)
		var mu sync.Mutex
		var runs int
		gate := make(chan struct{})
		entered := make(chan struct{})
		job := FuncJob(func() {
			mu.Lock()
			runs++
			n := runs
			mu.Unlock()
			if n == 1 {
				close(entered)
				<-gate
				panic("boom")
			}
		})
		wrapped := NewChain(Recover(logger), c.Wrap).Then(job)

		go wrapped.Run()
		<-entered
		wrapped.Run() // coalesced into pending while run 1 is blocked
		close(gate)   // run 1 panics; pending must be dropped, state reset

		// Wait for the panicking run to unwind.
		deadline := time.Now().Add(time.Second)
		for {
			c.mu.Lock()
			idle := true
			for _, s := range c.states {
				s.mu.Lock()
				if s.running || s.pending {
					idle = false
				}
				s.mu.Unlock()
			}
			c.mu.Unlock()
			if idle {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("coalescer state stuck after panic")
			}
			time.Sleep(time.Millisecond)
		}

		// The stale pending trigger was dropped: only one run so far.
		mu.Lock()
		if runs != 1 {
			t.Fatalf("expected 1 run after panic, got %d", runs)
		}
		mu.Unlock()

		// New triggers are still accepted.
		wrapped.Run()
		mu.Lock()
		if runs != 2 {
			t.Errorf("expected job to run again after panic, got %d runs", runs)
		}
		mu.Unlock()
	})
}

func TestCoalesceStop(t *testing.T) {
	c := NewCoalescer(DiscardLogger)
	j := &gatedJob{
		gates:  map[int]chan struct{}{1: make(chan struct{})},
		notify: map[int]chan struct{}{1: make(chan struct{})},
	}
	wrapped := c.Wrap(j)

	go wrapped.Run()
	<-j.notify[1]

	wrapped.Run() // coalesced into one pending follow-up run

	c.Stop(false) // close: no new triggers accepted from now on

	// Triggers after Stop are rejected.
	for i := 0; i < 10; i++ {
		wrapped.Run()
	}

	close(j.gates[1])
	// Stop(true) waits for the in-flight run and the already-coalesced
	// follow-up run to complete.
	c.Stop(true)

	runs, _ := j.stats()
	if runs != 2 {
		t.Errorf("expected current run + 1 coalesced run to complete, got %d runs", runs)
	}

	// After Stop, triggers remain no-ops.
	wrapped.Run()
	if runs, _ := j.stats(); runs != 2 {
		t.Errorf("expected no runs after Stop, got %d", runs)
	}
}

func TestCoalesceStopConcurrentTriggers(t *testing.T) {
	c := NewCoalescer(DiscardLogger)
	wrapped := c.Wrap(FuncJob(func() {
		time.Sleep(time.Millisecond)
	}))

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				wrapped.Run()
			}
		}()
	}
	// Race Stop with in-flight triggers; must not deadlock or corrupt state.
	time.Sleep(5 * time.Millisecond)
	c.Stop(false)
	wg.Wait()
	c.Stop(true)
}

func TestCoalesceDifferentJobsIndependent(t *testing.T) {
	c := NewCoalescer(DiscardLogger)
	j1 := &gatedJob{
		gates:  map[int]chan struct{}{1: make(chan struct{})},
		notify: map[int]chan struct{}{1: make(chan struct{})},
	}
	j2 := &gatedJob{
		gates:  map[int]chan struct{}{1: make(chan struct{})},
		notify: map[int]chan struct{}{1: make(chan struct{})},
	}
	wrapped1 := c.Wrap(j1)
	wrapped2 := c.Wrap(j2)

	go wrapped1.Run()
	go wrapped2.Run()
	<-j1.notify[1]
	<-j2.notify[1]
	wrapped1.Run()
	wrapped2.Run()
	close(j1.gates[1])
	close(j2.gates[1])
	c.Stop(true)

	runs1, _ := j1.stats()
	runs2, _ := j2.stats()
	if runs1 != 2 || runs2 != 2 {
		t.Errorf("expected both jobs to run twice, got %d and %d", runs1, runs2)
	}
}

// ExampleCoalescer demonstrates merging rapid triggers into one follow-up
// run, in contrast to SkipIfStillRunning which would drop them entirely.
func ExampleCoalescer() {
	coalescer := NewCoalescer(DiscardLogger)
	var runs int32
	started := make(chan struct{})
	var once sync.Once
	job := FuncJob(func() {
		atomic.AddInt32(&runs, 1)
		once.Do(func() { close(started) })
		time.Sleep(50 * time.Millisecond)
	})
	wrapped := coalescer.Wrap(job)

	go wrapped.Run() // first run, takes 50ms
	<-started
	wrapped.Run() // coalesced into a pending follow-up run
	wrapped.Run() // merged into the same pending run
	coalescer.Stop(true)

	fmt.Println(atomic.LoadInt32(&runs))
	// Output: 2
}

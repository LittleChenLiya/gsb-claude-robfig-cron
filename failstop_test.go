package cron

// Targeted tests for the FailStop job wrapper.
//
// Run the whole suite with:
//
//	go test -run TestFailStop -v
//
// Run a single test with, e.g.:
//
//	go test -run TestFailStop_GenerationInterleaving -v
//
// State description: FailStop is a per-job circuit breaker. A job starts
// active with a consecutive-failure count of 0. Each run is followed by the
// caller-supplied result function; a failure increments the count and a
// success resets it to 0. When the count reaches the threshold the job is
// disabled: later triggers skip the job (emitting the onSkip event) without
// running it, while the cron schedule and all other entries are unaffected.
// Recovery is explicit only: Reset() re-enables the job, clears the count,
// and bumps the generation. Every run captures the generation at start; a
// late completion from an old generation never modifies the new count.

import (
	"io/ioutil"
	"log"
	"sync"
	"testing"
	"time"
)

// failJob records how many times it ran and reports failure through the
// caller-supplied result function.
type failJob struct {
	mu         sync.Mutex
	ran        int
	failed     bool
	release    chan struct{} // if non-nil, Run blocks until closed
	blockFirst bool          // if true, only the first run blocks on release
}

func (j *failJob) Run() {
	j.mu.Lock()
	j.ran++
	run := j.ran
	j.mu.Unlock()
	if j.release != nil && (!j.blockFirst || run == 1) {
		<-j.release
	}
}

func (j *failJob) Ran() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.ran
}

func (j *failJob) Failed() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.failed
}

func (j *failJob) SetFailed(v bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.failed = v
}

// skipCounter counts skip events.
type skipCounter struct {
	mu sync.Mutex
	n  int
	ch chan struct{}
}

func (s *skipCounter) skip() {
	s.mu.Lock()
	s.n++
	s.mu.Unlock()
	if s.ch != nil {
		select {
		case s.ch <- struct{}{}:
		default:
		}
	}
}

func (s *skipCounter) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}

func TestFailStop_DisablesAfterThreshold(t *testing.T) {
	var job failJob
	job.SetFailed(true)
	var skips skipCounter
	fs := NewFailStop(2, job.Failed, skips.skip)
	wrapped := fs.Wrap(&job)

	wrapped.Run()
	wrapped.Run()
	count, disabled, _ := fs.State()
	if count != 2 || !disabled {
		t.Fatalf("expected count 2 and disabled, got count %d disabled %v", count, disabled)
	}
	if job.Ran() != 2 {
		t.Fatalf("expected job ran twice, got %d", job.Ran())
	}

	// A third trigger must skip the job and emit the independent skip event.
	wrapped.Run()
	if job.Ran() != 2 {
		t.Errorf("expected job not run while disabled, got %d runs", job.Ran())
	}
	if skips.count() != 1 {
		t.Errorf("expected 1 skip event, got %d", skips.count())
	}
	count, disabled, _ = fs.State()
	if count != 2 || !disabled {
		t.Errorf("expected count still 2 and disabled, got count %d disabled %v", count, disabled)
	}
}

func TestFailStop_SuccessResetsCount(t *testing.T) {
	var job failJob
	var skips skipCounter
	fs := NewFailStop(2, job.Failed, skips.skip)
	wrapped := fs.Wrap(&job)

	job.SetFailed(true)
	wrapped.Run() // count 1
	job.SetFailed(false)
	wrapped.Run() // success -> count 0
	job.SetFailed(true)
	wrapped.Run() // count 1 again, not disabled

	count, disabled, _ := fs.State()
	if count != 1 || disabled {
		t.Fatalf("expected count 1 and active, got count %d disabled %v", count, disabled)
	}
	if skips.count() != 0 {
		t.Errorf("expected no skips, got %d", skips.count())
	}
}

func TestFailStop_ResetClearsCountAndStartsNewGeneration(t *testing.T) {
	var job failJob
	job.SetFailed(true)
	var skips skipCounter
	fs := NewFailStop(2, job.Failed, skips.skip)
	wrapped := fs.Wrap(&job)

	wrapped.Run()
	wrapped.Run()
	_, disabled, gen := fs.State()
	if !disabled {
		t.Fatalf("expected disabled after 2 failures")
	}

	// Explicit recovery: count 0, active, new generation.
	fs.Reset()
	count, disabled, newGen := fs.State()
	if count != 0 || disabled {
		t.Fatalf("expected count 0 and active after reset, got count %d disabled %v", count, disabled)
	}
	if newGen != gen+1 {
		t.Fatalf("expected generation %d after reset, got %d", gen+1, newGen)
	}

	// A successful run after recovery keeps the count at 0.
	job.SetFailed(false)
	wrapped.Run()
	count, disabled, _ = fs.State()
	if count != 0 || disabled {
		t.Fatalf("expected count 0 and active after successful recovery run, got count %d disabled %v", count, disabled)
	}
	if skips.count() != 0 {
		t.Errorf("expected no skips after recovery, got %d", skips.count())
	}
}

// TestFailStop_GenerationInterleaving verifies, over channels, that a late
// completion from an old generation never modifies the new count:
//
//	gen 0 run starts (blocked) -> Reset bumps generation -> old run completes
//	late -> new generation runs. The late old result must be ignored in both
//	directions (old failure must not add to the new count; old success must
//	not reset it).
func TestFailStop_GenerationInterleaving(t *testing.T) {
	t.Run("late old failure does not add to new count", func(t *testing.T) {
		var job failJob
		job.SetFailed(true)
		job.release = make(chan struct{})
		fs := NewFailStop(2, job.Failed, nil)
		wrapped := fs.Wrap(&job)

		// Start a generation-0 run and keep it in flight.
		go wrapped.Run()
		waitUntil(t, func() bool { return job.Ran() == 1 })

		// Recover explicitly while the old run is still in flight.
		fs.Reset()
		_, disabled, gen := fs.State()
		if disabled || gen != 1 {
			t.Fatalf("expected active generation 1 after reset, got disabled %v gen %d", disabled, gen)
		}

		// Let the old run complete late. Its failure must be ignored.
		close(job.release)
		waitUntil(t, func() bool {
			count, _, _ := fs.State()
			return count == 0
		})

		// A new-generation failure counts once; the ignored old failure must
		// not stack on top of it and trip the job.
		job.release = nil
		wrapped.Run()
		count, disabled, _ := fs.State()
		if count != 1 || disabled {
			t.Fatalf("expected new count 1 and active, got count %d disabled %v", count, disabled)
		}
	})

	t.Run("late old success does not reset new count", func(t *testing.T) {
		var job failJob
		job.SetFailed(false) // old run will succeed
		job.release = make(chan struct{})
		job.blockFirst = true // only the old (first) run blocks; new runs proceed
		fs := NewFailStop(2, job.Failed, nil)
		wrapped := fs.Wrap(&job)

		go wrapped.Run()
		waitUntil(t, func() bool { return job.Ran() == 1 })

		fs.Reset() // generation 1, count 0

		// New generation fails once; it must proceed while the old run is
		// still blocked.
		job.SetFailed(true)
		wrapped.Run()
		if count, _, _ := fs.State(); count != 1 {
			t.Fatalf("expected new count 1, got %d", count)
		}

		// Old run completes late with a success; it must not zero the new
		// count.
		close(job.release)
		waitUntil(t, func() bool {
			count, _, _ := fs.State()
			return count == 1
		})
		count, disabled, _ := fs.State()
		if count != 1 || disabled {
			t.Fatalf("expected new count to stay 1 and active, got count %d disabled %v", count, disabled)
		}
	})
}

// TestFailStop_PanicWithRecover verifies that FailStop composes with Recover
// without getting state stuck: a panic is still reported through the result
// function supplied by the caller, the mutex is released, and the job can
// trip and recover normally afterwards.
func TestFailStop_PanicWithRecover(t *testing.T) {
	panicking := FuncJob(func() {
		panic("boom")
	})

	t.Run("recover outside failstop", func(t *testing.T) {
		var failed bool
		var skips skipCounter
		fs := NewFailStop(2, func() bool { return failed }, skips.skip)
		// Recover is outermost: it catches the panic after FailStop records
		// the result.
		wrapped := NewChain(Recover(PrintfLogger(log.New(ioutil.Discard, "", 0)))).Then(fs.Wrap(panicking))

		failed = true
		wrapped.Run()
		count, disabled, _ := fs.State()
		if count != 1 || disabled {
			t.Fatalf("expected count 1 and active after panic, got count %d disabled %v", count, disabled)
		}

		wrapped.Run()
		count, disabled, _ = fs.State()
		if count != 2 || !disabled {
			t.Fatalf("expected count 2 and disabled after two panics, got count %d disabled %v", count, disabled)
		}

		// Disabled: trigger is skipped, job does not run.
		wrapped.Run()
		if skips.count() != 1 {
			t.Errorf("expected 1 skip event, got %d", skips.count())
		}

		// Explicit recovery unsticks the state.
		fs.Reset()
		failed = false
		wrapped.Run()
		count, disabled, _ = fs.State()
		if count != 0 || disabled {
			t.Fatalf("expected count 0 and active after reset, got count %d disabled %v", count, disabled)
		}
	})

	t.Run("recover inside failstop", func(t *testing.T) {
		var failed bool
		fs := NewFailStop(2, func() bool { return failed }, nil)
		// Recover is innermost: it catches the panic and FailStop sees a
		// normal return; state must remain consistent.
		wrapped := fs.Wrap(NewChain(Recover(PrintfLogger(log.New(ioutil.Discard, "", 0)))).Then(panicking))

		failed = true
		wrapped.Run()
		wrapped.Run()
		_, disabled, _ := fs.State()
		if !disabled {
			t.Fatalf("expected disabled after two recovered panics")
		}
		fs.Reset()
		count, disabled, _ := fs.State()
		if count != 0 || disabled {
			t.Fatalf("expected count 0 and active after reset, got count %d disabled %v", count, disabled)
		}
	})
}

// intervalSchedule returns times at a fixed real-time interval. It never
// blocks, so the cron scheduler keeps running entries on its own.
type intervalSchedule struct {
	interval time.Duration
}

func (s *intervalSchedule) Next(t time.Time) time.Time {
	return t.Add(s.interval)
}

// TestFailStop_CronKeepsEntriesAndSkips verifies at the cron level that a
// disabled job is skipped (with its own event) while the schedule and all
// other entries keep running.
func TestFailStop_CronKeepsEntriesAndSkips(t *testing.T) {
	var job1, job2 failJob
	job1.SetFailed(true)
	var skips skipCounter
	skips.ch = make(chan struct{}, 8)

	fs := NewFailStop(2, job1.Failed, skips.skip)
	c := New(
		WithChain(Recover(DiscardLogger)),
		WithLogger(DiscardLogger),
	)

	const interval = 10 * time.Millisecond
	id1 := c.Schedule(&intervalSchedule{interval}, fs.Wrap(&job1))
	c.Schedule(&intervalSchedule{interval}, &job2)
	c.Start()
	defer c.Stop()

	// job1 fails on its first two runs and trips; the next trigger is
	// skipped and emits the independent skip event.
	select {
	case <-skips.ch:
	case <-time.After(time.Second):
		t.Fatal("expected skip event for disabled job")
	}
	if job1.Ran() != 2 {
		t.Errorf("expected job1 to run exactly 2 times before tripping, got %d", job1.Ran())
	}
	// job2 kept running while job1 was disabled.
	if job2.Ran() < 3 {
		t.Errorf("expected job2 to keep running, got %d runs", job2.Ran())
	}

	// Both entries are still present: disabling does not remove entries.
	if entries := c.Entries(); len(entries) != 2 {
		t.Fatalf("expected 2 entries after disabling, got %d", len(entries))
	}
	if got := c.Entry(id1); !got.Valid() {
		t.Errorf("expected disabled entry to remain, got %+v", got)
	}

	// Explicit recovery: job1 runs again, count 0.
	fs.Reset()
	job1.SetFailed(false)
	ranBefore := job1.Ran()
	waitUntil(t, func() bool { return job1.Ran() > ranBefore })
	count, disabled, _ := fs.State()
	if count != 0 || disabled {
		t.Fatalf("expected count 0 and active after recovery, got count %d disabled %v", count, disabled)
	}
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met before deadline")
}

package cron

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeTimer is a controllable cronTimer: nothing fires until the test sends
// a value on the channel, so a due signal can be held back and released at a
// chosen moment.
type fakeTimer struct {
	ch      chan time.Time
	stopped int32
}

func (f *fakeTimer) C() <-chan time.Time { return f.ch }

func (f *fakeTimer) Stop() bool {
	atomic.AddInt32(&f.stopped, 1)
	return true
}

// fakeTimerFactory records every timer the scheduler arms.
type fakeTimerFactory struct {
	mu     sync.Mutex
	timers []*fakeTimer
}

func (f *fakeTimerFactory) newTimer(time.Duration) cronTimer {
	t := &fakeTimer{ch: make(chan time.Time, 1)}
	f.mu.Lock()
	f.timers = append(f.timers, t)
	f.mu.Unlock()
	return t
}

func (f *fakeTimerFactory) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.timers)
}

func (f *fakeTimerFactory) get(i int) *fakeTimer {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.timers[i]
}

func waitForCondition(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func assertNoRun(t *testing.T, runs <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-runs:
		t.Fatalf("%s: job ran unexpectedly", what)
	case <-time.After(200 * time.Millisecond):
	}
}

func assertOneRun(t *testing.T, runs <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-runs:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: job did not run", what)
	}
}

// The stale due signal of the old schedule, released after the new schedule
// has been published, must not run the job; the new schedule fires it once.
func TestUpdateScheduleStaleTimerSignalDiscarded(t *testing.T) {
	factory := &fakeTimerFactory{}
	c := New()
	c.newTimer = factory.newTimer

	runs := make(chan struct{}, 4)
	id := c.Schedule(Every(time.Hour), FuncJob(func() { runs <- struct{}{} }))
	c.Start()
	defer c.Stop()

	waitForCondition(t, "first timer to be armed", func() bool { return factory.count() == 1 })
	staleTimer := factory.get(0)

	gen, err := c.UpdateSchedule(id, Every(time.Minute))
	if err != nil {
		t.Fatalf("UpdateSchedule returned error: %v", err)
	}
	if gen == 0 {
		t.Fatal("UpdateSchedule returned a zero generation")
	}
	waitForCondition(t, "timer to be re-armed after update", func() bool { return factory.count() == 2 })

	// Release the held-back due signal of the old schedule's timer.
	staleTimer.ch <- time.Now()
	assertNoRun(t, runs, "stale signal from old schedule")

	// The new schedule's timer fires the job exactly once, on time.
	fireAt := c.Entry(id).Next
	factory.get(1).ch <- fireAt
	assertOneRun(t, runs, "new schedule")
	assertNoRun(t, runs, "extra run after new schedule fired")

	entry := c.Entry(id)
	if entry.Generation != gen {
		t.Errorf("entry generation = %d, want %d", entry.Generation, gen)
	}
	if !entry.Prev.Equal(fireAt) {
		t.Errorf("entry Prev = %v, want the scheduled activation %v", entry.Prev, fireAt)
	}
	if entry.Next.IsZero() {
		t.Error("entry Next is zero after run on the new schedule")
	}
}

// Two consecutive replacements get strictly increasing generation codes, the
// entry snapshot reports the latest code, and stale timers of both previous
// generations are discarded.
func TestUpdateScheduleConsecutiveGenerations(t *testing.T) {
	factory := &fakeTimerFactory{}
	c := New()
	c.newTimer = factory.newTimer

	runs := make(chan struct{}, 4)
	id := c.Schedule(Every(time.Hour), FuncJob(func() { runs <- struct{}{} }))
	c.Start()
	defer c.Stop()
	waitForCondition(t, "first timer to be armed", func() bool { return factory.count() == 1 })

	gen1, err := c.UpdateSchedule(id, Every(2*time.Hour))
	if err != nil {
		t.Fatalf("first update: %v", err)
	}
	gen2, err := c.UpdateSchedule(id, Every(3*time.Hour))
	if err != nil {
		t.Fatalf("second update: %v", err)
	}
	if gen2 != gen1+1 {
		t.Fatalf("generations = %d, %d; want strictly increasing by one", gen1, gen2)
	}
	waitForCondition(t, "timer to be re-armed", func() bool { return factory.count() == 3 })

	// Stale signals from the original timer and the first update's timer.
	factory.get(0).ch <- time.Now()
	factory.get(1).ch <- time.Now()
	assertNoRun(t, runs, "stale signals of generations before the second update")

	entry := c.Entry(id)
	if entry.Generation != gen2 {
		t.Errorf("entry generation = %d, want %d", entry.Generation, gen2)
	}
	if sched, ok := entry.Schedule.(ConstantDelaySchedule); !ok || sched.Delay != 3*time.Hour {
		t.Errorf("entry schedule = %v, want Every(3h)", entry.Schedule)
	}

	factory.get(2).ch <- entry.Next
	assertOneRun(t, runs, "schedule of the second update")
	assertNoRun(t, runs, "extra run")
}

// A schedule with no upcoming activation time, or a nil schedule, is rejected
// and leaves the entry untouched, both while stopped and while running.
func TestUpdateScheduleInvalidNext(t *testing.T) {
	c := New()
	id := c.Schedule(Every(time.Hour), FuncJob(func() {}))

	if _, err := c.UpdateSchedule(id, nil); err != ErrInvalidSchedule {
		t.Errorf("nil schedule: err = %v, want %v", err, ErrInvalidSchedule)
	}
	if _, err := c.UpdateSchedule(id, &ZeroSchedule{}); err != ErrUnsatisfiableSchedule {
		t.Errorf("zero Next schedule: err = %v, want %v", err, ErrUnsatisfiableSchedule)
	}
	entry := c.Entry(id)
	if entry.Generation != 0 {
		t.Errorf("failed update consumed a generation: %d", entry.Generation)
	}
	if _, ok := entry.Schedule.(ConstantDelaySchedule); !ok {
		t.Errorf("failed update replaced the schedule: %v", entry.Schedule)
	}

	c.Start()
	defer c.Stop()
	gen, err := c.UpdateSchedule(id, &ZeroSchedule{})
	if err != ErrUnsatisfiableSchedule {
		t.Errorf("while running: err = %v, want %v", err, ErrUnsatisfiableSchedule)
	}
	if gen != 0 {
		t.Errorf("failed update returned generation %d, want 0", gen)
	}
	if entry := c.Entry(id); entry.Generation != 0 {
		t.Errorf("while running: entry generation = %d, want 0", entry.Generation)
	}
}

// Updating a removed or never-existing entry fails deterministically.
func TestUpdateScheduleEntryNotFound(t *testing.T) {
	c := New()
	if _, err := c.UpdateSchedule(EntryID(999), Every(time.Minute)); err != ErrEntryNotFound {
		t.Errorf("unknown ID: err = %v, want %v", err, ErrEntryNotFound)
	}
	removed := c.Schedule(Every(time.Hour), FuncJob(func() {}))
	c.Remove(removed)
	if _, err := c.UpdateSchedule(removed, Every(time.Minute)); err != ErrEntryNotFound {
		t.Errorf("removed while stopped: err = %v, want %v", err, ErrEntryNotFound)
	}

	c.Start()
	defer c.Stop()
	id := c.Schedule(Every(time.Hour), FuncJob(func() {}))
	c.Remove(id)
	if _, err := c.UpdateSchedule(id, Every(time.Minute)); err != ErrEntryNotFound {
		t.Errorf("removed while running: err = %v, want %v", err, ErrEntryNotFound)
	}
}

// Update racing with Stop has a deterministic result: the entry exists, so
// the update succeeds and the snapshot reports the returned generation,
// regardless of which call wins the race.
func TestUpdateScheduleRacingStop(t *testing.T) {
	for i := 0; i < 10; i++ {
		c := New()
		c.Start()
		id := c.Schedule(Every(time.Hour), FuncJob(func() {}))

		stopped := make(chan struct{})
		go func() {
			c.Stop()
			close(stopped)
		}()

		gen, err := c.UpdateSchedule(id, Every(time.Minute))
		if err != nil {
			t.Fatalf("iteration %d: update racing Stop failed: %v", i, err)
		}
		if gen == 0 {
			t.Fatalf("iteration %d: update racing Stop returned zero generation", i)
		}
		<-stopped
		if entry := c.Entry(id); entry.Generation != gen {
			t.Fatalf("iteration %d: entry generation = %d, want %d", i, entry.Generation, gen)
		}
		c.Stop()
	}
}

// A job already running when its schedule is updated is not cancelled.
func TestUpdateScheduleDoesNotCancelRunningJob(t *testing.T) {
	factory := &fakeTimerFactory{}
	c := New()
	c.newTimer = factory.newTimer

	started := make(chan struct{})
	finish := make(chan struct{})
	finished := make(chan struct{})
	id := c.Schedule(Every(time.Hour), FuncJob(func() {
		close(started)
		<-finish
		close(finished)
	}))
	c.Start()
	defer c.Stop()
	waitForCondition(t, "first timer to be armed", func() bool { return factory.count() == 1 })

	factory.get(0).ch <- c.Entry(id).Next
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("job did not start")
	}

	gen, err := c.UpdateSchedule(id, Every(time.Minute))
	if err != nil {
		t.Fatalf("update while job running: %v", err)
	}
	if gen == 0 {
		t.Fatal("update while job running returned zero generation")
	}

	close(finish)
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("running job did not complete after the update")
	}
}

// Concurrent updates, snapshots, removals and stops must be race-free.
// Run with: go test -race -run TestUpdateScheduleConcurrent
func TestUpdateScheduleConcurrent(t *testing.T) {
	c := New()
	c.Start()

	var ids []EntryID
	for i := 0; i < 5; i++ {
		ids = append(ids, c.Schedule(Every(time.Hour), FuncJob(func() {})))
	}

	var wg sync.WaitGroup
	for _, id := range ids {
		id := id
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_, err := c.UpdateSchedule(id, Every(time.Duration(j+1)*time.Minute))
				if err != nil && err != ErrEntryNotFound {
					t.Errorf("unexpected update error: %v", err)
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 50; j++ {
			for _, entry := range c.Entries() {
				_ = entry.Generation
			}
			for _, id := range ids {
				c.Entry(id)
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Remove(ids[len(ids)-1])
	}()
	wg.Wait()
	c.Stop()
}

// TestUpdateScheduleDemo narrates the core guarantee of UpdateSchedule with a
// controllable timer: the old schedule's held-back due signal is discarded
// once the new schedule is published, and the job then fires exactly on the
// new schedule. It completes in well under 90 seconds; watch it with:
//
//	go test -v -run TestUpdateScheduleDemo
func TestUpdateScheduleDemo(t *testing.T) {
	start := time.Now()
	log := func(format string, args ...interface{}) {
		t.Logf("[%+7.3fs] %s", time.Since(start).Seconds(), fmt.Sprintf(format, args...))
	}

	factory := &fakeTimerFactory{}
	c := New()
	c.newTimer = factory.newTimer

	var mu sync.Mutex
	runs := 0
	id := c.Schedule(Every(time.Hour), FuncJob(func() {
		mu.Lock()
		runs++
		mu.Unlock()
	}))
	runCount := func() int {
		mu.Lock()
		defer mu.Unlock()
		return runs
	}

	c.Start()
	defer c.Stop()
	waitForCondition(t, "old timer to be armed", func() bool { return factory.count() == 1 })
	log("entry %d scheduled on the OLD plan (Every 1h); old timer armed, due signal held back", id)

	gen, err := c.UpdateSchedule(id, Every(time.Minute))
	if err != nil {
		t.Fatalf("UpdateSchedule: %v", err)
	}
	waitForCondition(t, "new timer to be armed", func() bool { return factory.count() == 2 })
	scheduledAt := c.Entry(id).Next
	log("NEW plan published (Every 1m), generation=%d, next=%s", gen, scheduledAt.Format("15:04:05.000"))

	log("releasing the OLD timer's late due signal...")
	factory.get(0).ch <- time.Now()
	time.Sleep(200 * time.Millisecond)
	log("old signal discarded: job runs = %d (want 0)", runCount())
	if runCount() != 0 {
		t.Fatalf("stale signal ran the job; runs = %d", runCount())
	}

	log("firing the NEW plan's timer at the scheduled time %s", scheduledAt.Format("15:04:05.000"))
	factory.get(1).ch <- scheduledAt
	waitForCondition(t, "job to run on the new plan", func() bool { return runCount() == 1 })

	entry := c.Entry(id)
	log("job ran on time: Prev=%s matches scheduled Next=%s, generation=%d",
		entry.Prev.Format("15:04:05.000"), scheduledAt.Format("15:04:05.000"), entry.Generation)
	if !entry.Prev.Equal(scheduledAt) {
		t.Fatalf("job ran at %v, want the scheduled time %v", entry.Prev, scheduledAt)
	}
	if entry.Generation != gen {
		t.Fatalf("entry generation = %d, want %d", entry.Generation, gen)
	}
	if runCount() != 1 {
		t.Fatalf("job runs = %d, want exactly 1", runCount())
	}
	log("done in %.3fs: stale signal discarded, new plan fired exactly once, on time",
		time.Since(start).Seconds())
}

package cron

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeTimer is a controllable scheduleTimer: the test decides when (and if)
// its signal is delivered.
type fakeTimer struct {
	ch      chan time.Time
	stopped int32
}

func (f *fakeTimer) Channel() <-chan time.Time { return f.ch }

func (f *fakeTimer) Stop() bool {
	return atomic.CompareAndSwapInt32(&f.stopped, 0, 1)
}

// installFakeTimers replaces the scheduler's timer factory with one that hands
// out controllable timers, and returns a channel that receives every timer the
// scheduler arms, in order.
func installFakeTimers(t *testing.T) chan *fakeTimer {
	t.Helper()
	timers := make(chan *fakeTimer, 100)
	original := newTimer
	newTimer = func(d time.Duration) scheduleTimer {
		ft := &fakeTimer{ch: make(chan time.Time, 1)}
		timers <- ft
		return ft
	}
	t.Cleanup(func() { newTimer = original })
	return timers
}

// zeroSchedule is unsatisfiable: Next always returns the zero time.
type zeroSchedule struct{}

func (zeroSchedule) Next(time.Time) time.Time { return time.Time{} }

// waitRuns polls until the counter reaches want or the timeout elapses.
func waitRuns(counter *int32, want int32, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(counter) == want {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return atomic.LoadInt32(counter) == want
}

// A stale timer signal from the old schedule, released after a new schedule
// has been published, must not run the job an extra time; the job runs when
// the new schedule's timer fires.
func TestUpdateScheduleDiscardsStaleTimerSignal(t *testing.T) {
	timers := installFakeTimers(t)

	var runs int32
	cron := New()
	id := cron.Schedule(Every(time.Second), FuncJob(func() {
		atomic.AddInt32(&runs, 1)
	}))
	cron.Start()
	defer cron.Stop()

	// The scheduler arms a timer for the original (1s) schedule. Its due
	// signal is held back by the controllable timer.
	oldTimer := <-timers

	// Publish a new schedule while the old signal is still pending.
	rev, err := cron.UpdateSchedule(id, Every(2*time.Second))
	if err != nil {
		t.Fatalf("UpdateSchedule failed: %v", err)
	}
	if rev != 1 {
		t.Errorf("expected revision 1, got %d", rev)
	}

	// The old timer was stopped and a new timer was armed for the new plan.
	if atomic.LoadInt32(&oldTimer.stopped) != 1 {
		t.Error("expected the old timer to be stopped by the update")
	}
	newTimer := <-timers

	entry := cron.Entry(id)
	if entry.Revision != rev {
		t.Errorf("entry revision %d does not match update revision %d", entry.Revision, rev)
	}
	if entry.Schedule != Every(2*time.Second) {
		t.Errorf("entry schedule %v is not the updated schedule", entry.Schedule)
	}
	if entry.Next.IsZero() {
		t.Error("entry Next should be computed from the new schedule")
	}

	// Release the stale signal of the old schedule now that the new schedule
	// is published. It must be discarded: the scheduler no longer listens on
	// the old timer, so nothing may run.
	oldTimer.ch <- time.Now()
	time.Sleep(100 * time.Millisecond)
	if got := atomic.LoadInt32(&runs); got != 0 {
		t.Fatalf("stale timer signal triggered %d extra run(s)", got)
	}

	// Fire the new timer exactly at the scheduled Next: the job runs once.
	newTimer.ch <- entry.Next
	if !waitRuns(&runs, 1, OneSecond) {
		t.Fatal("job did not run on the new schedule")
	}
	if got := atomic.LoadInt32(&runs); got != 1 {
		t.Fatalf("expected exactly 1 run, got %d", got)
	}
}

// Two consecutive replacements for revisions: each update gets an increasing revision
// and Entry queries reflect the latest generation consistently.
func TestUpdateScheduleRevisionIncrements(t *testing.T) {
	timers := installFakeTimers(t)
	cron := New()
	id := cron.Schedule(Every(time.Second), FuncJob(func() {}))
	cron.Start()
	defer cron.Stop()
	<-timers

	rev1, err := cron.UpdateSchedule(id, Every(2*time.Second))
	if err != nil {
		t.Fatalf("first update failed: %v", err)
	}
	rev2, err := cron.UpdateSchedule(id, Every(3*time.Second))
	if err != nil {
		t.Fatalf("second update failed: %v", err)
	}
	if !(rev1 < rev2) {
		t.Errorf("revisions must increase: rev1=%d rev2=%d", rev1, rev2)
	}

	entry := cron.Entry(id)
	if entry.Revision != rev2 {
		t.Errorf("entry revision %d, want %d (latest update)", entry.Revision, rev2)
	}
	if entry.Schedule != Every(3*time.Second) {
		t.Errorf("entry schedule %v, want Every(3s)", entry.Schedule)
	}
	// Next must belong to the same generation as Schedule: roughly 3s out.
	if d := time.Until(entry.Next); d <= 2*time.Second || d > 3*time.Second+time.Second {
		t.Errorf("entry Next %s is inconsistent with the 3s schedule", entry.Next)
	}
}

// Updating before Start applies directly and is reported consistently.
func TestUpdateScheduleWhileNotRunning(t *testing.T) {
	cron := New()
	id := cron.Schedule(Every(time.Second), FuncJob(func() {}))

	rev, err := cron.UpdateSchedule(id, Every(5*time.Second))
	if err != nil {
		t.Fatalf("UpdateSchedule failed: %v", err)
	}
	if rev != 1 {
		t.Errorf("expected revision 1, got %d", rev)
	}
	entry := cron.Entry(id)
	if entry.Revision != 1 || entry.Schedule != Every(5*time.Second) {
		t.Errorf("inconsistent entry after update: %+v", entry)
	}
	if entry.Next.IsZero() {
		t.Error("Next should be computed from the new schedule")
	}
}

// An unsatisfiable schedule (zero Next) is a valid update target: the entry
// simply never runs again, and Entry reports the zero Next consistently.
func TestUpdateScheduleUnsatisfiableNext(t *testing.T) {
	timers := installFakeTimers(t)

	var runs int32
	cron := New()
	id := cron.Schedule(Every(time.Second), FuncJob(func() {
		atomic.AddInt32(&runs, 1)
	}))
	cron.Start()
	defer cron.Stop()
	<-timers

	rev, err := cron.UpdateSchedule(id, zeroSchedule{})
	if err != nil {
		t.Fatalf("UpdateSchedule with zero-Next schedule failed: %v", err)
	}
	entry := cron.Entry(id)
	if entry.Revision != rev {
		t.Errorf("entry revision %d, want %d", entry.Revision, rev)
	}
	if !entry.Next.IsZero() {
		t.Errorf("Next should be zero for an unsatisfiable schedule, got %s", entry.Next)
	}

	// The scheduler re-armed its long-sleep timer; nothing may fire.
	<-timers
	time.Sleep(100 * time.Millisecond)
	if got := atomic.LoadInt32(&runs); got != 0 {
		t.Errorf("job ran %d time(s) on an unsatisfiable schedule", got)
	}
}

// A nil schedule is rejected deterministically and changes nothing.
func TestUpdateScheduleNilSchedule(t *testing.T) {
	cron := New()
	id := cron.Schedule(Every(time.Second), FuncJob(func() {}))

	if _, err := cron.UpdateSchedule(id, nil); err != ErrNilSchedule {
		t.Fatalf("expected ErrNilSchedule, got %v", err)
	}
	entry := cron.Entry(id)
	if entry.Revision != 0 || entry.Schedule != Every(time.Second) {
		t.Errorf("entry changed after failed update: %+v", entry)
	}
}

// Updating a removed (or never-existing) entry fails with ErrEntryNotFound.
func TestUpdateScheduleRemovedEntry(t *testing.T) {
	cron := New()
	id := cron.Schedule(Every(time.Second), FuncJob(func() {}))
	cron.Start()
	defer cron.Stop()

	cron.Remove(id)
	if _, err := cron.UpdateSchedule(id, Every(2*time.Second)); err != ErrEntryNotFound {
		t.Errorf("updating removed entry: expected ErrEntryNotFound, got %v", err)
	}
	if _, err := cron.UpdateSchedule(EntryID(9999), Every(2*time.Second)); err != ErrEntryNotFound {
		t.Errorf("updating unknown entry: expected ErrEntryNotFound, got %v", err)
	}
}

// After Stop, updates still apply deterministically to the stopped entry list.
func TestUpdateScheduleAfterStop(t *testing.T) {
	cron := New()
	id := cron.Schedule(Every(time.Second), FuncJob(func() {}))
	cron.Start()
	cron.Stop()

	rev, err := cron.UpdateSchedule(id, Every(2*time.Second))
	if err != nil {
		t.Fatalf("UpdateSchedule after Stop failed: %v", err)
	}
	if rev != 1 {
		t.Errorf("expected revision 1, got %d", rev)
	}
	entry := cron.Entry(id)
	if entry.Revision != 1 || entry.Schedule != Every(2*time.Second) {
		t.Errorf("inconsistent entry after stopped update: %+v", entry)
	}
}

// Stop racing with UpdateSchedule must produce a deterministic result: the
// update is applied either before the scheduler halts or directly to the
// stopped entry list; it never deadlocks and never fails spuriously.
func TestUpdateScheduleStopRace(t *testing.T) {
	for i := 0; i < 50; i++ {
		cron := New()
		id := cron.Schedule(Every(time.Second), FuncJob(func() {}))
		cron.Start()

		var wg sync.WaitGroup
		wg.Add(2)
		var rev uint64
		var err error
		go func() {
			defer wg.Done()
			rev, err = cron.UpdateSchedule(id, Every(2*time.Second))
		}()
		go func() {
			defer wg.Done()
			cron.Stop()
		}()

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Stop racing UpdateSchedule deadlocked")
		}
		if err != nil {
			t.Fatalf("iteration %d: update failed: %v", i, err)
		}
		if rev != 1 {
			t.Fatalf("iteration %d: expected revision 1, got %d", i, rev)
		}
		entry := cron.Entry(id)
		if entry.Revision != 1 || entry.Schedule != Every(2*time.Second) {
			t.Fatalf("iteration %d: inconsistent entry: %+v", i, entry)
		}
	}
}

// A job of the old generation that is already running when its schedule is
// replaced is not cancelled; it runs to completion.
func TestUpdateScheduleDoesNotCancelRunningJob(t *testing.T) {
	timers := installFakeTimers(t)

	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})

	cron := New()
	id := cron.Schedule(Every(time.Second), FuncJob(func() {
		close(started)
		<-release
		close(finished)
	}))
	cron.Start()
	defer cron.Stop()

	// Trigger the first run of the old generation.
	timer := <-timers
	entry := cron.Entry(id)
	timer.ch <- entry.Next

	select {
	case <-started:
	case <-time.After(OneSecond):
		t.Fatal("job did not start")
	}

	// Replace the schedule while the old-generation job is blocked mid-run.
	rev, err := cron.UpdateSchedule(id, Every(2*time.Second))
	if err != nil {
		t.Fatalf("UpdateSchedule failed: %v", err)
	}
	if rev != 1 {
		t.Errorf("expected revision 1, got %d", rev)
	}

	// The running job must still be able to complete.
	close(release)
	select {
	case <-finished:
	case <-time.After(OneSecond):
		t.Fatal("running job was cancelled by the schedule update")
	}

	// Wait for the job to fully wind down before stopping.
	ctx := cron.Stop()
	<-ctx.Done()
}

// Concurrent updates, snapshots, adds and removes must be data-race free.
func TestUpdateScheduleRace(t *testing.T) {
	cron := New()
	var ids []EntryID
	var mu sync.Mutex
	for i := 0; i < 5; i++ {
		id := cron.Schedule(Every(time.Second), FuncJob(func() {}))
		ids = append(ids, id)
	}
	cron.Start()
	defer cron.Stop()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() { // updater
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			mu.Lock()
			id := ids[i%len(ids)]
			mu.Unlock()
			cron.UpdateSchedule(id, Every(time.Duration(1+i%5)*time.Second))
			i++
		}
	}()

	wg.Add(1)
	go func() { // reader
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, e := range cron.Entries() {
				_ = e.Revision
				_ = e.Next
			}
		}
	}()

	wg.Add(1)
	go func() { // add/remove churn
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			id := cron.Schedule(Every(time.Second), FuncJob(func() {}))
			cron.Remove(id)
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// Demo: shows within a couple of seconds that a stale signal of the old
// schedule is discarded while the new schedule fires on time.
// Run it with: go test -run TestUpdateScheduleDemo -v
func TestUpdateScheduleDemo(t *testing.T) {
	start := time.Now()
	log := func(format string, args ...interface{}) {
		elapsed := time.Since(start).Round(time.Millisecond)
		t.Logf("[+%s] "+format, append([]interface{}{elapsed}, args...)...)
	}
	timers := installFakeTimers(t)

	var runs int32
	cron := New()
	id := cron.Schedule(Every(time.Second), FuncJob(func() {
		atomic.AddInt32(&runs, 1)
	}))
	cron.Start()
	defer cron.Stop()

	oldTimer := <-timers
	log("old schedule (every 1s) armed; its due signal is held back")

	rev, _ := cron.UpdateSchedule(id, Every(2*time.Second))
	newTimer := <-timers
	entry := cron.Entry(id)
	log("published new schedule (every 2s), revision=%d, next=%s", rev, entry.Next.Format("15:04:05.000"))

	log("releasing the stale signal of the old schedule")
	oldTimer.ch <- time.Now()
	time.Sleep(100 * time.Millisecond)
	log("stale signal discarded: job runs so far = %d", atomic.LoadInt32(&runs))

	log("firing the new timer exactly at the scheduled Next")
	newTimer.ch <- entry.Next
	waitRuns(&runs, 1, OneSecond)
	log("new schedule fired on time: job runs = %d", atomic.LoadInt32(&runs))

	if atomic.LoadInt32(&runs) != 1 {
		t.Fatalf("demo failed: expected exactly 1 run, got %d", runs)
	}
}

package cron

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "time/tzdata" // embedded zoneinfo so DST tests work without a system tz database
)

// fakeClock is a manually advanced Clock for deterministic scheduler tests.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers map[*fakeTimer]struct{}
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start, timers: make(map[*fakeTimer]struct{})}
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) NewTimer(d time.Duration) Timer {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := &fakeTimer{clock: f, deadline: f.now.Add(d), ch: make(chan time.Time, 1)}
	if !t.deadline.After(f.now) {
		// Like time.Timer, a non-positive duration fires immediately.
		t.ch <- t.deadline
		return t
	}
	f.timers[t] = struct{}{}
	return t
}

// Advance moves the fake clock forward and fires any timers that come due.
func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
	for t := range f.timers {
		if !t.deadline.After(f.now) {
			delete(f.timers, t)
			t.ch <- t.deadline
		}
	}
}

type fakeTimer struct {
	clock    *fakeClock
	deadline time.Time
	ch       chan time.Time
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if _, ok := t.clock.timers[t]; ok {
		delete(t.clock.timers, t)
		return true
	}
	return false
}

// recordLogger captures log records for assertions.
type recordLogger struct {
	mu     sync.Mutex
	infos  []recordedLog
	errors []recordedLog
}

type recordedLog struct {
	msg string
	kv  []interface{}
}

func (l *recordLogger) Info(msg string, keysAndValues ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.infos = append(l.infos, recordedLog{msg, keysAndValues})
}

func (l *recordLogger) Error(err error, msg string, keysAndValues ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errors = append(l.errors, recordedLog{err.Error() + ": " + msg, keysAndValues})
}

func (l *recordLogger) errorCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.errors)
}

// catchUpScheduled returns the formatted scheduled times of all replayed
// catch-up runs, in replay order.
func (l *recordLogger) catchUpScheduled() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, e := range l.infos {
		if e.msg != "catch-up" {
			continue
		}
		for i := 0; i+1 < len(e.kv); i += 2 {
			if e.kv[i] == "scheduled" {
				out = append(out, e.kv[i+1].(time.Time).Format("15:04:05"))
			}
		}
	}
	return out
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func everyMinute(t *testing.T) Schedule {
	t.Helper()
	sched, err := secondParser.Parse("0 * * * * *")
	if err != nil {
		t.Fatalf("failed to parse spec: %v", err)
	}
	return sched
}

// TestPauseResumeMisfirePolicies is the acceptance scenario: a per-minute
// job is paused at 00:00:30 and resumed at 00:04:10, missing the 00:01,
// 00:02, 00:03 and 00:04 triggers. skip replays nothing, coalesce replays
// once, and catch_up with MaxCatchUp=2 replays 00:03 and 00:04, reporting
// 2 dropped triggers.
func TestPauseResumeMisfirePolicies(t *testing.T) {
	cases := []struct {
		name         string
		policy       MisfirePolicy
		maxCatchUp   int
		wantCatchUp  int64
		wantDropped  int
		wantSchedule []string
	}{
		{"skip", MisfireSkip, 0, 0, 0, nil},
		{"coalesce", MisfireCoalesce, 0, 1, 0, []string{"00:04:00"}},
		{"catch_up max 2", MisfireCatchUp, 2, 2, 2, []string{"00:03:00", "00:04:00"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clock := newFakeClock(time.Date(2026, 9, 22, 0, 0, 30, 0, time.UTC))
			logger := &recordLogger{}
			cron := New(WithClock(clock), WithLocation(time.UTC), WithLogger(logger))
			var runs int64
			id := cron.ScheduleWithOptions(everyMinute(t), FuncJob(func() {
				atomic.AddInt64(&runs, 1)
			}), WithMisfirePolicy(tc.policy), WithMaxCatchUp(tc.maxCatchUp))
			cron.Start()
			defer cron.Stop()

			cron.Pause()
			cron.Pause()                                  // repeated Pause is a no-op
			clock.Advance(3*time.Minute + 40*time.Second) // 00:04:10
			cron.Resume()
			cron.Resume() // repeated Resume is a no-op

			if tc.wantCatchUp > 0 {
				waitFor(t, "catch-up runs", func() bool {
					return atomic.LoadInt64(&runs) == tc.wantCatchUp
				})
			} else {
				// Give any erroneous catch-up run a chance to happen.
				time.Sleep(50 * time.Millisecond)
			}
			if got := atomic.LoadInt64(&runs); got != tc.wantCatchUp {
				t.Errorf("catch-up runs = %d, want %d", got, tc.wantCatchUp)
			}
			if got := cron.Entry(id).DroppedMisses; got != tc.wantDropped {
				t.Errorf("DroppedMisses = %d, want %d", got, tc.wantDropped)
			}
			gotSched := logger.catchUpScheduled()
			if len(gotSched) != len(tc.wantSchedule) {
				t.Errorf("catch-up scheduled times = %v, want %v", gotSched, tc.wantSchedule)
			} else {
				for i := range gotSched {
					if gotSched[i] != tc.wantSchedule[i] {
						t.Errorf("catch-up scheduled times = %v, want %v", gotSched, tc.wantSchedule)
						break
					}
				}
			}

			// Normal scheduling resumes at the next minute boundary.
			clock.Advance(time.Minute) // 00:05:10, fires 00:05:00
			waitFor(t, "normal run after resume", func() bool {
				return atomic.LoadInt64(&runs) == tc.wantCatchUp+1
			})
		})
	}
}

// TestCatchUpMergesNormalTriggers verifies that normal triggers arriving
// while a catch-up replay is still in flight are merged into a single run
// that executes once the replay completes.
func TestCatchUpMergesNormalTriggers(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 9, 22, 0, 0, 30, 0, time.UTC))
	cron := New(WithClock(clock), WithLocation(time.UTC))
	var runs int64
	release := make(chan struct{})
	cron.ScheduleWithOptions(everyMinute(t), FuncJob(func() {
		atomic.AddInt64(&runs, 1)
		<-release
	}), WithMisfirePolicy(MisfireCatchUp), WithMaxCatchUp(2))
	cron.Start()
	defer cron.Stop()

	cron.Pause()
	clock.Advance(3*time.Minute + 40*time.Second) // 00:04:10
	cron.Resume()

	// The first replayed run (00:03) is in flight and blocked.
	waitFor(t, "first catch-up run", func() bool { return atomic.LoadInt64(&runs) == 1 })

	// Normal triggers at 00:05 and 00:06 arrive during the replay.
	clock.Advance(time.Minute) // 00:05:10
	clock.Advance(time.Minute) // 00:06:10

	release <- struct{}{} // 00:03 run completes, 00:04 run starts
	waitFor(t, "second catch-up run", func() bool { return atomic.LoadInt64(&runs) == 2 })
	release <- struct{}{} // 00:04 run completes, merged normal run starts
	waitFor(t, "merged normal run", func() bool { return atomic.LoadInt64(&runs) == 3 })
	release <- struct{}{} // merged run completes

	// The two normal triggers were merged into exactly one run.
	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt64(&runs); got != 3 {
		t.Errorf("runs = %d, want 3 (2 catch-up + 1 merged normal)", got)
	}

	// Normal scheduling continues afterwards.
	clock.Advance(time.Minute) // 00:07:10, fires 00:07:00
	waitFor(t, "next normal run", func() bool { return atomic.LoadInt64(&runs) == 4 })
	release <- struct{}{}
}

// TestPauseLetsRunningJobFinish verifies that pausing does not interrupt a
// job that is already running, and that no new jobs start while paused.
func TestPauseLetsRunningJobFinish(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC))
	cron := New(WithClock(clock), WithLocation(time.UTC))
	var runs int64
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	finished := make(chan struct{}, 2)
	cron.ScheduleWithOptions(everyMinute(t), FuncJob(func() {
		atomic.AddInt64(&runs, 1)
		started <- struct{}{}
		<-release
		finished <- struct{}{}
	}), WithMisfirePolicy(MisfireSkip))
	cron.Start()
	defer cron.Stop()

	cron.Entries()            // sync with the run loop before advancing the clock
	clock.Advance(time.Minute) // 00:01:00 fires
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("job did not start")
	}

	cron.Pause()
	clock.Advance(3 * time.Minute) // 00:04:00; 00:02-00:04 triggers missed

	// The already-running job is allowed to complete while paused.
	release <- struct{}{}
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("running job did not finish while paused")
	}

	cron.Resume()
	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt64(&runs); got != 1 {
		t.Errorf("runs = %d, want 1 (skip policy replays nothing)", got)
	}

	clock.Advance(time.Minute) // 00:05:00 fires
	waitFor(t, "run after resume", func() bool { return atomic.LoadInt64(&runs) == 2 })
	release <- struct{}{}
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("job after resume did not finish")
	}
}

// TestResumeAfterStopIsNoop verifies that Resume cannot restart a stopped
// cron, and that Pause/Resume are safe no-ops when not running.
func TestResumeAfterStopIsNoop(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 9, 22, 0, 0, 30, 0, time.UTC))
	cron := New(WithClock(clock), WithLocation(time.UTC))
	var runs int64
	cron.ScheduleWithOptions(everyMinute(t), FuncJob(func() {
		atomic.AddInt64(&runs, 1)
	}), WithMisfirePolicy(MisfireCatchUp))
	cron.Start()
	cron.Pause()
	clock.Advance(3*time.Minute + 40*time.Second) // 00:04:10

	ctx := cron.Stop()
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Stop context did not close")
	}

	// Resume after Stop must not restart the scheduler.
	cron.Resume()
	cron.Pause()
	clock.Advance(5 * time.Minute)
	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt64(&runs); got != 0 {
		t.Errorf("runs = %d, want 0 after Stop", got)
	}

	// Pause/Resume before Start are no-ops and do not block.
	neverStarted := New(WithClock(newFakeClock(time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC))))
	neverStarted.Pause()
	neverStarted.Resume()
	neverStarted.Stop()
}

// staticSchedule never advances: its Next returns the time it was given.
type staticSchedule struct{}

func (staticSchedule) Next(t time.Time) time.Time { return t }

// TestNonAdvancingScheduleRejected verifies that a schedule whose Next does
// not advance is disabled with an error log instead of looping forever, both
// on the normal scheduling path and while computing missed triggers.
func TestNonAdvancingScheduleRejected(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC))
	logger := &recordLogger{}
	cron := New(WithClock(clock), WithLocation(time.UTC), WithLogger(logger))
	var goodRuns, badRuns int64
	bad := cron.ScheduleWithOptions(staticSchedule{}, FuncJob(func() {
		atomic.AddInt64(&badRuns, 1)
	}), WithMisfirePolicy(MisfireCatchUp))
	cron.ScheduleWithOptions(everyMinute(t), FuncJob(func() {
		atomic.AddInt64(&goodRuns, 1)
	}))
	cron.Start()
	defer cron.Stop()

	cron.Entries() // sync with the run loop before advancing the clock
	clock.Advance(time.Minute)
	waitFor(t, "first good run", func() bool { return atomic.LoadInt64(&goodRuns) >= 1 })
	clock.Advance(time.Minute)
	waitFor(t, "second good run", func() bool { return atomic.LoadInt64(&goodRuns) >= 2 })
	if got := atomic.LoadInt64(&badRuns); got != 0 {
		t.Errorf("non-advancing schedule ran %d times, want 0", got)
	}
	if entry := cron.Entry(bad); !entry.Next.IsZero() {
		t.Errorf("non-advancing entry Next = %v, want zero (rejected)", entry.Next)
	}
	if logger.errorCount() == 0 {
		t.Error("expected an error log for the non-advancing schedule")
	}

	// The missed-trigger computation rejects it too, without hanging.
	bad2 := cron.ScheduleWithOptions(staticSchedule{}, FuncJob(func() {
		atomic.AddInt64(&badRuns, 1)
	}), WithMisfirePolicy(MisfireCatchUp))
	cron.Pause()
	clock.Advance(time.Minute)
	cron.Resume()
	if entry := cron.Entry(bad2); !entry.Next.IsZero() {
		t.Errorf("non-advancing entry Next = %v, want zero (rejected)", entry.Next)
	}
}

// TestPauseResumeAcrossDST exercises pause/resume across DST transitions:
// the spring-forward hour that does not exist and the fall-back hour that
// happens twice.
func TestPauseResumeAcrossDST(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("timezone database not available: %v", err)
	}

	t.Run("spring forward skips nonexistent 02:30", func(t *testing.T) {
		// On 2026-03-08 in America/New_York, 02:00-03:00 does not exist.
		clock := newFakeClock(time.Date(2026, 3, 8, 0, 30, 0, 0, loc))
		cron := New(WithClock(clock), WithLocation(loc))
		sched, err := standardParser.Parse("30 2 * * *")
		if err != nil {
			t.Fatalf("failed to parse spec: %v", err)
		}
		var runs int64
		id := cron.ScheduleWithOptions(sched, FuncJob(func() {
			atomic.AddInt64(&runs, 1)
		}), WithMisfirePolicy(MisfireCatchUp))
		cron.Start()
		defer cron.Stop()

		cron.Pause()
		clock.Advance(4 * time.Hour) // 04:30 EDT
		cron.Resume()

		time.Sleep(50 * time.Millisecond)
		if got := atomic.LoadInt64(&runs); got != 0 {
			t.Errorf("runs = %d, want 0: 02:30 did not exist on the spring-forward day", got)
		}
		wantNext := time.Date(2026, 3, 9, 2, 30, 0, 0, loc)
		if got := cron.Entry(id).Next; !got.Equal(wantNext) {
			t.Errorf("Next = %v, want %v", got, wantNext)
		}
	})

	t.Run("fall back replays the repeated hour twice", func(t *testing.T) {
		// On 2026-11-01 in America/New_York, 01:00-02:00 happens twice and
		// the schedule activates at both 01:30 occurrences, so both are
		// replayed on resume.
		clock := newFakeClock(time.Date(2026, 11, 1, 0, 30, 0, 0, loc))
		cron := New(WithClock(clock), WithLocation(loc))
		sched, err := standardParser.Parse("30 1 * * *")
		if err != nil {
			t.Fatalf("failed to parse spec: %v", err)
		}
		var runs int64
		id := cron.ScheduleWithOptions(sched, FuncJob(func() {
			atomic.AddInt64(&runs, 1)
		}), WithMisfirePolicy(MisfireCatchUp))
		cron.Start()
		defer cron.Stop()

		cron.Pause()
		clock.Advance(3 * time.Hour) // 03:30 EST
		cron.Resume()

		waitFor(t, "two catch-up runs", func() bool { return atomic.LoadInt64(&runs) == 2 })
		time.Sleep(50 * time.Millisecond)
		if got := atomic.LoadInt64(&runs); got != 2 {
			t.Errorf("runs = %d, want 2: both 01:30 occurrences are scheduled", got)
		}
		wantNext := time.Date(2026, 11, 2, 1, 30, 0, 0, loc)
		if got := cron.Entry(id).Next; !got.Equal(wantNext) {
			t.Errorf("Next = %v, want %v", got, wantNext)
		}
	})
}

// catchUpLogger prints the scheduled time of each replayed catch-up run.
type catchUpLogger struct{}

func (catchUpLogger) Info(msg string, keysAndValues ...interface{}) {
	if msg != "catch-up" {
		return
	}
	for i := 0; i+1 < len(keysAndValues); i += 2 {
		if keysAndValues[i] == "scheduled" {
			fmt.Println("catch-up run scheduled at", keysAndValues[i+1].(time.Time).Format("15:04:05"))
		}
	}
}

func (catchUpLogger) Error(err error, msg string, keysAndValues ...interface{}) {}

func ExampleCron_Pause() {
	clock := newFakeClock(time.Date(2026, 9, 22, 0, 0, 30, 0, time.UTC))
	c := New(WithClock(clock), WithLocation(time.UTC), WithLogger(catchUpLogger{}))
	sched, _ := secondParser.Parse("0 * * * * *")
	var mu sync.Mutex
	var order []int
	c.ScheduleWithOptions(sched, FuncJob(func() {
		mu.Lock()
		order = append(order, len(order)+1)
		mu.Unlock()
	}), WithMisfirePolicy(MisfireCatchUp), WithMaxCatchUp(2))
	c.Start()
	defer c.Stop()

	// Pause at 00:00:30 and resume at 00:04:10: the 00:01-00:04 triggers are
	// missed, and the two most recent ones are replayed in scheduled order.
	c.Pause()
	clock.Advance(3*time.Minute + 40*time.Second)
	c.Resume()

	// Wait for the replayed runs to finish.
	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		n := len(order)
		mu.Unlock()
		if n == 2 || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	fmt.Println("actual run order:", order)

	// Output:
	// catch-up run scheduled at 00:03:00
	// catch-up run scheduled at 00:04:00
	// actual run order: [1 2]
}

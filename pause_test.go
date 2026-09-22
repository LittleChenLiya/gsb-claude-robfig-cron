package cron

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock is a Clock implementation driven manually by tests.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers map[*fakeTimer]struct{}
}

func newFakeClock(now time.Time) *fakeClock {
	return &fakeClock{now: now, timers: make(map[*fakeTimer]struct{})}
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) NewTimer(d time.Duration) Timer {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := &fakeTimer{clock: f, at: f.now.Add(d), ch: make(chan time.Time, 1)}
	if d <= 0 {
		t.ch <- f.now
		return t
	}
	f.timers[t] = struct{}{}
	return t
}

// Advance moves the clock forward and fires any timers that come due.
func (f *fakeClock) Advance(d time.Duration) { f.AdvanceTo(f.Now().Add(d)) }

// AdvanceTo moves the clock to an absolute time and fires due timers.
func (f *fakeClock) AdvanceTo(t time.Time) {
	f.mu.Lock()
	f.now = t
	f.mu.Unlock()
	// Fire due timers, then keep re-checking: the scheduler re-arms its
	// timer asynchronously after each wake, and the new timer may already
	// be due relative to the new now.
	quiet := 0
	for i := 0; i < 200; i++ {
		time.Sleep(2 * time.Millisecond)
		f.mu.Lock()
		fired := false
		for timer := range f.timers {
			if !timer.at.After(f.now) {
				delete(f.timers, timer)
				timer.ch <- timer.at
				fired = true
			}
		}
		f.mu.Unlock()
		if fired {
			quiet = 0
		} else {
			quiet++
		}
		if quiet >= 3 {
			return
		}
	}
}

func (f *fakeClock) timerCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.timers)
}

// waitTimer polls until the scheduler has armed a timer. Tests use it to
// synchronize with the run loop, which arms its timer asynchronously after
// Start, Resume and every wake; advancing the clock before the timer is
// armed would skip activations.
func (f *fakeClock) waitTimer() bool {
	for i := 0; i < 2500; i++ {
		if f.timerCount() > 0 {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return false
}

func (f *fakeClock) mustTimer(t *testing.T) {
	t.Helper()
	if !f.waitTimer() {
		t.Fatal("scheduler did not arm a timer")
	}
}

type fakeTimer struct {
	clock *fakeClock
	at    time.Time
	ch    chan time.Time
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	_, ok := t.clock.timers[t]
	delete(t.clock.timers, t)
	return ok
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func stopAndWait(c *Cron) {
	ctx := c.Stop()
	<-ctx.Done()
}

// Acceptance scenario: an every-minute job is paused at 00:00:30 and resumed
// at 00:04:10, missing the 01/02/03/04 minute activations. skip runs 0
// times, coalesce runs once, catch_up with max 2 replays 03:00 and 04:00
// and reports 2 dropped.
func TestPauseResumeMisfirePolicies(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC))
	c := New(WithClock(clock), WithSeconds(), WithLogger(DiscardLogger))

	var skipRuns, coalesceRuns, catchUpRuns int32
	skipID, err := c.AddFunc("0 * * * * *", func() { atomic.AddInt32(&skipRuns, 1) })
	if err != nil {
		t.Fatal(err)
	}
	coalesceID, err := c.AddFunc("0 * * * * *", func() { atomic.AddInt32(&coalesceRuns, 1) })
	if err != nil {
		t.Fatal(err)
	}
	catchUpID, err := c.AddFunc("0 * * * * *", func() { atomic.AddInt32(&catchUpRuns, 1) })
	if err != nil {
		t.Fatal(err)
	}
	c.SetMisfirePolicy(skipID, MisfireSkip, 0)
	c.SetMisfirePolicy(coalesceID, MisfireCoalesce, 0)
	c.SetMisfirePolicy(catchUpID, MisfireCatchUp, 2)

	c.Start()
	clock.mustTimer(t)
	clock.AdvanceTo(time.Date(2026, 1, 15, 0, 0, 30, 0, time.UTC))
	c.Pause()
	clock.AdvanceTo(time.Date(2026, 1, 15, 0, 4, 10, 0, time.UTC))
	c.Resume()

	waitFor(t, "catch-up replays", func() bool { return atomic.LoadInt32(&catchUpRuns) == 2 })
	waitFor(t, "coalesced run", func() bool { return atomic.LoadInt32(&coalesceRuns) == 1 })
	stopAndWait(c)

	if n := atomic.LoadInt32(&skipRuns); n != 0 {
		t.Errorf("skip policy ran %d times, want 0", n)
	}
	if n := atomic.LoadInt32(&coalesceRuns); n != 1 {
		t.Errorf("coalesce policy ran %d times, want 1", n)
	}
	if n := atomic.LoadInt32(&catchUpRuns); n != 2 {
		t.Errorf("catch-up policy ran %d times, want 2", n)
	}
	entry := c.Entry(catchUpID)
	if entry.DroppedMisses != 2 {
		t.Errorf("dropped misses = %d, want 2", entry.DroppedMisses)
	}
	want := time.Date(2026, 1, 15, 0, 4, 0, 0, time.UTC)
	if !entry.Prev.Equal(want) {
		t.Errorf("last replayed scheduled time = %v, want %v", entry.Prev, want)
	}
}

// Pause and Resume are idempotent, and Resume after Stop does not restart
// the scheduler.
func TestPauseResumeIdempotentAndStop(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC))
	c := New(WithClock(clock), WithSeconds(), WithLogger(DiscardLogger))
	var runs int32
	if _, err := c.AddFunc("0 * * * * *", func() { atomic.AddInt32(&runs, 1) }); err != nil {
		t.Fatal(err)
	}
	c.Start()
	c.Pause()
	c.Pause()
	c.Resume()
	c.Resume()
	clock.mustTimer(t)
	clock.Advance(time.Minute)
	waitFor(t, "first normal run", func() bool { return atomic.LoadInt32(&runs) == 1 })
	clock.mustTimer(t)
	clock.Advance(time.Minute)
	waitFor(t, "second normal run", func() bool { return atomic.LoadInt32(&runs) == 2 })
	stopAndWait(c)

	c.Resume() // must not restart a stopped cron
	clock.Advance(5 * time.Minute)
	time.Sleep(50 * time.Millisecond)
	if n := atomic.LoadInt32(&runs); n != 2 {
		t.Errorf("runs after Stop+Resume = %d, want 2", n)
	}
}

// A job already running when Pause is called is allowed to finish, and no
// new jobs start while paused.
func TestPauseLetsRunningJobFinish(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC))
	c := New(WithClock(clock), WithSeconds(), WithLogger(DiscardLogger))
	var runs int32
	started := make(chan struct{}, 10)
	release := make(chan struct{})
	if _, err := c.AddFunc("0 * * * * *", func() {
		n := atomic.AddInt32(&runs, 1)
		started <- struct{}{}
		if n == 1 {
			<-release // first run stays in flight across the pause
		}
	}); err != nil {
		t.Fatal(err)
	}
	c.Start()
	clock.mustTimer(t)
	clock.AdvanceTo(time.Date(2026, 1, 15, 0, 1, 0, 0, time.UTC))
	<-started
	c.Pause()
	clock.AdvanceTo(time.Date(2026, 1, 15, 0, 3, 0, 0, time.UTC))
	select {
	case <-started:
		t.Fatal("a new job started while paused")
	case <-time.After(100 * time.Millisecond):
	}
	close(release) // the in-flight job finishes during the pause
	c.Resume()
	// Default coalesce policy: the two missed activations collapse into one.
	waitFor(t, "coalesced run after resume", func() bool { return atomic.LoadInt32(&runs) == 2 })
	stopAndWait(c)
}

// Catch-up replays run serially per entry; normal activations that fire
// while a catch-up round is still running are merged into a single run
// afterwards.
func TestCatchUpCoalescesNormalTriggers(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC))
	c := New(WithClock(clock), WithSeconds(), WithLogger(DiscardLogger))
	var runs int32
	firstStarted := make(chan struct{}, 1)
	firstBlock := make(chan struct{})
	id, err := c.AddFunc("0 * * * * *", func() {
		n := atomic.AddInt32(&runs, 1)
		if n == 1 {
			firstStarted <- struct{}{}
			<-firstBlock
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	c.SetMisfirePolicy(id, MisfireCatchUp, 5)
	c.Start()
	clock.mustTimer(t)
	clock.AdvanceTo(time.Date(2026, 1, 15, 0, 0, 30, 0, time.UTC))
	c.Pause()
	clock.AdvanceTo(time.Date(2026, 1, 15, 0, 2, 10, 0, time.UTC))
	c.Resume() // replays 00:01:00 and 00:02:00; the first one blocks
	<-firstStarted
	clock.mustTimer(t)
	// The 00:03:00 activation fires while the catch-up round is in flight.
	clock.AdvanceTo(time.Date(2026, 1, 15, 0, 3, 30, 0, time.UTC))
	time.Sleep(50 * time.Millisecond)
	close(firstBlock)
	// 2 replays + 1 coalesced normal trigger.
	waitFor(t, "replays plus coalesced trigger", func() bool { return atomic.LoadInt32(&runs) == 3 })
	stopAndWait(c)
}

// Resuming across a DST spring-forward: the 02:30 wall time does not exist
// on 2026-03-08 in America/New_York, so there is nothing to miss; the
// scheduler must not hang, and the job next runs on the following day.
func TestResumeAcrossDSTSpringForward(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("timezone database unavailable: %v", err)
	}
	clock := newFakeClock(time.Date(2026, 3, 8, 0, 0, 0, 0, loc))
	c := New(WithClock(clock), WithLocation(loc), WithLogger(DiscardLogger))
	var runs int32
	if _, err := c.AddFunc("30 2 * * *", func() { atomic.AddInt32(&runs, 1) }); err != nil {
		t.Fatal(err)
	}
	c.Start()
	clock.mustTimer(t)
	c.Pause()
	clock.AdvanceTo(time.Date(2026, 3, 8, 4, 0, 0, 0, loc))
	c.Resume()
	clock.mustTimer(t)
	time.Sleep(100 * time.Millisecond)
	if n := atomic.LoadInt32(&runs); n != 0 {
		t.Fatalf("runs across DST gap = %d, want 0 (02:30 did not exist)", n)
	}
	// The schedule still fires the next day at 02:30 EDT.
	clock.AdvanceTo(time.Date(2026, 3, 9, 3, 0, 0, 0, loc))
	waitFor(t, "run on the day after the DST gap", func() bool { return atomic.LoadInt32(&runs) == 1 })
	stopAndWait(c)
}

// Resuming across a DST fall-back sees both occurrences of the repeated
// hour: 01:30 EDT and 01:30 EST are distinct scheduled times.
func TestResumeAcrossDSTFallBack(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("timezone database unavailable: %v", err)
	}
	clock := newFakeClock(time.Date(2026, 11, 1, 0, 0, 0, 0, loc))
	c := New(WithClock(clock), WithLocation(loc), WithLogger(DiscardLogger))
	var runs int32
	id, err := c.AddFunc("30 1 * * *", func() { atomic.AddInt32(&runs, 1) })
	if err != nil {
		t.Fatal(err)
	}
	c.SetMisfirePolicy(id, MisfireCatchUp, 10)
	c.Start()
	clock.mustTimer(t)
	c.Pause()
	clock.AdvanceTo(time.Date(2026, 11, 1, 3, 0, 0, 0, loc))
	c.Resume()
	waitFor(t, "both repeated-hour activations", func() bool { return atomic.LoadInt32(&runs) == 2 })
	stopAndWait(c)
	// The last replay is the second 01:30, i.e. 01:30 EST = 07:30 UTC.
	want := time.Date(2026, 11, 1, 6, 30, 0, 0, time.UTC)
	if prev := c.Entry(id).Prev; !prev.Equal(want) {
		t.Errorf("last replayed scheduled time = %v, want %v (01:30 EST)", prev, want)
	}
}

// stuckSchedule never advances time.
type stuckSchedule struct{}

func (stuckSchedule) Next(t time.Time) time.Time { return t }

// A custom Schedule that does not advance time is rejected on resume
// instead of wedging the scheduler in a loop.
func TestResumeRejectsNonAdvancingSchedule(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC))
	c := New(WithClock(clock), WithLogger(DiscardLogger))
	c.Pause() // paused before Start: no timer ever fires for the entry
	c.Start()
	var runs int32
	id := c.Schedule(stuckSchedule{}, FuncJob(func() { atomic.AddInt32(&runs, 1) }))
	clock.Advance(time.Minute)
	c.Resume()
	waitFor(t, "entry deactivation", func() bool { return c.Entry(id).Next.IsZero() })
	stopAndWait(c)
	if n := atomic.LoadInt32(&runs); n != 0 {
		t.Errorf("non-advancing schedule ran %d times, want 0", n)
	}
}

package cron

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

var errFake = errors.New("fake failure")

// resultOf returns a result function whose verdict is read from *verdict.
func resultOf(verdict *error) func() error {
	return func() error { return *verdict }
}

// 阈值 2 连续失败后后续触发跳过；显式恢复后成功运行计数为 0。
func TestFailureSwitchPausesAfterThreshold(t *testing.T) {
	verdict := errFake
	sw := NewFailureSwitch(2, resultOf(&verdict), DiscardLogger)

	var runs int32
	job := sw.Wrap(FuncJob(func() { atomic.AddInt32(&runs, 1) }))

	job.Run() // failure #1
	if got := sw.Failures(); got != 1 {
		t.Fatalf("after 1st failure, Failures() = %d, want 1", got)
	}
	if sw.Paused() {
		t.Fatal("paused after only 1 failure, want still active")
	}

	job.Run() // failure #2 -> threshold reached
	if got := sw.Failures(); got != 2 {
		t.Fatalf("after 2nd failure, Failures() = %d, want 2", got)
	}
	if !sw.Paused() {
		t.Fatal("not paused after 2 consecutive failures")
	}

	// Subsequent triggers are skipped, each with its own independent event.
	job.Run()
	job.Run()
	if got := atomic.LoadInt32(&runs); got != 2 {
		t.Fatalf("paused job executed %d times, want 2 (skipped runs must not execute)", got)
	}
	for i := 0; i < 2; i++ {
		select {
		case ev := <-sw.Skipped():
			if ev.Failures != 2 || ev.Generation != 0 {
				t.Fatalf("skip event %+v, want Failures=2 Generation=0", ev)
			}
		default:
			t.Fatalf("missing independent skip event #%d", i+1)
		}
	}

	// Explicit resume: new generation, counter cleared.
	sw.Resume()
	if sw.Paused() {
		t.Fatal("still paused after Resume()")
	}
	if got := sw.Generation(); got != 1 {
		t.Fatalf("Generation() = %d after resume, want 1", got)
	}

	// A success after resume leaves the counter at 0.
	verdict = nil
	job.Run()
	if got := atomic.LoadInt32(&runs); got != 3 {
		t.Fatalf("job executed %d times after resume, want 3", got)
	}
	if got := sw.Failures(); got != 0 {
		t.Fatalf("Failures() = %d after successful run, want 0", got)
	}
}

// 成功会把连续失败计数清零，之后需要重新累计到阈值才会停用。
func TestFailureSwitchSuccessResetsCount(t *testing.T) {
	verdict := errFake
	sw := NewFailureSwitch(2, resultOf(&verdict), DiscardLogger)
	job := sw.Wrap(FuncJob(func() {}))

	job.Run() // failure, count = 1
	verdict = nil
	job.Run() // success -> count = 0
	if got := sw.Failures(); got != 0 {
		t.Fatalf("Failures() = %d after success, want 0", got)
	}
	verdict = errFake
	job.Run() // failure, count = 1
	if sw.Paused() {
		t.Fatal("paused without 2 *consecutive* failures; success did not reset")
	}
	job.Run() // failure, count = 2 -> paused
	if !sw.Paused() {
		t.Fatal("not paused after 2 consecutive failures post-reset")
	}
}

// 恢复与迟到旧任务用通道交错：旧代次迟到完成不改新代次计数。
func TestFailureSwitchLateCompletionDoesNotAffectNewGeneration(t *testing.T) {
	verdict := errFake
	sw := NewFailureSwitch(2, resultOf(&verdict), DiscardLogger)

	started := make(chan struct{})
	release := make(chan struct{})
	var calls int32
	job := sw.Wrap(FuncJob(func() {
		if atomic.AddInt32(&calls, 1) == 2 {
			close(started) // the second (old-generation) run is now in flight
			<-release      // block until the test releases it
		}
	}))

	job.Run() // failure #1 (gen 0), count = 1

	// Old-generation run starts and blocks inside the job.
	done := make(chan struct{})
	go func() { defer close(done); job.Run() }()
	<-started // the old run is now in flight in generation 0

	job.Run() // failure #2 (gen 0) -> paused
	if !sw.Paused() {
		t.Fatal("not paused after 2 consecutive failures")
	}

	// Explicit resume while the old run is still in flight: new generation.
	sw.Resume()
	if got := sw.Generation(); got != 1 {
		t.Fatalf("Generation() = %d, want 1", got)
	}

	// The old run finishes late with a failure verdict; it must be ignored.
	close(release)
	<-done
	if got := sw.Failures(); got != 0 {
		t.Fatalf("late old-generation completion changed new count to %d, want 0", got)
	}
	if sw.Paused() {
		t.Fatal("late old-generation failure re-paused the switch")
	}

	// The new generation counts independently: one failure -> count 1.
	job.Run()
	if got := sw.Failures(); got != 1 {
		t.Fatalf("Failures() = %d in new generation, want 1", got)
	}
}

// panic 可与 Recover 组合：panic 被 Recover 捕获后结果函数仍被求值，状态不卡住。
func TestFailureSwitchComposesWithRecover(t *testing.T) {
	verdict := errFake
	sw := NewFailureSwitch(2, resultOf(&verdict), DiscardLogger)
	job := NewChain(sw.Wrap, Recover(DiscardLogger)).Then(
		FuncJob(func() { panic("boom") }))

	job.Run() // panic recovered inside; result() still evaluated -> failure 1
	if got := sw.Failures(); got != 1 {
		t.Fatalf("Failures() = %d after recovered panic, want 1", got)
	}
	job.Run() // failure 2 -> paused
	if !sw.Paused() {
		t.Fatal("not paused after 2 recovered-panic failures")
	}
	job.Run() // skipped; must not deadlock or stay stuck
	select {
	case <-sw.Skipped():
	case <-time.After(time.Second):
		t.Fatal("expected skip event for paused trigger")
	}

	// Recover outside the switch: panic propagates past the switch and is
	// recovered by Recover; the switch holds no lock and is not stuck.
	sw2 := NewFailureSwitch(1, resultOf(&verdict), DiscardLogger)
	var panics int32
	job2 := NewChain(Recover(DiscardLogger), sw2.Wrap).Then(
		FuncJob(func() {
			if atomic.AddInt32(&panics, 1) == 1 {
				panic("boom")
			}
		}))
	job2.Run() // panic recovered by outer Recover; no verdict recorded
	if sw2.Paused() {
		t.Fatal("switch paused without a result-function verdict")
	}
	verdict = errFake
	job2.Run() // next run works fine: state was never stuck
	if !sw2.Paused() {
		t.Fatal("switch with threshold 1 not paused after evaluated failure")
	}
}

// 停用不删除其它 Entry，默认 cron 调度不变：被停用的 Job 后续触发被跳过，
// 而同一 Cron 上的其它 Entry 照常运行。
func TestFailureSwitchKeepsCronScheduleAndOtherEntries(t *testing.T) {
	verdict := errFake
	sw := NewFailureSwitch(2, resultOf(&verdict), DiscardLogger)

	var pausedRuns, otherRuns int32
	cron := New()
	// The failing job is wrapped explicitly; default scheduling is unchanged.
	cron.Schedule(Every(100*time.Millisecond),
		sw.Wrap(FuncJob(func() { atomic.AddInt32(&pausedRuns, 1) })))
	// A second entry that is NOT paused must keep running on schedule.
	cron.Schedule(Every(100*time.Millisecond),
		FuncJob(func() { atomic.AddInt32(&otherRuns, 1) }))
	cron.Start()
	defer cron.Stop()

	// Wait until the failing job is paused and at least one skip event fired.
	deadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case <-sw.Skipped():
			goto skipped
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for a skip event from the scheduler")
		}
		time.Sleep(10 * time.Millisecond)
	}
skipped:
	runsAtPause := atomic.LoadInt32(&pausedRuns)
	time.Sleep(350 * time.Millisecond) // several more schedule ticks
	if got := atomic.LoadInt32(&pausedRuns); got != runsAtPause {
		t.Fatalf("paused job ran %d more times; triggers must be skipped", got-runsAtPause)
	}
	if got := atomic.LoadInt32(&otherRuns); got < 2 {
		t.Fatalf("other entry ran %d times while first was paused, want >= 2", got)
	}
	if len(cron.Entries()) != 2 {
		t.Fatalf("entries = %d, want 2: pausing must not remove any entry", len(cron.Entries()))
	}
}

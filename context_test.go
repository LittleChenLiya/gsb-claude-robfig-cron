package cron

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// seqIDGen returns a deterministic counter-based ID generator and a pointer
// to the counter, so tests can assert exactly how many IDs were produced.
func seqIDGen() (IDFunc, *int) {
	var n int
	return func() RunID {
		n++
		return RunID(fmt.Sprintf("run-%d", n))
	}, &n
}

// contextCountJob is a small Job fixture that records the run identity it
// observed on each invocation.
type contextCountJob struct {
	mu   sync.Mutex
	runs int
	ids  []RunID
	wg   *sync.WaitGroup
}

func (j *contextCountJob) Run() { j.RunWithContext(context.Background()) }

func (j *contextCountJob) RunWithContext(ctx context.Context) {
	rc, ok := FromContext(ctx)
	if !ok {
		panic("missing run context")
	}
	j.mu.Lock()
	j.runs++
	j.ids = append(j.ids, rc.ID)
	j.mu.Unlock()
	if j.wg != nil {
		j.wg.Done()
	}
}

func (j *contextCountJob) snapshot() (int, []RunID) {
	j.mu.Lock()
	defer j.mu.Unlock()
	ids := make([]RunID, len(j.ids))
	copy(ids, j.ids)
	return j.runs, ids
}

// sequential triggers produce run-1 then run-2.
func TestContextSequentialRuns(t *testing.T) {
	idGen, n := seqIDGen()
	tag := Tag{Name: "plan-a"}
	var got []RunID
	notify := func(rc *RunContext, err error) { got = append(got, rc.ID) }

	wrapped := WithContext(idGen, tag, notify)(FuncJob(func() {}))
	wrapped.Run()
	wrapped.Run()

	if len(got) != 2 || got[0] != "run-1" || got[1] != "run-2" {
		t.Fatalf("expected run-1/run-2, got %v", got)
	}
	if *n != 2 {
		t.Fatalf("expected id generator called 2 times, got %d", *n)
	}
}

// two-layer wrapping still calls the ID generator only twice (once per run).
func TestContextNestedWrappingGeneratesOnce(t *testing.T) {
	idGen, n := seqIDGen()
	tag := Tag{Name: "plan-a"}

	wrapped := WithContext(idGen, tag, nil)(
		WithContext(idGen, tag, nil)(FuncJob(func() {})))
	wrapped.Run()
	wrapped.Run()

	if *n != 2 {
		t.Fatalf("expected id generator called 2 times, got %d", *n)
	}
}

// the tag handed to a job is a snapshot: mutating it does not change the
// scheduler configuration, and the next run still gets the original tag.
func TestContextTagSnapshot(t *testing.T) {
	tag := Tag{Name: "plan-a"}
	var seen []Tag
	job := ContextJobFunc(func(ctx context.Context) {
		rc, _ := FromContext(ctx)
		seen = append(seen, rc.Tag)
		rc.Tag.Name = "hacked"
	})

	wrapped := WithContext(func() RunID { return "run-x" }, tag, nil)(job)
	wrapped.Run()
	wrapped.Run()

	if len(seen) != 2 || seen[0].Name != "plan-a" || seen[1].Name != "plan-a" {
		t.Fatalf("expected snapshot plan-a on both runs, got %q/%q", seen[0].Name, seen[1].Name)
	}
	if tag.Name != "plan-a" {
		t.Fatalf("scheduler tag was mutated to %q", tag.Name)
	}
}

// panic notification and normal-end notification see the same identity the
// job observed, and the end notification fires exactly once after a panic.
func TestContextPanicAndNormalNotifySameIdentity(t *testing.T) {
	idGen, _ := seqIDGen()
	tag := Tag{Name: "plan-a"}

	type rec struct {
		id  RunID
		err error
	}
	var jobIDs []RunID
	var ends []rec
	notify := func(rc *RunContext, err error) { ends = append(ends, rec{rc.ID, err}) }

	panicking := ContextJobFunc(func(ctx context.Context) {
		rc, _ := FromContext(ctx)
		jobIDs = append(jobIDs, rc.ID)
		panic("boom")
	})
	normal := ContextJobFunc(func(ctx context.Context) {
		rc, _ := FromContext(ctx)
		jobIDs = append(jobIDs, rc.ID)
	})

	// WithContext sits inside Recover so the adapter observes the panic.
	NewChain(Recover(DiscardLogger), WithContext(idGen, tag, notify)).Then(panicking).Run()
	NewChain(Recover(DiscardLogger), WithContext(idGen, tag, notify)).Then(normal).Run()

	if len(jobIDs) != 2 || len(ends) != 2 {
		t.Fatalf("expected 2 job runs and 2 end notifications, got %d/%d", len(jobIDs), len(ends))
	}
	for i := range jobIDs {
		if jobIDs[i] != ends[i].id {
			t.Errorf("run %d: job saw %s, notification saw %s", i, jobIDs[i], ends[i].id)
		}
	}
	if ends[0].err == nil {
		t.Errorf("expected panic run to notify with error, got nil")
	}
	if ends[1].err != nil {
		t.Errorf("expected normal run to notify with nil error, got %v", ends[1].err)
	}
}

// the end notification fires exactly once after a panic.
func TestContextPanicEndNotificationCount(t *testing.T) {
	idGen, _ := seqIDGen()
	var endCount int
	notify := func(rc *RunContext, err error) { endCount++ }

	panicking := FuncJob(func() { panic("boom") })
	NewChain(Recover(DiscardLogger), WithContext(idGen, Tag{}, notify)).Then(panicking).Run()

	if endCount != 1 {
		t.Fatalf("expected 1 end notification after panic, got %d", endCount)
	}
}

// a notification function that panics must not prevent the necessary cleanup
// (the original panic still propagates to outer Recover and is logged).
func TestContextNotifyPanicDoesNotStopCleanup(t *testing.T) {
	var buf syncWriter
	logger := PrintfLogger(log.New(&buf, "", 0))

	idGen, _ := seqIDGen()
	notify := func(rc *RunContext, err error) { panic("notify boom") }
	panicking := FuncJob(func() { panic("job boom") })

	NewChain(Recover(logger), WithContext(idGen, Tag{}, notify)).Then(panicking).Run()

	if !strings.Contains(buf.String(), "job boom") {
		t.Fatalf("expected original panic to still be logged, got %q", buf.String())
	}
}

// a notification function that panics must not crash a normal run.
func TestContextNotifyPanicDoesNotCrashNormalRun(t *testing.T) {
	idGen, _ := seqIDGen()
	notify := func(rc *RunContext, err error) { panic("notify boom") }

	wrapped := WithContext(idGen, Tag{}, notify)(FuncJob(func() {}))
	wrapped.Run() // must not panic
}

// concurrent runs each get a unique identity; run under the race detector.
func TestContextRace(t *testing.T) {
	var n int64
	idGen := func() RunID {
		return RunID(fmt.Sprintf("run-%d", atomic.AddInt64(&n, 1)))
	}
	tag := Tag{Name: "plan-a"}

	var j contextCountJob
	wrapped := WithContext(idGen, tag, nil)(&j)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wrapped.Run()
		}()
	}
	wg.Wait()

	runs, ids := j.snapshot()
	if runs != 50 {
		t.Fatalf("expected 50 runs, got %d", runs)
	}
	seen := make(map[RunID]bool, len(ids))
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("duplicate run id %s", id)
		}
		seen[id] = true
	}
}

// fastSchedule fires every d, used to exercise the scheduler without waiting
// for wall-clock seconds.
type fastSchedule struct {
	d time.Duration
}

func (f fastSchedule) Next(t time.Time) time.Time { return t.Add(f.d) }

// integration with the scheduler: two sequential triggers produce run-1/run-2.
func TestContextCronIntegration(t *testing.T) {
	var n int64
	idGen := func() RunID {
		return RunID(fmt.Sprintf("run-%d", atomic.AddInt64(&n, 1)))
	}
	tag := Tag{Name: "plan-a"}

	var j contextCountJob
	wg := sync.WaitGroup{}
	wg.Add(2)
	j.wg = &wg

	c := New(
		WithParser(NewParser(Second|Minute|Hour|Dom|Month|Dow)),
		WithChain(Recover(DiscardLogger), WithContext(idGen, tag, nil)),
	)
	c.Start()
	defer c.Stop()
	c.Schedule(fastSchedule{d: 20 * time.Millisecond}, &j)

	select {
	case <-wait(&wg):
	case <-time.After(2 * time.Second):
		runs, _ := j.snapshot()
		t.Fatalf("expected 2 runs, got %d", runs)
	}

	_, ids := j.snapshot()
	if len(ids) != 2 || ids[0] != "run-1" || ids[1] != "run-2" {
		t.Fatalf("expected run-1/run-2, got %v", ids)
	}
}

// ExampleWithContext prints the order of events for two sequential runs.
func ExampleWithContext() {
	var events []string
	var n int
	idGen := func() RunID {
		n++
		return RunID(fmt.Sprintf("run-%d", n))
	}
	tag := Tag{Name: "daily-report"}
	notify := func(rc *RunContext, err error) {
		status := "ok"
		if err != nil {
			status = "panic: " + err.Error()
		}
		events = append(events, fmt.Sprintf("end   id=%s tag=%s status=%s", rc.ID, rc.Tag.Name, status))
	}
	job := ContextJobFunc(func(ctx context.Context) {
		rc, _ := FromContext(ctx)
		events = append(events, fmt.Sprintf("run   id=%s tag=%s", rc.ID, rc.Tag.Name))
	})

	wrapped := WithContext(idGen, tag, notify)(job)
	wrapped.Run()
	wrapped.Run()

	fmt.Println(strings.Join(events, "\n"))
	// Output:
	// run   id=run-1 tag=daily-report
	// end   id=run-1 tag=daily-report status=ok
	// run   id=run-2 tag=daily-report
	// end   id=run-2 tag=daily-report status=ok
}

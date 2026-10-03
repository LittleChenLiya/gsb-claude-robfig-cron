package cron

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

// countingIDGen returns a deterministic ID generator producing
// "run-1", "run-2", ... and reports how many times it was called.
// It is safe for concurrent use.
func countingIDGen() (gen func() string, calls func() int) {
	var mu sync.Mutex
	var n int
	return func() string {
			mu.Lock()
			defer mu.Unlock()
			n++
			return fmt.Sprintf("run-%d", n)
		}, func() int {
			mu.Lock()
			defer mu.Unlock()
			return n
		}
}

// recordingJob is a small job fixture that records the RunInfo it observes
// on each run.
type recordingJob struct {
	mu    sync.Mutex
	infos []RunInfo
	onRun func(info RunInfo)
}

// Run implements Job so the fixture can be scheduled directly.
func (j *recordingJob) Run() { j.RunContext(context.Background()) }

func (j *recordingJob) RunContext(ctx context.Context) {
	info, ok := RunInfoFromContext(ctx)
	if !ok {
		panic("recordingJob: no RunInfo in context")
	}
	j.mu.Lock()
	// Record a snapshot: the tags map is the job's per-run copy and the job
	// may mutate it afterwards.
	j.infos = append(j.infos, RunInfo{ID: info.ID, Tags: copyTags(info.Tags)})
	j.mu.Unlock()
	if j.onRun != nil {
		j.onRun(info)
	}
}

func (j *recordingJob) observed() []RunInfo {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]RunInfo(nil), j.infos...)
}

// notifyRecorder records end-of-run notifications.
type notifyRecorder struct {
	mu     sync.Mutex
	ids    []string
	events []RunEvent
	panics []interface{}
}

func (r *notifyRecorder) notify() RunNotifier {
	return func(info RunInfo, event RunEvent, panicValue interface{}) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.ids = append(r.ids, info.ID)
		r.events = append(r.events, event)
		r.panics = append(r.panics, panicValue)
	}
}

func (r *notifyRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

// Two sequential runs of a wrapped job observe run-1 and run-2.
func TestWithRunContextSequentialIDs(t *testing.T) {
	gen, calls := countingIDGen()
	job := &recordingJob{}
	wrapped := NewChain(WithRunContext(RunContextOptions{NewID: gen})).Then(job)

	wrapped.Run()
	wrapped.Run()

	infos := job.observed()
	if len(infos) != 2 {
		t.Fatalf("expected 2 runs, got %d", len(infos))
	}
	if infos[0].ID != "run-1" || infos[1].ID != "run-2" {
		t.Errorf("expected run-1/run-2, got %s/%s", infos[0].ID, infos[1].ID)
	}
	if calls() != 2 {
		t.Errorf("expected ID generator to be called twice, got %d", calls())
	}
}

// Two nested layers of WithRunContext still generate the identity only once
// per run: two runs mean exactly two generator calls.
func TestWithRunContextNestedGeneratesOnce(t *testing.T) {
	gen, calls := countingIDGen()
	wrapper := WithRunContext(RunContextOptions{NewID: gen})
	job := &recordingJob{}
	wrapped := NewChain(wrapper, wrapper).Then(job)

	wrapped.Run()
	wrapped.Run()

	if calls() != 2 {
		t.Errorf("expected ID generator to be called twice for two nested runs, got %d", calls())
	}
	infos := job.observed()
	if infos[0].ID != "run-1" || infos[1].ID != "run-2" {
		t.Errorf("expected run-1/run-2, got %s/%s", infos[0].ID, infos[1].ID)
	}
}

// The tags snapshot cannot be mutated by the job back into the scheduler
// configuration: mutations by the job and mutations of the original map do
// not leak into the next run.
func TestWithRunContextTagsSnapshot(t *testing.T) {
	gen, _ := countingIDGen()
	tags := map[string]string{"schedule": "every-minute", "owner": "batch"}
	job := &recordingJob{
		onRun: func(info RunInfo) {
			// The job tries to corrupt the snapshot.
			info.Tags["schedule"] = "mutated"
			info.Tags["injected"] = "x"
		},
	}
	wrapped := NewChain(WithRunContext(RunContextOptions{NewID: gen, Tags: tags})).Then(job)

	// The caller mutates its own map after wrapping; this must not leak in.
	tags["schedule"] = "changed-afterwards"

	wrapped.Run()
	wrapped.Run()

	infos := job.observed()
	want := map[string]string{"schedule": "every-minute", "owner": "batch"}
	for i, info := range infos {
		if !reflect.DeepEqual(info.Tags, want) {
			t.Errorf("run %d: expected tags %v, got %v", i+1, want, info.Tags)
		}
	}
}

// Normal completion and panic notifications observe the same identity as the
// run itself, and each run is notified exactly once.
func TestWithRunContextNotifySeesRunIdentity(t *testing.T) {
	gen, _ := countingIDGen()
	rec := &notifyRecorder{}

	okJob := &recordingJob{}
	wrapped := NewChain(WithRunContext(RunContextOptions{
		NewID:  gen,
		Notify: rec.notify(),
	})).Then(okJob)
	wrapped.Run()

	panicky := ContextFunc(func(ctx context.Context) {
		info, _ := RunInfoFromContext(ctx)
		if info.ID != "run-2" {
			t.Errorf("panicky job: expected run-2, got %s", info.ID)
		}
		panic("boom")
	})
	wrappedPanic := NewChain(WithRunContext(RunContextOptions{
		NewID:  gen,
		Notify: rec.notify(),
	})).Then(panicky)
	func() {
		defer func() {
			if r := recover(); r != "boom" {
				t.Errorf("expected panic to propagate, got %v", r)
			}
		}()
		wrappedPanic.Run()
	}()

	if rec.count() != 2 {
		t.Fatalf("expected 2 notifications, got %d", rec.count())
	}
	if rec.ids[0] != "run-1" || rec.events[0] != RunCompleted || rec.panics[0] != nil {
		t.Errorf("unexpected completion notification: id=%s event=%s panic=%v",
			rec.ids[0], rec.events[0], rec.panics[0])
	}
	if rec.ids[1] != "run-2" || rec.events[1] != RunPanicked || rec.panics[1] != "boom" {
		t.Errorf("unexpected panic notification: id=%s event=%s panic=%v",
			rec.ids[1], rec.events[1], rec.panics[1])
	}
}

// A panicking run nested under two wrappers is still notified exactly once.
func TestWithRunContextPanicNotifiedOnceWhenNested(t *testing.T) {
	gen, _ := countingIDGen()
	rec := &notifyRecorder{}
	wrapper := WithRunContext(RunContextOptions{NewID: gen, Notify: rec.notify()})
	panicky := ContextFunc(func(ctx context.Context) { panic("boom") })
	wrapped := NewChain(wrapper, wrapper).Then(panicky)

	func() {
		defer func() { _ = recover() }()
		wrapped.Run()
	}()

	if rec.count() != 1 {
		t.Fatalf("expected exactly 1 panic notification, got %d", rec.count())
	}
	if rec.events[0] != RunPanicked || rec.ids[0] != "run-1" {
		t.Errorf("unexpected notification: id=%s event=%s", rec.ids[0], rec.events[0])
	}
}

// A notifier that panics must not disrupt run finalization: a normal run
// still returns cleanly, and a panicking run still re-throws its original
// panic value.
func TestWithRunContextNotifierPanicDoesNotBlockCleanup(t *testing.T) {
	gen, _ := countingIDGen()
	badNotify := func(RunInfo, RunEvent, interface{}) { panic("notifier boom") }

	okJob := &recordingJob{}
	wrapped := NewChain(WithRunContext(RunContextOptions{
		NewID:  gen,
		Notify: badNotify,
	})).Then(okJob)
	wrapped.Run() // must not panic
	if len(okJob.observed()) != 1 {
		t.Fatal("job did not run")
	}

	panicky := ContextFunc(func(ctx context.Context) { panic("job boom") })
	wrappedPanic := NewChain(WithRunContext(RunContextOptions{
		NewID:  gen,
		Notify: badNotify,
	})).Then(panicky)
	defer func() {
		if r := recover(); r != "job boom" {
			t.Errorf("expected original panic to propagate past failing notifier, got %v", r)
		}
	}()
	wrappedPanic.Run()
}

// WithRunContext requires an ID generator.
func TestWithRunContextRequiresIDGenerator(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("expected panic when NewID is nil")
		}
	}()
	WithRunContext(RunContextOptions{})
}

// End-to-end: a Cron runner with the wrapper installed via WithChain assigns
// sequential identities and notifies once per run. Run with -race.
func TestWithRunContextWithRunningCron(t *testing.T) {
	gen, _ := countingIDGen()
	notified := make(chan RunInfo, 2)
	c := New(WithChain(WithRunContext(RunContextOptions{
		NewID: gen,
		Tags:  map[string]string{"schedule": "every-second"},
		Notify: func(info RunInfo, event RunEvent, _ interface{}) {
			if event != RunCompleted {
				t.Errorf("unexpected event: %s", event)
			}
			notified <- info
		},
	})))
	if _, err := c.AddFunc("@every 1s", func() {}); err != nil {
		t.Fatal(err)
	}
	c.Start()
	defer c.Stop()

	for i, want := range []string{"run-1", "run-2"} {
		select {
		case info := <-notified:
			if info.ID != want {
				t.Errorf("notification %d: expected %s, got %s", i+1, want, info.ID)
			}
			if info.Tag("schedule") != "every-second" {
				t.Errorf("notification %d: unexpected tags %v", i+1, info.Tags)
			}
		case <-time.After(OneSecond):
			t.Fatalf("timed out waiting for notification %d", i+1)
		}
	}
}

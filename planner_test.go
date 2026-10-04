package cron

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// countingJob records how many times Run is invoked, without depending on a
// running scheduler.
type countingJob struct {
	name  string
	count *int64
}

func (j countingJob) Run() {
	atomic.AddInt64(j.count, 1)
}

func newCountingJob(name string, counter *int64) Job {
	return countingJob{name: name, count: counter}
}

// TestConfigPlannerAcceptance covers the main acceptance scenario: two valid
// expressions produce a brand-new, not-started Cron whose jobs never run,
// while the original Cron keeps its entry.
func TestConfigPlannerAcceptance(t *testing.T) {
	var alphaRuns, betaRuns int64

	old := New(WithLogger(DiscardLogger))
	old.AddFunc("@every 1h", func() {})

	planner := NewConfigPlanner()
	planner.Add("alpha", "@every 1h", newCountingJob("alpha", &alphaRuns))
	planner.Add("beta", "0 0 * * *", newCountingJob("beta", &betaRuns))

	plan, err := planner.Prepare()
	if err != nil {
		t.Fatalf("expected valid plan, got error: %v", err)
	}
	if plan == nil {
		t.Fatal("expected a non-nil plan")
	}

	fresh := plan.Apply()
	if fresh == old {
		t.Fatal("Apply must create a new Cron instance, got the old one")
	}
	if fresh.running {
		t.Fatal("the new Cron must not be started by Apply")
	}

	entries := fresh.Entries()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries on the new Cron, got %d", len(entries))
	}
	for _, e := range entries {
		if !e.Next.IsZero() {
			t.Errorf("entry %d must not be scheduled until Start, got Next=%v", e.ID, e.Next)
		}
	}

	if got := atomic.LoadInt64(&alphaRuns); got != 0 {
		t.Errorf("alpha ran %d times before Start; want 0", got)
	}
	if got := atomic.LoadInt64(&betaRuns); got != 0 {
		t.Errorf("beta ran %d times before Start; want 0", got)
	}

	// The old instance is untouched and still holds its original entry.
	if oldEntries := old.Entries(); len(oldEntries) != 1 {
		t.Errorf("expected the old Cron to keep its 1 entry, got %d", len(oldEntries))
	}
	if old.running {
		t.Error("the old Cron must not have been started by the planner")
	}
}

// TestConfigPlannerRejectsInvalidBatch ensures that one invalid expression
// rejects the whole batch: no plan and nothing applied.
func TestConfigPlannerRejectsInvalidBatch(t *testing.T) {
	planner := NewConfigPlanner()
	planner.Add("good-one", "@daily", newCountingJob("good-one", new(int64)))
	planner.Add("good-two", "0 0 1 * *", newCountingJob("good-two", new(int64)))
	planner.Add("bad-one", "this is not a cron expr", newCountingJob("bad-one", new(int64)))

	plan, err := planner.Prepare()
	if err == nil {
		t.Fatal("expected an error for the invalid expression, got nil")
	}
	if plan != nil {
		t.Fatalf("expected no partial plan, got %#v", plan)
	}
	if !strings.Contains(err.Error(), "bad-one") {
		t.Errorf("error should name the offending job, got: %v", err)
	}
}

// TestConfigPlannerDuplicateNameFirst verifies that a duplicate name is
// reported as a single conflict and takes precedence over expression errors.
func TestConfigPlannerDuplicateNameFirst(t *testing.T) {
	planner := NewConfigPlanner()
	planner.Add("dup", "@daily", newCountingJob("dup", new(int64)))
	planner.Add("other", "@daily", newCountingJob("other", new(int64)))
	// Same name again, and with an invalid spec: the conflict must win.
	planner.Add("dup", "totally invalid", newCountingJob("dup", new(int64)))

	plan, err := planner.Prepare()
	if err == nil {
		t.Fatal("expected a duplicate-name error, got nil")
	}
	if plan != nil {
		t.Fatal("expected no plan on name conflict")
	}
	if !strings.Contains(err.Error(), `duplicate job name "dup"`) {
		t.Errorf("expected a single duplicate-name conflict, got: %v", err)
	}
	if strings.Contains(err.Error(), "invalid schedule") {
		t.Errorf("name conflict must be reported before parse errors, got: %v", err)
	}
}

// TestConfigPlannerCustomParser verifies the planner honors a parser option,
// including seconds and optional-seconds parsers.
func TestConfigPlannerCustomParser(t *testing.T) {
	secondsParser := NewParser(Second | Minute | Hour | Dom | Month | Dow | Descriptor)
	planner := NewConfigPlanner(WithParser(secondsParser))
	planner.Add("seconds", "0 * * * * *", newCountingJob("seconds", new(int64)))

	plan, err := planner.Prepare()
	if err != nil {
		t.Fatalf("seconds expression should be valid with custom parser: %v", err)
	}
	if got := plan.Labels(); len(got) != 1 || got[0] != "seconds" {
		t.Errorf("unexpected labels: %#v", got)
	}

	// The same expression must fail under the standard parser.
	std := NewConfigPlanner()
	std.Add("seconds", "0 * * * * *", newCountingJob("seconds", new(int64)))
	if _, err := std.Prepare(); err == nil {
		t.Error("expected standard parser to reject a six-field expression")
	}

	// An optional-seconds parser accepts both five- and six-field specs.
	optional := NewConfigPlanner(WithParser(NewParser(
		SecondOptional | Minute | Hour | Dom | Month | Dow | Descriptor,
	)))
	optional.Add("five", "0 * * * *", newCountingJob("five", new(int64)))
	optional.Add("six", "30 0 * * * *", newCountingJob("six", new(int64)))
	if _, err := optional.Prepare(); err != nil {
		t.Errorf("optional-seconds parser should accept both field counts: %v", err)
	}
}

// TestConfigPlannerEmptyBatch covers preparing and applying an empty batch.
func TestConfigPlannerEmptyBatch(t *testing.T) {
	planner := NewConfigPlanner()

	plan, err := planner.Prepare()
	if err != nil {
		t.Fatalf("empty batch should be valid, got: %v", err)
	}
	if plan == nil {
		t.Fatal("expected a non-nil empty plan")
	}
	if labels := plan.Labels(); len(labels) != 0 {
		t.Errorf("expected no labels, got %#v", labels)
	}
	if entries := plan.Entries(); len(entries) != 0 {
		t.Errorf("expected no entries, got %#v", entries)
	}

	fresh := plan.Apply()
	if fresh.running {
		t.Error("empty plan must not create a running Cron")
	}
	if entries := fresh.Entries(); len(entries) != 0 {
		t.Errorf("expected no entries on the new Cron, got %d", len(entries))
	}
}

// TestPlanCopiesIndependent ensures labels and the result list are returned as
// independent copies, and that each Apply yields an independent Cron.
func TestPlanCopiesIndependent(t *testing.T) {
	var runs int64
	planner := NewConfigPlanner()
	planner.Add("one", "@hourly", newCountingJob("one", &runs))
	planner.Add("two", "@daily", newCountingJob("two", &runs))

	plan, err := planner.Prepare()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Every planned entry carries the parsed schedule and the explicit job.
	entries := plan.Entries()
	if len(entries) != 2 {
		t.Fatalf("expected 2 planned entries, got %d", len(entries))
	}
	for i, e := range entries {
		if e.ID != EntryID(i+1) {
			t.Errorf("entry %d: expected ID %d, got %d", i, i+1, e.ID)
		}
		if e.Schedule == nil {
			t.Errorf("entry %q has no parsed schedule", e.Name)
		}
		if e.Job == nil {
			t.Errorf("entry %q lost its job", e.Name)
		}
		if e.Schedule.Next(time.Now()).IsZero() {
			t.Errorf("entry %q schedule has no next activation", e.Name)
		}
	}

	// Mutating returned copies must not affect the plan.
	labels := plan.Labels()
	labels[0] = "mutated"
	entries[0] = PlannedEntry{}
	entries[1].Name = "mutated"

	again := plan.Entries()
	if again[0].Name != "one" || again[1].Name != "two" {
		t.Errorf("plan entries were mutated through the returned copy: %#v", again)
	}
	if got := plan.Labels(); got[0] != "one" || got[1] != "two" {
		t.Errorf("plan labels were mutated through the returned copy: %#v", got)
	}

	// Repeated application creates independent, not-started instances.
	first := plan.Apply()
	second := plan.Apply(WithLocation(time.UTC))
	if first == second {
		t.Fatal("expected separate Cron instances from repeated Apply")
	}
	if len(first.Entries()) != 2 || len(second.Entries()) != 2 {
		t.Fatalf("expected 2 entries on each instance, got %d and %d",
			len(first.Entries()), len(second.Entries()))
	}
	if second.Location() != time.UTC {
		t.Error("extra Apply options should configure only the new instance")
	}
	if first.Location() == time.UTC {
		t.Error("extra Apply options leaked into another instance")
	}
}

// TestPrepareDoesNotRunJobs verifies neither preparation nor application
// invokes a job; only an explicit manual Run does.
func TestPrepareDoesNotRunJobs(t *testing.T) {
	var runs int64
	job := newCountingJob("manual", &runs)

	planner := NewConfigPlanner()
	planner.Add("manual", "@every 1m", job)

	plan, err := planner.Prepare()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fresh := plan.Apply()
	if got := atomic.LoadInt64(&runs); got != 0 {
		t.Fatalf("job ran %d times during prepare/apply", got)
	}
	if fresh.running {
		t.Fatal("the new Cron must remain stopped")
	}

	// The caller may trigger a planned job manually, independent of the
	// scheduler.
	plan.Entries()[0].Job.Run()
	if got := atomic.LoadInt64(&runs); got != 1 {
		t.Fatalf("expected exactly 1 run after manual trigger, got %d", got)
	}
}

package cron

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// countingJob records how many times Run is invoked.
type countingJob struct {
	runs int64
}

func (j *countingJob) Run() {
	atomic.AddInt64(&j.runs, 1)
}

func (j *countingJob) count() int64 {
	return atomic.LoadInt64(&j.runs)
}

// Two valid expressions produce a new, unstarted instance whose jobs have a
// run count of 0, while the original instance keeps its entries and keeps
// running.
func TestPlanValidAdoption(t *testing.T) {
	old := newWithSeconds()
	oldJob := &countingJob{}
	old.AddJob("* * * * * *", oldJob) // fires every second
	old.Start()
	defer old.Stop()

	j1 := &countingJob{}
	j2 := &countingJob{}
	plan, err := NewPlan(
		PlanJob("a", "@every 1s", j1),
		PlanJob("b", "0 * * * *", j2),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan == nil {
		t.Fatal("expected a non-nil plan")
	}

	fresh := New()
	ids := fresh.SchedulePlan(plan)
	if len(ids) != 2 {
		t.Fatalf("expected 2 ids, got %d", len(ids))
	}

	// Wait long enough for the new cron to have fired if it were running,
	// and for the old cron to fire at least once.
	time.Sleep(1100 * time.Millisecond)

	if j1.count() != 0 || j2.count() != 0 {
		t.Errorf("planned jobs ran on an unstarted cron: j1=%d j2=%d", j1.count(), j2.count())
	}
	if oldJob.count() == 0 {
		t.Error("original cron did not fire; it should still be running")
	}
	if entries := old.Entries(); len(entries) != 1 {
		t.Errorf("original cron lost its entries: got %d", len(entries))
	}
	if entries := fresh.Entries(); len(entries) != 2 {
		t.Errorf("new cron should have 2 entries, got %d", len(entries))
	}
}

// A single invalid expression rejects the whole batch: no plan is returned.
func TestPlanInvalidRejectsAll(t *testing.T) {
	j1 := &countingJob{}
	j2 := &countingJob{}
	j3 := &countingJob{}
	plan, err := NewPlan(
		PlanJob("a", "@every 1s", j1),
		PlanJob("b", "0 * * * *", j2),
		PlanJob("c", "not a spec", j3),
	)
	if err == nil {
		t.Fatal("expected an error for invalid spec, got nil")
	}
	if plan != nil {
		t.Fatalf("expected nil plan on invalid spec, got %+v", plan)
	}
	if j1.count() != 0 || j2.count() != 0 || j3.count() != 0 {
		t.Error("jobs ran during plan preparation")
	}
}

// A duplicate name is reported as a conflict, and the conflict is reported
// before any spec is parsed (so it wins over a coexisting parse error).
func TestPlanDuplicateNameReportedFirst(t *testing.T) {
	plan, err := NewPlan(
		PlanJob("dup", "not a spec", &countingJob{}),
		PlanJob("dup", "* * * * *", &countingJob{}),
	)
	if err == nil {
		t.Fatal("expected a duplicate-name error, got nil")
	}
	if plan != nil {
		t.Fatal("expected nil plan on duplicate name")
	}
	if !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("expected duplicate conflict error, got: %v", err)
	}
}

// A plain duplicate of two otherwise-valid specs is also rejected.
func TestPlanDuplicateValidSpecs(t *testing.T) {
	plan, err := NewPlan(
		PlanJob("dup", "@every 1s", &countingJob{}),
		PlanJob("dup", "@every 2s", &countingJob{}),
	)
	if err == nil {
		t.Fatal("expected a duplicate-name error, got nil")
	}
	if plan != nil {
		t.Fatal("expected nil plan on duplicate name")
	}
}

// WithPlanParser overrides the parser used to parse plan specs.
func TestPlanParserOption(t *testing.T) {
	seconds := NewParser(Second | Minute | Hour | Dom | Month | Dow)
	plan, err := NewPlan(
		WithPlanParser(seconds),
		PlanJob("s", "* * * * * *", &countingJob{}),
	)
	if err != nil {
		t.Fatalf("unexpected error with seconds parser: %v", err)
	}
	if len(plan.Entries()) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(plan.Entries()))
	}

	// The same 6-field spec is rejected by the standard parser.
	_, err = NewPlan(
		PlanJob("s", "* * * * * *", &countingJob{}),
	)
	if err == nil {
		t.Error("expected error parsing 6-field spec with standard parser")
	}
}

// A nil plan parser is rejected.
func TestPlanNilParser(t *testing.T) {
	_, err := NewPlan(
		WithPlanParser(nil),
		PlanJob("a", "@every 1s", &countingJob{}),
	)
	if err == nil {
		t.Error("expected error for nil parser")
	}
}

// An empty plan is valid and adopts nothing.
func TestPlanEmpty(t *testing.T) {
	plan, err := NewPlan()
	if err != nil {
		t.Fatalf("unexpected error for empty plan: %v", err)
	}
	if plan == nil {
		t.Fatal("expected non-nil empty plan")
	}
	if len(plan.Labels()) != 0 || len(plan.Entries()) != 0 {
		t.Error("expected empty plan")
	}
	c := New()
	ids := c.SchedulePlan(plan)
	if len(ids) != 0 {
		t.Errorf("expected no ids, got %d", len(ids))
	}
	if len(c.Entries()) != 0 {
		t.Error("expected empty cron")
	}
}

// Labels and Entries return independent copies; adopting the same plan onto
// two crons yields independent entry sets.
func TestPlanCopiesAreIndependent(t *testing.T) {
	plan, err := NewPlan(
		PlanJob("a", "@every 1s", &countingJob{}),
		PlanJob("b", "0 * * * *", &countingJob{}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	labels := plan.Labels()
	labels[0] = "mutated"
	if got := plan.Labels()[0]; got != "a" {
		t.Errorf("plan labels mutated through copy: got %q", got)
	}

	entries := plan.Entries()
	entries[0] = PlanEntry{Name: "mutated"}
	if got := plan.Entries()[0].Name; got != "a" {
		t.Errorf("plan entries mutated through copy: got %q", got)
	}

	c1 := New()
	c2 := New()
	c1.SchedulePlan(plan)
	c2.SchedulePlan(plan)
	if len(c1.Entries()) != 2 || len(c2.Entries()) != 2 {
		t.Fatal("expected both crons to have 2 entries")
	}
	c1.Remove(c1.Entries()[0].ID)
	if len(c1.Entries()) != 1 {
		t.Errorf("c1 should have 1 entry after remove, got %d", len(c1.Entries()))
	}
	if len(c2.Entries()) != 2 {
		t.Errorf("c2 should be unaffected by c1 remove, got %d", len(c2.Entries()))
	}
}

// Job.Run is never invoked while a plan is prepared or adopted.
func TestPlanPreparationDoesNotRunJobs(t *testing.T) {
	j := &countingJob{}
	plan, err := NewPlan(
		PlanJob("a", "@every 1s", j),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := New()
	c.SchedulePlan(plan)
	if j.count() != 0 {
		t.Errorf("job ran during preparation/adoption: count=%d", j.count())
	}
}

// PlanFunc registers a plain func.
func TestPlanFunc(t *testing.T) {
	plan, err := NewPlan(
		PlanFunc("f", "@every 1s", func() {}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(plan.Entries()) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(plan.Entries()))
	}
	if plan.Entries()[0].Name != "f" {
		t.Errorf("expected name f, got %q", plan.Entries()[0].Name)
	}
}

// An empty name or nil job is rejected.
func TestPlanInvalidOptions(t *testing.T) {
	if _, err := NewPlan(PlanJob("", "@every 1s", &countingJob{})); err == nil {
		t.Error("expected error for empty name")
	}
	if _, err := NewPlan(PlanJob("a", "@every 1s", nil)); err == nil {
		t.Error("expected error for nil job")
	}
}

// String renders the plan deterministically.
func TestPlanString(t *testing.T) {
	plan, err := NewPlan(
		PlanJob("heartbeat", "@every 1s", &countingJob{}),
		PlanJob("report", "0 * * * *", &countingJob{}),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "Plan with 2 entries:\n" +
		"  1. heartbeat -> @every 1s\n" +
		"  2. report -> 0 * * * *\n"
	if got := plan.String(); got != want {
		t.Errorf("plan string mismatch:\ngot:  %q\nwant: %q", got, want)
	}
}

package cron

import (
	"fmt"
	"strings"
)

// PlanEntry is a single named job within a Plan. It pairs the Job explicitly
// given to the planner with the name it was registered under, the spec it was
// registered with, and the pre-parsed Schedule produced for that spec.
type PlanEntry struct {
	// Name is the caller-provided label for this entry. It is unique within a
	// Plan.
	Name string

	// Spec is the cron expression that was parsed to produce Schedule.
	Spec string

	// Job is the work to run when the schedule fires. It is never invoked
	// during plan preparation.
	Job Job

	// Schedule is the result of parsing Spec with the plan's parser. It is
	// stored so that a Cron can adopt the entry without parsing it again.
	Schedule Schedule
}

// Plan is a validated batch of named jobs whose schedules have all been
// pre-parsed up front. A Plan is immutable once built: it owns independent
// copies of the entry labels and parse results, so later changes to returned
// slices or to the jobs' schedules do not affect it.
//
// A Plan is safe to hand to multiple Cron instances; each adoption creates
// independent Cron entries without starting the scheduler or touching any
// other instance.
type Plan struct {
	// labels is the ordered, de-duplicated set of entry names. It is kept as
	// a separate slice so that it can be copied independently of the entries.
	labels []string

	// entries is the ordered list of pre-parsed plan entries.
	entries []PlanEntry
}

// planBuilder accumulates PlanOptions before they are validated and turned
// into a Plan.
type planBuilder struct {
	parser  ScheduleParser
	labels  []string
	entries []PlanEntry
}

// PlanOption configures a Plan.
type PlanOption func(*planBuilder) error

// WithPlanParser sets the parser used to parse the specs of plan entries.
// When not provided, the standard parser is used.
func WithPlanParser(p ScheduleParser) PlanOption {
	return func(b *planBuilder) error {
		if p == nil {
			return fmt.Errorf("plan parser must not be nil")
		}
		b.parser = p
		return nil
	}
}

// PlanJob registers the given Job in the plan under the given name, using
// spec as its schedule expression. The spec is not parsed until NewPlan is
// called, and the job is never run during preparation.
func PlanJob(name, spec string, job Job) PlanOption {
	return func(b *planBuilder) error {
		if name == "" {
			return fmt.Errorf("plan entry name must not be empty")
		}
		if job == nil {
			return fmt.Errorf("plan entry %q: job must not be nil", name)
		}
		b.labels = append(b.labels, name)
		b.entries = append(b.entries, PlanEntry{Name: name, Spec: spec, Job: job})
		return nil
	}
}

// PlanFunc registers the given func in the plan under the given name, using
// spec as its schedule expression.
func PlanFunc(name, spec string, cmd func()) PlanOption {
	return PlanJob(name, spec, FuncJob(cmd))
}

// NewPlan constructs a Plan by parsing every registered spec up front.
//
// The entire batch is rejected (returning a nil plan and an error) if any name
// is duplicated or any spec fails to parse. Name conflicts are detected and
// reported before any spec is parsed, so a conflict is always reported even
// when some specs are also invalid.
//
// On success the plan owns independent copies of the labels and parse
// results; later mutation of slices returned by Labels or Entries does not
// affect the plan.
func NewPlan(opts ...PlanOption) (*Plan, error) {
	b := &planBuilder{parser: standardParser}
	for _, opt := range opts {
		if err := opt(b); err != nil {
			return nil, err
		}
	}

	// Validate names first: a duplicate is a conflict that must be reported
	// before any parsing is attempted.
	seen := make(map[string]int, len(b.labels))
	for i, name := range b.labels {
		if prev, ok := seen[name]; ok {
			return nil, fmt.Errorf("duplicate plan entry name %q: entries %d and %d", name, prev+1, i+1)
		}
		seen[name] = i
	}

	// Parse every spec. The plan gets its own copies of the labels and
	// results so it cannot be aliased to the builder's internal slices.
	labels := make([]string, len(b.labels))
	entries := make([]PlanEntry, len(b.entries))
	for i, e := range b.entries {
		schedule, err := b.parser.Parse(e.Spec)
		if err != nil {
			return nil, fmt.Errorf("plan entry %q: %v", e.Name, err)
		}
		labels[i] = e.Name
		entries[i] = PlanEntry{
			Name:     e.Name,
			Spec:     e.Spec,
			Job:      e.Job,
			Schedule: schedule,
		}
	}
	return &Plan{labels: labels, entries: entries}, nil
}

// Labels returns a copy of the entry names in plan order.
func (p *Plan) Labels() []string {
	return append([]string(nil), p.labels...)
}

// Entries returns a copy of the planned entries. Mutating the returned slice
// does not affect the plan.
func (p *Plan) Entries() []PlanEntry {
	return append([]PlanEntry(nil), p.entries...)
}

// String renders the plan as a human-readable list of numbered entries.
func (p *Plan) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "Plan with %d entries:\n", len(p.entries))
	for i, e := range p.entries {
		fmt.Fprintf(&b, "  %d. %s -> %s\n", i+1, e.Name, e.Spec)
	}
	return b.String()
}

// SchedulePlan adds every entry of the plan to c, returning the assigned
// entry IDs in plan order.
//
// The cron is not started and no job is run: this only adopts the pre-parsed
// schedules. The plan is not modified, and entries added to c are independent
// of entries in any other Cron instance.
func (c *Cron) SchedulePlan(p *Plan) []EntryID {
	ids := make([]EntryID, len(p.entries))
	for i, e := range p.entries {
		ids[i] = c.Schedule(e.Schedule, e.Job)
	}
	return ids
}

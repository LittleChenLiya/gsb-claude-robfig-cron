package cron

import "fmt"

// JobConfig is one job to be validated and staged by a ConfigPlanner.
//
// Name is the explicit label assigned to the job; it must be unique within a
// single plan. Spec is the cron expression parsed when the plan is prepared.
// Job is the job that will eventually run; it is never invoked while a plan
// is being prepared.
type JobConfig struct {
	// Name is the explicit, caller-assigned label of the job.
	Name string

	// Spec is the cron schedule expression to validate up front.
	Spec string

	// Job is the job staged for the new Cron instance. It is not run while
	// the configuration is being prepared.
	Job Job
}

// PlannedEntry is one validated item in a Plan: the caller-provided job, its
// name, and the schedule produced by parsing its expression.
type PlannedEntry struct {
	// ID is the Cron-assigned ID the entry will have once the plan is
	// applied (IDs start at one in addition order).
	ID EntryID

	// Name is the explicit label given to the job.
	Name string

	// Spec is the cron expression that was parsed.
	Spec string

	// Schedule is the result of parsing Spec.
	Schedule Schedule

	// Job is the explicitly submitted job.
	Job Job
}

// Plan is the immutable result of preparing a batch of JobConfigs: every
// expression has been parsed and every name is unique. Apply creates a fresh,
// not-started Cron holding the planned jobs; it never touches a previously
// created or running Cron instance.
type Plan struct {
	opts    []Option
	entries []PlannedEntry
}

// ConfigPlanner accumulates JobConfigs and prepares them as a single atomic
// batch. A failed Prepare rejects the whole batch: no plan is produced and no
// Cron instance is modified.
type ConfigPlanner struct {
	opts    []Option
	configs []JobConfig
}

// NewConfigPlanner returns a ConfigPlanner using the given Cron options. The
// options (for example WithParser or WithLocation) configure both the parser
// used while preparing a plan and every Cron instance created by Plan.Apply.
//
// The planner does not start anything: it never creates a running scheduler
// and it never steals entries from an existing Cron instance.
func NewConfigPlanner(opts ...Option) *ConfigPlanner {
	return &ConfigPlanner{
		opts: append([]Option(nil), opts...),
	}
}

// Add stages a job configuration, recording the explicit job, name, and the
// expression to parse later. It does not parse anything and never calls Run.
func (p *ConfigPlanner) Add(name, spec string, job Job) {
	p.configs = append(p.configs, JobConfig{
		Name: name,
		Spec: spec,
		Job:  job,
	})
}

// Prepare validates the whole staged batch and returns an independent Plan.
//
// All names are checked for duplicates first: if the same name appears more
// than once, the first duplicate is reported (and duplicate-name errors take
// precedence over expression errors). Only when every name is unique are the
// expressions parsed; if any expression is invalid, the entire batch is
// rejected and no partial plan is returned. Job.Run is never invoked.
func (p *ConfigPlanner) Prepare() (*Plan, error) {
	// Duplicate names are rejected before any parsing takes place, so a
	// conflict is always reported ahead of a bad expression.
	seen := make(map[string]int, len(p.configs))
	for i, cfg := range p.configs {
		if first, ok := seen[cfg.Name]; ok {
			return nil, fmt.Errorf("cron: duplicate job name %q (configs %d and %d)", cfg.Name, first, i)
		}
		seen[cfg.Name] = i
	}

	// A throwaway, never-started Cron supplies the configured parser and
	// location without touching any instance the caller may be running.
	c := New(p.opts...)

	entries := make([]PlannedEntry, 0, len(p.configs))
	for _, cfg := range p.configs {
		schedule, err := c.parser.Parse(cfg.Spec)
		if err != nil {
			// Discard everything parsed so far: never return a half plan.
			return nil, fmt.Errorf("cron: invalid schedule for job %q (%q): %v", cfg.Name, cfg.Spec, err)
		}
		entries = append(entries, PlannedEntry{
			ID:       EntryID(len(entries) + 1),
			Name:     cfg.Name,
			Spec:     cfg.Spec,
			Schedule: schedule,
			Job:      cfg.Job,
		})
	}

	return &Plan{
		opts:    append([]Option(nil), p.opts...),
		entries: entries,
	}, nil
}

// Labels returns an independent copy of the planned job names, in the order
// the jobs were added. Mutating the returned slice never affects the plan.
func (p *Plan) Labels() []string {
	labels := make([]string, len(p.entries))
	for i, e := range p.entries {
		labels[i] = e.Name
	}
	return labels
}

// Entries returns an independent copy of the planned result list. Mutating the
// returned slice or its elements never affects the plan.
func (p *Plan) Entries() []PlannedEntry {
	entries := make([]PlannedEntry, len(p.entries))
	copy(entries, p.entries)
	return entries
}

// Apply creates a brand-new, not-started Cron containing the planned jobs and
// returns it. The jobs are scheduled from the schedules already parsed, so no
// expression can fail here. The extra options are applied after the planner's
// options and configure only the new instance.
//
// Apply does not start the returned Cron (call Start or Run when ready) and
// never moves or removes entries from any other Cron instance. It may be
// called repeatedly to create independent instances from the same plan.
func (p *Plan) Apply(extraOpts ...Option) *Cron {
	opts := make([]Option, 0, len(p.opts)+len(extraOpts))
	opts = append(opts, p.opts...)
	opts = append(opts, extraOpts...)

	c := New(opts...)
	for _, e := range p.entries {
		c.Schedule(e.Schedule, e.Job)
	}
	return c
}

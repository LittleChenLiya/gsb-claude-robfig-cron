package cron

import "context"

// RunInfo carries the identity and schedule tags of a single run of a job.
//
// A RunInfo is created once per run by the WithRunContext wrapper: the ID is
// produced by the caller-supplied generator, and Tags is a per-run snapshot
// of the schedule tags, so a job may read (or even mutate) its copy without
// affecting the scheduler's configuration or subsequent runs.
type RunInfo struct {
	// ID is the unique identity of this run, e.g. "run-1".
	ID string

	// Tags is a snapshot of the schedule tags for this run.
	Tags map[string]string
}

// Tag returns the value of the schedule tag with the given key, or the empty
// string if the tag is not present.
func (r RunInfo) Tag(key string) string {
	return r.Tags[key]
}

// runInfoContextKey is the context key under which the RunInfo is stored.
type runInfoContextKey struct{}

// RunInfoFromContext returns the RunInfo of the current run and whether one
// was found. A RunInfo is present when the job is wrapped by WithRunContext.
func RunInfoFromContext(ctx context.Context) (RunInfo, bool) {
	info, ok := ctx.Value(runInfoContextKey{}).(RunInfo)
	return info, ok
}

// ContextJob is a Job that receives the context of the current run. Jobs that
// want access to the RunInfo installed by WithRunContext should implement
// this interface (or use ContextFunc) and call RunInfoFromContext.
type ContextJob interface {
	RunContext(ctx context.Context)
}

// ContextFunc adapts a func(context.Context) into a Job that is also a
// ContextJob, so it can be scheduled directly and still observe the run
// context installed by wrappers.
type ContextFunc func(ctx context.Context)

// RunContext implements ContextJob.
func (f ContextFunc) RunContext(ctx context.Context) { f(ctx) }

// Run implements Job, invoking the function with a background context.
func (f ContextFunc) Run() { f(context.Background()) }

// RunEvent identifies how a run of a job ended.
type RunEvent int

const (
	// RunCompleted indicates the run returned normally.
	RunCompleted RunEvent = iota
	// RunPanicked indicates the run panicked.
	RunPanicked
)

// String returns a human-readable name for the event, e.g. "completed".
func (e RunEvent) String() string {
	switch e {
	case RunCompleted:
		return "completed"
	case RunPanicked:
		return "panicked"
	default:
		return "unknown"
	}
}

// RunNotifier is invoked exactly once when a run of the job ends, whether the
// run completed normally or panicked. It always observes the same RunInfo
// (identity and tags) that the run itself saw.
//
// panicValue holds the recovered panic value when event is RunPanicked, and
// is nil otherwise.
//
// A panic raised by the notifier itself is recovered and discarded, so a
// misbehaving notifier cannot disrupt the finalization of the run (in
// particular, the original panic is still re-thrown afterwards).
type RunNotifier func(info RunInfo, event RunEvent, panicValue interface{})

// RunContextOptions configures the WithRunContext wrapper.
type RunContextOptions struct {
	// NewID generates the identity for each run. It is called exactly once
	// per run, even when the wrapper is nested. It must not be nil.
	//
	// Callers supply the generator so tests can be deterministic (e.g. a
	// counter producing "run-1", "run-2", ...) instead of relying on random
	// values.
	NewID func() string

	// Tags are schedule tags attached to every run of the job. The map is
	// copied when the wrapper is created and again for every run, so neither
	// the job nor later mutations of the original map can change what other
	// runs observe.
	Tags map[string]string

	// Notify, if non-nil, is called exactly once when each run ends. See
	// RunNotifier for the contract.
	Notify RunNotifier
}

// WithRunContext returns a JobWrapper that gives each run of the job a unique
// identity and a snapshot of schedule tags, and notifies when the run ends.
//
// For every run, the wrapper:
//
//   - generates a run identity by calling opts.NewID exactly once;
//   - attaches a RunInfo{ID, Tags} to the run's context, retrievable by the
//     job via RunInfoFromContext (the job must be a ContextJob, e.g. a
//     ContextFunc, to receive the context);
//   - calls opts.Notify exactly once when the run ends, with the same
//     RunInfo, whether the run completed normally or panicked.
//
// When several WithRunContext wrappers are nested (directly or via a Chain),
// only the outermost one generates an identity and notifies; inner wrappers
// reuse the identity already present in the context, so NewID is still called
// only once per run.
//
// Panics of the wrapped job are re-thrown after the notification, so they
// compose with the Recover wrapper as usual. Panics of the notifier itself
// are swallowed so that run finalization is never disrupted.
func WithRunContext(opts RunContextOptions) JobWrapper {
	if opts.NewID == nil {
		panic("cron: WithRunContext requires a non-nil NewID generator")
	}
	return func(j Job) Job {
		return runContextJob{
			job:    j,
			newID:  opts.NewID,
			tags:   copyTags(opts.Tags),
			notify: opts.Notify,
		}
	}
}

// runContextJob is the Job produced by WithRunContext.
type runContextJob struct {
	job    Job
	newID  func() string
	tags   map[string]string
	notify RunNotifier
}

// Run implements Job, starting a fresh run from a background context.
func (r runContextJob) Run() {
	r.RunContext(context.Background())
}

// RunContext implements ContextJob so that nested wrappers compose: an inner
// runContextJob receives the context of the outer one and reuses its RunInfo
// instead of generating a second identity.
func (r runContextJob) RunContext(ctx context.Context) {
	if _, ok := RunInfoFromContext(ctx); ok {
		// An outer WithRunContext wrapper already established the identity
		// for this run; reuse it and leave notification to that wrapper.
		runInContext(ctx, r.job)
		return
	}
	info := RunInfo{
		ID:   r.newID(),
		Tags: copyTags(r.tags),
	}
	ctx = context.WithValue(ctx, runInfoContextKey{}, info)
	defer r.finish(info)
	runInContext(ctx, r.job)
}

// finish notifies the end of the run exactly once, with the same RunInfo the
// run observed, and re-throws a job panic after the notification.
func (r runContextJob) finish(info RunInfo) {
	event := RunCompleted
	var panicValue interface{}
	if v := recover(); v != nil {
		event = RunPanicked
		panicValue = v
	}
	r.notifySafely(info, event, panicValue)
	if event == RunPanicked {
		panic(panicValue)
	}
}

// notifySafely invokes the notifier, recovering any panic it raises so that
// run finalization (including re-throwing a job panic) is never disrupted.
func (r runContextJob) notifySafely(info RunInfo, event RunEvent, panicValue interface{}) {
	if r.notify == nil {
		return
	}
	defer func() { _ = recover() }()
	r.notify(info, event, panicValue)
}

// runInContext dispatches to RunContext when the job is context-aware, and
// falls back to Run otherwise.
func runInContext(ctx context.Context, j Job) {
	if cj, ok := j.(ContextJob); ok {
		cj.RunContext(ctx)
		return
	}
	j.Run()
}

// copyTags returns a copy of the given tags map, or nil if it is empty.
func copyTags(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	copied := make(map[string]string, len(tags))
	for k, v := range tags {
		copied[k] = v
	}
	return copied
}

package cron

import (
	"context"
	"fmt"
)

// RunID uniquely identifies a single invocation of a Job.
//
// It differs from an EntryID, which identifies a scheduled entry: a RunID
// identifies one particular execution of that entry. Every time a job is
// triggered it gets a fresh RunID, so logs and metrics can tell apart
// repeated runs of the same job.
type RunID string

// IDFunc generates a new RunID for a job invocation.
//
// Callers supply their own generator so that run identifiers are
// deterministic and free of randomness in tests. A typical implementation
// wraps a counter:
//
//	var n uint64
//	idGen := func() cron.RunID {
//	    return cron.RunID(fmt.Sprintf("run-%d", atomic.AddUint64(&n, 1)))
//	}
type IDFunc func() RunID

// Tag is a scheduler-configured label attached to every run of a job.
//
// The value handed to a running job (see RunContext) is a snapshot taken when
// the run starts. Mutating that snapshot never affects the scheduler's
// configuration: the next run gets a fresh snapshot carrying the original
// value.
type Tag struct {
	// Name is the label configured by the scheduler.
	Name string
}

// RunContext carries the identity and tag of a single job invocation.
//
// It is stored in the context.Context passed to ContextJob implementations and
// can be retrieved with FromContext.
type RunContext struct {
	// ID is the unique identity of this invocation.
	ID RunID
	// Tag is the scheduler-configured label, snapshotted for this run.
	Tag Tag
}

// NotifyFunc is called when a job invocation finishes, either normally or
// because it panicked. err is non-nil when the job panicked.
//
// The same RunContext is passed in both cases, so the panic notification and
// the normal-end notification agree on the identity of the run.
//
// A notification function that itself panics must not prevent the rest of the
// run's cleanup; the adapter guards against that.
type NotifyFunc func(rc *RunContext, err error)

// contextKey is the unexported key used to store a *RunContext in a context.
type contextKey struct{}

// ContextJob is implemented by Jobs that want to receive the RunContext for
// each invocation. Jobs that do not implement it run exactly as before.
//
// The same context is shared by nested context adapters (see WithContext), so
// the run identity is generated only once even when the adapter is applied
// multiple times.
type ContextJob interface {
	Job
	RunWithContext(ctx context.Context)
}

// ContextJobFunc adapts a function to the ContextJob interface.
type ContextJobFunc func(ctx context.Context)

// Run implements Job by running the function with a background context.
func (f ContextJobFunc) Run() { f(context.Background()) }

// RunWithContext implements ContextJob.
func (f ContextJobFunc) RunWithContext(ctx context.Context) { f(ctx) }

// WithContext returns a JobWrapper that assigns each invocation a unique
// identity and a snapshot of the configured tag.
//
// The idGen is called exactly once per run, unless an outer adapter already
// created a RunContext (in which case it is reused). The notify function, if
// non-nil, is called when the run finishes (normally or by panic) with the
// same RunContext. Panics raised by the job are re-raised after notification
// so outer wrappers such as Recover still observe them; panics raised by
// notify are swallowed so the cleanup is not interrupted.
func WithContext(idGen IDFunc, tag Tag, notify NotifyFunc) JobWrapper {
	return func(j Job) Job {
		return &contextAdapter{next: j, idGen: idGen, tag: tag, notify: notify}
	}
}

type contextAdapter struct {
	next   Job
	idGen  IDFunc
	tag    Tag
	notify NotifyFunc
}

func (a *contextAdapter) Run() {
	a.run(context.Background())
}

func (a *contextAdapter) RunWithContext(ctx context.Context) {
	a.run(ctx)
}

func (a *contextAdapter) run(parent context.Context) {
	rc, ok := FromContext(parent)
	if !ok {
		// First adapter in the chain: generate the identity and take a
		// snapshot of the configured tag. Nested adapters reuse this.
		rc = &RunContext{ID: a.idGen(), Tag: a.tag}
		parent = context.WithValue(parent, contextKey{}, rc)
	}

	var (
		panicked bool
		rec      interface{}
		err      error
	)
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			rec = r
			if e, ok := r.(error); ok {
				err = e
			} else {
				err = fmt.Errorf("%v", r)
			}
		}
		if a.notify != nil {
			// A panic from the notification function must not prevent the
			// rest of the cleanup (in particular, re-raising the job's panic
			// so outer wrappers can still log it).
			func() {
				defer func() { _ = recover() }()
				a.notify(rc, err)
			}()
		}
		if panicked {
			panic(rec)
		}
	}()

	if cj, ok := a.next.(ContextJob); ok {
		cj.RunWithContext(parent)
	} else {
		a.next.Run()
	}
}

// FromContext returns the RunContext for the current job invocation.
// It returns false if ctx is not a job invocation context.
func FromContext(ctx context.Context) (*RunContext, bool) {
	rc, ok := ctx.Value(contextKey{}).(*RunContext)
	return rc, ok
}

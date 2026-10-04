package cron

import (
	"sync"
)

// Coalescer coalesces triggers of a job while it is still running: instead of
// running invocations concurrently (no serialization) or queuing every
// trigger for a later run (DelayIfStillRunning) or dropping them entirely
// (SkipIfStillRunning), all triggers that arrive during a run are merged into
// exactly one follow-up run. Triggers arriving during that follow-up run are
// again merged into a single subsequent run, and so on.
//
// A Coalescer also provides an explicit shutdown entry: after Stop, new
// triggers are ignored, and Stop may optionally wait for the in-flight run
// and the already-coalesced follow-up run to complete.
//
// Compose it with Recover so panics are logged; the Coalescer resets its
// internal state even when the job panics, so the "running" mark is never
// stuck and later triggers are still accepted:
//
//	coalescer := cron.NewCoalescer(logger)
//	cron.New(cron.WithChain(
//		cron.Recover(logger),
//		coalescer.Wrap,
//	))
type Coalescer struct {
	logger Logger

	mu     sync.Mutex
	states []*coalesceState
}

// coalesceState holds the per-wrapped-job state.
type coalesceState struct {
	mu      sync.Mutex
	running bool // a run (or coalesced follow-up run) is in progress
	pending bool // at least one trigger arrived while running
	closed  bool // Stop was called; new triggers are rejected
	wg      sync.WaitGroup
}

// NewCoalescer returns a Coalescer that logs coalesced (merged) triggers and
// rejected triggers to the given logger at Info level.
func NewCoalescer(logger Logger) *Coalescer {
	return &Coalescer{logger: logger}
}

// Wrap is a JobWrapper and may be used anywhere a JobWrapper is accepted,
// e.g. cron.NewChain(coalescer.Wrap, ...).Then(job) or cron.WithChain.
//
// Each wrapped job gets independent state, but Stop closes all of them.
//
// The returned job runs the underlying job synchronously in the caller's
// goroutine when idle; triggers that arrive while a run is in progress are
// coalesced into a pending flag and return immediately.
func (c *Coalescer) Wrap(j Job) Job {
	s := &coalesceState{}
	c.mu.Lock()
	c.states = append(c.states, s)
	c.mu.Unlock()
	return FuncJob(func() {
		s.run(j, c.logger)
	})
}

// Stop closes the Coalescer: triggers arriving after Stop are ignored. If
// wait is true, Stop blocks until the currently running job and the one
// already-coalesced follow-up run (if any) have completed. Stop is idempotent
// and safe to call concurrently with job triggers.
func (c *Coalescer) Stop(wait bool) {
	c.mu.Lock()
	states := make([]*coalesceState, len(c.states))
	copy(states, c.states)
	c.mu.Unlock()
	for _, s := range states {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
	}
	if wait {
		for _, s := range states {
			s.wg.Wait()
		}
	}
}

// run executes j, coalescing concurrent triggers into follow-up runs.
func (s *coalesceState) run(j Job, logger Logger) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		logger.Info("coalesce: trigger rejected, coalescer stopped")
		return
	}
	if s.running {
		// A run is in progress: merge this trigger into the single pending
		// follow-up run instead of running concurrently or queueing.
		if !s.pending {
			logger.Info("coalesce: trigger coalesced into pending run")
		}
		s.pending = true
		s.mu.Unlock()
		return
	}
	s.running = true
	s.wg.Add(1)
	s.mu.Unlock()

	defer s.wg.Done()
	// If the job panics (and the panic is not recovered by an inner
	// wrapper), reset the state before propagating the panic so that the
	// running mark is never stuck and future triggers are still accepted.
	// Compose with Recover to log the panic.
	defer func() {
		if p := recover(); p != nil {
			s.mu.Lock()
			s.running = false
			s.pending = false
			s.mu.Unlock()
			panic(p)
		}
	}()

	for {
		j.Run()
		s.mu.Lock()
		if !s.pending {
			s.running = false
			s.mu.Unlock()
			return
		}
		// At least one trigger arrived during the run: execute exactly one
		// coalesced follow-up run. Triggers arriving during it set pending
		// again, producing at most one further run, and so on. A pending
		// run requested before Stop is still executed.
		s.pending = false
		s.mu.Unlock()
	}
}

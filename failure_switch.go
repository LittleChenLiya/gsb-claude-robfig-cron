package cron

import (
	"sync"
	"time"
)

// SkipEvent is emitted once for every trigger that a paused FailureSwitch
// skips. Each skipped invocation produces exactly one independent event.
type SkipEvent struct {
	// Time is when the trigger was skipped.
	Time time.Time

	// Generation is the switch generation in which the skip happened.
	Generation uint64

	// Failures is the consecutive-failure count that keeps the switch paused.
	Failures int
}

// FailureSwitch is a stateful JobWrapper that pauses a job after a number of
// consecutive failures and requires an explicit Resume to run it again.
//
// State machine:
//
//	Active  --(threshold consecutive failures)--> Paused
//	Active  --(one success)--> Active, failure count reset to 0
//	Paused  --(Resume())--> Active, failure count 0, new generation
//
// Failure is decided ONLY by the caller-supplied result function: after each
// executed run the switch calls result(); a non-nil return counts one failure,
// a nil return resets the consecutive-failure counter to zero. Panics are not
// interpreted by the switch itself; combine with Recover (e.g.
// NewChain(sw.Wrap, Recover(logger))) so a panicking job is recovered before
// the result function is evaluated. The switch holds no lock while the job
// runs, so a panic can never leave the switch stuck.
//
// Pausing never removes the cron Entry (nor any other entry): the scheduler
// keeps firing on the original schedule and each firing is skipped with a
// SkipEvent. Resume is an explicit operation; it starts a new generation.
// Runs that started in an older generation and finish after Resume ("late
// completions") are ignored and do not change the new generation's counter.
type FailureSwitch struct {
	mu         sync.Mutex
	threshold  int
	result     func() error
	logger     Logger
	failures   int
	paused     bool
	generation uint64
	events     chan SkipEvent
}

// NewFailureSwitch returns a FailureSwitch that pauses the wrapped job after
// threshold consecutive failures. threshold values below 1 are treated as 1.
// result is called after every executed run to decide success (nil) or
// failure (non-nil); it is the only source of failure verdicts. A nil logger
// falls back to DefaultLogger.
func NewFailureSwitch(threshold int, result func() error, logger Logger) *FailureSwitch {
	if threshold < 1 {
		threshold = 1
	}
	if logger == nil {
		logger = DefaultLogger
	}
	return &FailureSwitch{
		threshold: threshold,
		result:    result,
		logger:    logger,
		events:    make(chan SkipEvent, 64),
	}
}

// Wrap decorates j with the pause-on-failure behavior. The method value
// satisfies JobWrapper, so it can be used directly in a Chain:
//
//	sw := cron.NewFailureSwitch(2, func() error { return lastErr }, logger)
//	cron.New(cron.WithChain(cron.NewChain(sw.Wrap, cron.Recover(logger))))
func (s *FailureSwitch) Wrap(j Job) Job {
	return FuncJob(func() {
		s.mu.Lock()
		if s.paused {
			ev := SkipEvent{
				Time:       time.Now(),
				Generation: s.generation,
				Failures:   s.failures,
			}
			s.mu.Unlock()
			s.logger.Info("skip", "reason", "failure-switch paused",
				"generation", ev.Generation, "failures", ev.Failures)
			select {
			case s.events <- ev:
			default: // never block a trigger on a full event channel
			}
			return
		}
		gen := s.generation
		s.mu.Unlock()

		j.Run()

		var err error
		if s.result != nil {
			err = s.result()
		}

		s.mu.Lock()
		defer s.mu.Unlock()
		if gen != s.generation {
			// Late completion of a previous generation (a Resume happened
			// while this run was in flight): its verdict must not touch the
			// new generation's counter.
			s.logger.Info("ignore late completion",
				"run-generation", gen, "generation", s.generation)
			return
		}
		if err != nil {
			s.failures++
			s.logger.Info("failure", "failures", s.failures, "threshold", s.threshold)
			if s.failures >= s.threshold {
				s.paused = true
				s.logger.Info("pause", "failures", s.failures, "generation", s.generation)
			}
		} else {
			s.failures = 0
		}
	})
}

// Resume explicitly re-enables the wrapped job: it clears the paused state,
// resets the consecutive-failure counter to zero and starts a new generation
// so that late completions of older runs cannot affect the new counter.
// Resume is a no-op when the switch is not paused, but it always starts a new
// generation.
func (s *FailureSwitch) Resume() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.paused = false
	s.failures = 0
	s.generation++
	s.logger.Info("resume", "generation", s.generation)
}

// Paused reports whether the switch is currently pausing the job.
func (s *FailureSwitch) Paused() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.paused
}

// Failures returns the consecutive-failure count of the current generation.
func (s *FailureSwitch) Failures() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failures
}

// Generation returns the current generation, incremented on every Resume.
func (s *FailureSwitch) Generation() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.generation
}

// Skipped returns the channel on which a SkipEvent is delivered for every
// skipped trigger. Events are sent non-blockingly; if nobody receives and the
// buffer fills, events are dropped rather than blocking the scheduler.
func (s *FailureSwitch) Skipped() <-chan SkipEvent {
	return s.events
}

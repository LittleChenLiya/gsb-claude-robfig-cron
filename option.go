package cron

import (
	"time"
)

// Option represents a modification to the default behavior of a Cron.
type Option func(*Cron)

// WithLocation overrides the timezone of the cron instance.
func WithLocation(loc *time.Location) Option {
	return func(c *Cron) {
		c.location = loc
	}
}

// WithSeconds overrides the parser used for interpreting job schedules to
// include a seconds field as the first one.
func WithSeconds() Option {
	return WithParser(NewParser(
		Second | Minute | Hour | Dom | Month | Dow | Descriptor,
	))
}

// WithParser overrides the parser used for interpreting job schedules.
func WithParser(p ScheduleParser) Option {
	return func(c *Cron) {
		c.parser = p
	}
}

// WithChain specifies Job wrappers to apply to all jobs added to this cron.
// Refer to the Chain* functions in this package for provided wrappers.
func WithChain(wrappers ...JobWrapper) Option {
	return func(c *Cron) {
		c.chain = NewChain(wrappers...)
	}
}

// WithLogger uses the provided logger.
func WithLogger(logger Logger) Option {
	return func(c *Cron) {
		c.logger = logger
	}
}

// WithClock overrides the clock used by the cron instance to tell time and
// to wait between activations. The default is the real system clock, which
// leaves existing behavior unchanged; tests can inject a fake clock to
// control time deterministically.
func WithClock(clock Clock) Option {
	return func(c *Cron) {
		c.clock = clock
	}
}

// EntryOption customizes a single entry when it is scheduled via
// ScheduleWithOptions.
type EntryOption func(*Entry)

// WithMisfirePolicy sets how triggers missed while the cron is paused are
// handled when it resumes. See the Misfire* constants. The default is
// MisfireSkip.
func WithMisfirePolicy(policy MisfirePolicy) EntryOption {
	return func(e *Entry) {
		e.MisfirePolicy = policy
	}
}

// WithMaxCatchUp bounds how many missed triggers are replayed on resume
// for entries using MisfireCatchUp. When more triggers were missed, only
// the most recent n are replayed and the number of discarded triggers is
// reported on Entry.DroppedMisses. Zero (the default) means no limit.
func WithMaxCatchUp(n int) EntryOption {
	return func(e *Entry) {
		e.MaxCatchUp = n
	}
}

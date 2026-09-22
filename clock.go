package cron

import "time"

// Clock abstracts the source of time and timers used by the scheduler.
// The default implementation uses the system clock; tests may inject a fake
// clock via WithClock to make scheduling deterministic.
type Clock interface {
	// Now returns the current time.
	Now() time.Time

	// NewTimer returns a Timer that will send the current time on its
	// channel after at least duration d, according to this clock.
	NewTimer(d time.Duration) Timer
}

// Timer abstracts *time.Timer so that fake clocks can drive wakeups.
type Timer interface {
	// C returns the channel on which the firing time is delivered.
	C() <-chan time.Time

	// Stop prevents the timer from firing. It returns false if the timer
	// has already expired or been stopped.
	Stop() bool
}

// realClock is the default Clock, backed by the system clock.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) NewTimer(d time.Duration) Timer { return realTimer{time.NewTimer(d)} }

type realTimer struct {
	timer *time.Timer
}

func (r realTimer) C() <-chan time.Time { return r.timer.C }

func (r realTimer) Stop() bool { return r.timer.Stop() }

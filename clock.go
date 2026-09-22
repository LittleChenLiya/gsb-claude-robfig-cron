package cron

import "time"

// Clock abstracts the source of time and timers used by the scheduler.
// The default implementation (realClock) delegates to the time package and
// preserves the historical behavior of Cron. Tests may inject a fake clock
// via WithClock to control time deterministically.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// NewTimer returns a Timer that fires after the given duration.
	NewTimer(d time.Duration) Timer
}

// Timer abstracts time.Timer so fake clocks can drive scheduler wakeups.
type Timer interface {
	// C returns the channel on which the fire time is delivered.
	C() <-chan time.Time
	// Stop prevents the timer from firing.
	Stop() bool
}

// realClock is the default Clock, backed by the time package.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) NewTimer(d time.Duration) Timer {
	return realTimer{timer: time.NewTimer(d)}
}

type realTimer struct {
	timer *time.Timer
}

func (t realTimer) C() <-chan time.Time { return t.timer.C }
func (t realTimer) Stop() bool          { return t.timer.Stop() }

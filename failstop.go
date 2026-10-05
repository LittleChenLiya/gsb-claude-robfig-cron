package cron

import "sync"

// FailStop suspends ("trips") a single job after a configured number of
// consecutive failures. It is used to stop a repeatedly failing job from
// running on every trigger while leaving the cron schedule and all other
// entries untouched.
//
// Failure is decided solely by the caller-supplied result function:
// isFailure is invoked after every run of the wrapped job (including runs
// that panic, so FailStop composes with Recover without leaving state
// stuck) and must report whether that run failed. It is the ONLY authority
// used to decide failure; FailStop never infers failure from panics itself.
//
// A FailStop instance wraps a single job via Wrap and is not shared between
// jobs. Create one FailStop per job that needs fail-stop protection.
//
// State machine:
//
//	[active] --isFailure()==true-->  [active, count+1]
//	[active, count == threshold] --> [disabled]
//	[disabled] --trigger-->          skip (onSkip event emitted), job NOT run
//	[disabled/active] --Reset()-->   [active, count=0, generation+1]
//
// Every run captures the generation that was current when it started. When
// the run completes, its result is applied only if that generation is still
// current; a late completion from an old generation never modifies the new
// count. Recovery is always explicit: nothing re-enables a disabled job
// except a call to Reset.
type FailStop struct {
	threshold int
	isFailure func() bool
	onSkip    func()

	mu         sync.Mutex
	count      int
	generation uint64
	disabled   bool
}

// NewFailStop returns a FailStop that disables the wrapped job after
// threshold consecutive failures.
//
// isFailure is the caller-supplied result function and the only authority
// used to decide failure: it is called after each run and must return true
// when that run failed. It must not be nil.
//
// onSkip, if non-nil, is invoked once for every trigger that is skipped
// while the job is disabled; that callback is the independent "skip" event.
func NewFailStop(threshold int, isFailure func() bool, onSkip func()) *FailStop {
	if threshold < 1 {
		threshold = 1
	}
	if isFailure == nil {
		isFailure = func() bool { return false }
	}
	return &FailStop{
		threshold: threshold,
		isFailure: isFailure,
		onSkip:    onSkip,
	}
}

// Wrap returns a Job that applies fail-stop behavior to the given job. The
// FailStop wraps a single job; create one FailStop per job that needs
// protection. It composes with the other wrappers, e.g.:
//
//	c := cron.New(cron.WithChain(cron.Recover(logger)))
//	c.Schedule(schedule, fs.Wrap(job))
func (f *FailStop) Wrap(j Job) Job {
	return FuncJob(func() { f.run(j) })
}

// Reset explicitly re-enables the job and starts a fresh generation with the
// consecutive-failure count at zero. Late completions from runs started in
// an earlier generation are ignored and never modify the new count.
func (f *FailStop) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.disabled = false
	f.count = 0
	f.generation++
}

// State reports the current consecutive-failure count, whether the job is
// disabled, and the current generation number.
func (f *FailStop) State() (count int, disabled bool, generation uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.count, f.disabled, f.generation
}

func (f *FailStop) run(j Job) {
	f.mu.Lock()
	if f.disabled {
		f.mu.Unlock()
		if f.onSkip != nil {
			f.onSkip()
		}
		return
	}
	gen := f.generation
	f.mu.Unlock()

	// Consult the caller's result function exactly once per run, outside the
	// lock so the callback may safely call State/Reset. The deferred update
	// also runs when the job panics, so FailStop composes with Recover
	// without leaving state stuck.
	defer func() {
		failed := f.isFailure()

		f.mu.Lock()
		defer f.mu.Unlock()
		if gen != f.generation {
			// Late completion from an old generation: never touch the new
			// count.
			return
		}
		if failed {
			f.count++
			if f.count >= f.threshold {
				f.disabled = true
			}
		} else {
			f.count = 0
		}
	}()

	j.Run()
}

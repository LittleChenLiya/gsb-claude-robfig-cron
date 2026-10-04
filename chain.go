package cron

import (
	"fmt"
	"runtime"
	"sync"
	"time"
)

// JobWrapper decorates the given Job with some behavior.
type JobWrapper func(Job) Job

// Chain is a sequence of JobWrappers that decorates submitted jobs with
// cross-cutting behaviors like logging or synchronization.
type Chain struct {
	wrappers []JobWrapper
}

// NewChain returns a Chain consisting of the given JobWrappers.
func NewChain(c ...JobWrapper) Chain {
	return Chain{c}
}

// Then decorates the given job with all JobWrappers in the chain.
//
// This:
//     NewChain(m1, m2, m3).Then(job)
// is equivalent to:
//     m1(m2(m3(job)))
func (c Chain) Then(j Job) Job {
	for i := range c.wrappers {
		j = c.wrappers[len(c.wrappers)-i-1](j)
	}
	return j
}

// Recover panics in wrapped jobs and log them with the provided logger.
func Recover(logger Logger) JobWrapper {
	return func(j Job) Job {
		return FuncJob(func() {
			defer func() {
				if r := recover(); r != nil {
					const size = 64 << 10
					buf := make([]byte, size)
					buf = buf[:runtime.Stack(buf, false)]
					err, ok := r.(error)
					if !ok {
						err = fmt.Errorf("%v", r)
					}
					logger.Error(err, "panic", "stack", "...\n"+string(buf))
				}
			}()
			j.Run()
		})
	}
}

// DelayIfStillRunning serializes jobs, delaying subsequent runs until the
// previous one is complete. Jobs running after a delay of more than a minute
// have the delay logged at Info.
func DelayIfStillRunning(logger Logger) JobWrapper {
	return func(j Job) Job {
		var mu sync.Mutex
		return FuncJob(func() {
			start := time.Now()
			mu.Lock()
			defer mu.Unlock()
			if dur := time.Since(start); dur > time.Minute {
				logger.Info("delay", "duration", dur)
			}
			j.Run()
		})
	}
}

// SkipIfStillRunning skips an invocation of the Job if a previous invocation is
// still running. It logs skips to the given logger at Info level.
func SkipIfStillRunning(logger Logger) JobWrapper {
	return func(j Job) Job {
		var ch = make(chan struct{}, 1)
		ch <- struct{}{}
		return FuncJob(func() {
			select {
			case v := <-ch:
				defer func() { ch <- v }()
				j.Run()
			default:
				logger.Info("skip")
			}
		})
	}
}

// CoalesceIfStillRunning serializes job invocations: at most one invocation
// runs at a time, and triggers that arrive while an invocation is in progress
// are not dropped (like SkipIfStillRunning) nor queued one-by-one (like
// DelayIfStillRunning). Instead they are merged (coalesced) into a single
// follow-up invocation that runs once the current one completes. Triggers
// arriving during that follow-up are coalesced again into yet another one, so
// a busy period always results in exactly one extra invocation, never a backlog.
//
// The first invocation blocks until every coalesced follow-up has completed;
// later invocations return immediately while an invocation is in progress.
//
// Panics from the wrapped job are propagated to the caller so that an outer
// Recover wrapper handles them in the usual way; the wrapper's running state
// is reset even when a panic occurs, so subsequent invocations are not stuck.
//
// The returned Job also implements CoalescedJob; use its Close method to stop
// accepting new invocations, optionally waiting for the current and coalesced
// invocations to finish.
//
// Compared with SkipIfStillRunning:
//
//	SkipIfStillRunning          CoalesceIfStillRunning
//	--------------------------   --------------------------
//	Concurrent trigger: drop    Concurrent trigger: remember it
//	Idle after a busy period    One follow-up after a busy period
//	Panic: flag released        Panic: flag released
//	No close support            Close() to reject new triggers
func CoalesceIfStillRunning(logger Logger) JobWrapper {
	return func(j Job) Job {
		c := &coalesceJob{
			job:    j,
			logger: logger,
			done:   make(chan struct{}),
		}
		return c
	}
}

// CoalescedJob is a Job created by CoalesceIfStillRunning.
type CoalescedJob interface {
	Job
	// Close prevents any further invocations of the wrapped job. If wait is
	// true, it blocks until the currently running invocation (if any) and any
	// coalesced follow-up invocation have completed.
	Close(wait bool)
}

type coalesceJob struct {
	mu     sync.Mutex
	job    Job
	logger Logger

	running bool // an invocation is currently in progress
	pending bool // at least one trigger arrived while an invocation was running
	closed  bool // Close was called

	done     chan struct{} // closed once the worker has stopped
	doneOnce sync.Once
}

func (c *coalesceJob) Run() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	if c.running {
		// An invocation is in progress. Coalesce this trigger into a single
		// follow-up invocation instead of running it concurrently or queuing
		// it individually.
		if !c.pending {
			c.logger.Info("coalesce")
		}
		c.pending = true
		c.mu.Unlock()
		return
	}
	// No invocation in progress: become the worker and run the job.
	c.running = true
	c.pending = false
	c.mu.Unlock()

	c.loop()
}

// loop runs the wrapped job serially for as long as there is pending work.
// It always executes in the goroutine that called Run, so panics propagate to
// the caller (and thus to an outer Recover wrapper).
func (c *coalesceJob) loop() {
	for {
		c.runOnce()

		c.mu.Lock()
		c.running = false
		if !c.pending {
			// No more follow-up work: stop the worker. This is checked after
			// Close as well, so a coalesced follow-up is allowed to finish.
			c.mu.Unlock()
			c.signalDone()
			return
		}
		// A trigger arrived during the last invocation: run once more.
		c.pending = false
		c.running = true
		c.mu.Unlock()
	}
}

// runOnce invokes the wrapped job. It resets the running state even if the
// job panics, and re-panics so that an outer Recover wrapper handles the
// panic in the usual way.
func (c *coalesceJob) runOnce() {
	defer func() {
		if r := recover(); r != nil {
			c.mu.Lock()
			c.running = false
			closed := c.closed
			c.mu.Unlock()
			if closed {
				c.signalDone()
			}
			panic(r)
		}
	}()
	c.job.Run()
}

func (c *coalesceJob) Close(wait bool) {
	c.mu.Lock()
	c.closed = true
	active := c.running
	c.mu.Unlock()

	if !active {
		// No worker is active, so nobody else will close the done channel.
		c.signalDone()
	}
	if wait {
		<-c.done
	}
}

func (c *coalesceJob) signalDone() {
	c.doneOnce.Do(func() { close(c.done) })
}

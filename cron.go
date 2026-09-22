package cron

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries   []*Entry
	chain     Chain
	stop      chan struct{}
	add       chan *Entry
	remove    chan EntryID
	snapshot  chan chan []Entry
	control   chan controlMsg
	catchUp   chan EntryID
	running   bool
	logger    Logger
	runningMu sync.Mutex
	location  *time.Location
	parser    ScheduleParser
	nextID    EntryID
	jobWaiter sync.WaitGroup
	clock     Clock
}

// controlSignal is a pause or resume request sent to the run loop.
type controlSignal int

const (
	controlPause controlSignal = iota
	controlResume
)

// controlMsg carries a controlSignal plus a channel that is closed once the
// run loop has applied the signal, making Pause and Resume synchronous.
type controlMsg struct {
	signal controlSignal
	done   chan struct{}
}

// ScheduleParser is an interface for schedule spec parsers that return a Schedule
type ScheduleParser interface {
	Parse(spec string) (Schedule, error)
}

// Job is an interface for submitted cron jobs.
type Job interface {
	Run()
}

// Schedule describes a job's duty cycle.
type Schedule interface {
	// Next returns the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}

// EntryID identifies an entry within a Cron instance
type EntryID int

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// ID is the cron-assigned ID of this entry, which may be used to look up a
	// snapshot or remove it.
	ID EntryID

	// Schedule on which this job should be run.
	Schedule Schedule

	// Next time the job will run, or the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	Next time.Time

	// Prev is the last time this job was run, or the zero time if never.
	Prev time.Time

	// WrappedJob is the thing to run when the Schedule is activated.
	WrappedJob Job

	// Job is the thing that was submitted to cron.
	// It is kept around so that user code that needs to get at the job later,
	// e.g. via Entries() can do so.
	Job Job

	// MisfirePolicy determines how triggers missed while the cron was paused
	// are handled when it resumes. Defaults to MisfireSkip.
	MisfirePolicy MisfirePolicy

	// MaxCatchUp bounds how many missed triggers are replayed on resume when
	// MisfirePolicy is MisfireCatchUp. Zero means no limit.
	MaxCatchUp int

	// DroppedMisses reports how many missed triggers were discarded at the
	// most recent resume because they exceeded MaxCatchUp.
	DroppedMisses int

	// Internal catch-up state, only mutated by the scheduler goroutine.
	missed         []time.Time
	catchUpRunning bool
	pendingTrigger bool
}

// MisfirePolicy selects what happens to job triggers that were missed
// while the cron scheduler was paused.
type MisfirePolicy int

const (
	// MisfireSkip discards triggers missed during a pause; the job next runs
	// at its first scheduled time after resume. This is the default.
	MisfireSkip MisfirePolicy = iota

	// MisfireCoalesce runs the job at most once on resume, no matter how
	// many triggers were missed during the pause.
	MisfireCoalesce

	// MisfireCatchUp replays the missed triggers on resume, in their
	// original scheduled order, bounded by the entry's MaxCatchUp.
	MisfireCatchUp
)

// Valid returns true if this is not the zero entry.
func (e Entry) Valid() bool { return e.ID != 0 }

// byTime is a wrapper for sorting the entry array by time
// (with zero time at the end).
type byTime []*Entry

func (s byTime) Len() int      { return len(s) }
func (s byTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byTime) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	if s[i].Next.IsZero() {
		return false
	}
	if s[j].Next.IsZero() {
		return true
	}
	return s[i].Next.Before(s[j].Next)
}

// New returns a new Cron job runner, modified by the given options.
//
// Available Settings
//
//   Time Zone
//     Description: The time zone in which schedules are interpreted
//     Default:     time.Local
//
//   Parser
//     Description: Parser converts cron spec strings into cron.Schedules.
//     Default:     Accepts this spec: https://en.wikipedia.org/wiki/Cron
//
//   Chain
//     Description: Wrap submitted jobs to customize behavior.
//     Default:     A chain that recovers panics and logs them to stderr.
//
// See "cron.With*" to modify the default behavior.
func New(opts ...Option) *Cron {
	c := &Cron{
		entries:   nil,
		chain:     NewChain(),
		add:       make(chan *Entry),
		stop:      make(chan struct{}),
		snapshot:  make(chan chan []Entry),
		remove:    make(chan EntryID),
		control:   make(chan controlMsg),
		catchUp:   make(chan EntryID),
		running:   false,
		runningMu: sync.Mutex{},
		logger:    DefaultLogger,
		location:  time.Local,
		parser:    standardParser,
		clock:     realClock{},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// FuncJob is a wrapper that turns a func() into a cron.Job
type FuncJob func()

func (f FuncJob) Run() { f() }

// AddFunc adds a func to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
func (c *Cron) AddFunc(spec string, cmd func()) (EntryID, error) {
	return c.AddJob(spec, FuncJob(cmd))
}

// AddJob adds a Job to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
func (c *Cron) AddJob(spec string, cmd Job) (EntryID, error) {
	schedule, err := c.parser.Parse(spec)
	if err != nil {
		return 0, err
	}
	return c.Schedule(schedule, cmd), nil
}

// Schedule adds a Job to the Cron to be run on the given schedule.
// The job is wrapped with the configured Chain.
func (c *Cron) Schedule(schedule Schedule, cmd Job) EntryID {
	return c.ScheduleWithOptions(schedule, cmd)
}

// ScheduleWithOptions adds a Job to the Cron to be run on the given
// schedule, like Schedule, additionally applying the given per-entry
// options such as WithMisfirePolicy and WithMaxCatchUp.
func (c *Cron) ScheduleWithOptions(schedule Schedule, cmd Job, opts ...EntryOption) EntryID {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	c.nextID++
	entry := &Entry{
		ID:         c.nextID,
		Schedule:   schedule,
		WrappedJob: c.chain.Then(cmd),
		Job:        cmd,
	}
	for _, opt := range opts {
		opt(entry)
	}
	if !c.running {
		c.entries = append(c.entries, entry)
	} else {
		c.add <- entry
	}
	return entry.ID
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []Entry {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		replyChan := make(chan []Entry, 1)
		c.snapshot <- replyChan
		return <-replyChan
	}
	return c.entrySnapshot()
}

// Location gets the time zone location
func (c *Cron) Location() *time.Location {
	return c.location
}

// Entry returns a snapshot of the given entry, or nil if it couldn't be found.
func (c *Cron) Entry(id EntryID) Entry {
	for _, entry := range c.Entries() {
		if id == entry.ID {
			return entry
		}
	}
	return Entry{}
}

// Remove an entry from being run in the future.
func (c *Cron) Remove(id EntryID) {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		c.remove <- id
	} else {
		c.removeEntry(id)
	}
}

// Start the cron scheduler in its own goroutine, or no-op if already started.
func (c *Cron) Start() {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		return
	}
	c.running = true
	go c.run()
}

// Run the cron scheduler, or no-op if already running.
func (c *Cron) Run() {
	c.runningMu.Lock()
	if c.running {
		c.runningMu.Unlock()
		return
	}
	c.running = true
	c.runningMu.Unlock()
	c.run()
}

// Pause suspends the scheduling of new job runs. Jobs already running are
// allowed to complete. Triggers whose scheduled times pass while paused are
// recorded as misses and handled according to each entry's MisfirePolicy
// when Resume is called. Pause is idempotent: pausing an already-paused (or
// not-yet-started) cron has no effect.
func (c *Cron) Pause() {
	c.sendControl(controlPause)
}

// Resume restarts scheduling after a Pause. The resume instant is captured
// once; every schedule activation time falling in (pauseTime, resumeTime]
// is treated as a missed trigger and handled per the entry's MisfirePolicy.
// Time drifting by while missed triggers are replayed does not create new
// misses. Resume is idempotent and cannot restart a cron that has been
// stopped with Stop.
func (c *Cron) Resume() {
	c.sendControl(controlResume)
}

// sendControl delivers a control signal to the run loop and waits until it
// has been applied. It is a no-op when the cron is not running.
func (c *Cron) sendControl(signal controlSignal) {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if !c.running {
		return
	}
	done := make(chan struct{})
	c.control <- controlMsg{signal: signal, done: done}
	<-done
}

// run the scheduler.. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run() {
	c.logger.Info("start")

	// runDone is closed when the run loop exits, releasing any catch-up
	// goroutines that are still trying to report their completion.
	runDone := make(chan struct{})
	defer close(runDone)

	// Figure out the next activation times for each entry.
	now := c.now()
	for _, entry := range c.entries {
		entry.Next = c.nextAfter(entry, now)
		c.logger.Info("schedule", "now", now, "entry", entry.ID, "next", entry.Next)
	}

	var paused bool
	var pauseTime time.Time

	for {
		// Determine the next entry to run.
		sort.Sort(byTime(c.entries))

		// While paused no timer is armed: a nil channel blocks forever, so
		// only control, add/remove, snapshot and stop events are handled.
		var timer Timer
		var timerC <-chan time.Time
		if !paused {
			if len(c.entries) == 0 || c.entries[0].Next.IsZero() {
				// If there are no entries yet, just sleep - it still handles new entries
				// and stop requests.
				timer = c.clock.NewTimer(100000 * time.Hour)
			} else {
				timer = c.clock.NewTimer(c.entries[0].Next.Sub(now))
			}
			timerC = timer.C()
		}

		for {
			select {
			case now = <-timerC:
				now = now.In(c.location)
				c.logger.Info("wake", "now", now)

				// Run every entry whose next time was less than now
				for _, e := range c.entries {
					if e.Next.After(now) || e.Next.IsZero() {
						break
					}
					if e.catchUpRunning {
						// A catch-up round is still replaying missed
						// triggers for this entry: merge this (and any
						// further) normal triggers into a single run
						// once the round completes.
						e.pendingTrigger = true
					} else {
						c.startJob(e.WrappedJob)
					}
					e.Prev = e.Next
					e.Next = c.nextAfter(e, now)
					c.logger.Info("run", "now", now, "entry", e.ID, "next", e.Next)
				}

			case newEntry := <-c.add:
				stopTimer(timer)
				now = c.now()
				newEntry.Next = c.nextAfter(newEntry, now)
				c.entries = append(c.entries, newEntry)
				c.logger.Info("added", "now", now, "entry", newEntry.ID, "next", newEntry.Next)

			case replyChan := <-c.snapshot:
				replyChan <- c.entrySnapshot()
				continue

			case <-c.stop:
				stopTimer(timer)
				c.logger.Info("stop")
				return

			case id := <-c.remove:
				stopTimer(timer)
				now = c.now()
				c.removeEntry(id)
				c.logger.Info("removed", "entry", id)

			case msg := <-c.control:
				stopTimer(timer)
				now = c.now()
				switch msg.signal {
				case controlPause:
					if !paused {
						paused = true
						pauseTime = now
						c.logger.Info("pause", "now", now)
					}
				case controlResume:
					if paused {
						paused = false
						c.logger.Info("resume", "now", now)
						c.resumeEntries(pauseTime, now, runDone)
					}
				}
				close(msg.done)

			case id := <-c.catchUp:
				// A catch-up round finished; run the single merged
				// normal trigger if one arrived while it was in flight.
				for _, e := range c.entries {
					if e.ID == id {
						e.catchUpRunning = false
						if !paused && len(e.missed) > 0 {
							// Another pause/resume cycle queued more
							// missed triggers while this round ran.
							c.startCatchUp(e, runDone)
						} else if e.pendingTrigger && !paused {
							e.pendingTrigger = false
							c.startJob(e.WrappedJob)
						}
						break
					}
				}
				continue
			}

			break
		}
	}
}

// startJob runs the given job in a new goroutine.
func (c *Cron) startJob(j Job) {
	c.jobWaiter.Add(1)
	go func() {
		defer c.jobWaiter.Done()
		j.Run()
	}()
}

// now returns current time in c location
func (c *Cron) now() time.Time {
	return c.clock.Now().In(c.location)
}

// stopTimer stops the given timer if one is armed.
func stopTimer(t Timer) {
	if t != nil {
		t.Stop()
	}
}

// errScheduleDoesNotAdvance is logged when a Schedule returns a next
// activation time that does not move past the time it was given.
var errScheduleDoesNotAdvance = errors.New("schedule.Next returned a time not after the given time; schedules must advance to avoid infinite loops")

// nextAfter computes the next activation of the entry strictly after t. A
// schedule that returns a non-zero time not after t is rejected: the
// entry is disabled (its Next is zeroed) rather than looping forever.
func (c *Cron) nextAfter(entry *Entry, t time.Time) time.Time {
	next := entry.Schedule.Next(t)
	if next.IsZero() {
		return next
	}
	if !next.After(t) {
		c.logger.Error(errScheduleDoesNotAdvance, "entry", entry.ID, "time", t, "next", next)
		return time.Time{}
	}
	return next
}

// resumeEntries applies each entry's MisfirePolicy to the triggers missed
// during (pauseTime, resumeTime] and re-arms normal scheduling. The resume
// time is captured once by the caller, so time drifting by while catch-up
// runs execute is not counted as additional misses.
func (c *Cron) resumeEntries(pauseTime, resumeTime time.Time, runDone chan struct{}) {
	for _, e := range c.entries {
		if e.MisfirePolicy != MisfireSkip {
			missed, dropped, ok := c.missedTriggers(e, pauseTime, resumeTime)
			if !ok {
				// Non-advancing schedule: reject the entry.
				e.Next = time.Time{}
				continue
			}
			e.DroppedMisses = dropped
			if dropped > 0 {
				c.logger.Info("catch-up truncated", "entry", e.ID, "dropped", dropped, "max_catch_up", e.MaxCatchUp)
			}
			e.missed = missed
		}
		e.Next = c.nextAfter(e, resumeTime)
		c.logger.Info("resume", "entry", e.ID, "missed", len(e.missed), "next", e.Next)
		if len(e.missed) > 0 && !e.catchUpRunning {
			c.startCatchUp(e, runDone)
		}
		if e.pendingTrigger && !e.catchUpRunning {
			e.pendingTrigger = false
			c.startJob(e.WrappedJob)
		}
	}
}

// missedTriggers returns the activation times of the entry's schedule
// falling in (from, to], in chronological order. For MisfireCoalesce only
// the most recent trigger is kept; for MisfireCatchUp with a positive
// MaxCatchUp only the most recent MaxCatchUp triggers are kept and the
// number of discarded earlier triggers is reported as dropped. ok is false
// if the schedule failed to advance past a time it was given.
func (c *Cron) missedTriggers(e *Entry, from, to time.Time) (kept []time.Time, dropped int, ok bool) {
	limit := 0
	countDropped := false
	switch e.MisfirePolicy {
	case MisfireCoalesce:
		limit = 1
	case MisfireCatchUp:
		limit = e.MaxCatchUp
		countDropped = e.MaxCatchUp > 0
	}
	t := from
	for {
		next := e.Schedule.Next(t)
		if next.IsZero() || next.After(to) {
			return kept, dropped, true
		}
		if !next.After(t) {
			c.logger.Error(errScheduleDoesNotAdvance, "entry", e.ID, "time", t, "next", next)
			return nil, 0, false
		}
		t = next
		if limit > 0 && len(kept) == limit {
			kept = kept[1:]
			if countDropped {
				dropped++
			}
		}
		kept = append(kept, next)
	}
}

// startCatchUp launches the serial replay of the entry's queued missed
// triggers. The replay runs in a single goroutine, so missed runs execute
// one at a time, in scheduled order, through the same wrapped job chain as
// normal runs. It is tracked by the job waiter, so Stop waits for it.
func (c *Cron) startCatchUp(e *Entry, runDone chan struct{}) {
	times := e.missed
	e.missed = nil
	e.catchUpRunning = true
	c.jobWaiter.Add(1)
	go func() {
		defer c.jobWaiter.Done()
		for _, t := range times {
			c.logger.Info("catch-up", "entry", e.ID, "scheduled", t)
			e.WrappedJob.Run()
		}
		select {
		case c.catchUp <- e.ID:
		case <-runDone:
		}
	}()
}

// Stop stops the cron scheduler if it is running; otherwise it does nothing.
// A context is returned so the caller can wait for running jobs to complete.
func (c *Cron) Stop() context.Context {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		c.stop <- struct{}{}
		c.running = false
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		c.jobWaiter.Wait()
		cancel()
	}()
	return ctx
}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot() []Entry {
	var entries = make([]Entry, len(c.entries))
	for i, e := range c.entries {
		entries[i] = *e
	}
	return entries
}

func (c *Cron) removeEntry(id EntryID) {
	var entries []*Entry
	for _, e := range c.entries {
		if e.ID != id {
			entries = append(entries, e)
		}
	}
	c.entries = entries
}

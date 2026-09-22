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
	pause     chan time.Time
	resume    chan time.Time
	setPolicy chan policyRequest
	jobDone   chan *Entry
	runDone   chan struct{}
	running   bool
	paused    bool
	pauseTime time.Time
	clock     Clock
	logger    Logger
	runningMu sync.Mutex
	location  *time.Location
	parser    ScheduleParser
	nextID    EntryID
	jobWaiter sync.WaitGroup
}

// MisfirePolicy determines how activations missed while the scheduler was
// paused are handled when the scheduler resumes.
type MisfirePolicy int

const (
	// MisfireCoalesce runs the job at most once on resume, no matter how many
	// activations were missed during the pause. This is the default and
	// matches the historical behavior of collapsing missed activations.
	MisfireCoalesce MisfirePolicy = iota
	// MisfireSkip discards all activations missed during the pause; the job
	// next runs at its first scheduled time after the resume.
	MisfireSkip
	// MisfireCatchUp replays missed activations, in their original scheduled
	// order, when the scheduler resumes. The number of replays is bounded by
	// the entry's MaxCatchUp; when more activations were missed, only the
	// most recent ones are kept and the discarded count is reported.
	MisfireCatchUp
)

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

	// DroppedMisses is the total number of missed activations discarded
	// because they exceeded the entry's MaxCatchUp limit on resume.
	DroppedMisses int

	// misfirePolicy determines how missed activations are handled on resume.
	misfirePolicy MisfirePolicy
	// maxCatchUp bounds how many missed activations are replayed on resume
	// when the policy is MisfireCatchUp. Zero means no limit.
	maxCatchUp int
	// catchUpQueue holds scheduled times still to be replayed, in order.
	catchUpQueue []time.Time
	// catchingUp is true while a catch-up replay of this entry is running.
	catchingUp bool
	// pendingCoalesce records that one or more normal activations fired
	// while a catch-up replay was in progress; they are merged into a
	// single run once the current catch-up round finishes.
	pendingCoalesce bool
}

// policyRequest configures the misfire behavior of one entry.
type policyRequest struct {
	id         EntryID
	policy     MisfirePolicy
	maxCatchUp int
}

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
		pause:     make(chan time.Time),
		resume:    make(chan time.Time),
		setPolicy: make(chan policyRequest),
		jobDone:   make(chan *Entry),
		runDone:   make(chan struct{}),
		running:   false,
		clock:     realClock{},
		runningMu: sync.Mutex{},
		logger:    DefaultLogger,
		location:  time.Local,
		parser:    standardParser,
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
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	c.nextID++
	entry := &Entry{
		ID:         c.nextID,
		Schedule:   schedule,
		WrappedJob: c.chain.Then(cmd),
		Job:        cmd,
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
	go c.run(c.paused, c.pauseTime)
}

// Run the cron scheduler, or no-op if already running.
func (c *Cron) Run() {
	c.runningMu.Lock()
	if c.running {
		c.runningMu.Unlock()
		return
	}
	c.running = true
	paused, pauseTime := c.paused, c.pauseTime
	c.runningMu.Unlock()
	c.run(paused, pauseTime)
}

// run the scheduler.. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run(paused bool, pauseTime time.Time) {
	c.logger.Info("start")

	// Figure out the next activation times for each entry.
	now := c.now()
	for _, entry := range c.entries {
		entry.Next = entry.Schedule.Next(now)
		c.logger.Info("schedule", "now", now, "entry", entry.ID, "next", entry.Next)
	}

	// Re-arm runDone so that catch-up goroutines from a previous run can
	// never block a new one.
	c.runDone = make(chan struct{})
	defer close(c.runDone)

	for {
		// Determine the next entry to run.
		sort.Sort(byTime(c.entries))

		var timer Timer
		var timerC <-chan time.Time
		if paused {
			// While paused no new jobs are started, so no timer is armed;
			// the loop below still services add/remove/snapshot/stop/resume.
		} else if len(c.entries) == 0 || c.entries[0].Next.IsZero() {
			// If there are no entries yet, just sleep - it still handles new entries
			// and stop requests.
			timer = c.clock.NewTimer(100000 * time.Hour)
			timerC = timer.C()
		} else {
			timer = c.clock.NewTimer(c.entries[0].Next.Sub(now))
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
					if e.catchingUp || len(e.catchUpQueue) > 0 {
						// A catch-up round is in progress for this entry;
						// merge any normal activations that arrive during
						// it into a single run afterwards.
						e.pendingCoalesce = true
					} else {
						c.startJob(e.WrappedJob)
					}
					e.Prev = e.Next
					e.Next = e.Schedule.Next(now)
					c.logger.Info("run", "now", now, "entry", e.ID, "next", e.Next)
				}

			case newEntry := <-c.add:
				if timer != nil {
					timer.Stop()
				}
				now = c.now()
				newEntry.Next = newEntry.Schedule.Next(now)
				c.entries = append(c.entries, newEntry)
				c.logger.Info("added", "now", now, "entry", newEntry.ID, "next", newEntry.Next)

			case t := <-c.pause:
				if timer != nil {
					timer.Stop()
				}
				if !paused {
					paused = true
					pauseTime = t
				}
				c.logger.Info("pause", "now", t)

			case t := <-c.resume:
				if paused {
					paused = false
					now = t.In(c.location)
					c.logger.Info("resume", "now", now)
					c.resumeEntries(pauseTime, now)
					for _, e := range c.entries {
						c.maybeStartCatchUp(e)
					}
				}

			case req := <-c.setPolicy:
				c.applyPolicy(req)

			case e := <-c.jobDone:
				e.catchingUp = false
				if !paused {
					c.maybeStartCatchUp(e)
				}

			case replyChan := <-c.snapshot:
				replyChan <- c.entrySnapshot()
				continue

			case <-c.stop:
				if timer != nil {
					timer.Stop()
				}
				c.logger.Info("stop")
				return

			case id := <-c.remove:
				if timer != nil {
					timer.Stop()
				}
				now = c.now()
				c.removeEntry(id)
				c.logger.Info("removed", "entry", id)
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

// Pause suspends the scheduler: no new jobs are started while paused, but
// jobs already running are allowed to finish. The pause time is captured
// once, from the cron's clock. Pause is idempotent: pausing an already
// paused cron keeps the original pause time. If the cron has not been
// started yet, it will come up paused when Start is called.
func (c *Cron) Pause() {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.paused {
		return
	}
	c.paused = true
	c.pauseTime = c.now()
	if c.running {
		c.pause <- c.pauseTime
	}
}

// Resume restarts scheduling after a Pause. It captures a single resume
// time and, for every entry, computes the activations missed while paused:
// the scheduled times returned by Schedule.Next that fall in the interval
// (pauseTime, resumeTime]. Those missed activations are then handled
// according to each entry's misfire policy (see SetMisfirePolicy). Time
// drifting by while missed activations are replayed does not create new
// misses. Resume is idempotent and is a no-op when the cron is not paused;
// in particular it never restarts a cron that has been stopped.
func (c *Cron) Resume() {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if !c.paused {
		return
	}
	c.paused = false
	if c.running {
		c.resume <- c.now()
	}
}

// SetMisfirePolicy configures how activations of the given entry that are
// missed during a pause are handled on resume:
//
//   - MisfireCoalesce (the default): run the job at most once on resume.
//   - MisfireSkip: discard the missed activations entirely.
//   - MisfireCatchUp: replay the missed activations in their original
//     scheduled order, serially per entry, through the same wrapper chain
//     as normal runs. maxCatchUp bounds how many of the most recent missed
//     activations are replayed; zero means no limit. When activations are
//     dropped because of the limit, the discarded count is logged and added
//     to the entry's DroppedMisses counter.
//
// Unknown entry IDs are ignored.
func (c *Cron) SetMisfirePolicy(id EntryID, policy MisfirePolicy, maxCatchUp int) {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	req := policyRequest{id: id, policy: policy, maxCatchUp: maxCatchUp}
	if c.running {
		c.setPolicy <- req
	} else {
		c.applyPolicy(req)
	}
}

// applyPolicy sets the misfire policy of a single entry.
func (c *Cron) applyPolicy(req policyRequest) {
	for _, e := range c.entries {
		if e.ID == req.id {
			e.misfirePolicy = req.policy
			e.maxCatchUp = req.maxCatchUp
			return
		}
	}
}

// errScheduleNotAdvancing is reported when a Schedule fails to return
// strictly increasing times, which would otherwise wedge the scheduler.
var errScheduleNotAdvancing = errors.New("cron: schedule does not advance time")

// maxMissedIterations bounds the scan for missed activations so a schedule
// that keeps returning times inside the pause window cannot loop forever.
const maxMissedIterations = 10000

// missedActivations returns the scheduled activation times of s that fall in
// (from, to], in order. It refuses schedules that do not advance time.
func missedActivations(s Schedule, from, to time.Time) ([]time.Time, error) {
	var missed []time.Time
	prev := from
	for i := 0; i < maxMissedIterations; i++ {
		next := s.Next(prev)
		if next.IsZero() || next.After(to) {
			return missed, nil
		}
		if !next.After(prev) {
			return nil, errScheduleNotAdvancing
		}
		missed = append(missed, next)
		prev = next
	}
	return nil, errScheduleNotAdvancing
}

// resumeEntries handles the activations each entry missed in (from, to]
// according to its misfire policy, and re-arms future scheduling from to.
func (c *Cron) resumeEntries(from, to time.Time) {
	for _, e := range c.entries {
		missed, err := missedActivations(e.Schedule, from, to)
		if err != nil {
			c.logger.Error(err, "schedule rejected during resume", "entry", e.ID)
			e.Next = time.Time{}
			e.catchUpQueue = nil
			continue
		}
		next := e.Schedule.Next(to)
		if !next.IsZero() && !next.After(to) {
			c.logger.Error(errScheduleNotAdvancing, "schedule rejected during resume", "entry", e.ID)
			e.Next = time.Time{}
			e.catchUpQueue = nil
			continue
		}
		e.Next = next
		switch e.misfirePolicy {
		case MisfireSkip:
			if len(missed) > 0 {
				c.logger.Info("misfires skipped", "entry", e.ID, "count", len(missed))
			}
		case MisfireCatchUp:
			kept := missed
			if e.maxCatchUp > 0 && len(kept) > e.maxCatchUp {
				dropped := len(kept) - e.maxCatchUp
				kept = kept[len(kept)-e.maxCatchUp:]
				e.DroppedMisses += dropped
				c.logger.Info("misfires dropped", "entry", e.ID, "dropped", dropped, "maxCatchUp", e.maxCatchUp)
			}
			e.catchUpQueue = append(e.catchUpQueue, kept...)
		default: // MisfireCoalesce
			if len(missed) > 0 {
				e.Prev = missed[len(missed)-1]
				c.startJob(e.WrappedJob)
			}
		}
	}
}

// maybeStartCatchUp starts the next queued catch-up replay for an entry, or
// the single coalesced run for normal activations that fired while a
// catch-up round was in progress. Replays run serially per entry, through
// the entry's WrappedJob so the configured wrapper chain still applies.
func (c *Cron) maybeStartCatchUp(e *Entry) {
	if e.catchingUp {
		return
	}
	if len(e.catchUpQueue) > 0 {
		t := e.catchUpQueue[0]
		e.catchUpQueue = e.catchUpQueue[1:]
		e.catchingUp = true
		e.Prev = t
		c.startCatchUpJob(e)
	} else if e.pendingCoalesce {
		e.pendingCoalesce = false
		c.startJob(e.WrappedJob)
	}
}

// startCatchUpJob runs one catch-up replay in a new goroutine and reports
// its completion back to the scheduler loop.
func (c *Cron) startCatchUpJob(e *Entry) {
	runDone := c.runDone
	c.jobWaiter.Add(1)
	go func() {
		defer c.jobWaiter.Done()
		e.WrappedJob.Run()
		select {
		case c.jobDone <- e:
		case <-runDone:
		}
	}()
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

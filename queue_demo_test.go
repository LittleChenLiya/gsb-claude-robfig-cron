package cron

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestQueuedJobDemo90s simulates a 90-second window of a job that is
// triggered every 30 seconds but takes ~45 seconds to run, using a
// controllable trigger source (EnqueueAt with synthetic planned times) so no
// real minutes are waited. It prints the planned times, the actual start
// order and the drop counts for both overflow policies.
func TestQueuedJobDemo90s(t *testing.T) {
	for _, policy := range []OverflowPolicy{DropOldest, RejectNewest} {
		runQueueDemo(t, policy)
	}
}

func runQueueDemo(t *testing.T, policy OverflowPolicy) {
	// Simulated timeline: triggers planned at 0s, 30s, 60s, 90s.
	base := time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)
	planned := plannedTimes(base, 4)

	// The job "takes 45s"; in the demo each simulated second costs 1ms.
	job := newSeqJob()
	job.gate = nil // no manual gate; the first run blocks on simulated work
	firstStarted := make(chan struct{})
	var once sync.Once
	inner := FuncJob(func() {
		job.Run()
		once.Do(func() { close(firstStarted) })
		time.Sleep(45 * time.Millisecond)
	})

	q := NewQueuedJob(inner, 1, policy, DiscardLogger)

	fmt.Printf("=== demo: 90s window, trigger every 30s, job takes 45s, capacity 1, policy %s ===\n", policy)
	for _, p := range planned {
		fmt.Printf("trigger planned at %s\n", p.Format("15:04:05"))
		q.EnqueueAt(p)
		if p.Equal(planned[0]) {
			<-firstStarted // make sure the first run holds the slot
		}
	}

	q.Close()
	q.Wait()

	for i, rec := range q.History() {
		fmt.Printf("run #%d: planned=%s started=%s (start order %d)\n",
			i+1, rec.Planned.Format("15:04:05"), rec.Started.Format("15:04:05.000"), i+1)
	}
	for _, d := range q.Dropped() {
		fmt.Printf("dropped: planned=%s reason=%s\n", d.Planned.Format("15:04:05"), d.Reason)
	}
	s := q.Stats()
	fmt.Printf("stats: triggers=%d enqueued=%d started=%d completed=%d droppedOldest=%d rejectedNewest=%d totalDropped=%d\n",
		s.Triggers, s.Enqueued, s.Started, s.Completed, s.DroppedOldest, s.RejectedNewest,
		s.DroppedOldest+s.RejectedNewest+s.RejectedClosed)

	if s.Triggers != 4 || s.Completed != 2 {
		t.Errorf("policy %s: expected 4 triggers and 2 completions, got %+v", policy, s)
	}
	if got := s.DroppedOldest + s.RejectedNewest; got != 2 {
		t.Errorf("policy %s: expected 2 dropped triggers, got %d", policy, got)
	}
}

package cron

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPreviewMergesSchedulesAndAppliesBounds(t *testing.T) {
	c := New()
	first := c.Schedule(Every(30*time.Second), FuncJob(func() {}))
	second := c.Schedule(Every(45*time.Second), FuncJob(func() {}))
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Minute)

	entries, err := c.Preview(start, end, 4)
	if err != nil {
		t.Fatal(err)
	}
	want := []ScheduledEntry{
		{EntryID: first, Time: start.Add(30 * time.Second)},
		{EntryID: second, Time: start.Add(45 * time.Second)},
		{EntryID: first, Time: start.Add(60 * time.Second)},
		{EntryID: first, Time: start.Add(90 * time.Second)},
	}
	if len(entries) != len(want) {
		t.Fatalf("got %d entries, want %d: %#v", len(entries), len(want), entries)
	}
	for i := range want {
		if entries[i] != want[i] {
			t.Errorf("entry %d = %#v, want %#v", i, entries[i], want[i])
		}
	}

	tieStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tie := New()
	id1 := tie.Schedule(Every(time.Minute), FuncJob(func() {}))
	id2 := tie.Schedule(Every(time.Minute), FuncJob(func() {}))
	tieEntries, err := tie.Preview(tieStart, tieStart.Add(time.Minute), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(tieEntries) != 2 || tieEntries[0].EntryID != id1 || tieEntries[1].EntryID != id2 {
		t.Fatalf("same-time entries are not ordered by EntryID: %#v", tieEntries)
	}
}

func TestPreviewValidationAndZeroSchedule(t *testing.T) {
	c := New()
	if _, err := c.Preview(time.Time{}, time.Time{}, 1); err == nil {
		t.Fatal("expected invalid range error")
	}
	start := time.Now()
	if _, err := c.Preview(start, start.Add(time.Minute), 0); err == nil {
		t.Fatal("expected invalid limit error")
	}
	c.Schedule(new(ZeroSchedule), FuncJob(func() {}))
	entries, err := c.Preview(start, start.Add(time.Minute), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("zero schedule returned %#v", entries)
	}
}

type previewFixedSchedule struct{ next time.Time }

func (s previewFixedSchedule) Next(time.Time) time.Time { return s.next }

type previewInputSchedule struct{}

func (previewInputSchedule) Next(t time.Time) time.Time { return t }

func TestPreviewRejectsNonAdvancingSchedule(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c := New()
	c.Schedule(previewInputSchedule{}, FuncJob(func() {}))
	if _, err := c.Preview(start, start.Add(time.Minute), 1); err == nil {
		t.Fatal("expected non-advancing schedule error")
	} else if !strings.Contains(err.Error(), "did not advance") {
		t.Fatalf("unexpected error: %v", err)
	}

	c = New()
	c.Schedule(previewFixedSchedule{next: start.Add(10 * time.Second)}, FuncJob(func() {}))
	if _, err := c.Preview(start, start.Add(time.Minute), 2); err == nil {
		t.Fatal("expected non-advancing schedule error after first result")
	}
}

func TestPreviewExcludesRemovedAndDoesNotChangeEntries(t *testing.T) {
	c := New()
	removed := c.Schedule(Every(time.Minute), FuncJob(func() {}))
	kept := c.Schedule(Every(time.Minute), FuncJob(func() {}))
	c.Remove(removed)
	before := c.Entry(kept)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := c.Preview(start, start.Add(time.Minute), 1); err != nil {
		t.Fatal(err)
	}
	after := c.Entry(kept)
	if before.Next != after.Next || !before.Prev.Equal(after.Prev) {
		t.Fatalf("preview changed entry state: before=%#v after=%#v", before, after)
	}
}

func TestPreviewRunningUsesOneSnapshot(t *testing.T) {
	c := New()
	schedule := &previewGatedSchedule{entered: make(chan struct{}), release: make(chan struct{})}
	c.Schedule(schedule, FuncJob(func() {}))
	c.Start()
	defer c.Stop()

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	previewDone := make(chan struct{})
	var got []ScheduledEntry
	var err error
	go func() {
		got, err = c.Preview(start, start.Add(time.Minute), 10)
		close(previewDone)
	}()
	<-schedule.entered
	added := make(chan struct{})
	go func() {
		c.Schedule(Every(time.Minute), FuncJob(func() {}))
		c.Remove(1)
		close(added)
	}()
	select {
	case <-added:
		t.Fatal("concurrent add/remove was not held behind the preview snapshot")
	case <-time.After(10 * time.Millisecond):
	}
	close(schedule.release)
	<-previewDone
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].EntryID != 1 {
		t.Fatalf("preview did not use one snapshot: %#v", got)
	}
}

type previewGatedSchedule struct {
	entered chan struct{}
	release chan struct{}
	mu      sync.Mutex
	calls   int
}

func (s *previewGatedSchedule) Next(t time.Time) time.Time {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()
	if call == 2 {
		close(s.entered)
		<-s.release
	}
	return t.Add(time.Minute)
}

func TestPreviewMatchesScheduleAcrossDST(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	c := New(WithLocation(loc))
	id, err := c.AddFunc("CRON_TZ=America/New_York 30 1 * * *", func() {})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 10, 31, 0, 0, 0, 0, loc)
	entries, err := c.Preview(start, start.Add(72*time.Hour), 4)
	if err != nil {
		t.Fatal(err)
	}
	next := start
	schedule := c.Entry(id).Schedule
	for i, entry := range entries {
		next = schedule.Next(next)
		if entry.Time != next || entry.EntryID != id {
			t.Fatalf("DST result %d = %#v, schedule.Next = %v", i, entry, next)
		}
	}
}

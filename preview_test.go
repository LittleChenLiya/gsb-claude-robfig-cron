package cron

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestPreviewMergesEntriesAndAppliesBoundsAndLimit(t *testing.T) {
	c := New()
	first := c.Schedule(Every(30*time.Second), FuncJob(func() {}))
	second := c.Schedule(Every(time.Minute), FuncJob(func() {}))
	start := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	end := start.Add(time.Minute)

	items, err := c.Preview(start, end, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 {
		t.Fatalf("got %d preview items, want 3: %#v", len(items), items)
	}
	want := []PreviewEntry{
		{Time: start.Add(30 * time.Second), EntryID: first},
		{Time: end, EntryID: first},
		{Time: end, EntryID: second},
	}
	for i := range want {
		if !items[i].Time.Equal(want[i].Time) || items[i].EntryID != want[i].EntryID {
			t.Fatalf("item %d = %#v, want %#v", i, items[i], want[i])
		}
	}

	limited, err := c.Preview(start, end, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 2 || limited[0] != want[0] || limited[1] != want[1] {
		t.Fatalf("limited preview = %#v, want %#v", limited, want[:2])
	}
	if _, err := c.Preview(end, start, 1); err == nil {
		t.Fatal("expected invalid range error")
	}
	if _, err := c.Preview(start, end, 0); err == nil {
		t.Fatal("expected invalid limit error")
	}
	entry := c.Entry(first)
	if !entry.Next.IsZero() || !entry.Prev.IsZero() {
		t.Fatalf("preview changed entry state: %#v", entry)
	}
}

type zeroPreviewSchedule struct{}

func (zeroPreviewSchedule) Next(time.Time) time.Time { return time.Time{} }

type stuckPreviewSchedule struct{}

func (stuckPreviewSchedule) Next(t time.Time) time.Time { return t }

func TestPreviewHandlesEmptyRemovedAndInvalidSchedules(t *testing.T) {
	c := New()
	c.Schedule(zeroPreviewSchedule{}, FuncJob(func() {}))
	removed := c.Schedule(Every(time.Minute), FuncJob(func() {}))
	c.Remove(removed)
	start := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	items, err := c.Preview(start, start.Add(time.Hour), 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("got %#v, want empty preview", items)
	}

	bad := New()
	id := bad.Schedule(stuckPreviewSchedule{}, FuncJob(func() {}))
	_, err = bad.Preview(start, start.Add(time.Hour), 5)
	if err == nil || !strings.Contains(err.Error(), "entry "+itoa(id)) || !strings.Contains(err.Error(), "did not advance") {
		t.Fatalf("unexpected stuck schedule error: %v", err)
	}
}

func TestPreviewUsesOneSnapshotWhileCronRuns(t *testing.T) {
	start := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	entered := make(chan struct{})
	release := make(chan struct{})
	schedule := blockingPreviewSchedule{at: start, entered: entered, release: release}
	c := New()
	initial := c.Schedule(schedule, FuncJob(func() {}))
	c.Start()
	defer c.Stop()

	result := make(chan []PreviewEntry, 1)
	errs := make(chan error, 1)
	go func() {
		items, err := c.Preview(start, start.Add(time.Minute), 5)
		result <- items
		errs <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("preview did not reach schedule")
	}
	added := c.Schedule(Every(time.Minute), FuncJob(func() {}))
	c.Remove(initial)
	close(release)
	items := <-result
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].EntryID != initial {
		t.Fatalf("snapshot preview = %#v, want only entry %d", items, initial)
	}
	if items[0].EntryID == added {
		t.Fatal("preview included an entry added after its snapshot")
	}
}

func TestPreviewMatchesScheduleAcrossDST(t *testing.T) {
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("timezone data unavailable: %v", err)
	}
	schedule, err := ParseStandard("CRON_TZ=America/New_York 30 1 * * *")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2024, 11, 3, 0, 0, 0, 0, location)
	end := time.Date(2024, 11, 4, 0, 0, 0, 0, location)
	want := schedule.Next(start)
	c := New(WithLocation(location))
	c.Schedule(schedule, FuncJob(func() {}))
	items, err := c.Preview(start, end, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || !items[0].Time.Equal(want) {
		t.Fatalf("DST preview = %#v, want %v", items, want)
	}
}

type blockingPreviewSchedule struct {
	at      time.Time
	entered chan struct{}
	release chan struct{}
}

func (s blockingPreviewSchedule) Next(t time.Time) time.Time {
	if t.Equal(s.at) {
		select {
		case <-s.entered:
		default:
			close(s.entered)
		}
		<-s.release
	}
	return t.Add(time.Minute)
}

func itoa(id EntryID) string {
	return fmt.Sprintf("%d", id)
}

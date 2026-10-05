package cron_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
)

// scriptedSchedule is a tiny injected Schedule: Next simply returns the next
// preset instant strictly later than its argument. It lets the report tests
// exercise exact DST moments without depending on the host zone database.
type scriptedSchedule struct {
	times []time.Time
	calls int
}

func (s *scriptedSchedule) Next(t time.Time) time.Time {
	s.calls++
	for _, at := range s.times {
		if at.After(t) {
			return at
		}
	}
	return time.Time{}
}

// frozenSchedule is a broken Schedule: Next always returns the same instant
// and never advances.
type frozenSchedule struct{ at time.Time }

func (s frozenSchedule) Next(time.Time) time.Time { return s.at }

// tracingSchedule records every argument handed to Next.
type tracingSchedule struct {
	scriptedSchedule
	args []time.Time
}

func (s *tracingSchedule) Next(t time.Time) time.Time {
	s.args = append(s.args, t.UTC())
	return s.scriptedSchedule.Next(t)
}

var (
	edt = time.FixedZone("EDT", -4*60*60)
	est = time.FixedZone("EST", -5*60*60)
)

// Acceptance: within the autumn fall-back window both local 01:30 labels are
// reported, and they keep distinct UTC identities.
func TestInspectSchedule_FallBackKeepsBothWallClocks(t *testing.T) {
	firstOneThirty := time.Date(2021, 11, 7, 1, 30, 0, 0, edt)  // 05:30Z
	secondOneThirty := time.Date(2021, 11, 7, 1, 30, 0, 0, est) // 06:30Z
	if !secondOneThirty.After(firstOneThirty) {
		t.Fatal("fixture broken: the two 01:30 labels must be distinct instants")
	}

	sched := &scriptedSchedule{times: []time.Time{firstOneThirty, secondOneThirty}}
	start := time.Date(2021, 11, 7, 5, 0, 0, 0, time.UTC)
	end := time.Date(2021, 11, 7, 7, 0, 0, 0, time.UTC)

	report, err := cron.InspectSchedule(sched, start, end)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(report.Activations) != 2 {
		t.Fatalf("expected both 01:30 activations, got %d", len(report.Activations))
	}

	for i, wantUTC := range []string{
		"2021-11-07T05:30:00Z",
		"2021-11-07T06:30:00Z",
	} {
		a := report.Activations[i]
		if got := a.UTC.Format(time.RFC3339); got != wantUTC {
			t.Errorf("activation %d: UTC = %s, want %s", i, got, wantUTC)
		}
		if got := a.Local.Format("2006-01-02 15:04:05"); got != "2021-11-07 01:30:00" {
			t.Errorf("activation %d: local wall = %s, want 2021-11-07 01:30:00", i, got)
		}
		if !a.RepeatedWall {
			t.Errorf("activation %d (%s): expected RepeatedWall flag", i, wantUTC)
		}
		if a.Gap {
			t.Errorf("activation %d (%s): fall back is not a forward gap", i, wantUTC)
		}
	}

	if report.Activations[0].UTC.Equal(report.Activations[1].UTC) {
		t.Fatal("the two repeated wall clocks must retain distinct UTC identities")
	}
	if report.Activations[0].Zone != "EDT" || report.Activations[1].Zone != "EST" {
		t.Errorf("zones = %q, %q, want EDT, EST",
			report.Activations[0].Zone, report.Activations[1].Zone)
	}
	if report.Activations[0].Offset != -4*3600 || report.Activations[1].Offset != -5*3600 {
		t.Errorf("offsets = %d, %d, want -14400, -18000",
			report.Activations[0].Offset, report.Activations[1].Offset)
	}
}

// Acceptance: a Schedule whose Next returns the same instant is bad; the walk
// must stop and report an error wrapping ErrBadSchedule.
func TestInspectSchedule_BadScheduleStops(t *testing.T) {
	at := time.Date(2021, 11, 7, 6, 30, 0, 0, time.UTC)
	start := at.Add(-time.Hour)
	end := at.Add(time.Hour)

	report, err := cron.InspectSchedule(frozenSchedule{at: at}, start, end)
	if !errors.Is(err, cron.ErrBadSchedule) {
		t.Fatalf("err = %v, want error wrapping ErrBadSchedule", err)
	}
	if report != nil {
		t.Fatalf("expected nil report for a bad schedule, got %d activations", len(report.Activations))
	}
}

// A larger offset between consecutive activations marks the spring-forward
// gap on the first activation after the jump.
func TestInspectSchedule_SpringForwardGap(t *testing.T) {
	// 2021-03-14 in New York: 01:00 EST (-5) then 03:00 EDT (-4), 02:xx missing.
	sched := &scriptedSchedule{times: []time.Time{
		time.Date(2021, 3, 14, 1, 0, 0, 0, est), // 06:00Z
		time.Date(2021, 3, 14, 3, 0, 0, 0, edt), // 07:00Z
		time.Date(2021, 3, 14, 4, 0, 0, 0, edt),
	}}
	start := time.Date(2021, 3, 14, 5, 0, 0, 0, time.UTC)
	end := time.Date(2021, 3, 14, 9, 0, 0, 0, time.UTC)

	report, err := cron.InspectSchedule(sched, start, end)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(report.Activations) != 3 {
		t.Fatalf("got %d activations, want 3", len(report.Activations))
	}
	if report.Activations[0].Gap || report.Activations[2].Gap {
		t.Errorf("only the first activation after the jump may be a gap: %+v", report.Activations)
	}
	if !report.Activations[1].Gap {
		t.Errorf("03:00 activation should be flagged as the DST gap")
	}
	for i := range report.Activations {
		if report.Activations[i].RepeatedWall {
			t.Errorf("activation %d must not be flagged RepeatedWall", i)
		}
	}
}

// The window is half-open: an activation exactly at end is not listed.
func TestInspectSchedule_EndIsExclusive(t *testing.T) {
	start := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Hour)
	sched := cron.Every(time.Hour)

	report, err := cron.InspectSchedule(sched, start, end)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(report.Activations) != 1 {
		t.Fatalf("got %d activations, want exactly 1 (end excluded)", len(report.Activations))
	}
	if got := report.Activations[0].UTC; !got.Equal(start.Add(time.Hour)) {
		t.Errorf("activation = %s, want %s", got, start.Add(time.Hour))
	}
	if !report.Start.Equal(start) || !report.End.Equal(end) {
		t.Errorf("bounds = %s..%s, want %s..%s", report.Start, report.End, start, end)
	}
}

// Results must appear in strict ascending UTC order.
func TestInspectSchedule_StrictlyAdvancing(t *testing.T) {
	sched := cron.Every(30 * time.Minute)
	start := time.Date(2021, 6, 1, 12, 0, 0, 0, time.UTC)
	end := start.Add(3 * time.Hour)

	report, err := cron.InspectSchedule(sched, start, end)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(report.Activations) != 5 { // :30, 1:00, 1:30, 2:00, 2:30
		t.Fatalf("got %d activations, want 5", len(report.Activations))
	}
	for i := 1; i < len(report.Activations); i++ {
		if !report.Activations[i].UTC.After(report.Activations[i-1].UTC) {
			t.Fatalf("activation %d is not strictly after %d", i, i-1)
		}
	}
}

// The walk drives Next with start and then each previous result verbatim, and
// never calls Next again once a result reaches or passes end.
func TestInspectSchedule_CallsOriginalNext(t *testing.T) {
	a := time.Date(2021, 6, 1, 12, 30, 0, 0, time.UTC)
	b := time.Date(2021, 6, 1, 13, 0, 0, 0, time.UTC)
	start := time.Date(2021, 6, 1, 12, 0, 0, 0, time.UTC)
	end := b

	sched := &tracingSchedule{scriptedSchedule: scriptedSchedule{times: []time.Time{a, b}}}
	if _, err := cron.InspectSchedule(sched, start, end); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sched.args) != 2 {
		t.Fatalf("Next called %d times, want 2 (start, then a); b == end must not be fetched", len(sched.args))
	}
	if !sched.args[0].Equal(start) || !sched.args[1].Equal(a) {
		t.Fatalf("Next arguments = %v, want [%s %s]", sched.args, start, a)
	}
}

// Inspecting never starts or runs a Job.
func TestInspectSchedule_DoesNotRunJobs(t *testing.T) {
	c := cron.New()
	var ran int
	_, err := c.AddFunc("@every 1m", func() { ran++ })
	if err != nil {
		t.Fatal(err)
	}
	entry := c.Entries()[0]

	start := time.Date(2021, 6, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	if _, err := cron.InspectSchedule(entry.Schedule, start, end); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ran != 0 {
		t.Fatalf("inspection ran the job %d times; it must not start jobs", ran)
	}
}

// Empty or invalid windows are rejected up front.
func TestInspectSchedule_InvalidWindow(t *testing.T) {
	start := time.Date(2021, 6, 1, 0, 0, 0, 0, time.UTC)
	if _, err := cron.InspectSchedule(cron.Every(time.Minute), start, start); err == nil {
		t.Fatal("expected error for empty window")
	}
	if _, err := cron.InspectSchedule(nil, start, start.Add(time.Hour)); err == nil {
		t.Fatal("expected error for nil schedule")
	}
}

// The rendered table must mention both UTC instants and the repeated note.
func TestReport_String(t *testing.T) {
	sched := &scriptedSchedule{times: []time.Time{
		time.Date(2021, 11, 7, 1, 30, 0, 0, edt),
		time.Date(2021, 11, 7, 1, 30, 0, 0, est),
	}}
	start := time.Date(2021, 11, 7, 5, 0, 0, 0, time.UTC)
	end := time.Date(2021, 11, 7, 7, 0, 0, 0, time.UTC)

	report, err := cron.InspectSchedule(sched, start, end)
	if err != nil {
		t.Fatal(err)
	}
	out := report.String()
	for _, want := range []string{
		"2021-11-07T05:30:00Z",
		"2021-11-07T06:30:00Z",
		"2021-11-07 01:30:00",
		"-04:00",
		"-05:00",
		"repeated local wall-clock time (DST fall back)",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered report missing %q:\n%s", want, out)
		}
	}
}

// Cross-check with a real SpecSchedule in America/New_York when the zone
// database is available on the host. "30 1 * * *" fires twice in the
// fall-back window (01:30 EDT and 01:30 EST).
func TestInspectSchedule_SpecFallBackInNewYork(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("America/New_York unavailable on this host: %v", err)
	}

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sched, err := parser.Parse("CRON_TZ=America/New_York 30 1 * * *")
	if err != nil {
		t.Fatal(err)
	}

	start := time.Date(2021, 11, 7, 0, 0, 0, 0, loc)
	end := time.Date(2021, 11, 7, 12, 0, 0, 0, loc)
	report, err := cron.InspectSchedule(sched, start, end)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(report.Activations) != 2 {
		t.Fatalf("got %d activations, want both 01:30 labels: %v",
			len(report.Activations), report.Activations)
	}
	for i, a := range report.Activations {
		if a.Local.Format("15:04") != "01:30" {
			t.Errorf("activation %d wall = %s, want 01:30", i, a.Local.Format("15:04"))
		}
		if !a.RepeatedWall {
			t.Errorf("activation %d not flagged RepeatedWall", i)
		}
	}
	if report.Activations[0].UTC.Equal(report.Activations[1].UTC) {
		t.Fatal("the two 01:30 labels must have distinct UTC instants")
	}
}

// Same cross-check for the spring gap: an hourly New York schedule jumps
// from 01:00 EST to 03:00 EDT and the 03:00 activation is flagged Gap.
func TestInspectSchedule_SpecSpringGapInNewYork(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("America/New_York unavailable on this host: %v", err)
	}

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sched, err := parser.Parse("CRON_TZ=America/New_York 0 * * * *")
	if err != nil {
		t.Fatal(err)
	}

	start := time.Date(2021, 3, 14, 0, 30, 0, 0, loc)
	end := time.Date(2021, 3, 14, 5, 0, 0, 0, loc)
	report, err := cron.InspectSchedule(sched, start, end)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(report.Activations) != 3 {
		t.Fatalf("got %d activations, want 01:00, 03:00, 04:00: %v",
			len(report.Activations), report.Activations)
	}
	if got := report.Activations[1].Local.Format("15:04"); got != "03:00" {
		t.Errorf("second activation wall = %s, want 03:00 (02:00 skipped)", got)
	}
	if !report.Activations[1].Gap {
		t.Errorf("03:00 activation must be flagged Gap")
	}
	if report.Activations[0].Gap || report.Activations[2].Gap {
		t.Errorf("only the first activation after the jump may be a gap")
	}
}

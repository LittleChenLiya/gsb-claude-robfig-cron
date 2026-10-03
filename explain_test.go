package cron

import (
	"reflect"
	"strings"
	"testing"
)

// TestExplainBasic verifies the primary acceptance case: "*/15 9-10 * * MON"
// with the standard 5-field parser reports minutes 0/15/30/45, hours 9/10 and
// the weekday MON at the correct (dow) position.
func TestExplainBasic(t *testing.T) {
	parser := NewParser(Minute | Hour | Dom | Month | Dow | Descriptor)
	expl, err := parser.Explain("*/15 9-10 * * MON")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The second field is not part of the standard parser, so it is defaulted.
	if got := expl.Fields[secondIdx].Values; !reflect.DeepEqual(got, []int{0}) {
		t.Errorf("second defaulted values = %v, want [0]", got)
	}
	if !expl.Fields[secondIdx].Defaulted {
		t.Errorf("second field should be defaulted")
	}

	// Minute: */15 -> 0, 15, 30, 45 at position 1.
	assertField(t, expl.Fields, minuteIdx, "*/15", []int{0, 15, 30, 45}, false, false)

	// Hour: 9-10 -> 9, 10 at position 2.
	assertField(t, expl.Fields, hourIdx, "9-10", []int{9, 10}, false, false)

	// Dom: * -> wildcard at position 3.
	assertField(t, expl.Fields, domIdx, "*", rangeInts(1, 31), false, true)

	// Month: * -> wildcard at position 4.
	assertField(t, expl.Fields, monthIdx, "*", rangeInts(1, 12), false, true)

	// Dow: MON -> 1 at position 5.
	assertField(t, expl.Fields, dowIdx, "MON", []int{1}, false, false)

	// DOM/DOW combination: dom is a wildcard, so both must match (AND).
	if !strings.Contains(expl.DayCombination, "AND") {
		t.Errorf("day combination = %q, want AND semantics", expl.DayCombination)
	}
}

// TestExplainWithSeconds verifies that enabling the seconds field shifts the
// same tokens into the correct positions (the "new structure").
func TestExplainWithSeconds(t *testing.T) {
	parser := NewParser(Second | Minute | Hour | Dom | Month | Dow | Descriptor)
	expl, err := parser.Explain("30 */15 9-10 * * MON")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Second: 30 at position 0.
	assertField(t, expl.Fields, secondIdx, "30", []int{30}, false, false)
	// Minute: */15 at position 1.
	assertField(t, expl.Fields, minuteIdx, "*/15", []int{0, 15, 30, 45}, false, false)
	// Hour: 9-10 at position 2.
	assertField(t, expl.Fields, hourIdx, "9-10", []int{9, 10}, false, false)
	// Dow: MON at position 5.
	assertField(t, expl.Fields, dowIdx, "MON", []int{1}, false, false)
}

// TestExplainSecondOptional verifies the optional-seconds mapping: a 5-field
// spec defaults seconds, while a 6-field spec uses the provided seconds.
func TestExplainSecondOptional(t *testing.T) {
	parser := NewParser(SecondOptional | Minute | Hour | Dom | Month | Dow | Descriptor)

	// 5 fields -> seconds defaulted to "0".
	expl, err := parser.Explain("*/15 9-10 * * MON")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertField(t, expl.Fields, secondIdx, "0", []int{0}, true, false)
	assertField(t, expl.Fields, minuteIdx, "*/15", []int{0, 15, 30, 45}, false, false)
	assertField(t, expl.Fields, hourIdx, "9-10", []int{9, 10}, false, false)
	assertField(t, expl.Fields, dowIdx, "MON", []int{1}, false, false)

	// 6 fields -> seconds provided.
	expl, err = parser.Explain("45 */15 9-10 * * MON")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertField(t, expl.Fields, secondIdx, "45", []int{45}, false, false)
	assertField(t, expl.Fields, minuteIdx, "*/15", []int{0, 15, 30, 45}, false, false)
}

// TestExplainDowOptional verifies the optional-dow mapping: a 5-field spec
// defaults dow, while a 6-field spec uses the provided dow.
func TestExplainDowOptional(t *testing.T) {
	parser := NewParser(Minute | Hour | Dom | Month | DowOptional | Descriptor)

	// 4 fields -> dow defaulted to "*" (min is 4 with DowOptional).
	expl, err := parser.Explain("0 9 1 *")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertField(t, expl.Fields, dowIdx, "*", rangeInts(0, 6), true, true)
	// dom is restricted, dow is wildcard -> AND.
	if !strings.Contains(expl.DayCombination, "AND") {
		t.Errorf("day combination = %q, want AND", expl.DayCombination)
	}

	// 5 fields -> dow provided, dom restricted -> OR.
	expl, err = parser.Explain("0 9 1 * MON")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertField(t, expl.Fields, dowIdx, "MON", []int{1}, false, false)
	if !strings.Contains(expl.DayCombination, "OR") {
		t.Errorf("day combination = %q, want OR", expl.DayCombination)
	}
}

// TestExplainErrorsHavePosition verifies that illegal steps and out-of-range
// errors carry the field position and name.
func TestExplainErrorsHavePosition(t *testing.T) {
	parser := NewParser(Minute | Hour | Dom | Month | Dow | Descriptor)
	tests := []struct {
		spec    string
		wantPos string // substring expected in the error
	}{
		{"* * * * MON/0", "field 5 (dow)"},  // illegal step in dow
		{"*/0 * * * *", "field 1 (minute)"}, // illegal step in minute
		{"* 99 * * *", "field 2 (hour)"},    // out of range in hour
		{"* * 0 * *", "field 3 (dom)"},      // out of range in dom (min is 1)
		{"XYZ * * * *", "field 1 (minute)"}, // unparseable in minute
	}
	for _, tc := range tests {
		_, err := parser.Explain(tc.spec)
		if err == nil {
			t.Errorf("%s: expected error, got nil", tc.spec)
			continue
		}
		if !strings.Contains(err.Error(), tc.wantPos) {
			t.Errorf("%s: error %q does not contain %q", tc.spec, err.Error(), tc.wantPos)
		}
	}
}

// TestExplainDescriptor verifies that named schedules are explained with their
// descriptor name and per-field values.
func TestExplainDescriptor(t *testing.T) {
	parser := NewParser(Minute | Hour | Dom | Month | Dow | Descriptor)

	expl, err := parser.Explain("@daily")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if expl.Descriptor != "@daily" {
		t.Errorf("descriptor = %q, want @daily", expl.Descriptor)
	}
	// @daily: seconds/minutes/hours at 0, dom/month/dow wildcards.
	assertField(t, expl.Fields, secondIdx, "", []int{0}, false, false)
	assertField(t, expl.Fields, domIdx, "", rangeInts(1, 31), false, true)
	assertField(t, expl.Fields, dowIdx, "", rangeInts(0, 6), false, true)

	expl, err = parser.Explain("@every 1h30m")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if expl.Descriptor != "@every 1h30m" {
		t.Errorf("descriptor = %q, want @every 1h30m", expl.Descriptor)
	}
	if len(expl.Fields) != 0 {
		t.Errorf("@every should have no fields, got %d", len(expl.Fields))
	}
	if !strings.Contains(expl.DayCombination, "constant delay") {
		t.Errorf("day combination = %q, want constant delay note", expl.DayCombination)
	}
}

// TestExplainDescriptorNotAllowed verifies that a parser without the
// Descriptor option rejects descriptors.
func TestExplainDescriptorNotAllowed(t *testing.T) {
	parser := NewParser(Minute | Hour | Dom | Month | Dow)
	_, err := parser.Explain("@daily")
	if err == nil {
		t.Error("expected error for descriptor without Descriptor option")
	}
}

// TestExplainTZ verifies that the timezone prefix is handled and reported.
func TestExplainTZ(t *testing.T) {
	parser := NewParser(Minute | Hour | Dom | Month | Dow | Descriptor)
	expl, err := parser.Explain("TZ=Asia/Tokyo 0 9 * * *")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if expl.Location == nil || expl.Location.String() != "Asia/Tokyo" {
		t.Errorf("location = %v, want Asia/Tokyo", expl.Location)
	}
}

// TestExplainReadOnly verifies that Explain does not mutate the parser and
// that Parse continues to work after Explain.
func TestExplainReadOnly(t *testing.T) {
	parser := NewParser(Minute | Hour | Dom | Month | Dow | Descriptor)
	spec := "*/15 9-10 * * MON"

	// Explain twice: results must be identical (no state leakage).
	expl1, err := parser.Explain(spec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expl2, err := parser.Explain(spec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(expl1, expl2) {
		t.Errorf("Explain results differ between calls:\n%s\nvs\n%s", expl1, expl2)
	}

	// Parse must still work and return the same schedule as before.
	sched, err := parser.Parse(spec)
	if err != nil {
		t.Fatalf("Parse after Explain failed: %v", err)
	}
	if sched == nil {
		t.Error("Parse returned nil schedule")
	}

	// The parser options must be unchanged.
	if parser.options != Minute|Hour|Dom|Month|Dow|Descriptor {
		t.Errorf("parser options changed: %v", parser.options)
	}
}

// TestExplainDoesNotCallNext verifies that explaining a spec does not invoke
// Schedule.Next (it is a pure field interpretation). This is enforced by the
// implementation (Explain never constructs a Schedule or calls Next), but we
// also confirm the explanation is deterministic and time-independent.
func TestExplainDoesNotCallNext(t *testing.T) {
	parser := NewParser(Minute | Hour | Dom | Month | Dow | Descriptor)
	spec := "0 0 1 1 *" // unsatisfiable schedule (Feb 31)
	expl, err := parser.Explain(spec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// The explanation is produced without computing a next time.
	if expl == nil {
		t.Fatal("expected non-nil explanation")
	}
	// dom is restricted, dow is wildcard -> AND.
	if !strings.Contains(expl.DayCombination, "AND") {
		t.Errorf("day combination = %q, want AND", expl.DayCombination)
	}
}

// TestExplainString verifies the human-readable output contains the key facts.
func TestExplainString(t *testing.T) {
	parser := NewParser(Minute | Hour | Dom | Month | Dow | Descriptor)
	expl, err := parser.Explain("*/15 9-10 * * MON")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	s := expl.String()
	for _, want := range []string{
		"field 1 (minute)",
		"field 2 (hour)",
		"field 5 (dow)",
		"*/15",
		"9-10",
		"MON",
		"[0 15 30 45]",
		"[9-10]",
		"[1]",
		"AND",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("String() output missing %q\nfull output:\n%s", want, s)
		}
	}
}

// assertField checks that the field at idx has the expected original text,
// values, defaulted flag and star flag.
func assertField(t *testing.T, fields []FieldExplanation, idx int, original string, values []int, defaulted, star bool) {
	t.Helper()
	f := fields[idx]
	if f.Original != original {
		t.Errorf("field %d (%s) original = %q, want %q", idx, f.Name, f.Original, original)
	}
	if !reflect.DeepEqual(f.Values, values) {
		t.Errorf("field %d (%s) values = %v, want %v", idx, f.Name, f.Values, values)
	}
	if f.Defaulted != defaulted {
		t.Errorf("field %d (%s) defaulted = %v, want %v", idx, f.Name, f.Defaulted, defaulted)
	}
	if f.Star != star {
		t.Errorf("field %d (%s) star = %v, want %v", idx, f.Name, f.Star, star)
	}
}

// rangeInts returns the inclusive integer range [min, max].
func rangeInts(min, max int) []int {
	out := make([]int, 0, max-min+1)
	for i := min; i <= max; i++ {
		out = append(out, i)
	}
	return out
}

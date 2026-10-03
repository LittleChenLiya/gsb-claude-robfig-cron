package cron

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

var explainParser = NewParser(Minute | Hour | Dom | Month | Dow | Descriptor)

func fieldByName(t *testing.T, exp *Explanation, name string) FieldExplanation {
	t.Helper()
	for _, f := range exp.Fields {
		if f.Name == name {
			return f
		}
	}
	t.Fatalf("no field named %q in explanation", name)
	return FieldExplanation{}
}

func TestExplainStandardSpec(t *testing.T) {
	exp, err := explainParser.Explain("*/15 9-10 * * MON")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(exp.Fields) != 6 {
		t.Fatalf("expected 6 fields, got %d", len(exp.Fields))
	}

	cases := []struct {
		name     string
		position int
		original string
		values   []uint
		star     bool
	}{
		{"Second", -1, "0", []uint{0}, false},
		{"Minute", 0, "*/15", []uint{0, 15, 30, 45}, false},
		{"Hour", 1, "9-10", []uint{9, 10}, false},
		{"Day-of-month", 2, "*", nil, true},
		{"Month", 3, "*", nil, true},
		{"Day-of-week", 4, "MON", []uint{1}, false},
	}
	for _, c := range cases {
		f := fieldByName(t, exp, c.name)
		if f.Position != c.position {
			t.Errorf("%s: expected position %d, got %d", c.name, c.position, f.Position)
		}
		if f.Original != c.original {
			t.Errorf("%s: expected original %q, got %q", c.name, c.original, f.Original)
		}
		if c.values != nil && !reflect.DeepEqual(f.Values, c.values) {
			t.Errorf("%s: expected values %v, got %v", c.name, c.values, f.Values)
		}
		if f.Star != c.star {
			t.Errorf("%s: expected star=%v, got %v", c.name, c.star, f.Star)
		}
	}

	// Star fields select their full range.
	if got := fieldByName(t, exp, "Day-of-month").Values; len(got) != 31 || got[0] != 1 || got[30] != 31 {
		t.Errorf("Day-of-month: expected 1..31, got %v", got)
	}
	if got := fieldByName(t, exp, "Month").Values; len(got) != 12 || got[0] != 1 || got[11] != 12 {
		t.Errorf("Month: expected 1..12, got %v", got)
	}

	if sem := exp.DaySemantics(); !strings.Contains(sem, "BOTH") {
		t.Errorf("expected AND day semantics for star Dom, got %q", sem)
	}
}

func TestExplainWithSeconds(t *testing.T) {
	parser := NewParser(Second | Minute | Hour | Dom | Month | Dow)

	// The same five-field configuration no longer parses: with the seconds
	// field enabled, six fields are required.
	if _, err := parser.Explain("*/15 9-10 * * MON"); err == nil ||
		!strings.Contains(err.Error(), "expected exactly 6 fields") {
		t.Fatalf("expected field-count error, got %v", err)
	}

	exp, err := parser.Explain("5 */15 9-10 * * MON")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f := fieldByName(t, exp, "Second"); f.Position != 0 || !reflect.DeepEqual(f.Values, []uint{5}) {
		t.Errorf("Second: expected position 0 values [5], got position %d values %v", f.Position, f.Values)
	}
	if f := fieldByName(t, exp, "Minute"); f.Position != 1 || !reflect.DeepEqual(f.Values, []uint{0, 15, 30, 45}) {
		t.Errorf("Minute: expected position 1 values [0 15 30 45], got position %d values %v", f.Position, f.Values)
	}
	if f := fieldByName(t, exp, "Day-of-week"); f.Position != 5 || f.Original != "MON" || !reflect.DeepEqual(f.Values, []uint{1}) {
		t.Errorf("Day-of-week: expected position 5 original MON values [1], got position %d original %q values %v",
			f.Position, f.Original, f.Values)
	}
}

func TestExplainSecondOptional(t *testing.T) {
	parser := NewParser(SecondOptional | Minute | Hour | Dom | Month | Dow | Descriptor)

	// Omitted seconds: the same spec is reinterpreted under the new structure,
	// with the seconds field defaulted and every other position unchanged.
	exp, err := parser.Explain("*/15 9-10 * * MON")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f := fieldByName(t, exp, "Second"); f.Position != -1 || f.Original != "0" {
		t.Errorf("Second: expected defaulted field, got position %d original %q", f.Position, f.Original)
	}
	if f := fieldByName(t, exp, "Minute"); f.Position != 0 || f.Original != "*/15" {
		t.Errorf("Minute: expected position 0 original */15, got position %d original %q", f.Position, f.Original)
	}
	if f := fieldByName(t, exp, "Day-of-week"); f.Position != 4 || f.Original != "MON" {
		t.Errorf("Day-of-week: expected position 4 original MON, got position %d original %q", f.Position, f.Original)
	}

	// Provided seconds: positions shift by one.
	exp, err = parser.Explain("30 */15 9-10 * * MON")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f := fieldByName(t, exp, "Second"); f.Position != 0 || !reflect.DeepEqual(f.Values, []uint{30}) {
		t.Errorf("Second: expected position 0 values [30], got position %d values %v", f.Position, f.Values)
	}
	if f := fieldByName(t, exp, "Day-of-week"); f.Position != 5 {
		t.Errorf("Day-of-week: expected position 5, got %d", f.Position)
	}
}

func TestExplainDowOptional(t *testing.T) {
	parser := NewParser(Minute | Hour | Dom | Month | DowOptional)

	exp, err := parser.Explain("0 0 1 *")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f := fieldByName(t, exp, "Day-of-week"); f.Position != -1 || f.Original != "*" || !f.Star {
		t.Errorf("Day-of-week: expected defaulted star, got %+v", f)
	}
	if f := fieldByName(t, exp, "Month"); f.Position != 3 {
		t.Errorf("Month: expected position 3, got %d", f.Position)
	}

	exp, err = parser.Explain("0 0 1 * MON")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f := fieldByName(t, exp, "Day-of-week"); f.Position != 4 || !reflect.DeepEqual(f.Values, []uint{1}) {
		t.Errorf("Day-of-week: expected position 4 values [1], got %+v", f)
	}
}

func TestExplainErrors(t *testing.T) {
	tests := []struct {
		name   string
		parser Parser
		spec   string
		substr []string
	}{
		{"ZeroStep", explainParser, "*/0 9-10 * * MON",
			[]string{"Minute", "#1", "step of range should be a positive number"}},
		{"BadStep", explainParser, "0 0 * * */abc",
			[]string{"Day-of-week", "#5", "failed to parse int"}},
		{"HourAboveMax", explainParser, "0 25 * * *",
			[]string{"Hour", "#2", "above maximum"}},
		{"DomBelowMin", explainParser, "0 0 0 * *",
			[]string{"Day-of-month", "#3", "below minimum"}},
		{"MonthAboveMax", explainParser, "0 0 * 13 *",
			[]string{"Month", "#4", "above maximum"}},
		{"SecondAboveMax", NewParser(Second | Minute | Hour | Dom | Month | Dow), "61 * * * * *",
			[]string{"Second", "#1", "above maximum"}},
		{"TooManyHyphens", explainParser, "1-2-3 * * * *",
			[]string{"Minute", "#1", "too many hyphens"}},
		{"EmptySpec", explainParser, "",
			[]string{"empty spec string"}},
		{"FieldCount", explainParser, "* * *",
			[]string{"expected exactly 5 fields"}},
	}
	for _, c := range tests {
		t.Run(c.name, func(t *testing.T) {
			exp, err := c.parser.Explain(c.spec)
			if err == nil {
				t.Fatalf("expected error, got explanation %+v", exp)
			}
			for _, sub := range c.substr {
				if !strings.Contains(err.Error(), sub) {
					t.Errorf("expected error to contain %q, got %q", sub, err.Error())
				}
			}
		})
	}
}

func TestExplainDescriptor(t *testing.T) {
	exp, err := explainParser.Explain("@weekly")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exp.Descriptor != "@weekly" {
		t.Errorf("expected descriptor @weekly, got %q", exp.Descriptor)
	}
	if len(exp.Fields) != 6 {
		t.Fatalf("expected 6 fields, got %d", len(exp.Fields))
	}
	for _, f := range exp.Fields {
		if f.Position != -1 {
			t.Errorf("%s: descriptor fields have no spec position, got %d", f.Name, f.Position)
		}
	}
	if f := fieldByName(t, exp, "Day-of-week"); !reflect.DeepEqual(f.Values, []uint{0}) {
		t.Errorf("Day-of-week: expected [0] (Sunday), got %v", f.Values)
	}
	if f := fieldByName(t, exp, "Hour"); !reflect.DeepEqual(f.Values, []uint{0}) {
		t.Errorf("Hour: expected [0], got %v", f.Values)
	}

	// @every yields an interval, not calendar fields.
	exp, err = explainParser.Explain("@every 1h30m")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exp.Every != 90*time.Minute {
		t.Errorf("expected interval 1h30m, got %v", exp.Every)
	}
	if len(exp.Fields) != 0 {
		t.Errorf("expected no fields for @every, got %v", exp.Fields)
	}
	if sem := exp.DaySemantics(); sem != "" {
		t.Errorf("expected no day semantics for @every, got %q", sem)
	}

	// Descriptors must be enabled on the parser.
	noDesc := NewParser(Minute | Hour | Dom | Month | Dow)
	if _, err := noDesc.Explain("@daily"); err == nil ||
		!strings.Contains(err.Error(), "does not accept descriptors") {
		t.Errorf("expected descriptor rejection, got %v", err)
	}

	// Unknown descriptors still fail.
	if _, err := explainParser.Explain("@sometimes"); err == nil ||
		!strings.Contains(err.Error(), "unrecognized descriptor") {
		t.Errorf("expected unrecognized descriptor error, got %v", err)
	}
}

func TestExplainDomDowSemantics(t *testing.T) {
	tests := []struct {
		spec string
		want string
	}{
		// Both restricted: either match fires the schedule.
		{"0 0 1 * MON", "EITHER"},
		{"0 0 1,15 * MON-FRI", "EITHER"},
		// At least one star: both must match.
		{"0 0 * * MON", "BOTH"},
		{"0 0 1 * *", "BOTH"},
		{"0 0 * * *", "BOTH"},
		{"0 0 ? * MON", "BOTH"},
	}
	for _, c := range tests {
		exp, err := explainParser.Explain(c.spec)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", c.spec, err)
			continue
		}
		if sem := exp.DaySemantics(); !strings.Contains(sem, c.want) {
			t.Errorf("%s: expected day semantics containing %q, got %q", c.spec, c.want, sem)
		}
	}

	// The explanation must never merge Dom and Dow into a cartesian product:
	// each field keeps its own independent value set.
	exp, err := explainParser.Explain("0 0 1,15 * MON,WED")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f := fieldByName(t, exp, "Day-of-month"); !reflect.DeepEqual(f.Values, []uint{1, 15}) {
		t.Errorf("Day-of-month: expected [1 15], got %v", f.Values)
	}
	if f := fieldByName(t, exp, "Day-of-week"); !reflect.DeepEqual(f.Values, []uint{1, 3}) {
		t.Errorf("Day-of-week: expected [1 3], got %v", f.Values)
	}
}

func TestExplainTimezone(t *testing.T) {
	exp, err := explainParser.Explain("TZ=UTC 0 9 * * *")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exp.TimeZone != "UTC" {
		t.Errorf("expected time zone UTC, got %q", exp.TimeZone)
	}
	if f := fieldByName(t, exp, "Minute"); f.Position != 0 || f.Original != "0" {
		t.Errorf("Minute: expected position 0 in the spec after the TZ prefix, got %+v", f)
	}
	if f := fieldByName(t, exp, "Hour"); !reflect.DeepEqual(f.Values, []uint{9}) {
		t.Errorf("Hour: expected [9], got %v", f.Values)
	}
}

func TestExplainReadOnly(t *testing.T) {
	parser := NewParser(SecondOptional | Minute | Hour | Dom | Month | Dow | Descriptor)
	const spec = "*/15 9-10 * * MON"

	// Schedules parsed before and after Explain must be identical: Explain
	// must not mutate the parser or any shared state.
	before, err := parser.Parse(spec)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	first, err := parser.Explain(spec)
	if err != nil {
		t.Fatalf("unexpected explain error: %v", err)
	}
	after, err := parser.Parse(spec)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Errorf("Parse result changed after Explain: %v vs %v", before, after)
	}

	// Repeated Explain calls return equal results.
	second, err := parser.Explain(spec)
	if err != nil {
		t.Fatalf("unexpected explain error: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Errorf("Explain not repeatable:\n%v\n%v", first, second)
	}

	// Mutating a returned explanation must not affect later calls.
	first.Fields[1].Values[0] = 999
	third, err := parser.Explain(spec)
	if err != nil {
		t.Fatalf("unexpected explain error: %v", err)
	}
	if got := fieldByName(t, third, "Minute").Values; !reflect.DeepEqual(got, []uint{0, 15, 30, 45}) {
		t.Errorf("Explain shares state between calls, minute values: %v", got)
	}
}

func TestExplainString(t *testing.T) {
	exp, err := explainParser.Explain("*/15 9-10 * * MON")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := exp.String()
	for _, sub := range []string{
		"Spec: */15 9-10 * * MON",
		"Minute", "\"*/15\"", "0 15 30 45",
		"Hour", "\"9-10\"", "9 10",
		"Day-of-week", "\"MON\"", "#5",
		"Day semantics:",
	} {
		if !strings.Contains(out, sub) {
			t.Errorf("expected report to contain %q, got:\n%s", sub, out)
		}
	}
}

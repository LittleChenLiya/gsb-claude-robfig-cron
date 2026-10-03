package cron

import (
	"fmt"
	"strings"
	"time"
)

// FieldExplanation describes a single cron field after parsing: the raw text
// the user wrote, where it appeared in the original spec, and the concrete
// set of values it selects.
type FieldExplanation struct {
	// Position is the 0-based index of the field within the whitespace-separated
	// fields of the original spec. It is -1 when the field was not present in
	// the spec and was populated with a default value (e.g. an omitted optional
	// field, or a field not configured on the parser).
	Position int

	// Name is the canonical field name, e.g. "Minute" or "Day-of-week".
	Name string

	// Original is the raw text of the field, exactly as written in the spec
	// (or the default text when Position is -1). For schedules produced by a
	// descriptor such as "@daily", Original is empty because there is no
	// per-field source text; see Explanation.Descriptor instead.
	Original string

	// Values is the expanded set of values selected by the field, in ascending
	// order. Ranges, steps, lists and names are all resolved, e.g. "*/15"
	// in the minute field yields [0 15 30 45].
	Values []uint

	// Star reports whether the field matches every value in the sense of the
	// day-matching rules: it is true when the field was written as "*" or "?"
	// (with a step of 1). Note that "*/2" selects every other value but is not
	// a star for the purpose of the Dom/Dow combination semantics.
	Star bool
}

// Explanation is the result of Parser.Explain: a human- and machine-readable
// breakdown of what each field of a cron spec selects, without computing any
// activation times.
type Explanation struct {
	// Spec is the original spec string, including any TZ=/CRON_TZ= prefix.
	Spec string

	// TimeZone is the location name from a TZ= or CRON_TZ= prefix, if any.
	TimeZone string

	// Descriptor is the descriptor the spec resolved to (e.g. "@daily"), if
	// the spec used one. In that case Fields describes the schedule the
	// descriptor expands to.
	Descriptor string

	// Every is the fixed interval for "@every <duration>" specs, and zero
	// otherwise. When Every is non-zero, Fields is empty because the schedule
	// is interval-based rather than field-based.
	Every time.Duration

	// Fields holds one entry per cron field, in canonical order:
	// Second, Minute, Hour, Day-of-month, Month, Day-of-week.
	Fields []FieldExplanation
}

// fieldNames and fieldBounds are parallel to places (parser.go).
var fieldNames = []string{
	"Second",
	"Minute",
	"Hour",
	"Day-of-month",
	"Month",
	"Day-of-week",
}

var fieldBounds = []bounds{seconds, minutes, hours, dom, months, dow}

// Explain parses the spec exactly as Parse would, honoring the parser's
// configured options, the standard range/step/list syntax and descriptors,
// but instead of returning a Schedule it reports, for each field, the
// original text, its position in the original spec and the concrete set of
// values it selects. It never calls Next or otherwise guesses activation
// times; the reported values come directly from the parsed field bits.
//
// The day-of-month / day-of-week combination semantics are described
// separately by DaySemantics; the field value sets are intentionally NOT
// combined into a cartesian product.
func (p Parser) Explain(spec string) (*Explanation, error) {
	if len(spec) == 0 {
		return nil, fmt.Errorf("empty spec string")
	}

	exp := &Explanation{Spec: spec}

	// Extract timezone if present (same rules as Parse).
	var loc = time.Local
	if strings.HasPrefix(spec, "TZ=") || strings.HasPrefix(spec, "CRON_TZ=") {
		var err error
		i := strings.Index(spec, " ")
		eq := strings.Index(spec, "=")
		if loc, err = time.LoadLocation(spec[eq+1 : i]); err != nil {
			return nil, fmt.Errorf("provided bad location %s: %v", spec[eq+1:i], err)
		}
		exp.TimeZone = spec[eq+1 : i]
		spec = strings.TrimSpace(spec[i:])
	}

	// Handle named schedules (descriptors), if configured.
	if strings.HasPrefix(spec, "@") {
		if p.options&Descriptor == 0 {
			return nil, fmt.Errorf("parser does not accept descriptors: %v", spec)
		}
		return explainDescriptor(spec, loc, exp)
	}

	// Split on whitespace.
	fields := strings.Fields(spec)

	// Validate & fill in any omitted or optional fields, keeping track of
	// where each field appeared in the original spec.
	expanded, positions, err := normalizeFieldsWithPositions(fields, p.options)
	if err != nil {
		return nil, err
	}

	for i := range places {
		bits, err := getField(expanded[i], fieldBounds[i])
		if err != nil {
			return nil, explainFieldError(fieldNames[i], positions[i], expanded[i], err)
		}
		exp.Fields = append(exp.Fields, FieldExplanation{
			Position: positions[i],
			Name:     fieldNames[i],
			Original: expanded[i],
			Values:   bitsToValues(bits, fieldBounds[i]),
			Star:     bits&starBit > 0,
		})
	}
	return exp, nil
}

// explainFieldError wraps a field parse error with the field's name and its
// 1-based position in the original spec, so users can locate the problem.
func explainFieldError(name string, position int, original string, err error) error {
	if position < 0 {
		return fmt.Errorf("%s field (defaulted to %q): %v", name, original, err)
	}
	return fmt.Errorf("%s field (spec field #%d, %q): %v", name, position+1, original, err)
}

// explainDescriptor explains a descriptor spec by resolving it through the
// same code path as Parse, then reporting the resulting schedule's fields.
func explainDescriptor(spec string, loc *time.Location, exp *Explanation) (*Explanation, error) {
	sched, err := parseDescriptor(spec, loc)
	if err != nil {
		return nil, err
	}
	exp.Descriptor = spec
	switch s := sched.(type) {
	case *SpecSchedule:
		bits := []uint64{s.Second, s.Minute, s.Hour, s.Dom, s.Month, s.Dow}
		for i := range places {
			exp.Fields = append(exp.Fields, FieldExplanation{
				Position: -1,
				Name:     fieldNames[i],
				Original: "",
				Values:   bitsToValues(bits[i], fieldBounds[i]),
				Star:     bits[i]&starBit > 0,
			})
		}
	case ConstantDelaySchedule:
		exp.Every = s.Delay
		exp.Fields = nil
	default:
		return nil, fmt.Errorf("cannot explain schedule of type %T", sched)
	}
	return exp, nil
}

// bitsToValues expands the set bits (excluding the star bit) into the sorted
// list of values they represent, clamped to the field's bounds.
func bitsToValues(bits uint64, r bounds) []uint {
	var values []uint
	for i := r.min; i <= r.max; i++ {
		if bits&(1<<i) > 0 {
			values = append(values, i)
		}
	}
	return values
}

// DaySemantics describes how the day-of-month and day-of-week fields combine,
// mirroring the rules used when the schedule actually runs (see dayMatches):
//
//   - If either field is a star ("*" or "?"), a day must match BOTH fields.
//   - Otherwise (both fields are restricted), a day matches when EITHER
//     field matches.
//
// The two fields are never multiplied into a cartesian product; they act as
// two independent filters over calendar days.
func (e *Explanation) DaySemantics() string {
	if len(e.Fields) != len(places) {
		return ""
	}
	dom, dow := e.Fields[3], e.Fields[5]
	if dom.Star || dow.Star {
		return "a day must match BOTH day-of-month and day-of-week, " +
			"because at least one of them is '*' (unrestricted)"
	}
	return "a day matches when EITHER day-of-month OR day-of-week matches, " +
		"because both fields are restricted"
}

// String renders the explanation as a human-readable report.
func (e *Explanation) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "Spec: %s\n", e.Spec)
	if e.TimeZone != "" {
		fmt.Fprintf(&b, "Time zone: %s\n", e.TimeZone)
	}
	if e.Descriptor != "" {
		fmt.Fprintf(&b, "Descriptor: %s\n", e.Descriptor)
	}
	if e.Every != 0 {
		fmt.Fprintf(&b, "Runs every %s (interval schedule; no calendar fields).\n", e.Every)
		return b.String()
	}
	for _, f := range e.Fields {
		pos := "default"
		if f.Position >= 0 {
			pos = fmt.Sprintf("#%d", f.Position+1)
		}
		original := f.Original
		if original == "" {
			original = "(from descriptor)"
		}
		fmt.Fprintf(&b, "Field %-7s %-12s %-8q -> %s\n", pos, f.Name, original, f.formatValues())
	}
	if sem := e.DaySemantics(); sem != "" {
		fmt.Fprintf(&b, "Day semantics: %s.\n", sem)
	}
	return b.String()
}

// formatValues renders the selected values compactly: contiguous runs are
// shown as "lo-hi" and everything else as individual values.
func (f FieldExplanation) formatValues() string {
	if len(f.Values) == 0 {
		return "(none)"
	}
	var parts []string
	for i := 0; i < len(f.Values); {
		j := i
		for j+1 < len(f.Values) && f.Values[j+1] == f.Values[j]+1 {
			j++
		}
		switch {
		case j == i:
			parts = append(parts, fmt.Sprintf("%d", f.Values[i]))
		case j == i+1:
			parts = append(parts, fmt.Sprintf("%d %d", f.Values[i], f.Values[j]))
		default:
			parts = append(parts, fmt.Sprintf("%d-%d", f.Values[i], f.Values[j]))
		}
		i = j + 1
	}
	s := strings.Join(parts, " ")
	if f.Star {
		s += " (all)"
	}
	return s
}

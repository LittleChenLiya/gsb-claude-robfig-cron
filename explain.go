package cron

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Normalized field positions. These index the six-field list produced by
// normalizeFields (second=0, minute=1, hour=2, dom=3, month=4, dow=5).
const (
	secondIdx = 0
	minuteIdx = 1
	hourIdx   = 2
	domIdx    = 3
	monthIdx  = 4
	dowIdx    = 5
)

// fieldNames and fieldBounds are indexed by the normalized field position.
var (
	fieldNames  = [...]string{"second", "minute", "hour", "dom", "month", "dow"}
	fieldBounds = [...]bounds{seconds, minutes, hours, dom, months, dow}
)

// FieldExplanation describes how a single field of a cron expression was
// interpreted. It reports the verbatim original text of the field, the
// position it ended up in (after applying the parser's field options), and
// the resolved set of values.
type FieldExplanation struct {
	// Name is the canonical field name: "second", "minute", "hour", "dom",
	// "month" or "dow".
	Name string

	// Position is the 0-based index of the field within the normalized
	// six-field list (second=0, minute=1, hour=2, dom=3, month=4, dow=5).
	Position int

	// Original is the text of the field that was parsed. For fields present
	// in the spec this is the verbatim token; for fields that were defaulted
	// it is the default value (see Defaulted).
	Original string

	// Defaulted is true when the field was not present in the spec and was
	// filled in with its default value.
	Defaulted bool

	// Values is the sorted, de-duplicated set of values the field resolves
	// to. The wildcard bit is not included; see Star.
	Values []int

	// Star reports whether the field was a wildcard ("*" or "?").
	Star bool
}

// Explanation is the structured result of explaining a cron spec. It is
// produced by Parser.Explain without computing any next execution times.
type Explanation struct {
	// Descriptor is set when the spec used a named schedule (e.g. "@daily"
	// or "@every 1h30m"). It is empty for ordinary cron expressions.
	Descriptor string

	// Location is the timezone used to interpret the spec. It is nil when no
	// timezone was given (time.Local is used at parse time).
	Location *time.Location

	// Fields holds the per-field explanations in normalized order
	// (second, minute, hour, dom, month, dow).
	Fields []FieldExplanation

	// DayCombination explains how the day-of-month and day-of-week fields
	// combine, since they are not intersected as a Cartesian product.
	DayCombination string
}

// Explain parses the spec and returns a structured explanation of each field
// without computing any next execution times. It follows the same range, step
// and descriptor rules as Parse, but reports the verbatim original text, the
// resolved set of values and the position of every field. The day-of-month and
// day-of-week fields are explained separately (see DayCombination) rather than
// being intersected as a Cartesian product.
//
// Explain is read-only: it does not mutate the Parser or the spec, and it does
// not call Schedule.Next.
func (p Parser) Explain(spec string) (*Explanation, error) {
	if len(spec) == 0 {
		return nil, fmt.Errorf("empty spec string")
	}

	// Extract timezone if present (same logic as Parse).
	var loc = time.Local
	if strings.HasPrefix(spec, "TZ=") || strings.HasPrefix(spec, "CRON_TZ=") {
		var err error
		i := strings.Index(spec, " ")
		eq := strings.Index(spec, "=")
		if loc, err = time.LoadLocation(spec[eq+1 : i]); err != nil {
			return nil, fmt.Errorf("provided bad location %s: %v", spec[eq+1:i], err)
		}
		spec = strings.TrimSpace(spec[i:])
	}

	// Handle named schedules (descriptors), if configured.
	if strings.HasPrefix(spec, "@") {
		if p.options&Descriptor == 0 {
			return nil, fmt.Errorf("parser does not accept descriptors: %v", spec)
		}
		return p.explainDescriptor(spec, loc)
	}

	rawFields := strings.Fields(spec)

	// Normalize into the full six-field list (same as Parse).
	normalized, err := normalizeFields(rawFields, p.options)
	if err != nil {
		return nil, err
	}

	// Determine which normalized positions are active and whether the
	// optional field was defaulted, so we can report the correct original
	// text for every position.
	opts := p.options
	if opts&SecondOptional > 0 {
		opts |= Second
	}
	if opts&DowOptional > 0 {
		opts |= Dow
	}
	var active []int
	for i, place := range places {
		if opts&place > 0 {
			active = append(active, i)
		}
	}

	// The optional field is defaulted when the spec provided one fewer field
	// than the active count.
	secondOptionalDefaulted := p.options&SecondOptional > 0 && len(rawFields) == len(active)-1
	dowOptionalDefaulted := p.options&DowOptional > 0 && len(rawFields) == len(active)-1

	// Map each normalized position to its source: either a raw field or the
	// default.
	source := make([]string, len(places))
	defaulted := make([]bool, len(places))
	for i := range source {
		source[i] = defaults[i]
		defaulted[i] = true
	}
	rawIdx := 0
	for _, pos := range active {
		switch {
		case pos == secondIdx && secondOptionalDefaulted:
			// seconds defaulted to "0" (already set)
		case pos == dowIdx && dowOptionalDefaulted:
			// dow defaulted to "*" (already set)
		default:
			source[pos] = rawFields[rawIdx]
			defaulted[pos] = false
			rawIdx++
		}
	}

	// Parse each normalized field, annotating errors with the position.
	fields := make([]FieldExplanation, len(places))
	for i := range places {
		_, values, star, err := fieldDetails(normalized[i], fieldBounds[i], i, fieldNames[i])
		if err != nil {
			return nil, err
		}
		fields[i] = FieldExplanation{
			Name:      fieldNames[i],
			Position:  i,
			Original:  source[i],
			Defaulted: defaulted[i],
			Values:    values,
			Star:      star,
		}
	}

	return &Explanation{
		Location:       loc,
		Fields:         fields,
		DayCombination: describeDayCombination(fields[domIdx], fields[dowIdx]),
	}, nil
}

// fieldDetails parses a field expression and returns the raw bits (including
// the wildcard bit), the resolved values and whether it was a wildcard.
// Errors are annotated with the field position and name.
func fieldDetails(field string, r bounds, pos int, name string) (bits uint64, values []int, star bool, err error) {
	bits, err = getField(field, r)
	if err != nil {
		return 0, nil, false, fmt.Errorf("field %d (%s): %v", pos, name, err)
	}
	star = bits&starBit != 0
	plain := bits &^ starBit
	for v := r.min; v <= r.max; v++ {
		if plain&(1<<v) != 0 {
			values = append(values, int(v))
		}
	}
	sort.Ints(values)
	return bits, values, star, nil
}

// explainDescriptor builds an explanation for a named schedule.
func (p Parser) explainDescriptor(descriptor string, loc *time.Location) (*Explanation, error) {
	sched, err := parseDescriptor(descriptor, loc)
	if err != nil {
		return nil, err
	}
	e := &Explanation{
		Descriptor: descriptor,
		Location:   loc,
	}
	switch s := sched.(type) {
	case *SpecSchedule:
		bits := [6]uint64{s.Second, s.Minute, s.Hour, s.Dom, s.Month, s.Dow}
		fields := make([]FieldExplanation, 6)
		for i := 0; i < 6; i++ {
			star := bits[i]&starBit != 0
			plain := bits[i] &^ starBit
			var values []int
			for v := fieldBounds[i].min; v <= fieldBounds[i].max; v++ {
				if plain&(1<<v) != 0 {
					values = append(values, int(v))
				}
			}
			fields[i] = FieldExplanation{
				Name:     fieldNames[i],
				Position: i,
				Values:   values,
				Star:     star,
			}
		}
		e.Fields = fields
		e.DayCombination = describeDayCombination(fields[domIdx], fields[dowIdx])
	case ConstantDelaySchedule:
		e.DayCombination = fmt.Sprintf("not applicable (constant delay of %s)", s.Delay)
	}
	return e, nil
}

// describeDayCombination explains how dom and dow combine. They are not
// intersected as a Cartesian product: if either is a wildcard both must match
// (AND), otherwise either may match (OR). This mirrors dayMatches in spec.go.
func describeDayCombination(dom, dow FieldExplanation) string {
	if dom.Star || dow.Star {
		return "day-of-month AND day-of-week must both match (one field is a wildcard)"
	}
	return "day-of-month OR day-of-week must match (both fields are restricted)"
}

// String returns a human-readable, multi-line explanation.
func (e *Explanation) String() string {
	var b strings.Builder
	if e.Descriptor != "" {
		fmt.Fprintf(&b, "descriptor: %s\n", e.Descriptor)
	}
	if e.Location != nil {
		fmt.Fprintf(&b, "location: %s\n", e.Location)
	} else {
		b.WriteString("location: local\n")
	}
	if len(e.Fields) > 0 {
		b.WriteString("fields:\n")
		for _, f := range e.Fields {
			b.WriteString("  ")
			b.WriteString(f.String())
			b.WriteString("\n")
		}
	}
	if e.DayCombination != "" {
		fmt.Fprintf(&b, "day combination: %s\n", e.DayCombination)
	}
	return b.String()
}

// String returns a human-readable explanation of a single field.
func (f FieldExplanation) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "field %d (%s)", f.Position, f.Name)
	if f.Defaulted {
		b.WriteString(" [defaulted]")
	}
	fmt.Fprintf(&b, ": %q", f.Original)
	fmt.Fprintf(&b, " -> %s", formatValues(f.Values))
	if f.Star {
		b.WriteString(" (wildcard)")
	}
	return b.String()
}

// formatValues renders a sorted value set compactly: contiguous ranges are
// shown as "min-max", single values as "[v]", and non-contiguous sets as a
// list.
func formatValues(values []int) string {
	if len(values) == 0 {
		return "[]"
	}
	contiguous := true
	for i := 1; i < len(values); i++ {
		if values[i] != values[i-1]+1 {
			contiguous = false
			break
		}
	}
	if contiguous {
		if len(values) == 1 {
			return fmt.Sprintf("[%d]", values[0])
		}
		return fmt.Sprintf("[%d-%d]", values[0], values[len(values)-1])
	}
	return fmt.Sprintf("%v", values)
}

package cron_test

import (
	"fmt"

	"github.com/robfig/cron/v3"
)

// ExampleParser_Explain demonstrates explaining a standard 5-field cron
// expression without computing any next execution times.
func ExampleParser_Explain() {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	expl, err := parser.Explain("*/15 9-10 * * MON")
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	for _, f := range expl.Fields {
		if f.Defaulted {
			continue
		}
		fmt.Printf("field %d (%s): %v\n", f.Position, f.Name, f.Values)
	}
	fmt.Println(expl.DayCombination)
	// Output:
	// field 1 (minute): [0 15 30 45]
	// field 2 (hour): [9 10]
	// field 3 (dom): [1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31]
	// field 4 (month): [1 2 3 4 5 6 7 8 9 10 11 12]
	// field 5 (dow): [1]
	// day-of-month AND day-of-week must both match (one field is a wildcard)
}

// Command cronexplain prints a human-readable explanation of a cron spec:
// for each field it shows the original text, its position in the spec and
// the concrete set of values it selects, plus the day-of-month/day-of-week
// combination semantics. It never runs the schedule.
//
// Usage:
//
//	cronexplain [flags] "spec"
//
// Examples:
//
//	cronexplain "*/15 9-10 * * MON"
//	cronexplain -seconds "5 */15 9-10 * * MON"
//	cronexplain "@daily"
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/robfig/cron/v3"
)

func main() {
	var (
		seconds        = flag.Bool("seconds", false, "require a seconds field")
		secondOptional = flag.Bool("second-optional", false, "allow an optional seconds field")
		dowOptional    = flag.Bool("dow-optional", false, "allow an optional day-of-week field")
		noDescriptor   = flag.Bool("no-descriptor", false, "disallow @descriptors")
	)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: cronexplain [flags] \"spec\"\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}

	opts := cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow
	switch {
	case *seconds:
		opts |= cron.Second
	case *secondOptional:
		opts |= cron.SecondOptional
	}
	if *dowOptional {
		opts |= cron.DowOptional
	}
	if !*noDescriptor {
		opts |= cron.Descriptor
	}

	parser := cron.NewParser(opts)
	exp, err := parser.Explain(flag.Arg(0))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Print(exp)
}

// Command explain prints a structured explanation of cron expressions without
// computing any next execution times.
//
// Usage:
//
//	explain [-s] <spec> [spec...]
//
// By default it uses the standard 5-field parser (minute hour dom month dow)
// with descriptors enabled. Pass -s to use the 6-field Quartz-style parser
// that includes a leading seconds field.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/robfig/cron/v3"
)

func main() {
	seconds := flag.Bool("s", false, "include a seconds field (6-field Quartz-style spec)")
	flag.Parse()
	if flag.NArg() == 0 {
		fmt.Fprintf(os.Stderr, "usage: explain [-s] <spec> [spec...]\n")
		os.Exit(1)
	}

	opts := cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor
	if *seconds {
		opts |= cron.Second
	}
	parser := cron.NewParser(opts)

	for _, spec := range flag.Args() {
		expl, err := parser.Explain(spec)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", spec, err)
			continue
		}
		fmt.Printf("spec: %s\n%s\n", spec, expl)
	}
}

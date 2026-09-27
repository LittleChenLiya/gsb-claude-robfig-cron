[![GoDoc](http://godoc.org/github.com/robfig/cron?status.png)](http://godoc.org/github.com/robfig/cron)
[![Build Status](https://travis-ci.org/robfig/cron.svg?branch=master)](https://travis-ci.org/robfig/cron)

# cron

Cron V3 has been released!

To download the specific tagged release, run:
```bash
go get github.com/robfig/cron/v3@v3.0.0
```
Import it in your program as:
```go
import "github.com/robfig/cron/v3"
```
It requires Go 1.11 or later due to usage of Go Modules.

Refer to the documentation here:
http://godoc.org/github.com/robfig/cron

The rest of this document describes the the advances in v3 and a list of
breaking changes for users that wish to upgrade from an earlier version.

## Upgrading to v3 (June 2019)

cron v3 is a major upgrade to the library that addresses all outstanding bugs,
feature requests, and rough edges. It is based on a merge of master which
contains various fixes to issues found over the years and the v2 branch which
contains some backwards-incompatible features like the ability to remove cron
jobs. In addition, v3 adds support for Go Modules, cleans up rough edges like
the timezone support, and fixes a number of bugs.

New features:

- Support for Go modules. Callers must now import this library as
  `github.com/robfig/cron/v3`, instead of `gopkg.in/...`

- Fixed bugs:
  - 0f01e6b parser: fix combining of Dow and Dom (#70)
  - dbf3220 adjust times when rolling the clock forward to handle non-existent midnight (#157)
  - eeecf15 spec_test.go: ensure an error is returned on 0 increment (#144)
  - 70971dc cron.Entries(): update request for snapshot to include a reply channel (#97)
  - 1cba5e6 cron: fix: removing a job causes the next scheduled job to run too late (#206)

- Standard cron spec parsing by default (first field is "minute"), with an easy
  way to opt into the seconds field (quartz-compatible). Although, note that the
  year field (optional in Quartz) is not supported.

- Extensible, key/value logging via an interface that complies with
  the https://github.com/go-logr/logr project.

- The new Chain & JobWrapper types allow you to install "interceptors" to add
  cross-cutting behavior like the following:
  - Recover any panics from jobs
  - Delay a job's execution if the previous run hasn't completed yet
  - Skip a job's execution if the previous run hasn't completed yet
  - Log each job's invocations
  - Notification when jobs are completed

It is backwards incompatible with both v1 and v2. These updates are required:

- The v1 branch accepted an optional seconds field at the beginning of the cron
  spec. This is non-standard and has led to a lot of confusion. The new default
  parser conforms to the standard as described by [the Cron wikipedia page].

  UPDATING: To retain the old behavior, construct your Cron with a custom
  parser:
```go
// Seconds field, required
cron.New(cron.WithSeconds())

// Seconds field, optional
cron.New(cron.WithParser(cron.NewParser(
	cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
)))
```
- The Cron type now accepts functional options on construction rather than the
  previous ad-hoc behavior modification mechanisms (setting a field, calling a setter).

  UPDATING: Code that sets Cron.ErrorLogger or calls Cron.SetLocation must be
  updated to provide those values on construction.

- CRON_TZ is now the recommended way to specify the timezone of a single
  schedule, which is sanctioned by the specification. The legacy "TZ=" prefix
  will continue to be supported since it is unambiguous and easy to do so.

  UPDATING: No update is required.

- By default, cron will no longer recover panics in jobs that it runs.
  Recovering can be surprising (see issue #192) and seems to be at odds with
  typical behavior of libraries. Relatedly, the `cron.WithPanicLogger` option
  has been removed to accommodate the more general JobWrapper type.

  UPDATING: To opt into panic recovery and configure the panic logger:
```go
cron.New(cron.WithChain(
  cron.Recover(logger),  // or use cron.DefaultLogger
))
```
- In adding support for https://github.com/go-logr/logr, `cron.WithVerboseLogger` was
  removed, since it is duplicative with the leveled logging.

  UPDATING: Callers should use `WithLogger` and specify a logger that does not
  discard `Info` logs. For convenience, one is provided that wraps `*log.Logger`:
```go
cron.New(
  cron.WithLogger(cron.VerbosePrintfLogger(logger)))
```

### Background - Cron spec format

There are two cron spec formats in common usage:

- The "standard" cron format, described on [the Cron wikipedia page] and used by
  the cron Linux system utility.

- The cron format used by [the Quartz Scheduler], commonly used for scheduled
  jobs in Java software

[the Cron wikipedia page]: https://en.wikipedia.org/wiki/Cron
[the Quartz Scheduler]: http://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/tutorial-lesson-06.html

The original version of this package included an optional "seconds" field, which
made it incompatible with both of these formats. Now, the "standard" format is
the default format accepted, and the Quartz format is opt-in.

## Staggering schedules (deterministic jitter)

Many jobs scheduled for the same instant (for example, everything registered
with `* * * * *`) all fire at once. `StaggerSchedule` wraps any existing
`Schedule` and shifts every one of its activation instants by a deterministic,
key-dependent delay: the same key and the same original instant always produce
the same delay, in every process and on every machine.

```go
original, _ := cron.ParseStandard("* * * * *")

// Up to 20 seconds of deterministic spread; queries are only valid at or
// after start.
start := time.Now()
staggered, err := cron.NewStaggerSchedule(original, "tenant-42", 20*time.Second, start)
if err != nil {
    log.Fatal(err)
}

c := cron.New()
c.Schedule(staggered, job)
```

Properties:

- A delayed activation never crosses the following original activation. If the
  computed delay would reach or pass the next original instant, the activation
  is clamped to **one nanosecond before** that next original instant.
- `Next(t)` returns the earliest actual activation strictly later than `t`.
- Wrapping fixes an explicit start time. `Next(t)` panics (and `NextAfter(t)`
  returns `ErrStaggerBeforeStart`) when `t` is before that start time; original
  activations earlier than the start are dropped.
- Querying after an original instant but before its delayed instant still
  returns that pending delayed instant; the activation is not skipped.
- A zero result means the wrapped schedule itself has no further activation.

### Fixed delay algorithm

The algorithm and encoding below are part of the package's stable contract:
given the same inputs, the computed delay never changes.

For each original activation instant `T`:

1. Encode `T` as its **UTC Unix time in nanoseconds**, formatted as a decimal
   ASCII integer (`time.Time.UTC().UnixNano()`). Using the absolute UTC
   instant means time zones and DST transitions do not affect the result.
2. Build the message bytes as: the UTF-8 bytes of the key, one NUL separator
   byte (`0x00`), and the decimal instant bytes. Keys containing a NUL byte are
   rejected. The separator guarantees that different `(key, instant)` pairs
   cannot collide.
3. Hash the message with **64-bit FNV-1a** as implemented by
   [`hash/fnv`](https://pkg.go.dev/hash/fnv).
4. Reduce the hash modulo `maxDelay` in nanoseconds, producing a delay in
   `[0, maxDelay)` nanoseconds.
5. Add the delay to the original instant; if the result reaches or passes the
   next original instant, clamp it to one nanosecond before that instant.

With a per-minute schedule and a 20 second maximum delay every activation is
therefore delayed by a stable amount between 0 and 19.999999999 seconds.

See `ExampleStaggerSchedule` for a fixed-clock demonstration that prints the
original and staggered plans side by side:

```
go test -run ExampleStaggerSchedule -v
```

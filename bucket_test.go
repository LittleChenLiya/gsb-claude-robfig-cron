package cron

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func staticKeyFunc(key string) BucketKeyFunc {
	return func(time.Time) string { return key }
}

func sequenceKeyFunc(keys ...string) BucketKeyFunc {
	var (
		mu  sync.Mutex
		idx int
	)
	return func(time.Time) string {
		mu.Lock()
		k := keys[idx]
		idx++
		mu.Unlock()
		return k
	}
}

func TestSkipDuplicateInBucket_Concurrent(t *testing.T) {
	var calls int32
	job := FuncJob(func() {
		atomic.AddInt32(&calls, 1)
	})
	wrapped := SkipDuplicateInBucket(DiscardLogger, 2, staticKeyFunc("A"), nil)(job)

	const n = 2
	var barrier sync.WaitGroup
	barrier.Add(n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			barrier.Done()
			barrier.Wait()
			wrapped.Run()
		}()
	}
	wg.Wait()

	if calls != 1 {
		t.Errorf("expected underlying job to run once, got %d", calls)
	}
}

func TestSkipDuplicateInBucket_SkipsAfterSuccess(t *testing.T) {
	var calls int32
	job := FuncJob(func() {
		atomic.AddInt32(&calls, 1)
	})
	wrapped := SkipDuplicateInBucket(DiscardLogger, 2, staticKeyFunc("A"), nil)(job)

	wrapped.Run()
	wrapped.Run()
	wrapped.Run()

	if calls != 1 {
		t.Errorf("expected 1 run (subsequent skips), got %d", calls)
	}
}

func TestSkipDuplicateInBucket_PanicRetry(t *testing.T) {
	var calls int32
	job := FuncJob(func() {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			panic("first attempt panics")
		}
	})
	wrapped := SkipDuplicateInBucket(DiscardLogger, 2, staticKeyFunc("A"), nil)(job)

	wrapped.Run()
	wrapped.Run()

	if calls != 2 {
		t.Errorf("expected 2 runs (panic then retry), got %d", calls)
	}
}

func TestSkipDuplicateInBucket_OnError(t *testing.T) {
	var got error
	job := FuncJob(func() {
		panic("boom")
	})
	wrapped := SkipDuplicateInBucket(DiscardLogger, 2, staticKeyFunc("A"), func(err error) {
		got = err
	})(job)

	wrapped.Run()

	if got == nil {
		t.Fatal("expected onError to be called, got nil")
	}
	if got.Error() != "boom" {
		t.Errorf("expected error %q, got %q", "boom", got.Error())
	}
}

func TestSkipDuplicateInBucket_EvictionKeepsInFlight(t *testing.T) {
	var (
		started = make(chan struct{})
		release = make(chan struct{})
		calls   int32
	)
	job := FuncJob(func() {
		if atomic.AddInt32(&calls, 1) == 1 {
			close(started)
			<-release
		}
	})
	d := newBucketDeduperForTest(DiscardLogger, 2, sequenceKeyFunc("A", "B", "C"), nil, job)
	wrapped := FuncJob(d.run)

	go wrapped.Run()
	<-started

	wrapped.Run()
	wrapped.Run()

	want := []string{"A", "C"}
	if got := d.keys(); !reflect.DeepEqual(got, want) {
		t.Errorf("expected keys %v, got %v", want, got)
	}

	close(release)
}

func TestSkipDuplicateInBucket_EvictionOrder(t *testing.T) {
	job := FuncJob(func() {})
	d := newBucketDeduperForTest(DiscardLogger, 2, sequenceKeyFunc("A", "B", "C"), nil, job)
	wrapped := FuncJob(d.run)

	wrapped.Run()
	wrapped.Run()
	wrapped.Run()

	want := []string{"B", "C"}
	if got := d.keys(); !reflect.DeepEqual(got, want) {
		t.Errorf("expected keys %v, got %v", want, got)
	}
}

func TestSkipDuplicateInBucket_EvictionSkips(t *testing.T) {
	var calls int32
	job := FuncJob(func() {
		atomic.AddInt32(&calls, 1)
	})
	wrapped := SkipDuplicateInBucket(DiscardLogger, 2, sequenceKeyFunc("A", "B", "C", "A", "B", "C"), nil)(job)

	wrapped.Run()
	wrapped.Run()
	wrapped.Run()
	wrapped.Run()
	wrapped.Run()
	wrapped.Run()

	if calls != 6 {
		t.Errorf("expected 6 runs, got %d", calls)
	}
}

func TestSkipDuplicateInBucket_DifferentBucketsIndependent(t *testing.T) {
	var calls int32
	job := FuncJob(func() {
		atomic.AddInt32(&calls, 1)
	})
	wrapped := SkipDuplicateInBucket(DiscardLogger, 2, sequenceKeyFunc("A", "B", "A", "B"), nil)(job)

	wrapped.Run()
	wrapped.Run()
	wrapped.Run()
	wrapped.Run()

	if calls != 2 {
		t.Errorf("expected 2 runs (A and B), got %d", calls)
	}
}

func TestSkipDuplicateInBucket_Race(t *testing.T) {
	var (
		keyMu sync.Mutex
		keyN  int
	)
	keyFunc := func(time.Time) string {
		keyMu.Lock()
		k := fmt.Sprintf("key-%d", keyN%5)
		keyN++
		keyMu.Unlock()
		return k
	}
	var calls int32
	job := FuncJob(func() {
		atomic.AddInt32(&calls, 1)
	})
	wrapped := SkipDuplicateInBucket(DiscardLogger, 2, keyFunc, nil)(job)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wrapped.Run()
		}()
	}
	wg.Wait()
}

func TestSkipDuplicateInBucket_InvalidArgs(t *testing.T) {
	t.Run("zero size", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Error("expected panic for zero size")
			}
		}()
		SkipDuplicateInBucket(DiscardLogger, 0, staticKeyFunc("A"), nil)
	})
	t.Run("nil keyFunc", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Error("expected panic for nil keyFunc")
			}
		}()
		SkipDuplicateInBucket(DiscardLogger, 2, nil, nil)
	})
}

func newBucketDeduperForTest(logger Logger, size int, keyFunc BucketKeyFunc, onError func(error), job Job) *bucketDeduper {
	return &bucketDeduper{
		size:    size,
		keyFunc: keyFunc,
		onError: onError,
		logger:  logger,
		job:     job,
		buckets: make(map[string]*bucketRun),
	}
}

package semaphore

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestHighConcurrencyFullCapacity verifies that under 100 concurrent
// acquirers on a 5-permit semaphore with persistence, the number of permits
// held simultaneously stably reaches the full permit count and never
// exceeds it. This is the core throughput/latency requirement.
func TestHighConcurrencyFullCapacity(t *testing.T) {
	_, insts := newCluster(t, 4, 5*time.Second)
	defer func() {
		for _, in := range insts {
			in.Close()
		}
	}()
	const permits = 5
	ctx := context.Background()
	if err := insts[0].EnsureSemaphore(ctx, "pool", permits, FIFO); err != nil {
		t.Fatal(err)
	}
	const goroutines = 100
	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	cur, max := 0, 0
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			in := insts[idx%len(insts)]
			<-start
			l, err := in.Acquire(ctx, "pool", AcquireOptions{})
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			mu.Lock()
			cur++
			if cur > max {
				max = cur
			}
			mu.Unlock()
			time.Sleep(20 * time.Millisecond)
			mu.Lock()
			cur--
			mu.Unlock()
			if err := in.Release(ctx, l); err != nil {
				t.Errorf("release: %v", err)
			}
		}(g)
	}
	close(start)
	wg.Wait()
	if max < permits {
		t.Fatalf("never reached full capacity: max=%d want=%d", max, permits)
	}
	if max > permits {
		t.Fatalf("oversold: max=%d > %d", max, permits)
	}
	owner := ownerInst(insts, "pool")
	if owner == nil {
		t.Fatal("owner not found")
	}
	snap, _ := owner.engine.snapshot("pool")
	if snap.usedCount() != 0 || len(snap.Waiters) != 0 || len(snap.Holders) != 0 {
		t.Fatalf("state not clean: used=%d holders=%d waiters=%d",
			snap.usedCount(), len(snap.Holders), len(snap.Waiters))
	}
}

// TestPersistenceThroughput100 verifies that 100 concurrent acquirers on a
// 5-permit semaphore with persistence complete within 10 seconds.
func TestPersistenceThroughput100(t *testing.T) {
	dir := t.TempDir()
	b := NewBus()
	in, err := NewInstance(Config{
		InstanceID:      "solo",
		DataDir:         dir,
		DefaultLeaseTTL: 10 * time.Second,
		TickInterval:    10 * time.Millisecond,
	}, b)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	ctx := context.Background()
	if err := in.EnsureSemaphore(ctx, "pool", 5, FIFO); err != nil {
		t.Fatal(err)
	}
	const goroutines = 100
	start := time.Now()
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := in.Acquire(ctx, "pool", AcquireOptions{})
			if err != nil {
				t.Error(err)
				return
			}
			time.Sleep(20 * time.Millisecond)
			if err := in.Release(ctx, l); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	if elapsed > 10*time.Second {
		t.Fatalf("throughput too slow: %v", elapsed)
	}
	t.Logf("100 goroutines elapsed: %v", elapsed)
}

// TestGrantTimelinessUnderLoad measures the time from the first release to
// the last waiter grant under 100 concurrent acquirers. Grant latency must
// not degrade linearly with the number of waiters.
func TestGrantTimelinessUnderLoad(t *testing.T) {
	dir := t.TempDir()
	b := NewBus()
	in, err := NewInstance(Config{
		InstanceID:      "solo",
		DataDir:         dir,
		DefaultLeaseTTL: 10 * time.Second,
		TickInterval:    10 * time.Millisecond,
	}, b)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	ctx := context.Background()
	if err := in.EnsureSemaphore(ctx, "pool", 5, FIFO); err != nil {
		t.Fatal(err)
	}
	const waiters = 100
	// Fill all 5 permits with holders that release after a short delay.
	holders := make([]*Lease, 5)
	holderRelease := make(chan struct{})
	for i := range holders {
		l, err := in.Acquire(ctx, "pool", AcquireOptions{})
		if err != nil {
			t.Fatal(err)
		}
		in.stopLeaseHeartbeat(l.HolderID)
		holders[i] = l
	}
	// Queue 100 waiters.
	granted := make(chan time.Time, waiters)
	var wg sync.WaitGroup
	for i := 0; i < waiters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := in.Acquire(ctx, "pool", AcquireOptions{})
			if err != nil {
				t.Error(err)
				return
			}
			granted <- time.Now()
			_ = in.Release(ctx, l)
		}()
	}
	// Wait for all waiters to be queued.
	time.Sleep(200 * time.Millisecond)
	// Release holders one at a time, recording grant times.
	releaseStart := time.Now()
	for i := 0; i < 5; i++ {
		if err := in.Release(ctx, holders[i]); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Collect all grant times.
	deadline := time.After(10 * time.Second)
	count := 0
	for count < waiters {
		select {
		case <-granted:
			count++
		case <-deadline:
			t.Fatalf("only %d/%d waiters granted", count, waiters)
		}
	}
	elapsed := time.Since(releaseStart)
	if elapsed > 5*time.Second {
		t.Fatalf("grant timeliness too slow: %v", elapsed)
	}
	t.Logf("all %d waiters granted in %v", waiters, elapsed)
	close(holderRelease)
	wg.Wait()
}

// TestSemaphoreIsolation verifies that a high-throughput workload on one
// semaphore does not slow down operations on another semaphore.
func TestSemaphoreIsolation(t *testing.T) {
	dir := t.TempDir()
	b := NewBus()
	in, err := NewInstance(Config{
		InstanceID:      "solo",
		DataDir:         dir,
		DefaultLeaseTTL: 10 * time.Second,
		TickInterval:    10 * time.Millisecond,
	}, b)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	ctx := context.Background()
	if err := in.EnsureSemaphore(ctx, "a", 5, FIFO); err != nil {
		t.Fatal(err)
	}
	if err := in.EnsureSemaphore(ctx, "b", 5, FIFO); err != nil {
		t.Fatal(err)
	}
	// Fill "a" with 5 holders (no heartbeat, long TTL).
	holders := make([]*Lease, 5)
	for i := range holders {
		l, err := in.Acquire(ctx, "a", AcquireOptions{})
		if err != nil {
			t.Fatal(err)
		}
		in.stopLeaseHeartbeat(l.HolderID)
		holders[i] = l
	}
	var wg sync.WaitGroup
	start := time.Now()
	// Release "a" holders concurrently with "b" acquire/release cycles.
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if err := in.Release(ctx, holders[idx]); err != nil {
				t.Error(err)
			}
		}(i)
	}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := in.Acquire(ctx, "b", AcquireOptions{})
			if err != nil {
				t.Error(err)
				return
			}
			if err := in.Release(ctx, l); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	if elapsed > 5*time.Second {
		t.Fatalf("isolation too slow: %v", elapsed)
	}
	t.Logf("isolation elapsed: %v", elapsed)
}

// TestConcurrentCancelNoLeak verifies that concurrent canceled acquires do
// not leak permits or waiters.
func TestConcurrentCancelNoLeak(t *testing.T) {
	_, insts := newCluster(t, 4, 5*time.Second)
	defer func() {
		for _, in := range insts {
			in.Close()
		}
	}()
	const permits = 5
	ctx := context.Background()
	if err := insts[0].EnsureSemaphore(ctx, "pool", permits, FIFO); err != nil {
		t.Fatal(err)
	}
	const goroutines = 100
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			in := insts[idx%len(insts)]
			cctx, cancel := context.WithCancel(ctx)
			go func() {
				time.Sleep(time.Duration(idx%5) * time.Millisecond)
				cancel()
			}()
			l, err := in.Acquire(cctx, "pool", AcquireOptions{})
			if err == nil {
				time.Sleep(10 * time.Millisecond)
				_ = in.Release(ctx, l)
			}
		}(g)
	}
	wg.Wait()
	owner := ownerInst(insts, "pool")
	if owner == nil {
		t.Fatal("owner not found")
	}
	snap, _ := owner.engine.snapshot("pool")
	if snap.usedCount() != 0 || len(snap.Waiters) != 0 || len(snap.Holders) != 0 {
		t.Fatalf("leak: used=%d holders=%d waiters=%d",
			snap.usedCount(), len(snap.Holders), len(snap.Waiters))
	}
}

// TestConcurrentTimeoutNoLeak verifies that concurrent timed-out acquires do
// not leak permits or waiters.
func TestConcurrentTimeoutNoLeak(t *testing.T) {
	_, insts := newCluster(t, 4, 5*time.Second)
	defer func() {
		for _, in := range insts {
			in.Close()
		}
	}()
	const permits = 5
	ctx := context.Background()
	if err := insts[0].EnsureSemaphore(ctx, "pool", permits, FIFO); err != nil {
		t.Fatal(err)
	}
	const goroutines = 100
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			in := insts[idx%len(insts)]
			cctx, cancel := context.WithTimeout(ctx, time.Duration(idx%10+1)*time.Millisecond)
			defer cancel()
			l, err := in.Acquire(cctx, "pool", AcquireOptions{})
			if err == nil {
				time.Sleep(5 * time.Millisecond)
				_ = in.Release(ctx, l)
			}
		}(g)
	}
	wg.Wait()
	owner := ownerInst(insts, "pool")
	if owner == nil {
		t.Fatal("owner not found")
	}
	snap, _ := owner.engine.snapshot("pool")
	if snap.usedCount() != 0 || len(snap.Waiters) != 0 || len(snap.Holders) != 0 {
		t.Fatalf("leak: used=%d holders=%d waiters=%d",
			snap.usedCount(), len(snap.Holders), len(snap.Waiters))
	}
}

// TestCrashRecoveryAfterConcurrentGrants verifies that after 5 concurrent
// grants and a crash, the holders are recovered exactly once.
func TestCrashRecoveryAfterConcurrentGrants(t *testing.T) {
	dir := t.TempDir()
	b := NewBus()
	in, err := NewInstance(Config{
		InstanceID:      "solo",
		DataDir:         dir,
		DefaultLeaseTTL: 10 * time.Second,
		TickInterval:    10 * time.Millisecond,
	}, b)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := in.EnsureSemaphore(ctx, "pool", 5, FIFO); err != nil {
		t.Fatal(err)
	}
	leases := make([]*Lease, 5)
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			l, err := in.Acquire(ctx, "pool", AcquireOptions{})
			if err != nil {
				t.Error(err)
				return
			}
			leases[idx] = l
		}(i)
	}
	wg.Wait()
	in.Close()
	b2 := NewBus()
	in2, err := NewInstance(Config{
		InstanceID:      "solo",
		DataDir:         dir,
		DefaultLeaseTTL: 10 * time.Second,
		TickInterval:    10 * time.Millisecond,
	}, b2)
	if err != nil {
		t.Fatal(err)
	}
	defer in2.Close()
	snap, ok := in2.engine.snapshot("pool")
	if !ok {
		t.Fatal("semaphore not recovered")
	}
	if snap.usedCount() != 5 || len(snap.Holders) != 5 {
		t.Fatalf("holders not recovered: used=%d holders=%d",
			snap.usedCount(), len(snap.Holders))
	}
	for _, l := range leases {
		if err := in2.Release(ctx, l); err != nil {
			t.Fatal(err)
		}
	}
	snap, _ = in2.engine.snapshot("pool")
	if snap.usedCount() != 0 || len(snap.Holders) != 0 {
		t.Fatalf("state not clean: used=%d holders=%d", snap.usedCount(), len(snap.Holders))
	}
}

// TestDirectGrantWhenFree verifies that when free permits exist and the wait
// queue is empty, an acquire is granted immediately without entering the
// wait queue.
func TestDirectGrantWhenFree(t *testing.T) {
	dir := t.TempDir()
	b := NewBus()
	in, err := NewInstance(Config{
		InstanceID:      "solo",
		DataDir:         dir,
		DefaultLeaseTTL: 10 * time.Second,
		TickInterval:    10 * time.Millisecond,
	}, b)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	ctx := context.Background()
	if err := in.EnsureSemaphore(ctx, "pool", 5, FIFO); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		l, err := in.Acquire(ctx, "pool", AcquireOptions{})
		if err != nil {
			t.Fatal(err)
		}
		snap, _ := in.engine.snapshot("pool")
		if snap.usedCount() != 1 || len(snap.Waiters) != 0 {
			t.Fatalf("after %d grants: used=%d waiters=%d",
				i+1, snap.usedCount(), len(snap.Waiters))
		}
		_ = in.Release(ctx, l)
	}
}

var _ = fmt.Sprintf

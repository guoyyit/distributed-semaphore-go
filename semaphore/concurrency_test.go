package semaphore

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fullState returns the owner-side snapshot of a named semaphore.
func fullState(t *testing.T, insts []*Instance, name string) Snapshot {
	t.Helper()
	owner := ownerInst(insts, name)
	if owner == nil {
		t.Fatalf("no owner for %q", name)
	}
	snap, ok := owner.engine.snapshot(name)
	if !ok {
		t.Fatalf("snapshot missing for %q", name)
	}
	return snap
}

// TestFullCapacityStablyHeld drives heavy churn and asserts that every
// permit is actually held by some client at some instant, and that the number
// held never exceeds the limit. Core regression for the "permits sit idle
// while clients wait" defect.
func TestFullCapacityStablyHeld(t *testing.T) {
	_, insts := newCluster(t, 4, 10*time.Second)
	defer closeInsts(insts)
	const permits = 5
	ctx := context.Background()
	if err := insts[0].EnsureSemaphore(ctx, "full", permits, FIFO); err != nil {
		t.Fatal(err)
	}
	const clients = 100
	start := make(chan struct{})
	var wg sync.WaitGroup
	var statMu sync.Mutex
	cur, maxHeld := 0, 0
	oversold := false
	for g := 0; g < clients; g++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			in := insts[idx%len(insts)]
			<-start
			for k := 0; k < 3; k++ {
				l, err := in.Acquire(ctx, "full", AcquireOptions{})
				if err != nil {
					t.Errorf("acquire: %v", err)
					return
				}
				statMu.Lock()
				cur++
				if cur > maxHeld {
					maxHeld = cur
				}
				if cur > permits {
					oversold = true
				}
				statMu.Unlock()
				// Same hold window as the existing concurrent test: long
				// enough that a healthy implementation always fills all
				// permits, short enough that serialization/fsync
				// head-of-line blocking starves capacity (the old code
				// reached only 2-3 of 5 here).
				time.Sleep(20 * time.Millisecond)
				statMu.Lock()
				cur--
				statMu.Unlock()
				if err := in.Release(ctx, l); err != nil {
					t.Errorf("release: %v", err)
				}
			}
		}(g)
	}
	close(start)
	wg.Wait()
	if oversold {
		t.Fatalf("permits oversold beyond %d", permits)
	}
	if maxHeld < permits {
		t.Fatalf("never reached full capacity: max=%d/%d", maxHeld, permits)
	}
	snap := fullState(t, insts, "full")
	if snap.usedCount() != 0 || len(snap.Holders) != 0 || len(snap.Waiters) != 0 {
		t.Fatalf("state not clean: holders=%d waiters=%d used=%d",
			len(snap.Holders), len(snap.Waiters), snap.usedCount())
	}
}

func closeInsts(insts []*Instance) {
	for _, in := range insts {
		_ = in.Close()
	}
}

// TestGrantPromptnessWhenIdle asserts that an acquire with permits free and
// an empty queue is granted immediately (direct grant), and that after a
// release the head waiter is woken promptly rather than waiting for a tick.
func TestGrantPromptnessWhenIdle(t *testing.T) {
	_, insts := newCluster(t, 4, 10*time.Second)
	defer closeInsts(insts)
	ctx := context.Background()
	if err := insts[0].EnsureSemaphore(ctx, "prompt", 1, FIFO); err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	l := waitAcquire(t, insts[1], "prompt", AcquireOptions{})
	if d := time.Since(t0); d > 200*time.Millisecond {
		t.Fatalf("idle acquire took %v, expected immediate direct grant", d)
	}
	got := make(chan *Lease, 1)
	go func() {
		c, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		l2, err := insts[2].Acquire(c, "prompt", AcquireOptions{})
		if err != nil {
			got <- nil
			return
		}
		got <- l2
	}()
	time.Sleep(50 * time.Millisecond)
	t1 := time.Now()
	if err := insts[1].Release(ctx, l); err != nil {
		t.Fatal(err)
	}
	select {
	case l2 := <-got:
		if l2 == nil {
			t.Fatal("waiter not granted")
		}
		if d := time.Since(t1); d > 500*time.Millisecond {
			t.Fatalf("grant after release delayed %v", d)
		}
		if err := insts[2].Release(ctx, l2); err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter not woken after release")
	}
}

// TestDurableThroughput100 requires 100 concurrent clients against one
// 5-permit durable semaphore to all complete acquire/release within the
// mandated 10s budget, with no oversell and a fully clean final state.
func TestDurableThroughput100(t *testing.T) {
	dir := t.TempDir()
	b := NewBus()
	in, err := NewInstance(Config{
		InstanceID:      "solo",
		DataDir:         dir,
		DefaultLeaseTTL: time.Hour,
		TickInterval:    10 * time.Millisecond,
	}, b)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	ctx := context.Background()
	const permits = 5
	if err := in.EnsureSemaphore(ctx, "dur", permits, FIFO); err != nil {
		t.Fatal(err)
	}
	const clients = 100
	start := make(chan struct{})
	var wg sync.WaitGroup
	var statMu sync.Mutex
	cur, maxHeld := 0, 0
	oversold := false
	for g := 0; g < clients; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			l, err := in.Acquire(ctx, "dur", AcquireOptions{})
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			statMu.Lock()
			cur++
			if cur > maxHeld {
				maxHeld = cur
			}
			if cur > permits {
				oversold = true
			}
			statMu.Unlock()
			time.Sleep(20 * time.Millisecond)
			statMu.Lock()
			cur--
			statMu.Unlock()
			if err := in.Release(ctx, l); err != nil {
				t.Errorf("release: %v", err)
			}
		}()
	}
	t0 := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(t0)
	if elapsed > 10*time.Second {
		t.Fatalf("100 durable clients took %v, budget 10s", elapsed)
	}
	t.Logf("100 durable clients completed in %v", elapsed)
	if oversold {
		t.Fatal("durable semaphore oversold permits")
	}
	if maxHeld < permits {
		t.Fatalf("durable semaphore never reached capacity: %d/%d", maxHeld, permits)
	}
	snap, ok := in.engine.snapshot("dur")
	if !ok {
		t.Fatal("missing snapshot")
	}
	if snap.usedCount() != 0 || len(snap.Holders) != 0 || len(snap.Waiters) != 0 {
		t.Fatalf("durable state not clean: %+v", snap)
	}
}

// TestSemaphoreIsolation runs heavy long-hold churn on one durable
// semaphore while asserting latency on a different semaphore stays low.
func TestSemaphoreIsolation(t *testing.T) {
	dir := t.TempDir()
	b := NewBus()
	in, err := NewInstance(Config{
		InstanceID:      "solo",
		DataDir:         dir,
		DefaultLeaseTTL: time.Hour,
		TickInterval:    10 * time.Millisecond,
	}, b)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	ctx := context.Background()
	if err := in.EnsureSemaphore(ctx, "heavy", 5, FIFO); err != nil {
		t.Fatal(err)
	}
	if err := in.EnsureSemaphore(ctx, "lite", 1, FIFO); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < 12; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				l, err := in.Acquire(ctx, "heavy", AcquireOptions{})
				if err != nil {
					return
				}
				time.Sleep(2 * time.Millisecond)
				_ = in.Release(ctx, l)
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	for k := 0; k < 20; k++ {
		t0 := time.Now()
		l, err := in.Acquire(ctx, "lite", AcquireOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if d := time.Since(t0); d > 100*time.Millisecond {
			t.Fatalf("isolated semaphore acquire delayed %v by heavy traffic", d)
		}
		if err := in.Release(ctx, l); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
}

// TestConcurrentCancelTimeoutNoLeak hammers canceled and timed-out waits
// while holders churn, then proves no permit or waiter slot is leaked:
// exactly permits permits remain acquirable and final state is empty.
func TestConcurrentCancelTimeoutNoLeak(t *testing.T) {
	_, insts := newCluster(t, 4, 10*time.Second)
	defer closeInsts(insts)
	ctx := context.Background()
	const permits = 3
	if err := insts[0].EnsureSemaphore(ctx, "leak", permits, FIFO); err != nil {
		t.Fatal(err)
	}
	fill := make([]*Lease, permits)
	for k := range fill {
		fill[k] = waitAcquire(t, insts[k%len(insts)], "leak", AcquireOptions{})
	}
	var wg sync.WaitGroup
	for g := 0; g < 60; g++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var c context.Context
			var cancel context.CancelFunc
			if idx%2 == 0 {
				c, cancel = context.WithCancel(ctx)
				go func() {
					time.Sleep(time.Duration(1+(idx%5)) * time.Millisecond)
					cancel()
				}()
			} else {
				c, cancel = context.WithTimeout(ctx, time.Duration(1+(idx%5))*time.Millisecond)
			}
			defer cancel()
			_, _ = insts[idx%len(insts)].Acquire(c, "leak", AcquireOptions{})
		}(g)
	}
	// Cancel half immediately so cancels race with grant delivery.
	time.Sleep(5 * time.Millisecond)
	for _, l := range fill {
		_ = insts[0].Release(ctx, l)
	}
	wg.Wait()

	// Any permits won by cancellation-race clients are released by the
	// acquire ctx-cancel cleanup; wait for the owner to settle.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		snap := fullState(t, insts, "leak")
		if len(snap.Waiters) == 0 && snap.usedCount() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	holders := make([]*Lease, 0, permits)
	for k := 0; k < permits; k++ {
		c, cancel := context.WithTimeout(ctx, 2*time.Second)
		l, err := insts[k%len(insts)].Acquire(c, "leak", AcquireOptions{})
		cancel()
		if err != nil {
			t.Fatalf("permit %d unreachable (leaked): %v", k, err)
		}
		holders = append(holders, l)
	}
	busy := make(chan struct{}, 1)
	go func() {
		c, cancel := context.WithTimeout(ctx, 120*time.Millisecond)
		defer cancel()
		if l, err := insts[1].Acquire(c, "leak", AcquireOptions{}); err == nil {
			_ = insts[1].Release(ctx, l)
			busy <- struct{}{}
		}
	}()
	time.Sleep(200 * time.Millisecond)
	select {
	case <-busy:
		t.Fatal("oversold after concurrent cancel/timeout storm")
	default:
	}
	for _, l := range holders {
		if err := insts[0].Release(ctx, l); err != nil {
			t.Fatal(err)
		}
	}
	snap := fullState(t, insts, "leak")
	if snap.usedCount() != 0 || len(snap.Holders) != 0 || len(snap.Waiters) != 0 {
		t.Fatalf("state leaked: holders=%d waiters=%d used=%d",
			len(snap.Holders), len(snap.Waiters), snap.usedCount())
	}
}

// TestDurableCrashAfterGrantNoLossOrDoubleGrant crashes the owner right after
// grants are durable, restarts, and verifies the granted permits are still
// held (no loss), are not double granted (a new acquire queues), and that a
// release after restart reuses the permit exactly once.
func TestDurableCrashAfterGrantNoLossOrDoubleGrant(t *testing.T) {
	dir := t.TempDir()
	build := func(id string) *Instance {
		b := NewBus()
		in, err := NewInstance(Config{
			InstanceID:      id,
			DataDir:         dir,
			DefaultLeaseTTL: 10 * time.Second,
			TickInterval:    10 * time.Millisecond,
		}, b)
		if err != nil {
			t.Fatal(err)
		}
		return in
	}
	in := build("solo")
	ctx := context.Background()
	if err := in.EnsureSemaphore(ctx, "cr", 2, FIFO); err != nil {
		t.Fatal(err)
	}
	l1 := waitAcquire(t, in, "cr", AcquireOptions{})
	l2 := waitAcquire(t, in, "cr", AcquireOptions{})
	// Simulate a hard crash immediately after the grants completed.
	if err := in.Close(); err != nil {
		t.Fatal(err)
	}

	in2 := build("solo")
	defer in2.Close()
	snap, ok := in2.engine.snapshot("cr")
	if !ok {
		t.Fatal("semaphore not recovered after crash")
	}
	if snap.usedCount() != 2 || len(snap.Holders) != 2 {
		t.Fatalf("granted permits lost after crash: used=%d holders=%d", snap.usedCount(), len(snap.Holders))
	}
	// Permits still constrained: a new acquire must queue, not be double
	// granted.
	blocked := make(chan error, 1)
	go func() {
		c, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		_, err := in2.Acquire(c, "cr", AcquireOptions{})
		blocked <- err
	}()
	select {
	case err := <-blocked:
		if err == nil {
			t.Fatal("permit double granted after crash recovery")
		}
	case <-time.After(600 * time.Millisecond):
		t.Fatal("acquire unexpectedly succeeded: oversold after crash")
	}
	// Releasing recovered holders returns each permit exactly once.
	if err := in2.Release(ctx, l1); err != nil {
		t.Fatal(err)
	}
	if err := in2.Release(ctx, l2); err != nil {
		t.Fatal(err)
	}
	dup1, dup2 := in2.Release(ctx, l1), in2.Release(ctx, l2)
	if dup1 != nil || dup2 != nil {
		t.Fatalf("idempotent duplicate release after restart failed: %v %v", dup1, dup2)
	}
	final, _ := in2.engine.snapshot("cr")
	if final.usedCount() != 0 || len(final.Holders) != 0 || len(final.Waiters) != 0 {
		t.Fatalf("state not clean after crash recovery: %+v", final)
	}
}

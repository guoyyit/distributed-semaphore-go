package semaphore

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// holderWithoutHeartbeat acquires and immediately disables auto-renew, so the
// owner-side lease expiry is the only thing that can reclaim its permits.
func holderWithoutHeartbeat(t *testing.T, in *Instance, name string, opts AcquireOptions) *Lease {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	l, err := in.Acquire(ctx, name, opts)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	in.stopLeaseHeartbeat(l.HolderID)
	return l
}

func ownerInst(insts []*Instance, name string) *Instance {
	for _, in := range insts {
		if in.isOwner(name) {
			return in
		}
	}
	return nil
}

func TestEndToEndLeaseExpiry(t *testing.T) {
	_, insts := newCluster(t, 3, 150*time.Millisecond)
	defer func() {
		for _, in := range insts {
			in.Close()
		}
	}()
	ctx := context.Background()
	if err := insts[0].EnsureSemaphore(ctx, "lock", 1, FIFO); err != nil {
		t.Fatal(err)
	}
	l := holderWithoutHeartbeat(t, insts[1], "lock", AcquireOptions{})
	// Another client waits; the expired lease must be recycled and handed
	// over automatically.
	got := make(chan *Lease, 1)
	go func() {
		ctx2, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		l2, err := insts[2].Acquire(ctx2, "lock", AcquireOptions{})
		if err != nil {
			got <- nil
			return
		}
		got <- l2
	}()
	select {
	case l2 := <-got:
		if l2 == nil {
			t.Fatal("waiter failed to get recycled permit")
		}
		if l2.HolderID == l.HolderID {
			t.Fatal("recycled grant reused holder identity")
		}
		// old lease release must be a harmless idempotent no-op
		if err := insts[1].Release(ctx, l); err != nil {
			t.Fatalf("release of expired lease should be no-op: %v", err)
		}
		if err := insts[2].Release(ctx, l2); err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("expired lease was not recycled")
	}
}

func TestEndToEndFencingPreemption(t *testing.T) {
	_, insts := newCluster(t, 3, 5*time.Second)
	defer func() {
		for _, in := range insts {
			in.Close()
		}
	}()
	ctx := context.Background()
	if err := insts[0].EnsureSemaphore(ctx, "crit", 1, Priority); err != nil {
		t.Fatal(err)
	}
	incumbent := waitAcquire(t, insts[1], "crit", AcquireOptions{Priority: 1})
	// High-priority waiter queues.
	highCh := make(chan *Lease, 1)
	go func() {
		c, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		l, err := insts[2].Acquire(c, "crit", AcquireOptions{Priority: 100})
		if err != nil {
			highCh <- nil
			return
		}
		highCh <- l
	}()
	time.Sleep(80 * time.Millisecond)
	// Preempt the incumbent through the coordinating node.
	if err := insts[0].Preempt(ctx, "crit", []string{incumbent.HolderID}); err != nil {
		t.Fatal(err)
	}
	var high *Lease
	select {
	case high = <-highCh:
		if high == nil {
			t.Fatal("high priority not granted after preempt")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("preempt did not wake high-priority waiter")
	}
	// Old incumbent token is invalid: release rejected as no-op, renew fails.
	if err := insts[1].Release(ctx, incumbent); err != nil {
		t.Fatalf("release of preempted holder should be no-op: %v", err)
	}
	rc, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if _, err := insts[1].Renew(rc, incumbent, time.Second); err == nil {
		t.Fatal("renew with preempted token unexpectedly succeeded")
	}
	// High holder operates normally, then releases; permit returns to pool.
	if err := insts[2].Release(ctx, high); err != nil {
		t.Fatal(err)
	}
	// Next normal acquire succeeds.
	next := waitAcquire(t, insts[1], "crit", AcquireOptions{})
	if next.Fence <= high.Fence {
		t.Fatalf("fence not monotonic: %d <= %d", next.Fence, high.Fence)
	}
	_ = insts[1].Release(ctx, next)
}

func TestCrashRecoveryRestoresHoldersAndQueue(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	cfgs := map[string]string{"nodeA": dirA, "nodeB": dirB}
	build := func(b *bus) map[string]*Instance {
		out := map[string]*Instance{}
		for _, id := range []string{"nodeA", "nodeB"} {
			in, err := NewInstance(Config{
				InstanceID:      id,
				DataDir:         cfgs[id],
				DefaultLeaseTTL: 10 * time.Second,
				TickInterval:    10 * time.Millisecond,
			}, b)
			if err != nil {
				t.Fatal(err)
			}
			out[id] = in
		}
		return out
	}
	bus1 := NewBus()
	cl := build(bus1)
	ctx := context.Background()
	name := ""
	for _, cand := range []string{"r1", "r2", "r3", "r4", "r5", "r6"} {
		if cl["nodeA"].isOwner(cand) {
			name = cand
			break
		}
	}
	if name == "" {
		t.Fatal("no test name owned by nodeA")
	}
	if err := cl["nodeA"].EnsureSemaphore(ctx, name, 1, FIFO); err != nil {
		t.Fatal(err)
	}
	l := waitAcquire(t, cl["nodeB"], name, AcquireOptions{})
	// CRASH both instances (no graceful wait cancel).
	for _, in := range cl {
		in.Close()
	}

	// Restart from the same data dirs on a new bus.
	bus2 := NewBus()
	cl2 := build(bus2)
	defer func() {
		for _, in := range cl2 {
			in.Close()
		}
	}()
	snap, ok := cl2["nodeA"].engine.snapshot(name)
	if !ok || snap.usedCount() != 1 || len(snap.Holders) != 1 {
		t.Fatalf("holder not recovered: ok=%v snap=%+v", ok, snap)
	}

	// A new waiter queues against the recovered holder.
	ch := make(chan *Lease, 1)
	go func() {
		c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		l3, err := cl2["nodeB"].Acquire(c, name, AcquireOptions{})
		if err != nil {
			ch <- nil
			return
		}
		ch <- l3
	}()
	time.Sleep(100 * time.Millisecond)
	// Holder (also restarted) releases using its durable token: exactly one
	// permit is returned and the waiter is granted (no double grant, no loss).
	if err := cl2["nodeB"].Release(ctx, l); err != nil {
		t.Fatalf("recovered release: %v", err)
	}
	select {
	case l3 := <-ch:
		if l3 == nil {
			t.Fatal("waiter not woken after recovered release")
		}
		if l3.HolderID == l.HolderID {
			t.Fatal("reused holder identity")
		}
		if err := cl2["nodeB"].Release(ctx, l3); err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("permit not reusable after crash recovery")
	}
	// Duplicate release of the recovered holder stays idempotent.
	if err := cl2["nodeB"].Release(ctx, l); err != nil {
		t.Fatalf("duplicate recovered release: %v", err)
	}
	snap, _ = cl2["nodeA"].engine.snapshot(name)
	if snap.usedCount() != 0 || len(snap.Holders) != 0 || len(snap.Waiters) != 0 {
		t.Fatalf("state not clean after recovery cycle: %+v", snap)
	}
}

func TestRenewRotatesTokenAcrossInstances(t *testing.T) {
	_, insts := newCluster(t, 3, 5*time.Second)
	defer func() {
		for _, in := range insts {
			in.Close()
		}
	}()
	ctx := context.Background()
	_ = insts[0].EnsureSemaphore(ctx, "ren", 1, FIFO)
	l := waitAcquire(t, insts[1], "ren", AcquireOptions{})
	old := l.Fence
	nl, err := insts[2].Renew(ctx, l, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if nl.Fence <= old {
		t.Fatal("token not rotated")
	}
	// old token release rejected
	if _, _, ok := tryStale(insts, l); !ok {
		t.Fatal("stale release unexpectedly allowed")
	}
	_ = insts[1].Release(ctx, nl)
}

func tryStale(insts []*Instance, old *Lease) (string, bool, bool) {
	owner := ownerInst(insts, old.Semaphore)
	if owner == nil {
		return "", false, false
	}
	_, _, err := owner.engine.release(old.Semaphore, old.HolderID, old.Fence,
		time.Now().UnixNano(), int64(time.Second), func(evs []Event) error { return nil })
	return fmt.Sprintf("%v", err), err == ErrStaleFencingToken, err != nil
}

var _ = sync.Once{}

func TestWaitTimeoutReleasesSlot(t *testing.T) {
	_, insts := newCluster(t, 3, 5*time.Second)
	defer func() {
		for _, in := range insts {
			in.Close()
		}
	}()
	ctx := context.Background()
	if err := insts[0].EnsureSemaphore(ctx, "tmo", 1, FIFO); err != nil {
		t.Fatal(err)
	}
	held := waitAcquire(t, insts[0], "tmo", AcquireOptions{})
	c, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	if _, err := insts[1].Acquire(c, "tmo", AcquireOptions{}); err == nil {
		t.Fatal("expected timeout error")
	}
	// After timeout the slot must be gone: releasing the holder lets a fresh
	// acquire through immediately (not blocked behind the timed-out waiter).
	ch := make(chan *Lease, 1)
	go func() {
		ctx2, cancel2 := context.WithTimeout(ctx, 3*time.Second)
		defer cancel2()
		l, err := insts[2].Acquire(ctx2, "tmo", AcquireOptions{})
		ch <- l
		_ = err
	}()
	time.Sleep(80 * time.Millisecond)
	if err := insts[0].Release(ctx, held); err != nil {
		t.Fatal(err)
	}
	select {
	case l := <-ch:
		if l == nil {
			t.Fatal("fresh acquire failed after waiter timeout")
		}
		if err := insts[2].Release(ctx, l); err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed-out waiter slot leaked and blocked the queue")
	}
}

func TestCancelGrantRaceNoLeak(t *testing.T) {
	_, insts := newCluster(t, 3, 5*time.Second)
	defer func() {
		for _, in := range insts {
			in.Close()
		}
	}()
	ctx := context.Background()
	if err := insts[0].EnsureSemaphore(ctx, "race", 1, FIFO); err != nil {
		t.Fatal(err)
	}
	// Fill the only permit, then hammer canceled waits and releases.
	held := waitAcquire(t, insts[1], "race", AcquireOptions{})
	var wg sync.WaitGroup
	for round := 0; round < 30; round++ {
		cctx, cancel := context.WithCancel(ctx)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = insts[2].Acquire(cctx, "race", AcquireOptions{})
		}()
		// cancel quickly, racing with any grant delivery
		if round%2 == 0 {
			cancel()
		} else {
			go func() { time.Sleep(time.Duration(round%3) * time.Millisecond); cancel() }()
		}
	}
	// Some waiters may actually win grants; ensure eventual cleanup by
	// releasing the holder and then draining any granted leases via fresh
	// acquire/release cycles until stable.
	if err := insts[1].Release(ctx, held); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	wg.Wait()
	// Strong invariant: there must be exactly 1 permit total, never lost or
	// duplicated. Repeatedly acquire/release through all instances; the
	// semaphore must remain usable and accountably bounded.
	owner := ownerInst(insts, "race")
	if owner == nil {
		t.Fatal("no owner")
	}
	for k := 0; k < 5; k++ {
		l := waitAcquire(t, insts[k%3], "race", AcquireOptions{})
		snap, _ := owner.engine.snapshot("race")
		if snap.usedCount() != 1 {
			for _, h := range snap.Holders {
				t.Logf("LEAK holder=%s fence=%d waiter=%q", h.ID, h.Fence, h.WaiterID)
			}
			t.Fatalf("invariant broken: used=%d permits=%d holders=%d waiters=%d", snap.usedCount(), snap.Permits, len(snap.Holders), len(snap.Waiters))
		}
		// second acquire blocks
		busy := make(chan struct{}, 1)
		go func() {
			c, cancel := context.WithTimeout(ctx, 120*time.Millisecond)
			defer cancel()
			ll, err := insts[(k+1)%3].Acquire(c, "race", AcquireOptions{})
			if err == nil {
				_ = insts[(k+1)%3].Release(ctx, ll)
				busy <- struct{}{}
			}
		}()
		time.Sleep(180 * time.Millisecond)
		select {
		case <-busy:
			t.Fatal("permit oversold after cancel/grant race")
		default:
		}
		if err := insts[k%3].Release(ctx, l); err != nil {
			t.Fatal(err)
		}
	}
	snap, _ := owner.engine.snapshot("race")
	if snap.usedCount() != 0 || len(snap.Holders) != 0 || len(snap.Waiters) != 0 {
		t.Fatalf("leaked permits: used=%d holders=%d waiters=%d",
			snap.usedCount(), len(snap.Holders), len(snap.Waiters))
	}
}

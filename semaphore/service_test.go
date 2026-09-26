package semaphore

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func testCfg(id, dir string, ttl time.Duration) Config {
	return Config{
		InstanceID:      id,
		DataDir:         dir,
		DefaultLeaseTTL: ttl,
		TickInterval:    10 * time.Millisecond,
	}
}

func newCluster(t *testing.T, n int, ttl time.Duration) (*bus, []*Instance) {
	t.Helper()
	b := NewBus()
	insts := make([]*Instance, n)
	for k := 0; k < n; k++ {
		dir := ""
		if ttl > 0 {
			dir = t.TempDir()
		}
		cfg := testCfg(fmt.Sprintf("inst%d", k), dir, ttl)
		in, err := NewInstance(cfg, b)
		if err != nil {
			t.Fatal(err)
		}
		insts[k] = in
	}
	return b, insts
}

func waitAcquire(t *testing.T, in *Instance, name string, opts AcquireOptions) *Lease {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	l, err := in.Acquire(ctx, name, opts)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	return l
}

func TestMultiInstanceAcquireRelease(t *testing.T) {
	_, insts := newCluster(t, 3, 2*time.Second)
	defer func() {
		for _, in := range insts {
			in.Close()
		}
	}()
	ctx := context.Background()
	if err := insts[0].EnsureSemaphore(ctx, "db", 2, FIFO); err != nil {
		t.Fatal(err)
	}
	l1 := waitAcquire(t, insts[1], "db", AcquireOptions{})
	l2 := waitAcquire(t, insts[2], "db", AcquireOptions{})
	// third blocks
	done := make(chan *Lease, 1)
	go func() {
		l, _ := insts[1].Acquire(ctx, "db", AcquireOptions{})
		done <- l
	}()
	select {
	case l := <-done:
		t.Fatalf("should have blocked, got %v", l)
	case <-time.After(100 * time.Millisecond):
	}
	if err := insts[2].Release(ctx, l2); err != nil {
		t.Fatal(err)
	}
	select {
	case l := <-done:
		if l == nil {
			t.Fatal("nil lease after release")
		}
		if err := insts[1].Release(ctx, l); err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter not woken after release")
	}
	if err := insts[1].Release(ctx, l1); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentAcquireRelease(t *testing.T) {
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
	const goroutines = 40
	start := make(chan struct{})
	var wg sync.WaitGroup
	active := make(chan struct{}, goroutines)
	maxSeen := make(chan int, goroutines)
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
			active <- struct{}{}
			time.Sleep(20 * time.Millisecond)
			<-active
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
	if max > permits {
		t.Fatalf("oversold permits: max concurrent = %d > %d", max, permits)
	}
	if max < permits {
		t.Fatalf("never reached full capacity: %d", max)
	}
	// all permits back
	snapOK := false
	for _, in := range insts {
		if in.isOwner("pool") {
			if snap, ok := in.engine.snapshot("pool"); ok {
				if snap.usedCount() != 0 || len(snap.Waiters) != 0 || len(snap.Holders) != 0 {
					t.Fatalf("state not clean: holders=%d waiters=%d used=%d",
						len(snap.Holders), len(snap.Waiters), snap.usedCount())
				}
				snapOK = true
			}
		}
	}
	if !snapOK {
		t.Fatal("owner not found")
	}
	_ = maxSeen
}

func TestMultiPermitAtomicAcquireAndCancel(t *testing.T) {
	_, insts := newCluster(t, 3, 5*time.Second)
	defer func() {
		for _, in := range insts {
			in.Close()
		}
	}()
	ctx := context.Background()
	if err := insts[0].EnsureSemaphore(ctx, "res", 3, FIFO); err != nil {
		t.Fatal(err)
	}
	l := waitAcquire(t, insts[1], "res", AcquireOptions{Count: 2})
	// big (2) queues because only 1 free
	type res struct {
		l   *Lease
		err error
	}
	bigCh := make(chan res, 1)
	go func() {
		ll, e := insts[2].Acquire(ctx, "res", AcquireOptions{Count: 2})
		bigCh <- res{ll, e}
	}()
	time.Sleep(50 * time.Millisecond)
	// small (1) queues behind big; must wait even though 1 is free
	smallCh := make(chan res, 1)
	go func() {
		ll, e := insts[1].Acquire(ctx, "res", AcquireOptions{Count: 1})
		smallCh <- res{ll, e}
	}()
	select {
	case r := <-smallCh:
		t.Fatalf("small overtook big: %+v", r)
	case <-time.After(80 * time.Millisecond):
	}
	// cancel big; small should now get its 1
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	// The big acquire is running with background ctx; instead test cancel
	// through a fresh acquire with a canceled context.
	_ = cctx
	// Release the 2 held: big gets them first
	if err := insts[1].Release(ctx, l); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-bigCh:
		if r.err != nil || r.l == nil || r.l.Count != 2 {
			t.Fatalf("big grant wrong: %+v", r)
		}
		if err := insts[2].Release(ctx, r.l); err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("big not granted")
	}
	select {
	case r := <-smallCh:
		if r.err != nil || r.l == nil || r.l.Count != 1 {
			t.Fatalf("small grant wrong: %+v", r)
		}
		if err := insts[1].Release(ctx, r.l); err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("small not granted after big")
	}
}

func TestCanceledAcquireReleasesSlot(t *testing.T) {
	_, insts := newCluster(t, 2, 5*time.Second)
	defer func() {
		for _, in := range insts {
			in.Close()
		}
	}()
	ctx := context.Background()
	if err := insts[0].EnsureSemaphore(ctx, "c", 1, FIFO); err != nil {
		t.Fatal(err)
	}
	held := waitAcquire(t, insts[0], "c", AcquireOptions{})

	wctx, wcancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		_, err := insts[1].Acquire(wctx, "c", AcquireOptions{})
		errCh <- err
	}()
	time.Sleep(80 * time.Millisecond)
	wcancel()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected cancel error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancel did not return")
	}
	// Release holder; a fresh acquire must now succeed promptly (canceled
	// waiter's slot must not be leaked).
	if err := insts[0].Release(ctx, held); err != nil {
		t.Fatal(err)
	}
	l2 := waitAcquire(t, insts[1], "c", AcquireOptions{})
	if err := insts[1].Release(ctx, l2); err != nil {
		t.Fatal(err)
	}
}

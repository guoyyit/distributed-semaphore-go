package cluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/guoyyit/distributed-semaphore-go/config"
)

func waitAvailable(t *testing.T, nodes []*Instance, name string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			st, err := n.Stats(name)
			if err == nil && st.Available == want {
				return
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	for _, n := range nodes {
		if st, err := n.Stats(name); err == nil {
			t.Fatalf("available=%d want %d (node %s, grants=%d waiters=%d)",
				st.Available, want, n.id, len(st.Grants), len(st.Waiters))
		}
	}
}

func TestConcurrentAcquireReleaseNoOvergrant(t *testing.T) {
	_, nodes := testNodes(t, 3, 5*time.Second)
	waitLeader(t, nodes)
	cl := NewClient(nodes...).WithTTL(5 * time.Second)
	ctx := context.Background()
	const capacity = 5
	if err := cl.Init(ctx, "s", capacity); err != nil {
		t.Fatal(err)
	}

	const holders = 25
	var wg sync.WaitGroup
	var (
		overMu sync.Mutex
		over   []string
	)
	start := make(chan struct{})
	for i := 0; i < holders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			holder := fmt.Sprintf("h%d", i)
			<-start
			tok, _, rid, err := cl.Acquire(ctx, "s", holder, 1, 0, 5*time.Second)
			if err != nil {
				return
			}
			_ = rid
			// Observe total outstanding; capacity must never be exceeded.
			var maxOut int
			for _, n := range nodes {
				if st, err := n.Stats("s"); err == nil {
					out := capacity - st.Available
					if out > maxOut {
						maxOut = out
					}
				}
			}
			if maxOut > capacity {
				for _, n := range nodes {
					if st, err := n.Stats("s"); err == nil {
						total := 0
						for _, g := range st.Grants {
							total += g.Permits
						}
						if total > capacity {
							detail := fmt.Sprintf("node %s grants=%d waiters=%d avail=%d:",
								n.id, total, len(st.Waiters), st.Available)
							for _, g := range st.Grants {
								detail += fmt.Sprintf(" [%s p=%d tok=%d req=%s]", g.HolderID, g.Permits, g.Token, g.RequestID)
							}
							overMu.Lock()
							over = append(over, "overgrant: "+detail)
							overMu.Unlock()
						}
					}
				}
			}
			time.Sleep(10 * time.Millisecond)
			if err := cl.Release(ctx, "s", holder, tok); err != nil {
				overMu.Lock()
				over = append(over, "release: "+err.Error())
				overMu.Unlock()
			}
		}(i)
	}
	close(start)
	wg.Wait()
	for _, msg := range over {
		t.Fatal(msg)
	}
	waitAvailable(t, nodes, "s", capacity, 3*time.Second)
}

func TestLeaseExpiryClusterReclaims(t *testing.T) {
	_, nodes := testNodes(t, 3, 300*time.Millisecond)
	waitLeader(t, nodes)
	cl := NewClient(nodes...).WithTTL(200 * time.Millisecond)
	ctx := context.Background()
	if err := cl.Init(ctx, "s", 1); err != nil {
		t.Fatal(err)
	}
	tok, _, _, err := cl.Acquire(ctx, "s", "crashy", 1, 0, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// Holder "crashes": never renews and never releases. Lease must expire and
	// the permit must return to the pool automatically.
	deadline := time.Now().Add(3 * time.Second)
	var ok bool
	for time.Now().Before(deadline) {
		if _, _, _, err := cl.Acquire(ctx, "s", "next", 1, 0, 500*time.Millisecond); err == nil {
			ok = true
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if !ok {
		t.Fatal("permit was permanently lost after holder crash (lease expiry did not reclaim)")
	}
	waitAvailable(t, nodes, "s", 0, time.Second)
	// Old token from the crashed holder must be an idempotent no-op now.
	if err := cl.Release(ctx, "s", "crashy", tok); err != nil {
		t.Fatalf("late stale release returned error: %v", err)
	}
	waitAvailable(t, nodes, "s", 0, time.Second)
}

func TestFencingPreemptionStaleToken(t *testing.T) {
	_, nodes := testNodes(t, 3, 5*time.Second)
	waitLeader(t, nodes)
	cl := NewClient(nodes...).WithTTL(5 * time.Second)
	ctx := context.Background()
	if err := cl.Init(ctx, "s", 1); err != nil {
		t.Fatal(err)
	}
	tok1, _, _, err := cl.Acquire(ctx, "s", "h1", 1, 0, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	tok2, _, err := cl.Renew(ctx, "s", "h1", tok1)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if tok2 == tok1 {
		t.Fatal("renew must rotate token")
	}
	// Renew with the stale token must fail.
	if _, _, err := cl.Renew(ctx, "s", "h1", tok1); err == nil {
		t.Fatal("renew with stale token unexpectedly succeeded")
	}
	// Release with stale token must fail and leave permit held by h1.
	if err := cl.Release(ctx, "s", "h1", tok1); err == nil {
		t.Fatal("release with stale token unexpectedly succeeded")
	}
	waitAvailable(t, nodes, "s", 0, time.Second)
	// Current token releases cleanly.
	if err := cl.Release(ctx, "s", "h1", tok2); err != nil {
		t.Fatalf("release current token: %v", err)
	}
	waitAvailable(t, nodes, "s", 1, time.Second)
}

func TestMultiPermitAtomicAndCancel(t *testing.T) {
	_, nodes := testNodes(t, 3, 5*time.Second)
	waitLeader(t, nodes)
	cl := NewClient(nodes...).WithTTL(5 * time.Second)
	ctx := context.Background()
	if err := cl.Init(ctx, "s", 3); err != nil {
		t.Fatal(err)
	}
	// Take 2; one remains.
	tok, _, _, err := cl.Acquire(ctx, "s", "a", 2, 0, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// Request 3: must wait fully (no partial grant consuming the 1 leftover).
	bigID := newID()
	acquireErr := make(chan error, 1)
	go func() {
		_, _, _, e := cl.AcquireWithID(ctx, "s", "big", bigID, 3, 0, 3*time.Second)
		acquireErr <- e
	}()
	// The single free permit must remain free while big waits head-of-line.
	time.Sleep(200 * time.Millisecond)
	waitAvailable(t, nodes, "s", 1, time.Second)

	// Cancel big's queued request.
	if err := cl.Cancel(ctx, "s", bigID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	select {
	case e := <-acquireErr:
		if e == nil {
			t.Fatal("expected canceled acquire to return an error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancel did not unblock waiter")
	}
	// A 1-permit request can now be served (slot freed, leftover usable).
	if _, _, _, err := cl.Acquire(ctx, "s", "small", 1, 0, time.Second); err != nil {
		t.Fatalf("post-cancel acquire: %v", err)
	}
	waitAvailable(t, nodes, "s", 0, time.Second)
	_ = tok
}

func TestPriorityWaitersServedInOrder(t *testing.T) {
	_, nodes := testNodes(t, 3, 5*time.Second)
	waitLeader(t, nodes)
	cl := NewClient(nodes...).WithPolicy(config.QueuePriority).WithTTL(5 * time.Second)
	ctx := context.Background()
	if err := cl.Init(ctx, "s", 1); err != nil {
		t.Fatal(err)
	}
	holderTok, _, _, err := cl.Acquire(ctx, "s", "held", 1, 0, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	type res struct {
		holder string
		tok    uint64
		err    error
	}
	resCh := make(chan res, 3)
	acquire := func(holder string, prio int) {
		tok, _, _, e := cl.Acquire(ctx, "s", holder, 1, prio, 5*time.Second)
		resCh <- res{holder, tok, e}
	}
	go acquire("low", 1)
	go acquire("high", 10)
	go acquire("mid", 5)
	time.Sleep(300 * time.Millisecond) // let all three enqueue
	if err := cl.Release(ctx, "s", "held", holderTok); err != nil {
		t.Fatal(err)
	}
	var order []string
	for i := 0; i < 3; i++ {
		r := <-resCh
		if r.err != nil {
			t.Fatalf("holder %s: %v", r.holder, r.err)
		}
		order = append(order, r.holder)
		cl.Release(ctx, "s", r.holder, r.tok)
	}
	want := []string{"high", "mid", "low"}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("service order=%v want %v", order, want)
		}
	}
}

func TestWaitTimeoutFreesQueueSlot(t *testing.T) {
	_, nodes := testNodes(t, 3, 5*time.Second)
	waitLeader(t, nodes)
	cl := NewClient(nodes...).WithTTL(5 * time.Second)
	ctx := context.Background()
	if err := cl.Init(ctx, "s", 1); err != nil {
		t.Fatal(err)
	}
	holderTok, _, _, err := cl.Acquire(ctx, "s", "held", 1, 0, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// A waiter with a short queue timeout: it must give up and free its slot.
	if _, _, _, err := cl.Acquire(ctx, "s", "impatient", 1, 0, 300*time.Millisecond); err == nil {
		t.Fatal("expected wait timeout error")
	}
	// Slot must be gone from the queue promptly (reaper-driven).
	deadline := time.Now().Add(3 * time.Second)
	for {
		st, _ := nodes[0].Stats("s")
		if len(st.Waiters) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed-out waiter still queued: %+v", st.Waiters)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// The freed queue must not block the next waiter once permits are back.
	if err := cl.Release(ctx, "s", "held", holderTok); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := cl.Acquire(ctx, "s", "next", 1, 0, 2*time.Second); err != nil {
		t.Fatalf("acquire after wait-timeout: %v", err)
	}
}

func TestMultiInstanceInitiation(t *testing.T) {
	_, nodes := testNodes(t, 3, 5*time.Second)
	waitLeader(t, nodes)
	ctx := context.Background()
	// Each client knows all members but first-contacts a different node:
	// operations originate on every instance, then follow the leader.
	clients := make([]*Client, len(nodes))
	for i := range nodes {
		clients[i] = NewClient(nodes...).WithStartNode(i).WithTTL(5 * time.Second)
	}
	if err := clients[0].Init(ctx, "s", 3); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 30)
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := clients[i%len(clients)]
			holder := fmt.Sprintf("h%d", i)
			tok, _, _, err := c.Acquire(ctx, "s", holder, 1, 0, 5*time.Second)
			if err != nil {
				errs <- err
				return
			}
			time.Sleep(5 * time.Millisecond)
			if err := c.Release(ctx, "s", holder, tok); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	waitAvailable(t, nodes, "s", 3, 3*time.Second)
}

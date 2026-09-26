package cluster

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/guoyyit/distributed-semaphore-go/config"
	"github.com/guoyyit/distributed-semaphore-go/semaphore"
)

// startNode creates and starts one instance rooted at a stable dir.
func startNode(t *testing.T, bus *Bus, dir, id string, ttl time.Duration) *Instance {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.InstanceID = id
	cfg.DataDir = filepath.Join(dir, id)
	cfg.LeaseTTL = ttl
	cfg.ElectionTimeout = 250 * time.Millisecond
	cfg.HeartbeatInterval = 40 * time.Millisecond
	n, err := NewInstance(cfg, bus)
	if err != nil {
		t.Fatalf("new instance %s: %v", id, err)
	}
	if err := n.Start(context.Background()); err != nil {
		t.Fatalf("start %s: %v", id, err)
	}
	return n
}

func TestCrashRecoveryRestoresGrantsAndQueue(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// --- Phase 1: run a 3-node cluster, grant 2 permits, queue 1 waiter. ---
	bus := NewBus()
	var nodes []*Instance
	for i := 0; i < 3; i++ {
		nodes = append(nodes, startNode(t, bus, dir, nodeName(i), 30*time.Second))
	}
	waitLeader(t, nodes)
	cl := NewClient(nodes...).WithTTL(30 * time.Second)
	if err := cl.Init(ctx, "s", 3); err != nil {
		t.Fatal(err)
	}
	tok1, _, _, err := cl.Acquire(ctx, "s", "h1", 1, 0, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	tok2, _, _, err := cl.Acquire(ctx, "s", "h2", 1, 0, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// A waiter needing 2 permits: only 1 free -> queued.
	waiterID := newID()
	waiterDone := make(chan error, 1)
	go func() {
		_, _, _, e := cl.AcquireWithID(ctx, "s", "w1", waiterID, 2, 0, 10*time.Second)
		waiterDone <- e
	}()
	// Wait until the waiter is durably queued.
	deadline := time.Now().Add(3 * time.Second)
	for {
		st, err := nodes[0].Stats("s")
		if err == nil && len(st.Waiters) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("waiter never enqueued")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// --- Phase 2: crash the whole cluster (no graceful snapshot). ---
	for _, n := range nodes {
		_ = n.Stop()
	}

	// --- Phase 3: restart from durable state only. ---
	bus2 := NewBus()
	var nodes2 []*Instance
	for i := 0; i < 3; i++ {
		nodes2 = append(nodes2, startNode(t, bus2, dir, nodeName(i), 30*time.Second))
	}
	defer func() {
		for _, n := range nodes2 {
			_ = n.Stop()
		}
	}()
	leader := waitLeader(t, nodes2)

	// Granted permits must be restored exactly once (no double-grant, no loss).
	st, err := leader.Stats("s")
	if err != nil {
		t.Fatal(err)
	}
	if st.Available != 1 || len(st.Grants) != 2 {
		t.Fatalf("recovered grants wrong: avail=%d grants=%d", st.Available, len(st.Grants))
	}
	tokens := map[uint64]bool{}
	holders := map[string]bool{}
	for _, g := range st.Grants {
		if tokens[g.Token] {
			t.Fatalf("duplicate fencing token after recovery: %d", g.Token)
		}
		tokens[g.Token] = true
		holders[g.HolderID] = true
	}
	if !holders["h1"] || !holders["h2"] {
		t.Fatalf("lost holders after recovery: %v", holders)
	}
	// Waiting queue must be restored.
	if len(st.Waiters) != 1 || st.Waiters[0].RequestID != waiterID {
		t.Fatalf("waiter queue not recovered: %+v", st.Waiters)
	}

	// While permits stay at 1, the 2-permit waiter must remain queued and must
	// not be duplicated by repeated planning (observed, not re-submitted).
	st, _ = leader.Stats("s")
	if len(st.Grants) != 2 || st.Available != 1 || len(st.Waiters) != 1 {
		t.Fatalf("post-recovery state wrong: avail=%d grants=%d waiters=%d",
			st.Available, len(st.Grants), len(st.Waiters))
	}
	cl2 := NewClient(nodes2...).WithTTL(30 * time.Second)

	// Releasing both original holders frees exactly 2 permits and the queued
	// 2-permit waiter is then granted atomically.
	if err := cl2.Release(ctx, "s", "h1", tok1); err != nil {
		t.Fatalf("release h1: %v", err)
	}
	if err := cl2.Release(ctx, "s", "h2", tok2); err != nil {
		t.Fatalf("release h2: %v", err)
	}
	select {
	case err := <-waiterDone:
		// The pre-crash waiter client was attached to dead nodes; it may have
		// errored out. The grant itself is verified via stats below.
		_ = err
	case <-time.After(500 * time.Millisecond):
	}
	// Re-attach the waiter to collect its grant.
	tok3, _, _, err := cl2.AcquireWithID(ctx, "s", "w1", waiterID, 2, 0, 3*time.Second)
	if err != nil {
		t.Fatalf("waiter re-attach after recovery: %v", err)
	}
	if tok3 == 0 {
		t.Fatal("waiter grant lost after recovery")
	}
	st, _ = leader.Stats("s")
	if st.Available != 1 || len(st.Grants) != 1 {
		t.Fatalf("final state wrong: avail=%d grants=%d", st.Available, len(st.Grants))
	}
	// Permit conservation: available + outstanding == capacity.
	total := st.Available
	for _, g := range st.Grants {
		total += g.Permits
	}
	if total != 3 {
		t.Fatalf("permit leak: total=%d want 3", total)
	}
}

func TestLeaderFailoverContinuesService(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	bus := NewBus()
	var nodes []*Instance
	for i := 0; i < 3; i++ {
		nodes = append(nodes, startNode(t, bus, dir, nodeName(i), 30*time.Second))
	}
	leader := waitLeader(t, nodes)
	cl := NewClient(nodes...).WithTTL(30 * time.Second)
	if err := cl.Init(ctx, "s", 2); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := cl.Acquire(ctx, "s", "h1", 1, 0, 2*time.Second); err != nil {
		t.Fatal(err)
	}

	// Kill the leader; the remaining nodes must elect a new one and keep
	// serving without losing or duplicating the outstanding grant.
	_ = leader.Stop()
	time.Sleep(100 * time.Millisecond)
	var survivors []*Instance
	for _, n := range nodes {
		if n != leader {
			survivors = append(survivors, n)
		}
	}
	defer func() {
		for _, n := range survivors {
			_ = n.Stop()
		}
	}()
	newLeader := waitLeader(t, survivors)
	if newLeader == leader {
		t.Fatal("same leader after stop")
	}
	// The new leader must commit its inauguration no-op before entries from
	// the prior term are applied; poll until the grant is visible.
	var st semaphore.Stats
	deadline := time.Now().Add(3 * time.Second)
	for {
		st, _ = newLeader.Stats("s")
		if len(st.Grants) == 1 && st.Available == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("grant lost across failover: %+v", st)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// New acquires still work on the new leader.
	if _, _, _, err := cl.Acquire(ctx, "s", "h2", 1, 0, 3*time.Second); err != nil {
		t.Fatalf("acquire after failover: %v", err)
	}
	waitAvailable(t, survivors, "s", 0, 3*time.Second)
	st, _ = newLeader.Stats("s")
	if len(st.Grants) != 2 {
		t.Fatalf("post-failover grants wrong: %+v", st)
	}
	fmt.Println("failover ok")
}

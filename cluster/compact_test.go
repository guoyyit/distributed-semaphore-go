package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/guoyyit/distributed-semaphore-go/config"
)

func TestCompactAndRestartFromSnapshot(t *testing.T) {
	dir := t.TempDir()
	id := "solo"
	cfg := config.DefaultConfig()
	cfg.InstanceID = id
	cfg.DataDir = dir
	cfg.LeaseTTL = 30 * time.Second
	bus := NewBus()
	n, err := NewInstance(cfg, bus)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitLeader(t, []*Instance{n})
	cl := NewClient(n).WithTTL(30 * time.Second)
	ctx := context.Background()
	if err := cl.Init(ctx, "s", 2); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := cl.Acquire(ctx, "s", "h1", 2, 0, time.Second); err != nil {
		t.Fatal(err)
	}
	// A waiter must survive the snapshot too.
	waiterID := newID()
	go cl.AcquireWithID(ctx, "s", "w1", waiterID, 1, 0, 5*time.Second)
	deadline := time.Now().Add(3 * time.Second)
	for {
		st, _ := n.Stats("s")
		if len(st.Waiters) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("waiter not enqueued")
		}
		time.Sleep(15 * time.Millisecond)
	}

	// Compact: everything committed becomes a snapshot; WAL keeps only suffix.
	n.mu.Lock()
	if err := n.compactLocked(); err != nil {
		n.mu.Unlock()
		t.Fatalf("compact: %v", err)
	}
	preStop, _ := n.machine.Stats("s")
	n.mu.Unlock()
	if err := n.Stop(); err != nil {
		t.Fatal(err)
	}

	// Restart: state must come from the snapshot (+suffix), grants and queue.
	bus2 := NewBus()
	n2, err := NewInstance(cfg, bus2)
	if err != nil {
		t.Fatal(err)
	}
	if err := n2.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer n2.Stop()
	st, err := n2.Stats("s")
	if err != nil {
		t.Fatal(err)
	}
	if st.Available != preStop.Available || len(st.Grants) != len(preStop.Grants) ||
		len(st.Waiters) != 1 || st.Waiters[0].RequestID != waiterID {
		t.Fatalf("snapshot restore mismatch: %+v (want avail=%d grants=%d)",
			st, preStop.Available, len(preStop.Grants))
	}
	// Permit conservation after snapshot-based recovery.
	total := st.Available
	for _, g := range st.Grants {
		total += g.Permits
	}
	if total != st.Capacity {
		t.Fatalf("permit leak after snapshot recovery: %d != %d", total, st.Capacity)
	}
}

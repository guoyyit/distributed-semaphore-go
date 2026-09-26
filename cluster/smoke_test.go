package cluster

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/guoyyit/distributed-semaphore-go/config"
)

func testNodes(t *testing.T, n int, ttl time.Duration) (*Bus, []*Instance) {
	t.Helper()
	bus := NewBus()
	var nodes []*Instance
	for i := 0; i < n; i++ {
		cfg := config.DefaultConfig()
		cfg.InstanceID = nodeName(i)
		cfg.DataDir = filepath.Join(t.TempDir(), cfg.InstanceID)
		cfg.LeaseTTL = ttl
		cfg.ElectionTimeout = 250 * time.Millisecond
		cfg.HeartbeatInterval = 40 * time.Millisecond
		node, err := NewInstance(cfg, bus)
		if err != nil {
			t.Fatalf("new node: %v", err)
		}
		if err := node.Start(context.Background()); err != nil {
			t.Fatalf("start: %v", err)
		}
		nodes = append(nodes, node)
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			_ = n.Stop()
		}
	})
	return bus, nodes
}

func nodeName(i int) string {
	return string(rune('n' + i))
}

func waitLeader(t *testing.T, nodes []*Instance) *Instance {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.IsLeader() {
				return n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader elected")
	return nil
}

func TestSmokeAcquireRelease(t *testing.T) {
	_, nodes := testNodes(t, 3, 2*time.Second)
	leader := waitLeader(t, nodes)
	cl := NewClient(nodes...).WithTTL(time.Second)
	ctx := context.Background()
	if err := cl.Init(ctx, "db", 2); err != nil {
		t.Fatalf("init: %v", err)
	}
	tok, exp, rid, err := cl.Acquire(ctx, "db", "h1", 1, 0, time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if tok == 0 || exp.IsZero() || rid == "" {
		t.Fatalf("bad grant: tok=%d exp=%v rid=%q", tok, exp, rid)
	}
	st, err := leader.Stats("db")
	if err != nil {
		t.Fatal(err)
	}
	if st.Available != 1 {
		t.Fatalf("available=%d want 1", st.Available)
	}
	if err := cl.Release(ctx, "db", "h1", tok); err != nil {
		t.Fatalf("release: %v", err)
	}
	st, _ = leader.Stats("db")
	if st.Available != 2 {
		t.Fatalf("available after release=%d want 2", st.Available)
	}
}

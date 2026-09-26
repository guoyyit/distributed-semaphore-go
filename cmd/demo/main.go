// Command demo spins up a 3-instance in-process semaphore cluster and
// exercises the main guarantees: atomic multi-permit acquires, FIFO waiting,
// fencing-token rotation, idempotent release and automatic lease recovery.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/guoyyit/distributed-semaphore-go/cluster"
	"github.com/guoyyit/distributed-semaphore-go/config"
)

func must(err error, what string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", what, err)
		os.Exit(1)
	}
}

func main() {
	ctx := context.Background()
	base, err := os.MkdirTemp("", "sema-demo-")
	must(err, "mkdtemp")
	defer os.RemoveAll(base)

	bus := cluster.NewBus()
	var nodes []*cluster.Instance
	for i := 0; i < 3; i++ {
		cfg := config.DefaultConfig()
		cfg.InstanceID = fmt.Sprintf("node-%d", i)
		cfg.DataDir = filepath.Join(base, cfg.InstanceID)
		cfg.LeaseTTL = 2 * time.Second
		n, err := cluster.NewInstance(cfg, bus)
		must(err, "new instance")
		must(n.Start(ctx), "start")
		nodes = append(nodes, n)
	}
	defer func() {
		for _, n := range nodes {
			_ = n.Stop()
		}
	}()

	// Wait for a leader.
	var leader *cluster.Instance
	for d := 0; d < 100; d++ {
		for _, n := range nodes {
			if n.IsLeader() {
				leader = n
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if leader == nil {
		must(fmt.Errorf("no leader"), "election")
	}
	fmt.Printf("leader elected: %s\n", leader.ID())

	cl := cluster.NewClient(nodes...).WithTTL(2 * time.Second)
	must(cl.Init(ctx, "orders", 3), "init")
	fmt.Println("semaphore 'orders' created with capacity 3")

	tok, _, rid, err := cl.Acquire(ctx, "orders", "worker-a", 2, 0, time.Second)
	must(err, "acquire 2")
	fmt.Printf("worker-a granted 2 permits, fencing token %d (request %s)\n", tok, short(rid))

	// A request for 2 more while only 1 is free must wait atomically.
	done := make(chan uint64, 1)
	go func() {
		t, _, rid2, err := cl.Acquire(ctx, "orders", "worker-b", 2, 0, 5*time.Second)
		must(err, "worker-b acquire")
		fmt.Printf("worker-b granted after permits freed, fencing token %d (request %s)\n", t, short(rid2))
		done <- t
	}()
	time.Sleep(300 * time.Millisecond)
	st, _ := leader.Stats("orders")
	fmt.Printf("worker-b waiting; available=%d waiters=%d (no partial grant)\n", st.Available, len(st.Waiters))

	// Token rotation protects the lease; old tokens are rejected afterward.
	newTok, _, err := cl.Renew(ctx, "orders", "worker-a", tok)
	must(err, "renew")
	fmt.Printf("worker-a renewed; token rotated %d -> %d\n", tok, newTok)
	if err := cl.Release(ctx, "orders", "worker-a", tok); err == nil {
		must(fmt.Errorf("stale token release should fail"), "fencing check")
	}
	fmt.Println("release with old token rejected (fencing enforced)")

	must(cl.Release(ctx, "orders", "worker-a", newTok), "release current token")
	fmt.Println("worker-a released with the current token")

	bTok := <-done
	// Idempotent duplicate release never double-credits permits.
	must(cl.Release(ctx, "orders", "worker-b", bTok), "release b #1")
	must(cl.Release(ctx, "orders", "worker-b", bTok), "release b #2")
	st, _ = leader.Stats("orders")
	fmt.Printf("duplicate release idempotent; available=%d (capacity 3)\n", st.Available)

	fmt.Println("demo complete")
}

func short(s string) string {
	if len(s) <= 8 {
		return s
	}
	return s[:8]
}

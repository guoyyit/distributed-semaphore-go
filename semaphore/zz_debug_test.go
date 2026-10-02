package semaphore

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestDebugTimeoutLeak7(t *testing.T) {
	for run := 0; run < 30; run++ {
		_, insts := newCluster(t, 4, 5*time.Second)
		const permits = 5
		ctx := context.Background()
		if err := insts[0].EnsureSemaphore(ctx, "pool", permits, FIFO); err != nil {
			t.Fatal(err)
		}
		owner := ownerInst(insts, "pool")
		fmt.Printf("run %d owner=%s\n", run, owner.cfg.InstanceID)
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
					rerr := in.Release(ctx, l)
					if rerr != nil {
						fmt.Printf("run %d RELEASE FAILED holder=%s err=%v\n", run, l.HolderID, rerr)
					}
				}
			}(g)
		}
		wg.Wait()
		snap, _ := owner.engine.snapshot("pool")
		if snap.usedCount() != 0 || len(snap.Holders) != 0 || len(snap.Waiters) != 0 {
			fmt.Printf("run %d LEAK: used=%d holders=%d waiters=%d\n", run, snap.usedCount(), len(snap.Holders), len(snap.Waiters))
			for _, h := range snap.Holders {
				fmt.Printf("  holder=%s waiter=%q fence=%d\n", h.ID, h.WaiterID, h.Fence)
			}
		}
		for _, in := range insts {
			in.Close()
		}
	}
}

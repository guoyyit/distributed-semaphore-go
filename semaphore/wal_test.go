package semaphore

import (
	"context"
	"testing"
	"time"
)

func TestSnapshotCompactionAndRecovery(t *testing.T) {
	dir := t.TempDir()
	bus := NewBus()
	cfg := Config{
		InstanceID:      "solo",
		DataDir:         dir,
		DefaultLeaseTTL: time.Hour,
		TickInterval:    10 * time.Millisecond,
		SnapshotAfter:   3,
	}
	in, err := NewInstance(cfg, bus)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := in.EnsureSemaphore(ctx, "sem", 2, FIFO); err != nil {
		t.Fatal(err)
	}
	// Generate > SnapshotAfter events (acquire/release cycles).
	for k := 0; k < 10; k++ {
		l, err := in.Acquire(ctx, "sem", AcquireOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := in.Release(ctx, l); err != nil {
			t.Fatal(err)
		}
	}
	// Hold two permits so snapshot contains live holders.
	h1 := waitAcquire(t, in, "sem", AcquireOptions{})
	h2 := waitAcquire(t, in, "sem", AcquireOptions{})
	if err := in.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen and verify recovered state from snapshot + tail events.
	bus2 := NewBus()
	in2, err := NewInstance(cfg, bus2)
	if err != nil {
		t.Fatal(err)
	}
	defer in2.Close()
	snap, ok := in2.engine.snapshot("sem")
	if !ok {
		t.Fatal("semaphore not recovered")
	}
	if snap.usedCount() != 2 {
		t.Fatalf("recovered used=%d want 2", snap.usedCount())
	}
	if len(snap.Holders) != 2 {
		t.Fatalf("recovered holders=%d want 2", len(snap.Holders))
	}
	// Permits are still constrained: another acquire queues.
	blocked := make(chan error, 1)
	go func() {
		c, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		_, err := in2.Acquire(c, "sem", AcquireOptions{})
		blocked <- err
	}()
	select {
	case err := <-blocked:
		if err == nil {
			t.Fatal("expected blocking after recovery")
		}
	case <-time.After(600 * time.Millisecond):
		t.Fatal("acquire unexpectedly succeeded: permits oversold after recovery")
	}
	// Release recovered holder: permit reusable.
	if err := in2.Release(ctx, h1); err != nil {
		t.Fatal(err)
	}
	l, err := func() (*Lease, error) {
		c, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		return in2.Acquire(c, "sem", AcquireOptions{})
	}()
	if err != nil {
		t.Fatal(err)
	}
	_ = in2.Release(ctx, l)
	_ = in2.Release(ctx, h2)
}

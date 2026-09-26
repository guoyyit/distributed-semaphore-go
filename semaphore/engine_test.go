package semaphore

import (
	"testing"
	"time"
)

func noopSave(evs []Event) error { return nil }

func TestAcquireReleaseAndReuse(t *testing.T) {
	e := newEngine()
	now := time.Unix(1, 0).UnixNano()
	ttl := int64(time.Second)
	if _, err := e.create("s", 2, FIFO, now, noopSave); err != nil {
		t.Fatal(err)
	}
	evs, err := e.acquire("s", "h1", "w1", 1, 0, 0, now, ttl, noopSave)
	if err != nil {
		t.Fatal(err)
	}
	ls := leasesFromEvents(evs)
	if len(ls) != 1 || ls[0].Fence != 1 {
		t.Fatalf("unexpected lease: %+v", ls)
	}
	// second permit available
	if _, err := e.acquire("s", "h2", "w2", 1, 0, 0, now, ttl, noopSave); err != nil {
		t.Fatal(err)
	}
	// third must queue
	evs, err = e.acquire("s", "h3", "w3", 1, 0, 0, now, ttl, noopSave)
	if err != nil || len(leasesFromEvents(evs)) != 0 {
		t.Fatalf("expected queue, got evs=%v err=%v", evs, err)
	}
	snap, _ := e.snapshot("s")
	if snap.usedCount() != 2 || len(snap.Waiters) != 1 {
		t.Fatalf("unexpected snap used=%d waiters=%d", snap.usedCount(), len(snap.Waiters))
	}
	// release h1 -> w3 granted in same batch
	evs, _, err = e.release("s", "h1", 1, now+10, ttl, noopSave)
	if err != nil {
		t.Fatal(err)
	}
	ls = leasesFromEvents(evs)
	if len(ls) != 1 || ls[0].HolderID != "h3" {
		t.Fatalf("expected h3 granted, got %+v", ls)
	}
	snap, _ = e.snapshot("s")
	if snap.usedCount() != 2 {
		t.Fatalf("used=%d want 2", snap.usedCount())
	}
}

func TestReleaseIdempotent(t *testing.T) {
	e := newEngine()
	now := int64(0)
	ttl := int64(100)
	_, _ = e.create("s", 1, FIFO, now, noopSave)
	evs, _ := e.acquire("s", "h1", "w1", 1, 0, 0, now, ttl, noopSave)
	fence := leasesFromEvents(evs)[0].Fence
	if _, idem, err := e.release("s", "h1", fence, now+1, ttl, noopSave); err != nil || !idem {
		t.Fatalf("first release failed: %v %v", idem, err)
	}
	// duplicate release: no events, no wakeup, no error
	evs2, idem, err := e.release("s", "h1", fence, now+2, ttl, noopSave)
	if err != nil || !idem || len(evs2) != 0 {
		t.Fatalf("idempotent release produced events=%v err=%v", evs2, err)
	}
	snap, _ := e.snapshot("s")
	if snap.usedCount() != 0 {
		t.Fatalf("used=%d", snap.usedCount())
	}
}

func TestStaleFencingToken(t *testing.T) {
	e := newEngine()
	now := int64(0)
	ttl := int64(1000)
	_, _ = e.create("s", 1, FIFO, now, noopSave)
	evs, _ := e.acquire("s", "h1", "w1", 1, 0, 0, now, ttl, noopSave)
	old := leasesFromEvents(evs)[0].Fence
	ev, err := e.renew("s", "h1", old, now+500, now+10, noopSave)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Fence <= old {
		t.Fatalf("fence did not rotate: %d -> %d", old, ev.Fence)
	}
	// old token renew rejected
	if _, err := e.renew("s", "h1", old, now+600, now+20, noopSave); err != ErrStaleFencingToken {
		t.Fatalf("want stale, got %v", err)
	}
	// old token release rejected and permits stay held
	if _, _, err := e.release("s", "h1", old, now+30, ttl, noopSave); err != ErrStaleFencingToken {
		t.Fatalf("want stale release, got %v", err)
	}
	snap, _ := e.snapshot("s")
	if snap.usedCount() != 1 {
		t.Fatalf("permits changed despite stale release: %d", snap.usedCount())
	}
	// new token release works
	if _, _, err := e.release("s", "h1", ev.Fence, now+40, ttl, noopSave); err != nil {
		t.Fatal(err)
	}
}

func TestFIFOStrategy(t *testing.T) {
	e := newEngine()
	now, ttl := int64(0), int64(1000)
	_, _ = e.create("s", 1, FIFO, now, noopSave)
	_, _ = e.acquire("s", "held", "w0", 1, 0, 0, now, ttl, noopSave)
	// w-low arrives first but lower priority, w-high second
	_, _ = e.acquire("s", "low", "wL", 1, 1, 0, now, ttl, noopSave)
	_, _ = e.acquire("s", "high", "wH", 1, 10, 0, now, ttl, noopSave)
	evs, _, _ := e.release("s", "held", 1, now+1, ttl, noopSave)
	ls := leasesFromEvents(evs)
	if len(ls) != 1 || ls[0].HolderID != "low" {
		t.Fatalf("FIFO violated: %+v", ls)
	}
}

func TestPriorityStrategy(t *testing.T) {
	e := newEngine()
	now, ttl := int64(0), int64(1000)
	_, _ = e.create("s", 1, Priority, now, noopSave)
	_, _ = e.acquire("s", "held", "w0", 1, 0, 0, now, ttl, noopSave)
	_, _ = e.acquire("s", "low", "wL", 1, 1, 0, now, ttl, noopSave)
	_, _ = e.acquire("s", "high", "wH", 1, 10, 0, now, ttl, noopSave)
	evs, _, _ := e.release("s", "held", 1, now+1, ttl, noopSave)
	ls := leasesFromEvents(evs)
	if len(ls) != 1 || ls[0].HolderID != "high" {
		t.Fatalf("priority violated: %+v", ls)
	}
	// releasing high then admits low
	evs, _, _ = e.release("s", "high", ls[0].Fence, now+2, ttl, noopSave)
	if l := leasesFromEvents(evs); len(l) != 1 || l[0].HolderID != "low" {
		t.Fatalf("low not served next: %+v", l)
	}
}

func TestMultiPermitAtomic(t *testing.T) {
	e := newEngine()
	now, ttl := int64(0), int64(1000)
	_, _ = e.create("s", 3, FIFO, now, noopSave)
	_, _ = e.acquire("s", "h", "w0", 2, 0, 0, now, ttl, noopSave) // used 2
	// big request (2) cannot fit: queues and must not split
	_, _ = e.acquire("s", "big", "wB", 2, 0, 0, now, ttl, noopSave)
	// small request behind it must NOT overtake under strict FIFO
	evs, _ := e.acquire("s", "small", "wS", 1, 0, 0, now, ttl, noopSave)
	if len(leasesFromEvents(evs)) != 0 {
		t.Fatal("small overtook big")
	}
	// release 2 -> big gets all; small still waits
	evs, _, _ = e.release("s", "h", 1, now+1, ttl, noopSave)
	ls := leasesFromEvents(evs)
	// big is served first with its full 2 permits; small (1) also fits in
	// the same atomic batch, and never overtakes big.
	if len(ls) < 1 || ls[0].HolderID != "big" || ls[0].Count != 2 {
		t.Fatalf("expected atomic big grant first: %+v", ls)
	}
}

func TestCancelReleasesSlot(t *testing.T) {
	e := newEngine()
	now, ttl := int64(0), int64(1000)
	_, _ = e.create("s", 1, FIFO, now, noopSave)
	_, _ = e.acquire("s", "held", "w0", 1, 0, 0, now, ttl, noopSave)
	_, _ = e.acquire("s", "w", "w1", 1, 0, 0, now, ttl, noopSave)
	if ok, err := e.cancel("s", "w1", now+1, noopSave); err != nil || !ok {
		t.Fatalf("cancel: %v %v", ok, err)
	}
	// double cancel is idempotent
	if ok, err := e.cancel("s", "w1", now+2, noopSave); err != nil || ok {
		t.Fatalf("double cancel: %v %v", ok, err)
	}
	// new waiter can now take the queue head; releasing holder admits it
	_, _ = e.acquire("s", "w2h", "w2", 1, 0, 0, now+3, ttl, noopSave)
	evs, _, _ := e.release("s", "held", 1, now+4, ttl, noopSave)
	if l := leasesFromEvents(evs); len(l) != 1 || l[0].HolderID != "w2h" {
		t.Fatalf("expected w2 admitted, got %+v", l)
	}
}

func TestLeaseExpiryRecycles(t *testing.T) {
	e := newEngine()
	now, ttl := int64(0), int64(100)
	_, _ = e.create("s", 1, FIFO, now, noopSave)
	_, _ = e.acquire("s", "h", "w0", 1, 0, 0, now, ttl, noopSave)
	_, _ = e.acquire("s", "next", "w1", 1, 0, 0, now, ttl, noopSave)
	// before expiry nothing happens
	if evs, _ := e.expire("s", "h", false, now+50, ttl, noopSave); len(evs) != 0 {
		t.Fatal("reaped too early")
	}
	// expired holder recycled and next granted atomically
	evs, _ := e.expire("s", "h", false, now+101, ttl, noopSave)
	if l := leasesFromEvents(evs); len(l) != 1 || l[0].HolderID != "next" {
		t.Fatalf("expected next granted after expiry: %+v", l)
	}
	snap, _ := e.snapshot("s")
	if snap.usedCount() != 1 {
		t.Fatalf("used=%d", snap.usedCount())
	}
}

func TestPreemptInvalidatesOldToken(t *testing.T) {
	e := newEngine()
	now, ttl := int64(0), int64(1000)
	_, _ = e.create("s", 1, FIFO, now, noopSave)
	evs, _ := e.acquire("s", "h", "w0", 1, 0, 0, now, ttl, noopSave)
	fence := leasesFromEvents(evs)[0].Fence
	_, _ = e.acquire("s", "hp", "w1", 1, 10, 0, now, ttl, noopSave)
	pevs, err := e.preempt("s", []string{"h"}, now+1, ttl, noopSave)
	if err != nil {
		t.Fatal(err)
	}
	if l := leasesFromEvents(pevs); len(l) != 1 || l[0].HolderID != "hp" {
		t.Fatalf("preempt did not admit hp: %+v", l)
	}
	// old holder release is an idempotent no-op (holder gone)
	if evs, _, err := e.release("s", "h", fence, now+2, ttl, noopSave); err != nil || len(evs) != 0 {
		t.Fatalf("stale preempted release should be no-op: %v %v", evs, err)
	}
}

func TestWaiterTimeout(t *testing.T) {
	e := newEngine()
	now, ttl := int64(0), int64(1000)
	_, _ = e.create("s", 1, FIFO, now, noopSave)
	_, _ = e.acquire("s", "h", "w0", 1, 0, 0, now, ttl, noopSave)
	_, _ = e.acquire("s", "w", "w1", 1, 0, now+100, now, ttl, noopSave)
	evs, _ := e.expire("s", "w1", true, now+101, ttl, noopSave)
	if len(evs) != 1 || evs[0].Type != evDequeued {
		t.Fatalf("expected dequeue, got %+v", evs)
	}
	snap, _ := e.snapshot("s")
	if len(snap.Waiters) != 0 || snap.usedCount() != 1 {
		t.Fatalf("queue not cleaned: %+v", snap)
	}
}

package semaphore

import (
	"errors"
	"testing"
	"time"

	"github.com/guoyyit/distributed-semaphore-go/config"
)

var t0 = time.Unix(1_700_000_000, 0)

func applyAll(t *testing.T, m *Machine, evs []Event) {
	t.Helper()
	for _, e := range evs {
		if err := m.Apply(e); err != nil {
			t.Fatalf("apply seq %d: %v", e.Seq, err)
		}
	}
}

func initSema(t *testing.T, m *Machine, name string, cap int, policy config.QueuePolicy) {
	t.Helper()
	e, err := m.PlanInit(1, t0, name, cap, policy)
	if err != nil {
		t.Fatalf("plan init: %v", err)
	}
	if err := m.Apply(e); err != nil {
		t.Fatalf("apply init: %v", err)
	}
}

func TestAtomicMultiPermitGrantOrFullWait(t *testing.T) {
	m := NewMachine()
	initSema(t, m, "s", 3, config.QueueFIFO)

	// Grant 2 to h1 -> 1 free.
	e, err := m.PlanAcquire(2, t0, "s", "r1", "h1", 2, 0, time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	if e.Type != EventAcquired {
		t.Fatalf("want immediate grant, got %v", e.Type)
	}
	m.Apply(e)

	// Request 3 permits: only 1 free -> must go fully to wait, never partial.
	e, err = m.PlanAcquire(3, t0, "s", "r2", "h2", 3, 0, time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	if e.Type != EventWait {
		t.Fatalf("want full wait for oversized request, got %v", e.Type)
	}
	m.Apply(e)

	st, _ := m.Stats("s")
	if st.Available != 1 {
		t.Fatalf("available=%d want 1 (partial grant leaked)", st.Available)
	}
	if len(st.Waiters) != 1 || st.Waiters[0].Permits != 3 {
		t.Fatalf("waiter not enqueued wholly: %+v", st.Waiters)
	}
}

func TestFencingTokenRotationAndStaleReject(t *testing.T) {
	m := NewMachine()
	initSema(t, m, "s", 1, config.QueueFIFO)
	e, _ := m.PlanAcquire(2, t0, "s", "r1", "h1", 1, 0, time.Second, 0)
	m.Apply(e)
	tok := e.Acquired.Token

	// Renew rotates the token.
	re, err := m.PlanRenew(3, t0, "s", "rrenew", "h1", tok, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	m.Apply(re)
	newTok := re.Renewed.NewToken
	if newTok == tok {
		t.Fatal("renewal must rotate fencing token")
	}
	g, _ := m.Grant("s", "h1")
	if g.Token != newTok {
		t.Fatalf("grant token=%d want %d", g.Token, newTok)
	}

	// Old token release must be rejected and not affect state.
	if _, _, err := m.PlanRelease(4, t0, "s", "rrel", "h1", tok); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("stale release err=%v want ErrInvalidToken", err)
	}
	st, _ := m.Stats("s")
	if st.Available != 0 || len(st.Grants) != 1 {
		t.Fatalf("stale token mutated state: avail=%d grants=%d", st.Available, len(st.Grants))
	}

	// New token release succeeds and frees the permit.
	re2, ok, err := m.PlanRelease(5, t0, "s", "rrel2", "h1", newTok)
	if err != nil || !ok {
		t.Fatalf("current-token release: ok=%v err=%v", ok, err)
	}
	m.Apply(re2)
	st, _ = m.Stats("s")
	if st.Available != 1 {
		t.Fatalf("available=%d want 1", st.Available)
	}

	// Duplicate release is an idempotent no-op and must not double-credit.
	_, ok, err = m.PlanRelease(6, t0, "s", "rrel3", "h1", newTok)
	if err != nil || ok {
		t.Fatalf("duplicate release should be no-op: ok=%v err=%v", ok, err)
	}
	st, _ = m.Stats("s")
	if st.Available != 1 {
		t.Fatalf("idempotent release changed available=%d", st.Available)
	}
}

func TestLeaseExpirationReclaimsAndPumps(t *testing.T) {
	m := NewMachine()
	initSema(t, m, "s", 2, config.QueueFIFO)
	e, _ := m.PlanAcquire(2, t0, "s", "r1", "h1", 2, 0, time.Second, 0)
	m.Apply(e)
	tok := e.Acquired.Token

	// Two waiters, each wants 1.
	w1, _ := m.PlanAcquire(3, t0, "s", "w1", "c1", 1, 0, time.Second, 0)
	m.Apply(w1)
	w2, _ := m.PlanAcquire(4, t0, "s", "w2", "c2", 1, 0, time.Second, 0)
	m.Apply(w2)
	st, _ := m.Stats("s")
	if st.Available != 0 || len(st.Waiters) != 2 {
		t.Fatalf("setup wrong: %+v", st)
	}

	// Before expiry: nothing reaped.
	if evs := m.PlanExpirations(5, t0.Add(time.Second-1)); len(evs) != 0 {
		t.Fatalf("reaped too early: %d", len(evs))
	}
	// At expiry: grant expires and BOTH waiters get promoted atomically.
	evs := m.PlanExpirations(5, t0.Add(time.Second+1))
	if len(evs) != 3 {
		t.Fatalf("want 1 expiry + 2 grants, got %d", len(evs))
	}
	if evs[0].Type != EventExpired || evs[1].Type != EventAcquired || evs[2].Type != EventAcquired {
		t.Fatalf("unexpected batch order: %v %v %v", evs[0].Type, evs[1].Type, evs[2].Type)
	}
	applyAll(t, m, evs)
	st, _ = m.Stats("s")
	if st.Available != 0 || len(st.Grants) != 2 || len(st.Waiters) != 0 {
		t.Fatalf("post-expiry state wrong: avail=%d grants=%d waiters=%d", st.Available, len(st.Grants), len(st.Waiters))
	}
	// Expired holder no longer exists: releasing its old token is a safe
	// idempotent no-op (it must never reach or alter the new holders).
	if _, ok, err := m.PlanRelease(99, t0.Add(2*time.Second), "s", "late", "h1", tok); err != nil || ok {
		t.Fatalf("expired-token release must be no-op: ok=%v err=%v", ok, err)
	}
	st, _ = m.Stats("s")
	if st.Available != 0 {
		t.Fatalf("late release mutated available=%d", st.Available)
	}
	_ = tok
}

func TestWaiterTimeoutReleasesSlotAndGrantsNext(t *testing.T) {
	m := NewMachine()
	initSema(t, m, "s", 1, config.QueueFIFO)
	g, _ := m.PlanAcquire(2, t0, "s", "r1", "h1", 1, 0, time.Second, 0)
	m.Apply(g)

	// First waiter wants 1 and times out in 500ms while queued.
	big, _ := m.PlanAcquire(3, t0, "s", "big", "cb", 1, 0, time.Second, 500*time.Millisecond)
	m.Apply(big)
	// Second waiter behind.
	small, _ := m.PlanAcquire(4, t0, "s", "small", "cs", 1, 0, time.Second, 0)
	m.Apply(small)

	// Grant frees permits at t=1s, but "big" already timed out at t=0.5s:
	// reaping batch must remove big and grant small.
	evs := m.PlanExpirations(5, t0.Add(1100*time.Millisecond))
	// Expect: waiter big expired, grant h1 expired, then small acquired.
	var sawBigExp, sawSmallGrant bool
	for _, e := range evs {
		if e.Type == EventExpired && e.Expired.RequestID == "big" {
			sawBigExp = true
		}
		if e.Type == EventAcquired && e.Acquired.RequestID == "small" {
			sawSmallGrant = true
		}
	}
	if !sawBigExp || !sawSmallGrant {
		t.Fatalf("expected big expiry + small grant in batch, got %+v", evs)
	}
	applyAll(t, m, evs)
	st, _ := m.Stats("s")
	if len(st.Waiters) != 0 {
		t.Fatalf("timed-out waiter leaked in queue: %+v", st.Waiters)
	}
}

func TestPriorityQueueOrdering(t *testing.T) {
	m := NewMachine()
	initSema(t, m, "s", 1, config.QueuePriority)
	g, _ := m.PlanAcquire(2, t0, "s", "r1", "h1", 1, 0, time.Second, 0)
	m.Apply(g)

	low, _ := m.PlanAcquire(3, t0, "s", "low", "cl", 1, 1, time.Second, 0)
	m.Apply(low)
	high, _ := m.PlanAcquire(4, t0.Add(time.Millisecond), "s", "high", "ch", 1, 10, time.Second, 0)
	m.Apply(high)
	mid, _ := m.PlanAcquire(5, t0.Add(2*time.Millisecond), "s", "mid", "cm", 1, 5, time.Second, 0)
	m.Apply(mid)

	// Release h1; pump must promote high first.
	rel, _, _ := m.PlanRelease(6, t0.Add(time.Second), "s", "rel1", "h1", g.Acquired.Token)
	m.Apply(rel)
	evs := m.PlanPump(7, t0.Add(time.Second))
	if len(evs) != 1 || evs[0].Acquired.RequestID != "high" {
		t.Fatalf("expected high priority grant, got %+v", evs)
	}
	m.Apply(evs[0])

	// h(high) releases -> mid next (priority 5 beats low priority 1).
	rel2, _, _ := m.PlanRelease(8, t0.Add(2*time.Second), "s", "relh", "ch", evs[0].Acquired.Token)
	m.Apply(rel2)
	evs = m.PlanPump(9, t0.Add(2*time.Second))
	if len(evs) != 1 || evs[0].Acquired.RequestID != "mid" {
		t.Fatalf("expected mid grant, got %+v", evs)
	}
}

func TestCancelReleasesQueueSlot(t *testing.T) {
	m := NewMachine()
	initSema(t, m, "s", 1, config.QueueFIFO)
	g, _ := m.PlanAcquire(2, t0, "s", "r1", "h1", 1, 0, time.Second, 0)
	m.Apply(g)
	w, _ := m.PlanAcquire(3, t0, "s", "w1", "cw", 1, 0, time.Second, 0)
	m.Apply(w)

	ce, ok, err := m.PlanCancel(4, t0, "s", "w1", "cancel-op-1")
	if err != nil || !ok {
		t.Fatalf("cancel: ok=%v err=%v", ok, err)
	}
	m.Apply(ce)
	st, _ := m.Stats("s")
	if len(st.Waiters) != 0 {
		t.Fatal("cancel did not remove waiter")
	}
	// Cancelling again is an idempotent no-op.
	if _, ok, err := m.PlanCancel(5, t0, "s", "w1", "cancel-op-2"); err != nil || ok {
		t.Fatalf("double cancel should be no-op: ok=%v err=%v", ok, err)
	}
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	m := NewMachine()
	initSema(t, m, "s", 2, config.QueueFIFO)
	e, _ := m.PlanAcquire(2, t0, "s", "r1", "h1", 1, 0, time.Second, 0)
	m.Apply(e)
	w, _ := m.PlanAcquire(3, t0, "s", "w1", "cw", 1, 0, time.Second, 0)
	m.Apply(w)

	data := m.Snapshot()
	m2 := NewMachine()
	if err := m2.Restore(data); err != nil {
		t.Fatal(err)
	}
	a, _ := m.Stats("s")
	b, _ := m2.Stats("s")
	if a.Available != b.Available || a.Capacity != b.Capacity ||
		len(a.Grants) != len(b.Grants) || len(a.Waiters) != len(b.Waiters) {
		t.Fatalf("snapshot mismatch: %+v vs %+v", a, b)
	}
}

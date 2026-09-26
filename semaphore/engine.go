package semaphore

import (
	"sort"
	"sync"
	"time"
)

// engine holds the in-memory state of all semaphores owned by one instance.
//
// Concurrency/consistency model:
//   - Every state transition is planned as one or more Events while the
//     engine mutex is held. Planning is pure: it never mutates state.
//   - Still holding the mutex, the owner durably persists the batch via the
//     save callback. If persistence fails, nothing has changed.
//   - Only after a successful save are events applied. State therefore never
//     reflects a transition that was not durably logged, and a recycle plus
//     the grants it enables commit as one atomic batch: permit accounting
//     and the wait queue can never disagree.
type engine struct {
	mu      sync.Mutex
	sems    map[string]*semaState
	compact func(name string, snap Snapshot) // optional WAL compaction hook
}

// semaState is the state of one named semaphore.
type semaState struct {
	name     string
	permits  int
	used     int
	strategy Strategy
	fenceSeq int64 // global monotonic fencing token counter for this semaphore
	eventSeq int64 // last applied event sequence
	holders  map[string]*Holder
	waiters  map[string]*Waiter
	notified map[int64]bool // fences of queue grants delivered to clients
	queue    []string       // waiter ids in arrival order
}

func newEngine() *engine {
	return &engine{sems: make(map[string]*semaState)}
}

// saver persists an event batch; engine applies events only if it succeeds.
type saver func(evs []Event) error

// leasesFromEvents extracts granted leases from a committed event batch.
func leasesFromEvents(evs []Event) []Lease {
	var out []Lease
	for _, ev := range evs {
		if ev.Type == evAcquired || ev.Type == evGranted {
			out = append(out, Lease{
				Semaphore: ev.Name,
				HolderID:  ev.HolderID,
				Fence:     ev.Fence,
				Count:     ev.Count,
				ExpiresAt: time.Unix(0, ev.ExpiresAt),
			})
		}
	}
	return out
}

// commit plans a batch, persists it, then applies it atomically.
func (e *engine) commit(evs []Event, err error, save saver) ([]Event, error) {
	if err != nil {
		return nil, err
	}
	if len(evs) == 0 || save == nil {
		return evs, nil
	}
	if err := save(evs); err != nil {
		return nil, err
	}
	for _, ev := range evs {
		e.applyLocked(ev)
	}
	if e.compact != nil {
		seen := map[string]bool{}
		for _, ev := range evs {
			if seen[ev.Name] {
				continue
			}
			seen[ev.Name] = true
			if st, ok := e.sems[ev.Name]; ok {
				e.compact(ev.Name, st.deepSnapshot())
			}
		}
	}
	return evs, nil
}

// create is idempotent: creating an existing identical semaphore is a no-op.
func (e *engine) create(name string, permits int, strategy Strategy, now int64, save saver) ([]Event, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if permits < 0 {
		return nil, ErrInvalidPermitCount
	}
	if s, ok := e.sems[name]; ok {
		if s.permits != permits || s.strategy != strategy {
			return nil, ErrUnknownSemaphore
		}
		return nil, nil
	}
	ev := Event{Seq: 1, At: now, Type: evCreated, Name: name, Permits: permits, Strategy: strategy}
	return e.commit([]Event{ev}, nil, save)
}

// acquire either grants immediately (queue empty and enough free permits) or
// enqueues the whole request. Partial grants never happen.
func (e *engine) acquire(name, holderID, waiterID string, count, priority int, deadline, now, ttlNanos int64, save saver) ([]Event, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if count <= 0 {
		return nil, ErrInvalidPermitCount
	}
	s, ok := e.sems[name]
	if !ok {
		return nil, ErrUnknownSemaphore
	}
	var evs []Event
	if len(s.queue) == 0 && s.used+count <= s.permits {
		fence := s.fenceSeq + 1
		ev := Event{
			Seq: s.eventSeq + 1, At: now, Type: evAcquired, Name: name,
			HolderID: holderID, Fence: fence, Count: count, ExpiresAt: now + ttlNanos,
		}
		evs = []Event{ev}
	} else {
		ev := Event{
			Seq: s.eventSeq + 1, At: now, Type: evQueued, Name: name,
			WaiterID: waiterID, HolderID: holderID, Count: count,
			Priority: priority, Deadline: deadline, WaiterSeq: s.eventSeq + 1,
		}
		evs = []Event{ev}
	}
	return e.commit(evs, nil, save)
}

// release returns permits. Idempotent:
//   - holder unknown (released/reclaimed/preempted): no event, no wakeup;
//   - holder known but fence stale: rejected with ErrStaleFencingToken.
//
// Waiters made serviceable are granted within the same atomic batch.
func (e *engine) release(name, holderID string, fence, now, ttlNanos int64, save saver) ([]Event, bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, ok := e.sems[name]
	if !ok {
		return nil, false, ErrUnknownSemaphore
	}
	h, ok := s.holders[holderID]
	if !ok {
		return nil, true, nil // idempotent no-op
	}
	if h.Fence != fence {
		return nil, false, ErrStaleFencingToken
	}
	evs, err := e.commit(s.planRecycle([]string{holderID}, nil, now, ttlNanos), nil, save)
	return evs, true, err
}

// releaseHolder returns a holder's permits without requiring a known fencing
// token. Used only for compensating cleanup of an acquire the caller itself
// abandoned (the holder is authenticated by identity). Idempotent and wakes
// serviceable waiters in the same atomic batch.
func (e *engine) releaseHolder(name, holderID string, now, ttlNanos int64, save saver) ([]Event, bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, ok := e.sems[name]
	if !ok {
		return nil, false, ErrUnknownSemaphore
	}
	if _, ok := s.holders[holderID]; !ok {
		return nil, true, nil
	}
	evs, err := e.commit(s.planRecycle([]string{holderID}, nil, now, ttlNanos), nil, save)
	return evs, true, err
}

// renew validates the latest fencing token, extends the deadline and rotates
// to a fresh token. The old token can never be used again.
func (e *engine) renew(name, holderID string, fence, deadline, now int64, save saver) (Event, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, ok := e.sems[name]
	if !ok {
		return Event{}, ErrUnknownSemaphore
	}
	h, ok := s.holders[holderID]
	if !ok || h.Fence != fence {
		return Event{}, ErrStaleFencingToken
	}
	ev := Event{
		Seq: s.eventSeq + 1, At: now, Type: evRenewed, Name: name,
		HolderID: h.ID, OldFence: fence, Fence: s.fenceSeq + 1,
		Count: h.Count, ExpiresAt: deadline,
	}
	_, err := e.commit([]Event{ev}, nil, save)
	return ev, err
}

// cancel removes a queued waiter. Idempotent when the waiter was already
// granted, canceled or expired: no event, no error, no resource change.
func (e *engine) cancel(name, waiterID string, now int64, save saver) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, ok := e.sems[name]
	if !ok {
		return false, ErrUnknownSemaphore
	}
	w, ok := s.waiters[waiterID]
	if !ok {
		return false, nil
	}
	ev := Event{Seq: s.eventSeq + 1, At: now, Type: evDequeued, Name: name, WaiterID: w.ID}
	_, err := e.commit([]Event{ev}, nil, save)
	return true, err
}

// expire reaps one expired holder lease or one timed-out waiter. Holder
// permits are recycled and serviceable waiters are granted in one batch.
func (e *engine) expire(name, id string, isWaiter bool, now, ttlNanos int64, save saver) ([]Event, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, ok := e.sems[name]
	if !ok {
		return nil, ErrUnknownSemaphore
	}
	if isWaiter {
		w, ok := s.waiters[id]
		if !ok || w.Deadline == 0 || w.Deadline > now {
			return nil, nil
		}
		ev := Event{Seq: s.eventSeq + 1, At: now, Type: evDequeued, Name: name, WaiterID: w.ID}
		return e.commit([]Event{ev}, nil, save)
	}
	h, ok := s.holders[id]
	if !ok || h.ExpiresAt > now {
		return nil, nil
	}
	return e.commit(s.planRecycle([]string{id}, nil, now, ttlNanos), nil, save)
}

// preempt forcibly revokes holders (to admit high-priority waiters). Revoked
// holders disappear and their tokens are invalidated; freed permits are
// offered to the queue in the same atomic batch.
func (e *engine) preempt(name string, holderIDs []string, now, ttlNanos int64, save saver) ([]Event, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, ok := e.sems[name]
	if !ok {
		return nil, ErrUnknownSemaphore
	}
	var live []string
	for _, id := range holderIDs {
		if _, ok := s.holders[id]; ok {
			live = append(live, id)
		}
	}
	if len(live) == 0 {
		return nil, nil
	}
	return e.commit(s.planRecycle(live, nil, now, ttlNanos), nil, save)
}

// planRecycle builds an atomic batch: release events for holders, dequeue
// events for waiters, then grants for every waiter that fits. Pure.
func (s *semaState) planRecycle(holders []string, waiters []string, now, ttlNanos int64) []Event {
	removedWaiters := map[string]bool{}
	seq := s.eventSeq
	fence := s.fenceSeq

	var evs []Event
	simUsed := s.used
	for _, id := range holders {
		if h := s.holders[id]; h != nil {
			simUsed -= h.Count
			if simUsed < 0 {
				simUsed = 0
			}
			seq++
			evs = append(evs, Event{
				Seq: seq, At: now, Type: evReleased, Name: s.name,
				HolderID: h.ID, Fence: h.Fence, Count: h.Count,
			})
		}
	}
	for _, id := range waiters {
		if w := s.waiters[id]; w != nil {
			removedWaiters[id] = true
			seq++
			evs = append(evs, Event{Seq: seq, At: now, Type: evDequeued, Name: s.name, WaiterID: w.ID})
		}
	}

	order := make([]string, 0, len(s.queue))
	for _, id := range s.queue {
		if !removedWaiters[id] {
			order = append(order, id)
		}
	}
	if s.strategy == Priority {
		sort.SliceStable(order, func(i, j int) bool {
			wi, wj := s.waiters[order[i]], s.waiters[order[j]]
			if wi.Priority != wj.Priority {
				return wi.Priority > wj.Priority
			}
			return wi.Seq < wj.Seq
		})
	}
	// Strict order: the first request that does not fit blocks all later
	// ones, so all-or-nothing multi-permit requests never split permits.
	for _, wid := range order {
		w := s.waiters[wid]
		if w == nil || simUsed+w.Count > s.permits {
			break
		}
		simUsed += w.Count
		fence++
		seq++
		evs = append(evs, Event{
			Seq: seq, At: now, Type: evGranted, Name: s.name,
			WaiterID: wid, HolderID: w.HolderID, Fence: fence,
			Count: w.Count, ExpiresAt: now + ttlNanos,
		})
	}
	return evs
}

// applyLocked mutates state for an already-persisted event (engine.mu held).
func (e *engine) applyLocked(ev Event) {
	if ev.Type == evCreated {
		if _, ok := e.sems[ev.Name]; ok {
			return
		}
		e.sems[ev.Name] = &semaState{
			name: ev.Name, permits: ev.Permits, strategy: ev.Strategy,
			fenceSeq: 0, eventSeq: ev.Seq,
			holders: map[string]*Holder{}, waiters: map[string]*Waiter{},
			notified: map[int64]bool{},
		}
		return
	}
	s := e.sems[ev.Name]
	if s == nil {
		return
	}
	if ev.Seq <= s.eventSeq {
		return // idempotent replay
	}
	s.eventSeq = ev.Seq
	switch ev.Type {
	case evAcquired:
		if _, dup := s.holders[ev.HolderID]; dup {
			return
		}
		s.holders[ev.HolderID] = &Holder{ID: ev.HolderID, Fence: ev.Fence, Count: ev.Count, ExpiresAt: ev.ExpiresAt}
		s.used += ev.Count
		if ev.Fence > s.fenceSeq {
			s.fenceSeq = ev.Fence
		}
	case evGranted:
		if _, dup := s.holders[ev.HolderID]; dup {
			break
		}
		delete(s.waiters, ev.WaiterID)
		s.removeFromQueue(ev.WaiterID)
		s.holders[ev.HolderID] = &Holder{ID: ev.HolderID, WaiterID: ev.WaiterID, Fence: ev.Fence, Count: ev.Count, ExpiresAt: ev.ExpiresAt}
		s.used += ev.Count
		if ev.Fence > s.fenceSeq {
			s.fenceSeq = ev.Fence
		}
	case evNotified:
		s.notified[ev.Fence] = true
	case evReleased:
		if h, ok := s.holders[ev.HolderID]; ok {
			s.used -= h.Count
			if s.used < 0 {
				s.used = 0
			}
			delete(s.holders, ev.HolderID)
		}
	case evRenewed:
		if h, ok := s.holders[ev.HolderID]; ok && h.Fence == ev.OldFence {
			h.Fence = ev.Fence
			h.ExpiresAt = ev.ExpiresAt
			if ev.Fence > s.fenceSeq {
				s.fenceSeq = ev.Fence
			}
		}
	case evQueued:
		if _, ok := s.waiters[ev.WaiterID]; ok {
			break
		}
		s.waiters[ev.WaiterID] = &Waiter{
			ID: ev.WaiterID, HolderID: ev.HolderID, Count: ev.Count,
			Priority: ev.Priority, Deadline: ev.Deadline, Seq: ev.WaiterSeq,
		}
		s.queue = append(s.queue, ev.WaiterID)
	case evDequeued:
		delete(s.waiters, ev.WaiterID)
		s.removeFromQueue(ev.WaiterID)
	}
}

// apply is exported for WAL replay at startup (no events exist in memory yet).
func (e *engine) apply(ev Event) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.applyLocked(ev)
}

// restore rebuilds engine state from a snapshot; caller then replays events.
func (e *engine) restore(snap Snapshot) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s := &semaState{
		name: snap.Name, permits: snap.Permits, strategy: snap.Strategy,
		fenceSeq: snap.FenceSeq, eventSeq: snap.Seq,
		holders: map[string]*Holder{}, waiters: map[string]*Waiter{},
		notified: map[int64]bool{},
	}
	for _, f := range snap.Notified {
		s.notified[f] = true
	}
	for i := range snap.Holders {
		h := snap.Holders[i]
		s.holders[h.ID] = &h
		s.used += h.Count
	}
	for i := range snap.Waiters {
		w := snap.Waiters[i]
		s.waiters[w.ID] = &w
		s.queue = append(s.queue, w.ID)
	}
	sort.SliceStable(s.queue, func(i, j int) bool {
		return s.waiters[s.queue[i]].Seq < s.waiters[s.queue[j]].Seq
	})
	e.sems[snap.Name] = s
}

// deepSnapshot returns a deep-copied durable snapshot; caller holds mu.
func (s *semaState) deepSnapshot() Snapshot {
	snap := Snapshot{
		Name: s.name, Permits: s.permits, Strategy: s.strategy,
		FenceSeq: s.fenceSeq, Seq: s.eventSeq,
	}
	for _, h := range s.holders {
		snap.Holders = append(snap.Holders, *h)
	}
	for _, wid := range s.queue {
		snap.Waiters = append(snap.Waiters, *s.waiters[wid])
	}
	for f := range s.notified {
		snap.Notified = append(snap.Notified, f)
	}
	return snap
}

// snapshot returns a deep-copied durable snapshot.
func (e *engine) snapshot(name string) (Snapshot, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, ok := e.sems[name]
	if !ok {
		return Snapshot{}, false
	}
	return s.deepSnapshot(), true
}

func (e *engine) names() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, 0, len(e.sems))
	for n := range e.sems {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// expired returns holder/waiter ids whose lease or deadline is due at now.
func (e *engine) expired(name string, now int64) (holders []string, waiters []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, ok := e.sems[name]
	if !ok {
		return nil, nil
	}
	for id, h := range s.holders {
		if h.ExpiresAt <= now {
			holders = append(holders, id)
		}
	}
	for id, w := range s.waiters {
		if w.Deadline != 0 && w.Deadline <= now {
			waiters = append(waiters, id)
		}
	}
	sort.Strings(holders)
	sort.Strings(waiters)
	return holders, waiters
}

func (e *engine) exists(name string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.sems[name]
	return ok
}

func (s *semaState) removeFromQueue(id string) {
	for i, qid := range s.queue {
		if qid == id {
			s.queue = append(s.queue[:i], s.queue[i+1:]...)
			return
		}
	}
}

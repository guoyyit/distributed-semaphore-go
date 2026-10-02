package semaphore

import (
	"sort"
	"sync"
	"time"
)

// engine holds the in-memory state of all semaphores owned by one instance.
//
// Concurrency/consistency model:
//   - Each named semaphore has its own mutex. Operations on different names
//     never block each other; operations on one name are strictly
//     serialized so planning and permit accounting always agree.
//   - Every state transition is planned as one or more Events while the
//     semaphore mutex is held, persisted via the save callback, and applied.
//     A recycle plus the grants it enables commit as one atomic batch.
type engine struct {
	mu      sync.Mutex // guards sems registry only
	sems    map[string]*semaState
	imu     sync.Mutex // guards waiterOwners index
	wowners map[string]string

	journal *journal // optional per-name group-commit pipeline

	compact func(name string, snap Snapshot) // optional WAL compaction hook
}

// semaState is the state of one named semaphore.
type semaState struct {
	mu       sync.Mutex
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
	return &engine{sems: make(map[string]*semaState), wowners: make(map[string]string)}
}

// setJournal enables the per-name group-commit durability pipeline.
func (e *engine) setJournal(j *journal) { e.journal = j }

// saver persists an event batch; engine applies events only if it succeeds.
type saver func(evs []Event) error

// stateLocked returns the named semaphore and acquires its mutex. The caller
// must call s.mu.Unlock when done.
func (e *engine) stateLocked(name string) (*semaState, bool) {
	e.mu.Lock()
	s, ok := e.sems[name]
	e.mu.Unlock()
	if !ok {
		return nil, false
	}
	s.mu.Lock()
	return s, true
}

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

// snapshotAtCompaction builds a durable snapshot under the semaphore mutex.
// Called only by the per-name journal writer at a drain point.
func (e *engine) snapshotAtCompaction(name string) (Snapshot, bool) {
	s, ok := e.stateLocked(name)
	if !ok {
		return Snapshot{}, false
	}
	defer s.mu.Unlock()
	return s.deepSnapshot(), true
}

// commit applies a planned batch and makes it durable, then releases s.mu
// (the lock is always released, even on error). With the group-commit
// journal, events are applied and enqueued while the mutex is held so queue
// order matches Seq order; the caller waits for durability only after
// releasing the mutex, letting later operations join the same fsync batch.
func (e *engine) commit(s *semaState, evs []Event) ([]Event, error) {
	if len(evs) == 0 || e.journal == nil {
		for i := range evs {
			e.applyNameLocked(s, evs[i])
		}
		if len(evs) > 0 && e.compact != nil {
			e.compact(s.name, s.deepSnapshot())
		}
		s.mu.Unlock()
		return evs, nil
	}
	for i := range evs {
		e.applyNameLocked(s, evs[i])
	}
	res := e.journal.enqueue(s.name, evs)
	s.mu.Unlock()
	if err := res.wait(); err != nil {
		return nil, err
	}
	return evs, nil
}

// create is idempotent: creating an existing identical semaphore is a no-op.
func (e *engine) create(name string, permits int, strategy Strategy, now int64, save saver) ([]Event, error) {
	e.mu.Lock()
	if permits < 0 {
		e.mu.Unlock()
		return nil, ErrInvalidPermitCount
	}
	s, ok := e.sems[name]
	if !ok {
		ev := Event{Seq: 1, At: now, Type: evCreated, Name: name, Permits: permits, Strategy: strategy}
		if save != nil {
			if err := save([]Event{ev}); err != nil {
				e.mu.Unlock()
				return nil, err
			}
		}
		ns := &semaState{
			name: name, permits: permits, strategy: strategy,
			fenceSeq: 0, eventSeq: 1,
			holders: map[string]*Holder{}, waiters: map[string]*Waiter{},
			notified: map[int64]bool{},
		}
		e.sems[name] = ns
		e.mu.Unlock()
		return []Event{ev}, nil
	}
	e.mu.Unlock()
	s.mu.Lock()
	if s.permits != permits || s.strategy != strategy {
		s.mu.Unlock()
		return nil, ErrUnknownSemaphore
	}
	s.mu.Unlock()
	return nil, nil
}

// acquire either grants immediately (queue empty and enough free permits) or
// enqueues the whole request. Partial grants never happen.
func (e *engine) acquire(name, holderID, waiterID string, count, priority int, deadline, now, ttlNanos int64, save saver) ([]Event, error) {
	if count <= 0 {
		return nil, ErrInvalidPermitCount
	}
	s, ok := e.stateLocked(name)
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
	return e.commit(s, evs)
}

// release returns permits. Idempotent:
//   - holder unknown (released/reclaimed/preempted): no event, no wakeup;
//   - holder known but fence stale: rejected with ErrStaleFencingToken.
//
// Waiters made serviceable are granted within the same atomic batch.
func (e *engine) release(name, holderID string, fence, now, ttlNanos int64, save saver) ([]Event, bool, error) {
	s, ok := e.stateLocked(name)
	if !ok {
		return nil, false, ErrUnknownSemaphore
	}
	h, ok := s.holders[holderID]
	if !ok {
		s.mu.Unlock()
		return nil, true, nil // idempotent no-op
	}
	if h.Fence != fence {
		s.mu.Unlock()
		return nil, false, ErrStaleFencingToken
	}
	evs, err := e.commit(s, s.planRecycle([]string{holderID}, nil, now, ttlNanos))
	return evs, true, err
}

// releaseHolder returns a holder's permits without requiring a known fencing
// token. Used only for compensating cleanup of an acquire the caller itself
// abandoned (the holder is authenticated by identity). Idempotent and wakes
// serviceable waiters in the same atomic batch.
func (e *engine) releaseHolder(name, holderID string, now, ttlNanos int64, save saver) ([]Event, bool, error) {
	s, ok := e.stateLocked(name)
	if !ok {
		return nil, false, ErrUnknownSemaphore
	}
	if _, ok := s.holders[holderID]; !ok {
		s.mu.Unlock()
		return nil, true, nil
	}
	evs, err := e.commit(s, s.planRecycle([]string{holderID}, nil, now, ttlNanos))
	return evs, true, err
}

// renew validates the latest fencing token, extends the deadline and rotates
// to a fresh token. The old token can never be used again.
func (e *engine) renew(name, holderID string, fence, deadline, now int64, save saver) (Event, error) {
	s, ok := e.stateLocked(name)
	if !ok {
		return Event{}, ErrUnknownSemaphore
	}
	h, ok := s.holders[holderID]
	if !ok || h.Fence != fence {
		s.mu.Unlock()
		return Event{}, ErrStaleFencingToken
	}
	ev := Event{
		Seq: s.eventSeq + 1, At: now, Type: evRenewed, Name: name,
		HolderID: h.ID, OldFence: fence, Fence: s.fenceSeq + 1,
		Count: h.Count, ExpiresAt: deadline,
	}
	_, err := e.commit(s, []Event{ev})
	return ev, err
}

// cancel removes a queued waiter. Idempotent when the waiter was already
// granted, canceled or expired: no event, no error, no resource change.
func (e *engine) cancel(name, waiterID string, now int64, save saver) (bool, error) {
	s, ok := e.stateLocked(name)
	if !ok {
		return false, ErrUnknownSemaphore
	}
	w, ok := s.waiters[waiterID]
	if !ok {
		s.mu.Unlock()
		return false, nil
	}
	ev := Event{Seq: s.eventSeq + 1, At: now, Type: evDequeued, Name: name, WaiterID: w.ID}
	_, err := e.commit(s, []Event{ev})
	return true, err
}

// expire reaps one expired holder lease or one timed-out waiter. Holder
// permits are recycled and serviceable waiters are granted in one batch.
func (e *engine) expire(name, id string, isWaiter bool, now, ttlNanos int64, save saver) ([]Event, error) {
	s, ok := e.stateLocked(name)
	if !ok {
		return nil, ErrUnknownSemaphore
	}
	if isWaiter {
		w, ok := s.waiters[id]
		if !ok || w.Deadline == 0 || w.Deadline > now {
			s.mu.Unlock()
			return nil, nil
		}
		ev := Event{Seq: s.eventSeq + 1, At: now, Type: evDequeued, Name: name, WaiterID: w.ID}
		return e.commit(s, []Event{ev})
	}
	h, ok := s.holders[id]
	if !ok || h.ExpiresAt > now {
		s.mu.Unlock()
		return nil, nil
	}
	return e.commit(s, s.planRecycle([]string{id}, nil, now, ttlNanos))
}

// preempt forcibly revokes holders (to admit high-priority waiters). Revoked
// holders disappear and their tokens are invalidated; freed permits are
// offered to the queue in the same atomic batch.
func (e *engine) preempt(name string, holderIDs []string, now, ttlNanos int64, save saver) ([]Event, error) {
	s, ok := e.stateLocked(name)
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
		s.mu.Unlock()
		return nil, nil
	}
	return e.commit(s, s.planRecycle(live, nil, now, ttlNanos))
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

// applyNameLocked mutates state for an already-persisted event (s.mu held).
func (e *engine) applyNameLocked(s *semaState, ev Event) {
	if ev.Type == evCreated {
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
		e.removeWaiterOwner(ev.WaiterID)
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
		e.addWaiterOwner(ev.WaiterID, s.name)
	case evDequeued:
		if _, ok := s.waiters[ev.WaiterID]; ok {
			e.removeWaiterOwner(ev.WaiterID)
		}
		delete(s.waiters, ev.WaiterID)
		s.removeFromQueue(ev.WaiterID)
	}
}

// apply is exported for WAL replay at startup (no events exist in memory yet).
func (e *engine) apply(ev Event) {
	if ev.Type == evCreated {
		e.mu.Lock()
		if _, ok := e.sems[ev.Name]; !ok {
			e.sems[ev.Name] = &semaState{
				name: ev.Name, permits: ev.Permits, strategy: ev.Strategy,
				fenceSeq: 0, eventSeq: ev.Seq,
				holders: map[string]*Holder{}, waiters: map[string]*Waiter{},
				notified: map[int64]bool{},
			}
		}
		e.mu.Unlock()
		return
	}
	s, ok := e.stateLocked(ev.Name)
	if !ok {
		return
	}
	defer s.mu.Unlock()
	e.applyNameLocked(s, ev)
}

func (e *engine) addWaiterOwner(waiterID, name string) {
	e.imu.Lock()
	e.wowners[waiterID] = name
	e.imu.Unlock()
}

func (e *engine) removeWaiterOwner(waiterID string) {
	e.imu.Lock()
	delete(e.wowners, waiterID)
	e.imu.Unlock()
}

// waiterOwner resolves the semaphore name of a live queued waiter in O(1).
func (e *engine) waiterOwner(waiterID string) string {
	e.imu.Lock()
	defer e.imu.Unlock()
	return e.wowners[waiterID]
}

// restore rebuilds engine state from a snapshot; caller then replays events.
func (e *engine) restore(snap Snapshot) {
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
		e.addWaiterOwner(w.ID, snap.Name)
	}
	sort.SliceStable(s.queue, func(i, j int) bool {
		return s.waiters[s.queue[i]].Seq < s.waiters[s.queue[j]].Seq
	})
	e.mu.Lock()
	if _, exists := e.sems[snap.Name]; !exists {
		e.sems[snap.Name] = s
	}
	e.mu.Unlock()
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
	s, ok := e.stateLocked(name)
	if !ok {
		return Snapshot{}, false
	}
	defer s.mu.Unlock()
	return s.deepSnapshot(), true
}

func (e *engine) names() []string {
	e.mu.Lock()
	out := make([]string, 0, len(e.sems))
	for n := range e.sems {
		out = append(out, n)
	}
	e.mu.Unlock()
	sort.Strings(out)
	return out
}

// expired returns holder/waiter ids whose lease or deadline is due at now.
func (e *engine) expired(name string, now int64) (holders []string, waiters []string) {
	s, ok := e.stateLocked(name)
	if !ok {
		return nil, nil
	}
	defer s.mu.Unlock()
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
	_, ok := e.sems[name]
	e.mu.Unlock()
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

package semaphore

import "sort"

// markNotified durably records that a queued grant (identified by waiter id)
// was delivered to the requesting instance. A restarted owner can then
// re-push exactly the grants whose delivery was never confirmed, without
// granting the same permits twice.
func (e *engine) markNotified(name, waiterID string, fence int64, now int64, save saver) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, ok := e.sems[name]
	if !ok {
		return ErrUnknownSemaphore
	}
	for _, h := range s.holders {
		if h.WaiterID != waiterID {
			continue
		}
		if h.Fence != fence || s.notified[fence] {
			return nil // superseded by renewal, or already confirmed
		}
		ev := Event{Seq: s.eventSeq + 1, At: now, Type: evNotified, Name: name, WaiterID: waiterID, Fence: fence}
		_, err := e.commit([]Event{ev}, nil, save)
		return err
	}
	return nil
}

// pendingGrants lists queue grants not yet confirmed as delivered.
func (e *engine) pendingGrants() []pendingGrant {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []pendingGrant
	for _, s := range e.sems {
		for _, h := range s.holders {
			if h.WaiterID == "" || s.notified[h.Fence] {
				continue
			}
			out = append(out, pendingGrant{
				name: s.name, waiterID: h.WaiterID, holderID: h.ID,
				fence: h.Fence, count: h.Count, expiresAt: h.ExpiresAt,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].name != out[j].name {
			return out[i].name < out[j].name
		}
		return out[i].fence < out[j].fence
	})
	return out
}

type pendingGrant struct {
	name      string
	waiterID  string
	holderID  string
	fence     int64
	count     int
	expiresAt int64
}

package semaphore

import (
	"sort"

	"github.com/guoyyit/distributed-semaphore-go/config"
)

// waiterQueue orders queued acquisition requests according to a policy.
//
// FIFO:      earlier EnqueuedAt is served first (arrival order).
// Priority:  higher Priority is served first; ties break on arrival order.
//
// Promotion is strictly head-of-line in both policies: a request is granted
// only when the head request can be granted *in full*. Multi-permit requests
// are therefore never partially granted, and a large head request is not
// bypassed, which keeps FIFO/priority guarantees and permit accounting honest.
type waiterQueue struct {
	policy  config.QueuePolicy
	waiters []Waiter
}

func newWaiterQueue(policy config.QueuePolicy) *waiterQueue {
	return &waiterQueue{policy: policy}
}

// less reports whether waiter i must be served before waiter j.
func (q *waiterQueue) less(i, j int) bool {
	a, b := q.waiters[i], q.waiters[j]
	switch q.policy {
	case config.QueuePriority:
		if a.Priority != b.Priority {
			return a.Priority > b.Priority
		}
	}
	if !a.EnqueuedAt.Equal(b.EnqueuedAt) {
		return a.EnqueuedAt.Before(b.EnqueuedAt)
	}
	// Deterministic final tie-break on the unique request id.
	return a.RequestID < b.RequestID
}

// sortLocked orders the queue; stable so equal keys keep insertion order.
func (q *waiterQueue) sortLocked() {
	sort.SliceStable(q.waiters, q.less)
}

func (q *waiterQueue) push(w Waiter) {
	q.waiters = append(q.waiters, w)
	q.sortLocked()
}

// remove deletes the identified waiter and returns it.
func (q *waiterQueue) remove(requestID string) (Waiter, bool) {
	for i, w := range q.waiters {
		if w.RequestID == requestID {
			q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
			return w, true
		}
	}
	return Waiter{}, false
}

func (q *waiterQueue) head() (Waiter, bool) {
	if len(q.waiters) == 0 {
		return Waiter{}, false
	}
	return q.waiters[0], true
}

func (q *waiterQueue) popHead() (Waiter, bool) {
	w, ok := q.head()
	if !ok {
		return Waiter{}, false
	}
	q.waiters = q.waiters[1:]
	return w, true
}

func (q *waiterQueue) get(requestID string) (Waiter, bool) {
	for _, w := range q.waiters {
		if w.RequestID == requestID {
			return w, true
		}
	}
	return Waiter{}, false
}

func (q *waiterQueue) len() int { return len(q.waiters) }

func (q *waiterQueue) snapshot() []Waiter {
	if len(q.waiters) == 0 {
		return nil
	}
	out := make([]Waiter, len(q.waiters))
	copy(out, q.waiters)
	return out
}

// restore replaces queue contents (used during event replay/snapshot load).
func (q *waiterQueue) restore(ws []Waiter) {
	q.waiters = nil
	if len(ws) > 0 {
		q.waiters = append(q.waiters, ws...)
		q.sortLocked()
	}
}

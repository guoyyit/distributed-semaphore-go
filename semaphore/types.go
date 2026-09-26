package semaphore

import (
	"errors"
	"time"
)

// Strategy selects how queued waiters are served.
type Strategy int

const (
	// FIFO serves waiters strictly in arrival order.
	FIFO Strategy = iota
	// Priority serves waiters by descending priority; ties keep arrival order.
	Priority
)

// Errors returned by semaphore operations.
var (
	ErrStaleFencingToken  = errors.New("semaphore: stale fencing token")
	ErrWaiterNotFound     = errors.New("semaphore: waiter not found")
	ErrNoRoute            = errors.New("semaphore: no owner instance for semaphore")
	ErrClosed             = errors.New("semaphore: service closed")
	ErrAcquireCanceled    = errors.New("semaphore: acquire canceled")
	ErrAcquireTimeout     = errors.New("semaphore: acquire timed out")
	ErrInvalidPermitCount = errors.New("semaphore: permit count must be positive")
	ErrUnknownSemaphore   = errors.New("semaphore: unknown semaphore")
)

// Lease is the result of a successful acquire.
type Lease struct {
	Semaphore string
	HolderID  string
	Fence     int64
	Count     int
	ExpiresAt time.Time
}

// Holder describes one granted permit set inside the owner state.
type Holder struct {
	ID        string
	WaiterID  string // non-empty when granted from the wait queue
	Fence     int64
	Count     int
	ExpiresAt int64 // unix nanos
}

// Waiter describes one queued acquire request.
type Waiter struct {
	ID       string
	HolderID string // holder id to use when the request is granted
	Count    int
	Priority int
	Deadline int64 // unix nanos; zero means no deadline
	Seq      int64 // arrival sequence
}

// Snapshot is the durable/restorable state of one named semaphore.
type Snapshot struct {
	Name     string
	Permits  int
	Strategy Strategy
	FenceSeq int64
	Seq      int64 // last applied event sequence
	Holders  []Holder
	Waiters  []Waiter
	Notified []int64 // fences of queued grants already delivered
}

// eventType enumerates durable state transitions.
type eventType string

const (
	evCreated  eventType = "created"
	evAcquired eventType = "acquired"
	evReleased eventType = "released"
	evRenewed  eventType = "renewed"
	evQueued   eventType = "queued"
	evDequeued eventType = "dequeued"
	evGranted  eventType = "granted"
	evNotified eventType = "notified"
)

// usedCount returns permits currently held (test/inspection helper).
func (s Snapshot) usedCount() int {
	n := 0
	for _, h := range s.Holders {
		n += h.Count
	}
	return n
}

// Event is one durable state transition record.
type Event struct {
	Seq       int64     `json:"seq"`
	At        int64     `json:"at"`
	Type      eventType `json:"type"`
	Name      string    `json:"name"`
	HolderID  string    `json:"holder_id,omitempty"`
	Fence     int64     `json:"fence,omitempty"`
	OldFence  int64     `json:"old_fence,omitempty"`
	Count     int       `json:"count,omitempty"`
	Permits   int       `json:"permits,omitempty"`
	Strategy  Strategy  `json:"strategy,omitempty"`
	WaiterID  string    `json:"waiter_id,omitempty"`
	Priority  int       `json:"priority,omitempty"`
	Deadline  int64     `json:"deadline,omitempty"`
	WaiterSeq int64     `json:"waiter_seq,omitempty"`
	ExpiresAt int64     `json:"expires_at,omitempty"`
}

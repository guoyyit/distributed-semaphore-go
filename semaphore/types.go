package semaphore

import "time"

// Grant is one held permit allocation.
type Grant struct {
	Semaphore string
	RequestID string
	HolderID  string
	Token     uint64
	Permits   int
	ExpiresAt time.Time
}

// Waiter is a queued acquisition request.
type Waiter struct {
	Semaphore  string
	RequestID  string
	HolderID   string
	Permits    int
	Priority   int
	EnqueuedAt time.Time
	// TTL is the queue wait timeout; LeaseTTL is the lease granted on promotion.
	TTL      time.Duration
	LeaseTTL time.Duration
	// Zero Deadline means the waiter never times out while queued.
	Deadline time.Time
}

// Stats reports the observable state of one named semaphore.
type Stats struct {
	Name      string
	Capacity  int
	Available int
	Grants    []Grant
	Waiters   []Waiter
}

// Event is a durable state-machine transition.
type Event struct {
	// Seq is the monotonic log sequence number assigned by the cluster layer.
	Seq uint64
	// Type identifies the concrete payload.
	Type EventType
	// Timestamp is the authoritative event time (leader clock at append).
	Timestamp time.Time

	Init     *InitPayload
	Acquired *AcquiredPayload
	Released *ReleasedPayload
	Renewed  *RenewedPayload
	Wait     *WaitPayload
	Cancel   *CancelPayload
	Expired  *ExpiredPayload
	Commit   *CommitPayload
}

// EventType enumerates durable event kinds.
type EventType uint8

const (
	EventUnknown EventType = iota
	EventInit
	EventAcquired
	EventReleased
	EventRenewed
	EventWait
	EventCancel
	EventExpired
	EventCommit
	// EventNoop is a leader-inauguration entry; committing it also commits all
	// preceding entries from earlier terms (standard Raft no-op).
	EventNoop
)

// InitPayload creates a named semaphore with a fixed capacity.
type InitPayload struct {
	Name     string
	Capacity int
	Policy   int // config.QueuePolicy, kept as int for stable JSON encoding
}

// AcquiredPayload records that a request was granted permits.
type AcquiredPayload struct {
	Name      string
	RequestID string
	HolderID  string
	Permits   int
	Token     uint64
	TTL       time.Duration
	ExpiresAt time.Time
}

// ReleasedPayload records that a holder (or expiration) returned permits.
type ReleasedPayload struct {
	Name      string
	RequestID string
	HolderID  string
	Token     uint64
	Permits   int
}

// RenewedPayload records a lease renewal that rotates the fencing token.
type RenewedPayload struct {
	Name      string
	RequestID string
	HolderID  string
	OldToken  uint64
	NewToken  uint64
	NewExpiry time.Time
}

// WaitPayload enqueues an unsatisfiable acquisition.
type WaitPayload struct {
	Name       string
	RequestID  string
	HolderID   string
	Permits    int
	Priority   int
	EnqueuedAt time.Time
	TTL        time.Duration
	LeaseTTL   time.Duration
	Deadline   time.Time
}

// CancelPayload removes a queued request. RequestID is the target waiter;
// OpID is the cancel operation's own id used to route its response.
type CancelPayload struct {
	Name      string
	RequestID string
	OpID      string
}

// ExpiredPayload records expiration of a grant or waiter.
type ExpiredPayload struct {
	Name     string
	HolderID string
	// Token is the grant token at expiry; zero for a waiter expiration.
	Token uint64
	// Permits returned (0 for waiter).
	Permits int
	// RequestID identifies an expired waiter; empty for a grant expiration.
	RequestID string
}

// CommitPayload marks that every event with index <= Index is committed.
// Commit markers are WAL-only control records and never mutate permits.
type CommitPayload struct {
	Index uint64
}

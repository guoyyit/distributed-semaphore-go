package semaphore

import "errors"

// Common errors returned by the semaphore state machine.
var (
	// ErrNotFound means no named semaphore has been initialized.
	ErrNotFound = errors.New("semaphore: not initialized")
	// ErrAlreadyInit means the named semaphore was initialized before.
	ErrAlreadyInit = errors.New("semaphore: already initialized")
	// ErrInvalidPermits means the requested permit count is zero or exceeds capacity.
	ErrInvalidPermits = errors.New("semaphore: invalid permit count")
	// ErrInvalidToken means the fencing token does not match the current holder.
	ErrInvalidToken = errors.New("semaphore: stale or unknown fencing token")
	// ErrNotWaiting means the waiter request id is unknown or no longer queued.
	ErrNotWaiting = errors.New("semaphore: waiter not found")
)

// ErrHolderBusy means a holder already owns a grant on the semaphore.
var ErrHolderBusy = errors.New("semaphore: holder already owns a grant")

// ErrInvalidLease means a non-positive lease TTL was supplied.
var ErrInvalidLease = errors.New("semaphore: lease ttl must be positive")

// ErrDuplicateRequest means a request id already has a terminal/active outcome;
// the caller should attach to the existing outcome instead of appending again.
var ErrDuplicateRequest = errors.New("semaphore: request id already processed")

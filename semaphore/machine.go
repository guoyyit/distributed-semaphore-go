// Package semaphore implements the pure, deterministic state machine for
// named, leased, fencing-token-protected permit allocations.
//
// The machine never mutates state directly on behalf of a client. Callers
// (the cluster leader) first ask for a planned Event via Plan*. The event is
// then durably replicated and applied to every replica via Apply. This makes
// recovery exact: replaying the WAL rebuilds identical state.
package semaphore

import (
	"encoding/json"
	"time"

	"github.com/guoyyit/distributed-semaphore-go/config"
)

// reqState is the lifecycle of a client (requestID) tracked for idempotency
// and for stable response attachment across leader changes / restarts.
type reqState uint8

const (
	reqWaiting reqState = iota
	reqGranted
	reqCanceled
	reqTimedOut
)

// Exported request lifecycle states for cluster-layer reattachment.
const (
	ReqWaiting  = uint8(reqWaiting)
	ReqGranted  = uint8(reqGranted)
	ReqCanceled = uint8(reqCanceled)
	ReqTimedOut = uint8(reqTimedOut)
)

// named is one named semaphore.
type named struct {
	Name      string
	Capacity  int
	Policy    config.QueuePolicy
	Available int
	Grants    map[string]*Grant // holderID -> grant
	Queue     *waiterQueue
	TokenSeq  uint64
	Requests  map[string]reqState
}

// Machine is the state machine for all named semaphores.
// All methods assume the caller serializes access (the node run-loop does).
type Machine struct {
	semas map[string]*named
	// tokenSeq is a global monotonic counter so tokens are unique cluster-wide
	// even across init/drop of individual semaphores.
	tokenSeq uint64
	// outcomes records the durable terminal/active result of every client op
	// keyed by its idempotency id, so retries after timeouts or leader changes
	// re-attach to the original outcome instead of applying the op twice.
	outcomes map[string]OpOutcome
}

// OpOutcome is the durable result of one client operation.
type OpOutcome struct {
	Name      string
	State     uint8 // one of ReqWaiting/ReqGranted/ReqCanceled/ReqTimedOut
	Token     uint64
	ExpiresAt time.Time
}

// NewMachine creates an empty state machine.
func NewMachine() *Machine {
	return &Machine{semas: map[string]*named{}, outcomes: map[string]OpOutcome{}}
}

// OpOutcome returns the durable result of an operation id.
func (m *Machine) OpOutcome(opID string) (OpOutcome, bool) {
	o, ok := m.outcomes[opID]
	return o, ok
}

// setOutcomeLocked records one operation result.
func (m *Machine) setOutcomeLocked(opID string, o OpOutcome) {
	if opID == "" {
		return
	}
	m.outcomes[opID] = o
}

func (m *Machine) nextTokenLocked() uint64 {
	m.tokenSeq++
	return m.tokenSeq
}

// snapshotJSON is the durable compacted image format.
type snapshotJSON struct {
	TokenSeq uint64
	Semas    []snapshotNamed
	Outcomes map[string]OpOutcome
}

type snapshotNamed struct {
	Name      string
	Capacity  int
	Policy    config.QueuePolicy
	Available int
	Grants    []*Grant
	Waiters   []Waiter
	TokenSeq  uint64
	Requests  map[string]reqState
}

// ---------- planning helpers ----------

func (m *Machine) get(name string) (*named, error) {
	s, ok := m.semas[name]
	if !ok {
		return nil, ErrNotFound
	}
	return s, nil
}

func validatePermits(s *named, permits int) error {
	if permits <= 0 || permits > s.Capacity {
		return ErrInvalidPermits
	}
	return nil
}

func event(seq uint64, t time.Time, typ EventType) Event {
	return Event{Seq: seq, Type: typ, Timestamp: t}
}

// PlanInit plans creation of a named semaphore.
func (m *Machine) PlanInit(seq uint64, now time.Time, name string, capacity int, policy config.QueuePolicy) (Event, error) {
	if capacity <= 0 {
		return Event{}, ErrInvalidPermits
	}
	if _, ok := m.semas[name]; ok {
		return Event{}, ErrAlreadyInit
	}
	e := event(seq, now, EventInit)
	e.Init = &InitPayload{Name: name, Capacity: capacity, Policy: int(policy)}
	return e, nil
}

// PlanAcquire plans an atomic acquire: an immediate grant when permits are
// free, otherwise a single enqueue of the full request (never a partial grant).
func (m *Machine) PlanAcquire(seq uint64, now time.Time, name, requestID, holderID string, permits, priority int, leaseTTL, waitTTL time.Duration) (Event, error) {
	s, err := m.get(name)
	if err != nil {
		return Event{}, err
	}
	if err := validatePermits(s, permits); err != nil {
		return Event{}, err
	}
	if leaseTTL <= 0 {
		return Event{}, ErrInvalidLease
	}
	// Idempotent re-delivery of the same request after a leader change:
	// produce no new event; caller must attach to the prior outcome.
	if st, ok := s.Requests[requestID]; ok && st != reqCanceled && st != reqTimedOut {
		return Event{}, ErrDuplicateRequest
	}
	if _, busy := s.Grants[holderID]; busy {
		return Event{}, ErrHolderBusy
	}
	if _, queued := s.Queue.get(requestID); queued {
		return Event{}, ErrDuplicateRequest
	}

	if permits <= s.Available {
		e := event(seq, now, EventAcquired)
		e.Acquired = &AcquiredPayload{
			Name: name, RequestID: requestID, HolderID: holderID,
			Permits: permits, Token: m.tokenSeq + 1,
			TTL: leaseTTL, ExpiresAt: now.Add(leaseTTL),
		}
		return e, nil
	}

	deadline := time.Time{}
	if waitTTL > 0 {
		deadline = now.Add(waitTTL)
	}
	e := event(seq, now, EventWait)
	e.Wait = &WaitPayload{
		Name: name, RequestID: requestID, HolderID: holderID,
		Permits: permits, Priority: priority, EnqueuedAt: now,
		TTL: waitTTL, LeaseTTL: leaseTTL, Deadline: deadline,
	}
	return e, nil
}

// PlanRelease plans a permit return. A non-current token is rejected and never
// touches state; releasing an unknown/already-returned holder is a harmless
// idempotent no-op (returns zero-value event, ok=false).
func (m *Machine) PlanRelease(seq uint64, now time.Time, name, requestID, holderID string, token uint64) (Event, bool, error) {
	if _, known := m.outcomes[requestID]; known {
		return Event{}, false, ErrDuplicateRequest
	}
	s, err := m.get(name)
	if err != nil {
		return Event{}, false, err
	}
	g, ok := s.Grants[holderID]
	if !ok {
		return Event{}, false, nil // already released / expired: idempotent no-op
	}
	if g.Token != token {
		return Event{}, false, ErrInvalidToken
	}
	e := event(seq, now, EventReleased)
	e.Released = &ReleasedPayload{Name: name, RequestID: requestID, HolderID: holderID, Token: token, Permits: g.Permits}
	return e, true, nil
}

// PlanRenew plans a lease renewal. It rotates the fencing token; an old token
// is rejected so a preempted/failed holder can never extend or take back a lease.
func (m *Machine) PlanRenew(seq uint64, now time.Time, name, requestID, holderID string, oldToken uint64, leaseTTL time.Duration) (Event, error) {
	if _, known := m.outcomes[requestID]; known {
		return Event{}, ErrDuplicateRequest
	}
	s, err := m.get(name)
	if err != nil {
		return Event{}, err
	}
	if leaseTTL <= 0 {
		return Event{}, ErrInvalidLease
	}
	g, ok := s.Grants[holderID]
	if !ok {
		return Event{}, ErrInvalidToken
	}
	if g.Token != oldToken {
		return Event{}, ErrInvalidToken
	}
	e := event(seq, now, EventRenewed)
	e.Renewed = &RenewedPayload{
		Name: name, RequestID: requestID, HolderID: holderID, OldToken: oldToken,
		NewToken: m.tokenSeq + 1, NewExpiry: now.Add(leaseTTL),
	}
	return e, nil
}

// PlanCancel plans removal of a queued request. Cancel of an unknown request
// is an idempotent no-op (ok=false); cancel of an already terminal request is
// rejected so a granted request is never silently dropped by a stale cancel.
func (m *Machine) PlanCancel(seq uint64, now time.Time, name, requestID, opID string) (Event, bool, error) {
	if _, known := m.outcomes[opID]; known {
		return Event{}, false, ErrDuplicateRequest
	}
	s, err := m.get(name)
	if err != nil {
		return Event{}, false, err
	}
	if _, queued := s.Queue.get(requestID); !queued {
		if st, known := s.Requests[requestID]; known && st == reqGranted {
			return Event{}, false, ErrInvalidToken
		}
		return Event{}, false, nil
	}
	e := event(seq, now, EventCancel)
	e.Cancel = &CancelPayload{Name: name, RequestID: requestID, OpID: opID}
	return e, true, nil
}

// PlanPump returns head-of-line promotions that are satisfiable right now.
// Multi-permit requests are promoted only as a whole; once the head cannot
// be satisfied the scan stops (no bypass, no starvation-induced accounting
// errors). Promotions append *after* the supplied expiry events so callers can
// first reclaim and then pump in one batch.
func (m *Machine) PlanPump(seq uint64, now time.Time) []Event {
	var out []Event
	nextSeq := seq
	nextToken := m.tokenSeq + 1
	// Deterministic cross-semaphore order keeps token assignment stable.
	names := make([]string, 0, len(m.semas))
	for n := range m.semas {
		names = append(names, n)
	}
	sortStrings(names)
	for _, n := range names {
		s := m.semas[n]
		available := s.Available // purely local simulation; no mutation
		for _, w := range s.Queue.snapshot() {
			if w.Permits > available {
				break // strict head-of-line: never bypass, never partial
			}
			e := event(nextSeq, now, EventAcquired)
			e.Acquired = &AcquiredPayload{
				Name: s.Name, RequestID: w.RequestID, HolderID: w.HolderID,
				Permits: w.Permits, Token: nextToken,
				TTL: w.LeaseTTL, ExpiresAt: now.Add(w.LeaseTTL),
			}
			out = append(out, e)
			available -= w.Permits
			nextToken++
			nextSeq++
		}
	}
	return out
}

// PlanExpirations reaps all grants whose lease has ended and waiters whose wait
// deadline has passed, returning the complete batch of expiry events followed
// by every newly satisfiable promotion. The whole batch must be committed
// atomically; until then no state changes.
func (m *Machine) PlanExpirations(seq uint64, now time.Time) []Event {
	base := m.Snapshot()
	var out []Event
	nextSeq := seq
	// Deterministic expiration order across replicas.
	for _, s := range m.semas {
		holders := make([]string, 0, len(s.Grants))
		for h := range s.Grants {
			holders = append(holders, h)
		}
		sortStrings(holders)
		for _, h := range holders {
			g := s.Grants[h]
			if !g.ExpiresAt.After(now) {
				e := event(nextSeq, now, EventExpired)
				e.Expired = &ExpiredPayload{
					Name: s.Name, HolderID: h, Token: g.Token,
					Permits: g.Permits,
				}
				out = append(out, e)
				m.applyExpired(e)
				nextSeq++
			}
		}
		// Expired waiters: deterministic by request id.
		var expiredWaiters []Waiter
		for _, w := range s.Queue.snapshot() {
			if !w.Deadline.IsZero() && !w.Deadline.After(now) {
				expiredWaiters = append(expiredWaiters, w)
			}
		}
		sortWaiters(expiredWaiters)
		for _, w := range expiredWaiters {
			e := event(nextSeq, now, EventExpired)
			e.Expired = &ExpiredPayload{Name: s.Name, RequestID: w.RequestID}
			out = append(out, e)
			m.applyExpired(e)
			nextSeq++
		}
	}
	out = append(out, m.PlanPump(nextSeq, now)...)
	if err := m.Restore(base); err != nil {
		panic("state machine snapshot rollback failed: " + err.Error())
	}
	return out
}

func sortStrings(x []string) {
	for i := 1; i < len(x); i++ {
		for j := i; j > 0 && x[j-1] > x[j]; j-- {
			x[j-1], x[j] = x[j], x[j-1]
		}
	}
}

func sortWaiters(ws []Waiter) {
	for i := 1; i < len(ws); i++ {
		for j := i; j > 0 && ws[j-1].RequestID > ws[j].RequestID; j-- {
			ws[j-1], ws[j] = ws[j], ws[j-1]
		}
	}
}

// ---------- apply ----------

// Apply commits one durable event. Apply is idempotent-safe for events that may
// be re-sent during replication retries only at the cluster level; within a
// single machine each unique event must be applied once (guaranteed by index).
func (m *Machine) Apply(e Event) error {
	switch e.Type {
	case EventInit:
		return m.applyInit(e)
	case EventAcquired:
		m.applyAcquired(e)
		return nil
	case EventReleased:
		m.applyReleased(e)
		return nil
	case EventRenewed:
		m.applyRenewed(e)
		return nil
	case EventWait:
		m.applyWait(e)
		return nil
	case EventCancel:
		m.applyCancel(e)
		return nil
	case EventExpired:
		m.applyExpired(e)
		return nil
	case EventCommit:
		return nil // control record
	case EventNoop:
		return nil // leader inauguration
	default:
		return ErrNotFound
	}
}

func (m *Machine) applyInit(e Event) error {
	p := e.Init
	if p == nil {
		return ErrNotFound
	}
	if _, ok := m.semas[p.Name]; ok {
		return nil // replay duplicate: keep first
	}
	m.setOutcomeLocked("init:"+p.Name, OpOutcome{Name: p.Name, State: ReqGranted})
	m.semas[p.Name] = &named{
		Name: p.Name, Capacity: p.Capacity,
		Policy: config.QueuePolicy(p.Policy), Available: p.Capacity,
		Grants:   map[string]*Grant{},
		Queue:    newWaiterQueue(config.QueuePolicy(p.Policy)),
		Requests: map[string]reqState{},
	}
	return nil
}

func (m *Machine) applyAcquired(e Event) {
	p := e.Acquired
	if p == nil {
		return
	}
	s, ok := m.semas[p.Name]
	if !ok {
		return
	}
	// Deduplicate by request id; ensure token counter stays monotonic.
	if st, seen := s.Requests[p.RequestID]; seen && st == reqGranted {
		return
	}
	if _, exists := s.Grants[p.HolderID]; exists {
		return
	}
	if p.Token > m.tokenSeq {
		m.tokenSeq = p.Token
	}
	if s.TokenSeq < p.Token {
		s.TokenSeq = p.Token
	}
	s.Available -= p.Permits
	s.Queue.remove(p.RequestID)
	s.Grants[p.HolderID] = &Grant{
		Semaphore: p.Name, RequestID: p.RequestID, HolderID: p.HolderID,
		Token: p.Token, Permits: p.Permits, ExpiresAt: p.ExpiresAt,
	}
	s.Requests[p.RequestID] = reqGranted
	m.setOutcomeLocked(p.RequestID, OpOutcome{Name: p.Name, State: ReqGranted, Token: p.Token, ExpiresAt: p.ExpiresAt})
}

func (m *Machine) applyReleased(e Event) {
	p := e.Released
	if p == nil {
		return
	}
	s, ok := m.semas[p.Name]
	if !ok {
		return
	}
	g, ok := s.Grants[p.HolderID]
	if !ok || g.Token != p.Token {
		return // stale / duplicate: no double credit
	}
	delete(s.Grants, p.HolderID)
	s.Available += g.Permits
	if s.Available > s.Capacity {
		s.Available = s.Capacity
	}
	// Keep the request record briefly so a late duplicate release is a no-op;
	// it is safe to delete as the grant is gone and holderID->grant map is the
	// authoritative release guard.
	m.setOutcomeLocked(p.RequestID, OpOutcome{Name: p.Name, State: ReqGranted})
}

func (m *Machine) applyRenewed(e Event) {
	p := e.Renewed
	if p == nil {
		return
	}
	s, ok := m.semas[p.Name]
	if !ok {
		return
	}
	g, ok := s.Grants[p.HolderID]
	if !ok || g.Token != p.OldToken {
		return // stale renewal after preemption: ignore
	}
	if p.NewToken > m.tokenSeq {
		m.tokenSeq = p.NewToken
	}
	g.Token = p.NewToken
	g.ExpiresAt = p.NewExpiry
	m.setOutcomeLocked(p.RequestID, OpOutcome{Name: p.Name, State: ReqGranted, Token: p.NewToken, ExpiresAt: p.NewExpiry})
}

func (m *Machine) applyWait(e Event) {
	p := e.Wait
	if p == nil {
		return
	}
	s, ok := m.semas[p.Name]
	if !ok {
		return
	}
	if _, seen := s.Requests[p.RequestID]; seen {
		return
	}
	if _, queued := s.Queue.get(p.RequestID); queued {
		return
	}
	s.Queue.push(Waiter{
		Semaphore: p.Name, RequestID: p.RequestID, HolderID: p.HolderID,
		Permits: p.Permits, Priority: p.Priority, EnqueuedAt: p.EnqueuedAt,
		TTL: p.TTL, LeaseTTL: p.LeaseTTL, Deadline: p.Deadline,
	})
	s.Requests[p.RequestID] = reqWaiting
	m.setOutcomeLocked(p.RequestID, OpOutcome{Name: p.Name, State: ReqWaiting})
}

func (m *Machine) applyCancel(e Event) {
	p := e.Cancel
	if p == nil {
		return
	}
	s, ok := m.semas[p.Name]
	if !ok {
		return
	}
	if _, ok := s.Queue.remove(p.RequestID); ok {
		s.Requests[p.RequestID] = reqCanceled
	}
	m.setOutcomeLocked(p.OpID, OpOutcome{Name: p.Name, State: ReqCanceled})
	// The acquisition itself is now terminal-canceled for reattachment.
	m.setOutcomeLocked(p.RequestID, OpOutcome{Name: p.Name, State: ReqCanceled})
}

func (m *Machine) applyExpired(e Event) {
	p := e.Expired
	if p == nil {
		return
	}
	s, ok := m.semas[p.Name]
	if !ok {
		return
	}
	if p.RequestID != "" {
		// Waiter expiration.
		if _, removed := s.Queue.remove(p.RequestID); removed {
			s.Requests[p.RequestID] = reqTimedOut
			m.setOutcomeLocked(p.RequestID, OpOutcome{Name: p.Name, State: ReqTimedOut})
		}
		return
	}
	g, ok := s.Grants[p.HolderID]
	if !ok || g.Token != p.Token {
		return // already released/renewed past this token: never double-credit
	}
	delete(s.Grants, p.HolderID)
	s.Available += g.Permits
	if s.Available > s.Capacity {
		s.Available = s.Capacity
	}
	delete(s.Requests, g.RequestID)
}

// ---------- snapshot / restore / reads ----------

// Snapshot returns a JSON image of the full machine.
func (m *Machine) Snapshot() []byte {
	img := snapshotJSON{TokenSeq: m.tokenSeq, Outcomes: map[string]OpOutcome{}}
	for k, v := range m.outcomes {
		img.Outcomes[k] = v
	}
	for _, s := range m.semas {
		sn := snapshotNamed{
			Name: s.Name, Capacity: s.Capacity, Policy: s.Policy,
			Available: s.Available, Waiters: s.Queue.snapshot(),
			TokenSeq: s.TokenSeq, Requests: map[string]reqState{},
		}
		for _, g := range s.Grants {
			g2 := *g
			sn.Grants = append(sn.Grants, &g2)
		}
		for k, v := range s.Requests {
			sn.Requests[k] = v
		}
		img.Semas = append(img.Semas, sn)
	}
	data, err := json.Marshal(img)
	if err != nil {
		panic(err)
	}
	return data
}

// Restore rebuilds state from a Snapshot image.
func (m *Machine) Restore(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	var img snapshotJSON
	if err := json.Unmarshal(data, &img); err != nil {
		return err
	}
	m.tokenSeq = img.TokenSeq
	m.semas = map[string]*named{}
	m.outcomes = map[string]OpOutcome{}
	for k, v := range img.Outcomes {
		m.outcomes[k] = v
	}
	for _, sn := range img.Semas {
		s := &named{
			Name: sn.Name, Capacity: sn.Capacity, Policy: sn.Policy,
			Available: sn.Available, TokenSeq: sn.TokenSeq,
			Grants:   map[string]*Grant{},
			Queue:    newWaiterQueue(sn.Policy),
			Requests: map[string]reqState{},
		}
		for _, g := range sn.Grants {
			g2 := *g
			s.Grants[g2.HolderID] = &g2
		}
		for _, w := range sn.Waiters {
			s.Queue.push(w)
		}
		for k, v := range sn.Requests {
			s.Requests[k] = v
		}
		m.semas[s.Name] = s
	}
	return nil
}

// Stats returns a consistent copy of one semaphore's observable state.
func (m *Machine) Stats(name string) (Stats, error) {
	s, err := m.get(name)
	if err != nil {
		return Stats{}, err
	}
	st := Stats{Name: s.Name, Capacity: s.Capacity, Available: s.Available, Waiters: s.Queue.snapshot()}
	for _, g := range s.Grants {
		st.Grants = append(st.Grants, *g)
	}
	return st, nil
}

// Grant returns the current grant for a holder (used to attach responses
// after leader changes / replay).
func (m *Machine) Grant(name, holderID string) (Grant, bool) {
	s, err := m.get(name)
	if err != nil {
		return Grant{}, false
	}
	g, ok := s.Grants[holderID]
	if !ok {
		return Grant{}, false
	}
	return *g, true
}

// Waiter returns a queued request by id.
func (m *Machine) Waiter(name, requestID string) (Waiter, bool) {
	s, err := m.get(name)
	if err != nil {
		return Waiter{}, false
	}
	return s.Queue.get(requestID)
}

// RequestState returns the persisted lifecycle state of a request id.
func (m *Machine) RequestState(name, requestID string) (uint8, bool) {
	s, err := m.get(name)
	if err != nil {
		return 0, false
	}
	st, ok := s.Requests[requestID]
	return uint8(st), ok
}

// PendingRequest reports whether a request is still attached (waiting/granted).
func (m *Machine) PendingRequest(name, requestID string) bool {
	st, ok := m.RequestState(name, requestID)
	return ok && (st == uint8(reqWaiting) || st == uint8(reqGranted))
}

// GrantByRequest returns the grant for a persisted request id, allowing a
// retried acquire to re-attach to its original outcome after a leader change.
func (m *Machine) GrantByRequest(name, requestID string) (Grant, bool) {
	s, err := m.get(name)
	if err != nil {
		return Grant{}, false
	}
	for _, g := range s.Grants {
		if g.RequestID == requestID {
			return *g, true
		}
	}
	return Grant{}, false
}

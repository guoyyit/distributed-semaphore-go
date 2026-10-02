package semaphore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"
)

// Config configures an Instance.
type Config struct {
	// InstanceID uniquely identifies this instance in the simulated cluster.
	InstanceID string
	// DataDir is the WAL/snapshot directory; "" means in-memory (non-durable).
	DataDir string
	// DefaultLeaseTTL is granted on every acquire/grant.
	DefaultLeaseTTL time.Duration
	// TickInterval drives lease/waiter expiry processing.
	TickInterval time.Duration
	// SnapshotAfter compacts the WAL after this many new events (0 = never).
	SnapshotAfter int
	// Clock overrides time for tests.
	Clock Clock
}

// request is the wire payload for inter-instance RPCs.
type request struct {
	op       string // create|acquire|release|renew|cancel|preempt
	From     string // requesting instance id
	name     string
	holderID string
	waiterID string
	fence    int64
	count    int
	priority int
	deadline int64
	ttl      time.Duration
	permits  int
	strategy Strategy
}

// pendingAcquire is a local caller blocked waiting for a queue grant.
type pendingAcquire struct {
	leaseCh chan *Lease
	errCh   chan error
	owner   string
}

// Instance is one node of the simulated cluster. Every named semaphore has a
// deterministic owner instance (hash of the name); acquire/release/renew are
// routed to the owner over the in-process bus. The owner durably logs every
// transition, so any instance restart can recover granted permits and the
// wait queue without double granting or losing available permits.
type Instance struct {
	cfg     Config
	bus     *bus
	engine  *engine
	w       *wal
	journal *journal

	inbox chan message

	// Per-name ordered execution: every message targeting one semaphore is
	// processed by one dedicated goroutine, preserving FIFO request order
	// for that name while different names never block each other.
	queuesMu sync.Mutex
	queues   map[string]*nameQueue

	waitsMu sync.Mutex
	waits   map[string]*pendingAcquire // waiter id -> local waiter

	hbMu sync.Mutex
	hb   *heartbeatState

	closeOnce sync.Once
	closed    chan struct{}
}

// NewBus creates the shared in-process simulated network.
func NewBus() *bus { return newBus() }

// NewInstance creates an instance, joins the bus, and recovers durable state.
func NewInstance(cfg Config, b *bus) (*Instance, error) {
	if cfg.InstanceID == "" {
		return nil, fmt.Errorf("semaphore: InstanceID required")
	}
	if cfg.DefaultLeaseTTL <= 0 {
		cfg.DefaultLeaseTTL = 10 * time.Second
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 25 * time.Millisecond
	}
	if cfg.Clock == nil {
		cfg.Clock = realClock{}
	}
	w, err := openWAL(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	in := &Instance{
		cfg:    cfg,
		bus:    b,
		engine: newEngine(),
		w:      w,
		inbox:  make(chan message, 1024),
		queues: make(map[string]*nameQueue),
		waits:  make(map[string]*pendingAcquire),
		hb:     newHeartbeatState(),
		closed: make(chan struct{}),
	}
	if cfg.SnapshotAfter > 0 {
		in.engine.compact = func(name string, snap Snapshot) {
			in.maybeCompact(name, snap)
		}
	}
	if w.dir != "" {
		j := newJournal(
			func(name string, evs []Event) error { return w.appendNameEvents(name, evs) },
			in.maybeCompactJournal,
		)
		in.engine.setJournal(j)
		in.journal = j
	}
	if err := in.recoverState(); err != nil {
		w.Close()
		return nil, err
	}
	b.register(cfg.InstanceID, in.inbox)
	go in.serve()
	go in.tickLoop()
	return in, nil
}

// ---- routing ----

func (b *bus) ownerOf(members []string, name string) string {
	if len(members) == 0 {
		return ""
	}
	h := fnv.New32a()
	h.Write([]byte(name))
	return members[int(h.Sum32())%len(members)]
}

func (i *Instance) ownerFor(name string) string {
	return i.bus.ownerOf(i.bus.members(), name)
}

func (i *Instance) isOwner(name string) bool { return i.ownerFor(name) == i.cfg.InstanceID }

// ---- persistence glue ----

func (i *Instance) save(evs []Event) error {
	return i.w.append(evs)
}

// maybeCompact writes a snapshot once the post-snapshot log crosses the
// configured threshold. Invoked inside the engine lock, so it must not call
// back into the engine.
func (i *Instance) maybeCompact(name string, snap Snapshot) {
	if i.cfg.SnapshotAfter <= 0 {
		return
	}
	if _, evs, err := i.w.readNameExported(name); err == nil && len(evs) >= i.cfg.SnapshotAfter {
		_ = i.w.saveSnapshot(snap)
	}
}

// maybeCompactJournal runs on the per-name journal writer at each drain
// point: no appends can interleave, so snapshot + log rewrite stay
// consistent with everything just flushed.
func (i *Instance) maybeCompactJournal(name string) {
	if i.cfg.SnapshotAfter <= 0 {
		return
	}
	if n, err := i.w.tailLenLocked(name); err == nil && n >= i.cfg.SnapshotAfter {
		if snap, ok := i.engine.snapshotAtCompaction(name); ok {
			_ = i.w.compactLocked(snap)
		}
	}
}

func (i *Instance) recoverState() error {
	snaps, evs, err := i.w.loadAll()
	if err != nil {
		return err
	}
	for k := range snaps {
		i.engine.restore(snaps[k])
	}
	// Ensure events apply in per-name sequence order.
	sort.SliceStable(evs, func(a, b int) bool {
		if evs[a].Name != evs[b].Name {
			return evs[a].Name < evs[b].Name
		}
		return evs[a].Seq < evs[b].Seq
	})
	for _, ev := range evs {
		if ev.Type == evCreated && !i.engine.exists(ev.Name) {
			i.engine.apply(ev)
			continue
		}
		if !i.engine.exists(ev.Name) {
			continue
		}
		i.engine.apply(ev)
	}
	// Re-deliver grants whose notification never got confirmed.
	for _, g := range i.engine.pendingGrants() {
		gg := g
		go i.deliverGrant(gg)
	}
	return nil
}

// ---- public API ----

// EnsureSemaphore creates a named semaphore with the given permit limit and
// queue strategy. It is idempotent for identical parameters.
func (i *Instance) EnsureSemaphore(ctx context.Context, name string, permits int, strategy Strategy) error {
	if i.isClosed() {
		return ErrClosed
	}
	owner := i.ownerFor(name)
	req := &request{op: "create", From: i.cfg.InstanceID, name: name, permits: permits, strategy: strategy}
	if owner == i.cfg.InstanceID {
		_, err := i.engine.create(name, permits, strategy, i.nowNanos(), i.save)
		return err
	}
	r := i.bus.send(ctx, owner, req)
	return r.err
}

// AcquireOptions customizes an acquire call.
type AcquireOptions struct {
	// Count permits to take atomically (default 1).
	Count int
	// Priority only matters under the Priority queue strategy.
	Priority int
	// Timeout bounds queue waiting; zero waits until ctx.
	Timeout time.Duration
}

// Acquire obtains opts.Count permits for name. Under insufficient permits the
// whole request queues (never partially granted). The returned Lease carries
// a unique fencing token and a lease deadline; renew before expiry or the
// permits are reclaimed automatically.
func (i *Instance) Acquire(ctx context.Context, name string, opts AcquireOptions) (*Lease, error) {
	if i.isClosed() {
		return nil, ErrClosed
	}
	if opts.Count <= 0 {
		opts.Count = 1
	}
	now := i.nowNanos()
	ttl := i.cfg.DefaultLeaseTTL
	deadline := int64(0)
	if opts.Timeout > 0 {
		deadline = now + opts.Timeout.Nanoseconds()
	}
	holderID := i.newID("h")
	waiterID := i.newID("w")
	req := &request{
		op: "acquire", From: i.cfg.InstanceID, name: name,
		holderID: holderID, waiterID: waiterID, count: opts.Count,
		priority: opts.Priority, deadline: deadline, ttl: ttl,
	}

	owner := i.ownerFor(name)
	if owner == i.cfg.InstanceID {
		return i.acquireLocal(ctx, req)
	}
	return i.acquireRemote(ctx, owner, req)
}

func (i *Instance) acquireLocal(ctx context.Context, req *request) (*Lease, error) {
	pa := i.registerWait(req.waiterID, i.cfg.InstanceID)
	evs, err := i.engine.acquire(req.name, req.holderID, req.waiterID,
		req.count, req.priority, req.deadline, i.nowNanos(), req.ttl.Nanoseconds(), i.save)
	if err != nil {
		i.unregisterWait(req.waiterID)
		return nil, err
	}
	if ls := leasesFromEvents(evs); len(ls) > 0 {
		l := ls[0]
		i.unregisterWait(req.waiterID)
		i.startLeaseHeartbeat(&l)
		return &l, nil
	}
	return i.awaitRegistered(ctx, req.waiterID, pa)
}

func (i *Instance) acquireRemote(ctx context.Context, owner string, req *request) (*Lease, error) {
	// Register the waiter before sending so a racing grant notification
	// cannot be missed.
	pa := i.registerWait(req.waiterID, owner)
	r := i.bus.send(ctx, owner, req)
	if r.err != nil {
		i.unregisterWait(req.waiterID)
		// The message may already have reached the owner: it could have
		// directly granted holderID or queued waiterID and later granted
		// it. Compensate both idempotently with bounded retries.
		i.scheduleAbortedAcquireCleanup(req, owner)
		return nil, r.err
	}
	if r.lease != nil {
		// Direct grant: registration is no longer needed.
		i.unregisterWait(req.waiterID)
		l := *r.lease
		i.startLeaseHeartbeat(&l)
		return &l, nil
	}
	// Queued: keep the local registration for the async grant push.
	return i.awaitRegistered(ctx, req.waiterID, pa)
}

// awaitGrant registers locally and waits (local-owner queue path).
func (i *Instance) awaitGrant(ctx context.Context, req *request, owner string) (*Lease, error) {
	pa := i.registerWait(req.waiterID, owner)
	return i.awaitRegistered(ctx, req.waiterID, pa)
}

func (i *Instance) awaitRegistered(ctx context.Context, waiterID string, pa *pendingAcquire) (*Lease, error) {
	select {
	case l := <-pa.leaseCh:
		i.startLeaseHeartbeat(l)
		return l, nil
	case err := <-pa.errCh:
		return nil, err
	case <-ctx.Done():
		// Remove the queue slot on the owner first, then unregister locally.
		// Ordering matters for the grant/cancel race:
		//   - a grant delivered before unregister lands in leaseCh and is
		//     drained below and released;
		//   - a grant delivered after unregister cannot find the waiter, so
		//     the receiver auto-releases it.
		// Either way no permit is leaked.
		i.cancelWait(waiterID, pa.owner)
		i.scheduleCancelRetry(waiterID, pa.owner)
		i.unregisterWait(waiterID)
		for {
			select {
			case l := <-pa.leaseCh:
				if l != nil {
					_ = i.Release(context.Background(), l)
				}
			default:
				return nil, mapCtxErr(ctx.Err())
			}
		}
	case <-i.closed:
		i.unregisterWait(waiterID)
		return nil, ErrClosed
	}
}

// Release returns a holder's permits. It is idempotent: releasing twice,
// releasing a reclaimed/preempted holder never inflates available permits or
// spuriously wakes a waiter. A stale fencing token is rejected.
func (i *Instance) Release(ctx context.Context, l *Lease) error {
	if i.isClosed() {
		return ErrClosed
	}
	if l == nil {
		return nil
	}
	owner := i.ownerFor(l.Semaphore)
	req := &request{op: "release", From: i.cfg.InstanceID, name: l.Semaphore, holderID: l.HolderID, fence: l.Fence}
	if owner == i.cfg.InstanceID {
		evs, _, err := i.engine.release(l.Semaphore, l.HolderID, l.Fence, i.nowNanos(),
			i.cfg.DefaultLeaseTTL.Nanoseconds(), i.save)
		if err == nil {
			i.notifyGrants(evs)
		}
		return err
	}
	r := i.bus.send(ctx, owner, req)
	if r.err == nil {
		i.stopLeaseHeartbeat(l.HolderID)
	}
	return r.err
}

// Renew extends a lease and returns a fresh Lease carrying the new fencing
// token. The old token is invalid immediately; subsequent Release/Renew must
// use the returned lease.
func (i *Instance) Renew(ctx context.Context, l *Lease, ttl time.Duration) (*Lease, error) {
	if i.isClosed() {
		return nil, ErrClosed
	}
	if l == nil {
		return nil, ErrStaleFencingToken
	}
	if ttl <= 0 {
		ttl = i.cfg.DefaultLeaseTTL
	}
	deadline := i.nowNanos() + ttl.Nanoseconds()
	owner := i.ownerFor(l.Semaphore)
	req := &request{op: "renew", From: i.cfg.InstanceID, name: l.Semaphore, holderID: l.HolderID, fence: l.Fence, ttl: ttl}
	if owner == i.cfg.InstanceID {
		ev, err := i.engine.renew(l.Semaphore, l.HolderID, l.Fence, deadline, i.nowNanos(), i.save)
		if err != nil {
			return nil, err
		}
		nl := &Lease{Semaphore: l.Semaphore, HolderID: l.HolderID, Fence: ev.Fence, Count: ev.Count, ExpiresAt: time.Unix(0, ev.ExpiresAt)}
		i.replaceHeartbeat(l.HolderID, nl)
		return nl, nil
	}
	r := i.bus.send(ctx, owner, req)
	if r.err != nil {
		return nil, r.err
	}
	i.replaceHeartbeat(l.HolderID, r.lease)
	return r.lease, nil
}

// Preempt forcibly revokes holders so queued (typically high-priority)
// waiters can be admitted in the same atomic transition. Preempted holders'
// fencing tokens are invalid immediately; their later release/renew is
// rejected and cannot affect other holders.
func (i *Instance) Preempt(ctx context.Context, name string, holderIDs []string) error {
	if i.isClosed() {
		return ErrClosed
	}
	owner := i.ownerFor(name)
	req := &request{op: "preempt", From: i.cfg.InstanceID, name: name, holderID: joinIDs(holderIDs)}
	if owner == i.cfg.InstanceID {
		evs, err := i.engine.preempt(name, holderIDs, i.nowNanos(),
			i.cfg.DefaultLeaseTTL.Nanoseconds(), i.save)
		if err != nil {
			return err
		}
		i.notifyGrants(evs)
		return nil
	}
	return i.bus.send(ctx, owner, req).err
}

// Close stops background loops and flushes durable state.
func (i *Instance) Close() error {
	var err error
	i.closeOnce.Do(func() {
		close(i.closed)
		i.bus.unregister(i.cfg.InstanceID)
		if i.journal != nil {
			i.journal.close()
		}
		err = i.w.Close()
	})
	return err
}

func (i *Instance) isClosed() bool {
	select {
	case <-i.closed:
		return true
	default:
		return false
	}
}

// ---- wait registry ----

func (i *Instance) registerWait(waiterID, owner string) *pendingAcquire {
	pa := &pendingAcquire{leaseCh: make(chan *Lease, 1), errCh: make(chan error, 1), owner: owner}
	i.waitsMu.Lock()
	i.waits[waiterID] = pa
	i.waitsMu.Unlock()
	return pa
}

func (i *Instance) unregisterWait(waiterID string) {
	i.waitsMu.Lock()
	delete(i.waits, waiterID)
	i.waitsMu.Unlock()
}

func (i *Instance) lookupWait(waiterID string) *pendingAcquire {
	i.waitsMu.Lock()
	defer i.waitsMu.Unlock()
	return i.waits[waiterID]
}

func (i *Instance) cancelWait(waiterID, owner string) {
	req := &request{op: "cancel", From: i.cfg.InstanceID, name: "", waiterID: waiterID}
	// name is resolved by the owner's waiters table; route to recorded owner.
	i.bus.send(context.Background(), owner, req)
}

// scheduleAbortedAcquireCleanup covers the race where an acquire request
// reached and was processed by the owner (direct grant or queued grant) just
// as the client gave up. It cancels the waiter slot and idempotently returns
// any permits the owner ended up granting to this holder.
func (i *Instance) scheduleAbortedAcquireCleanup(req *request, owner string) {
	go func() {
		for n := 0; n < 8; n++ {
			if i.isClosed() {
				return
			}
			cancelReq := &request{op: "cancel", From: i.cfg.InstanceID, waiterID: req.waiterID}
			i.bus.send(context.Background(), owner, cancelReq)
			// release by holder id with a wildcard fence: the owner's
			// release-by-holder op validates identity without a known fence.
			relReq := &request{op: "releaseholder", From: i.cfg.InstanceID, name: req.name, holderID: req.holderID}
			i.bus.send(context.Background(), owner, relReq)
			time.Sleep(i.cfg.TickInterval)
		}
	}()
}

// ---- owner-side server loop ----

// msgName resolves the semaphore a message targets. Cancel messages carry
// no name; the owner resolves it through the live waiter index.
func (i *Instance) msgName(m message) string {
	if m.req.name != "" {
		return m.req.name
	}
	if m.req.op == "cancel" {
		return i.engine.waiterOwner(m.req.waiterID)
	}
	return ""
}

func (i *Instance) serve() {
	for {
		select {
		case <-i.closed:
			return
		case m := <-i.inbox:
			// Client-targeted control messages (grant push, waiter timeout)
			// never touch owner state: answer immediately.
			if m.req.op == "notify" || m.req.op == "waittimeout" {
				i.handle(m)
				continue
			}
			name := i.msgName(m)
			if name == "" {
				// Unknown target (e.g. already-canceled waiter): idempotent.
				i.handle(m)
				continue
			}
			i.dispatch(name, m)
		}
	}
}

func (i *Instance) dispatch(name string, m message) {
	i.queuesMu.Lock()
	q, ok := i.queues[name]
	if !ok {
		q = &nameQueue{wake: make(chan struct{}, 1)}
		i.queues[name] = q
		go i.serveName(name, q)
	}
	i.queuesMu.Unlock()
	if !q.push(m) {
		m.res <- response{err: ErrClosed}
	}
}

// nameQueue is an unbounded, single-consumer FIFO. push never blocks the
// central serve loop, yet messages for one name are consumed in order.
type nameQueue struct {
	mu     sync.Mutex
	closed bool
	buf    []message
	wake   chan struct{}
}

func (q *nameQueue) push(m message) bool {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return false
	}
	q.buf = append(q.buf, m)
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
	return true
}

func (q *nameQueue) drainOne() (message, bool) {
	q.mu.Lock()
	if len(q.buf) > 0 {
		m := q.buf[0]
		q.buf = q.buf[1:]
		q.mu.Unlock()
		return m, true
	}
	q.mu.Unlock()
	return message{}, false
}

func (i *Instance) serveName(name string, q *nameQueue) {
	for {
		select {
		case <-i.closed:
			return
		case <-q.wake:
			for {
				m, ok := q.drainOne()
				if !ok {
					break
				}
				i.handle(m)
			}
		}
	}
}

func (i *Instance) handle(m message) {
	req := m.req
	switch req.op {
	case "create":
		_, err := i.engine.create(req.name, req.permits, req.strategy, i.nowNanos(), i.save)
		m.res <- response{ack: err == nil, err: err}
	case "acquire":
		evs, err := i.engine.acquire(req.name, req.holderID, req.waiterID,
			req.count, req.priority, req.deadline, i.nowNanos(), req.ttl.Nanoseconds(), i.save)
		if err != nil {
			m.res <- response{err: err}
			return
		}
		if ls := leasesFromEvents(evs); len(ls) > 0 {
			l := ls[0]
			m.res <- response{lease: &l}
			// Direct grants do not need waiter delivery confirmation.
			return
		}
		m.res <- response{ack: true} // queued; grant pushed via notify
	case "release":
		evs, idem, err := i.engine.release(req.name, req.holderID, req.fence, i.nowNanos(),
			i.cfg.DefaultLeaseTTL.Nanoseconds(), i.save)
		if err != nil {
			m.res <- response{err: err}
			return
		}
		m.res <- response{ack: idem}
		i.notifyGrants(evs)
	case "releaseholder":
		// Compensating release by holder identity (acquire abandoned on the
		// client side). Idempotent; wakes waiters atomically.
		if evs, idem, err := i.engine.releaseHolder(req.name, req.holderID, i.nowNanos(),
			i.cfg.DefaultLeaseTTL.Nanoseconds(), i.save); err != nil {
			m.res <- response{err: err}
		} else {
			m.res <- response{ack: idem}
			i.notifyGrants(evs)
		}
	case "renew":
		deadline := i.nowNanos() + req.ttl.Nanoseconds()
		ev, err := i.engine.renew(req.name, req.holderID, req.fence, deadline, i.nowNanos(), i.save)
		if err != nil {
			m.res <- response{err: err}
			return
		}
		l := &Lease{Semaphore: req.name, HolderID: req.holderID, Fence: ev.Fence, Count: ev.Count, ExpiresAt: time.Unix(0, ev.ExpiresAt)}
		m.res <- response{lease: l, ack: true}
	case "cancel":
		// Resolve the semaphore for this waiter through the live index.
		name := i.engine.waiterOwner(req.waiterID)
		if name == "" {
			m.res <- response{ack: true} // already gone: idempotent
			return
		}
		_, err := i.engine.cancel(name, req.waiterID, i.nowNanos(), i.save)
		m.res <- response{ack: err == nil, err: err}
	case "preempt":
		ids := splitIDs(req.holderID)
		evs, err := i.engine.preempt(req.name, ids, i.nowNanos(),
			i.cfg.DefaultLeaseTTL.Nanoseconds(), i.save)
		if err != nil {
			m.res <- response{err: err}
			return
		}
		i.notifyGrants(evs)
		m.res <- response{ack: true}
	case "waittimeout":
		if pa := i.lookupWait(req.waiterID); pa != nil {
			select {
			case pa.errCh <- ErrAcquireTimeout:
			default:
			}
		}
		m.res <- response{ack: true}
	case "notify":
		// A restarted owner re-pushes a recovered queue grant.
		l := &Lease{
			Semaphore: req.name, HolderID: req.holderID,
			Fence: req.fence, Count: req.count,
			ExpiresAt: time.Unix(0, req.deadline),
		}
		if i.pushToWaiter(req.waiterID, l) {
			m.res <- response{ack: true}
			return
		}
		// The acquire call that owned this waiter is gone (canceled or the
		// instance restarted without it): return the orphaned permits so
		// they are never leaked. Release is idempotent and routes to owner.
		go func() { _ = i.Release(context.Background(), l) }()
		m.res <- response{ack: true}
	default:
		m.res <- response{err: fmt.Errorf("semaphore: unknown op %q", req.op)}
	}
}

// notifyGrants delivers queued grants produced by any committed batch.
func (i *Instance) notifyGrants(evs []Event) {
	for _, ev := range evs {
		if ev.Type != evGranted {
			continue
		}
		l := &Lease{
			Semaphore: ev.Name, HolderID: ev.HolderID, Fence: ev.Fence,
			Count: ev.Count, ExpiresAt: time.Unix(0, ev.ExpiresAt),
		}
		instance := waiterInstance(ev.WaiterID)
		waiter := ev.WaiterID
		if instance == i.cfg.InstanceID {
			i.deliverLocal(waiter, l)
		} else {
			i.deliverRemote(instance, waiter, l)
		}
	}
}

func (i *Instance) deliverGrant(g pendingGrant) {
	l := &Lease{
		Semaphore: g.name, HolderID: g.holderID, Fence: g.fence,
		Count: g.count, ExpiresAt: time.Unix(0, g.expiresAt),
	}
	instance := waiterInstance(g.waiterID)
	if instance == i.cfg.InstanceID {
		i.deliverLocal(g.waiterID, l)
	} else {
		i.deliverRemote(instance, g.waiterID, l)
	}
}

func (i *Instance) deliverLocal(waiterID string, l *Lease) {
	if i.pushToWaiter(waiterID, l) {
		_ = i.engine.markNotified(l.Semaphore, waiterID, l.Fence, i.nowNanos(), i.save)
		return
	}
	go func() { _ = i.Release(context.Background(), l) }()
}

func (i *Instance) deliverRemote(instance, waiterID string, l *Lease) {
	go func() {
		req := &request{
			op: "notify", From: i.cfg.InstanceID, name: l.Semaphore,
			holderID: l.HolderID, waiterID: waiterID, fence: l.Fence,
			count: l.Count, deadline: l.ExpiresAt.UnixNano(),
		}
		for attempt := 0; attempt < 5; attempt++ {
			if i.isClosed() {
				return
			}
			r := i.bus.send(context.Background(), instance, req)
			if r.err == nil && r.ack {
				_ = i.engine.markNotified(l.Semaphore, waiterID, l.Fence, i.nowNanos(), i.save)
				return
			}
			time.Sleep(i.cfg.TickInterval)
		}
		// Give up in-memory; the lease will expire and be reclaimed if the
		// client never comes back, and a restart re-pushes pending grants.
	}()
}

// pushToWaiter resolves a local pending acquire (local owner case).
func (i *Instance) pushToWaiter(waiterID string, l *Lease) bool {
	pa := i.lookupWait(waiterID)
	if pa == nil {
		return false
	}
	select {
	case pa.leaseCh <- l:
		return true
	default:
		// Already resolved (cancel race or duplicate delivery).
		return false
	}
}

// ---- background lease reaping ----

func (i *Instance) tickLoop() {
	ticker := i.cfg.Clock.NewTicker(i.cfg.TickInterval)
	defer ticker.Stop()
	ch := ticker.Chan()
	for {
		select {
		case <-i.closed:
			return
		case <-ch:
			i.tick()
		}
	}
}

func (i *Instance) tick() {
	now := i.nowNanos()
	for _, name := range i.engine.names() {
		holders, waiters := i.engine.expired(name, now)
		// Expire waiters first (their timeout frees no permits), then
		// recycle each expired holder; each recycle may grant queued
		// waiters atomically.
		for _, wid := range waiters {
			evs, err := i.engine.expire(name, wid, true, now, i.cfg.DefaultLeaseTTL.Nanoseconds(), i.save)
			if err == nil && len(evs) > 0 {
				i.failWaiter(wid, ErrAcquireTimeout)
				i.notifyGrants(evs)
			}
		}
		for _, hid := range holders {
			evs, err := i.engine.expire(name, hid, false, now, i.cfg.DefaultLeaseTTL.Nanoseconds(), i.save)
			if err != nil {
				continue
			}
			i.failHolder(hid)
			i.notifyGrants(evs)
		}
	}
}

// failWaiter wakes a local waiter whose queue slot timed out. Remote
// waiters are notified by the owner over the bus (timeout notify).
func (i *Instance) failWaiter(waiterID string, err error) {
	if pa := i.lookupWait(waiterID); pa != nil {
		select {
		case pa.errCh <- err:
		default:
		}
		return
	}
	instance := waiterInstance(waiterID)
	if instance != "" && instance != i.cfg.InstanceID {
		req := &request{op: "waittimeout", From: i.cfg.InstanceID, waiterID: waiterID}
		_ = i.bus.send(context.Background(), instance, req)
	}
}

func (i *Instance) failHolder(hid string) {
	i.stopLeaseHeartbeat(hid)
}

// ---- helpers ----

func (i *Instance) nowNanos() int64 { return i.cfg.Clock.Now().UnixNano() }

func (i *Instance) newID(prefix string) string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return prefix + "-" + i.cfg.InstanceID + "-" + hex.EncodeToString(b)
}

func waiterInstance(waiterID string) string {
	// id format: <prefix>-<instance>-<random>
	parts := splitDash(waiterID)
	if len(parts) >= 3 {
		return parts[1]
	}
	return ""
}

func mapCtxErr(err error) error {
	if err == context.DeadlineExceeded {
		return ErrAcquireTimeout
	}
	if err != nil {
		return ErrAcquireCanceled
	}
	return nil
}

// scheduleCancelRetry issues bounded idempotent cancels for an already
// established queue slot (the acquire RPC itself had succeeded).
func (i *Instance) scheduleCancelRetry(waiterID, owner string) {
	go func() {
		for n := 0; n < 8; n++ {
			if i.isClosed() {
				return
			}
			i.cancelWait(waiterID, owner)
			time.Sleep(i.cfg.TickInterval)
		}
	}()
}

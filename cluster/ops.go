package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/guoyyit/distributed-semaphore-go/config"
	"github.com/guoyyit/distributed-semaphore-go/semaphore"
	"github.com/guoyyit/distributed-semaphore-go/store"
)

// Submit applies a client request through the replicated log. It may be called
// on any node; a non-leader replies with a leader hint and the Client retries.
func (n *Instance) Submit(ctx context.Context, req ClientRequest) (ClientResponse, error) {
	ch := make(chan ClientResponse, 1)
	req.Source = n.id
	n.mu.Lock()
	if n.role != Leader {
		hint := n.leaderID
		n.mu.Unlock()
		return ClientResponse{CorrID: req.CorrID, LeaderHint: hint, Err: "not-leader"}, nil
	}
	if _, exists := n.pending[req.CorrID]; !exists {
		n.pending[req.CorrID] = &pendingOp{req: req, resp: ch}
	}
	n.bus.Send(n.id, Message{ClientReq: &req})
	n.mu.Unlock()

	select {
	case r := <-ch:
		return r, nil
	case <-ctx.Done():
		n.mu.Lock()
		delete(n.pending, req.CorrID)
		n.mu.Unlock()
		return ClientResponse{CorrID: req.CorrID, Err: ctx.Err().Error()}, ctx.Err()
	}
}

// handleClientRequest runs only on the leader (its run-loop).
func (n *Instance) handleClientRequest(ctx context.Context, req ClientRequest) {
	if req.Source != n.id {
		// Forwarded from another node: remember where to send the answer.
		if _, ok := n.pending[req.RequestID]; !ok {
			n.pending[req.RequestID] = &pendingOp{req: req}
		}
	}
	// Re-attach to an already decided outcome after a client retry / re-route.
	plan := n.plannerLocked()
	if req.Op == OpCancel {
		if _, queued := plan.Waiter(req.Name, req.RequestID); !queued {
			n.routeResponseLocked(req, ClientResponse{CorrID: req.CorrID, OK: true})
			return
		}
	} else if resp, ok := n.reattachLocked(req); ok {
		n.routeResponseLocked(req, resp)
		return
	}

	now := n.now()
	var events []semaphore.Event
	var err error
	switch req.Op {
	case OpInit:
		// Idempotent: a retried Init with the same key after a prior success is
		// answered OK without appending a duplicate event.
		if _, serr := plan.Stats(req.Name); serr == nil {
			n.routeResponseLocked(req, ClientResponse{CorrID: req.CorrID, OK: true})
			return
		}
		var e semaphore.Event
		e, err = plan.PlanInit(n.nextEventIndex(), now, req.Name, req.Permits, req.Policy)
		if err == nil {
			events = append(events, e)
		}
	case OpAcquire:
		events, err = n.planAcquireLocked(plan, req, now)
	case OpRelease:
		var e semaphore.Event
		var planned bool
		e, planned, err = plan.PlanRelease(n.nextEventIndex(), now, req.Name, req.CorrID, req.HolderID, req.Token)
		if err == nil && planned {
			events = append(events, e)
		}
	case OpRenew:
		var e semaphore.Event
		e, err = plan.PlanRenew(n.nextEventIndex(), now, req.Name, req.CorrID, req.HolderID, req.Token, req.TTL)
		if err == nil {
			events = append(events, e)
		}
	case OpCancel:
		var e semaphore.Event
		var planned bool
		e, planned, err = plan.PlanCancel(n.nextEventIndex(), now, req.Name, req.RequestID, req.CorrID)
		if err == nil && planned {
			events = append(events, e)
		}
	default:
		err = errors.New("unknown op")
	}

	if err != nil {
		if errors.Is(err, semaphore.ErrDuplicateRequest) {
			// A duplicate delivery of an op whose outcome is already durable:
			// the original answer stands; nothing more to do here.
			return
		}
		n.routeResponseLocked(req, ClientResponse{CorrID: req.CorrID, Err: err.Error()})
		return
	}
	if len(events) == 0 {
		// Idempotent no-op success (release/cancel of already-terminal request).
		n.routeResponseLocked(req, ClientResponse{CorrID: req.CorrID, OK: true})
		return
	}
	n.appendEventsLocked(events)
}

// planAcquireLocked handles both the policy initialization at first use and
// the all-or-nothing acquire (immediate grant OR one full wait event).
func (n *Instance) planAcquireLocked(plan *semaphore.Machine, req ClientRequest, now time.Time) ([]semaphore.Event, error) {
	lease := req.TTL
	if lease <= 0 {
		lease = n.cfg.LeaseTTL
	}
	if _, err := plan.Stats(req.Name); errors.Is(err, semaphore.ErrNotFound) {
		// Capacity must be declared via Init before acquire.
		return nil, err
	}
	e, err := plan.PlanAcquire(n.nextEventIndex(), now,
		req.Name, req.RequestID, req.HolderID, req.Permits, req.Priority, lease, req.WaitTTL)
	if err != nil {
		if errors.Is(err, semaphore.ErrDuplicateRequest) {
			return nil, err
		}
		return nil, err
	}
	return []semaphore.Event{e}, nil
}

// reattachLocked answers from committed state when a request id is known.
func (n *Instance) reattachLocked(req ClientRequest) (ClientResponse, bool) {
	m := n.plannerLocked()
	// 1. Durable outcome of this exact operation id (covers renew/release/
	//    cancel retries and retried acquires after grant/timeout).
	if o, ok := m.OpOutcome(req.CorrID); ok && o.Name == req.Name {
		switch o.State {
		case semaphore.ReqGranted:
			return ClientResponse{CorrID: req.CorrID, OK: true, Token: o.Token, ExpiresAt: o.ExpiresAt}, true
		case semaphore.ReqWaiting:
			// Still queued: stay attached; the grant/timeout event will answer.
			return ClientResponse{}, false
		default:
			return ClientResponse{CorrID: req.CorrID, Err: "request finished"}, true
		}
	}
	// 2. Acquire retries keyed by RequestID (== CorrID for acquires).
	if req.Op == OpAcquire {
		if g, ok := m.GrantByRequest(req.Name, req.RequestID); ok {
			return ClientResponse{CorrID: req.CorrID, OK: true, Token: g.Token, ExpiresAt: g.ExpiresAt}, true
		}
		if _, ok := m.Waiter(req.Name, req.RequestID); ok {
			return ClientResponse{}, false
		}
	}
	return ClientResponse{}, false
}

// appendEventsLocked durably appends a planned batch to the leader log and
// immediately replicates. Events already carry sequentially reserved Seq
// values (batches reserve a contiguous range up-front), so they are used as
// the log indices verbatim.
func (n *Instance) appendEventsLocked(events []semaphore.Event) {
	var recs []store.Record
	entries := make([]semaphoreEntry, 0, len(events))
	expected := n.lastLogIndexLocked() + 1
	for _, e := range events {
		if e.Seq != expected {
			// Defensive: a mis-planned batch must never corrupt index order.
			continue
		}
		data, err := json.Marshal(e)
		if err != nil {
			continue
		}
		recs = append(recs, store.Record{Index: e.Seq, Term: n.term, Data: data})
		entries = append(entries, semaphoreEntry{Index: e.Seq, Term: n.term, Event: e})
		expected++
	}
	if len(recs) == 0 {
		return
	}
	// Leader durability first (fsync), then replicate.
	if err := n.wal.AppendAll(recs); err != nil {
		return
	}
	n.log = append(n.log, entries...)
	// Reflect appended events in the speculative view immediately so the next
	// concurrent request sees the permits as already accounted for.
	if n.planning != nil {
		for _, e := range events {
			_ = n.planning.Apply(e)
		}
	}
	if len(n.bus.Members()) == 1 {
		// Single node: the leader log is the quorum; commit directly.
		n.advanceCommitLocked(n.lastLogIndexLocked())
		n.tryPumpLocked(context.Background())
		return
	}
	n.broadcastAppendEntriesLocked()
}

// answerForEventLocked resolves client operations whose event just committed.
func (n *Instance) answerForEventLocked(e semaphore.Event) {
	switch e.Type {
	case semaphore.EventInit:
		n.answerInitLocked(e.Init.Name, ClientResponse{OK: true})
	case semaphore.EventAcquired:
		p := e.Acquired
		if g, ok := n.machine.GrantByRequest(p.Name, p.RequestID); ok {
			n.answerByIDLocked(p.Name, p.RequestID,
				ClientResponse{OK: true, Token: g.Token, ExpiresAt: g.ExpiresAt})
		}
	case semaphore.EventReleased:
		p := e.Released
		n.answerByIDLocked(p.Name, p.RequestID, ClientResponse{OK: true})
	case semaphore.EventRenewed:
		p := e.Renewed
		if g, ok := n.machine.Grant(p.Name, p.HolderID); ok {
			n.answerByIDLocked(p.Name, p.RequestID,
				ClientResponse{OK: true, Token: g.Token, ExpiresAt: g.ExpiresAt})
		}
	case semaphore.EventCancel:
		p := e.Cancel
		n.answerByIDLocked(p.Name, p.OpID, ClientResponse{OK: true})
		// The canceled acquisition (if its client is still attached) is
		// notified immediately instead of waiting out its timeout.
		n.answerByIDLocked(p.Name, p.RequestID, ClientResponse{Err: "acquire canceled"})
	case semaphore.EventWait:
		// Nothing to answer yet: the request stays attached until grant,
		// cancel, or wait timeout.
	case semaphore.EventExpired:
		// If a *waiter* expired, notify its waiting client if still attached.
		if e.Expired != nil && e.Expired.RequestID != "" {
			n.answerByIDLocked(e.Expired.Name, e.Expired.RequestID,
				ClientResponse{Err: "acquire wait timed out"})
		}
	}
}

// answerInitLocked resolves the Init op for a name, keyed as "init:<name>".
func (n *Instance) answerInitLocked(name string, resp ClientResponse) {
	n.answerByIDLocked(name, "init:"+name, resp)
}

func (n *Instance) answerByIDLocked(name, requestID string, resp ClientResponse) {
	p, ok := n.pending[requestID]
	if !ok || p.req.Name != name {
		return
	}
	delete(n.pending, requestID)
	resp.CorrID = p.req.CorrID
	n.deliverLocked(p, resp)
}

// deliverLocked sends a final response to a resolved pending op, locally or
// back to the node that originally received the client request.
func (n *Instance) deliverLocked(p *pendingOp, resp ClientResponse) {
	if p.req.Source == n.id && p.resp != nil {
		select {
		case p.resp <- resp:
		default:
		}
		return
	}
	if p.req.Source != "" && p.req.Source != n.id {
		n.bus.Send(p.req.Source, Message{ClientResp: &resp})
	}
}

func (n *Instance) routeResponseLocked(req ClientRequest, resp ClientResponse) {
	resp.CorrID = req.CorrID
	if req.Source == n.id {
		if p, ok := n.pending[req.CorrID]; ok && p.resp != nil {
			delete(n.pending, req.CorrID)
			select {
			case p.resp <- resp:
			default:
			}
		}
		return
	}
	n.bus.Send(req.Source, Message{ClientResp: &resp})
}

// appendNoopLocked durably appends a leader-inauguration no-op entry.
func (n *Instance) appendNoopLocked() {
	e := semaphore.Event{Seq: n.nextEventIndex(), Type: semaphore.EventNoop, Timestamp: n.now()}
	data, err := json.Marshal(e)
	if err != nil {
		return
	}
	rec := store.Record{Index: e.Seq, Term: n.term, Data: data}
	if err := n.wal.AppendAll([]store.Record{rec}); err != nil {
		return
	}
	n.log = append(n.log, semaphoreEntry{Index: e.Seq, Term: n.term, Event: e})
	if len(n.bus.Members()) == 1 {
		n.advanceCommitLocked(n.lastLogIndexLocked())
		return
	}
	n.broadcastAppendEntriesLocked()
}

// tryPumpLocked promotes all now-satisfiable head waiters in one atomic batch.
func (n *Instance) tryPumpLocked(ctx context.Context) {
	if n.role != Leader {
		return
	}
	events := n.plannerLocked().PlanPump(n.nextEventIndex(), n.now())
	if len(events) > 0 {
		n.appendEventsLocked(events)
	}
}

// reapExpired durably reaps expired grants and waiters, then pumps, in one
// batch. Runs only on the leader, serialized on the run-loop.
func (n *Instance) reapExpired() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return
	}
	events := n.plannerLocked().PlanExpirations(n.nextEventIndex(), n.now())
	if len(events) == 0 {
		return
	}
	n.appendEventsLocked(events)
}

// nextEventIndex reserves the next 1-based log index for a planned event.
// Note: callers plan one event at a time; batches must pass an explicit base.
func (n *Instance) nextEventIndex() uint64 {
	return n.lastLogIndexLocked() + 1
}

// keep the imports honest in this file even when some are only used elsewhere.
var _ = context.Background
var _ = config.QueueFIFO
var _ = time.Now

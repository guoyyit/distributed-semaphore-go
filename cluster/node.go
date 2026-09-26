// Package cluster runs the replicated semaphore as several in-process
// instances coordinating over an in-memory Bus. Consensus follows a small
// Raft subset (leader election + log replication + durable term/vote) so that
// permits are never double-granted and are fully recoverable on restart.
package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/guoyyit/distributed-semaphore-go/config"
	"github.com/guoyyit/distributed-semaphore-go/semaphore"
	"github.com/guoyyit/distributed-semaphore-go/store"
)

const mailboxSize = 512

// pendingOp is an in-flight client request awaiting commit / future grant.
type pendingOp struct {
	req  ClientRequest
	resp chan ClientResponse
}

// Instance is one node of the in-process semaphore cluster.
type Instance struct {
	cfg config.Config
	id  string
	bus *Bus

	machine *semaphore.Machine
	// planning is the leader-only speculative view: committed state plus the
	// uncommitted tail. All Plan* calls run against it so concurrent client
	// requests never observe stale permit counts. Discarded on step-down.
	planning *semaphore.Machine
	wal      *store.WAL
	snap     *store.Snapshot
	meta     *store.Meta

	mu sync.Mutex // guards all fields below; the run-loop is the main writer
	// Raft volatile state.
	role        Role
	term        uint64
	votedFor    string
	log         []logEntry
	commitIndex uint64
	lastApplied uint64
	leaderID    string
	// Leader bookkeeping.
	nextIndex  map[string]uint64
	matchIndex map[string]uint64
	// Client operations keyed by request id.
	pending map[string]*pendingOp
	// Highest log index included in the most recent AppendEntries to a peer.
	aeLastSent map[string]uint64
	// Votes collected during the current election term.
	collectVotesLocked map[string]bool
	// Timing (test-injectable).
	nowFunc    func() time.Time
	electionD  time.Duration
	heartbeatD time.Duration
	// lastContact is the last time we heard from a valid leader or started
	// an election; elections only fire after a full timeout of silence.
	lastContact time.Time

	mailbox chan Message
	cancel  context.CancelFunc
	done    chan struct{}
}

type logEntry = semaphoreEntry

// semaphoreEntry is a local alias wrapper for an encoded event with raft meta.
type semaphoreEntry struct {
	Index uint64
	Term  uint64
	Event semaphore.Event
}

// NewInstance creates a node and recovers its durable state from dataDir.
func NewInstance(cfg config.Config, bus *Bus) (*Instance, error) {
	if cfg.InstanceID == "" {
		return nil, fmt.Errorf("cluster: InstanceID required")
	}
	d := cfg
	if d.ElectionTimeout == 0 {
		d.ElectionTimeout = 200 * time.Millisecond
	}
	if d.HeartbeatInterval == 0 {
		d.HeartbeatInterval = 50 * time.Millisecond
	}
	if d.LeaseTTL == 0 {
		d.LeaseTTL = 5 * time.Second
	}
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = fmt.Sprintf(".data/%s", cfg.InstanceID)
	}
	wal, recs, err := store.OpenWAL(dataDir)
	if err != nil {
		return nil, err
	}
	snapStore, err := store.OpenSnapshot(dataDir)
	if err != nil {
		return nil, err
	}
	meta, err := store.OpenMeta(dataDir)
	if err != nil {
		return nil, err
	}

	n := &Instance{
		cfg: d, id: d.InstanceID, bus: bus,
		machine: semaphore.NewMachine(),
		wal:     wal, snap: snapStore, meta: meta,
		role: Follower,
		term: meta.Term, votedFor: meta.VotedFor,
		nextIndex: map[string]uint64{}, matchIndex: map[string]uint64{},
		pending:    map[string]*pendingOp{},
		aeLastSent: map[string]uint64{},
		nowFunc:    time.Now,
		electionD:  d.ElectionTimeout, heartbeatD: d.HeartbeatInterval,
		mailbox: make(chan Message, mailboxSize),
		done:    make(chan struct{}),
	}

	// 1. Restore the latest snapshot into the state machine.
	var snapIndex uint64
	if data, idx, _, lerr := snapStore.Load(); lerr == nil {
		if rerr := n.machine.Restore(data); rerr != nil {
			return nil, rerr
		}
		snapIndex = idx
	}

	// 2. Reconstruct committed log entries from WAL. Entries past the durable
	// commit point were never acknowledged to a quorum; truncate them so the
	// local WAL is a clean committed prefix (a fresh leader re-proposes as
	// needed). The WAL's Append API requires a contiguous index space.
	lastCommit := meta.CommitIndex
	var kept []store.Record
	for _, r := range recs {
		if r.Index <= snapIndex {
			continue
		}
		if r.Index > lastCommit {
			break
		}
		var ev semaphore.Event
		if jerr := json.Unmarshal(r.Data, &ev); jerr != nil {
			return nil, jerr
		}
		ev.Seq = r.Index
		n.log = append(n.log, logEntry{Index: r.Index, Term: r.Term, Event: ev})
		kept = append(kept, r)
	}
	if len(kept) < len(recs) {
		if err := wal.RewriteFrom(kept); err != nil {
			return nil, err
		}
	}
	// 3. Apply committed entries in order.
	for _, e := range n.log {
		if e.Event.Type != semaphore.EventCommit {
			if err := n.machine.Apply(e.Event); err != nil {
				return nil, err
			}
		}
		n.lastApplied = e.Index
	}
	n.commitIndex = lastCommit
	return n, nil
}

// Start launches the node's event loop and registers its mailbox.
func (n *Instance) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	n.cancel = cancel
	n.bus.Register(n.id, n.mailbox)
	go n.run(ctx)
	return nil
}

// Stop shuts the node down and closes durable storage.
func (n *Instance) Stop() error {
	if n.cancel != nil {
		n.cancel()
	}
	<-n.done
	n.bus.Unregister(n.id)
	return n.wal.Close()
}

func (n *Instance) run(ctx context.Context) {
	defer close(n.done)
	electionTimer := time.NewTimer(n.randomElectionTimeout())
	defer electionTimer.Stop()
	heartbeatTicker := time.NewTicker(n.heartbeatD)
	defer heartbeatTicker.Stop()
	reapTicker := time.NewTicker(n.heartbeatD * 2)
	defer reapTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-n.mailbox:
			n.handleMessage(ctx, msg)
		case <-heartbeatTicker.C:
			n.mu.Lock()
			if n.role == Leader {
				n.broadcastAppendEntriesLocked()
			}
			n.mu.Unlock()
		case <-reapTicker.C:
			n.reapExpired()
		case <-electionTimer.C:
			n.mu.Lock()
			if n.role != Leader && n.now().Sub(n.lastContact) >= n.electionD {
				n.startElectionLocked(ctx)
			}
			electionTimer.Reset(n.randomElectionTimeout())
			n.mu.Unlock()
		}
	}
}

func (n *Instance) randomElectionTimeout() time.Duration {
	// Deterministic jitter derived from node id hash would be better in tests;
	// here use a fixed spread within [T, 1.5T) to avoid split votes.
	base := n.electionD
	spread := base / 2
	return base + time.Duration(hashID(n.id)%uint64(spread+1))
}

func hashID(s string) uint64 {
	var h uint64 = 1469598103934665603
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

// ---------- message dispatch ----------

func (n *Instance) handleMessage(ctx context.Context, msg Message) {
	n.mu.Lock()
	switch {
	case msg.ClientReq != nil:
		n.handleClientRequest(ctx, *msg.ClientReq)
	case msg.AE != nil:
		n.handleAppendEntries(*msg.AE)
	case msg.AEResult != nil:
		n.handleAppendResultLocked(ctx, *msg.AEResult)
	case msg.Vote != nil:
		n.handleVote(*msg.Vote)
	case msg.VoteResult != nil:
		n.handleVoteResultLocked(ctx, *msg.VoteResult)
	case msg.ClientResp != nil:
		n.deliverExternalResponseLocked(*msg.ClientResp)
	}
	n.mu.Unlock()
}

func (n *Instance) deliverExternalResponseLocked(resp ClientResponse) {
	// Forwarded operations return to the receiving node, which maps CorrID
	// (== RequestID) back to its waiter.
	if p, ok := n.pending[resp.CorrID]; ok {
		delete(n.pending, resp.CorrID)
		if p.resp != nil {
			select {
			case p.resp <- resp:
			default:
			}
		}
	}
}

// ---------- election ----------

func (n *Instance) startElectionLocked(ctx context.Context) {
	n.term++
	n.role = Candidate
	n.votedFor = n.id
	n.lastContact = n.now()
	if err := n.meta.SaveTermVote(n.term, n.votedFor); err != nil {
		// Persistence failure: step down; the next round retries.
		n.role = Follower
		return
	}
	votes := map[string]bool{n.id: true}
	req := RequestVote{
		From: n.id, Term: n.term,
		LastLogIndex: n.lastLogIndexLocked(), LastLogTerm: n.lastLogTermLocked(),
	}
	n.broadcastLocked(Message{Vote: &req})
	// Single-node cluster wins immediately.
	if len(n.bus.Members()) <= 1 {
		n.becomeLeaderLocked(ctx)
		return
	}
	n.collectVotesLocked = votes
}

func (n *Instance) handleVote(v RequestVote) {
	if v.Term < n.term {
		n.send(v.From, Message{VoteResult: &RequestVoteResult{From: n.id, Term: n.term, VoteGranted: false}})
		return
	}
	if v.Term > n.term {
		n.term = v.Term
		n.stepDownLocked()
		n.votedFor = ""
	}
	grant := false
	if (n.votedFor == "" || n.votedFor == v.From) && n.logUpToDate(v.LastLogIndex, v.LastLogTerm) {
		n.votedFor = v.From
		_ = n.meta.SaveTermVote(n.term, n.votedFor)
		n.lastContact = n.now()
		grant = true
	}
	n.send(v.From, Message{VoteResult: &RequestVoteResult{From: n.id, Term: n.term, VoteGranted: grant}})
}

func (n *Instance) handleVoteResultLocked(ctx context.Context, r RequestVoteResult) {
	if r.Term > n.term {
		n.term = r.Term
		n.stepDownLocked()
		n.votedFor = ""
		return
	}
	if n.role != Candidate || r.Term != n.term || !r.VoteGranted {
		return
	}
	if n.collectVotesLocked == nil {
		n.collectVotesLocked = map[string]bool{}
	}
	n.collectVotesLocked[r.From] = true
	if len(n.collectVotesLocked)*2 > len(n.bus.Members()) {
		n.becomeLeaderLocked(ctx)
	}
}

func (n *Instance) becomeLeaderLocked(ctx context.Context) {
	n.role = Leader
	n.leaderID = n.id
	last := n.lastLogIndexLocked()
	for _, id := range n.peerIDs() {
		n.nextIndex[id] = last + 1
		n.matchIndex[id] = 0
	}
	// Speculative planning view = committed state; uncommitted tail is
	// re-planned through it from now on.
	n.planning = semaphore.NewMachine()
	if err := n.planning.Restore(n.machine.Snapshot()); err != nil {
		n.planning = nil
		return
	}
	// Leader inauguration no-op: committing it also commits every prior entry
	// (including entries from older terms) per Raft.
	n.appendNoopLocked()
}

// stepDownLocked transitions to Follower, dropping the speculative planning
// view and failing fast any client ops still waiting on this leadership.
func (n *Instance) stepDownLocked() {
	if n.role == Leader {
		n.planning = nil
		for id, p := range n.pending {
			delete(n.pending, id)
			n.deliverLocked(p, ClientResponse{CorrID: p.req.CorrID, Err: "not-leader"})
		}
	}
	n.role = Follower
}

// plannerLocked returns the machine to plan against (leader: speculative view).
func (n *Instance) plannerLocked() *semaphore.Machine {
	if n.role == Leader && n.planning != nil {
		return n.planning
	}
	return n.machine
}

func (n *Instance) logUpToDate(candidateIndex, candidateTerm uint64) bool {
	myIndex := n.lastLogIndexLocked()
	myTerm := n.lastLogTermLocked()
	if candidateTerm != myTerm {
		return candidateTerm > myTerm
	}
	return candidateIndex >= myIndex
}

// ---------- append entries (follower side) ----------

func (n *Instance) handleAppendEntries(m AppendEntries) {
	if m.Term < n.term {
		n.send(m.From, Message{AEResult: &AppendEntriesResult{From: n.id, Term: n.term, OK: false, LastIndex: n.lastLogIndexLocked()}})
		return
	}
	if m.Term > n.term {
		n.term = m.Term
		n.votedFor = ""
		_ = n.meta.SaveTermVote(n.term, "")
	}
	n.stepDownLocked()
	n.leaderID = m.From
	n.lastContact = n.now()

	ok := true
	if m.PrevIndex > 0 {
		if !n.hasLogLocked(m.PrevIndex, m.PrevTerm) {
			ok = false
		}
	}
	if ok {
		for _, le := range m.Entries {
			n.appendOrReplaceLocked(le)
		}
		if m.CommitIndex > n.commitIndex {
			newCommit := m.CommitIndex
			if last := n.lastLogIndexLocked(); newCommit > last {
				newCommit = last
			}
			n.advanceCommitLocked(newCommit)
		}
	}
	n.send(m.From, Message{AEResult: &AppendEntriesResult{From: n.id, Term: n.term, OK: ok, LastIndex: n.lastLogIndexLocked()}})
}

func (n *Instance) hasLogLocked(index, term uint64) bool {
	for i := len(n.log) - 1; i >= 0; i-- {
		if n.log[i].Index == index {
			return n.log[i].Term == term
		}
		if n.log[i].Index < index {
			return false
		}
	}
	return false
}

// appendOrReplaceLocked persists one entry, resolving conflicts Raft-style:
// any local entry at the same index with a different term is overwritten and
// the tail after it is discarded on both the in-memory log and durable WAL.
func (n *Instance) appendOrReplaceLocked(le LogEntry) {
	for i := range n.log {
		if n.log[i].Index == le.Index {
			if n.log[i].Term == le.Term {
				return
			}
			// Conflict: truncate memory and WAL to the conflicting prefix.
			keep := n.log[:i]
			n.log = keep
			var keepLast uint64
			if len(keep) > 0 {
				keepLast = keep[len(keep)-1].Index
			}
			var recs []store.Record
			for _, e := range keep {
				data, _ := encodeEvent(e.Event)
				recs = append(recs, store.Record{Index: e.Index, Term: e.Term, Data: data})
			}
			if err := n.wal.RewriteFrom(recs); err != nil {
				return
			}
			_ = keepLast
			break
		}
	}
	var ev semaphore.Event
	if err := json.Unmarshal(le.Data, &ev); err != nil {
		return
	}
	ev.Seq = le.Index
	n.log = append(n.log, semaphoreEntry{Index: le.Index, Term: le.Term, Event: ev})
	if err := n.wal.Append(store.Record{Index: le.Index, Term: le.Term, Data: le.Data}); err != nil {
		// Persistence failure on a follower: drop from memory so the next
		// leader retry re-adds it; the leader remains the authority.
		n.log = n.log[:len(n.log)-1]
	}
}

// ---------- append entries (leader side) ----------

func (n *Instance) broadcastAppendEntriesLocked() {
	for _, id := range n.peerIDs() {
		n.sendAppendToLocked(id)
	}
}

func (n *Instance) sendAppendToLocked(peer string) {
	next := n.nextIndex[peer]
	var prevIndex, prevTerm uint64
	var entries []LogEntry
	start := n.indexOfLocked(next)
	if start == -1 {
		if next > 1 {
			prevIndex = next - 1
			prevTerm = n.termAtLocked(prevIndex)
		}
	} else {
		if next > 1 {
			prevIndex = next - 1
			prevTerm = n.termAtLocked(prevIndex)
		}
		for _, e := range n.log[start:] {
			data, _ := encodeEvent(e.Event)
			entries = append(entries, LogEntry{Index: e.Index, Term: e.Term, Data: data})
		}
	}
	var sent uint64
	if len(entries) > 0 {
		sent = entries[len(entries)-1].Index
	} else {
		sent = prevIndex
	}
	n.aeLastSent[peer] = sent
	n.send(peer, Message{AE: &AppendEntries{
		From: n.id, Term: n.term, PrevIndex: prevIndex, PrevTerm: prevTerm,
		Entries: entries, CommitIndex: n.commitIndex,
	}})
}

func (n *Instance) handleAppendResultLocked(ctx context.Context, r AppendEntriesResult) {
	if r.Term > n.term {
		n.term = r.Term
		n.stepDownLocked()
		n.votedFor = ""
		_ = n.meta.SaveTermVote(n.term, "")
		return
	}
	if n.role != Leader || r.Term != n.term {
		return
	}
	if r.OK {
		// The follower durably holds a contiguous prefix through LastIndex.
		ack := r.LastIndex
		if last := n.lastLogIndexLocked(); ack > last {
			ack = last
		}
		if ack > n.matchIndex[r.From] {
			n.matchIndex[r.From] = ack
		}
		if n.nextIndex[r.From] < ack+1 {
			n.nextIndex[r.From] = ack + 1
		}
		n.advanceCommitFromQuorumLocked(ctx)
		// Push any entries the follower still lacks.
		if n.matchIndex[r.From] < n.lastLogIndexLocked() {
			n.sendAppendToLocked(r.From)
		}
	} else {
		// Log inconsistency: back up the follower's next index and retry.
		if n.nextIndex[r.From] > 1 {
			n.nextIndex[r.From]--
		}
		n.sendAppendToLocked(r.From)
	}
}

func (n *Instance) advanceCommitFromQuorumLocked(ctx context.Context) {
	for idx := n.lastLogIndexLocked(); idx > n.commitIndex; idx-- {
		// Only commit entries from the current term (Raft safety).
		if n.termAtLocked(idx) != n.term {
			continue
		}
		count := 1 // leader
		for _, id := range n.peerIDs() {
			if n.matchIndex[id] >= idx {
				count++
			}
		}
		if count*2 > len(n.bus.Members()) {
			n.advanceCommitLocked(idx)
			n.tryPumpLocked(ctx)
			break
		}
	}
}

// advanceCommitLocked applies all committed-but-unapplied events in order,
// durably records the committed high-water mark, and answers completed clients.
func (n *Instance) advanceCommitLocked(target uint64) {
	if target <= n.commitIndex {
		return
	}
	for _, e := range n.log {
		if e.Index <= n.commitIndex || e.Index > target {
			continue
		}
		if e.Event.Type != semaphore.EventCommit {
			if err := n.machine.Apply(e.Event); err != nil {
				continue
			}
			n.answerForEventLocked(e.Event)
		}
		n.lastApplied = e.Index
	}
	n.commitIndex = target
	// Durable high-water mark so a restart never applies uncommitted entries.
	_ = n.meta.SaveCommit(target)
	// Permits freed by now-committed releases/expirations can serve waiters.
	if n.role == Leader {
		n.tryPumpLocked(context.Background())
	}
}

// ---------- log helpers ----------

func (n *Instance) lastLogIndexLocked() uint64 {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Index
}

func (n *Instance) lastLogTermLocked() uint64 {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Term
}

func (n *Instance) termAtLocked(index uint64) uint64 {
	for i := len(n.log) - 1; i >= 0; i-- {
		if n.log[i].Index == index {
			return n.log[i].Term
		}
	}
	return 0
}

func (n *Instance) indexOfLocked(index uint64) int {
	for i := range n.log {
		if n.log[i].Index == index {
			return i
		}
	}
	return -1
}

func (n *Instance) peerIDs() []string {
	var out []string
	for _, id := range n.bus.Members() {
		if id != n.id {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

func (n *Instance) send(to string, msg Message) { n.bus.Send(to, msg) }
func (n *Instance) broadcastLocked(msg Message) {
	for _, id := range n.peerIDs() {
		n.bus.Send(id, msg)
	}
}

// now returns the node clock (overridable in tests).
func (n *Instance) now() time.Time { return n.nowFunc() }

// ID returns this instance's cluster id.
func (n *Instance) ID() string { return n.id }

// IsLeader reports whether this node currently leads.
func (n *Instance) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == Leader
}

// Leader returns the current known leader id ("" if unknown).
func (n *Instance) Leader() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// Stats returns the local state machine view of one semaphore.
func (n *Instance) Stats(name string) (semaphore.Stats, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.machine.Stats(name)
}

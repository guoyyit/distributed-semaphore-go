package cluster

import (
	"time"

	"github.com/guoyyit/distributed-semaphore-go/config"
)

// Role of an instance in the replicated state machine.
type Role uint8

const (
	Follower Role = iota
	Candidate
	Leader
)

// RequestOp enumerates client operations.
type RequestOp uint8

const (
	OpInit RequestOp = iota
	OpAcquire
	OpRelease
	OpRenew
	OpCancel
)

// ClientRequest is a user operation submitted to any instance.
type ClientRequest struct {
	// Source is the instance that first received the request and must get
	// the final response (for attachment after wait/leader changes).
	Source    string
	CorrID    string
	Op        RequestOp
	Name      string
	RequestID string // client-generated idempotency key for acquire/cancel
	HolderID  string
	Permits   int
	Priority  int
	Token     uint64
	// TTL is the lease duration; WaitTTL bounds time spent queued (0 = forever).
	TTL     time.Duration
	WaitTTL time.Duration
	Policy  config.QueuePolicy
	// LeaderHint lets a client pin retries to the known leader.
	LeaderHint string
}

// ClientResponse is the outcome of a client request.
type ClientResponse struct {
	CorrID    string
	OK        bool
	Token     uint64
	ExpiresAt time.Time
	// LeaderHint names the current leader when the contact is not leader.
	LeaderHint string
	Err        string
}

// LogEntry is a replicated event with term/index.
type LogEntry struct {
	Index uint64
	Term  uint64
	Data  []byte
}

// AppendEntries is the Raft replication / heartbeat RPC.
type AppendEntries struct {
	From        string
	Term        uint64
	PrevIndex   uint64
	PrevTerm    uint64
	Entries     []LogEntry
	CommitIndex uint64
}

// AppendEntriesResult is the reply to AppendEntries. LastIndex is the
// follower's durable last log index after processing, so the leader can
// advance matchIndex precisely (never beyond what the follower really has).
type AppendEntriesResult struct {
	From      string
	Term      uint64
	OK        bool
	LastIndex uint64
}

// RequestVote is the Raft pre-vote/vote RPC.
type RequestVote struct {
	From         string
	Term         uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteResult is the reply to RequestVote.
type RequestVoteResult struct {
	From        string
	Term        uint64
	VoteGranted bool
}

// Message wraps every inter-instance envelope.
type Message struct {
	ClientReq  *ClientRequest
	ClientResp *ClientResponse
	AE         *AppendEntries
	AEResult   *AppendEntriesResult
	Vote       *RequestVote
	VoteResult *RequestVoteResult
}

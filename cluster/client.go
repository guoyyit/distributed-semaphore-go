package cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/guoyyit/distributed-semaphore-go/config"
)

// Client is a convenience wrapper that talks to a cluster via any node and
// follows leader hints automatically. All operations carry client-generated
// idempotency keys, so a retry after a timeout or leader change never applies
// the operation twice.
type Client struct {
	nodes  []*Instance
	member int // first-contact node (round-robin / pinned)
	ttl    time.Duration
	policy config.QueuePolicy
}

// NewClient constructs a cluster client over the given instances.
func NewClient(nodes ...*Instance) *Client {
	return &Client{nodes: nodes, ttl: 5 * time.Second, policy: config.QueueFIFO}
}

// WithStartNode pins the first contact to nodes[i]; leader hints still route
// retries to the current leader. This models clients co-located with an
// instance: operations can originate from any member.
func (c *Client) WithStartNode(i int) *Client {
	c.member = i % len(c.nodes)
	return c
}

// WithTTL sets the default lease TTL for acquisitions/renewals.
func (c *Client) WithTTL(d time.Duration) *Client { c.ttl = d; return c }

// WithPolicy sets the waiter ordering policy used at Init.
func (c *Client) WithPolicy(p config.QueuePolicy) *Client { c.policy = p; return c }

func newID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// do retries submission across nodes, following leader hints until the request
// is applied (or the context expires). The idempotency key is stable across
// retries.
func (c *Client) do(ctx context.Context, req ClientRequest) (ClientResponse, error) {
	if req.RequestID == "" {
		req.RequestID = newID()
	}
	if req.CorrID == "" {
		req.CorrID = newID()
	}
	var lastErr error
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return ClientResponse{}, err
		}
		node := c.pickNode(req, attempt)
		if node == nil {
			return ClientResponse{}, errors.New("semaphore: no cluster nodes")
		}
		rpcCtx, cancel := context.WithTimeout(ctx, c.rpcTimeout())
		resp, err := node.Submit(rpcCtx, req)
		cancel()
		// A response (even with OK=false but an empty Err) is a definitive
		// application outcome: only transport/context errors are retried.
		if err == nil && resp.Err == "" {
			return resp, nil
		}
		if err == nil && resp.Err != "" && resp.LeaderHint == "" {
			// A definitive application error (not a leadership redirect).
			return resp, errors.New(resp.Err)
		}
		if err == nil && resp.LeaderHint != "" {
			req.LeaderHint = resp.LeaderHint
			lastErr = errors.New("not-leader")
		} else if err == nil && resp.Err == "not-leader" {
			lastErr = errors.New("not-leader")
		} else if err != nil {
			lastErr = err
		} else {
			lastErr = errors.New("request rejected without leader hint")
		}
		// Brief backoff to let an election settle.
		select {
		case <-ctx.Done():
			return ClientResponse{}, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
		_ = lastErr
	}
}

func (c *Client) pickNode(req ClientRequest, attempt int) *Instance {
	if req.LeaderHint != "" {
		for _, n := range c.nodes {
			if n.id == req.LeaderHint {
				return n
			}
		}
	}
	if len(c.nodes) == 0 {
		return nil
	}
	return c.nodes[(c.member+attempt)%len(c.nodes)]
}

func (c *Client) rpcTimeout() time.Duration {
	if d := c.nodes[0].electionD * 3; d > 0 {
		return d
	}
	return time.Second
}

// Init declares a named semaphore with a fixed permit capacity.
func (c *Client) Init(ctx context.Context, name string, capacity int) error {
	id := "init:" + name
	_, err := c.do(ctx, ClientRequest{
		CorrID: id, RequestID: id, Op: OpInit,
		Name: name, Permits: capacity, Policy: c.policy,
	})
	return err
}

// Acquire obtains permits atomically, waiting up to waitTimeout (0 = forever).
// It returns the current fencing token, lease expiry and the generated request
// id (required to cancel a still-queued attempt).
func (c *Client) Acquire(ctx context.Context, name, holderID string, permits, priority int, waitTimeout time.Duration) (token uint64, expiresAt time.Time, requestID string, err error) {
	requestID = newID()
	return c.AcquireWithID(ctx, name, holderID, requestID, permits, priority, waitTimeout)
}

// AcquireWithID is Acquire with a caller-supplied stable idempotency key.
func (c *Client) AcquireWithID(ctx context.Context, name, holderID, requestID string, permits, priority int, waitTimeout time.Duration) (uint64, time.Time, string, error) {
	waitCtx := ctx
	var cancel context.CancelFunc
	if waitTimeout > 0 {
		waitCtx, cancel = context.WithTimeout(ctx, waitTimeout)
		defer cancel()
	}
	resp, derr := c.do(waitCtx, ClientRequest{
		CorrID: requestID, RequestID: requestID, Op: OpAcquire,
		Name: name, HolderID: holderID, Permits: permits,
		Priority: priority, TTL: c.ttl, WaitTTL: waitTimeout,
	})
	if derr != nil {
		// Best-effort cancel so the queued slot is released if we timed out
		// waiting locally but the request may still be queued cluster-side.
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 2*time.Second)
		_ = c.Cancel(cleanupCtx, name, requestID)
		cancelCleanup()
		return 0, time.Time{}, requestID, derr
	}
	return resp.Token, resp.ExpiresAt, requestID, nil
}

// Release returns permits using the holder's latest fencing token. It is
// idempotent: duplicate releases or a release after lease expiry never credit
// permits twice and never spuriously wake waiters.
func (c *Client) Release(ctx context.Context, name, holderID string, token uint64) error {
	_, err := c.do(ctx, ClientRequest{
		Op: OpRelease, Name: name, HolderID: holderID, Token: token,
	})
	return err
}

// Renew extends the lease and rotates the fencing token; callers must present
// the most recent token. The returned token supersedes the old one.
func (c *Client) Renew(ctx context.Context, name, holderID string, token uint64) (uint64, time.Time, error) {
	resp, err := c.do(ctx, ClientRequest{
		Op: OpRenew, Name: name, HolderID: holderID, Token: token, TTL: c.ttl,
	})
	if err != nil {
		return 0, time.Time{}, err
	}
	return resp.Token, resp.ExpiresAt, nil
}

// Cancel abandons a waiting acquisition and frees its queue slot.
func (c *Client) Cancel(ctx context.Context, name, requestID string) error {
	_, err := c.do(ctx, ClientRequest{
		Op: OpCancel, Name: name, RequestID: requestID,
	})
	return err
}

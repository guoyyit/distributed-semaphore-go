package semaphore

import (
	"context"
	"sort"
	"sync"
)

// request is the wire payload for inter-instance RPCs.
// (declared in service.go along with the public API)

// response is the RPC reply exchanged between instances.
type response struct {
	lease *Lease
	ok    bool
	ack   bool
	err   error
}

// message is one RPC delivered to an owner instance's inbox.
type message struct {
	from string
	req  *request
	res  chan response
}

// bus simulates a lossless local network between in-process instances.
// Sends are synchronous RPCs: the caller blocks until the owner handles the
// request or the caller's context expires.
type bus struct {
	mu       sync.RWMutex
	registry map[string]chan message
}

func newBus() *bus {
	return &bus{registry: make(map[string]chan message)}
}

// register adds an instance. The inbox is unbuffered on the receive side;
// senders create buffered reply channels so owners never block on reply.
func (b *bus) register(id string, inbox chan message) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.registry[id] = inbox
}

func (b *bus) unregister(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.registry, id)
}

func (b *bus) send(ctx context.Context, to string, req *request) response {
	b.mu.RLock()
	inbox, ok := b.registry[to]
	b.mu.RUnlock()
	if !ok {
		return response{err: ErrNoRoute}
	}
	ch := make(chan response, 1)
	m := message{from: req.From, req: req, res: ch}
	select {
	case inbox <- m:
	case <-ctx.Done():
		return response{err: ctx.Err()}
	}
	select {
	case r := <-ch:
		return r
	case <-ctx.Done():
		return response{err: ctx.Err()}
	}
}

// deliver grants to a waiting client instance. "notify" is an instance-local
// call rather than a bus RPC: the owner already holds the answer and pushes
// it into the waiter's result channel via the local notify registry.

func (b *bus) members() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]string, 0, len(b.registry))
	for id := range b.registry {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

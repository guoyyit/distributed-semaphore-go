package cluster

import (
	"sort"
	"sync"
)

// Bus is the in-process transport between instances. Delivery is goroutine-
// decoupled (buffered mailbox per instance), so a slow/stopped node never
// blocks the sender: a full mailbox drops the message (Raft retries and
// client retries make this safe).
type Bus struct {
	mu        sync.RWMutex
	instances map[string]chan Message
}

// NewBus creates an empty in-process bus.
func NewBus() *Bus { return &Bus{instances: map[string]chan Message{}} }

// Register attaches an instance mailbox.
func (b *Bus) Register(id string, ch chan Message) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.instances[id] = ch
}

// Unregister removes an instance mailbox.
func (b *Bus) Unregister(id string) {
	b.mu.Lock()
	ch, ok := b.instances[id]
	if ok {
		delete(b.instances, id)
	}
	b.mu.Unlock()
	if ok {
		// Drain pending messages so no sender is left blocked on a dead mailbox.
		go func() {
			for range ch {
			}
		}()
	}
}

// Send delivers one message to a single instance. It never blocks: if the
// target mailbox is full the message is dropped.
func (b *Bus) Send(to string, msg Message) {
	b.mu.RLock()
	ch, ok := b.instances[to]
	b.mu.RUnlock()
	if !ok {
		return
	}
	select {
	case ch <- msg:
	default:
	}
}

// Members returns registered instance ids sorted for deterministic behavior.
func (b *Bus) Members() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]string, 0, len(b.instances))
	for id := range b.instances {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

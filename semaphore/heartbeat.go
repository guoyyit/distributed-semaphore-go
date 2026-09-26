package semaphore

import (
	"context"
	"sync"
	"time"
)

// heartbeatState tracks automatic lease renewal for leases held by this
// instance. Auto-renew is best-effort; the authoritative safety net is
// owner-side lease expiry reaping, so a failed/absent heartbeat never
// corrupts state (the lease simply expires and its permits are recycled).
type heartbeatState struct {
	mu     sync.Mutex
	leases map[string]*Lease
	stop   map[string]chan struct{}
}

func newHeartbeatState() *heartbeatState {
	return &heartbeatState{leases: map[string]*Lease{}, stop: map[string]chan struct{}{}}
}

func (i *Instance) startLeaseHeartbeat(l *Lease) { i.heartbeat(l) }

func (i *Instance) stopLeaseHeartbeat(id string) {
	i.hbMu.Lock()
	if ch, ok := i.hb.stop[id]; ok {
		close(ch)
		delete(i.hb.stop, id)
	}
	delete(i.hb.leases, id)
	i.hbMu.Unlock()
}

func (i *Instance) replaceHeartbeat(id string, l *Lease) {
	if l == nil {
		return
	}
	i.hbMu.Lock()
	i.hb.leases[id] = l
	i.hbMu.Unlock()
}

func (i *Instance) heartbeat(l *Lease) {
	i.hbMu.Lock()
	i.hb.leases[l.HolderID] = l
	ch, existed := i.hb.stop[l.HolderID]
	if !existed {
		ch = make(chan struct{})
		i.hb.stop[l.HolderID] = ch
	}
	i.hbMu.Unlock()
	if !existed {
		go i.heartbeatLoop(l.HolderID, ch)
	}
}

func (i *Instance) heartbeatLoop(id string, stop chan struct{}) {
	interval := i.cfg.DefaultLeaseTTL / 3
	if interval <= 0 {
		interval = time.Second
	}
	for {
		wait := make(chan struct{}, 1)
		t := i.cfg.Clock.AfterFunc(interval, func() {
			select {
			case wait <- struct{}{}:
			default:
			}
		})
		select {
		case <-stop:
			t.Stop()
			return
		case <-i.closed:
			t.Stop()
			return
		case <-wait:
		}
		i.hbMu.Lock()
		l := i.hb.leases[id]
		i.hbMu.Unlock()
		if l == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), i.cfg.DefaultLeaseTTL)
		nl, err := i.Renew(ctx, l, i.cfg.DefaultLeaseTTL)
		cancel()
		if err != nil || nl == nil {
			i.stopLeaseHeartbeat(id)
			return
		}
		i.replaceHeartbeat(id, nl)
	}
}

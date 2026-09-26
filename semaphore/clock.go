package semaphore

import "time"

// Clock abstracts time and tickers for deterministic tests.
type Clock interface {
	Now() time.Time
	AfterFunc(d time.Duration, f func()) timer
	NewTicker(d time.Duration) ticker
}

type timer interface {
	Stop() bool
	Reset(d time.Duration) bool
}

type ticker interface {
	Chan() <-chan time.Time
	Stop()
}

// ---- real clock ----

type realClock struct{}

func (realClock) Now() time.Time                            { return time.Now() }
func (realClock) AfterFunc(d time.Duration, f func()) timer { return time.AfterFunc(d, f) }
func (realClock) NewTicker(d time.Duration) ticker          { return &realTicker{t: time.NewTicker(d)} }

type realTicker struct{ t *time.Ticker }

func (r *realTicker) Chan() <-chan time.Time { return r.t.C }
func (r *realTicker) Stop()                  { r.t.Stop() }

// ---- fake clock ----

type fakeClock struct {
	c       chan time.Time
	tickGap time.Duration

	mu      chan struct{} // mutex
	now     time.Time
	pending []fakeTimer
	seq     int64
}

type fakeTimer struct {
	at    time.Time
	fn    func()
	seq   int64
	fired bool
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start, c: make(chan time.Time, 1024), mu: make(chan struct{}, 1)}
}

func (c *fakeClock) lock()   { c.mu <- struct{}{} }
func (c *fakeClock) unlock() { <-c.mu }

func (c *fakeClock) Now() time.Time {
	c.lock()
	defer c.unlock()
	return c.now
}

func (c *fakeClock) AfterFunc(d time.Duration, f func()) timer {
	c.lock()
	defer c.unlock()
	c.seq++
	e := &fakeTimerEntry{clock: c, seq: c.seq}
	e.t = fakeTimer{at: c.now.Add(d), fn: f, seq: c.seq}
	c.pending = append(c.pending, e.t)
	return e
}

func (c *fakeClock) NewTicker(d time.Duration) ticker {
	c.lock()
	c.tickGap = d
	c.unlock()
	ft := &fakeTicker{clock: c, ch: c.c, gap: d}
	return ft
}

type fakeTicker struct {
	clock *fakeClock
	ch    chan time.Time
	gap   time.Duration
	stop  bool
}

func (t *fakeTicker) Chan() <-chan time.Time { return t.ch }
func (t *fakeTicker) Stop()                  { t.stop = true }

// Advance moves time forward, firing due one-shot timers and queueing ticker
// ticks, in chronological order.
func (c *fakeClock) Advance(d time.Duration) {
	c.lock()
	target := c.now.Add(d)
	gap := c.tickGap
	c.now = target
	// collect due timers
	type fire struct {
		at time.Time
		fn func()
	}
	var fires []fire
	kept := c.pending[:0]
	for _, t := range c.pending {
		if !t.fired && !t.at.After(target) {
			fires = append(fires, fire{t.at, t.fn})
			continue
		}
		kept = append(kept, t)
	}
	c.pending = kept
	if gap > 0 {
		// Number of ticks that should be published.
	}
	c.unlock()

	// fire one-shots (timers may reschedule via AfterFunc, but those new
	// timers are scheduled at the advanced "now" and are not fired here).
	for _, fr := range fires {
		fr.fn()
	}
	// publish a single tick; tick loop may call Advance again for more.
	if gap > 0 {
		select {
		case c.c <- target:
		default:
		}
	}
}

type fakeTimerEntry struct {
	clock *fakeClock
	t     fakeTimer
	seq   int64
}

func (e *fakeTimerEntry) Stop() bool {
	e.clock.lock()
	defer e.clock.unlock()
	for i := range e.clock.pending {
		if e.clock.pending[i].seq == e.seq {
			was := !e.clock.pending[i].fired
			e.clock.pending[i].fired = true
			return was
		}
	}
	return false
}

func (e *fakeTimerEntry) Reset(d time.Duration) bool {
	e.clock.lock()
	defer e.clock.unlock()
	for i := range e.clock.pending {
		if e.clock.pending[i].seq == e.seq {
			was := !e.clock.pending[i].fired
			e.clock.pending[i].at = e.clock.now.Add(d)
			e.clock.pending[i].fired = false
			return was
		}
	}
	return false
}

var (
	_ Clock  = realClock{}
	_ Clock  = (*fakeClock)(nil)
	_ ticker = (*fakeTicker)(nil)
)

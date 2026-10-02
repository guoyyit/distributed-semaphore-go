package semaphore

import "sync"

import "time"

// journalJob is one committed batch queued for the per-name writer.
type journalJob struct {
	evs  []Event
	done chan error
}

// enqueueResult is returned by enqueue: the caller releases its state mutex
// and then calls wait(), so the writer never blocks against that mutex.
type enqueueResult struct {
	done chan error
}

// journal is the per-name group-commit durability pipeline.
//
// Each semaphore name has exactly one writer goroutine. State transitions
// are planned and applied under the engine semaphore mutex, then appended to
// the journal queue in Seq order; the caller blocks until its batch is
// durable. The writer coalesces every batch queued since it last drained
// into a single write+fsync, so N racing operations on one name cost one
// fsync instead of N. Different names use independent writers and never block
// each other.
//
// Snapshot compaction is performed by the same per-name writer, but only on
// the next idle turn after durability was signaled. By then every submitter
// has released its state mutex, and no appends are in flight, so snapshot +
// log rewrite never interleave with event writes and never self-deadlock.
type journal struct {
	mu      sync.Mutex
	pending map[string][]journalJob
	needCC  map[string]bool
	cond    *sync.Cond
	started map[string]bool
	closed  bool
	wg      sync.WaitGroup

	appendBatch  func(name string, evs []Event) error
	maybeCompact func(name string)
}

func newJournal(appendBatch func(name string, evs []Event) error, maybeCompact func(name string)) *journal {
	j := &journal{
		pending:      make(map[string][]journalJob),
		needCC:       make(map[string]bool),
		started:      make(map[string]bool),
		appendBatch:  appendBatch,
		maybeCompact: maybeCompact,
	}
	j.cond = sync.NewCond(&j.mu)
	return j
}

// enqueue queues one batch. Called while the engine semaphore mutex is held,
// so queue order is exactly Seq order. The caller must release the state
// mutex and then call wait() on the returned result.
func (j *journal) enqueue(name string, evs []Event) enqueueResult {
	if len(evs) == 0 {
		return enqueueResult{}
	}
	job := journalJob{evs: evs, done: make(chan error, 1)}
	j.mu.Lock()
	if j.closed {
		j.mu.Unlock()
		job.done <- ErrClosed
		return enqueueResult{done: job.done}
	}
	j.pending[name] = append(j.pending[name], job)
	if !j.started[name] {
		j.started[name] = true
		j.wg.Add(1)
		go j.run(name)
	}
	j.cond.Broadcast()
	j.mu.Unlock()
	return enqueueResult{done: job.done}
}

// wait blocks until the enqueued batch is durable.
func (r enqueueResult) wait() error {
	if r.done == nil {
		return nil
	}
	return <-r.done
}

// run is the single durability writer for one name.
func (j *journal) run(name string) {
	defer j.wg.Done()
	for {
		j.mu.Lock()
		for !j.closed && len(j.pending[name]) == 0 && !j.needCC[name] {
			j.cond.Wait()
		}
		closed := j.closed
		jobs := j.pending[name]
		j.pending[name] = nil
		needCC := j.needCC[name]
		j.needCC[name] = false
		j.mu.Unlock()

		if closed {
			return
		}
		if len(jobs) > 0 {
			var all []Event
			for _, job := range jobs {
				all = append(all, job.evs...)
			}
			err := j.appendBatch(name, all)
			for _, job := range jobs {
				job.done <- err
			}
			// Defer compaction to the next idle turn so submitters have
			// released their state mutex before the snapshot is taken.
			if err == nil && j.maybeCompact != nil {
				j.mu.Lock()
				j.needCC[name] = true
				j.cond.Broadcast()
				j.mu.Unlock()
			}
			continue
		}
		if needCC && j.maybeCompact != nil {
			// We are idle (no in-flight batch); yield so any submitter that
			// was unblocked by the previous drain can release its state
			// mutex before the snapshot takes it.
			time.Sleep(time.Millisecond)
			j.maybeCompact(name)
		}
	}
}

// close stops accepting batches, wakes all writers and waits for queued
// batches to be flushed so Close never loses a completed grant.
func (j *journal) close() {
	j.mu.Lock()
	j.closed = true
	j.cond.Broadcast()
	j.mu.Unlock()
	j.wg.Wait()
}

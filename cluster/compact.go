package cluster

import (
	"encoding/json"

	"github.com/guoyyit/distributed-semaphore-go/store"
)

// compactLocked durably snapshots the committed state machine up through
// commitIndex and rewrites the WAL to retain only the post-snapshot suffix.
// It runs on a node that is caught up (callers serialize on the run-loop).
// Recovery (NewInstance) restores the snapshot first and then replays the
// suffix, so permits, grants and waiters survive a restart either way.
func (n *Instance) compactLocked() error {
	if n.commitIndex == 0 {
		return nil
	}
	snapTerm := n.termAtLocked(n.commitIndex)
	if err := n.snap.Save(n.machine.Snapshot(), n.commitIndex, snapTerm); err != nil {
		return err
	}
	var kept []store.Record
	var keptLog []semaphoreEntry
	for _, e := range n.log {
		if e.Index <= n.commitIndex {
			continue
		}
		data, err := json.Marshal(e.Event)
		if err != nil {
			return err
		}
		kept = append(kept, store.Record{Index: e.Index, Term: e.Term, Data: data})
		keptLog = append(keptLog, e)
	}
	if err := n.wal.RewriteFrom(kept); err != nil {
		return err
	}
	n.log = keptLog
	return nil
}

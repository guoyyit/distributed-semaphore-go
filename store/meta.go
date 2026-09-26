package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

const metaName = "meta.json"

// Meta records the durable Raft term/vote needed to prevent stale leaders.
type Meta struct {
	mu          sync.Mutex
	dir         string
	Term        uint64 `json:"term"`
	VotedFor    string `json:"voted_for"`
	CommitIndex uint64 `json:"commit_index"`
}

// OpenMeta loads (or creates) the term/vote metadata in dir.
func OpenMeta(dir string) (*Meta, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	m := &Meta{dir: dir}
	data, err := os.ReadFile(filepath.Join(dir, metaName))
	if err == nil {
		if jerr := json.Unmarshal(data, m); jerr != nil {
			return nil, jerr
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	return m, nil
}

// SaveCommit atomically persists the committed high-water mark.
func (m *Meta) SaveCommit(index uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.CommitIndex = index
	return m.writeLocked()
}

// SaveTermVote atomically persists the current term and vote.
func (m *Meta) SaveTermVote(term uint64, votedFor string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Term = term
	m.VotedFor = votedFor
	return m.writeLocked()
}

func (m *Meta) writeLocked() error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	path := filepath.Join(m.dir, metaName)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

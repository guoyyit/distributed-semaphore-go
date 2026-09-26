package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	snapName = "snapshot.bin"
	// 8-byte index, 8-byte term, then arbitrary machine payload.
	snapHeader = 16
)

// ErrNoSnapshot means no snapshot exists yet.
var ErrNoSnapshot = errors.New("store: no snapshot")

// Snapshot persists a compacted state-machine image atomically.
type Snapshot struct {
	mu  sync.Mutex
	dir string
}

// OpenSnapshot prepares snapshot storage inside dir.
func OpenSnapshot(dir string) (*Snapshot, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Snapshot{dir: dir}, nil
}

// Save atomically writes a new snapshot covering through lastIndex/lastTerm.
func (s *Snapshot) Save(data []byte, lastIndex, lastTerm uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := filepath.Join(s.dir, snapName)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	var hdr [snapHeader]byte
	binary.LittleEndian.PutUint64(hdr[0:8], lastIndex)
	binary.LittleEndian.PutUint64(hdr[8:16], lastTerm)
	if _, err := f.Write(hdr[:]); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Load reads the latest snapshot, if any.
func (s *Snapshot) Load() (data []byte, lastIndex, lastTerm uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.Open(filepath.Join(s.dir, snapName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, 0, ErrNoSnapshot
		}
		return nil, 0, 0, err
	}
	defer f.Close()
	var hdr [snapHeader]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		return nil, 0, 0, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	lastIndex = binary.LittleEndian.Uint64(hdr[0:8])
	lastTerm = binary.LittleEndian.Uint64(hdr[8:16])
	data, err = io.ReadAll(f)
	if err != nil {
		return nil, 0, 0, err
	}
	return data, lastIndex, lastTerm, nil
}

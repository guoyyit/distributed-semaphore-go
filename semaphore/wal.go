package semaphore

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// Per-named-semaphore WAL layout inside an instance's DataDir:
//
//	snapshot-<name>.json - latest compacted snapshot for that semaphore
//	events-<name>.log    - events with Seq greater than the snapshot's Seq
//
// State of a semaphore = its snapshot + its subsequent events. Every append
// is fsynced before the caller applies the events to memory, so an instance
// crash never loses a granted permit nor keeps a released one.
type wal struct {
	dir string
	mu  sync.Mutex
}

type snapshotEnvelope struct {
	Version int      `json:"version"`
	Snap    Snapshot `json:"snapshot"`
}

func openWAL(dir string) (*wal, error) {
	if dir == "" {
		return &wal{}, nil // in-memory mode: durable no-op
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &wal{dir: dir}, nil
}

func (w *wal) eventFile(name string) string {
	return filepath.Join(w.dir, "events-"+sanitize(name)+".log")
}

func (w *wal) snapFile(name string) string {
	return filepath.Join(w.dir, "snapshot-"+sanitize(name)+".json")
}

func (w *wal) append(evs []Event) error {
	if len(evs) == 0 || w.dir == "" {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	// Group by name; within each group sequences are contiguous.
	byName := map[string][]Event{}
	var order []string
	for _, ev := range evs {
		if _, ok := byName[ev.Name]; !ok {
			order = append(order, ev.Name)
		}
		byName[ev.Name] = append(byName[ev.Name], ev)
	}
	for _, name := range order {
		if err := w.appendName(name, byName[name]); err != nil {
			return err
		}
	}
	return nil
}

func (w *wal) appendName(name string, evs []Event) error {
	f, err := os.OpenFile(w.eventFile(name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	bw := bufio.NewWriter(f)
	for _, ev := range evs {
		b, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		if _, err := bw.Write(b); err != nil {
			return err
		}
		if err := bw.WriteByte('\n'); err != nil {
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	return f.Sync()
}

// sinceSnapshot returns the maximum post-snapshot event count across names.
func (w *wal) sinceSnapshot() int {
	if w.dir == "" {
		return 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	max := 0
	for _, name := range w.listNames() {
		_, evs, err := w.readName(name)
		if err != nil {
			continue
		}
		if len(evs) > max {
			max = len(evs)
		}
	}
	return max
}

// saveSnapshot atomically stores s and rewrites that name's event log to
// contain only events after s.Seq.
func (w *wal) saveSnapshot(s Snapshot) error {
	if w.dir == "" {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	b, err := json.Marshal(snapshotEnvelope{Version: 1, Snap: s})
	if err != nil {
		return err
	}
	path := w.snapFile(s.Name)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	_, evs, err := w.readName(s.Name)
	if err != nil {
		return err
	}
	var kept []Event
	for _, ev := range evs {
		if ev.Seq > s.Seq {
			kept = append(kept, ev)
		}
	}
	return w.rewriteEvents(s.Name, kept)
}

func (w *wal) rewriteEvents(name string, evs []Event) error {
	path := w.eventFile(name)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(f)
	for _, ev := range evs {
		eb, err := json.Marshal(ev)
		if err != nil {
			f.Close()
			return err
		}
		if _, err := bw.Write(eb); err != nil {
			f.Close()
			return err
		}
		if err := bw.WriteByte('\n'); err != nil {
			f.Close()
			return err
		}
	}
	if err := bw.Flush(); err != nil {
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

// loadAll returns snapshots and post-snapshot events for every semaphore.
func (w *wal) loadAll() ([]Snapshot, []Event, error) {
	if w.dir == "" {
		return nil, nil, nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	var snaps []Snapshot
	var evs []Event
	for _, name := range w.listNames() {
		snap, nameEvs, err := w.readName(name)
		if err != nil {
			return nil, nil, err
		}
		if snap != nil {
			snaps = append(snaps, *snap)
		}
		evs = append(evs, nameEvs...)
	}
	return snaps, evs, nil
}

// listNames derives semaphore names from snapshot files (always present once
// a semaphore was created), falling back to event logs.
func (w *wal) listNames() []string {
	seen := map[string]bool{}
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil
	}
	for _, e := range entries {
		n := e.Name()
		if strings.HasPrefix(n, "snapshot-") && strings.HasSuffix(n, ".json") {
			core := strings.TrimSuffix(strings.TrimPrefix(n, "snapshot-"), ".json")
			if name, err := unsanitize(core); err == nil {
				seen[name] = true
			}
		}
		if strings.HasPrefix(n, "events-") && strings.HasSuffix(n, ".log") {
			core := strings.TrimSuffix(strings.TrimPrefix(n, "events-"), ".log")
			if name, err := unsanitize(core); err == nil {
				seen[name] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func (w *wal) readName(name string) (*Snapshot, []Event, error) {
	var snap *Snapshot
	if b, err := os.ReadFile(w.snapFile(name)); err == nil {
		var env snapshotEnvelope
		if err := json.Unmarshal(b, &env); err != nil {
			return nil, nil, fmt.Errorf("decode snapshot %s: %w", name, err)
		}
		s := env.Snap
		snap = &s
	} else if !os.IsNotExist(err) {
		return nil, nil, err
	}
	f, err := os.Open(w.eventFile(name))
	if err != nil {
		if os.IsNotExist(err) {
			return snap, nil, nil
		}
		return nil, nil, err
	}
	defer f.Close()
	var evs []Event
	dec := json.NewDecoder(bufio.NewReader(f))
	for {
		var ev Event
		if err := dec.Decode(&ev); err != nil {
			if err == io.EOF {
				break
			}
			return nil, nil, fmt.Errorf("decode event %s: %w", name, err)
		}
		if snap != nil && ev.Seq <= snap.Seq {
			continue
		}
		evs = append(evs, ev)
	}
	return snap, evs, nil
}

func (w *wal) Close() error { return nil }

// sanitize maps an arbitrary semaphore name to a filesystem-safe token.
func sanitize(name string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			fmt.Fprintf(&b, "_%02x", r)
		}
	}
	return b.String()
}

func unsanitize(tok string) (string, error) {
	var b strings.Builder
	for i := 0; i < len(tok); i++ {
		if tok[i] == '_' && i+2 < len(tok) {
			var r rune
			if _, err := fmt.Sscanf(tok[i+1:i+3], "%02x", &r); err == nil {
				b.WriteRune(r)
				i += 2
				continue
			}
		}
		b.WriteByte(tok[i])
	}
	return b.String(), nil
}

// readNameExported exposes post-snapshot events for snapshot policy checks.
func (w *wal) readNameExported(name string) (*Snapshot, []Event, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.readName(name)
}

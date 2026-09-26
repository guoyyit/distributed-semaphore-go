// Package store provides durable WAL + snapshot persistence used to rebuild a
// semaphore instance after restart.
package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	walName    = "wal.log"
	walMagic   = 0x53454D31        // "SEM1"
	headerSize = 4 + 8 + 8 + 8 + 4 // magic(4) index(8) term(8) len(8) crc(4)
)

// ErrCorrupt is returned when a WAL record fails its checksum or framing.
var ErrCorrupt = errors.New("store: corrupt wal record")

// Record is one persisted event with its cluster-assigned term and index.
type Record struct {
	Index uint64
	Term  uint64
	Data  []byte // JSON-encoded semaphore.Event
}

// WAL is an append-only, fsynced write-ahead log.
type WAL struct {
	mu  sync.Mutex
	dir string
	f   *os.File
	// last index persisted.
	lastIndex uint64
}

// OpenWAL opens (or creates) the WAL in dir and replays existing records.
// A torn tail from a crash is truncated when it fails validation; the last
// fully fsynced record is the recovery point.
func OpenWAL(dir string) (*WAL, []Record, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, err
	}
	path := filepath.Join(dir, walName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, err
	}
	recs, validBytes, err := readAll(f)
	if err != nil {
		return nil, nil, err
	}
	// Trim any torn tail.
	if err := f.Truncate(validBytes); err != nil {
		f.Close()
		return nil, nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, nil, err
	}
	w := &WAL{dir: dir, f: f}
	if len(recs) > 0 {
		w.lastIndex = recs[len(recs)-1].Index
	}
	return w, recs, nil
}

// Append writes and fsyncs one record. Indices must be strictly increasing.
func (w *WAL) Append(r Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if r.Index != w.lastIndex+1 {
		return fmt.Errorf("store: out-of-order append index %d, expected %d", r.Index, w.lastIndex+1)
	}
	if err := writeRecord(w.f, r); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	w.lastIndex = r.Index
	return nil
}

// AppendAll writes and fsyncs a contiguous batch as one durability unit.
func (w *WAL) AppendAll(rs []Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, r := range rs {
		if r.Index != w.lastIndex+1 {
			return fmt.Errorf("store: out-of-order append index %d, expected %d", r.Index, w.lastIndex+1)
		}
		if err := writeRecord(w.f, r); err != nil {
			return err
		}
		w.lastIndex = r.Index
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	return nil
}

// RewriteFrom replaces the log with rs (used after restoring a snapshot and
// keeping only the post-snapshot suffix). Callers serialize this on node stop.
func (w *WAL) RewriteFrom(rs []Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.f.Close(); err != nil {
		return err
	}
	path := filepath.Join(w.dir, walName)
	tmp := path + ".tmp"
	tf, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	var last uint64
	for _, r := range rs {
		if err := writeRecord(tf, r); err != nil {
			tf.Close()
			return err
		}
		last = r.Index
	}
	if err := tf.Sync(); err != nil {
		tf.Close()
		return err
	}
	if err := tf.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return err
	}
	w.f = f
	w.lastIndex = last
	return nil
}

// LastIndex returns the highest persisted index.
func (w *WAL) LastIndex() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastIndex
}

// Close flushes and closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Sync()
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	w.f = nil
	return err
}

func writeRecord(f *os.File, r Record) error {
	payload := encodePayload(r)
	var hdr [headerSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], walMagic)
	binary.LittleEndian.PutUint64(hdr[4:12], r.Index)
	binary.LittleEndian.PutUint64(hdr[12:20], r.Term)
	binary.LittleEndian.PutUint64(hdr[20:28], uint64(len(payload)))
	crc := crc32.ChecksumIEEE(payload)
	binary.LittleEndian.PutUint32(hdr[28:32], crc)
	if _, err := f.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := f.Write(payload); err != nil {
		return err
	}
	return nil
}

func encodePayload(r Record) []byte {
	// Payload repeats index/term before data so CRC covers full identity.
	var b bytes.Buffer
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], r.Index)
	b.Write(buf[:])
	binary.LittleEndian.PutUint64(buf[:], r.Term)
	b.Write(buf[:])
	b.Write(r.Data)
	return b.Bytes()
}

// readAll parses records, returning valid records and the byte count of the
// contiguous valid prefix.
func readAll(f *os.File) ([]Record, int64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, 0, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, 0, err
	}
	var recs []Record
	off := int64(0)
	for len(data) > 0 {
		if len(data) < headerSize {
			return recs, off, nil // torn header
		}
		magic := binary.LittleEndian.Uint32(data[0:4])
		if magic != walMagic {
			return nil, off, fmt.Errorf("%w: bad magic", ErrCorrupt)
		}
		index := binary.LittleEndian.Uint64(data[4:12])
		term := binary.LittleEndian.Uint64(data[12:20])
		plen := binary.LittleEndian.Uint64(data[20:28])
		wantCRC := binary.LittleEndian.Uint32(data[28:32])
		if plen > uint64(len(data)-headerSize) {
			return recs, off, nil // truncated payload from a crash
		}
		payload := data[headerSize : headerSize+int(plen)]
		if crc32.ChecksumIEEE(payload) != wantCRC {
			return recs, off, nil // torn payload: keep valid prefix
		}
		if plen < 16 {
			return nil, off, fmt.Errorf("%w: payload too small", ErrCorrupt)
		}
		body := payload[16:]
		recs = append(recs, Record{Index: index, Term: term, Data: append([]byte(nil), body...)})
		step := int64(headerSize + int(plen))
		off += step
		data = data[step:]
	}
	return recs, off, nil
}

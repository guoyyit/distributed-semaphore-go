package store

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWALAppendAndRecover(t *testing.T) {
	dir := t.TempDir()
	w, recs, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatal("fresh WAL should be empty")
	}
	for i := uint64(1); i <= 5; i++ {
		if err := w.Append(Record{Index: i, Term: 1, Data: []byte{byte(i)}}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w2, recs2, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs2) != 5 {
		t.Fatalf("recovered %d records want 5", len(recs2))
	}
	for i, r := range recs2 {
		if r.Index != uint64(i+1) || r.Term != 1 || r.Data[0] != byte(i+1) {
			t.Fatalf("record %d mismatch: %+v", i, r)
		}
	}
	_ = w2.Close()
}

func TestWALTornTailTruncated(t *testing.T) {
	dir := t.TempDir()
	w, _, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 3; i++ {
		if err := w.Append(Record{Index: i, Term: 1, Data: []byte("payload")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash mid-write: append garbage bytes after the valid log.
	f, err := os.OpenFile(filepath.Join(dir, walName), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xde, 0xad, 0xbe, 0xef, 1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	w2, recs, err := OpenWAL(dir)
	if err != nil {
		t.Fatalf("reopen after torn tail: %v", err)
	}
	if len(recs) != 3 {
		t.Fatalf("expected 3 intact records, got %d", len(recs))
	}
	// A subsequent append must land at index 4 (contiguous, hole-free).
	if err := w2.Append(Record{Index: 4, Term: 2, Data: []byte("x")}); err != nil {
		t.Fatalf("append after truncation: %v", err)
	}
	_ = w2.Close()

	w3, recs3, err := OpenWAL(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs3) != 4 || recs3[3].Index != 4 {
		t.Fatalf("post-recovery log wrong: %+v", recs3)
	}
	_ = w3.Close()
}

func TestWALRejectsOutOfOrder(t *testing.T) {
	dir := t.TempDir()
	w, _, _ := OpenWAL(dir)
	if err := w.Append(Record{Index: 1, Term: 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Index: 3, Term: 1}); err == nil {
		t.Fatal("expected out-of-order rejection")
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenSnapshot(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := s.Load(); err != ErrNoSnapshot {
		t.Fatalf("empty snapshot err=%v want ErrNoSnapshot", err)
	}
	if err := s.Save([]byte("image"), 7, 2); err != nil {
		t.Fatal(err)
	}
	data, idx, term, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "image" || idx != 7 || term != 2 {
		t.Fatalf("snapshot mismatch: %q idx=%d term=%d", data, idx, term)
	}
}

func TestMetaTermVoteCommit(t *testing.T) {
	dir := t.TempDir()
	m, err := OpenMeta(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.SaveTermVote(3, "n1"); err != nil {
		t.Fatal(err)
	}
	if err := m.SaveCommit(9); err != nil {
		t.Fatal(err)
	}
	m2, err := OpenMeta(dir)
	if err != nil {
		t.Fatal(err)
	}
	if m2.Term != 3 || m2.VotedFor != "n1" || m2.CommitIndex != 9 {
		t.Fatalf("meta mismatch: %+v", m2)
	}
}

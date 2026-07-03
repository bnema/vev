package kv

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func closeStore(t *testing.T, s *Store) {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSetGetDeleteRange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.kv")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer closeStore(t, s)

	if err := s.Set([]byte("b"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	if err := s.Set([]byte("a"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	got, ok := s.Get([]byte("a"))
	if !ok || string(got) != "one" {
		t.Fatalf("Get(a) = %q, %v", got, ok)
	}
	got[0] = 'X'
	got, _ = s.Get([]byte("a"))
	if string(got) != "one" {
		t.Fatalf("Get returned mutable value: %q", got)
	}
	if err := s.Delete([]byte("b")); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Get([]byte("b")); ok {
		t.Fatal("deleted key still present")
	}

	seen := map[string]string{}
	s.Range(func(k, v []byte) bool {
		seen[string(k)] = string(v)
		return true
	})
	if len(seen) != 1 || seen["a"] != "one" {
		t.Fatalf("Range saw %#v", seen)
	}
}

func TestReopenDurability(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.kv")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer closeStore(t, s)
	got, ok := s.Get([]byte("k"))
	if !ok || string(got) != "v" {
		t.Fatalf("after reopen Get(k) = %q, %v", got, ok)
	}
}

func TestCompactionShrinksWithDataIntact(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.kv")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Set([]byte("keep"), bytes.Repeat([]byte("x"), 128)); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	before := info.Size()
	if err := s.Set([]byte("keep"), []byte("y")); err != nil {
		t.Fatal(err)
	}
	info, err = os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	after := info.Size()
	if after >= before {
		t.Fatalf("expected compaction to shrink file: before=%d after=%d", before, after)
	}
	got, ok := s.Get([]byte("keep"))
	if !ok || string(got) != "y" {
		t.Fatalf("compacted value = %q, %v", got, ok)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenTruncatesMidRecordTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.kv")
	first, _ := encodeRecord(opSet, []byte("a"), []byte("one"))
	second, _ := encodeRecord(opSet, []byte("b"), []byte("two"))
	if err := os.WriteFile(path, append(first, second[:len(second)/2]...), 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer closeStore(t, s)
	if got, ok := s.Get([]byte("a")); !ok || string(got) != "one" {
		t.Fatalf("good record missing: %q %v", got, ok)
	}
	if _, ok := s.Get([]byte("b")); ok {
		t.Fatal("torn record was replayed")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != int64(len(first)) {
		t.Fatalf("tail not truncated: size=%d want=%d", info.Size(), len(first))
	}
}

func TestCRCcorruptionOfLastRecordDropsIt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.kv")
	first, _ := encodeRecord(opSet, []byte("k"), []byte("old"))
	second, _ := encodeRecord(opSet, []byte("k"), []byte("new"))
	buf := append(append([]byte{}, first...), second...)
	buf[len(first)+headerLen+payloadPrefixLen+1] ^= 0xff
	if err := os.WriteFile(path, buf, 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer closeStore(t, s)
	got, ok := s.Get([]byte("k"))
	if !ok || string(got) != "old" {
		t.Fatalf("corrupt last record not dropped: %q %v", got, ok)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != int64(len(first)) {
		t.Fatalf("corrupt tail not truncated: size=%d want=%d", info.Size(), len(first))
	}
}

func TestAbsentAndEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "store.kv")
	m, err := Replay(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 0 {
		t.Fatalf("Replay(absent) = %#v", m)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	m, err = Replay(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 0 {
		t.Fatalf("Replay(empty) = %#v", m)
	}
}

func TestTombstoneSurvivesCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.kv")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Set([]byte("dead"), bytes.Repeat([]byte("d"), 128)); err != nil {
		t.Fatal(err)
	}
	if err := s.Set([]byte("keep"), bytes.Repeat([]byte("k"), 128)); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete([]byte("dead")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer closeStore(t, s)
	if _, ok := s.Get([]byte("dead")); ok {
		t.Fatal("deleted key resurrected after compaction/reopen")
	}
	if got, ok := s.Get([]byte("keep")); !ok || len(got) != 128 || got[0] != 'k' {
		t.Fatalf("live key missing after compaction/reopen: len=%d ok=%v", len(got), ok)
	}
}

func TestReplayStopsAtBadPayloadLengthWithoutAllocatingHugeBuffer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.kv")
	first, _ := encodeRecord(opSet, []byte("a"), []byte("one"))
	header := make([]byte, headerLen)
	binary.BigEndian.PutUint32(header[0:4], 1<<31)
	if err := os.WriteFile(path, append(first, header...), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer closeStore(t, s)
	if got, ok := s.Get([]byte("a")); !ok || string(got) != "one" {
		t.Fatalf("good record missing before bad length: %q %v", got, ok)
	}
}

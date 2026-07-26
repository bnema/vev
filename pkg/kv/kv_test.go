package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/bnema/vev/pkg/safedir"
)

var compactionFaultPoints = []string{
	"before-write-next", "after-write-next", "before-sync-next", "after-sync-next",
	"after-sync-dir-next", "after-remove-stale-prev", "after-rename-current-prev",
	"after-sync-dir-prev", "after-rename-next-current", "after-sync-dir-current",
	"before-reopen-current", "after-reopen-current", "after-remove-prev", "after-final-sync-dir",
}

func privatePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "vev", "store.kv")
}

// currentFile frames records the way the store writes them today: a file
// header followed by self-checksummed record headers.
func currentFile(records ...[]byte) []byte {
	buf := fileHeader()
	for _, record := range records {
		buf = append(buf, record...)
	}
	return buf
}

// v1File is the previously released VEVK framing: magic and uint16 version,
// followed directly by the same self-checksummed records used today.
func v1File(records ...[]byte) []byte {
	buf := append([]byte(nil), fileMagic[:]...)
	buf = binary.BigEndian.AppendUint16(buf, formatVersionV1)
	for _, record := range records {
		buf = append(buf, record...)
	}
	return buf
}

// legacyFile reproduces the framing written by the released version: no file
// header, and a record header of length plus payload CRC only. Compatibility
// with these files is a hard requirement, so the fixture stays in the tests.
func legacyFile(records ...[]byte) []byte {
	var buf []byte
	for _, record := range records {
		payload := record[recordHeaderLen:]
		header := make([]byte, legacyHeaderLen)
		binary.BigEndian.PutUint32(header[0:4], uint32(len(payload)))
		binary.BigEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(payload))
		buf = append(append(buf, header...), payload...)
	}
	return buf
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := safedir.EnsurePrivate(filepath.Dir(path)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func closeStore(t *testing.T, s *Store) {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSetGetDeleteRange(t *testing.T) {
	path := privatePath(t)
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
	path := privatePath(t)
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
	path := privatePath(t)
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
	path := privatePath(t)
	first, _ := encodeRecord(opSet, []byte("a"), []byte("one"))
	second, _ := encodeRecord(opSet, []byte("b"), []byte("two"))
	if err := safedir.EnsurePrivate(filepath.Dir(path)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, currentFile(first, second[:len(second)/2]), 0o600); err != nil {
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
	if want := int64(fileHeaderLen + len(first)); info.Size() != want {
		t.Fatalf("tail not truncated: size=%d want=%d", info.Size(), want)
	}
}

func TestCRCcorruptionOfLastRecordFailsClosed(t *testing.T) {
	path := privatePath(t)
	first, _ := encodeRecord(opSet, []byte("k"), []byte("old"))
	second, _ := encodeRecord(opSet, []byte("k"), []byte("new"))
	buf := currentFile(first, second)
	buf[fileHeaderLen+len(first)+recordHeaderLen+payloadPrefixLen+1] ^= 0xff
	if err := safedir.EnsurePrivate(filepath.Dir(path)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, buf, 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := Open(path)
	if !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("Open error = %v, want ErrCorruptWAL", err)
	}
	info, statErr := os.Stat(path)
	if statErr != nil {
		t.Fatal(statErr)
	}
	if info.Size() != int64(len(buf)) {
		t.Fatalf("corrupt WAL was mutated: size=%d want=%d", info.Size(), len(buf))
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
	path := privatePath(t)
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

func TestReplayStrict(t *testing.T) {
	valid, err := encodeRecord(opSet, []byte("old"), []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	batch, err := encodeBatch([]BatchChange{{Key: []byte("old"), Delete: true}, {Key: []byte("new"), Value: []byte("value")}})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		wal     []byte
		torn    bool
		corrupt bool
	}{
		{name: "torn-final-header", wal: currentFile(valid, batch[:recordHeaderLen-1]), torn: true},
		{name: "torn-final-payload", wal: currentFile(valid, batch[:len(batch)-1]), torn: true},
		{name: "final-crc", wal: corruptRecord(currentFile(valid, batch), fileHeaderLen+len(valid)+recordHeaderLen), corrupt: true},
		{name: "middle-crc", wal: corruptRecord(currentFile(valid, batch), fileHeaderLen+recordHeaderLen), corrupt: true},
		{name: "invalid-op", wal: currentFile(rawRecord([]byte{0xff, 0, 0})), corrupt: true},
		{name: "impossible-batch-count", wal: currentFile(rawRecord([]byte{opBatch, 0xff, 0xff, 0xff, 0xff})), corrupt: true},
		{name: "duplicate-key-in-batch", wal: currentFile(rawRecord(rawBatchPayload([]BatchChange{{Key: []byte("x")}, {Key: []byte("x"), Delete: true}}))), corrupt: true},
		{name: "trailing-batch-garbage", wal: currentFile(rawRecord(append(rawBatchPayload([]BatchChange{{Key: []byte("x")}}), 0))), corrupt: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := privatePath(t)
			if err := safedir.EnsurePrivate(filepath.Dir(path)); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, tt.wal, 0o600); err != nil {
				t.Fatal(err)
			}
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			result, replayErr := replayFile(f)
			_ = f.Close()
			if tt.corrupt {
				if !errors.Is(replayErr, ErrCorruptWAL) {
					t.Fatalf("replay error = %v, want ErrCorruptWAL", replayErr)
				}
				return
			}
			if replayErr != nil {
				t.Fatal(replayErr)
			}
			if string(result.Data["old"]) != "value" || result.Data["new"] != nil {
				t.Fatalf("torn batch was partially applied: %#v", result.Data)
			}
			if !result.TornTail || result.LastGood != int64(fileHeaderLen+len(valid)) {
				t.Fatalf("result = %+v", result)
			}
		})
	}
}

// TestRecoverCompactionCleansStaleNextRegardlessOfPrev covers the branch
// where current is valid: any stale .next must be removed even when a .prev
// also exists (previously the "nextExists && !prevExists" guard left a .next
// behind whenever .prev was present too, so spec §3's complete-cleanup
// requirement was not met after certain crash-then-retry sequences).
func TestRecoverCompactionCleansStaleNextRegardlessOfPrev(t *testing.T) {
	oldRecord, _ := encodeRecord(opSet, []byte("version"), []byte("old"))
	newRecord, _ := encodeRecord(opSet, []byte("version"), []byte("new"))
	oldFile, newFile := currentFile(oldRecord), currentFile(newRecord)
	tests := []struct {
		name                string
		current, next, prev []byte
	}{
		{name: "current-next-only", current: oldFile, next: newFile},
		{name: "current-next-and-prev", current: oldFile, next: newFile, prev: oldFile},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := privatePath(t)
			if err := safedir.EnsurePrivate(filepath.Dir(path)); err != nil {
				t.Fatal(err)
			}
			for suffix, data := range map[string][]byte{"": tt.current, ".next": tt.next, ".prev": tt.prev} {
				if data != nil {
					if err := os.WriteFile(path+suffix, data, 0o600); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := recoverCompaction(path, defaultFSHooks()); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(path + ".next"); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf(".next survived recovery: err = %v", err)
			}
			if _, err := os.Stat(path + ".prev"); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf(".prev survived recovery: err = %v", err)
			}
			got, err := Replay(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(got["version"]) != "old" {
				t.Fatalf("version = %q, want %q (current must remain authoritative)", got["version"], "old")
			}
		})
	}
}

func TestReplayPreservesWrappedDecodeCause(t *testing.T) {
	path := privatePath(t)
	writeFile(t, path, currentFile(rawRecord([]byte{0xff, 0, 0})))
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	_, err = replayFile(f)
	if !errors.Is(err, ErrCorruptWAL) || !errors.Is(err, errBadRecord) {
		t.Fatalf("replayFile error = %v, want wrapped ErrCorruptWAL and errBadRecord", err)
	}
}

func TestBatchLiveSizeAccountsForRemainderExactly(t *testing.T) {
	data := map[string][]byte{}
	sizes := map[string]int64{}
	var live int64
	applyRecords(data, sizes, &live, []record{
		{op: opSet, key: []byte("a"), value: []byte("one")},
		{op: opSet, key: []byte("b"), value: []byte("two")},
		{op: opSet, key: []byte("c"), value: []byte("three")},
	}, 10)
	if live != 10 || sizes["a"]+sizes["b"]+sizes["c"] != 10 {
		t.Fatalf("live sizes = %v, live = %d, want exact total 10", sizes, live)
	}
}

func TestDecodeBatchRejectsImpossibleCountBeforeAllocating(t *testing.T) {
	payload := []byte{opBatch, 0xff, 0xff, 0xff, 0xff}
	allocs := testing.AllocsPerRun(10, func() {
		if _, err := decodeBatch(payload); !errors.Is(err, errBadRecord) {
			t.Fatalf("decodeBatch error = %v, want errBadRecord", err)
		}
	})
	if allocs != 0 {
		t.Fatalf("decodeBatch allocations = %v, want 0", allocs)
	}
}

func TestBatchAtomicity(t *testing.T) {
	path := privatePath(t)
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer closeStore(t, s)
	if err := s.Set([]byte("old"), []byte("before")); err != nil {
		t.Fatal(err)
	}
	if err := s.Batch(nil); err == nil {
		t.Fatal("empty batch succeeded")
	}
	if err := s.Batch([]BatchChange{{Key: []byte("duplicate")}, {Key: []byte("duplicate"), Delete: true}}); err == nil {
		t.Fatal("duplicate-key batch succeeded")
	}
	if err := s.Batch([]BatchChange{{Key: []byte("old"), Delete: true}, {Key: []byte("new"), Value: []byte("same-incarnation")}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Get([]byte("old")); ok {
		t.Fatal("old key remains")
	}
	got, ok := s.Get([]byte("new"))
	if !ok || !bytes.Equal(got, []byte("same-incarnation")) {
		t.Fatalf("new = %q, %v", got, ok)
	}
}

func TestCompactionRecoveryMatrix(t *testing.T) {
	oldRecord, _ := encodeRecord(opSet, []byte("version"), []byte("old"))
	newRecord, _ := encodeRecord(opSet, []byte("version"), []byte("new"))
	oldFile, newFile := currentFile(oldRecord), currentFile(newRecord)
	tests := []struct {
		name                string
		current, next, prev []byte
		want                string
		wantErr             bool
	}{
		{name: "current-alone", current: oldFile, want: "old"},
		{name: "current-next", current: oldFile, next: newFile, want: "old"},
		{name: "prev-next", prev: oldFile, next: newFile, want: "new"},
		{name: "next-alone", next: newFile, want: "new"},
		{name: "invalid-prev-valid-next", prev: currentFile(rawRecord([]byte{0xff, 0, 0})), next: newFile, want: "new"},
		{name: "valid-prev-invalid-next", prev: oldFile, next: currentFile(rawRecord([]byte{0xff, 0, 0})), want: "old"},
		{name: "prev-torn-next", prev: oldFile, next: newFile[:len(newFile)-1], want: "old"},
		{name: "current-prev", current: newFile, prev: oldFile, want: "new"},
		{name: "invalid-current-prev", current: currentFile(rawRecord([]byte{0xff, 0, 0})), prev: oldFile, wantErr: true},
		{name: "prev-alone", prev: oldFile, want: "old"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := privatePath(t)
			if err := safedir.EnsurePrivate(filepath.Dir(path)); err != nil {
				t.Fatal(err)
			}
			for suffix, data := range map[string][]byte{"": tt.current, ".next": tt.next, ".prev": tt.prev} {
				if data != nil {
					if err := os.WriteFile(path+suffix, data, 0o600); err != nil {
						t.Fatal(err)
					}
				}
			}
			before := candidateBytes(t, path)
			err := recoverCompaction(path, defaultFSHooks())
			if tt.wantErr {
				if !errors.Is(err, ErrCorruptWAL) {
					t.Fatalf("recoverCompaction error = %v, want ErrCorruptWAL", err)
				}
				if got := candidateBytes(t, path); !reflect.DeepEqual(got, before) {
					t.Fatalf("uncertain candidates changed: got %#v, want %#v", got, before)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got, err := Replay(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(got["version"]) != tt.want {
				t.Fatalf("version = %q, want %q", got["version"], tt.want)
			}
		})
	}

	for _, point := range compactionFaultPoints {
		t.Run("fault-"+point, func(t *testing.T) {
			path := privatePath(t)
			if err := safedir.EnsurePrivate(filepath.Dir(path)); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, oldFile, 0o600); err != nil {
				t.Fatal(err)
			}
			s, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			s.data = map[string][]byte{"version": []byte("new")}
			s.hooks.Fault = func(got string) error {
				if got == point {
					return errors.New("injected " + point)
				}
				return nil
			}
			if err := s.compactLocked(); err == nil {
				t.Fatalf("fault %q did not interrupt compaction", point)
			}
			beforeAppend := candidateBytes(t, path)
			if err := s.Set([]byte("later"), []byte("must-not-append")); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("append after compaction fault = %v, want closed store", err)
			}
			requireCandidateBytes(t, path, beforeAppend)
			if err := recoverCompaction(path, defaultFSHooks()); err != nil {
				t.Fatalf("recovery: %v", err)
			}
			got, err := Replay(path)
			if err != nil {
				t.Fatal(err)
			}
			version := string(got["version"])
			if (version != "old" && version != "new") || len(got) != 1 {
				t.Fatalf("mixed or empty recovered map: %#v", got)
			}
		})
	}
}

func TestCompactionFilesystemOperationFailuresPoisonAndRecover(t *testing.T) {
	fault := errors.New("injected filesystem failure")
	tests := []struct {
		name      string
		configure func(fsHooks, string) fsHooks
	}{
		{name: "write-next", configure: func(h fsHooks, _ string) fsHooks {
			h.Write = func(*os.File, []byte) (int, error) { return 0, fault }
			return h
		}},
		{name: "sync-next", configure: func(h fsHooks, _ string) fsHooks {
			h.Sync = func(*os.File) error { return fault }
			return h
		}},
		{name: "close-next", configure: func(h fsHooks, _ string) fsHooks {
			calls := 0
			h.Close = func(f *os.File) error {
				calls++
				if calls == 1 {
					return errors.Join(f.Close(), fault)
				}
				return f.Close()
			}
			return h
		}},
		{name: "remove-stale-prev", configure: failRemoveCall(fault, 1)},
		{name: "rename-current-prev", configure: failRenameCall(fault, 1)},
		{name: "sync-dir-next", configure: failSyncDirCall(fault, 1)},
		{name: "sync-dir-prev", configure: failSyncDirCall(fault, 2)},
		{name: "rename-next-current", configure: failRenameCall(fault, 2)},
		{name: "sync-dir-current", configure: failSyncDirCall(fault, 3)},
		{name: "reopen-current", configure: func(h fsHooks, path string) fsHooks {
			open := h.Open
			h.Open = func(name string, flags int, mode os.FileMode) (*os.File, error) {
				if name == path && flags == os.O_RDWR {
					return nil, fault
				}
				return open(name, flags, mode)
			}
			return h
		}},
		{name: "close-old-current", configure: func(h fsHooks, _ string) fsHooks {
			calls := 0
			h.Close = func(f *os.File) error {
				calls++
				if calls == 2 {
					return errors.Join(f.Close(), fault)
				}
				return f.Close()
			}
			return h
		}},
		{name: "remove-prev", configure: failRemoveCall(fault, 2)},
		{name: "sync-dir-final", configure: failSyncDirCall(fault, 4)},
	}
	oldRecord, err := encodeRecord(opSet, []byte("version"), []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	oldFile := currentFile(oldRecord)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := privatePath(t)
			if err := safedir.EnsurePrivate(filepath.Dir(path)); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, oldFile, 0o600); err != nil {
				t.Fatal(err)
			}
			s, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			s.data = map[string][]byte{"version": []byte("new")}
			s.hooks = tt.configure(s.hooks, path)

			err = s.compactLocked()
			if !errors.Is(err, fault) {
				t.Fatalf("compaction error = %v, want injected fault", err)
			}
			beforeAppend := candidateBytes(t, path)
			if err := s.Set([]byte("later"), []byte("must-not-append")); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("append after compaction fault = %v, want closed store", err)
			}
			requireCandidateBytes(t, path, beforeAppend)

			if err := recoverCompaction(path, defaultFSHooks()); err != nil {
				t.Fatalf("recovery: %v", err)
			}
			got, err := Replay(path)
			if err != nil {
				t.Fatal(err)
			}
			version := string(got["version"])
			if (version != "old" && version != "new") || len(got) != 1 {
				t.Fatalf("mixed or empty recovered map: %#v", got)
			}
		})
	}
}

func failRemoveCall(fault error, target int) func(fsHooks, string) fsHooks {
	return func(h fsHooks, _ string) fsHooks {
		remove := h.Remove
		calls := 0
		h.Remove = func(path string) error {
			calls++
			if calls == target {
				return fault
			}
			return remove(path)
		}
		return h
	}
}

func failRenameCall(fault error, target int) func(fsHooks, string) fsHooks {
	return func(h fsHooks, _ string) fsHooks {
		rename := h.Rename
		calls := 0
		h.Rename = func(oldPath, newPath string) error {
			calls++
			if calls == target {
				return fault
			}
			return rename(oldPath, newPath)
		}
		return h
	}
}

func failSyncDirCall(fault error, target int) func(fsHooks, string) fsHooks {
	return func(h fsHooks, _ string) fsHooks {
		sync := h.SyncDir
		calls := 0
		h.SyncDir = func(path string) error {
			calls++
			if calls == target {
				return fault
			}
			return sync(path)
		}
		return h
	}
}

func candidateBytes(t *testing.T, path string) map[string][]byte {
	t.Helper()
	got := make(map[string][]byte, 3)
	for _, suffix := range []string{"", ".next", ".prev"} {
		data, err := os.ReadFile(path + suffix)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		got[suffix] = data
	}
	return got
}

func requireCandidateBytes(t *testing.T, path string, want map[string][]byte) {
	t.Helper()
	got := candidateBytes(t, path)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("catalogue candidates changed after poisoned append: got %#v, want %#v", got, want)
	}
}

func corruptRecord(buf []byte, offset int) []byte { buf[offset] ^= 0xff; return buf }
func rawRecord(payload []byte) []byte             { return framePayload(payload) }
func rawBatchPayload(changes []BatchChange) []byte {
	payload := []byte{opBatch, 0, 0, 0, byte(len(changes))}
	for _, change := range changes {
		op := opSet
		if change.Delete {
			op = opDel
		}
		payload = append(payload, op, 0, byte(len(change.Key)))
		payload = append(payload, change.Key...)
		payload = binary.BigEndian.AppendUint32(payload, uint32(len(change.Value)))
		payload = append(payload, change.Value...)
	}
	return payload
}

// An oversized length must never drive an allocation. Legacy framing is
// accepted only after complete strict replay, while the current header checksum
// proves an oversized length was written that way; both fail closed.
func TestCorruptedCurrentHeaderNeverFallsBackToLegacyOrDeletesPredecessor(t *testing.T) {
	record, _ := encodeRecord(opSet, []byte("version"), []byte("current"))
	previous, _ := encodeRecord(opSet, []byte("version"), []byte("previous"))

	tests := []struct {
		name       string
		current    []byte
		standalone bool
	}{
		{name: "torn file header", current: append([]byte(nil), fileHeader()[:fileHeaderLen-1]...), standalone: true},
		{name: "torn current record", current: currentFile(record)[:fileHeaderLen+recordHeaderLen+1]},
	}
	for i := range fileMagicLen {
		corrupted := currentFile(record)
		corrupted[i] = 0
		tests = append(tests, struct {
			name       string
			current    []byte
			standalone bool
		}{name: fmt.Sprintf("corrupted magic byte %d", i), current: corrupted, standalone: true})
	}
	for length := 1; length < fileHeaderLen; length++ {
		tests = append(tests, struct {
			name       string
			current    []byte
			standalone bool
		}{name: fmt.Sprintf("torn header prefix %d", length), current: append([]byte(nil), fileHeader()[:length]...), standalone: true})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, withCandidates := range []bool{false, true} {
				if !withCandidates && !tt.standalone {
					continue
				}
				name := "standalone"
				if withCandidates {
					name = "with candidates"
				}
				t.Run(name, func(t *testing.T) {
					path := privatePath(t)
					writeFile(t, path, tt.current)
					if withCandidates {
						if err := os.WriteFile(path+".prev", currentFile(previous), 0o600); err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(path+".next", currentFile(record), 0o600); err != nil {
							t.Fatal(err)
						}
					}
					before := candidateBytes(t, path)
					if _, err := Open(path); !errors.Is(err, ErrCorruptWAL) {
						t.Fatalf("Open error = %v, want ErrCorruptWAL", err)
					}
					if got := candidateBytes(t, path); !reflect.DeepEqual(got, before) {
						t.Fatalf("candidates changed: got %#v, want %#v", got, before)
					}
				})
			}
		})
	}
}

func TestReplayRejectsOversizedLengthWithoutAllocating(t *testing.T) {
	first, _ := encodeRecord(opSet, []byte("a"), []byte("one"))
	legacyHeader := make([]byte, legacyHeaderLen)
	binary.BigEndian.PutUint32(legacyHeader[0:4], 1<<31)

	t.Run("legacy incomplete record fails closed", func(t *testing.T) {
		path := privatePath(t)
		wal := append(legacyFile(first), legacyHeader...)
		writeFile(t, path, wal)
		if _, err := Open(path); !errors.Is(err, ErrCorruptWAL) {
			t.Fatalf("Open error = %v, want ErrCorruptWAL", err)
		}
		requireFileBytes(t, path, wal)
	})

	t.Run("current oversized length fails closed", func(t *testing.T) {
		path := privatePath(t)
		wal := currentFile(first, oversizedRecordHeader(maxPayloadLen+1))
		writeFile(t, path, wal)
		if _, err := Open(path); !errors.Is(err, ErrCorruptWAL) {
			t.Fatalf("Open error = %v, want ErrCorruptWAL", err)
		}
		requireFileBytes(t, path, wal)
	})
}

// oversizedRecordHeader is a record header whose own checksum is valid, so the
// length is provably what the writer put there rather than a bit flip.
func oversizedRecordHeader(payloadLen uint32) []byte {
	header := make([]byte, recordHeaderLen)
	binary.BigEndian.PutUint32(header[0:4], payloadLen)
	binary.BigEndian.PutUint32(header[8:recordHeaderLen], crc32.ChecksumIEEE(header[0:8]))
	return header
}

func requireFileBytes(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("file was mutated: got %d bytes, want %d", len(got), len(want))
	}
}

// The regression this format exists for: a bit flip in the length field of an
// early record used to look exactly like a torn tail, so Open truncated every
// record written after it and reopened a smaller, structurally valid file.
func TestCorruptLengthOfEarlyRecordFailsClosedWithoutTruncating(t *testing.T) {
	first, _ := encodeRecord(opSet, []byte("a"), []byte("one"))
	second, _ := encodeRecord(opSet, []byte("b"), []byte("two"))
	third, _ := encodeRecord(opSet, []byte("c"), []byte("three"))

	tests := []struct {
		name   string
		offset int
	}{
		{name: "first-record-length", offset: fileHeaderLen},
		{name: "second-record-length", offset: fileHeaderLen + len(first) + 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := privatePath(t)
			wal := currentFile(first, second, third)
			// Flip a high bit so the length claims far more than the file holds:
			// the exact shape that used to be misread as an incomplete tail.
			wal[tt.offset] ^= 0x40
			writeFile(t, path, wal)

			if _, err := Open(path); !errors.Is(err, ErrCorruptWAL) {
				t.Fatalf("Open error = %v, want ErrCorruptWAL", err)
			}
			requireFileBytes(t, path, wal)
			if _, err := Replay(path); !errors.Is(err, ErrCorruptWAL) {
				t.Fatalf("Replay error = %v, want ErrCorruptWAL", err)
			}
		})
	}
}

// Every row of the replay decision table for the current format. A torn tail is
// the only truncatable outcome.
func TestCurrentFormatReplayDecisionTable(t *testing.T) {
	first, _ := encodeRecord(opSet, []byte("a"), []byte("one"))
	second, _ := encodeRecord(opSet, []byte("b"), []byte("two"))
	invalidOp := rawRecord([]byte{0xff, 0, 0})

	tests := []struct {
		name    string
		wal     []byte
		torn    bool
		corrupt bool
	}{
		{name: "partial-header", wal: currentFile(first, second[:recordHeaderLen-1]), torn: true},
		{name: "header-checksum-mismatch", wal: corruptRecord(currentFile(first, second), fileHeaderLen+len(first)+1), corrupt: true},
		{name: "short-payload", wal: currentFile(first, second[:len(second)-1]), torn: true},
		{name: "payload-checksum-mismatch", wal: corruptRecord(currentFile(first, second), fileHeaderLen+len(first)+recordHeaderLen), corrupt: true},
		{name: "undecodable-payload", wal: currentFile(first, invalidOp), corrupt: true},
		{name: "oversized-length", wal: currentFile(first, oversizedRecordHeader(maxPayloadLen+1)), corrupt: true},
		{name: "complete-records", wal: currentFile(first, second)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := privatePath(t)
			writeFile(t, path, tt.wal)
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			result, err := replayFile(f)
			_ = f.Close()

			if tt.corrupt {
				if !errors.Is(err, ErrCorruptWAL) {
					t.Fatalf("replay error = %v, want ErrCorruptWAL", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if result.Format != formatCurrent {
				t.Fatalf("format = %d, want current", result.Format)
			}
			if result.TornTail != tt.torn {
				t.Fatalf("TornTail = %v, want %v", result.TornTail, tt.torn)
			}
			if string(result.Data["a"]) != "one" {
				t.Fatalf("prefix lost: %#v", result.Data)
			}
			wantLastGood := int64(fileHeaderLen + len(first))
			if !tt.torn {
				wantLastGood += int64(len(second))
			}
			if result.LastGood != wantLastGood {
				t.Fatalf("LastGood = %d, want %d", result.LastGood, wantLastGood)
			}
		})
	}
}

// A file written by the released version must keep working: it opens with the
// original semantics, is upgraded in place, and reopens identically.
func TestVEVKV1FileOpensAndUpgradesInPlace(t *testing.T) {
	setA, _ := encodeRecord(opSet, []byte("a"), []byte("one"))
	setB, _ := encodeRecord(opSet, []byte("b"), bytes.Repeat([]byte("x"), 1<<20))
	for _, tc := range []struct {
		name string
		wal  []byte
	}{
		{name: "ordinary catalogue payload", wal: v1File(setA)},
		{name: "prior large catalogue payload", wal: v1File(setB)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := privatePath(t)
			writeFile(t, path, tc.wal)
			store, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			closeStore(t, store)
			upgraded, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(upgraded[:fileHeaderLen], fileHeader()) {
				t.Fatalf("upgraded header = %x, want %x", upgraded[:fileHeaderLen], fileHeader())
			}
		})
	}
}

func TestVEVKV1RejectsOversizedRecordBeforeAllocation(t *testing.T) {
	path := privatePath(t)
	writeFile(t, path, v1File(oversizedRecordHeader(maxPayloadLen+1)))
	if _, err := Open(path); !errors.Is(err, ErrCorruptWAL) {
		t.Fatalf("Open error = %v, want ErrCorruptWAL", err)
	}
}

func TestLegacyFileOpensAndUpgradesInPlace(t *testing.T) {
	setA, _ := encodeRecord(opSet, []byte("a"), []byte("one"))
	setB, _ := encodeRecord(opSet, []byte("b"), []byte("two"))
	replaceA, _ := encodeRecord(opSet, []byte("a"), []byte("uno"))
	delB, _ := encodeRecord(opDel, []byte("b"), nil)
	torn, _ := encodeRecord(opSet, []byte("c"), []byte("three"))
	tests := []struct {
		name string
		wal  []byte
		want map[string]string
	}{
		{name: "complete", wal: legacyFile(setA, setB, replaceA), want: map[string]string{"a": "uno", "b": "two"}},
		{name: "tombstone", wal: legacyFile(setA, setB, delB), want: map[string]string{"a": "one"}},
		{name: "torn-tail", wal: append(legacyFile(setA, setB), legacyFile(torn)[:legacyHeaderLen+2]...), want: map[string]string{"a": "one", "b": "two"}},
		{name: "empty", wal: nil, want: map[string]string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := privatePath(t)
			writeFile(t, path, tt.wal)

			s, err := Open(path)
			if err != nil {
				t.Fatalf("legacy file failed to open: %v", err)
			}
			requireContents(t, s, tt.want)
			if err := s.Set([]byte("added"), []byte("after-upgrade")); err != nil {
				t.Fatal(err)
			}
			closeStore(t, s)

			upgraded, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.HasPrefix(upgraded, fileMagic[:]) {
				t.Fatal("legacy file was not upgraded in place")
			}
			reopened, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer closeStore(t, reopened)
			want := map[string]string{"added": "after-upgrade"}
			maps.Copy(want, tt.want)
			requireContents(t, reopened, want)
			replayed, err := Replay(path)
			if err != nil {
				t.Fatal(err)
			}
			if len(replayed) != len(want) {
				t.Fatalf("Replay = %#v, want %#v", replayed, want)
			}
		})
	}
}

func requireContents(t *testing.T, s *Store, want map[string]string) {
	t.Helper()
	for key, value := range want {
		got, ok := s.Get([]byte(key))
		if !ok || string(got) != value {
			t.Fatalf("Get(%q) = %q, %v; want %q", key, got, ok, value)
		}
	}
	count := 0
	s.Range(func([]byte, []byte) bool { count++; return true })
	if count != len(want) {
		t.Fatalf("store holds %d keys, want %d", count, len(want))
	}
}

// The upgrade runs through the compaction protocol, so a crash at any of its
// boundaries must leave either the whole legacy file or the whole upgraded one.
func TestLegacyUpgradeCrashSafety(t *testing.T) {
	setA, _ := encodeRecord(opSet, []byte("a"), []byte("one"))
	setB, _ := encodeRecord(opSet, []byte("b"), []byte("two"))
	legacy := legacyFile(setA, setB)
	want := map[string]string{"a": "one", "b": "two"}

	for _, point := range compactionFaultPoints {
		t.Run("fault-"+point, func(t *testing.T) {
			path := privatePath(t)
			writeFile(t, path, legacy)
			hooks := defaultFSHooks()
			hooks.Fault = func(got string) error {
				if got == point {
					return errors.New("injected " + point)
				}
				return nil
			}
			if _, err := openWithHooks(path, hooks); err == nil {
				t.Fatalf("fault %q did not interrupt the upgrade", point)
			}

			// Restart: recovery selects a complete file, and the data is intact
			// whichever format that file happens to be in.
			s, err := Open(path)
			if err != nil {
				t.Fatalf("restart after fault %q: %v", point, err)
			}
			requireContents(t, s, want)
			closeStore(t, s)
			upgraded, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.HasPrefix(upgraded, fileMagic[:]) {
				t.Fatal("restart did not complete the upgrade")
			}
		})
	}
}

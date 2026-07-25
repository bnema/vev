package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func privatePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "vev", "store.kv")
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
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
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

func TestCRCcorruptionOfLastRecordFailsClosed(t *testing.T) {
	path := privatePath(t)
	first, _ := encodeRecord(opSet, []byte("k"), []byte("old"))
	second, _ := encodeRecord(opSet, []byte("k"), []byte("new"))
	buf := append(append([]byte{}, first...), second...)
	buf[len(first)+headerLen+payloadPrefixLen+1] ^= 0xff
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
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
		{name: "torn-final-header", wal: append(append([]byte{}, valid...), batch[:headerLen-1]...), torn: true},
		{name: "torn-final-payload", wal: append(append([]byte{}, valid...), batch[:len(batch)-1]...), torn: true},
		{name: "final-crc", wal: corruptRecord(append(append([]byte{}, valid...), batch...), len(valid)+4), corrupt: true},
		{name: "middle-crc", wal: corruptRecord(append(append([]byte{}, valid...), batch...), 4), corrupt: true},
		{name: "invalid-op", wal: rawRecord([]byte{0xff, 0, 0}), corrupt: true},
		{name: "impossible-batch-count", wal: rawRecord([]byte{opBatch, 0xff, 0xff, 0xff, 0xff}), corrupt: true},
		{name: "duplicate-key-in-batch", wal: rawRecord(rawBatchPayload([]BatchChange{{Key: []byte("x")}, {Key: []byte("x"), Delete: true}})), corrupt: true},
		{name: "trailing-batch-garbage", wal: rawRecord(append(rawBatchPayload([]BatchChange{{Key: []byte("x")}}), 0)), corrupt: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := privatePath(t)
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
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
			if !result.TornTail || result.LastGood != int64(len(valid)) {
				t.Fatalf("result = %+v", result)
			}
		})
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
	tests := []struct {
		name                string
		current, next, prev []byte
		want                string
	}{
		{name: "current-alone", current: oldRecord, want: "old"},
		{name: "current-next", current: oldRecord, next: newRecord, want: "old"},
		{name: "prev-next", prev: oldRecord, next: newRecord, want: "new"},
		{name: "prev-invalid-next", prev: oldRecord, next: rawRecord([]byte{0xff, 0, 0}), want: "old"},
		{name: "prev-torn-next", prev: oldRecord, next: newRecord[:len(newRecord)-1], want: "old"},
		{name: "current-prev", current: newRecord, prev: oldRecord, want: "new"},
		{name: "invalid-current-prev", current: rawRecord([]byte{0xff, 0, 0}), prev: oldRecord, want: "old"},
		{name: "prev-alone", prev: oldRecord, want: "old"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := privatePath(t)
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
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
			got, err := Replay(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(got["version"]) != tt.want {
				t.Fatalf("version = %q, want %q", got["version"], tt.want)
			}
		})
	}

	faultPoints := []string{
		"before-write-next", "after-write-next", "before-sync-next", "after-sync-next",
		"after-sync-dir-next", "after-remove-stale-prev", "after-rename-current-prev",
		"after-sync-dir-prev", "after-rename-next-current", "after-sync-dir-current",
		"before-reopen-current", "after-reopen-current", "after-remove-prev", "after-final-sync-dir",
	}
	for _, point := range faultPoints {
		t.Run("fault-"+point, func(t *testing.T) {
			path := privatePath(t)
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, oldRecord, 0o600); err != nil {
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
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := privatePath(t)
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, oldRecord, 0o600); err != nil {
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
func rawRecord(payload []byte) []byte {
	buf := make([]byte, headerLen+len(payload))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(payload))
	copy(buf[8:], payload)
	return buf
}
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

func TestReplayStopsAtBadPayloadLengthWithoutAllocatingHugeBuffer(t *testing.T) {
	path := privatePath(t)
	first, _ := encodeRecord(opSet, []byte("a"), []byte("one"))
	header := make([]byte, headerLen)
	binary.BigEndian.PutUint32(header[0:4], 1<<31)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
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

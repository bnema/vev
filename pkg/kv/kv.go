package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"

	"github.com/bnema/vev/pkg/safedir"
)

// ReplayResult describes a strictly validated WAL prefix.
type ReplayResult struct {
	Data      map[string][]byte
	EntrySize map[string]int64
	Total     int64
	Live      int64
	LastGood  int64
	TornTail  bool
}

type fsHooks struct {
	Open    func(string, int, os.FileMode) (*os.File, error)
	Remove  func(string) error
	Rename  func(string, string) error
	SyncDir func(string) error
	Fault   func(string) error
}

func defaultFSHooks() fsHooks {
	return fsHooks{Open: os.OpenFile, Remove: os.Remove, Rename: os.Rename, SyncDir: syncDir}
}

// Store is an append-only WAL-backed key/value store.
type Store struct {
	mu        sync.Mutex
	path      string
	file      *os.File
	lockFile  *os.File
	data      map[string][]byte
	entrySize map[string]int64
	total     int64
	live      int64
	closed    bool
	hooks     fsHooks
}

// Open opens or creates a store, strictly recovers compaction, and truncates only a torn tail.
func Open(path string) (*Store, error) { return openWithHooks(path, defaultFSHooks()) }

func openWithHooks(path string, hooks fsHooks) (*Store, error) {
	if err := safedir.EnsurePrivate(filepath.Dir(path)); err != nil {
		return nil, err
	}
	lockFile, err := acquireLock(path)
	if err != nil {
		return nil, err
	}
	cleanupLock := func(err error) (*Store, error) { _ = releaseLock(lockFile); return nil, err }
	if err := recoverCompaction(path, hooks); err != nil {
		return cleanupLock(err)
	}
	f, err := hooks.Open(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return cleanupLock(err)
	}
	result, err := replayFile(f)
	if err != nil {
		_ = f.Close()
		return cleanupLock(err)
	}
	if result.TornTail {
		if err := f.Truncate(result.LastGood); err != nil {
			_ = f.Close()
			return cleanupLock(err)
		}
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return cleanupLock(err)
	}
	s := &Store{path: path, file: f, lockFile: lockFile, data: result.Data, entrySize: result.EntrySize, total: result.Total, live: result.Live, hooks: hooks}
	if s.shouldCompact() {
		if err := s.compactLocked(); err != nil {
			_ = f.Close()
			return cleanupLock(err)
		}
	}
	return s, nil
}

// Replay reads path without mutating it and returns only strictly valid state.
func Replay(path string) (map[string][]byte, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string][]byte{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	result, err := replayFile(f)
	if err != nil {
		return nil, err
	}
	return result.Data, nil
}

func (s *Store) Get(key []byte) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[string(key)]
	return append([]byte(nil), v...), ok
}
func (s *Store) Set(key, val []byte) error {
	return s.appendEncoded(func() ([]byte, []record, error) {
		buf, err := encodeRecord(opSet, key, val)
		return buf, []record{{op: opSet, key: append([]byte(nil), key...), value: append([]byte(nil), val...)}}, err
	})
}
func (s *Store) Delete(key []byte) error {
	return s.appendEncoded(func() ([]byte, []record, error) {
		buf, err := encodeRecord(opDel, key, nil)
		return buf, []record{{op: opDel, key: append([]byte(nil), key...)}}, err
	})
}

// Batch appends all changes as one WAL record and applies them only after a complete write.
func (s *Store) Batch(changes []BatchChange) error {
	return s.appendEncoded(func() ([]byte, []record, error) {
		buf, err := encodeBatch(changes)
		if err != nil {
			return nil, nil, err
		}
		recs := make([]record, len(changes))
		for i, change := range changes {
			recs[i] = record{op: opSet, key: append([]byte(nil), change.Key...), value: append([]byte(nil), change.Value...)}
			if change.Delete {
				recs[i].op = opDel
			}
		}
		return buf, recs, nil
	})
}

func (s *Store) Range(fn func(k, v []byte) bool) {
	s.mu.Lock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	pairs := make([]record, 0, len(keys))
	for _, k := range keys {
		pairs = append(pairs, record{key: []byte(k), value: append([]byte(nil), s.data[k]...)})
	}
	s.mu.Unlock()
	for _, pair := range pairs {
		if !fn(pair.key, pair.value) {
			return
		}
	}
}
func (s *Store) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	return s.file.Sync()
}
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	err := errors.Join(s.file.Sync(), s.file.Close(), releaseLock(s.lockFile))
	s.closed = true
	return err
}

func acquireLock(path string) (*os.File, error) {
	f, err := os.OpenFile(path+".lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}
func releaseLock(f *os.File) error {
	if f == nil {
		return nil
	}
	return errors.Join(syscall.Flock(int(f.Fd()), syscall.LOCK_UN), f.Close())
}

func (s *Store) appendEncoded(encode func() ([]byte, []record, error)) error {
	buf, records, err := encode()
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	pos, err := s.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	n, writeErr := s.file.Write(buf)
	if writeErr != nil || n != len(buf) {
		if writeErr == nil {
			writeErr = io.ErrShortWrite
		}
		if err := errors.Join(s.file.Truncate(pos), seekTo(s.file, pos)); err != nil {
			s.closed = true
			return fmt.Errorf("write failed (%w) and WAL repair failed: %w", writeErr, err)
		}
		return writeErr
	}
	applyRecords(s.data, s.entrySize, &s.live, records, int64(len(buf)))
	s.total += int64(len(buf))
	if s.shouldCompact() {
		return s.compactLocked()
	}
	return nil
}
func seekTo(f *os.File, pos int64) error { _, err := f.Seek(pos, io.SeekStart); return err }
func (s *Store) shouldCompact() bool {
	return s.total > compactThreshold && float64(s.total-s.live)/float64(s.total) > compactWasteRatio
}

func (s *Store) compactLocked() (retErr error) {
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, s.poisonLocked())
		}
	}()
	next, prev := s.path+".next", s.path+".prev"
	if err := injectFault(s.hooks, "before-write-next"); err != nil {
		return err
	}
	f, err := s.hooks.Open(next, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		buf, err := encodeRecord(opSet, []byte(key), s.data[key])
		if err != nil {
			_ = f.Close()
			return err
		}
		if n, err := f.Write(buf); err != nil || n != len(buf) {
			_ = f.Close()
			if err == nil {
				err = io.ErrShortWrite
			}
			return err
		}
	}
	if err := injectFault(s.hooks, "after-write-next"); err != nil {
		_ = f.Close()
		return err
	}
	if err := injectFault(s.hooks, "before-sync-next"); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := injectFault(s.hooks, "after-sync-next"); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := validatePath(next); err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	if err := s.hooks.SyncDir(dir); err != nil {
		return err
	}
	if err := injectFault(s.hooks, "after-sync-dir-next"); err != nil {
		return err
	}
	if err := removeIfExists(s.hooks, prev); err != nil {
		return err
	}
	if err := injectFault(s.hooks, "after-remove-stale-prev"); err != nil {
		return err
	}
	if err := s.hooks.Rename(s.path, prev); err != nil {
		return err
	}
	if err := injectFault(s.hooks, "after-rename-current-prev"); err != nil {
		return err
	}
	if err := s.hooks.SyncDir(dir); err != nil {
		return err
	}
	if err := injectFault(s.hooks, "after-sync-dir-prev"); err != nil {
		return err
	}
	if err := s.hooks.Rename(next, s.path); err != nil {
		return err
	}
	if err := injectFault(s.hooks, "after-rename-next-current"); err != nil {
		return err
	}
	if err := s.hooks.SyncDir(dir); err != nil {
		return err
	}
	if err := injectFault(s.hooks, "after-sync-dir-current"); err != nil {
		return err
	}
	if err := injectFault(s.hooks, "before-reopen-current"); err != nil {
		return err
	}
	current, err := s.hooks.Open(s.path, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	if err := injectFault(s.hooks, "after-reopen-current"); err != nil {
		_ = current.Close()
		return err
	}
	result, err := replayFile(current)
	if err != nil || result.TornTail {
		_ = current.Close()
		if err == nil {
			err = ErrCorruptWAL
		}
		return err
	}
	if _, err := current.Seek(0, io.SeekEnd); err != nil {
		_ = current.Close()
		return err
	}
	old := s.file
	s.file = current
	s.data, s.entrySize, s.total, s.live = result.Data, result.EntrySize, result.Total, result.Live
	if err := old.Close(); err != nil {
		return err
	}
	if err := removeIfExists(s.hooks, prev); err != nil {
		return err
	}
	if err := injectFault(s.hooks, "after-remove-prev"); err != nil {
		return err
	}
	if err := s.hooks.SyncDir(dir); err != nil {
		return err
	}
	return injectFault(s.hooks, "after-final-sync-dir")
}

// poisonLocked permanently disables a store whose compaction did not complete.
// The caller holds s.mu, so no later append can retain a descriptor for a
// transitional pathname.
func (s *Store) poisonLocked() error {
	if s.closed {
		return nil
	}
	s.closed = true
	file, lockFile := s.file, s.lockFile
	s.file, s.lockFile = nil, nil
	var fileErr error
	if file != nil {
		fileErr = file.Close()
	}
	return errors.Join(fileErr, releaseLock(lockFile))
}

func replayFile(f *os.File) (ReplayResult, error) {
	info, err := f.Stat()
	if err != nil {
		return ReplayResult{}, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return ReplayResult{}, err
	}
	fileSize := info.Size()
	result := ReplayResult{Data: make(map[string][]byte), EntrySize: make(map[string]int64)}
	header := make([]byte, headerLen)
	for {
		n, err := io.ReadFull(f, header)
		if errors.Is(err, io.EOF) {
			return result, nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			_ = n
			result.TornTail = true
			return result, nil
		}
		if err != nil {
			return ReplayResult{}, err
		}
		payloadLen := binary.BigEndian.Uint32(header[:4])
		if int64(payloadLen) > fileSize-result.LastGood-headerLen {
			result.TornTail = true
			return result, nil
		}
		payload := make([]byte, int(payloadLen))
		if _, err := io.ReadFull(f, payload); errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			result.TornTail = true
			return result, nil
		} else if err != nil {
			return ReplayResult{}, err
		}
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(header[4:8]) {
			return ReplayResult{}, fmt.Errorf("%w: CRC mismatch at offset %d", ErrCorruptWAL, result.LastGood)
		}
		var records []record
		if len(payload) > 0 && payload[0] == opBatch {
			records, err = decodeBatch(payload)
		} else {
			var rec record
			rec, err = decodePayload(payload)
			records = []record{rec}
		}
		if err != nil {
			return ReplayResult{}, fmt.Errorf("%w: invalid record at offset %d: %v", ErrCorruptWAL, result.LastGood, err)
		}
		size := int64(headerLen + len(payload))
		applyRecords(result.Data, result.EntrySize, &result.Live, records, size)
		result.Total += size
		result.LastGood += size
	}
}

func applyRecords(data map[string][]byte, sizes map[string]int64, live *int64, records []record, size int64) {
	setCount := int64(0)
	for _, rec := range records {
		if rec.op == opSet {
			setCount++
		}
	}
	setSize := size
	if setCount > 1 {
		setSize = size / setCount
	}
	for _, rec := range records {
		key := string(rec.key)
		if old, ok := sizes[key]; ok {
			*live -= old
		}
		if rec.op == opSet {
			data[key] = append([]byte(nil), rec.value...)
			sizes[key] = setSize
			*live += setSize
		} else {
			delete(data, key)
			delete(sizes, key)
		}
	}
}

func recoverCompaction(path string, hooks fsHooks) error {
	_, currentExists, currentErr := candidate(path)
	nextResult, nextExists, nextErr := candidate(path + ".next")
	_, prevExists, prevErr := candidate(path + ".prev")
	if nextErr == nil && nextResult.TornTail {
		nextErr = ErrCorruptWAL
	}
	dir := filepath.Dir(path)
	if currentExists && currentErr == nil {
		if nextExists && !prevExists {
			if err := hooks.Remove(path + ".next"); err != nil {
				return err
			}
			if err := hooks.SyncDir(dir); err != nil {
				return err
			}
		}
		if prevExists {
			if err := hooks.Remove(path + ".prev"); err != nil {
				return err
			}
			if err := hooks.SyncDir(dir); err != nil {
				return err
			}
		}
		return nil
	}
	if currentExists && currentErr != nil {
		if !prevExists || prevErr != nil {
			return fmt.Errorf("%w: no valid current or predecessor", ErrCorruptWAL)
		}
		if err := hooks.Remove(path); err != nil {
			return err
		}
		if err := hooks.Rename(path+".prev", path); err != nil {
			return err
		}
		if err := hooks.SyncDir(dir); err != nil {
			return err
		}
		if nextExists {
			if err := hooks.Remove(path + ".next"); err != nil {
				return err
			}
			return hooks.SyncDir(dir)
		}
		return nil
	}
	if !currentExists && prevExists {
		if prevErr != nil {
			return fmt.Errorf("%w: invalid predecessor", ErrCorruptWAL)
		}
		source := path + ".prev"
		if nextExists && nextErr == nil {
			source = path + ".next"
		}
		if err := hooks.Rename(source, path); err != nil {
			return err
		}
		if err := hooks.SyncDir(dir); err != nil {
			return err
		}
		for _, stale := range []string{path + ".prev", path + ".next"} {
			if stale != source {
				if err := removeIfExists(hooks, stale); err != nil {
					return err
				}
			}
		}
		return hooks.SyncDir(dir)
	}
	if nextExists || (currentExists && currentErr != nil) || (prevExists && prevErr != nil) {
		return fmt.Errorf("%w: no valid current or predecessor", ErrCorruptWAL)
	}
	return nil
}

func candidate(path string) (ReplayResult, bool, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return ReplayResult{}, false, nil
	}
	if err != nil {
		return ReplayResult{}, true, err
	}
	defer func() { _ = f.Close() }()
	result, err := replayFile(f)
	return result, true, err
}
func validatePath(path string) error {
	result, exists, err := candidate(path)
	if err != nil {
		return err
	}
	if !exists || result.TornTail {
		return ErrCorruptWAL
	}
	return nil
}
func removeIfExists(hooks fsHooks, path string) error {
	err := hooks.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
func injectFault(hooks fsHooks, point string) error {
	if hooks.Fault == nil {
		return nil
	}
	return hooks.Fault(point)
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

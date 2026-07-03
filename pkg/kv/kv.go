package kv

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
)

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
}

// Open opens or creates a store at path, replays valid records, truncates any
// corrupt or torn tail, and compacts if the file contains enough obsolete data.
func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}

	lockFile, err := acquireLock(path)
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		_ = releaseLock(lockFile)
		return nil, err
	}

	data, sizes, total, live, lastGood, err := replayFile(f)
	if err != nil {
		_ = f.Close()
		_ = releaseLock(lockFile)
		return nil, err
	}
	if err := f.Truncate(lastGood); err != nil {
		_ = f.Close()
		_ = releaseLock(lockFile)
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		_ = releaseLock(lockFile)
		return nil, err
	}

	s := &Store{path: path, file: f, lockFile: lockFile, data: data, entrySize: sizes, total: total, live: live}
	if s.shouldCompact() {
		if err := s.compactLocked(); err != nil {
			_ = f.Close()
			_ = releaseLock(lockFile)
			return nil, err
		}
	}
	return s, nil
}

// Replay reads path without mutating it and returns the state from valid records.
func Replay(path string) (map[string][]byte, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string][]byte{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	data, _, _, _, _, err := replayFile(f)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Get returns a copy of the value for key.
func (s *Store) Get(key []byte) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, ok := s.data[string(key)]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), v...), true
}

// Set appends a SET record. It does not fsync.
func (s *Store) Set(key, val []byte) error {
	return s.appendRecord(opSet, key, val)
}

// Delete appends a DEL tombstone. It does not fsync.
func (s *Store) Delete(key []byte) error {
	return s.appendRecord(opDel, key, nil)
}

// Range calls fn for each key/value pair, using copies. Iteration stops when fn returns false.
func (s *Store) Range(fn func(k, v []byte) bool) {
	s.mu.Lock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	pairs := make([]struct{ k, v []byte }, 0, len(keys))
	for _, k := range keys {
		pairs = append(pairs, struct{ k, v []byte }{[]byte(k), append([]byte(nil), s.data[k]...)})
	}
	s.mu.Unlock()

	for _, p := range pairs {
		if !fn(p.k, p.v) {
			return
		}
	}
}

// Sync fsyncs the WAL file.
func (s *Store) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	return s.file.Sync()
}

// Close fsyncs and closes the store.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	errSync := s.file.Sync()
	errClose := s.file.Close()
	errUnlock := releaseLock(s.lockFile)
	s.closed = true
	if errSync != nil {
		return errSync
	}
	if errClose != nil {
		return errClose
	}
	return errUnlock
}

func acquireLock(path string) (*os.File, error) {
	lf, err := os.OpenFile(path+".lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lf.Close()
		return nil, err
	}
	return lf, nil
}

func releaseLock(f *os.File) error {
	if f == nil {
		return nil
	}
	errUnlock := syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	errClose := f.Close()
	if errUnlock != nil {
		return errUnlock
	}
	return errClose
}

func (s *Store) appendRecord(op byte, key, val []byte) error {
	buf, err := encodeRecord(op, key, val)
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
	if _, err := s.file.Write(buf); err != nil {
		_ = s.file.Truncate(pos)
		_, _ = s.file.Seek(pos, io.SeekStart)
		return err
	}

	k := string(key)
	if old, ok := s.entrySize[k]; ok {
		s.live -= old
	}
	s.total += int64(len(buf))
	if op == opSet {
		s.data[k] = append([]byte(nil), val...)
		s.entrySize[k] = int64(len(buf))
		s.live += int64(len(buf))
	} else {
		delete(s.data, k)
		delete(s.entrySize, k)
	}
	if s.shouldCompact() {
		return s.compactLocked()
	}
	return nil
}

func (s *Store) shouldCompact() bool {
	return s.total > compactThreshold && float64(s.total-s.live)/float64(s.total) > compactWasteRatio
}

func (s *Store) compactLocked() error {
	tmp := s.path + ".compact"
	tf, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}

	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	newSizes := make(map[string]int64, len(keys))
	var newTotal, newLive int64
	for _, k := range keys {
		buf, err := encodeRecord(opSet, []byte(k), s.data[k])
		if err != nil {
			_ = tf.Close()
			_ = os.Remove(tmp)
			return err
		}
		if _, err := tf.Write(buf); err != nil {
			_ = tf.Close()
			_ = os.Remove(tmp)
			return err
		}
		sz := int64(len(buf))
		newSizes[k] = sz
		newTotal += sz
		newLive += sz
	}
	if err := tf.Sync(); err != nil {
		_ = tf.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := tf.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := s.file.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	f, err := os.OpenFile(s.path, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return err
	}
	s.file = f
	s.entrySize = newSizes
	s.total = newTotal
	s.live = newLive
	return nil
}

func replayFile(f *os.File) (map[string][]byte, map[string]int64, int64, int64, int64, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, nil, 0, 0, 0, err
	}
	fileSize := info.Size()
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, nil, 0, 0, 0, err
	}
	data := make(map[string][]byte)
	sizes := make(map[string]int64)
	var total, live, off int64
	header := make([]byte, headerLen)
	for {
		n, err := io.ReadFull(f, header)
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			_ = n
			break
		}
		if err != nil {
			return nil, nil, 0, 0, 0, err
		}

		payloadLen := binary.BigEndian.Uint32(header[0:4])
		if int64(payloadLen) > fileSize-off-headerLen {
			break
		}
		wantCRC := binary.BigEndian.Uint32(header[4:8])
		payload := make([]byte, int(payloadLen))
		if _, err := io.ReadFull(f, payload); errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, nil, 0, 0, 0, err
		}
		if crc32.ChecksumIEEE(payload) != wantCRC {
			break
		}
		rec, err := decodePayload(payload)
		if err != nil {
			break
		}

		sz := int64(headerLen + len(payload))
		k := string(rec.key)
		if old, ok := sizes[k]; ok {
			live -= old
		}
		total += sz
		if rec.op == opSet {
			data[k] = rec.value
			sizes[k] = sz
			live += sz
		} else {
			delete(data, k)
			delete(sizes, k)
		}
		off += sz
	}
	return data, sizes, total, live, off, nil
}

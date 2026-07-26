package kv

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"

	"github.com/bnema/vev/pkg/safedir"
)

// Store is an in-memory key/value map persisted by whole-file atomic rewrite.
// Set and Delete mutate memory only; Sync is the durability barrier.
type Store struct {
	mu       sync.Mutex
	path     string
	lockFile *os.File
	data     map[string][]byte
	dirty    bool
	closed   bool
}

// Open reads path, or starts empty when it does not exist. Stray .tmp files
// are ignored. Any invalid existing main file fails closed and is untouched.
func Open(path string) (*Store, error) {
	if err := safedir.EnsurePrivate(filepath.Dir(path)); err != nil {
		return nil, err
	}
	lockFile, err := acquireLock(path)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*Store, error) {
		return nil, errors.Join(err, releaseLock(lockFile))
	}

	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		store := &Store{path: path, lockFile: lockFile, data: make(map[string][]byte), dirty: true}
		if err := store.syncLocked(); err != nil {
			return fail(err)
		}
		return store, nil
	}
	if err != nil {
		return fail(err)
	}
	data, err := decodeFile(raw)
	if err != nil {
		return fail(err)
	}
	return &Store{path: path, lockFile: lockFile, data: data}, nil
}

func (s *Store) Get(key []byte) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.data[string(key)]
	return append([]byte(nil), value...), ok
}

func (s *Store) Set(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	s.data[string(key)] = append([]byte(nil), value...)
	s.dirty = true
	return nil
}

func (s *Store) Delete(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	if _, exists := s.data[string(key)]; exists {
		delete(s.data, string(key))
		s.dirty = true
	}
	return nil
}

func (s *Store) Range(fn func(k, v []byte) bool) {
	s.mu.Lock()
	keys := make([]string, 0, len(s.data))
	for key := range s.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	values := make([][]byte, len(keys))
	for i, key := range keys {
		values[i] = append([]byte(nil), s.data[key]...)
	}
	s.mu.Unlock()
	for i, key := range keys {
		if !fn([]byte(key), values[i]) {
			return
		}
	}
}

// Sync writes path.tmp, fsyncs it, renames it over path, then fsyncs the
// directory. The main pathname therefore contains either the previous whole
// map or the next whole map, never a subset of mutations.
func (s *Store) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	return s.syncLocked()
}

func (s *Store) syncLocked() error {
	if !s.dirty {
		return nil
	}
	data := encodeFile(s.data)
	tmp := s.path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if err := writeAll(file, data); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Sync(); err != nil {
		return errors.Join(err, file.Close())
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(s.path)); err != nil {
		return err
	}
	s.dirty = false
	return nil
}

// Close syncs buffered mutations before releasing the process lock.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	err := s.syncLocked()
	s.closed = true
	return errors.Join(err, releaseLock(s.lockFile))
}

func writeAll(file *os.File, data []byte) error {
	for len(data) > 0 {
		n, err := file.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func acquireLock(path string) (*os.File, error) {
	file, err := os.OpenFile(path+".lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func releaseLock(file *os.File) error {
	if file == nil {
		return nil
	}
	return errors.Join(syscall.Flock(int(file.Fd()), syscall.LOCK_UN), file.Close())
}

func syncDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}

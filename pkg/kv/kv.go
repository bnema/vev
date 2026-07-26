package kv

import (
	"bytes"
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

// ReplayResult describes a strictly validated WAL prefix. LastGood is an
// absolute file offset, so truncating a torn tail to it preserves the file
// header of a current-format file.
type ReplayResult struct {
	Data      map[string][]byte
	EntrySize map[string]int64
	Total     int64
	Live      int64
	LastGood  int64
	TornTail  bool
	Format    walFormat
}

// walFormat is the on-disk framing a file was written with.
type walFormat uint8

const (
	// formatLegacy is a file written before the record header carried its own
	// checksum. It is read with the original semantics and upgraded at Open.
	formatLegacy walFormat = iota
	// formatCurrentV1 is the released VEVK v1 framing with its six-byte file
	// header. Its record headers are identical to the current format.
	formatCurrentV1
	formatCurrent
)

func (f walFormat) headerLen() int64 {
	if f != formatLegacy {
		return recordHeaderLen
	}
	return legacyHeaderLen
}

func (f walFormat) fileHeaderLen() int64 {
	switch f {
	case formatCurrentV1:
		return fileHeaderV1Len
	case formatCurrent:
		return fileHeaderLen
	default:
		return 0
	}
}

type headerStatus uint8

const (
	headerOK headerStatus = iota
	headerEOF
	headerTorn
)

type fsHooks struct {
	Open    func(string, int, os.FileMode) (*os.File, error)
	Write   func(*os.File, []byte) (int, error)
	Sync    func(*os.File) error
	Close   func(*os.File) error
	Remove  func(string) error
	Rename  func(string, string) error
	SyncDir func(string) error
	Fault   func(string) error
}

func defaultFSHooks() fsHooks {
	return fsHooks{
		Open:    os.OpenFile,
		Write:   func(f *os.File, p []byte) (int, error) { return f.Write(p) },
		Sync:    func(f *os.File) error { return f.Sync() },
		Close:   func(f *os.File) error { return f.Close() },
		Remove:  os.Remove,
		Rename:  os.Rename,
		SyncDir: syncDir,
	}
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
	// A legacy file has already replayed with its original semantics, so the
	// data is exactly what the previous release would have loaded. Rewriting it
	// through the compaction protocol upgrades the framing without a second
	// rewrite path: every crash state it can leave behind is one recoverCompaction
	// already resolves, so a restart sees either the whole legacy file or the
	// whole upgraded one. A failed compaction poisons the store, so there is no
	// safe way to continue in legacy mode; Open fails closed and the next start
	// retries from whichever complete file recovery selects.
	if result.Format != formatCurrent || s.shouldCompact() {
		if err := s.compactLocked(); err != nil {
			return nil, err
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
	// Every replacement is written in the current format, which is also how a
	// legacy file is upgraded.
	header := fileHeader()
	if n, err := s.hooks.Write(f, header); err != nil || n != len(header) {
		_ = s.hooks.Close(f)
		if err == nil {
			err = io.ErrShortWrite
		}
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
			_ = s.hooks.Close(f)
			return err
		}
		if n, err := s.hooks.Write(f, buf); err != nil || n != len(buf) {
			_ = s.hooks.Close(f)
			if err == nil {
				err = io.ErrShortWrite
			}
			return err
		}
	}
	if err := injectFault(s.hooks, "after-write-next"); err != nil {
		_ = s.hooks.Close(f)
		return err
	}
	if err := injectFault(s.hooks, "before-sync-next"); err != nil {
		_ = s.hooks.Close(f)
		return err
	}
	if err := s.hooks.Sync(f); err != nil {
		_ = s.hooks.Close(f)
		return err
	}
	if err := injectFault(s.hooks, "after-sync-next"); err != nil {
		_ = s.hooks.Close(f)
		return err
	}
	if err := s.hooks.Close(f); err != nil {
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
		_ = s.hooks.Close(current)
		return err
	}
	result, err := replayFile(current)
	if err != nil || result.TornTail || result.Format != formatCurrent {
		_ = s.hooks.Close(current)
		if err == nil {
			err = ErrCorruptWAL
		}
		return err
	}
	if _, err := current.Seek(0, io.SeekEnd); err != nil {
		_ = s.hooks.Close(current)
		return err
	}
	old := s.file
	s.file = current
	s.data, s.entrySize, s.total, s.live = result.Data, result.EntrySize, result.Total, result.Live
	if err := s.hooks.Close(old); err != nil {
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
		fileErr = s.hooks.Close(file)
	}
	return errors.Join(fileErr, releaseLock(lockFile))
}

// replayFile validates a WAL prefix in whichever framing the file uses. A torn
// tail is the only truncatable outcome; every other inconsistency is corruption
// and fails closed, so a damaged record can never be silently dropped along
// with everything written after it.
func replayFile(f *os.File) (ReplayResult, error) {
	info, err := f.Stat()
	if err != nil {
		return ReplayResult{}, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return ReplayResult{}, err
	}
	fileSize := info.Size()
	format, err := readFileFormat(f, fileSize)
	if err != nil {
		return ReplayResult{}, err
	}
	result := ReplayResult{Data: make(map[string][]byte), EntrySize: make(map[string]int64), Format: format, LastGood: format.fileHeaderLen()}
	for {
		header, status, err := readRecordHeader(f, format, fileSize, result.LastGood)
		if err != nil {
			return ReplayResult{}, err
		}
		if status == headerEOF {
			return result, nil
		}
		if status == headerTorn {
			result.TornTail = true
			return result, nil
		}
		payload := make([]byte, int(header.payloadLen))
		if _, err := io.ReadFull(f, payload); errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			result.TornTail = true
			return result, nil
		} else if err != nil {
			return ReplayResult{}, err
		}
		if crc32.ChecksumIEEE(payload) != header.payloadCRC {
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
			return ReplayResult{}, fmt.Errorf("%w: invalid record at offset %d: %w", ErrCorruptWAL, result.LastGood, err)
		}
		size := format.headerLen() + int64(len(payload))
		applyRecords(result.Data, result.EntrySize, &result.Live, records, size)
		result.Total += size
		result.LastGood += size
	}
}

// readFileFormat consumes the file header when one is present and leaves the
// reader positioned at the first record either way.
func readFileFormat(f io.ReadSeeker, fileSize int64) (walFormat, error) {
	if fileSize == 0 {
		return formatLegacy, nil
	}
	probeLen := min(fileSize, int64(fileHeaderLen))
	probe := make([]byte, probeLen)
	if _, err := io.ReadFull(f, probe); err != nil {
		return formatLegacy, err
	}
	if fileSize < fileMagicLen {
		if bytes.Equal(probe, fileMagic[:len(probe)]) {
			return formatLegacy, fmt.Errorf("%w: incomplete VEVK header", ErrCorruptWAL)
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return formatLegacy, err
		}
		return formatLegacy, nil
	}
	if !bytes.Equal(probe[:fileMagicLen], fileMagic[:]) {
		// A complete v2 identity checksum authenticates the intended magic as
		// well as the version. Detect it before legacy replay so damage to the
		// first magic byte cannot masquerade as a small legacy length and be
		// truncated or compacted away.
		if len(probe) == fileHeaderLen &&
			binary.BigEndian.Uint16(probe[fileMagicLen:fileHeaderBodyLen]) == formatVersion &&
			binary.BigEndian.Uint32(probe[fileHeaderBodyLen:]) == binary.BigEndian.Uint32(fileHeader()[fileHeaderBodyLen:]) {
			return formatLegacy, fmt.Errorf("%w: damaged VEVK v2 magic", ErrCorruptWAL)
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return formatLegacy, err
		}
		return formatLegacy, nil
	}
	if fileSize < fileHeaderV1Len {
		return formatLegacy, fmt.Errorf("%w: incomplete VEVK header", ErrCorruptWAL)
	}
	prefix := probe[:fileHeaderV1Len]
	if _, err := f.Seek(fileHeaderV1Len, io.SeekStart); err != nil {
		return formatLegacy, err
	}
	switch version := binary.BigEndian.Uint16(prefix[fileMagicLen:]); version {
	case formatVersionV1:
		return formatCurrentV1, nil
	case formatVersion:
		checksum := make([]byte, fileHeaderLen-fileHeaderV1Len)
		if _, err := io.ReadFull(f, checksum); err != nil {
			return formatLegacy, fmt.Errorf("%w: incomplete VEVK v2 header", ErrCorruptWAL)
		}
		if crc32.ChecksumIEEE(prefix) != binary.BigEndian.Uint32(checksum) {
			return formatLegacy, fmt.Errorf("%w: file header checksum mismatch", ErrCorruptWAL)
		}
		return formatCurrent, nil
	default:
		return formatLegacy, fmt.Errorf("%w: unsupported format version %d", ErrCorruptWAL, version)
	}
}

type recordHeader struct {
	payloadLen uint32
	payloadCRC uint32
}

// readRecordHeader implements the replay decision table. In the current format
// the header checksum is verified before the length is trusted for anything:
// an unverifiable header is corruption, and only a header that checks out may
// declare the record short (a torn tail).
func readRecordHeader(f io.Reader, format walFormat, fileSize, offset int64) (recordHeader, headerStatus, error) {
	buf := make([]byte, format.headerLen())
	if _, err := io.ReadFull(f, buf); errors.Is(err, io.EOF) {
		return recordHeader{}, headerEOF, nil
	} else if errors.Is(err, io.ErrUnexpectedEOF) {
		return recordHeader{}, headerTorn, nil
	} else if err != nil {
		return recordHeader{}, headerOK, err
	}
	header := recordHeader{
		payloadLen: binary.BigEndian.Uint32(buf[0:4]),
		payloadCRC: binary.BigEndian.Uint32(buf[4:8]),
	}
	if format != formatLegacy {
		if crc32.ChecksumIEEE(buf[0:8]) != binary.BigEndian.Uint32(buf[8:recordHeaderLen]) {
			return recordHeader{}, headerOK, fmt.Errorf("%w: record header CRC mismatch at offset %d", ErrCorruptWAL, offset)
		}
		// The length is now trustworthy, so an implausible one is corruption
		// rather than a short tail. Checked before any allocation.
	}
	// Every framing is allocation-bounded. In legacy framing an oversized
	// declaration remains corruption rather than driving an attacker-sized
	// allocation; ordinary short tails retain their released truncation semantics.
	if header.payloadLen > maxPayloadLen {
		return recordHeader{}, headerOK, fmt.Errorf("%w: record length %d exceeds maximum at offset %d", ErrCorruptWAL, header.payloadLen, offset)
	}
	if int64(header.payloadLen) > fileSize-offset-format.headerLen() {
		return recordHeader{}, headerTorn, nil
	}
	return header, headerOK, nil
}

func applyRecords(data map[string][]byte, sizes map[string]int64, live *int64, records []record, size int64) {
	setCount := int64(0)
	for _, rec := range records {
		if rec.op == opSet {
			setCount++
		}
	}
	setSize, remainder := size, int64(0)
	if setCount > 1 {
		setSize, remainder = size/setCount, size%setCount
	}
	for _, rec := range records {
		key := string(rec.key)
		if old, ok := sizes[key]; ok {
			*live -= old
		}
		if rec.op == opSet {
			entrySize := setSize
			if remainder > 0 {
				entrySize++
				remainder--
			}
			data[key] = append([]byte(nil), rec.value...)
			sizes[key] = entrySize
			*live += entrySize
		} else {
			delete(data, key)
			delete(sizes, key)
		}
	}
}

func recoverCompaction(path string, hooks fsHooks) error {
	currentResult, currentExists, currentErr := candidate(path)
	nextResult, nextExists, nextErr := candidate(path + ".next")
	_, prevExists, prevErr := candidate(path + ".prev")
	if nextErr == nil && nextResult.TornTail {
		nextErr = ErrCorruptWAL
	}
	dir := filepath.Dir(path)
	if currentExists && currentErr == nil {
		if currentResult.TornTail && prevExists {
			// A predecessor proves compaction was in flight. The torn current is
			// ambiguous, so preserve both candidates and fail closed rather than
			// truncating current and deleting the last complete predecessor.
			return fmt.Errorf("%w: torn current with predecessor", ErrCorruptWAL)
		}
		if nextExists {
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
		// Corruption does not prove which bytes are disposable. Preserve every
		// candidate for diagnosis/recovery instead of deleting current or a valid
		// predecessor and fail closed.
		return fmt.Errorf("%w: invalid current candidate", ErrCorruptWAL)
	}
	if !currentExists && (nextExists || prevExists) {
		// A complete .next is the newest fully published compaction candidate and
		// wins even when .prev exists but is corrupt. Otherwise a complete .prev
		// is the only safe rollback. Invalid candidates are never selected.
		source := ""
		if nextExists && nextErr == nil {
			source = path + ".next"
		} else if prevExists && prevErr == nil {
			source = path + ".prev"
		}
		if source == "" {
			return fmt.Errorf("%w: no valid current, successor, or predecessor", ErrCorruptWAL)
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
	// A replacement this package just wrote must be complete and in the current
	// format; anything else means the write did not land as intended.
	if !exists || result.TornTail || result.Format != formatCurrent {
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

// Package noticefile persists undeliverable daemon notices as newline-
// delimited JSON so they survive a daemon restart and can be shown at the
// next client attach.
package noticefile

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/safedir"
)

const (
	fileName             = "pending-notices.jsonl"
	inFlightFileName     = "pending-notices.inflight.jsonl"
	lockFileName         = "pending-notices.lock"
	maxNoticeRecordSize  = 4 << 20
	noticeReadBufferSize = 64 << 10
)

// Store persists notices as JSON-lines under dir.
type Store struct {
	dir string

	// afterClaimRead is used by package tests to pause a claim after its reader
	// reached EOF while it still owns the interprocess lock. It is nil in
	// production.
	afterClaimRead func()
}

var _ ports.NoticeStore = (*Store)(nil)

// New creates a file-backed notice store rooted at dir.
func New(dir string) *Store {
	return &Store{dir: dir}
}

func (s *Store) path() string {
	return filepath.Join(s.dir, fileName)
}

func (s *Store) inFlightPath() string {
	return filepath.Join(s.dir, inFlightFileName)
}

func (s *Store) lockPath() string {
	return filepath.Join(s.dir, lockFileName)
}

// withLock serializes Append, Claim, and Ack across Store instances and
// processes. Holding this lock while Claim rotates and reads the file prevents
// an appender with an already-open file descriptor from writing into a claim
// after its reader reached EOF.
func (s *Store) withLock(fn func() error) error {
	if err := safedir.EnsurePrivate(s.dir); err != nil {
		return fmt.Errorf("create notice dir: %w", err)
	}
	f, err := os.OpenFile(s.lockPath(), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open notice lock: %w", err)
	}
	defer func() { _ = f.Close() }()
	if err := f.Chmod(0o600); err != nil {
		return fmt.Errorf("secure notice lock: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock pending notices: %w", err)
	}
	defer func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }()
	return fn()
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return f.Sync()
}

// Append writes n as one JSON line, creating the private state dir if needed.
// A successful append is synced before it is made available to a later claim.
func (s *Store) Append(n domain.Notification) error {
	data, err := json.Marshal(n)
	if err != nil {
		return fmt.Errorf("marshal notice: %w", err)
	}
	data = append(data, '\n')

	return s.withLock(func() error {
		f, err := os.OpenFile(s.path(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			return fmt.Errorf("open pending notices: %w", err)
		}
		if err := f.Chmod(0o600); err != nil {
			_ = f.Close()
			return fmt.Errorf("secure pending notices: %w", err)
		}
		if _, err := f.Write(data); err != nil {
			_ = f.Close()
			return fmt.Errorf("write notice: %w", err)
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return fmt.Errorf("sync pending notices: %w", err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("close pending notices: %w", err)
		}
		if err := syncDir(s.dir); err != nil {
			return fmt.Errorf("sync notice dir: %w", err)
		}
		return nil
	})
}

// Claim atomically moves pending notices into an in-flight file and returns
// their valid JSONL entries in order. The in-flight file is intentionally kept
// until Ack, so an unacknowledged import is replayed after a crash. A missing
// pending file means there is nothing to claim.
func (s *Store) Claim() ([]domain.Notification, error) {
	var notices []domain.Notification
	err := s.withLock(func() error {
		inFlight := s.inFlightPath()
		if _, err := os.Stat(inFlight); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("stat in-flight notices: %w", err)
			}
			if err := os.Rename(s.path(), inFlight); err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return nil
				}
				return fmt.Errorf("claim pending notices: %w", err)
			}
			// fsync the parent directory so the atomic claim is recoverable after
			// power loss, not merely after a clean process restart.
			if err := syncDir(s.dir); err != nil {
				return fmt.Errorf("sync claimed notices: %w", err)
			}
		}

		var err error
		notices, err = readNotices(inFlight)
		if err == nil && s.afterClaimRead != nil {
			s.afterClaimRead()
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return notices, nil
}

func readNotices(path string) ([]domain.Notification, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open claimed notices: %w", err)
	}
	defer func() { _ = f.Close() }()

	var notices []domain.Notification
	reader := bufio.NewReaderSize(f, noticeReadBufferSize)
	for {
		line, oversized, eof, err := readNoticeRecord(reader)
		if err != nil {
			return nil, fmt.Errorf("read claimed notices: %w", err)
		}
		if len(line) > 0 && !oversized {
			var n domain.Notification
			if err := json.Unmarshal(line, &n); err == nil {
				notices = append(notices, n)
			}
		}
		if eof {
			return notices, nil
		}
	}
}

// readNoticeRecord reads one newline-delimited record without retaining more
// than maxNoticeRecordSize bytes. Oversized records are discarded through
// their delimiter so a later valid record can still be read.
func readNoticeRecord(reader *bufio.Reader) (line []byte, oversized, eof bool, err error) {
	for {
		fragment, readErr := reader.ReadSlice('\n')
		if len(fragment) > 0 && fragment[len(fragment)-1] == '\n' {
			fragment = fragment[:len(fragment)-1]
		}
		if !oversized {
			line, oversized = appendNoticeFragment(line, fragment)
		}

		switch {
		case readErr == nil:
			return line, oversized, false, nil
		case errors.Is(readErr, bufio.ErrBufferFull):
			continue
		case errors.Is(readErr, io.EOF):
			return line, oversized, true, nil
		default:
			return nil, false, false, readErr
		}
	}
}

func appendNoticeFragment(line, fragment []byte) ([]byte, bool) {
	if len(fragment) > maxNoticeRecordSize-len(line) {
		return nil, true
	}
	required := len(line) + len(fragment)
	if required > cap(line) {
		capacity := cap(line) * 2
		if capacity < noticeReadBufferSize {
			capacity = noticeReadBufferSize
		}
		if capacity > maxNoticeRecordSize {
			capacity = maxNoticeRecordSize
		}
		grown := make([]byte, len(line), capacity)
		copy(grown, line)
		line = grown
	}
	return append(line, fragment...), false
}

// Ack permanently removes the current in-flight claim. It is only called once
// the daemon has recorded and queued every claimed notice.
func (s *Store) Ack() error {
	return s.withLock(func() error {
		if err := os.Remove(s.inFlightPath()); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("ack claimed notices: %w", err)
		}
		if err := syncDir(s.dir); err != nil {
			return fmt.Errorf("sync acknowledged notices: %w", err)
		}
		return nil
	})
}

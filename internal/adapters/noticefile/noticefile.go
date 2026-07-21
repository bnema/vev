// Package noticefile persists undeliverable daemon notices as newline-
// delimited JSON so they survive a daemon restart and can be shown at the
// next client attach.
package noticefile

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/safedir"
)

const fileName = "pending-notices.jsonl"

// Store persists notices as JSON-lines under dir.
type Store struct {
	dir string
}

var _ ports.NoticeStore = (*Store)(nil)

// New creates a file-backed notice store rooted at dir.
func New(dir string) *Store {
	return &Store{dir: dir}
}

func (s *Store) path() string {
	return filepath.Join(s.dir, fileName)
}

// Append writes n as one JSON line, creating the private state dir if needed.
func (s *Store) Append(n domain.Notification) error {
	if err := safedir.EnsurePrivate(s.dir); err != nil {
		return fmt.Errorf("create notice dir: %w", err)
	}
	f, err := os.OpenFile(s.path(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open pending notices: %w", err)
	}
	defer func() { _ = f.Close() }()

	data, err := json.Marshal(n)
	if err != nil {
		return fmt.Errorf("marshal notice: %w", err)
	}
	data = append(data, '\n')
	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("write notice: %w", err)
	}
	return nil
}

// Drain returns all stored notices and truncates the store. A missing file
// means a fresh daemon has nothing pending, not an error.
func (s *Store) Drain() ([]domain.Notification, error) {
	f, err := os.Open(s.path())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open pending notices: %w", err)
	}

	var notices []domain.Notification
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 4<<20)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var n domain.Notification
		if err := json.Unmarshal(line, &n); err != nil {
			continue // corrupt or partial line must not abort the drain
		}
		notices = append(notices, n)
	}
	scanErr := scanner.Err()
	_ = f.Close()
	if scanErr != nil {
		return nil, fmt.Errorf("read pending notices: %w", scanErr)
	}

	if err := os.Truncate(s.path(), 0); err != nil {
		return nil, fmt.Errorf("truncate pending notices: %w", err)
	}
	return notices, nil
}

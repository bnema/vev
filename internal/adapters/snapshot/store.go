package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/safedir"
)

var safeNameRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,200}$`)

const maxSnapshotFileSize = 16 + (256 << 20)

// Store persists raw session snapshot blobs on disk.
type Store struct {
	dir string
	log *slog.Logger
}

var _ ports.SnapshotStore = (*Store)(nil)

// NewStore creates a file-backed snapshot store rooted at dir.
func NewStore(dir string) *Store {
	return &Store{dir: dir, log: slog.Default()}
}

// Write atomically writes data for name. The previous snapshot remains in place
// until the final rename succeeds.
func (s *Store) Write(name string, data []byte) error {
	if err := safedir.EnsurePrivate(s.dir); err != nil {
		return fmt.Errorf("create snapshot dir: %w", err)
	}

	tmp, err := os.CreateTemp(s.dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp snapshot: %w", err)
	}
	tmpName := tmp.Name()
	keepTmp := false
	defer func() {
		if !keepTmp {
			_ = os.Remove(tmpName)
		}
	}()

	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod temp snapshot: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp snapshot: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync temp snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp snapshot: %w", err)
	}

	path := filepath.Join(s.dir, filenameForName(name))
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("rename snapshot: %w", err)
	}
	keepTmp = true
	if err := syncDir(s.dir); err != nil {
		return fmt.Errorf("sync snapshot dir: %w", err)
	}
	return nil
}

// Load returns all raw .snap blobs. It does not validate the blob format.
func (s *Store) Load() ([]ports.SnapshotBlob, error) {
	entries, err := os.ReadDir(s.dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read snapshot dir: %w", err)
	}

	blobs := make([]ports.SnapshotBlob, 0, len(entries))
	for _, entry := range entries {
		base := entry.Name()
		path := filepath.Join(s.dir, base)
		if strings.HasPrefix(base, ".tmp-") {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				s.log.Warn("remove stale snapshot temp", "path", path, "err", err)
			}
			continue
		}
		if entry.IsDir() || !strings.HasSuffix(base, ".snap") {
			continue
		}
		data, err := readSnapshotFileBounded(path)
		if err != nil {
			s.log.Warn("read snapshot", "path", path, "err", err)
			continue
		}
		name := strings.TrimSuffix(base, ".snap")
		if strings.HasPrefix(name, "@") || !safeNameRE.MatchString(name) {
			name = ""
		}
		blobs = append(blobs, ports.SnapshotBlob{Name: name, Data: data})
	}
	return blobs, nil
}

// Delete removes the deterministic snapshot file for name. Missing files are ignored.
func (s *Store) Delete(name string) error {
	err := os.Remove(filepath.Join(s.dir, filenameForName(name)))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("delete snapshot: %w", err)
	}
	if err := syncDir(s.dir); err != nil {
		return fmt.Errorf("sync snapshot dir: %w", err)
	}
	return nil
}

func readSnapshotFileBounded(path string) ([]byte, error) {
	return readBounded(path)
}

func filenameForName(name string) string {
	if safeNameRE.MatchString(name) {
		return name + ".snap"
	}
	sum := sha256.Sum256([]byte(name))
	return "@" + hex.EncodeToString(sum[:])[:40] + ".snap"
}

func syncDir(dir string) error {
	return syncDirectory(dir)
}

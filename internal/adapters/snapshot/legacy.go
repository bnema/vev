package snapshot

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bnema/vev/internal/ports"
)

// LoadLegacy reads only pre-incremental root .snap files. It is deliberately
// isolated from repository reads so incremental directories are never traversed.
func (r *Repository) LoadLegacy(ctx context.Context) ([]ports.LegacySnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := os.Open(r.dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read legacy snapshot directory: %w", safeFilesystemError(err))
	}
	defer f.Close()

	out := make([]ports.LegacySnapshot, 0, maxLegacySnapshotFiles)
	files := 0
	total := 0
	for {
		entries, readErr := f.ReadDir(maintenanceBatch)
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".snap") {
				continue
			}
			files++
			if files > maxLegacySnapshotFiles {
				return nil, fmt.Errorf("legacy snapshot import has too many files (maximum %d)", maxLegacySnapshotFiles)
			}
			path := filepath.Join(r.dir, entry.Name())
			// Check the prospective size before readBounded allocates a buffer.
			// readBounded repeats the security validation and the actual length is
			// checked below to close the metadata/read race.
			if info, statErr := os.Lstat(path); statErr == nil && info.Mode().IsRegular() && info.Size() > int64(maxLegacySnapshotBytes-total) {
				return nil, fmt.Errorf("legacy snapshot import exceeds aggregate byte limit (%d bytes)", maxLegacySnapshotBytes)
			}
			data, dataErr := readBounded(path)
			if dataErr != nil {
				continue
			}
			if len(data) > maxLegacySnapshotBytes-total {
				return nil, fmt.Errorf("legacy snapshot import exceeds aggregate byte limit (%d bytes)", maxLegacySnapshotBytes)
			}
			total += len(data)
			name := strings.TrimSuffix(entry.Name(), ".snap")
			if strings.HasPrefix(name, "@") || !safeNameRE.MatchString(name) {
				name = ""
			}
			killed, tombstoneErr := r.tombstoned(name)
			if tombstoneErr != nil {
				return nil, fmt.Errorf("read killed session marker: %w", safeFilesystemError(tombstoneErr))
			}
			if killed {
				continue
			}
			out = append(out, ports.LegacySnapshot{Name: name, Data: data})
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("read legacy snapshot directory: %w", safeFilesystemError(readErr))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// DeleteLegacy deletes the deterministic v3 file and synchronizes the root.
// Once an unlink succeeds, its directory sync remains pending until it succeeds;
// this lets an absent-file retry complete the durability boundary without
// deleting a file recreated in the meantime.
func (r *Repository) DeleteLegacy(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	filename := filenameForName(name)
	lock := r.sessionLock("legacy-sync:" + filename)
	lock.Lock()
	defer lock.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, pending := r.pendingLegacySync.Load(filename); pending {
		return r.syncPendingLegacy(ctx, filename)
	}

	path := filepath.Join(r.dir, filename)
	if err := r.remove(path); errors.Is(err, os.ErrNotExist) {
		// Tombstones make a process-restart retry distinguish an already-unlinked
		// legacy source from a source that was absent before the kill. Re-sync the
		// root to complete the original unlink's durability boundary.
		killed, tombstoneErr := r.tombstoned(name)
		if tombstoneErr != nil {
			return fmt.Errorf("read killed session marker: %w", safeFilesystemError(tombstoneErr))
		}
		if !killed {
			return nil
		}
		return r.syncPendingLegacy(ctx, filename)
	} else if err != nil {
		return fmt.Errorf("delete legacy snapshot: %w", safeFilesystemError(err))
	}
	r.pendingLegacySync.Store(filename, struct{}{})
	return r.syncPendingLegacy(ctx, filename)
}

// Tombstone durably marks name as logically killed before either snapshot
// source is deleted. Startup consults this marker before importing legacy or
// restoring incremental state, so a partial offline kill cannot resurrect it.
func (r *Repository) Tombstone(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := sessionKey(name)
	lock := r.sessionLock(key)
	lock.Lock()
	defer lock.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.ensurePrivateDirectory(r.dir); err != nil {
		return fmt.Errorf("create killed session marker directory: %w", safeFilesystemError(err))
	}
	path := filepath.Join(r.dir, tombstoneFilename(key))
	if err := r.writeImmutable(path, []byte(name), func(data []byte) error {
		if string(data) != name {
			return fmt.Errorf("killed session marker identity mismatch")
		}
		return nil
	}); err != nil {
		return fmt.Errorf("write killed session marker: %w", safeFilesystemError(err))
	}
	return nil
}

// DeleteTombstone removes a marker only after both sources have been durably
// deleted. A missing marker is idempotent for offline-kill retries.
func (r *Repository) DeleteTombstone(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := sessionKey(name)
	lock := r.sessionLock(key)
	lock.Lock()
	defer lock.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.remove(filepath.Join(r.dir, tombstoneFilename(key))); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("delete killed session marker: %w", safeFilesystemError(err))
	}
	if err := r.syncDirectory(r.dir); err != nil {
		return fmt.Errorf("sync killed session marker directory: %w", safeFilesystemError(err))
	}
	return nil
}

func tombstoneFilename(key string) string { return ".killed-" + key }

func (r *Repository) tombstoned(name string) (bool, error) {
	if name == "" {
		return false, nil
	}
	data, err := readBounded(filepath.Join(r.dir, tombstoneFilename(sessionKey(name))))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if string(data) != name {
		return false, fmt.Errorf("killed session marker identity mismatch")
	}
	return true, nil
}

func (r *Repository) syncPendingLegacy(ctx context.Context, filename string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.syncDirectory(r.dir); err != nil {
		return fmt.Errorf("sync legacy snapshot directory: %w", safeFilesystemError(err))
	}
	r.pendingLegacySync.Delete(filename)
	return nil
}

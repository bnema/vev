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

const (
	legacyDeleteMarkerPrefix = ".legacy-delete-"
	maxLegacyDeleteMarkers   = maxLegacySnapshotFiles
	maxLegacyDeleteNameBytes = 200
)

type legacyDeleteMarker struct {
	name string
}

// LoadLegacy reads only pre-incremental root .snap files. It is deliberately
// isolated from repository reads so incremental directories are never traversed.
func (r *Repository) LoadLegacy(ctx context.Context) ([]ports.LegacySnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// A marker is an already-authorized deletion, not an import candidate. Finish
	// it before exposing any legacy blobs so process restart cannot republish one.
	if err := r.cleanupPendingLegacyDeletes(ctx); err != nil {
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

// DeleteLegacy durably authorizes deletion before unlinking the legacy source.
// The private marker survives process restart, so LoadLegacy retries an
// authorized deletion rather than returning it for a second import.
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
	if err := r.authorizeLegacyDelete(name); err != nil {
		return err
	}
	return r.deleteAuthorizedLegacy(ctx, name, filename)
}

func legacyDeleteMarkerFilename(key string) string { return legacyDeleteMarkerPrefix + key }

func (r *Repository) authorizeLegacyDelete(name string) error {
	if err := r.ensurePrivateDirectory(r.dir); err != nil {
		return fmt.Errorf("create legacy deletion marker directory: %w", safeFilesystemError(err))
	}
	path := filepath.Join(r.dir, legacyDeleteMarkerFilename(sessionKey(name)))
	if err := r.writeImmutable(path, []byte(name), func(data []byte) error {
		if string(data) != name {
			return fmt.Errorf("legacy deletion marker identity mismatch")
		}
		return nil
	}); err != nil {
		return fmt.Errorf("write legacy deletion marker: %w", safeFilesystemError(err))
	}
	return nil
}

func (r *Repository) deleteAuthorizedLegacy(ctx context.Context, name, filename string) error {
	if _, pending := r.pendingLegacySync.Load(filename); pending {
		if err := r.syncPendingLegacy(ctx, filename); err != nil {
			return err
		}
		return r.clearLegacyDeleteAuthorization(name)
	}
	path := filepath.Join(r.dir, filename)
	if err := r.remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("delete legacy snapshot: %w", safeFilesystemError(err))
	}
	r.pendingLegacySync.Store(filename, struct{}{})
	if err := r.syncPendingLegacy(ctx, filename); err != nil {
		return err
	}
	return r.clearLegacyDeleteAuthorization(name)
}

func (r *Repository) clearLegacyDeleteAuthorization(name string) error {
	path := filepath.Join(r.dir, legacyDeleteMarkerFilename(sessionKey(name)))
	if err := r.remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("clear legacy deletion marker: %w", safeFilesystemError(err))
	}
	if err := r.syncDirectory(r.dir); err != nil {
		return fmt.Errorf("sync legacy deletion marker directory: %w", safeFilesystemError(err))
	}
	return nil
}

func (r *Repository) cleanupPendingLegacyDeletes(ctx context.Context) error {
	markers, err := r.pendingLegacyDeleteMarkers(ctx)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, marker := range markers {
		if err := r.DeleteLegacy(ctx, marker.name); err != nil {
			return fmt.Errorf("retry authorized legacy deletion: %w", err)
		}
	}
	return nil
}

func (r *Repository) pendingLegacyDeleteMarkers(ctx context.Context) ([]legacyDeleteMarker, error) {
	f, err := os.Open(r.dir)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	markers := make([]legacyDeleteMarker, 0, maxLegacyDeleteMarkers)
	for {
		entries, readErr := f.ReadDir(maintenanceBatch)
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if entry.IsDir() || !strings.HasPrefix(entry.Name(), legacyDeleteMarkerPrefix) {
				continue
			}
			if len(markers) == maxLegacyDeleteMarkers {
				return nil, fmt.Errorf("legacy deletion retry has too many markers (maximum %d)", maxLegacyDeleteMarkers)
			}
			key := strings.TrimPrefix(entry.Name(), legacyDeleteMarkerPrefix)
			if !canonicalSessionKey(key) {
				return nil, fmt.Errorf("invalid legacy deletion marker")
			}
			path := filepath.Join(r.dir, entry.Name())
			if info, statErr := os.Lstat(path); statErr != nil || !info.Mode().IsRegular() || info.Size() > maxLegacyDeleteNameBytes {
				return nil, fmt.Errorf("invalid legacy deletion marker")
			}
			data, dataErr := readBounded(path)
			if dataErr != nil || len(data) > maxLegacyDeleteNameBytes || sessionKey(string(data)) != key {
				return nil, fmt.Errorf("invalid legacy deletion marker")
			}
			markers = append(markers, legacyDeleteMarker{name: string(data)})
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("read legacy deletion markers: %w", safeFilesystemError(readErr))
		}
	}
	return markers, nil
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

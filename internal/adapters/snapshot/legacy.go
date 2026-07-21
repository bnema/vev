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

// LoadLegacy reads only pre-incremental root .snap files. Store remains the
// legacy writer until the migration cutover, but is deliberately not used by
// this read path so incremental repository directories are never traversed.
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
		return nil
	} else if err != nil {
		return fmt.Errorf("delete legacy snapshot: %w", safeFilesystemError(err))
	}
	r.pendingLegacySync.Store(filename, struct{}{})
	return r.syncPendingLegacy(ctx, filename)
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

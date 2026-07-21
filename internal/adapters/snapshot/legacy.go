package snapshot

import (
	"context"
	"errors"
	"fmt"
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
	entries, err := os.ReadDir(r.dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read legacy snapshot directory: %w", safeFilesystemError(err))
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	out := make([]ports.LegacySnapshot, 0, len(entries))
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".snap") {
			continue
		}
		data, err := readBounded(filepath.Join(r.dir, entry.Name()))
		if err != nil {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), ".snap")
		if strings.HasPrefix(name, "@") || !safeNameRE.MatchString(name) {
			name = ""
		}
		out = append(out, ports.LegacySnapshot{Name: name, Data: data})
	}
	return out, nil
}

// DeleteLegacy deletes the deterministic v3 file and synchronizes the root.
// A sync failure is returned even though the unlink may have succeeded; retrying
// is safe because an already absent legacy file is considered deleted.
func (r *Repository) DeleteLegacy(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path := filepath.Join(r.dir, filenameForName(name))
	if err := r.remove(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("delete legacy snapshot: %w", safeFilesystemError(err))
	}
	if err := r.syncDirectory(r.dir); err != nil {
		return fmt.Errorf("sync legacy snapshot directory: %w", safeFilesystemError(err))
	}
	return nil
}

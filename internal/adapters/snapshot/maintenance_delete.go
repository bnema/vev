package snapshot

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Delete makes a session unavailable by durably moving it out of the canonical
// namespace. Maintain reaps the private quarantine later; Delete never restores
// a quarantined directory.
func (r *Repository) Delete(ctx context.Context, name string) error {
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

	canonical := r.sessionPath(key)
	sessions := filepath.Dir(canonical)
	// A prior rename may have succeeded while its parent sync failed. Complete
	// that durability boundary before considering a canonical directory: this
	// also leaves a newly recreated session untouched.
	pending, err := pendingQuarantine(sessions, key)
	if err != nil {
		return fmt.Errorf("read deleting snapshot session %q: %w", key, safeFilesystemError(err))
	}
	if pending {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := r.syncDirectory(sessions); err != nil {
			return fmt.Errorf("sync deleted snapshot session directory %q: %w", key, safeFilesystemError(err))
		}
		return nil
	}
	if _, err := os.Lstat(canonical); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("stat snapshot session %q: %w", key, safeFilesystemError(err))
	}
	quarantine := filepath.Join(sessions, ".deleting-"+key+"-"+fmt.Sprintf("%d", time.Now().UnixNano()))
	for attempt := 0; ; attempt++ {
		if _, err := os.Lstat(quarantine); errors.Is(err, os.ErrNotExist) {
			break
		} else if err != nil {
			return fmt.Errorf("stat deleting snapshot session %q: %w", key, safeFilesystemError(err))
		}
		quarantine = filepath.Join(sessions, ".deleting-"+key+"-"+fmt.Sprintf("%d-%d", time.Now().UnixNano(), attempt))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// A renamed session can be replaced by a later publication. Invalidate any
	// mark made for the old namespace before that replacement becomes possible.
	r.invalidateStorageEpoch(key)
	if err := r.rename(canonical, quarantine); err != nil {
		return fmt.Errorf("quarantine snapshot session %q: %w", key, safeFilesystemError(err))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.syncDirectory(sessions); err != nil {
		return fmt.Errorf("sync deleted snapshot session directory %q: %w", key, safeFilesystemError(err))
	}
	return nil
}

func pendingQuarantine(dir, key string) (pending bool, err error) {
	f, err := os.Open(dir)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close deleting snapshot directory: %w", safeFilesystemError(closeErr)))
		}
	}()
	prefix := ".deleting-" + key + "-"
	for {
		entries, err := f.ReadDir(maintenanceBatch)
		for _, entry := range entries {
			if entry.IsDir() && strings.HasPrefix(entry.Name(), prefix) {
				return true, nil
			}
		}
		if errors.Is(err, io.EOF) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
	}
}

// Maintain reaps a bounded amount of stale state. Continuation handles are
// repository-owned, so a stable prefix cannot starve entries later in a large
// directory. Cancellation and errors discard those handles: a restarted
// repository (and the next fresh cycle) always begins at a safe boundary.
func isQuarantine(name string) bool { return strings.HasPrefix(name, ".deleting-") }

// readDirBatch is used only for a mutating quarantine walk, where every
// successful step removes the entry it selected. General maintenance scans use
// repository-owned cursors above.
func readDirBatch(dir string, n int) ([]os.DirEntry, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	entries, readErr := f.ReadDir(n)
	closeErr := f.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return entries, nil
}

func (r *Repository) removeTreeBatch(ctx context.Context, path string, budget *int) (bool, error) {
	changed := false
	for *budget > 0 {
		did, err := r.removeTreeStep(ctx, path, budget)
		if err != nil || !did {
			return changed, err
		}
		changed = true
	}
	return changed, nil
}

func (r *Repository) removeTreeStep(ctx context.Context, path string, budget *int) (bool, error) {
	if *budget == 0 {
		return false, nil
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.IsDir() {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if err := r.remove(path); err != nil {
			return false, err
		}
		*budget--
		return true, nil
	}
	// A quarantine tree contains no live entries. Reopening from its start is
	// safe here because each successful step removes that first descendant.
	entries, err := readDirBatch(path, 1)
	if err != nil {
		return false, err
	}
	if len(entries) == 0 {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if err := r.remove(path); err != nil {
			return false, err
		}
		*budget--
		return true, nil
	}
	return r.removeTreeStep(ctx, filepath.Join(path, entries[0].Name()), budget)
}

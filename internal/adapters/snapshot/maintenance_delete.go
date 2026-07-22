package snapshot

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Delete makes a session unavailable by durably moving it out of the canonical
// namespace. Maintain reaps the private quarantine later; Delete never restores
// a quarantined directory.
func (r *Repository) Delete(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := sessionKey(name)
	lock := r.lockSession(key)
	defer r.unlockSession(lock)
	if err := ctx.Err(); err != nil {
		return err
	}

	canonical := r.sessionPath(key)
	sessions := filepath.Dir(canonical)
	// A prior rename may have succeeded while its parent sync failed. Complete
	// that durability boundary before considering a canonical directory: this
	// also leaves a newly recreated session untouched.
	pending, err := r.pendingQuarantine(sessions, key)
	if err != nil {
		return fmt.Errorf("stat deleting snapshot session %q: %w", key, safeFilesystemError(err))
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
	quarantine := filepath.Join(sessions, deletingSessionName(key))
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

// deletingSessionName is a deterministic pending-delete record. Its presence
// is checked directly rather than scanning an attacker-controlled sessions
// directory, and it prevents a retry from deleting a session republished after
// the initial rename reached disk but its directory sync failed.
func deletingSessionName(key string) string { return ".deleting-" + key }

func (r *Repository) pendingQuarantine(dir, key string) (bool, error) {
	path := filepath.Join(dir, deletingSessionName(key))
	if hook := r.hooks.beforePendingQuarantineCheck; hook != nil {
		hook(path)
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return info.IsDir(), nil
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

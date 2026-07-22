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

// quarantineMaintenance retains one root-to-current path, never an unbounded
// stack of directory handles or names. Parent paths are recovered after a
// removal, while descent resumes from current on the next budgeted step.
type quarantineMaintenance struct {
	root    string
	current string
}

// consumeQuarantineWork charges every filesystem operation that can be driven
// by quarantine contents. This makes deep and wide trees subject to exactly
// the same maintenanceBatch ceiling as removal work.
func (r *Repository) consumeQuarantineWork(budget *int, operation string) bool {
	if *budget == 0 {
		return false
	}
	*budget--
	if hook := r.hooks.beforeMaintenanceWork; hook != nil {
		hook(operation)
	}
	return true
}

// maintainQuarantine makes one bounded depth-first traversal step at a time.
// It deliberately reopens a directory to select its first child: every
// successful child removal changes that prefix, so no directory cursor is
// retained at each hostile nesting level.
func (r *Repository) maintainQuarantine(ctx context.Context, budget *int) (changed, done bool, err error) {
	state := r.maintenanceQuarantine
	if state == nil {
		return false, true, nil
	}
	for *budget > 0 {
		if err := ctx.Err(); err != nil {
			return changed, false, err
		}
		if !r.consumeQuarantineWork(budget, "stat") {
			return changed, false, nil
		}
		info, err := os.Lstat(state.current)
		if errors.Is(err, os.ErrNotExist) {
			if state.current == state.root {
				return changed, true, nil
			}
			state.current = filepath.Dir(state.current)
			continue
		}
		if err != nil {
			return changed, false, err
		}
		if !info.IsDir() {
			if !r.consumeQuarantineWork(budget, "remove") {
				return changed, false, nil
			}
			if err := r.remove(state.current); err != nil && !errors.Is(err, os.ErrNotExist) {
				return changed, false, err
			}
			changed = true
			state.current = filepath.Dir(state.current)
			continue
		}

		if !r.consumeQuarantineWork(budget, "open") {
			return changed, false, nil
		}
		dir, err := os.Open(state.current)
		if err != nil {
			return changed, false, err
		}
		if !r.consumeQuarantineWork(budget, "read") {
			_ = dir.Close()
			return changed, false, nil
		}
		entries, readErr := dir.ReadDir(1)
		closeErr := dir.Close()
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return changed, false, readErr
		}
		if closeErr != nil {
			return changed, false, closeErr
		}
		if len(entries) == 0 {
			if !r.consumeQuarantineWork(budget, "remove") {
				return changed, false, nil
			}
			if err := r.remove(state.current); err != nil && !errors.Is(err, os.ErrNotExist) {
				return changed, false, err
			}
			changed = true
			if state.current == state.root {
				return changed, true, nil
			}
			state.current = filepath.Dir(state.current)
			continue
		}
		if !r.consumeQuarantineWork(budget, "descend") {
			return changed, false, nil
		}
		state.current = filepath.Join(state.current, entries[0].Name())
	}
	return changed, false, nil
}

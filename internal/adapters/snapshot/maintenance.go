package snapshot

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const maintenanceBatch = 64

// These caps bound metadata retained across resumable GC passes. If a session
// exceeds either cap, marking remains conservative and no destructive sweep or
// generation collection occurs in that cycle.
const (
	maxMaintenanceMarkedGenerations = 128
	maxMaintenanceReferences        = 8192
)

// Maintain reaps a bounded amount of stale state. Continuation handles are
// repository-owned, so a stable prefix cannot starve entries later in a large
// directory. Cancellation and errors discard those handles.
func (r *Repository) Maintain(ctx context.Context) (err error) {
	r.maintenanceMu.Lock()
	defer r.maintenanceMu.Unlock()
	if err := ctx.Err(); err != nil {
		r.resetMaintenance()
		return err
	}
	defer func() {
		if err != nil {
			r.resetMaintenance()
		}
	}()
	if r.maintenanceCursors == nil {
		r.maintenanceCursors = make(map[string]*maintenanceCursor)
		r.maintenanceSessions = make(map[string]*sessionMaintenance)
	}

	budget := maintenanceBatch
	if _, err := r.removeTemps(ctx, r.dir, &budget, "root-temps"); err != nil || budget == 0 {
		return err
	}
	if r.maintenanceQuarantine != nil {
		changed, done, err := r.maintainQuarantine(ctx, &budget)
		if err != nil {
			return err
		}
		if changed {
			if err := r.syncDirectory(filepath.Join(r.dir, repositorySessionsDir)); err != nil {
				return fmt.Errorf("sync snapshot maintenance directory: %w", safeFilesystemError(err))
			}
		}
		if done {
			r.maintenanceQuarantine = nil
		}
		return nil
	}
	// Resume an incomplete session before discovering another one. This keeps
	// retained marks (and their session lock/epoch references) bounded even
	// when a hostile repository contains arbitrarily many partial sessions.
	for key := range r.maintenanceSessions {
		lock := r.lockSession(key)
		err = r.maintainSession(ctx, key, &budget)
		r.unlockSession(lock)
		return err
	}
	sessions := filepath.Join(r.dir, repositorySessionsDir)
	entries, _, err := r.readMaintenanceDir(sessions, maintenanceBatch, "sessions")
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read snapshot maintenance sessions: %w", safeFilesystemError(err))
	}
	for i, entry := range entries {
		if budget == 0 {
			r.requeueMaintenanceEntries(sessions, "sessions", entries[i:])
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		path := filepath.Join(sessions, entry.name)
		if isQuarantine(entry.name) {
			r.maintenanceQuarantine = &quarantineMaintenance{root: path, current: path}
			changed, done, err := r.maintainQuarantine(ctx, &budget)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove snapshot quarantine %q: %w", maintenancePath(r.dir, path), safeFilesystemError(err))
			}
			if changed {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := r.syncDirectory(sessions); err != nil {
					return fmt.Errorf("sync snapshot maintenance directory for %q: %w", maintenancePath(r.dir, path), safeFilesystemError(err))
				}
			}
			if done {
				r.maintenanceQuarantine = nil
			}
			if i+1 < len(entries) {
				r.requeueMaintenanceEntries(sessions, "sessions", entries[i+1:])
			}
			return nil
		}
		if !entry.isDir || !canonicalSessionKey(entry.name) {
			continue
		}
		lock := r.lockSession(entry.name)
		err = r.maintainSession(ctx, entry.name, &budget)
		r.unlockSession(lock)
		if err != nil {
			return err
		}
		if r.maintenanceSessions[entry.name] != nil {
			return nil
		}
	}
	return nil
}

// maintenanceCursor is deliberately reopenable metadata, not an open file.
// On Linux, d_off is the seek cookie returned by getdents(2); seeking a newly
// opened descriptor to it resumes the directory stream. Go's File.ReadDir
// cannot be used here: its 8 KiB internal getdents buffer advances the kernel
// offset beyond a short ReadDir result (see os/dir_unix.go), so saving SeekCur
// after ReadDir would skip buffered entries.

package snapshot

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

const (
	deletionTombstoneVersion = uint16(1)
	deletionTombstoneMagic   = "VEVD"
	deletionTombstonesDir    = "deletions"
)

var ErrMaintenanceBudgetTooSmall = errors.New("snapshot: maintenance budget too small")

func encodeDeletionTombstone(tombstone domain.DeletionTombstone) ([]byte, error) {
	if err := tombstone.Validate(); err != nil {
		return nil, err
	}
	if len(tombstone.Name) > 200 {
		return nil, fmt.Errorf("snapshot: deletion tombstone name too long")
	}
	encoded := make([]byte, 4+2+2+len(tombstone.Name)+len(tombstone.IncarnationID))
	copy(encoded, deletionTombstoneMagic)
	binary.BigEndian.PutUint16(encoded[4:6], deletionTombstoneVersion)
	binary.BigEndian.PutUint16(encoded[6:8], uint16(len(tombstone.Name)))
	copy(encoded[8:], tombstone.Name)
	copy(encoded[8+len(tombstone.Name):], tombstone.IncarnationID[:])
	return encoded, nil
}

func decodeDeletionTombstone(encoded []byte) (domain.DeletionTombstone, error) {
	if len(encoded) < 8 {
		return domain.DeletionTombstone{}, fmt.Errorf("snapshot: truncated deletion tombstone")
	}
	if string(encoded[:4]) != deletionTombstoneMagic || binary.BigEndian.Uint16(encoded[4:6]) != deletionTombstoneVersion {
		return domain.DeletionTombstone{}, fmt.Errorf("snapshot: invalid deletion tombstone header")
	}
	nameLength := int(binary.BigEndian.Uint16(encoded[6:8]))
	want := 8 + nameLength + len(domain.IncarnationID{})
	if len(encoded) != want {
		return domain.DeletionTombstone{}, fmt.Errorf("snapshot: malformed deletion tombstone length")
	}
	tombstone := domain.DeletionTombstone{Name: string(encoded[8 : 8+nameLength])}
	copy(tombstone.IncarnationID[:], encoded[8+nameLength:])
	if err := tombstone.Validate(); err != nil {
		return domain.DeletionTombstone{}, err
	}
	return tombstone, nil
}

func (r *Repository) deletionTombstonePath(id domain.IncarnationID) string {
	return filepath.Join(r.dir, deletionTombstonesDir, id.String()+".tombstone")
}

func (r *Repository) WriteDeletionTombstone(ctx context.Context, tombstone domain.DeletionTombstone) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	encoded, err := encodeDeletionTombstone(tombstone)
	if err != nil {
		return err
	}
	dir := filepath.Join(r.dir, deletionTombstonesDir)
	if err := r.ensurePrivateDirectory(dir); err != nil {
		return err
	}
	path := r.deletionTombstonePath(tombstone.IncarnationID)
	return r.writeImmutable(path, encoded, func(existing []byte) error {
		decoded, err := decodeDeletionTombstone(existing)
		if err != nil || decoded != tombstone {
			return fmt.Errorf("snapshot: deletion tombstone identity mismatch")
		}
		return nil
	})
}

func (r *Repository) ListDeletionTombstones(ctx context.Context, cursor ports.DeletionTombstoneCursor, budget ports.MaintenanceBudget) (ports.DeletionTombstonePage, error) {
	if budget.Entries == 0 || budget.Bytes == 0 {
		return ports.DeletionTombstonePage{}, ErrMaintenanceBudgetTooSmall
	}
	dir := filepath.Join(r.dir, deletionTombstonesDir)
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return ports.DeletionTombstonePage{Done: true}, nil
	}
	if err != nil {
		return ports.DeletionTombstonePage{}, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	page := ports.DeletionTombstonePage{}
	seen := make(map[domain.IncarnationID]struct{})
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return ports.DeletionTombstonePage{}, err
		}
		if entry.Name() <= cursor.After {
			continue
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".tombstone") {
			return ports.DeletionTombstonePage{}, fmt.Errorf("snapshot: invalid deletion tombstone entry")
		}
		if uint64(len(page.Tombstones)) >= budget.Entries {
			return page, nil
		}
		data, err := r.readBounded(filepath.Join(dir, entry.Name()))
		if err != nil {
			return ports.DeletionTombstonePage{}, err
		}
		decoded, err := decodeDeletionTombstone(data)
		if err != nil {
			return ports.DeletionTombstonePage{}, err
		}
		charge := uint64(len(entry.Name()) + len(data))
		if charge > budget.Bytes {
			if len(page.Tombstones) == 0 {
				return ports.DeletionTombstonePage{}, ErrMaintenanceBudgetTooSmall
			}
			return page, nil
		}
		if entry.Name() != decoded.IncarnationID.String()+".tombstone" {
			return ports.DeletionTombstonePage{}, fmt.Errorf("snapshot: non-canonical deletion tombstone filename")
		}
		if _, duplicate := seen[decoded.IncarnationID]; duplicate {
			return ports.DeletionTombstonePage{}, fmt.Errorf("snapshot: duplicate deletion tombstone identity")
		}
		seen[decoded.IncarnationID] = struct{}{}
		page.Tombstones = append(page.Tombstones, decoded)
		page.Next.After = entry.Name()
		budget.Bytes -= charge
	}
	page.Done = true
	return page, nil
}

func (r *Repository) DeleteDeletionTombstone(ctx context.Context, id domain.IncarnationID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := incarnationKey(id); err != nil {
		return err
	}
	path := r.deletionTombstonePath(id)
	dir := filepath.Dir(path)
	if _, err := os.Lstat(dir); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	if err := r.remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return r.syncDirectory(dir)
}

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

	canonical := r.legacySessionPath(key)
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
	if dir, err := r.openDirectory(canonical); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("stat snapshot session %q: %w", key, safeFilesystemError(err))
	} else if err := dir.Close(); err != nil {
		return fmt.Errorf("close snapshot session %q: %w", key, safeFilesystemError(err))
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
	file, err := r.openDirectory(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, file.Close()
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
		stat, err := r.stat(state.current)
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
		if !stat.IsDir() {
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
		dir, err := r.openDirectory(state.current)
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

func (r *Repository) QuarantineDeletionSources(ctx context.Context, tombstone domain.DeletionTombstone, includeLegacyName bool) error {
	if err := tombstone.Validate(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	quarantineDir := filepath.Join(r.dir, "quarantine", tombstone.IncarnationID.String())
	if err := r.ensurePrivateDirectory(quarantineDir); err != nil {
		return err
	}
	if err := r.quarantineSource(r.sessionPath(tombstone.IncarnationID), filepath.Join(quarantineDir, "snapshot")); err != nil {
		return err
	}
	if includeLegacyName {
		if err := r.quarantineSource(filepath.Join(r.dir, filenameForName(tombstone.Name)), filepath.Join(quarantineDir, "legacy.snap")); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repository) quarantineSource(source, target string) error {
	if _, err := os.Lstat(target); err == nil {
		if _, sourceErr := os.Lstat(source); errors.Is(sourceErr, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("snapshot: quarantine target already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if _, err := os.Lstat(source); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	if err := r.rename(source, target); err != nil {
		return err
	}
	if err := r.syncDirectory(filepath.Dir(source)); err != nil {
		return err
	}
	if filepath.Clean(filepath.Dir(source)) != filepath.Clean(filepath.Dir(target)) {
		return r.syncDirectory(filepath.Dir(target))
	}
	return nil
}

func (r *Repository) SaveQuarantineDescriptor(context.Context, domain.QuarantineDescriptor) error {
	return errors.New("snapshot: quarantine descriptors not implemented")
}

func (r *Repository) QuarantineIncarnation(ctx context.Context, id domain.IncarnationID) error {
	return r.QuarantineDeletionSources(ctx, domain.DeletionTombstone{Name: "quarantined", IncarnationID: id}, false)
}

func (r *Repository) DeleteIncarnation(context.Context, domain.IncarnationID) error {
	return errors.New("snapshot: direct incarnation deletion not implemented")
}

func (r *Repository) MaintainSession(context.Context, ports.RetentionPlan, ports.MaintenanceBudget) (bool, error) {
	return false, errors.New("snapshot: incarnation retention not implemented")
}

func (r *Repository) Reconcile(_ context.Context, _ []domain.CatalogueRecord, cursor ports.ReconcileCursor, _ ports.MaintenanceBudget) (ports.ReconcileCursor, []ports.ReconcileFinding, error) {
	return cursor, nil, errors.New("snapshot: incarnation reconciliation not implemented")
}

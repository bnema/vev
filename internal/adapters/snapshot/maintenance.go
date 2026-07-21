package snapshot

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bnema/vev/internal/ports"
	codec "github.com/bnema/vev/internal/usecase/snapshot"
)

// maintenanceBatch bounds both a single directory read and the number of
// namespace entries removed by one maintenance pass. A pass never materializes
// an unbounded directory in memory.
const maintenanceBatch = 64

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

func pendingQuarantine(dir, key string) (bool, error) {
	f, err := os.Open(dir)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer f.Close()
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
	if err := r.removeTemps(ctx, r.dir, &budget, "root-temps"); err != nil || budget == 0 {
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
		path := filepath.Join(sessions, entry.Name())
		if isQuarantine(entry.Name()) {
			changed, err := r.removeTreeBatch(ctx, path, &budget)
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
			continue
		}
		if !entry.IsDir() || !canonicalSessionKey(entry.Name()) {
			continue
		}
		lock := r.sessionLock(entry.Name())
		lock.Lock()
		err = r.maintainSession(ctx, entry.Name(), &budget)
		lock.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

type maintenanceCursor struct {
	file    *os.File
	pending []os.DirEntry
}

type manifestMaintenance struct {
	refs     map[ports.SnapshotDigest]codec.ObjectRef
	complete bool
}

type sessionMaintenance struct {
	token         string
	epoch         uint64
	marked        map[uint64]manifestMaintenance
	uncertain     bool
	conservative  bool
	markDone      bool
	manifestQueue []uint64
	sweepQueue    []string
	sweepRootDone bool
}

func (r *Repository) resetMaintenance() {
	for _, cursor := range r.maintenanceCursors {
		if cursor.file != nil {
			_ = cursor.file.Close()
		}
	}
	r.maintenanceCursors = nil
	r.maintenanceSessions = nil
}

func maintenanceCursorID(dir, purpose string) string { return purpose + "\x00" + dir }

// readMaintenanceDir advances one named directory cursor. Callers must return
// entries they could not process with requeueMaintenanceEntries before yielding
// for budget exhaustion. This prevents a ReadDir batch from skipping work when
// an earlier operation spends the remainder of the pass budget.
func (r *Repository) readMaintenanceDir(dir string, n int, purpose string) ([]os.DirEntry, bool, error) {
	id := maintenanceCursorID(dir, purpose)
	cursor := r.maintenanceCursors[id]
	if cursor != nil && len(cursor.pending) != 0 {
		entries := cursor.pending
		cursor.pending = nil
		return entries, cursor.file == nil, nil
	}
	if cursor != nil && cursor.file == nil {
		delete(r.maintenanceCursors, id)
		cursor = nil
	}
	if cursor == nil {
		file, err := os.Open(dir)
		if err != nil {
			return nil, false, err
		}
		cursor = &maintenanceCursor{file: file}
		r.maintenanceCursors[id] = cursor
	}
	entries, err := cursor.file.ReadDir(n)
	if err != nil && !errors.Is(err, io.EOF) {
		delete(r.maintenanceCursors, id)
		closeErr := cursor.file.Close()
		if closeErr != nil {
			return nil, false, closeErr
		}
		return nil, false, err
	}
	if !errors.Is(err, io.EOF) && len(entries) == n {
		return entries, false, nil
	}
	delete(r.maintenanceCursors, id)
	if closeErr := cursor.file.Close(); closeErr != nil {
		return nil, false, closeErr
	}
	return entries, true, nil
}

func (r *Repository) requeueMaintenanceEntries(dir, purpose string, entries []os.DirEntry) {
	if len(entries) == 0 {
		return
	}
	id := maintenanceCursorID(dir, purpose)
	cursor := r.maintenanceCursors[id]
	if cursor == nil {
		cursor = &maintenanceCursor{}
		r.maintenanceCursors[id] = cursor
	}
	cursor.pending = append(entries, cursor.pending...)
}

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

func (r *Repository) maintainSession(ctx context.Context, key string, budget *int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	state, err := r.sessionMaintenanceState(key)
	if err != nil {
		return err
	}
	if err := r.removeTemps(ctx, filepath.Join(r.sessionPath(key), repositoryGenerations), budget, "generation-temps:"+key); err != nil || *budget == 0 {
		return err
	}

	objectRoot := filepath.Join(r.sessionPath(key), repositoryObjectsDir)
	shards, _, err := r.readMaintenanceDir(objectRoot, maintenanceBatch, "object-temps-shards:"+key)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read snapshot object shards: %w", safeFilesystemError(err))
	}
	for i, shard := range shards {
		if *budget == 0 {
			r.requeueMaintenanceEntries(objectRoot, "object-temps-shards:"+key, shards[i:])
			return nil
		}
		if shard.IsDir() {
			if err := r.removeTemps(ctx, filepath.Join(objectRoot, shard.Name()), budget, "object-temps:"+key+":"+shard.Name()); err != nil {
				return err
			}
		}
	}
	if *budget == 0 {
		return nil
	}
	if !state.markDone {
		if err := r.markSession(ctx, key, state); err != nil {
			return err
		}
		if !state.markDone {
			return nil
		}
	}
	if state.epoch != r.storageEpoch(key) {
		// A publication may have left an unpublished manifest after this mark.
		// Discard all mark and sweep cursors; the next pass starts from it.
		r.clearSessionMaintenance(key)
		return nil
	}
	if err := r.removeObsoleteManifests(ctx, key, state, budget); err != nil || *budget == 0 {
		return err
	}
	if state.uncertain {
		// An unreadable or invalid manifest may reference any existing blob. Do
		// not sweep in this cycle; retry marking from a fresh directory pass.
		r.clearSessionMaintenance(key)
		return nil
	}
	if err := r.sweepSession(ctx, key, state, budget); err != nil {
		return err
	}
	return nil
}

func (r *Repository) sessionMaintenanceState(key string) (*sessionMaintenance, error) {
	token, conservative, err := r.maintenanceToken(key)
	if err != nil {
		return nil, err
	}
	state := r.maintenanceSessions[key]
	if state == nil || state.token != token || state.epoch != r.storageEpoch(key) {
		r.clearSessionMaintenance(key)
		state = &sessionMaintenance{
			token:        token,
			epoch:        r.storageEpoch(key),
			conservative: conservative,
			marked:       make(map[uint64]manifestMaintenance),
		}
		r.maintenanceSessions[key] = state
	}
	return state, nil
}

// maintenanceToken is the publication boundary. A missing or corrupt HEAD is
// itself stable maintenance state: we retain every classified reference until a
// valid publication changes that state, rather than restarting the mark pass.
func (r *Repository) maintenanceToken(key string) (string, bool, error) {
	data, exists, err := readOptionalBounded(r.headPath(key))
	if err != nil {
		return "", false, err
	}
	if !exists {
		return "missing", true, nil
	}
	sum := sha256.Sum256(data)
	if _, _, err := r.readHead(key); err != nil {
		return fmt.Sprintf("invalid:%x", sum), true, nil
	}
	return fmt.Sprintf("valid:%x", sum), false, nil
}

func (r *Repository) clearSessionMaintenance(key string) {
	delete(r.maintenanceSessions, key)
	prefix := "\x00" + filepath.Clean(r.sessionPath(key))
	for id, cursor := range r.maintenanceCursors {
		if strings.HasSuffix(id, prefix) || strings.Contains(id, ":"+key+":") || strings.Contains(id, ":"+key+"\x00") {
			if cursor.file != nil {
				_ = cursor.file.Close()
			}
			delete(r.maintenanceCursors, id)
		}
	}
}

func (r *Repository) markSession(ctx context.Context, key string, state *sessionMaintenance) error {
	dir := filepath.Join(r.sessionPath(key), repositoryGenerations)
	entries, done, err := r.readMaintenanceDir(dir, maintenanceBatch, "mark-generations:"+key)
	if errors.Is(err, os.ErrNotExist) {
		state.markDone = true
		return nil
	}
	if err != nil {
		return fmt.Errorf("read snapshot generations: %w", safeFilesystemError(err))
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			continue
		}
		generation, ok := parseGenerationFilename(entry.Name())
		if !ok {
			continue
		}
		item := manifestMaintenance{}
		data, err := readBounded(r.manifestPath(key, generation))
		if err != nil {
			state.uncertain = true
			state.marked[generation] = item
			continue
		}
		manifest, err := codec.UnmarshalManifest(data)
		if err != nil || manifest.Generation != generation || sessionKey(manifest.Name) != key {
			state.uncertain = true
			state.marked[generation] = item
			continue
		}
		item.refs = manifestRefs(manifest)
		if item.refs == nil || !withinGenerationBudget(len(data), item.refs) {
			state.uncertain = true
			state.marked[generation] = item
			continue
		}
		// Valid but incomplete generations are marked too. This protects blobs
		// written before a failed or in-flight publication reaches HEAD.
		_, loadErr := r.loadGeneration(ctx, manifest.Name, key, generation)
		item.complete = loadErr == nil
		state.marked[generation] = item
	}
	if done {
		state.markDone = true
		if !state.conservative {
			state.manifestQueue = retainedManifestQueue(state.marked)
		}
	}
	return nil
}

func retainedManifestQueue(marked map[uint64]manifestMaintenance) []uint64 {
	complete := make([]uint64, 0, len(marked))
	for generation, item := range marked {
		if item.complete {
			complete = append(complete, generation)
		}
	}
	for i := range complete {
		for j := i + 1; j < len(complete); j++ {
			if complete[j] > complete[i] {
				complete[i], complete[j] = complete[j], complete[i]
			}
		}
	}
	if len(complete) <= 2 {
		return nil
	}
	return complete[2:]
}

func (r *Repository) removeObsoleteManifests(ctx context.Context, key string, state *sessionMaintenance, budget *int) error {
	for len(state.manifestQueue) != 0 && *budget > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		generation := state.manifestQueue[0]
		path := r.manifestPath(key, generation)
		if err := r.remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove snapshot manifest %q: %w", maintenancePath(r.dir, path), safeFilesystemError(err))
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := r.syncDirectory(filepath.Dir(path)); err != nil {
			return fmt.Errorf("sync snapshot maintenance directory for %q: %w", maintenancePath(r.dir, path), safeFilesystemError(err))
		}
		state.manifestQueue = state.manifestQueue[1:]
		*budget--
	}
	return nil
}

func (r *Repository) sweepSession(ctx context.Context, key string, state *sessionMaintenance, budget *int) error {
	// Every bounded sweep batch is tied to the mark's storage epoch. Do not use
	// stale references after any publication or replacement.
	if state.epoch != r.storageEpoch(key) {
		r.clearSessionMaintenance(key)
		return nil
	}
	referenced := retainedReferences(state.marked, state.conservative)
	root := filepath.Join(r.sessionPath(key), repositoryObjectsDir)
	if !state.sweepRootDone {
		entries, done, err := r.readMaintenanceDir(root, maintenanceBatch, "sweep-shards:"+key)
		if errors.Is(err, os.ErrNotExist) {
			state.sweepRootDone = true
		} else if err != nil {
			return fmt.Errorf("read snapshot objects: %w", safeFilesystemError(err))
		} else {
			for _, entry := range entries {
				if entry.IsDir() {
					state.sweepQueue = append(state.sweepQueue, entry.Name())
				}
			}
			state.sweepRootDone = done
		}
	}
	if len(state.sweepQueue) != 0 && *budget > 0 {
		shard := state.sweepQueue[0]
		done, err := r.sweepShard(ctx, filepath.Join(root, shard), referenced, budget, key, shard)
		if err != nil {
			return err
		}
		if done {
			state.sweepQueue = state.sweepQueue[1:]
		}
	}
	if state.sweepRootDone && len(state.sweepQueue) == 0 {
		r.clearSessionMaintenance(key)
	}
	return nil
}

func retainedReferences(marked map[uint64]manifestMaintenance, conservative bool) map[ports.SnapshotDigest]struct{} {
	if conservative {
		references := make(map[ports.SnapshotDigest]struct{})
		for _, item := range marked {
			for digest := range item.refs {
				references[digest] = struct{}{}
			}
		}
		return references
	}
	complete := make([]uint64, 0, len(marked))
	for generation, item := range marked {
		if item.complete {
			complete = append(complete, generation)
		}
	}
	for i := range complete {
		for j := i + 1; j < len(complete); j++ {
			if complete[j] > complete[i] {
				complete[i], complete[j] = complete[j], complete[i]
			}
		}
	}
	keep := make(map[uint64]bool, 2)
	for _, generation := range complete {
		if len(keep) == 2 {
			break
		}
		keep[generation] = true
	}
	references := make(map[ports.SnapshotDigest]struct{})
	for generation, item := range marked {
		if !item.complete || keep[generation] {
			for digest := range item.refs {
				references[digest] = struct{}{}
			}
		}
	}
	return references
}

func (r *Repository) sweepShard(ctx context.Context, dir string, referenced map[ports.SnapshotDigest]struct{}, budget *int, key, shard string) (bool, error) {
	entries, done, err := r.readMaintenanceDir(dir, maintenanceBatch, "sweep-objects:"+key+":"+shard)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("read snapshot object shard: %w", safeFilesystemError(err))
	}
	for i, object := range entries {
		if *budget == 0 {
			r.requeueMaintenanceEntries(dir, "sweep-objects:"+key+":"+shard, entries[i:])
			return false, nil
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		digest, ok := parseObjectDigest(object.Name())
		if !ok || object.IsDir() {
			continue
		}
		if _, used := referenced[digest]; used {
			continue
		}
		path := filepath.Join(dir, object.Name())
		if err := r.remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return false, fmt.Errorf("remove snapshot object %q: %w", maintenancePath(r.dir, path), safeFilesystemError(err))
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if err := r.syncDirectory(dir); err != nil {
			return false, fmt.Errorf("sync snapshot maintenance directory for %q: %w", maintenancePath(r.dir, path), safeFilesystemError(err))
		}
		*budget--
	}
	return done, nil
}

func (r *Repository) removeTemps(ctx context.Context, dir string, budget *int, purpose string) error {
	if *budget == 0 {
		return nil
	}
	entries, _, err := r.readMaintenanceDir(dir, maintenanceBatch, purpose)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read snapshot maintenance directory: %w", safeFilesystemError(err))
	}
	for i, entry := range entries {
		if *budget == 0 {
			r.requeueMaintenanceEntries(dir, purpose, entries[i:])
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), ".tmp-") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		if err := r.remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove snapshot maintenance file %q: %w", maintenancePath(r.dir, path), safeFilesystemError(err))
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := r.syncDirectory(dir); err != nil {
			return fmt.Errorf("sync snapshot maintenance directory for %q: %w", maintenancePath(r.dir, path), safeFilesystemError(err))
		}
		*budget--
	}
	return nil
}

// maintenancePath avoids exposing the repository root while retaining a
// path-safe, actionable location in errors.
func maintenancePath(root, path string) string {
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." {
		return filepath.Base(path)
	}
	return rel
}

type safePathError struct {
	op    string
	cause error
}

func (e safePathError) Error() string { return e.op + ": " + e.cause.Error() }
func (e safePathError) Unwrap() error { return e.cause }

// safeFilesystemError preserves error matching without including a filesystem
// path from terminal-facing error text.
func safeFilesystemError(err error) error {
	var pathError *os.PathError
	if errors.As(err, &pathError) {
		return safePathError{op: pathError.Op, cause: pathError.Err}
	}
	return err
}

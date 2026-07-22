package snapshot

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bnema/vev/internal/ports"
	codec "github.com/bnema/vev/internal/usecase/snapshot"
)

type manifestMaintenance struct {
	refs     map[ports.SnapshotDigest]codec.ObjectRef
	complete bool
}

type sessionMaintenance struct {
	lock            *sessionMutex
	token           string
	epoch           uint64
	marked          map[uint64]manifestMaintenance
	referenceCount  int
	uncertain       bool
	conservative    bool
	markDone        bool
	manifestQueue   []uint64
	objectTempShard string
	objectTempsDone bool
	sweepShard      string
	sweepRootDone   bool
}

// canRetainManifest reserves bounded state before retaining a manifest's
// reference map. The caller must leave the map unretained when it returns
// false, and make the cycle conservative instead.
func (s *sessionMaintenance) canRetainManifest(referenceCount int) bool {
	if referenceCount < 0 || len(s.marked) >= maxMaintenanceMarkedGenerations {
		return false
	}
	if referenceCount > maxMaintenanceReferences-s.referenceCount {
		return false
	}
	s.referenceCount += referenceCount
	return true
}

func (r *Repository) maintainSession(ctx context.Context, key string, budget *int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	state, err := r.sessionMaintenanceState(key)
	if err != nil {
		return err
	}
	if _, err := r.removeTemps(ctx, filepath.Join(r.sessionPath(key), repositoryGenerations), budget, "generation-temps:"+key); err != nil || *budget == 0 {
		return err
	}
	if !state.objectTempsDone {
		done, err := r.removeObjectTemps(ctx, key, state, budget)
		if err != nil || !done || *budget == 0 {
			return err
		}
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
	if state.uncertain {
		// An unreadable, invalid, or unretained manifest may reference any
		// existing blob. Do not collect manifests or sweep in this cycle; retry
		// marking from a fresh directory pass.
		r.clearSessionMaintenance(key)
		return nil
	}
	if err := r.removeObsoleteManifests(ctx, key, state, budget); err != nil || *budget == 0 {
		return err
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
			lock:         r.retainSessionState(key),
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
	data, exists, err := r.readOptionalBounded(r.headPath(key))
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
	if state := r.maintenanceSessions[key]; state != nil && state.lock != nil {
		r.releaseSessionReference(state.lock)
	}
	delete(r.maintenanceSessions, key)
	prefix := "\x00" + filepath.Clean(r.sessionPath(key))
	for id := range r.maintenanceCursors {
		if strings.HasSuffix(id, prefix) || strings.Contains(id, ":"+key+":") || strings.Contains(id, ":"+key+"\x00") {
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
		if entry.isDir {
			continue
		}
		generation, ok := parseGenerationFilename(entry.name)
		if !ok {
			continue
		}
		data, err := r.readBounded(r.manifestPath(key, generation))
		if err != nil {
			state.uncertain = true
			continue
		}
		manifest, err := codec.UnmarshalManifest(data)
		if err != nil || manifest.Generation != generation || sessionKey(manifest.Name) != key {
			state.uncertain = true
			continue
		}
		refs, validRefs := maintenanceManifestRefs(manifest)
		if !validRefs || !withinGenerationBudget(len(data), refs) || !state.canRetainManifest(len(refs)) {
			state.uncertain = true
			continue
		}
		// Valid but incomplete generations are marked too. This protects blobs
		// written before a failed or in-flight publication reaches HEAD.
		_, loadErr := r.loadGeneration(ctx, manifest.Name, key, generation)
		state.marked[generation] = manifestMaintenance{refs: refs, complete: loadErr == nil}
	}
	if done {
		state.markDone = true
		if !state.conservative {
			state.manifestQueue = retainedManifestQueue(state.marked)
		}
	}
	return nil
}

// maintenanceManifestRefs applies the GC reference ceiling while building its
// map, rather than first materializing a hostile manifest's entire reference
// set. Conflicting references are invalid just as they are for publication.
func maintenanceManifestRefs(manifest codec.Manifest) (map[ports.SnapshotDigest]codec.ObjectRef, bool) {
	refs := make(map[ports.SnapshotDigest]codec.ObjectRef)
	add := func(ref codec.ObjectRef) bool {
		if old, ok := refs[ref.Digest]; ok {
			return old == ref
		}
		if len(refs) == maxMaintenanceReferences {
			return false
		}
		refs[ref.Digest] = ref
		return true
	}
	for _, tab := range manifest.Tabs {
		for _, pane := range tab.Panes {
			for _, ref := range pane.Sealed {
				if !add(ref) {
					return nil, false
				}
			}
			if !add(pane.Tail) || !add(pane.Visible) {
				return nil, false
			}
		}
	}
	return refs, true
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

// removeObjectTemps performs at most one shard traversal or one root-entry
// discovery per Maintain call. Empty attacker-controlled shards consume no
// removal budget, so this fixed step ceiling prevents an unbounded scan of
// them while retaining only the root cursor and one active shard cursor.
func (r *Repository) removeObjectTemps(ctx context.Context, key string, state *sessionMaintenance, budget *int) (bool, error) {
	root := filepath.Join(r.sessionPath(key), repositoryObjectsDir)
	if *budget == 0 {
		return false, nil
	}
	if state.objectTempShard != "" {
		done, err := r.removeTemps(ctx, filepath.Join(root, state.objectTempShard), budget, "object-temps:"+key+":"+state.objectTempShard)
		if err != nil || !done {
			return false, err
		}
		state.objectTempShard = ""
		return state.objectTempsDone, nil
	}
	if state.objectTempsDone {
		return true, nil
	}
	entries, done, err := r.readMaintenanceDir(root, 1, "object-temps-shards:"+key)
	if errors.Is(err, os.ErrNotExist) {
		state.objectTempsDone = true
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("read snapshot object shards: %w", safeFilesystemError(err))
	}
	state.objectTempsDone = done
	if len(entries) != 0 && entries[0].isDir {
		state.objectTempShard = entries[0].name
	}
	return state.objectTempsDone && state.objectTempShard == "", nil
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
	if state.sweepShard != "" && *budget > 0 {
		shard := state.sweepShard
		done, err := r.sweepShard(ctx, filepath.Join(root, shard), referenced, budget, key, shard)
		if err != nil {
			return err
		}
		if !done {
			return nil
		}
		state.sweepShard = ""
		// Release this shard's cursor before discovering another shard.
		return nil
	}
	if !state.sweepRootDone {
		entries, done, err := r.readMaintenanceDir(root, 1, "sweep-shards:"+key)
		if errors.Is(err, os.ErrNotExist) {
			state.sweepRootDone = true
		} else if err != nil {
			return fmt.Errorf("read snapshot objects: %w", safeFilesystemError(err))
		} else {
			state.sweepRootDone = done
			if len(entries) != 0 && entries[0].isDir {
				state.sweepShard = entries[0].name
			}
		}
	}
	if state.sweepRootDone && state.sweepShard == "" {
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
		digest, ok := parseObjectDigest(object.name)
		if !ok || object.isDir {
			continue
		}
		if _, used := referenced[digest]; used {
			continue
		}
		path := filepath.Join(dir, object.name)
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

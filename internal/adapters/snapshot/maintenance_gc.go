package snapshot

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	codec "github.com/bnema/vev/internal/usecase/snapshot"
)

type retentionMaintenance struct {
	invalidated    atomic.Bool
	token          string
	cursors        map[string]*maintenanceCursor
	references     map[ports.SnapshotDigest]struct{}
	validateQueue  []ports.CheckpointRef
	validateIndex  int
	validated      bool
	manifestsDone  bool
	objectShard    string
	objectRootDone bool
}

type manifestMaintenance struct {
	refs     map[ports.SnapshotDigest]codec.ObjectRef
	complete bool
}

type retentionEntryKind uint8

const (
	retentionManifest retentionEntryKind = iota + 1
	retentionObject
)

type retentionEntryPolicy struct {
	kind        retentionEntryKind
	generations map[uint64]ports.SnapshotDigest
	objects     map[ports.SnapshotDigest]struct{}
}

func (p retentionEntryPolicy) delete(entry maintenanceDirEntry) bool {
	if entry.isDir {
		return false
	}
	switch p.kind {
	case retentionManifest:
		generation, canonical := parseGenerationFilename(entry.name)
		_, retained := p.generations[generation]
		return canonical && !retained
	case retentionObject:
		digest, canonical := parseObjectDigest(entry.name)
		_, retained := p.objects[digest]
		return canonical && !retained
	default:
		return false
	}
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
	if _, err := r.removeTemps(ctx, filepath.Join(r.legacySessionPath(key), repositoryGenerations), budget, "generation-temps:"+key); err != nil || *budget == 0 {
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
	data, exists, err := r.readOptionalBounded(r.legacyHeadPath(key))
	if err != nil {
		return "", false, err
	}
	if !exists {
		return "missing", true, nil
	}
	sum := sha256.Sum256(data)
	if _, _, err := r.readLegacyHead(key); err != nil {
		return fmt.Sprintf("invalid:%x", sum), true, nil
	}
	return fmt.Sprintf("valid:%x", sum), false, nil
}

func (r *Repository) clearSessionMaintenance(key string) {
	if state := r.maintenanceSessions[key]; state != nil && state.lock != nil {
		r.releaseSessionReference(state.lock)
	}
	delete(r.maintenanceSessions, key)
	prefix := "\x00" + filepath.Clean(r.legacySessionPath(key))
	for id := range r.maintenanceCursors {
		if strings.HasSuffix(id, prefix) || strings.Contains(id, ":"+key+":") || strings.Contains(id, ":"+key+"\x00") {
			delete(r.maintenanceCursors, id)
		}
	}
}

func (r *Repository) markSession(ctx context.Context, key string, state *sessionMaintenance) error {
	dir := filepath.Join(r.legacySessionPath(key), repositoryGenerations)
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
		data, err := r.readBounded(r.legacyManifestPath(key, generation))
		if err != nil {
			state.uncertain = true
			continue
		}
		manifest, err := codec.UnmarshalManifest(data)
		if err != nil || manifest.Generation != generation || manifest.IncarnationID.String() != key {
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

// completeGenerationsDescending returns only complete generations in canonical
// newest-first order for both manifest and object retention.
func completeGenerationsDescending(marked map[uint64]manifestMaintenance) []uint64 {
	complete := make([]uint64, 0, len(marked))
	for generation, item := range marked {
		if item.complete {
			complete = append(complete, generation)
		}
	}
	sort.Slice(complete, func(i, j int) bool { return complete[i] > complete[j] })
	return complete
}

func retainedManifestQueue(marked map[uint64]manifestMaintenance) []uint64 {
	complete := completeGenerationsDescending(marked)
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
		path := r.legacyManifestPath(key, generation)
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
	root := filepath.Join(r.legacySessionPath(key), repositoryObjectsDir)
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
	root := filepath.Join(r.legacySessionPath(key), repositoryObjectsDir)
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

// keepSet is the complete catalogue-owned retention index. Directory order and
// HEAD never grant retention authority.
func keepSet(plan ports.RetentionPlan) map[uint64]ports.SnapshotDigest {
	keep := make(map[uint64]ports.SnapshotDigest, len(plan.Keep))
	for _, ref := range plan.Keep {
		keep[ref.Generation] = ref.ManifestDigest
	}
	return keep
}

// MaintainSession incrementally validates and retains only catalogue-indexed
// checkpoints. Every incarnation owns its budget and continuation state.
// processRetentionEntries charges stat-reported size for every entry, requeues
// the unadmitted suffix, and removes only canonical entries rejected by policy.
func (r *Repository) processRetentionEntries(ctx context.Context, state *retentionMaintenance, dir, purpose string, entries []maintenanceDirEntry, budget *ports.MaintenanceBudget, consumedBefore bool, policy retentionEntryPolicy) (admitted bool, err error) {
	removed := false
	defer func() {
		if !removed {
			return
		}
		if syncErr := r.syncDirectory(dir); syncErr != nil {
			admitted = false
			err = errors.Join(err, syncErr)
		}
	}()
	for i, entry := range entries {
		if state.invalidated.Load() {
			return false, nil
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		path := filepath.Join(dir, entry.name)
		info, statErr := r.stat(path)
		if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
			return false, statErr
		}
		size := uint64(0)
		if statErr == nil && info.Size() > 0 {
			size = uint64(info.Size())
		}
		if budget.Entries == 0 || size > budget.Bytes {
			r.requeueRetentionEntries(state, dir, purpose, entries[i:])
			if !consumedBefore && i == 0 {
				return false, ErrMaintenanceBudgetTooSmall
			}
			return false, nil
		}
		budget.Entries--
		budget.Bytes -= size
		if !policy.delete(entry) {
			continue
		}
		if err := r.remove(path); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return false, err
			}
		} else {
			removed = true
		}
	}
	return true, nil
}

func (r *Repository) MaintainSession(ctx context.Context, plan ports.RetentionPlan, budget ports.MaintenanceBudget) (done bool, err error) {
	requested := budget
	if err := ctx.Err(); err != nil {
		return false, err
	}
	key, err := incarnationKey(plan.IncarnationID)
	if err != nil {
		return false, err
	}
	defer func() {
		r.log.Info("snapshot_maintenance_progress",
			"incarnation", plan.IncarnationID.String(),
			"action", "retention",
			"retained", len(plan.Keep),
			"cursor", key,
			"consumed_entries", requested.Entries-budget.Entries,
			"consumed_bytes", requested.Bytes-budget.Bytes,
			"done", done,
			"budget_exhausted", !done && err == nil,
			"err", err,
		)
	}()
	if plan.PinAll {
		r.maintenanceMu.Lock()
		if state := r.retentionSessions[key]; state != nil {
			state.invalidated.Store(true)
		}
		r.clearRetentionMaintenance(key)
		r.maintenanceMu.Unlock()
		return true, nil
	}
	if budget.Entries == 0 || budget.Bytes == 0 {
		return false, ErrMaintenanceBudgetTooSmall
	}

	// Establish per-incarnation state under maintenanceMu, then retain only the
	// session mutex across payload I/O. Never reacquire maintenanceMu while the
	// session mutex is held; PinAll invalidates active state without waiting.
	r.maintenanceMu.Lock()
	lock := r.lockSession(key)
	if r.retentionSessions == nil {
		r.retentionSessions = make(map[string]*retentionMaintenance)
	}

	token := retentionToken(plan)
	state := r.retentionSessions[key]
	if state == nil || state.token != token {
		r.clearRetentionMaintenance(key)
		state = &retentionMaintenance{
			token:         token,
			cursors:       make(map[string]*maintenanceCursor),
			references:    make(map[ports.SnapshotDigest]struct{}),
			validateQueue: append([]ports.CheckpointRef(nil), plan.Keep...),
		}
		r.retentionSessions[key] = state
	}
	r.maintenanceMu.Unlock()
	defer func() {
		r.unlockSession(lock)
		if done {
			r.maintenanceMu.Lock()
			if r.retentionSessions[key] == state {
				delete(r.retentionSessions, key)
			}
			r.maintenanceMu.Unlock()
		}
	}()
	keep := keepSet(plan)
	if !state.validated {
		for state.validateIndex < len(state.validateQueue) {
			ref := state.validateQueue[state.validateIndex]
			if budget.Entries == 0 {
				return false, nil
			}
			validated, consumedBytes, admitted, err := r.validateMaintenanceCheckpoint(ctx, plan.IncarnationID, ref, budget.Bytes)
			if err != nil {
				return false, fmt.Errorf("validate retained manifest %d: %w", ref.Generation, err)
			}
			if !admitted {
				if requested.Entries == budget.Entries && requested.Bytes == budget.Bytes {
					return false, ErrMaintenanceBudgetTooSmall
				}
				return false, nil
			}
			budget.Entries--
			budget.Bytes -= consumedBytes
			for digest := range validated.refs {
				state.references[digest] = struct{}{}
			}
			state.validateIndex++
		}
		state.validated = true
	}

	purpose := "retention-manifests:" + key + ":" + token
	for !state.manifestsDone && budget.Entries > 0 && budget.Bytes > 0 {
		limit := int(min(budget.Entries, uint64(maintenanceBatch)))
		entries, done, err := r.readRetentionDir(state, filepath.Join(r.sessionPath(plan.IncarnationID), repositoryGenerations), limit, purpose)
		if errors.Is(err, os.ErrNotExist) {
			state.manifestsDone = true
			break
		}
		if err != nil {
			return false, err
		}
		dir := filepath.Join(r.sessionPath(plan.IncarnationID), repositoryGenerations)
		consumedBefore := requested.Entries != budget.Entries || requested.Bytes != budget.Bytes
		admitted, err := r.processRetentionEntries(ctx, state, dir, purpose, entries, &budget, consumedBefore, retentionEntryPolicy{kind: retentionManifest, generations: keep})
		if err != nil {
			return false, err
		}
		if !admitted {
			return false, nil
		}
		state.manifestsDone = done
		if len(entries) == 0 && !done {
			return false, nil
		}
	}
	if !state.manifestsDone {
		return false, nil
	}

	objectsRoot := filepath.Join(r.sessionPath(plan.IncarnationID), repositoryObjectsDir)
	for budget.Entries > 0 && budget.Bytes > 0 {
		if state.objectShard != "" {
			shard := state.objectShard
			dir := filepath.Join(objectsRoot, shard)
			objectPurpose := "retention-objects:" + key + ":" + shard + ":" + token
			entries, done, err := r.readRetentionDir(state, dir, int(min(budget.Entries, uint64(maintenanceBatch))), objectPurpose)
			if errors.Is(err, os.ErrNotExist) {
				state.objectShard = ""
				continue
			}
			if err != nil {
				return false, err
			}
			consumedBefore := requested.Entries != budget.Entries || requested.Bytes != budget.Bytes
			admitted, err := r.processRetentionEntries(ctx, state, dir, objectPurpose, entries, &budget, consumedBefore, retentionEntryPolicy{kind: retentionObject, objects: state.references})
			if err != nil {
				return false, err
			}
			if !admitted {
				return false, nil
			}
			if done {
				state.objectShard = ""
			} else if len(entries) == 0 {
				return false, nil
			}
			continue
		}
		if state.objectRootDone {
			return true, nil
		}
		entries, done, err := r.readRetentionDir(state, objectsRoot, 1, "retention-shards:"+key+":"+token)
		if errors.Is(err, os.ErrNotExist) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		state.objectRootDone = done
		if len(entries) == 0 {
			if !done {
				return false, nil
			}
			continue
		}
		budget.Entries--
		if entries[0].isDir {
			state.objectShard = entries[0].name
		}
	}
	return false, nil
}

func (r *Repository) clearRetentionMaintenance(key string) {
	delete(r.retentionSessions, key)
}

func (r *Repository) readRetentionDir(state *retentionMaintenance, dir string, n int, purpose string) ([]maintenanceDirEntry, bool, error) {
	id := maintenanceCursorID(dir, purpose)
	cursor := state.cursors[id]
	if cursor != nil && cursor.pending != nil {
		count := min(n, len(cursor.pending))
		entries := cursor.pending[:count]
		cursor.pending = cursor.pending[count:]
		if len(entries) != 0 {
			cursor.offset = entries[len(entries)-1].cookie
		}
		done := cursor.done && len(cursor.pending) == 0
		if done {
			delete(state.cursors, id)
		} else if count > 0 && len(cursor.pending) == 0 {
			cursor.pending = nil
		}
		return entries, done, nil
	}
	if cursor == nil {
		if len(state.cursors) >= maxMaintenanceCursors {
			return nil, false, errMaintenanceCursorLimit
		}
		cursor = &maintenanceCursor{}
		state.cursors[id] = cursor
	}
	entries, done, err := r.readMaintenanceDirent(dir, n, cursor)
	if err != nil || done {
		delete(state.cursors, id)
	}
	return entries, done, err
}

func (r *Repository) requeueRetentionEntries(state *retentionMaintenance, dir, purpose string, entries []maintenanceDirEntry) {
	if len(entries) == 0 {
		return
	}
	id := maintenanceCursorID(dir, purpose)
	cursor := state.cursors[id]
	if cursor == nil {
		cursor = &maintenanceCursor{done: true}
		state.cursors[id] = cursor
	}
	pending := make([]maintenanceDirEntry, 0, len(entries)+len(cursor.pending))
	pending = append(pending, entries...)
	pending = append(pending, cursor.pending...)
	cursor.pending = pending
}

func retentionToken(plan ports.RetentionPlan) string {
	var b strings.Builder
	b.WriteString(plan.IncarnationID.String())
	for _, ref := range plan.Keep {
		fmt.Fprintf(&b, ":%d:%x", ref.Generation, ref.ManifestDigest)
	}
	return b.String()
}

type validatedMaintenanceCheckpoint struct {
	refs   map[ports.SnapshotDigest]codec.ObjectRef
	parent *domain.CheckpointRef
}

// validateMaintenanceCheckpoint admits every payload by stat-reported size
// before reading it. It validates directly instead of calling LoadCheckpoint,
// which would materialize a complete generation before maintenance could
// enforce its byte budget.
func (r *Repository) validateMaintenanceCheckpoint(ctx context.Context, id domain.IncarnationID, ref ports.CheckpointRef, byteBudget uint64) (validatedMaintenanceCheckpoint, uint64, bool, error) {
	manifestPath := r.manifestPath(id, ref.Generation)
	info, err := r.stat(manifestPath)
	if err != nil {
		return validatedMaintenanceCheckpoint{}, 0, false, err
	}
	if info.Size() < 0 || uint64(info.Size()) > byteBudget {
		return validatedMaintenanceCheckpoint{}, 0, false, nil
	}
	consumed := uint64(info.Size())
	if hook := r.hooks.beforeMaintenancePayloadRead; hook != nil {
		hook(manifestPath)
	}
	data, err := r.readBounded(manifestPath)
	if err != nil {
		return validatedMaintenanceCheckpoint{}, consumed, false, err
	}
	manifest, err := codec.UnmarshalManifest(data)
	if err != nil || manifest.Generation != ref.Generation || manifest.IncarnationID != id || sha256.Sum256(data) != ref.ManifestDigest {
		return validatedMaintenanceCheckpoint{}, consumed, false, errors.New("catalogue reference mismatch")
	}
	refs, valid := maintenanceManifestRefs(manifest)
	if !valid || !withinGenerationBudget(len(data), refs) {
		return validatedMaintenanceCheckpoint{}, consumed, false, errors.New("invalid manifest references")
	}
	for digest, objectRef := range refs {
		if err := ctx.Err(); err != nil {
			return validatedMaintenanceCheckpoint{}, consumed, false, err
		}
		path := r.objectPath(id, digest)
		info, err := r.stat(path)
		if err != nil {
			return validatedMaintenanceCheckpoint{}, consumed, false, err
		}
		if info.Size() < 0 || uint64(info.Size()) > byteBudget-consumed {
			return validatedMaintenanceCheckpoint{}, consumed, false, nil
		}
		consumed += uint64(info.Size())
		if hook := r.hooks.beforeMaintenancePayloadRead; hook != nil {
			hook(path)
		}
		object, err := r.readBounded(path)
		if err != nil {
			return validatedMaintenanceCheckpoint{}, consumed, false, err
		}
		if sha256.Sum256(object) != digest || !validObject(object, objectRef) {
			return validatedMaintenanceCheckpoint{}, consumed, false, errors.New("invalid retained object")
		}
	}
	return validatedMaintenanceCheckpoint{refs: refs, parent: manifest.ParentCheckpoint}, consumed, true, nil
}

// Reconcile scans top-level incarnation namespaces from an external seek
// cookie. Checkpoint-chain work receives a fresh budget for every directory.
func (r *Repository) Reconcile(ctx context.Context, records []domain.CatalogueRecord, cursor ports.ReconcileCursor, budget ports.MaintenanceBudget) (nextCursor ports.ReconcileCursor, findings []ports.ReconcileFinding, retErr error) {
	requested := budget
	defer func() {
		consumed := ports.MaintenanceBudget{}
		for _, finding := range findings {
			consumed.Entries += finding.Consumed.Entries
			consumed.Bytes += finding.Consumed.Bytes
			r.log.Info("snapshot_reconciliation_progress",
				"incarnation", finding.Candidate.IncarnationID.String(),
				"action", finding.Kind,
				"status", finding.Status,
				"consumed_entries", finding.Consumed.Entries,
				"consumed_bytes", finding.Consumed.Bytes,
				"cursor", finding.Cursor.DirectoryCookie,
			)
		}
		r.log.Info("snapshot_reconciliation_complete",
			"action", "scan",
			"cursor", nextCursor.DirectoryCookie,
			"requested_entries", requested.Entries,
			"requested_bytes", requested.Bytes,
			"consumed_entries", consumed.Entries,
			"consumed_bytes", consumed.Bytes,
			"done", nextCursor.DirectoryCookie == 0 && retErr == nil,
			"err", retErr,
		)
	}()
	if err := ctx.Err(); err != nil {
		return cursor, nil, err
	}
	if budget.Entries == 0 || budget.Bytes == 0 {
		return cursor, nil, nil
	}
	offset := int64(cursor.DirectoryCookie)
	if offset < 0 {
		return cursor, nil, errors.New("snapshot: invalid reconciliation cursor")
	}
	byIncarnation := make(map[domain.IncarnationID]domain.CatalogueRecord, len(records))
	for _, record := range records {
		byIncarnation[record.IncarnationID] = record
	}

	root := filepath.Join(r.dir, repositorySessionsDir)
	state := &maintenanceCursor{offset: offset}
	findingCapacity := maintenanceBatch
	if budget.Entries < maintenanceBatch {
		findingCapacity = int(budget.Entries)
	}
	findings = make([]ports.ReconcileFinding, 0, findingCapacity)
	stepConsumed := ports.MaintenanceBudget{}
	resumeCursor := cursor
	for scanned := uint64(0); scanned < budget.Entries && stepConsumed.Entries < budget.Entries; scanned++ {
		entries, done, err := r.readMaintenanceDirent(root, 1, state)
		if errors.Is(err, os.ErrNotExist) {
			return ports.ReconcileCursor{}, findings, nil
		}
		if err != nil {
			return cursor, nil, err
		}
		if len(entries) == 0 {
			if done {
				return ports.ReconcileCursor{}, findings, nil
			}
			continue
		}
		entry := entries[0]
		nextCursor = ports.ReconcileCursor{DirectoryCookie: uint64(entry.cookie)}
		if !entry.isDir || isQuarantine(entry.name) {
			resumeCursor = nextCursor
			if done {
				return ports.ReconcileCursor{}, findings, nil
			}
			continue
		}
		findingIndex := len(findings)
		consumedBeforeCandidate := stepConsumed
		var id domain.IncarnationID
		if err := id.UnmarshalText([]byte(entry.name)); err != nil {
			findings = append(findings, ports.ReconcileFinding{Kind: ports.ReconcileUnknownIncarnation, Status: ports.ReconcileQuarantined, Candidate: ports.ReconcileCandidate{Name: entry.name}, Cursor: nextCursor, Consumed: ports.MaintenanceBudget{Entries: 1}})
		} else if record, ok := byIncarnation[id]; !ok {
			findings = append(findings, ports.ReconcileFinding{Kind: ports.ReconcileUnknownIncarnation, Status: ports.ReconcileQuarantined, Candidate: ports.ReconcileCandidate{Name: entry.name, IncarnationID: id}, Cursor: nextCursor, Consumed: ports.MaintenanceBudget{Entries: 1}})
		} else {
			remaining := ports.MaintenanceBudget{Entries: budget.Entries - stepConsumed.Entries, Bytes: budget.Bytes - stepConsumed.Bytes}
			finding := r.reconcileIncarnation(ctx, record, remaining)
			finding.Cursor = nextCursor
			findings = append(findings, finding)
		}
		if len(findings) > findingIndex {
			stepConsumed.Entries += findings[findingIndex].Consumed.Entries
			stepConsumed.Bytes += findings[findingIndex].Consumed.Bytes
			if findings[findingIndex].Status == ports.ReconcileBudgetExhausted {
				// If a fresh per-call budget cannot admit this candidate, retrying
				// the same fixed budget can never succeed. Report the bounded
				// exhaustion once and deliberately advance so later entries remain
				// reachable. A candidate reached after earlier work is retried on a
				// fresh call because it may fit that call's full budget.
				if consumedBeforeCandidate.Entries == 0 && consumedBeforeCandidate.Bytes == 0 {
					return nextCursor, findings, nil
				}
				return resumeCursor, findings, nil
			}
		}
		resumeCursor = nextCursor
		if done {
			return ports.ReconcileCursor{}, findings, nil
		}
	}
	return nextCursor, findings, nil
}

func (r *Repository) reconcileIncarnation(ctx context.Context, record domain.CatalogueRecord, budget ports.MaintenanceBudget) ports.ReconcileFinding {
	finding := ports.ReconcileFinding{Kind: ports.ReconcileInvalidCandidate, Status: ports.ReconcileQuarantined, Candidate: ports.ReconcileCandidate{Name: record.Name, IncarnationID: record.IncarnationID}, Consumed: ports.MaintenanceBudget{Entries: 1}}
	if record.Committed == nil {
		return finding
	}
	headPath := r.headPath(record.IncarnationID)
	info, err := r.stat(headPath)
	if err != nil || info.Size() < 0 {
		return finding
	}
	headBytes := uint64(info.Size())
	if headBytes > budget.Bytes-finding.Consumed.Bytes {
		finding.Status = ports.ReconcileBudgetExhausted
		return finding
	}
	finding.Consumed.Bytes += headBytes
	if hook := r.hooks.beforeMaintenancePayloadRead; hook != nil {
		hook(headPath)
	}
	generation, digest, err := r.readHead(record.IncarnationID)
	if err != nil {
		return finding
	}
	candidateRef := domain.CheckpointRef{Generation: generation, ManifestDigest: digest}
	finding.Candidate.Ref = candidateRef
	if generation <= record.Committed.Generation {
		finding.Status = ports.ReconcileValidated
		return finding
	}
	finding.Kind = ports.ReconcileForwardOrphan

	candidate, exhausted, err := r.reconcileLoad(ctx, record, candidateRef, &finding.Consumed, budget)
	if exhausted {
		finding.Status = ports.ReconcileBudgetExhausted
		return finding
	}
	if err != nil {
		return finding
	}
	finding.Candidate.Parent = candidate.parent
	parent := candidate.parent
	for parent != nil && *parent != *record.Committed {
		ancestor, exhausted, err := r.reconcileLoad(ctx, record, *parent, &finding.Consumed, budget)
		if exhausted {
			finding.Status = ports.ReconcileBudgetExhausted
			return finding
		}
		if err != nil {
			return finding
		}
		finding.AncestorChain = append(finding.AncestorChain, ports.ValidatedCheckpoint{Ref: *parent, Parent: ancestor.parent})
		parent = ancestor.parent
	}
	if parent == nil {
		return finding
	}
	finding.Status = ports.ReconcileValidated
	return finding
}

func (r *Repository) reconcileLoad(ctx context.Context, record domain.CatalogueRecord, ref domain.CheckpointRef, consumed *ports.MaintenanceBudget, budget ports.MaintenanceBudget) (validatedMaintenanceCheckpoint, bool, error) {
	if consumed.Entries >= budget.Entries || consumed.Bytes >= budget.Bytes {
		return validatedMaintenanceCheckpoint{}, true, nil
	}
	validated, bytes, admitted, err := r.validateMaintenanceCheckpoint(ctx, record.IncarnationID, ref, budget.Bytes-consumed.Bytes)
	consumed.Bytes += bytes
	if err != nil {
		return validatedMaintenanceCheckpoint{}, false, err
	}
	if !admitted {
		return validatedMaintenanceCheckpoint{}, true, nil
	}
	consumed.Entries++
	return validated, false, nil
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
	complete := completeGenerationsDescending(marked)
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

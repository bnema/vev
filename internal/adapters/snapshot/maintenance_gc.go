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

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	codec "github.com/bnema/vev/internal/usecase/snapshot"
)

type retentionMaintenance struct {
	token          string
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
func (r *Repository) MaintainSession(ctx context.Context, plan ports.RetentionPlan, budget ports.MaintenanceBudget) (done bool, err error) {
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
			"retained", len(plan.Keep),
			"cursor", key,
			"budget_exhausted", !done && err == nil,
		)
	}()
	if plan.PinAll {
		r.maintenanceMu.Lock()
		r.clearRetentionMaintenance(key)
		r.maintenanceMu.Unlock()
		return true, nil
	}
	if budget.Entries == 0 || budget.Bytes == 0 {
		return false, nil
	}

	r.maintenanceMu.Lock()
	defer r.maintenanceMu.Unlock()
	lock := r.lockSession(key)
	defer r.unlockSession(lock)
	if r.maintenanceCursors == nil {
		r.maintenanceCursors = make(map[string]*maintenanceCursor)
	}
	if r.retentionSessions == nil {
		r.retentionSessions = make(map[string]*retentionMaintenance)
	}

	token := retentionToken(plan)
	state := r.retentionSessions[key]
	if state == nil || state.token != token {
		r.clearRetentionMaintenance(key)
		state = &retentionMaintenance{
			token:         token,
			references:    make(map[ports.SnapshotDigest]struct{}),
			validateQueue: append([]ports.CheckpointRef(nil), plan.Keep...),
		}
		r.retentionSessions[key] = state
	}
	keep := keepSet(plan)
	if !state.validated {
		for state.validateIndex < len(state.validateQueue) {
			ref := state.validateQueue[state.validateIndex]
			generation, digest := ref.Generation, ref.ManifestDigest
			data, err := r.readBounded(r.manifestPath(plan.IncarnationID, generation))
			if err != nil {
				return false, fmt.Errorf("validate retained manifest %d: %w", generation, err)
			}
			manifest, err := codec.UnmarshalManifest(data)
			if err != nil || manifest.Generation != generation || manifest.IncarnationID != plan.IncarnationID || sha256.Sum256(data) != digest {
				return false, fmt.Errorf("validate retained manifest %d: catalogue reference mismatch", generation)
			}
			loaded, err := r.loadGeneration(ctx, manifest.Name, key, generation)
			if err != nil {
				return false, fmt.Errorf("validate retained manifest %d: %w", generation, err)
			}
			consumedBytes := uint64(len(data))
			for _, object := range loaded.Objects {
				consumedBytes += uint64(len(object))
			}
			if budget.Entries == 0 || consumedBytes > budget.Bytes {
				return false, nil
			}
			budget.Entries--
			budget.Bytes -= consumedBytes
			for digest := range loaded.Objects {
				state.references[digest] = struct{}{}
			}
			state.validateIndex++
		}
		state.validated = true
	}

	purpose := "retention-manifests:" + key + ":" + token
	for !state.manifestsDone && budget.Entries > 0 && budget.Bytes > 0 {
		limit := int(min(budget.Entries, uint64(maintenanceBatch)))
		entries, done, err := r.readMaintenanceDir(filepath.Join(r.sessionPath(plan.IncarnationID), repositoryGenerations), limit, purpose)
		if errors.Is(err, os.ErrNotExist) {
			state.manifestsDone = true
			break
		}
		if err != nil {
			return false, err
		}
		for i, entry := range entries {
			path := r.manifestPath(plan.IncarnationID, 0)
			path = filepath.Join(filepath.Dir(path), entry.name)
			info, statErr := r.stat(path)
			if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
				return false, statErr
			}
			size := uint64(0)
			if statErr == nil && info.Size() > 0 {
				size = uint64(info.Size())
			}
			if budget.Entries == 0 || size > budget.Bytes {
				r.requeueMaintenanceEntries(filepath.Dir(path), purpose, entries[i:])
				return false, nil
			}
			budget.Entries--
			budget.Bytes -= size
			generation, canonical := parseGenerationFilename(entry.name)
			if !entry.isDir && canonical {
				if _, retained := keep[generation]; !retained {
					if err := r.remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
						return false, err
					}
					if err := r.syncDirectory(filepath.Dir(path)); err != nil {
						return false, err
					}
				}
			}
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
			entries, done, err := r.readMaintenanceDir(dir, int(min(budget.Entries, uint64(maintenanceBatch))), objectPurpose)
			if errors.Is(err, os.ErrNotExist) {
				state.objectShard = ""
				continue
			}
			if err != nil {
				return false, err
			}
			for i, entry := range entries {
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
					r.requeueMaintenanceEntries(dir, objectPurpose, entries[i:])
					return false, nil
				}
				budget.Entries--
				budget.Bytes -= size
				digest, canonical := parseObjectDigest(entry.name)
				if !entry.isDir && canonical {
					if _, retained := state.references[digest]; !retained {
						if err := r.remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
							return false, err
						}
						if err := r.syncDirectory(dir); err != nil {
							return false, err
						}
					}
				}
			}
			if done {
				state.objectShard = ""
			} else if len(entries) == 0 {
				return false, nil
			}
			continue
		}
		if state.objectRootDone {
			delete(r.retentionSessions, key)
			return true, nil
		}
		entries, done, err := r.readMaintenanceDir(objectsRoot, 1, "retention-shards:"+key+":"+token)
		if errors.Is(err, os.ErrNotExist) {
			delete(r.retentionSessions, key)
			return true, nil
		}
		if err != nil {
			return false, err
		}
		state.objectRootDone = done
		if len(entries) != 0 {
			budget.Entries--
			if entries[0].isDir {
				state.objectShard = entries[0].name
			}
		}
	}
	return false, nil
}

func (r *Repository) clearRetentionMaintenance(key string) {
	delete(r.retentionSessions, key)
	for id := range r.maintenanceCursors {
		if strings.Contains(id, "retention-") && strings.Contains(id, key) {
			delete(r.maintenanceCursors, id)
		}
	}
}

func retentionToken(plan ports.RetentionPlan) string {
	var b strings.Builder
	b.WriteString(plan.IncarnationID.String())
	for _, ref := range plan.Keep {
		fmt.Fprintf(&b, ":%d:%x", ref.Generation, ref.ManifestDigest)
	}
	return b.String()
}

// Reconcile scans top-level incarnation namespaces from an external seek
// cookie. Checkpoint-chain work receives a fresh budget for every directory.
func (r *Repository) Reconcile(ctx context.Context, records []domain.CatalogueRecord, cursor ports.ReconcileCursor, budget ports.MaintenanceBudget) (ports.ReconcileCursor, []ports.ReconcileFinding, error) {
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

	r.maintenanceMu.Lock()
	defer r.maintenanceMu.Unlock()
	root := filepath.Join(r.dir, repositorySessionsDir)
	state := &maintenanceCursor{offset: offset}
	findingCapacity := maintenanceBatch
	if budget.Entries < maintenanceBatch {
		findingCapacity = int(budget.Entries)
	}
	findings := make([]ports.ReconcileFinding, 0, findingCapacity)
	for scanned := uint64(0); scanned < budget.Entries; scanned++ {
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
		next := ports.ReconcileCursor{DirectoryCookie: uint64(state.offset)}
		if !entry.isDir || isQuarantine(entry.name) {
			if done {
				return ports.ReconcileCursor{}, findings, nil
			}
			continue
		}
		var id domain.IncarnationID
		if err := id.UnmarshalText([]byte(entry.name)); err != nil {
			findings = append(findings, ports.ReconcileFinding{Kind: ports.ReconcileUnknownIncarnation, Status: ports.ReconcileQuarantined, Candidate: ports.ReconcileCandidate{Name: entry.name}, Cursor: next, Consumed: ports.MaintenanceBudget{Entries: 1}})
		} else if record, ok := byIncarnation[id]; !ok {
			findings = append(findings, ports.ReconcileFinding{Kind: ports.ReconcileUnknownIncarnation, Status: ports.ReconcileQuarantined, Candidate: ports.ReconcileCandidate{Name: entry.name, IncarnationID: id}, Cursor: next, Consumed: ports.MaintenanceBudget{Entries: 1}})
		} else {
			finding := r.reconcileIncarnation(ctx, record, budget)
			finding.Cursor = next
			findings = append(findings, finding)
		}
		if done {
			return ports.ReconcileCursor{}, findings, nil
		}
	}
	return ports.ReconcileCursor{DirectoryCookie: uint64(state.offset)}, findings, nil
}

func (r *Repository) reconcileIncarnation(ctx context.Context, record domain.CatalogueRecord, budget ports.MaintenanceBudget) ports.ReconcileFinding {
	finding := ports.ReconcileFinding{Kind: ports.ReconcileInvalidCandidate, Status: ports.ReconcileQuarantined, Candidate: ports.ReconcileCandidate{Name: record.Name, IncarnationID: record.IncarnationID}, Consumed: ports.MaintenanceBudget{Entries: 1}}
	if record.Committed == nil {
		return finding
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
	finding.Candidate.Parent = candidate.ParentCheckpoint
	parent := candidate.ParentCheckpoint
	for parent != nil && *parent != *record.Committed {
		ancestor, exhausted, err := r.reconcileLoad(ctx, record, *parent, &finding.Consumed, budget)
		if exhausted {
			finding.Status = ports.ReconcileBudgetExhausted
			return finding
		}
		if err != nil {
			return finding
		}
		finding.AncestorChain = append(finding.AncestorChain, ports.ValidatedCheckpoint{Ref: *parent, Parent: ancestor.ParentCheckpoint})
		parent = ancestor.ParentCheckpoint
	}
	if parent == nil {
		return finding
	}
	finding.Status = ports.ReconcileValidated
	return finding
}

func (r *Repository) reconcileLoad(ctx context.Context, record domain.CatalogueRecord, ref domain.CheckpointRef, consumed *ports.MaintenanceBudget, budget ports.MaintenanceBudget) (ports.SnapshotGeneration, bool, error) {
	if consumed.Entries >= budget.Entries {
		return ports.SnapshotGeneration{}, true, nil
	}
	generation, err := r.LoadCheckpoint(ctx, record.IncarnationID, record.Name, ref)
	if err != nil {
		return ports.SnapshotGeneration{}, false, err
	}
	bytes := uint64(len(generation.Manifest))
	for _, object := range generation.Objects {
		bytes += uint64(len(object))
	}
	if bytes > budget.Bytes-consumed.Bytes {
		return ports.SnapshotGeneration{}, true, nil
	}
	consumed.Entries++
	consumed.Bytes += bytes
	return generation, false, nil
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

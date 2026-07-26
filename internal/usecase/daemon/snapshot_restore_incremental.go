package daemon

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
)

const catalogueRestoreConcurrency = 8

func initialRuntimeRecoveryState(record domain.CatalogueRecord) (runtimeRecoveryState, chan struct{}) {
	done := make(chan struct{})
	var state runtimeRecoveryState
	switch record.RecoveryState {
	case domain.RecoveryFresh:
		state = runtimeFresh
		close(done)
	case domain.RecoveryHealthy:
		state = runtimeRestoring
	case domain.RecoveryDegraded:
		state = runtimeDegraded
		close(done)
	case domain.RecoveryDeleting:
		state = runtimeDeleting
		close(done)
	default:
		state = runtimeDegraded
		close(done)
	}
	return state, done
}

// restoreIncrementalSnapshots restores only records named by the authoritative
// catalogue. Snapshot repository discovery is deliberately not part of this
// path, so stale or oversized namespaces cannot affect unrelated sessions.
func (d *Daemon) restoreIncrementalSnapshots(ctx context.Context) {
	if d.catalogue == nil {
		return
	}
	d.restoreCatalogue(ctx, d.catalogue.Records())
}

// restoreCatalogue gives every expected record an independent bounded job.
// A failed record is degraded without cancelling any sibling restoration.
func (d *Daemon) restoreCatalogue(ctx context.Context, records []domain.CatalogueRecord) {
	jobs := make(chan domain.CatalogueRecord)
	workers := min(catalogueRestoreConcurrency, len(records))
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for record := range jobs {
				d.ensureCatalogueRegistryEntry(record)
				done := d.recordRestoreDone(record.Name)
				err := d.restoreRecord(ctx, record)
				d.finishRecordRestore(record, err, done)
			}
		})
	}
	for _, record := range records {
		jobs <- record
	}
	close(jobs)
	wg.Wait()
}

func (d *Daemon) ensureCatalogueRegistryEntry(record domain.CatalogueRecord) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if entry, ok := d.stopped[record.Name]; ok && entry.restoreDone != nil {
		return
	}
	state, done := initialRuntimeRecoveryState(record)
	d.stopped[record.Name] = stoppedSession{
		name:        record.Name,
		cwd:         record.Cwd,
		createdAt:   record.CreatedAt,
		incarnation: record.IncarnationID,
		lastUsedSeq: record.LastUsedSeq,
		tabNames:    append([]string(nil), record.TabNames...),
		record:      record,
		state:       state,
		restoreDone: done,
	}
}

func (d *Daemon) recordRestoreDone(name string) chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.stopped[name].restoreDone
}

func closeRuntimeRestoreDone(done chan struct{}) {
	if done == nil {
		return
	}
	select {
	case <-done:
	default:
		close(done)
	}
}

func (d *Daemon) setStoppedRecovery(record domain.CatalogueRecord, state runtimeRecoveryState) {
	d.mu.Lock()
	defer d.mu.Unlock()
	entry, ok := d.stopped[record.Name]
	if !ok {
		return
	}
	entry.record = record
	entry.state = state
	entry.cwd = record.Cwd
	entry.createdAt = record.CreatedAt
	entry.incarnation = record.IncarnationID
	entry.lastUsedSeq = record.LastUsedSeq
	entry.tabNames = append([]string(nil), record.TabNames...)
	d.stopped[record.Name] = entry
}

func (d *Daemon) finishRecordRestore(record domain.CatalogueRecord, restoreErr error, done chan struct{}) {
	if restoreErr != nil {
		if d.catalogue != nil {
			if current, ok := d.catalogue.Record(record.Name); ok && current.IncarnationID == record.IncarnationID {
				record = current
			}
		}
		record.RecoveryState = domain.RecoveryDegraded
		record.DegradedReason = "checkpoint validation failed"
		if errors.Is(restoreErr, context.Canceled) || errors.Is(restoreErr, context.DeadlineExceeded) {
			record.DegradedReason = "restore interrupted"
		}
		if d.catalogue != nil {
			if err := d.catalogue.Replace(record.Name, record); err != nil {
				d.log.Error("marking session degraded failed", "session", record.Name, "err", err)
			}
		}
	}

	d.mu.Lock()
	entry, ok := d.stopped[record.Name]
	if ok {
		if restoreErr != nil {
			entry.record = record
			entry.state = runtimeDegraded
			entry.cwd = record.Cwd
			entry.createdAt = record.CreatedAt
			entry.incarnation = record.IncarnationID
			entry.lastUsedSeq = record.LastUsedSeq
			entry.tabNames = append([]string(nil), record.TabNames...)
			d.stopped[record.Name] = entry
		}
		closeRuntimeRestoreDone(done)
	}
	d.mu.Unlock()
	if restoreErr != nil {
		d.log.Warn("session checkpoint restore failed", "session", record.Name, "err", restoreErr)
	}
}

func (d *Daemon) restoreRecord(ctx context.Context, record domain.CatalogueRecord) error {
	switch record.RecoveryState {
	case domain.RecoveryFresh:
		d.setStoppedRecovery(record, runtimeFresh)
		return nil
	case domain.RecoveryDegraded:
		d.setStoppedRecovery(record, runtimeDegraded)
		return nil
	case domain.RecoveryDeleting:
		d.setStoppedRecovery(record, runtimeDeleting)
		return nil
	case domain.RecoveryHealthy:
		// Continue below.
	default:
		return errors.New("snapshot: invalid catalogue recovery state")
	}
	if d.snapshotRepository == nil {
		return errors.New("snapshot: repository unavailable")
	}

	candidates := checkpointCandidates(record)
	if len(candidates) == 0 {
		return errors.New("snapshot: healthy record has no checkpoint")
	}
	var selected domain.CheckpointRef
	var selectedGeneration ports.SnapshotGeneration
	var selectedSnapshot snapcodec.Session
	selectedIndex := -1
	for i, candidate := range candidates {
		generation, err := d.snapshotRepository.LoadCheckpoint(ctx, record.IncarnationID, record.Name, candidate)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			continue
		}
		snapshot, err := sessionFromGeneration(generation)
		if err != nil || generation.IncarnationID != record.IncarnationID || generation.Name != record.Name || generation.Generation != candidate.Generation {
			continue
		}
		selected, selectedGeneration, selectedSnapshot, selectedIndex = candidate, generation, snapshot, i
		break
	}
	if selectedIndex < 0 {
		return errors.New("snapshot: no catalogue checkpoint validated")
	}

	if selectedIndex > 0 {
		if d.checkpointRecovery == nil {
			return errors.New("snapshot: checkpoint coordinator unavailable for fallback promotion")
		}
		outcome, err := d.checkpointRecovery.PromoteFallback(ctx, record.Name, selected)
		if err != nil {
			return fmt.Errorf("snapshot: promote fallback: %w", err)
		}
		if !outcome.CatalogueCommitted {
			return errors.New("snapshot: fallback promotion did not commit catalogue")
		}
		record = outcome.Record
		if outcome.HEADRepairError != nil {
			d.log.Warn("fallback committed with HEAD repair pending", "session", record.Name, "err", outcome.HEADRepairError)
		}
		d.log.Warn("restored session from fallback checkpoint", "session", record.Name, "generation", selected.Generation)
	} else if err := d.snapshotRepository.RepairHEAD(ctx, record.IncarnationID, selected); err != nil {
		d.log.Warn("checkpoint HEAD repair pending", "session", record.Name, "err", err)
	}

	if err := d.restoreSession(ctx, selectedSnapshot, selectedGeneration.Generation, selected); err != nil {
		return err
	}
	d.setStoppedRecovery(record, runtimeHealthy)
	return nil
}

func checkpointCandidates(record domain.CatalogueRecord) []domain.CheckpointRef {
	candidates := make([]domain.CheckpointRef, 0, 3)
	if record.Committed != nil {
		candidates = append(candidates, *record.Committed)
	}
	for _, fallback := range record.Fallbacks {
		if fallback != nil {
			candidates = append(candidates, *fallback)
		}
	}
	return candidates
}

func sessionFromGeneration(generation ports.SnapshotGeneration) (snapcodec.Session, error) {
	manifest, err := snapcodec.UnmarshalManifest(generation.Manifest)
	if err != nil {
		return snapcodec.Session{}, err
	}
	if manifest.IncarnationID != generation.IncarnationID || manifest.Name != generation.Name || manifest.Generation != generation.Generation {
		return snapcodec.Session{}, fmt.Errorf("snapshot: generation identity mismatch")
	}
	result := snapcodec.Session{Name: manifest.Name, CreatedAt: manifest.CreatedAt, Active: manifest.Active, Tabs: make([]snapcodec.Tab, 0, len(manifest.Tabs))}
	for _, tab := range manifest.Tabs {
		outTab := snapcodec.Tab{StableID: tab.StableID, Cols: tab.Cols, Rows: tab.Rows, NextPaneID: tab.NextPaneID, Focus: tab.Focus, Tree: tab.Tree, Panes: make([]snapcodec.Pane, 0, len(tab.Panes))}
		for _, pane := range tab.Panes {
			outPane := snapcodec.Pane{ID: pane.ID, StableID: pane.StableID, Cwd: pane.Cwd, Process: pane.Process}
			for _, ref := range pane.Sealed {
				data, err := generationObject(generation, ref, snapcodec.HistoryChunk)
				if err != nil {
					return snapcodec.Session{}, err
				}
				outPane.SealedChunks = append(outPane.SealedChunks, data)
			}
			if outPane.Tail, err = generationObject(generation, pane.Tail, snapcodec.HistoryTail); err != nil {
				return snapcodec.Session{}, err
			}
			if outPane.Visible, err = generationObject(generation, pane.Visible, snapcodec.Visible); err != nil {
				return snapcodec.Session{}, err
			}
			outTab.Panes = append(outTab.Panes, outPane)
		}
		result.Tabs = append(result.Tabs, outTab)
	}
	return result, nil
}

func generationObject(generation ports.SnapshotGeneration, ref snapcodec.ObjectRef, kind snapcodec.ObjectKind) ([]byte, error) {
	if ref.Kind != kind {
		return nil, fmt.Errorf("snapshot: object kind mismatch")
	}
	data, ok := generation.Objects[ref.Digest]
	if !ok || uint32(len(data)) != ref.Size || sha256.Sum256(data) != ref.Digest {
		return nil, fmt.Errorf("snapshot: missing object")
	}
	gotKind, payload, err := snapcodec.UnmarshalObject(data)
	if err != nil || gotKind != kind {
		return nil, fmt.Errorf("snapshot: invalid object")
	}
	return append([]byte(nil), payload...), nil
}

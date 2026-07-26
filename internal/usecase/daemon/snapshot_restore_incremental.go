package daemon

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"sync"
	"syscall"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
)

const catalogueRestoreConcurrency = 8

var errRetryableRestoreLoad = errors.New("snapshot: retryable checkpoint load")

func initialSessionState(record domain.CatalogueRecord) (ports.SessionState, chan struct{}) {
	done := make(chan struct{})
	state := ports.SessionStopped
	if record.DegradedReason != "" {
		state = ports.SessionBroken
		close(done)
	} else if record.Committed == nil {
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
	records, err := d.catalogue.Records()
	if err != nil {
		d.log.Error("loading catalogue for snapshot restoration failed", "err", err)
		return
	}
	d.restoreCatalogue(ctx, records)
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
	state, done := initialSessionState(record)
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

// closeRuntimeRestoreDoneLocked closes a per-record restoration barrier.
// Caller must hold d.mu so state transition and waiter release are atomic.
func closeRuntimeRestoreDoneLocked(done chan struct{}) {
	if done == nil {
		return
	}
	select {
	case <-done:
	default:
		close(done)
	}
}

func (d *Daemon) setStoppedRecovery(record domain.CatalogueRecord, state ports.SessionState) {
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
	retryable := errors.Is(restoreErr, errRetryableRestoreLoad)
	degraded := false
	if restoreErr != nil && d.catalogue != nil {
		current, ok, readErr := d.catalogue.Record(record.Name)
		switch {
		case readErr != nil:
			d.log.Error("reading catalogue before degradation failed", "session", record.Name, "err", readErr)
		case ok && current.IncarnationID == record.IncarnationID:
			record = current
			record.DegradedReason = "checkpoint validation failed"
			switch {
			case retryable:
				record.DegradedReason = "checkpoint load failed"
			case errors.Is(restoreErr, context.Canceled) || errors.Is(restoreErr, context.DeadlineExceeded):
				record.DegradedReason = "restore interrupted"
			}
			if err := d.catalogue.Replace(record.Name, record); err != nil {
				d.log.Error("marking session degraded failed", "session", record.Name, "err", err)
			}
			degraded = true
		}
	}

	d.mu.Lock()
	entry, ok := d.stopped[record.Name]
	if ok {
		switch {
		case degraded:
			entry.record = record
			entry.state = ports.SessionBroken
			entry.cwd = record.Cwd
			entry.createdAt = record.CreatedAt
			entry.incarnation = record.IncarnationID
			entry.lastUsedSeq = record.LastUsedSeq
			entry.tabNames = append([]string(nil), record.TabNames...)
			d.stopped[record.Name] = entry
		}
	}
	closeRuntimeRestoreDoneLocked(done)
	d.mu.Unlock()
	if degraded {
		reasonCode := "checkpoint-invalid"
		switch {
		case retryable:
			reasonCode = "checkpoint-load-failed"
		case errors.Is(restoreErr, context.Canceled) || errors.Is(restoreErr, context.DeadlineExceeded):
			reasonCode = "restore-interrupted"
		}
		d.logSessionDegraded(record, reasonCode)
	}
}

func (d *Daemon) restoreRecord(ctx context.Context, record domain.CatalogueRecord) error {
	if record.DegradedReason != "" {
		d.setStoppedRecovery(record, ports.SessionBroken)
		d.logSessionDegraded(record, "persisted-broken")
		return nil
	}
	if record.Committed == nil {
		d.setStoppedRecovery(record, ports.SessionStopped)
		d.logSessionRestoreComplete(record, 0, false)
		return nil
	}
	if d.snapshotRepository == nil {
		return errors.New("snapshot: repository unavailable")
	}

	if record.Committed == nil {
		return errors.New("snapshot: healthy record has no checkpoint")
	}
	selected := *record.Committed
	selectedGeneration, err := d.snapshotRepository.LoadCheckpoint(ctx, record.IncarnationID, record.Name, selected)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if isRetryableRestoreLoadError(err) {
			return fmt.Errorf("%w: %w", errRetryableRestoreLoad, err)
		}
		d.logRejectedRestoreCandidate(record, selected, false, "load-failed", err)
		return errors.New("snapshot: committed checkpoint did not validate")
	}
	selectedSnapshot, err := sessionFromGeneration(selectedGeneration)
	if err != nil {
		d.logRejectedRestoreCandidate(record, selected, false, "decode-failed", err)
		return errors.New("snapshot: committed checkpoint did not validate")
	}
	if selectedGeneration.IncarnationID != record.IncarnationID {
		d.logRejectedRestoreCandidate(record, selected, false, "incarnation-mismatch", nil)
		return errors.New("snapshot: committed checkpoint did not validate")
	}
	if selectedGeneration.Name != record.Name {
		d.logRejectedRestoreCandidate(record, selected, false, "name-mismatch", nil)
		return errors.New("snapshot: committed checkpoint did not validate")
	}
	if selectedGeneration.Generation != selected.Generation {
		d.logRejectedRestoreCandidate(record, selected, false, "generation-mismatch", nil)
		return errors.New("snapshot: committed checkpoint did not validate")
	}
	if err := d.snapshotRepository.RepairHEAD(ctx, record.IncarnationID, selected); err != nil {
		d.log.Warn("snapshot_head_repair_pending", "session", record.Name, "incarnation", record.IncarnationID.String(), "generation", selected.Generation, "reason_code", "repair-failed")
	} else {
		d.log.Info("snapshot_head_repair_complete", "session", record.Name, "incarnation", record.IncarnationID.String(), "generation", selected.Generation)
	}

	if err := d.restoreSession(ctx, selectedSnapshot, selectedGeneration.Generation, selected); err != nil {
		return err
	}
	d.setStoppedRecovery(record, ports.SessionStopped)
	d.logSessionRestoreComplete(record, selected.Generation, false)
	return nil
}

func isRetryableRestoreLoadError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, syscall.ENOSPC) ||
		errors.Is(err, syscall.ENOMEM) ||
		errors.Is(err, syscall.EMFILE) ||
		errors.Is(err, syscall.ENFILE) ||
		errors.Is(err, syscall.EDQUOT) {
		return true
	}
	if netErr, ok := errors.AsType[net.Error](err); ok && netErr.Timeout() {
		return true
	}
	temporary, ok := errors.AsType[interface {
		error
		Temporary() bool
	}](err)
	return ok && temporary.Temporary()
}

func (d *Daemon) logRejectedRestoreCandidate(record domain.CatalogueRecord, candidate domain.CheckpointRef, fallback bool, reason string, err error) {
	kind := "committed"
	if fallback {
		kind = "fallback"
	}
	d.log.Warn("snapshot_restore_candidate_rejected",
		"session", record.Name,
		"incarnation", record.IncarnationID.String(),
		"generation", candidate.Generation,
		"candidate", kind,
		"reason_code", reason,
		"err", err,
	)
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

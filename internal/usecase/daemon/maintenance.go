package daemon

import (
	"context"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

type maintenanceDependencies struct {
	catalogue  ports.Catalogue
	repository ports.SnapshotRepository
}

func newMaintenanceDependencies(catalogue ports.Catalogue, repository ports.SnapshotRepository) maintenanceDependencies {
	return maintenanceDependencies{catalogue: catalogue, repository: repository}
}

func (d *Daemon) startDurableMaintenance() {
	if d == nil {
		return
	}
	d.snapshotWorkerMu.Lock()
	if d.maintenanceWorkerCancel != nil {
		d.snapshotWorkerMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(d.serveCtx)
	d.maintenanceWorkerCancel = cancel
	d.maintenanceWorkerDone = make(chan struct{})
	done := d.maintenanceWorkerDone
	d.snapshotWorkerMu.Unlock()

	go func() {
		defer close(done)
		d.runStartupGarbageCollection(ctx)
	}()
}

// runStartupGarbageCollection waits for catalogue-driven restoration, then
// takes a fresh catalogue snapshot and applies GC exactly once. A catalogue
// read failure never produces a keep set, so destructive collection cannot run
// without known-good catalogue state.
func (d *Daemon) runStartupGarbageCollection(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	case <-d.restoreDone:
	}
	deps := &d.maintenance
	if deps.catalogue == nil || deps.repository == nil {
		return
	}
	records, err := deps.catalogue.Records()
	if err != nil {
		d.log.Warn("snapshot_garbage_collection_skipped", "reason", "catalogue-read-failed", "err", err)
		return
	}
	keep := make(map[domain.IncarnationID]domain.CheckpointRef, len(records))
	for _, record := range records {
		var committed domain.CheckpointRef
		if record.Committed != nil {
			committed = *record.Committed
		}
		keep[record.IncarnationID] = committed
	}
	if err := deps.repository.CollectGarbage(ctx, keep); err != nil {
		d.log.Warn("snapshot_garbage_collection_failed", "incarnations", len(keep), "err", err)
		return
	}
	d.log.Info("snapshot_garbage_collection_complete", "incarnations", len(keep))
}

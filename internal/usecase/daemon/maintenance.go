package daemon

import (
	"context"
	"fmt"
	"time"
)

// snapshotGarbageCollectionInterval spaces the periodic snapshot GC passes. A
// long-running daemon otherwise accumulates superseded generations and objects
// until its next restart, which then pays for the whole backlog.
const snapshotGarbageCollectionInterval = 30 * time.Minute

// startDurableMaintenance runs snapshot GC off the startup critical path: once
// catalogue restoration has finished, then periodically. The recovery
// coordinator fences each incarnation's step against catalogue mutations and
// checkpoint publications, so a mutation waits for one incarnation at most.
func (d *Daemon) startDurableMaintenance() {
	if d == nil || d.recovery == nil || !d.snapshotGarbageCollection {
		return
	}
	d.snapshots.mu.Lock()
	if d.snapshots.maintenanceWorkerCancel != nil {
		d.snapshots.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(d.serveCtx)
	d.snapshots.maintenanceWorkerCancel = cancel
	done := make(chan struct{})
	d.snapshots.maintenanceWorkerDone = done
	d.snapshots.mu.Unlock()

	go func() {
		defer close(done)
		d.runDurableMaintenance(ctx)
	}()
}

// cancelDurableMaintenance stops the GC worker without waiting for it;
// WaitDurableWriters owns the join.
func (d *Daemon) cancelDurableMaintenance() {
	d.snapshots.mu.Lock()
	defer d.snapshots.mu.Unlock()
	d.cancelDurableMaintenanceLocked()
}

// cancelDurableMaintenanceLocked requires snapshotWorker.mu (d.snapshots.mu).
func (d *Daemon) cancelDurableMaintenanceLocked() {
	if d.snapshots.maintenanceWorkerCancel != nil {
		d.snapshots.maintenanceWorkerCancel()
	}
}

func (d *Daemon) runDurableMaintenance(ctx context.Context) {
	// Restoration reads committed checkpoints; collecting afterwards keeps the
	// pass from competing with it for disk and for the coordinator fence.
	select {
	case <-ctx.Done():
		return
	case <-d.restoreDone:
	}
	for {
		if err := d.collectSnapshotGarbage(ctx); err != nil && ctx.Err() == nil {
			d.log.Warn("snapshot_garbage_collection_failed", "err", err)
		}
		timer := d.clock.NewTimer(snapshotGarbageCollectionInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C():
		}
	}
}

// collectSnapshotGarbage runs one coordinator-owned GC pass.
func (d *Daemon) collectSnapshotGarbage(ctx context.Context) error {
	incarnations, err := d.recovery.CollectGarbage(ctx)
	if err != nil {
		return fmt.Errorf("collect snapshots: %w", err)
	}
	d.log.Info("snapshot_garbage_collection_complete", "incarnations", incarnations)
	return nil
}

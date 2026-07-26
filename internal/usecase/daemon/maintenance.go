package daemon

import (
	"context"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

const (
	maintenanceInterval = time.Minute
	maintenanceEntries  = 64
	maintenanceBytes    = 8 << 20
)

// maintenanceDependencies is exclusively owned by runDurableMaintenance's
// single goroutine after startup. Shutdown joins that goroutine before these
// cursor and repair sets can be observed or reused.
type maintenanceDependencies struct {
	catalogue  ports.Catalogue
	repository ports.SnapshotRepository
	unresolved map[domain.IncarnationID]struct{}
}

func newMaintenanceDependencies(catalogue ports.Catalogue, repository ports.SnapshotRepository, unresolved []domain.IncarnationID) maintenanceDependencies {
	deps := maintenanceDependencies{
		catalogue: catalogue, repository: repository,
		unresolved: make(map[domain.IncarnationID]struct{}, len(unresolved)),
	}
	for _, id := range unresolved {
		if id != (domain.IncarnationID{}) {
			deps.unresolved[id] = struct{}{}
		}
	}
	return deps
}

func retentionPlan(record domain.CatalogueRecord, unresolved, restoring bool) ports.RetentionPlan {
	plan := ports.RetentionPlan{IncarnationID: record.IncarnationID}
	if unresolved || restoring || record.RecoveryState == domain.RecoveryDegraded || record.RecoveryState == domain.RecoveryDeleting {
		plan.PinAll = true
		return plan
	}
	if record.Committed != nil {
		plan.Keep = append(plan.Keep, *record.Committed)
	}
	return plan
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

	// Register the completion channel before launch so shutdown can never begin
	// waiting while this durable writer is still unregistered.
	go func() {
		defer close(done)
		d.runDurableMaintenance(ctx)
	}()
}

func (d *Daemon) runDurableMaintenance(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	case <-d.restoreDone:
	}
	for {
		incomplete := d.runDurableMaintenanceTick(ctx)
		if ctx.Err() != nil {
			return
		}
		if incomplete {
			continue
		}
		timer := d.clock.NewTimer(maintenanceInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C():
		}
	}
}

// runDurableMaintenanceTick reports whether bounded work remains and should be
// continued immediately rather than delayed until the periodic interval.
func (d *Daemon) runDurableMaintenanceTick(ctx context.Context) bool {
	deps := &d.maintenance
	if deps.catalogue == nil || deps.repository == nil {
		return false
	}

	records, err := deps.catalogue.Records()
	if err != nil {
		d.log.Warn("durable_maintenance_tick", "action", "catalogue-read", "done", false, "err", err)
		return false
	}
	d.mu.Lock()
	restoringIncarnations := make(map[domain.IncarnationID]struct{})
	for _, stopped := range d.stopped {
		if stopped.state == runtimeRestoring {
			restoringIncarnations[stopped.incarnation] = struct{}{}
		}
	}
	d.mu.Unlock()

	incomplete := false
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return false
		}
		_, unresolved := deps.unresolved[record.IncarnationID]
		_, restoring := restoringIncarnations[record.IncarnationID]
		plan := retentionPlan(record, unresolved, restoring)
		done, err := deps.repository.MaintainSession(ctx, plan, ports.MaintenanceBudget{Entries: maintenanceEntries, Bytes: maintenanceBytes})
		if err == nil && !done {
			incomplete = true
		}
		d.log.Info("durable_maintenance_session",
			"incarnation", record.IncarnationID.String(),
			"action", "retention",
			"pin_all", plan.PinAll,
			"requested_entries", maintenanceEntries,
			"requested_bytes", maintenanceBytes,
			"done", done,
			"err", err,
		)
	}
	return incomplete
}

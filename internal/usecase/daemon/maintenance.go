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

type maintenanceReconciler interface {
	Step(context.Context, ports.ReconcileCursor) (ports.ReconcileCursor, []ports.ReconcileDecision, error)
}

// maintenanceDependencies is exclusively owned by runDurableMaintenance's
// single goroutine after startup. Shutdown joins that goroutine before these
// cursor and repair sets can be observed or reused.
type maintenanceDependencies struct {
	catalogue       ports.Catalogue
	repository      ports.SnapshotRepository
	reconciler      maintenanceReconciler
	unresolved      map[domain.IncarnationID]struct{}
	repairUncertain map[domain.IncarnationID]struct{}
	cursor          ports.ReconcileCursor
}

func newMaintenanceDependencies(catalogue ports.Catalogue, repository ports.SnapshotRepository, reconciler maintenanceReconciler, unresolved []domain.IncarnationID) maintenanceDependencies {
	deps := maintenanceDependencies{
		catalogue: catalogue, repository: repository, reconciler: reconciler,
		unresolved:      make(map[domain.IncarnationID]struct{}, len(unresolved)),
		repairUncertain: make(map[domain.IncarnationID]struct{}),
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
	for _, fallback := range record.Fallbacks {
		if fallback != nil {
			plan.Keep = append(plan.Keep, *fallback)
		}
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
	if deps.catalogue == nil || deps.repository == nil || deps.reconciler == nil {
		return false
	}

	// Reconcile before collection. Until a complete top-level scan reaches EOF,
	// an unseen forward orphan may still be repairable, so retention yields
	// without deleting any incarnation data.
	previousCursor := deps.cursor
	next, decisions, err := deps.reconciler.Step(ctx, previousCursor)
	if err != nil {
		d.log.Warn("durable_reconciliation_tick", "action", "scan", "cursor", deps.cursor.DirectoryCookie, "done", false, "err", err)
		return false
	}
	deps.cursor = next
	for _, decision := range decisions {
		record, ok, err := deps.catalogue.Record(decision.Name)
		if err != nil {
			d.log.Warn("durable_reconciliation_tick", "action", "catalogue-read", "done", false, "err", err)
			return false
		}
		if ok && !decision.RetentionResolved {
			deps.repairUncertain[record.IncarnationID] = struct{}{}
		}
		// The catalogue lookup can miss (e.g. an unknown-incarnation finding for a
		// name the catalogue never held); logging record.IncarnationID.String() in
		// that case would emit a misleading all-zero incarnation, so the field is
		// omitted rather than logged as a fabricated identity.
		args := []any{
			"session", decision.Name,
			"action", decision.Kind,
			"reason_code", decision.ReasonCode,
			"cursor", next.DirectoryCookie,
		}
		if ok {
			args = append([]any{"incarnation", record.IncarnationID.String()}, args...)
		}
		d.log.Info("durable_reconciliation_decision", args...)
	}
	d.log.Info("durable_reconciliation_tick", "action", "scan", "cursor", next.DirectoryCookie, "done", next.DirectoryCookie == 0, "err", nil)
	if next.DirectoryCookie != 0 {
		return next.DirectoryCookie != previousCursor.DirectoryCookie
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
		_, uncertainRepair := deps.repairUncertain[record.IncarnationID]
		_, restoring := restoringIncarnations[record.IncarnationID]
		plan := retentionPlan(record, unresolved || uncertainRepair, restoring)
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
	clear(deps.repairUncertain)
	return incomplete
}

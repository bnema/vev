package daemon

import "github.com/bnema/vev/internal/domain"

func (d *Daemon) logSessionRestoreComplete(record domain.CatalogueRecord, generation uint64, fallback bool) {
	d.log.Info("session_restore_complete",
		"session", record.Name,
		"incarnation", record.IncarnationID.String(),
		"generation", generation,
		"fallback", fallback,
	)
}

func (d *Daemon) logSessionDegraded(record domain.CatalogueRecord, reasonCode string) {
	d.log.Warn("session_degraded",
		"session", record.Name,
		"incarnation", record.IncarnationID.String(),
		"reason_code", reasonCode,
	)
}

func (d *Daemon) logStartupRecoveryCounts(restoring int) {
	var records []domain.CatalogueRecord
	if d.persistEnabled && d.catalogue != nil {
		var err error
		records, err = d.catalogue.Records()
		if err != nil {
			d.log.Error("daemon_startup_catalogue_read_failed", "err", err)
			return
		}
	} else {
		d.mu.Lock()
		records = make([]domain.CatalogueRecord, 0, len(d.stopped))
		for _, entry := range d.stopped {
			records = append(records, entry.record)
		}
		d.mu.Unlock()
	}
	healthy, fresh, degraded := 0, 0, 0
	for _, record := range records {
		switch record.RecoveryState {
		case domain.RecoveryHealthy:
			healthy++
		case domain.RecoveryFresh:
			fresh++
		case domain.RecoveryDegraded:
			degraded++
		}
	}
	d.log.Info("daemon_startup_complete",
		"healthy", healthy,
		"fresh", fresh,
		"restoring", restoring,
		"degraded", degraded,
	)
}

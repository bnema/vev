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
			records = d.inactiveRecords()
		}
	} else {
		records = d.inactiveRecords()
	}
	healthy, fresh, broken := 0, 0, 0
	for _, record := range records {
		switch {
		case record.DegradedReason != "":
			broken++
		case record.Committed == nil:
			fresh++
		default:
			healthy++
		}
	}
	d.log.Info("daemon_startup_complete",
		"healthy", healthy,
		"fresh", fresh,
		"restoring", restoring,
		"broken", broken,
	)
}

func (d *Daemon) inactiveRecords() []domain.CatalogueRecord {
	d.mu.Lock()
	defer d.mu.Unlock()
	records := make([]domain.CatalogueRecord, 0, len(d.inactive))
	for _, entry := range d.inactive {
		records = append(records, entry.record)
	}
	return records
}

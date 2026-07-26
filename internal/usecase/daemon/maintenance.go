package daemon

import (
	"context"
	"fmt"

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

// CollectStartupGarbage runs the one GC pass before the daemon socket is
// published. In production the recovery coordinator fences the catalogue
// snapshot and collection against every durable mutation and checkpoint
// publication. The fallback keeps standalone daemon tests usable when no
// coordinator is installed.
func (d *Daemon) CollectStartupGarbage(ctx context.Context) error {
	if d == nil {
		return nil
	}
	if d.recovery != nil {
		incarnations, err := d.recovery.CollectGarbage(ctx)
		if err != nil {
			return fmt.Errorf("collect startup snapshots: %w", err)
		}
		d.log.Info("snapshot_garbage_collection_complete", "incarnations", incarnations)
		return nil
	}

	deps := d.maintenance
	if deps.catalogue == nil || deps.repository == nil {
		return nil
	}
	records, err := deps.catalogue.Records()
	if err != nil {
		return fmt.Errorf("read startup catalogue: %w", err)
	}
	keep := make(map[domain.IncarnationID]domain.CheckpointRef, len(records))
	for _, record := range records {
		if record.Committed != nil {
			keep[record.IncarnationID] = *record.Committed
			continue
		}
		keep[record.IncarnationID] = domain.CheckpointRef{}
	}
	if err := deps.repository.CollectGarbage(ctx, keep); err != nil {
		return fmt.Errorf("collect startup snapshots: %w", err)
	}
	d.log.Info("snapshot_garbage_collection_complete", "incarnations", len(keep))
	return nil
}

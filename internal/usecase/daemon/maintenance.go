package daemon

import (
	"context"
	"fmt"
)

// CollectStartupGarbage runs the one coordinator-owned GC pass before the
// daemon socket is published.
func (d *Daemon) CollectStartupGarbage(ctx context.Context) error {
	if d == nil || d.recovery == nil {
		return nil
	}
	incarnations, err := d.recovery.CollectGarbage(ctx)
	if err != nil {
		return fmt.Errorf("collect startup snapshots: %w", err)
	}
	d.log.Info("snapshot_garbage_collection_complete", "incarnations", incarnations)
	return nil
}

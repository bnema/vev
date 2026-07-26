package daemon

import "context"

// importLegacySnapshots is retained only for tests covering the retired
// pre-migration bridge. Normal runtime legacy decoding lives exclusively in
// the snapshot migration adapter.
func (d *Daemon) importLegacySnapshots(context.Context) {}

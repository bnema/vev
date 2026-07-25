package daemon

import (
	"bytes"
	"context"
	"fmt"

	"github.com/bnema/vev/internal/ports"
)

// importLegacySnapshots is retained only for tests covering the retired
// pre-migration bridge. Normal runtime legacy decoding lives exclusively in
// the snapshot migration adapter.
func (d *Daemon) importLegacySnapshots(context.Context) {}

func verifyLegacyImportGeneration(generation ports.SnapshotGeneration, publication ports.SnapshotPublication) error {
	if generation.Name != publication.Name || generation.Generation != publication.Generation || !bytes.Equal(generation.Manifest, publication.Manifest) {
		return fmt.Errorf("snapshot: legacy import verification mismatch")
	}
	return nil
}

// Package recovery coordinates crash-resumable operations across the catalogue
// and snapshot repository without depending on filesystem adapters.
package recovery

import (
	"context"
	"errors"
	"io"

	"github.com/bnema/vev/internal/ports"
)

var ErrPendingRecoveryUnsupported = errors.New("pending recovery intent requires transaction recovery")

var recoveryListingBudget = ports.MaintenanceBudget{Entries: 64, Bytes: 64 << 10}

type Coordinator struct {
	catalogue  ports.Catalogue
	repository ports.SnapshotRepository
	journal    ports.RecoveryJournal
	random     io.Reader
}

func NewCoordinator(catalogue ports.Catalogue, repository ports.SnapshotRepository, journal ports.RecoveryJournal, random io.Reader) *Coordinator {
	return &Coordinator{catalogue: catalogue, repository: repository, journal: journal, random: random}
}

// Recover is deliberately fail-closed until the transaction roll-forward
// handlers are introduced. Startup may proceed only when both intent sources
// have been completely and strictly enumerated.
func (c *Coordinator) Recover(ctx context.Context) error {
	if c == nil || c.repository == nil || c.journal == nil {
		return errors.New("recovery: incomplete coordinator dependencies")
	}
	intents, err := c.journal.ListDiscards(ctx)
	if err != nil {
		return err
	}
	pending := len(intents) != 0
	cursor := ports.DeletionTombstoneCursor{}
	for {
		page, err := c.repository.ListDeletionTombstones(ctx, cursor, recoveryListingBudget)
		if err != nil {
			return err
		}
		if len(page.Tombstones) != 0 {
			pending = true
		}
		if page.Done {
			break
		}
		if page.Next.After == cursor.After {
			return errors.New("recovery: tombstone listing did not advance")
		}
		cursor = page.Next
	}
	if pending {
		return ErrPendingRecoveryUnsupported
	}
	return nil
}

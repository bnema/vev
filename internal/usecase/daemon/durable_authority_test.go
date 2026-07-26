package daemon

import (
	"context"
	"crypto/rand"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/persist"
	"github.com/bnema/vev/internal/ports"
	recoveryusecase "github.com/bnema/vev/internal/usecase/recovery"
)

type testLifecycleRepository struct{ ports.SnapshotRepository }

func (testLifecycleRepository) WriteDeletionTombstone(context.Context, domain.DeletionTombstone) error {
	return nil
}
func (testLifecycleRepository) QuarantineDeletionSources(context.Context, domain.DeletionTombstone, bool) error {
	return nil
}
func (testLifecycleRepository) DeleteDeletionTombstone(context.Context, domain.IncarnationID) error {
	return nil
}

// WithStore is package-test composition glue. Production injects the same
// recovery coordinator explicitly from internal/app.
func WithStore(store ports.Store) Option {
	return func(d *Daemon) {
		if store == nil {
			return
		}
		catalogue := persist.New(store)
		records, err := catalogue.Records()
		if err != nil {
			return
		}
		d.catalogue = catalogue
		d.persistEnabled = true
		d.catalogueRecords = records
		d.catalogueRecordsProvided = true
		repository := d.snapshotRepository
		if repository == nil {
			repository = testLifecycleRepository{}
		}
		authority := recoveryusecase.NewCoordinator(catalogue, repository, nil, rand.Reader)
		d.lifecycleRecovery = authority
		d.checkpointRecovery = authority
	}
}

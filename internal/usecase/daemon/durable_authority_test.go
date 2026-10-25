package daemon

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/persist"
	"github.com/bnema/vev/internal/ports"
	recoveryusecase "github.com/bnema/vev/internal/usecase/recovery"
)

// WithStore is package-test composition glue. Production injects the same
// recovery coordinator explicitly from internal/app.
func WithStore(t testing.TB, store ports.Store) Option {
	t.Helper()
	return func(d *Daemon) {
		if store == nil {
			return
		}
		catalogue := persist.New(store)
		records, err := catalogue.Records()
		require.NoError(t, err)
		d.catalogue = catalogue
		d.persistEnabled = true
		d.catalogueRecords = records
		d.catalogueRecordsProvided = true
		repository := d.snapshotRepository
		if repository == nil {
			repository = newSnapshotRepositoryMock(t)
		}
		d.recovery = recoveryusecase.NewCoordinator(catalogue, repository, rand.Reader)
	}
}

func testPersister(t testing.TB, d *Daemon) *persist.Persister {
	t.Helper()
	persister, ok := d.catalogue.(*persist.Persister)
	require.True(t, ok, "catalogue type %T is not *persist.Persister", d.catalogue)
	return persister
}

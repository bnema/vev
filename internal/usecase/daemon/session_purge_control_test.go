package daemon

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/persist"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

func TestStoppedPurgeMetadataFailureFencesCatalogue(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	repository := &retryablePurgeRepository{}
	WithSnapshotRepository(repository)(d)
	store, state := newMockStore(t)
	WithStore(t, store)(d)
	record := domain.CatalogueRecord{Name: "work", IncarnationID: domain.IncarnationID{1}}
	require.NoError(t, d.catalogue.Create(record))
	metadataErr := errors.New("metadata delete failed")
	state.mu.Lock()
	state.deleteErr = func(string) error { return metadataErr }
	state.mu.Unlock()
	d.stopped["work"] = stoppedSession{name: "work", incarnation: record.IncarnationID, record: record}

	require.ErrorIs(t, d.retryStoppedPurge("work"), metadataErr)
	require.Empty(t, repository.calls, "snapshot deletion must wait for catalogue removal")

	state.mu.Lock()
	state.deleteErr = nil
	state.mu.Unlock()
	require.ErrorIs(t, d.retryStoppedPurge("work"), persist.ErrCatalogueDurability)
	require.Empty(t, repository.calls, "a fenced catalogue must not retry deletion")
}

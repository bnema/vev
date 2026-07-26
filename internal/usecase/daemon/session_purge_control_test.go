package daemon

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

func TestStoppedPurgeMetadataFailureRemainsFencedForRetry(t *testing.T) {
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
	require.NoError(t, d.retryStoppedPurge("work"))
	require.Equal(t, []string{"delete incarnation"}, repository.calls)
}

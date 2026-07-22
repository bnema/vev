package daemon

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

func TestHandleKillStoppedUsesDurableBothSourcePurge(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	repository := &retryablePurgeRepository{legacyErr: errors.New("legacy delete failed")}
	WithSnapshotRepository(repository, repository)(d)
	d.stopped["work"] = stoppedSession{name: "work"}

	transport := portsmocks.NewMockTransport(t)
	transport.EXPECT().Send(mock.Anything).Return(nil).Once()
	transport.EXPECT().Close().Return(nil).Once()
	d.handleKill(transport, ports.Frame{Type: ports.MsgKill, Payload: ports.MarshalKill(ports.Kill{Name: "work"})})

	require.Equal(t, []string{"tombstone", "incremental", "legacy"}, repository.calls)
	require.True(t, repository.tombstoned["work"])
	require.True(t, d.stopped["work"].purging)
}

func TestStoppedPurgeMetadataFailureRemainsFencedForRetry(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	repository := &retryablePurgeRepository{}
	WithSnapshotRepository(repository, repository)(d)
	store, state := newMockStore(t)
	WithStore(store)(d)
	metadataErr := errors.New("metadata delete failed")
	state.mu.Lock()
	state.deleteErr = func(string) error { return metadataErr }
	state.mu.Unlock()
	d.stopped["work"] = stoppedSession{name: "work"}

	require.ErrorIs(t, d.retryStoppedPurge("work"), metadataErr)
	require.True(t, repository.tombstoned["work"])
	require.Equal(t, []string{"tombstone", "incremental", "legacy"}, repository.calls)

	state.mu.Lock()
	state.deleteErr = nil
	state.mu.Unlock()
	require.NoError(t, d.retryStoppedPurge("work"))
	require.Equal(t, []string{"tombstone", "incremental", "legacy", "tombstone", "incremental", "legacy", "clear tombstone"}, repository.calls)
}

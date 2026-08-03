package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

func TestRenamePreservesIncarnationSnapshotSources(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	repository := &retryablePurgeRepository{}
	WithSnapshotRepository(repository)(d)
	store, state := newMockStore(t)
	WithStore(t, store)(d)
	sess := newSnapshotTestSession(t, "old", false, "/work")
	d.sessions = map[domain.SessionID]*session{sess.id: sess}
	sess.mu.Lock()
	record := sess.persistRecordLocked(1)
	sess.mu.Unlock()
	require.NoError(t, testPersister(t, d).Save(record))

	require.NoError(t, d.renameSession(sess, "new"))
	require.Empty(t, repository.calls, "rename must not delete incarnation-keyed snapshots")
	require.False(t, state.has("old"))
	require.True(t, state.has("new"))
	require.Equal(t, sess.incarnation, state.record(t, "new").IncarnationID)
}

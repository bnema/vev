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
	WithSnapshotRepository(repository, repository)(d)
	store, state := newMockStore(t)
	WithStore(store)(d)
	sess := newSnapshotTestSession(t, "old", false, "/work")
	d.sessions = map[domain.SessionID]*session{sess.id: sess}
	require.NoError(t, d.persist.Save(sess.persistRecordLocked(1)))

	require.NoError(t, d.renameSession(sess, "new"))
	require.Empty(t, repository.calls, "rename must not touch incarnation-keyed snapshot storage")
	require.False(t, state.has("old"))
	require.True(t, state.has("new"))
	require.Equal(t, sess.incarnation, state.record(t, "new").IncarnationID)
}

func TestRenameIgnoresUnrelatedLegacySourceFailure(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	repository := &retryablePurgeRepository{}
	WithSnapshotRepository(repository, repository)(d)
	store, state := newMockStore(t)
	WithStore(store)(d)
	sess := newSnapshotTestSession(t, "old", false, "/work")
	d.sessions = map[domain.SessionID]*session{sess.id: sess}
	require.NoError(t, d.persist.Save(sess.persistRecordLocked(1)))

	require.NoError(t, d.renameSession(sess, "new"))
	require.Empty(t, repository.calls)
	require.False(t, state.has("old"))
	require.True(t, state.has("new"))
}

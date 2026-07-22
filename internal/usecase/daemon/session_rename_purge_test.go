package daemon

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

func TestRenamePurgesOldIncrementalAndLegacyBeforeCommittingNewIdentity(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	repository := &retryablePurgeRepository{}
	WithSnapshotRepository(repository, repository)(d)
	store, state := newMockStore(t)
	WithStore(store)(d)
	sess := newSnapshotTestSession(t, "old", false, "/work")
	d.sessions = map[domain.SessionID]*session{sess.id: sess}
	require.NoError(t, d.persist.Save(sess.persistRecordLocked(1)))

	require.NoError(t, d.renameSession(sess, "new"))
	require.Equal(t, []string{"tombstone", "incremental", "legacy", "clear tombstone"}, repository.calls)
	require.False(t, state.has("old"))
	require.True(t, state.has("new"))
	require.False(t, repository.tombstoned["old"])
}

func TestRenameLegacyFailureLeavesOldIdentityFenced(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	repository := &retryablePurgeRepository{legacyErr: errors.New("legacy delete failed")}
	WithSnapshotRepository(repository, repository)(d)
	store, state := newMockStore(t)
	WithStore(store)(d)
	sess := newSnapshotTestSession(t, "old", false, "/work")
	d.sessions = map[domain.SessionID]*session{sess.id: sess}
	require.NoError(t, d.persist.Save(sess.persistRecordLocked(1)))

	require.Error(t, d.renameSession(sess, "new"))
	require.Equal(t, []string{"tombstone", "incremental", "legacy"}, repository.calls)
	require.True(t, repository.tombstoned["old"])
	require.True(t, state.has("old"))
	require.False(t, state.has("new"))
}

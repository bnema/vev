package daemon

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
)

type retryablePurgeRepository struct {
	*portsmocks.MockSnapshotRepository
	deleteErr      error
	calls          []string
	deadlineScoped []bool
}

// newRetryablePurgeRepository records each DeleteIncarnation call and whether
// its context carried a deadline, then fails with deleteErr.
func newRetryablePurgeRepository(t testing.TB, deleteErr error) *retryablePurgeRepository {
	t.Helper()
	r := &retryablePurgeRepository{deleteErr: deleteErr}
	r.MockSnapshotRepository = newSnapshotRepositoryMock(t, func(m *portsmocks.MockSnapshotRepository) {
		m.EXPECT().DeleteIncarnation(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, _ domain.IncarnationID) error {
			_, hasDeadline := ctx.Deadline()
			r.deadlineScoped = append(r.deadlineScoped, hasDeadline)
			r.calls = append(r.calls, "delete incarnation")
			return r.deleteErr
		}).Maybe()
	})
	return r
}

func TestLivePurgeLeavesFailedDirectoryDeletionForBackgroundGarbageCollection(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), newStubClock(t))
	repository := newRetryablePurgeRepository(t, errors.New("delete failed"))
	WithSnapshotRepository(repository)(d)
	store, _ := newMockStore(t)
	WithStore(t, store)(d)
	sess := newSnapshotTestSession(t, "work", false, "/work")
	record := sess.persistRecordLocked(1)
	require.NoError(t, d.catalogue.Create(record))
	pty, ok := sess.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
	require.True(t, ok)
	pty.EXPECT().Close().Return(nil).Once()
	d.sessions = map[domain.SessionID]*session{sess.id: sess}

	require.Error(t, d.killSession(sess, protocol.ReasonSessionKilled, true))
	require.Equal(t, []string{"delete incarnation"}, repository.calls)
	_, exists, err := d.catalogue.Record("work")
	require.NoError(t, err)
	require.False(t, exists, "catalogue removal commits before best-effort directory deletion")
	require.Equal(t, []bool{true}, repository.deadlineScoped)
}

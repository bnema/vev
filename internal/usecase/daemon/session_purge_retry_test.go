package daemon

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

type retryablePurgeRepository struct {
	noOpSnapshotRepository
	deleteErr      error
	calls          []string
	deadlineScoped []bool
}

func (r *retryablePurgeRepository) DeleteIncarnation(ctx context.Context, _ domain.IncarnationID) error {
	_, hasDeadline := ctx.Deadline()
	r.deadlineScoped = append(r.deadlineScoped, hasDeadline)
	r.calls = append(r.calls, "delete incarnation")
	return r.deleteErr
}

func TestLivePurgeLeavesFailedDirectoryDeletionForStartupGarbageCollection(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	repository := &retryablePurgeRepository{deleteErr: errors.New("delete failed")}
	WithSnapshotRepository(repository)(d)
	store, _ := newMockStore(t)
	WithStore(t, store)(d)
	sess := newSnapshotTestSession(t, "work", false, "/work")
	record := sess.persistRecordLocked(1)
	require.NoError(t, d.catalogue.Create(record))
	pty, ok := sess.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
	require.True(t, ok)
	pty.EXPECT().Close().Return(nil).Once()
	d.sessions = map[domain.SessionID]attachmentSession{sess.id: sess}

	require.Error(t, d.killSession(sess, ports.ReasonSessionKilled, true))
	require.Equal(t, []string{"delete incarnation"}, repository.calls)
	_, exists, err := d.catalogue.Record("work")
	require.NoError(t, err)
	require.False(t, exists, "catalogue removal commits before best-effort directory deletion")
	require.Equal(t, []bool{true}, repository.deadlineScoped)
}

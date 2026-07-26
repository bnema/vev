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
	tombstoned     map[string]bool
	calls          []string
	deadlineScoped []bool
}

func (r *retryablePurgeRepository) Publish(context.Context, ports.SnapshotPublication) error {
	return nil
}

func (r *retryablePurgeRepository) WriteDeletionTombstone(ctx context.Context, tombstone domain.DeletionTombstone) error {
	_, hasDeadline := ctx.Deadline()
	r.deadlineScoped = append(r.deadlineScoped, hasDeadline)
	if r.tombstoned == nil {
		r.tombstoned = make(map[string]bool)
	}
	r.calls = append(r.calls, "tombstone")
	r.tombstoned[tombstone.Name] = true
	return nil
}

func (r *retryablePurgeRepository) DeleteIncarnation(ctx context.Context, _ domain.IncarnationID) error {
	_, hasDeadline := ctx.Deadline()
	r.deadlineScoped = append(r.deadlineScoped, hasDeadline)
	r.calls = append(r.calls, "delete incarnation")
	return r.deleteErr
}

func (r *retryablePurgeRepository) DeleteDeletionTombstone(ctx context.Context, _ domain.IncarnationID) error {
	_, hasDeadline := ctx.Deadline()
	r.deadlineScoped = append(r.deadlineScoped, hasDeadline)
	r.calls = append(r.calls, "clear tombstone")
	clear(r.tombstoned)
	return nil
}

func TestLivePurgeRetainsTombstoneAcrossPartialDeletion(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	repository := &retryablePurgeRepository{deleteErr: errors.New("delete failed")}
	WithSnapshotRepository(repository)(d)
	store, _ := newMockStore(t)
	WithStore(t, store)(d)
	sess := newSnapshotTestSession(t, "work", false, "/work")
	record := sess.persistRecordLocked(1)
	record.RecoveryState = domain.RecoveryFresh
	require.NoError(t, d.catalogue.Create(record))
	pty, ok := sess.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
	require.True(t, ok)
	pty.EXPECT().Close().Return(nil).Once()
	d.sessions = map[domain.SessionID]*session{sess.id: sess}

	require.Error(t, d.killSession(sess, ports.ReasonSessionKilled, true))
	require.True(t, repository.tombstoned["work"])
	require.Equal(t, []string{"tombstone", "delete incarnation"}, repository.calls)

	repository.deleteErr = nil
	require.NoError(t, d.retryStoppedPurge("work"))
	require.False(t, repository.tombstoned["work"])
	require.Equal(t, []string{"tombstone", "delete incarnation", "tombstone", "delete incarnation", "clear tombstone"}, repository.calls)
	for _, scoped := range repository.deadlineScoped {
		require.True(t, scoped)
	}
}

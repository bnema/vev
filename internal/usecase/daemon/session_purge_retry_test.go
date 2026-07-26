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
	incrementalErr error
	legacyErr      error
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
func (r *retryablePurgeRepository) QuarantineDeletionSources(ctx context.Context, tombstone domain.DeletionTombstone, _ bool) error {
	_, hasDeadline := ctx.Deadline()
	r.deadlineScoped = append(r.deadlineScoped, hasDeadline)
	r.calls = append(r.calls, "incremental")
	if r.incrementalErr != nil {
		return r.incrementalErr
	}
	r.calls = append(r.calls, "legacy")
	return r.legacyErr
}
func (r *retryablePurgeRepository) DeleteDeletionTombstone(ctx context.Context, _ domain.IncarnationID) error {
	_, hasDeadline := ctx.Deadline()
	r.deadlineScoped = append(r.deadlineScoped, hasDeadline)
	r.calls = append(r.calls, "clear tombstone")
	for name := range r.tombstoned {
		delete(r.tombstoned, name)
	}
	return nil
}
func (*retryablePurgeRepository) LoadLegacy(context.Context) ([]ports.LegacySnapshot, error) {
	return nil, nil
}
func (*retryablePurgeRepository) DeleteVerifiedLegacy(context.Context, ports.LegacySnapshot) error {
	return nil
}
func (*retryablePurgeRepository) DeleteLegacy(context.Context, string) error { return nil }

func TestLivePurgeRetainsTombstoneAcrossPartialSourceDeletion(t *testing.T) {
	for _, tt := range []struct {
		name        string
		incremental error
		legacy      error
	}{
		{name: "incremental failure", incremental: errors.New("incremental failed")},
		{name: "legacy failure", legacy: errors.New("legacy failed")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
			repository := &retryablePurgeRepository{incrementalErr: tt.incremental, legacyErr: tt.legacy}
			WithSnapshotRepository(repository)(d)
			store, _ := newMockStore(t)
			WithStore(t, store)(d)
			sess := newSnapshotTestSession(t, "work", false, "/work")
			record := sess.persistRecordLocked(1)
			record.RecoveryState = domain.RecoveryFresh
			require.NoError(t, d.catalogue.Create(record))
			pty, ok := sess.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
			require.True(t, ok, "snapshot test pane must use MockPTY")
			pty.EXPECT().Close().Return(nil).Once()
			d.sessions = map[domain.SessionID]*session{sess.id: sess}

			require.Error(t, d.killSession(sess, ports.ReasonSessionKilled, true))
			require.True(t, repository.tombstoned["work"], "partial purge must fence startup restore/import")
			wantCalls := []string{"tombstone", "incremental"}
			if tt.incremental == nil {
				wantCalls = append(wantCalls, "legacy")
			}
			require.Equal(t, wantCalls, repository.calls)
			d.mu.Lock()
			stopped, ok := d.stopped["work"]
			d.mu.Unlock()
			require.True(t, ok)
			require.True(t, stopped.purging)

			repository.incrementalErr = nil
			repository.legacyErr = nil
			require.NoError(t, d.retryStoppedPurge("work"))
			require.False(t, repository.tombstoned["work"])
			wantCalls = append(wantCalls, "tombstone", "incremental", "legacy", "clear tombstone")
			require.Equal(t, wantCalls, repository.calls)
			require.NotEmpty(t, repository.deadlineScoped)
			for _, scoped := range repository.deadlineScoped {
				require.True(t, scoped, "every purge repository call must receive a deadline-scoped context")
			}
			d.mu.Lock()
			_, ok = d.stopped["work"]
			d.mu.Unlock()
			require.False(t, ok, "metadata is removed only after both source deletes and tombstone clear")
		})
	}
}

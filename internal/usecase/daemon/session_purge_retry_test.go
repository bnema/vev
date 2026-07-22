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
	incrementalErr error
	legacyErr      error
	tombstoned     map[string]bool
	calls          []string
}

func (r *retryablePurgeRepository) Publish(context.Context, ports.SnapshotPublication) error {
	return nil
}
func (r *retryablePurgeRepository) List(context.Context) ([]string, error) { return nil, nil }
func (r *retryablePurgeRepository) Load(context.Context, string) (ports.SnapshotGeneration, error) {
	return ports.SnapshotGeneration{}, errors.New("unused")
}
func (r *retryablePurgeRepository) Maintain(context.Context) error { return nil }
func (r *retryablePurgeRepository) Delete(_ context.Context, name string) error {
	r.calls = append(r.calls, "incremental")
	return r.incrementalErr
}
func (r *retryablePurgeRepository) LoadLegacy(context.Context) ([]ports.LegacySnapshot, error) {
	return nil, nil
}
func (r *retryablePurgeRepository) DeleteVerifiedLegacy(ctx context.Context, blob ports.LegacySnapshot) error {
	return r.DeleteLegacy(ctx, blob.Name)
}
func (r *retryablePurgeRepository) DeleteLegacy(_ context.Context, name string) error {
	r.calls = append(r.calls, "legacy")
	return r.legacyErr
}
func (r *retryablePurgeRepository) Tombstone(_ context.Context, name string) error {
	if r.tombstoned == nil {
		r.tombstoned = make(map[string]bool)
	}
	r.calls = append(r.calls, "tombstone")
	r.tombstoned[name] = true
	return nil
}
func (r *retryablePurgeRepository) DeleteTombstone(_ context.Context, name string) error {
	r.calls = append(r.calls, "clear tombstone")
	delete(r.tombstoned, name)
	return nil
}

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
			WithSnapshotRepository(repository, repository)(d)
			sess := newSnapshotTestSession(t, "work", false, "/work")
			pty, ok := sess.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
			require.True(t, ok, "snapshot test pane must use MockPTY")
			pty.EXPECT().Close().Return(nil).Once()
			d.sessions = map[domain.SessionID]*session{sess.id: sess}

			require.Error(t, d.killSession(sess, ports.ReasonSessionKilled, true))
			require.True(t, repository.tombstoned["work"], "partial purge must fence startup restore/import")
			require.Equal(t, []string{"tombstone", "incremental", "legacy"}, repository.calls)
			d.mu.Lock()
			stopped, ok := d.stopped["work"]
			d.mu.Unlock()
			require.True(t, ok)
			require.True(t, stopped.purging)

			repository.incrementalErr = nil
			repository.legacyErr = nil
			require.NoError(t, d.retryStoppedPurge("work"))
			require.False(t, repository.tombstoned["work"])
			require.Equal(t, []string{"tombstone", "incremental", "legacy", "tombstone", "incremental", "legacy", "clear tombstone"}, repository.calls)
			d.mu.Lock()
			_, ok = d.stopped["work"]
			d.mu.Unlock()
			require.False(t, ok, "metadata is removed only after both source deletes and tombstone clear")
		})
	}
}

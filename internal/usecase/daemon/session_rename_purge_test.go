package daemon

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

func TestRenamePreservesIncarnationSnapshotSources(t *testing.T) {
	legacyDeleteErr := errors.New("legacy delete failed")
	for _, tt := range []struct {
		name      string
		legacyErr error
	}{
		{name: "snapshot sources available"},
		{name: "unrelated legacy source delete would fail", legacyErr: legacyDeleteErr},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
			repository := &retryablePurgeRepository{legacyErr: tt.legacyErr}
			WithSnapshotRepository(repository)(d)
			store, state := newMockStore(t)
			WithStore(t, store)(d)
			sess := newSnapshotTestSession(t, "old", false, "/work")
			d.sessions = map[domain.SessionID]*session{sess.id: sess}
			require.NoError(t, testPersister(t, d).Save(sess.persistRecordLocked(1)))

			require.NoError(t, d.renameSession(sess, "new"))
			require.Empty(t, repository.calls, "rename must not invoke source deletion, including a failing legacy source")
			require.False(t, state.has("old"))
			require.True(t, state.has("new"))
			require.Equal(t, sess.incarnation, state.record(t, "new").IncarnationID)
		})
	}
}

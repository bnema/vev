package daemon

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	recoveryusecase "github.com/bnema/vev/internal/usecase/recovery"
)

// newSyncCountingCatalogue is a recovery catalogue mock whose Create commits
// like an identity write: it rejects an existing name and syncs durably. The
// returned state counts every Sync.
func newSyncCountingCatalogue(t testing.TB, records []domain.CatalogueRecord) (*portsmocks.MockCatalogue, *recoveryCatalogueState) {
	t.Helper()
	return newRecoveryCatalogueMock(t, records, func(m *portsmocks.MockCatalogue, state *recoveryCatalogueState) {
		m.EXPECT().Create(mock.Anything).RunAndReturn(func(record domain.CatalogueRecord) error {
			state.mu.Lock()
			if _, exists := state.records[record.Name]; exists {
				state.mu.Unlock()
				return errSessionNameInUse
			}
			state.records[record.Name] = record
			state.mu.Unlock()
			return state.Sync()
		}).Maybe()
	})
}

func TestMetadataWritesAreDeferred(t *testing.T) {
	t.Parallel()
	sess := newSnapshotTestSession(t, "work", false, "/work")
	sess.mu.Lock()
	record := sess.persistRecordLocked(sess.createdAt)
	sess.mu.Unlock()
	cat, catState := newSyncCountingCatalogue(t, []domain.CatalogueRecord{record})
	d := newTestDaemon(t, nil, newStubClock(t))
	d.catalogue = cat
	d.persistEnabled = true
	d.sessions[sess.id] = sess

	syncsBefore := catState.syncCount()
	require.NoError(t, d.renameTab(sess, sess.tabs[0], "build"))
	d.touchMRU(sess)
	require.Equal(t, syncsBefore, catState.syncCount(), "metadata updates must not force a durable write")

	require.NoError(t, d.flushCatalogue())
	require.Greater(t, catState.syncCount(), syncsBefore)

	got, ok, err := cat.Record("work")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []string{"build"}, got.TabNames)
}

func TestIdentityWritesAreSynchronous(t *testing.T) {
	t.Parallel()
	sess := newSnapshotTestSession(t, "0", true, "/work")
	cat, catState := newSyncCountingCatalogue(t, nil)
	d := newTestDaemon(t, nil, newStubClock(t))
	d.catalogue = cat
	d.persistEnabled = true
	d.recovery = recoveryusecase.NewCoordinator(cat, nil, bytes.NewReader(bytes.Repeat([]byte{1}, 16)))
	d.sessions[sess.id] = sess

	syncsBefore := catState.syncCount()
	require.NoError(t, d.renameSession(sess, "work"))
	require.Greater(t, catState.syncCount(), syncsBefore, "creating a named session must be durable before it is exposed")

	d.mu.Lock()
	published := d.findByNameLocked("work")
	d.mu.Unlock()
	require.Same(t, sess, published)
	sess.mu.Lock()
	require.False(t, sess.ephemeral)
	sess.mu.Unlock()
}

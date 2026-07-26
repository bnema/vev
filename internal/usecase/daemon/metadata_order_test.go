package daemon

import (
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	recoveryusecase "github.com/bnema/vev/internal/usecase/recovery"
)

type syncCountingCatalogue struct {
	*durableRecoveryCatalogue

	muSync sync.Mutex
	syncs  int
}

func (c *syncCountingCatalogue) Sync() error {
	c.muSync.Lock()
	defer c.muSync.Unlock()
	c.syncs++
	return nil
}

func (c *syncCountingCatalogue) syncCount() int {
	c.muSync.Lock()
	defer c.muSync.Unlock()
	return c.syncs
}

func (c *syncCountingCatalogue) Create(record domain.CatalogueRecord) error {
	c.mu.Lock()
	if _, exists := c.records[record.Name]; exists {
		c.mu.Unlock()
		return errSessionNameInUse
	}
	c.records[record.Name] = record
	c.mu.Unlock()
	return c.Sync()
}

func TestMetadataWritesAreDeferred(t *testing.T) {
	t.Parallel()
	sess := newSnapshotTestSession(t, "work", false, "/work")
	sess.mu.Lock()
	record := sess.persistRecordLocked(sess.createdAt)
	sess.mu.Unlock()
	cat := &syncCountingCatalogue{durableRecoveryCatalogue: newDurableRecoveryCatalogue([]domain.CatalogueRecord{record})}
	d := newTestDaemon(t, nil, stubClock{})
	d.catalogue = cat
	d.persistEnabled = true
	d.sessions[sess.id] = sess

	syncsBefore := cat.syncCount()
	require.NoError(t, d.renameTab(sess, sess.tabs[0], "build"))
	d.touchMRU(sess)
	require.Equal(t, syncsBefore, cat.syncCount(), "metadata updates must not force a durable write")

	require.NoError(t, d.flushCatalogue())
	require.Greater(t, cat.syncCount(), syncsBefore)

	got, ok, err := cat.Record("work")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []string{"build"}, got.TabNames)
}

func TestIdentityWritesAreSynchronous(t *testing.T) {
	t.Parallel()
	sess := newSnapshotTestSession(t, "0", true, "/work")
	cat := &syncCountingCatalogue{durableRecoveryCatalogue: newDurableRecoveryCatalogue(nil)}
	d := newTestDaemon(t, nil, stubClock{})
	d.catalogue = cat
	d.persistEnabled = true
	d.recovery = recoveryusecase.NewCoordinator(cat, nil, bytes.NewReader(bytes.Repeat([]byte{1}, 16)))
	d.sessions[sess.id] = sess

	syncsBefore := cat.syncCount()
	require.NoError(t, d.renameSession(sess, "work"))
	require.Greater(t, cat.syncCount(), syncsBefore, "creating a named session must be durable before it is exposed")

	d.mu.Lock()
	published := d.findByNameLocked("work")
	d.mu.Unlock()
	require.Same(t, sess, published)
	sess.mu.Lock()
	require.False(t, sess.ephemeral)
	sess.mu.Unlock()
}

var _ ports.Catalogue = (*syncCountingCatalogue)(nil)

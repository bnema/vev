package daemon

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

type blockingMetadataCatalogue struct {
	ports.Catalogue

	mu           sync.Mutex
	calls        int
	firstStarted chan struct{}
	releaseFirst chan struct{}
	firstErr     error
	secondErr    error
}

func (c *blockingMetadataCatalogue) UpdateMetadata(update domain.CatalogueMetadataUpdate) error {
	c.mu.Lock()
	c.calls++
	call := c.calls
	c.mu.Unlock()
	if call == 1 {
		close(c.firstStarted)
		<-c.releaseFirst
		if c.firstErr != nil {
			return c.firstErr
		}
	}
	if call == 2 && c.secondErr != nil {
		return c.secondErr
	}
	return c.Catalogue.UpdateMetadata(update)
}

func newBlockingCatalogue(sess *session, firstErr error) (*durableRecoveryCatalogue, *blockingMetadataCatalogue) {
	record := sess.persistRecordLocked(sess.createdAt)
	catalogue := newDurableRecoveryCatalogue([]domain.CatalogueRecord{record})
	return catalogue, &blockingMetadataCatalogue{
		Catalogue:    catalogue,
		firstStarted: make(chan struct{}),
		releaseFirst: make(chan struct{}),
		firstErr:     firstErr,
	}
}

func TestMetadataWritesPreserveLaterMutationWhenFirstWriteIsBlocked(t *testing.T) {
	firstFailure := errors.New("first metadata write failed")
	for _, tt := range []struct {
		name     string
		firstErr error
	}{
		{name: "delayed success"},
		{name: "delayed failure does not roll back newer state", firstErr: firstFailure},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sess := newSnapshotTestSession(t, "work", false, "/work")
			catalogue, blocking := newBlockingCatalogue(sess, tt.firstErr)
			d := newTestDaemon(t, nil, stubClock{})
			d.catalogue = blocking
			d.sessions[sess.id] = sess

			firstDone := make(chan error, 1)
			go func() { firstDone <- d.renameTab(sess, sess.tabs[0], "first") }()
			select {
			case <-blocking.firstStarted:
			case <-time.After(testWaitTimeout):
				t.Fatal("first metadata write did not block")
			}

			secondDone := make(chan error, 1)
			go func() { secondDone <- d.renameTab(sess, sess.tabs[0], "second") }()
			require.Eventually(t, func() bool {
				sess.mu.Lock()
				defer sess.mu.Unlock()
				return sess.tabs[0].name == "second"
			}, testWaitTimeout, time.Millisecond, "later mutation did not proceed while storage was blocked")

			close(blocking.releaseFirst)
			firstResult := <-firstDone
			if tt.firstErr == nil {
				require.NoError(t, firstResult)
			} else {
				require.ErrorIs(t, firstResult, tt.firstErr, "an in-memory revision cannot subsume a failed durable write")
			}
			require.NoError(t, <-secondDone)

			persisted, ok, err := catalogue.Record("work")
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, []string{"second"}, persisted.TabNames)
			require.Equal(t, "second", sess.tabs[0].name)
		})
	}
}

func TestTwoConsecutiveMetadataFailuresAreBothReported(t *testing.T) {
	firstFailure := errors.New("first metadata write failed")
	secondFailure := errors.New("second metadata write failed")
	sess := newSnapshotTestSession(t, "work", false, "/work")
	catalogue, blocking := newBlockingCatalogue(sess, firstFailure)
	blocking.secondErr = secondFailure
	close(blocking.releaseFirst)
	d := newTestDaemon(t, nil, stubClock{})
	d.catalogue = blocking
	d.sessions[sess.id] = sess

	require.ErrorIs(t, d.renameTab(sess, sess.tabs[0], "first"), firstFailure)
	require.ErrorIs(t, d.renameTab(sess, sess.tabs[0], "second"), secondFailure)

	persisted, ok, err := catalogue.Record("work")
	require.NoError(t, err)
	require.True(t, ok)
	require.Empty(t, persisted.TabNames)
	sess.mu.Lock()
	require.Empty(t, sess.tabs[0].name)
	sess.mu.Unlock()
}

func TestConsecutiveMetadataFailuresRestoreDurableState(t *testing.T) {
	firstFailure := errors.New("first metadata write failed")
	secondFailure := errors.New("second metadata write failed")
	sess := newSnapshotTestSession(t, "work", false, "/work")
	catalogue, blocking := newBlockingCatalogue(sess, firstFailure)
	blocking.secondErr = secondFailure
	d := newTestDaemon(t, nil, stubClock{})
	d.catalogue = blocking
	d.sessions[sess.id] = sess

	firstDone := make(chan error, 1)
	go func() { firstDone <- d.renameTab(sess, sess.tabs[0], "first") }()
	select {
	case <-blocking.firstStarted:
	case <-time.After(testWaitTimeout):
		t.Fatal("first metadata write did not block")
	}
	secondDone := make(chan error, 1)
	go func() { secondDone <- d.renameTab(sess, sess.tabs[0], "second") }()
	require.Eventually(t, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.tabs[0].name == "second"
	}, testWaitTimeout, time.Millisecond)

	close(blocking.releaseFirst)
	require.ErrorIs(t, <-firstDone, firstFailure)
	require.ErrorIs(t, <-secondDone, secondFailure)

	persisted, ok, err := catalogue.Record("work")
	require.NoError(t, err)
	require.True(t, ok)
	require.Empty(t, persisted.TabNames)
	sess.mu.Lock()
	require.Empty(t, sess.tabs[0].name)
	sess.mu.Unlock()
}

func TestNewerMetadataFailureDoesNotSkipOlderSnapshot(t *testing.T) {
	failure := errors.New("newer metadata write failed")
	sess := newSnapshotTestSession(t, "work", false, "/work")
	catalogue := newDurableRecoveryCatalogue([]domain.CatalogueRecord{sess.persistRecordLocked(sess.createdAt)})
	blocking := &blockingMetadataCatalogue{
		Catalogue:    catalogue,
		firstStarted: make(chan struct{}),
		releaseFirst: make(chan struct{}),
		firstErr:     failure,
	}
	close(blocking.releaseFirst)
	d := newTestDaemon(t, nil, stubClock{})
	d.catalogue = blocking
	d.sessions[sess.id] = sess
	tb := sess.tabs[0]

	sess.mu.Lock()
	tb.name = "first"
	firstRecord, firstVersion := sess.nextPersistRecordLocked(sess.createdAt + 1)
	tb.name = "second"
	secondRecord, secondVersion := sess.nextPersistRecordLocked(sess.createdAt + 2)
	sess.mu.Unlock()

	secondRollback := func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if tb.name != "second" {
			return false
		}
		tb.name = "first"
		return true
	}
	firstRollback := func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if tb.name != "first" {
			return false
		}
		tb.name = ""
		return true
	}

	_, err := d.persistSessionMetadata(sess, secondVersion, secondRecord.MetadataUpdate(), secondRollback)
	require.ErrorIs(t, err, failure)
	_, err = d.persistSessionMetadata(sess, firstVersion, firstRecord.MetadataUpdate(), firstRollback)
	require.NoError(t, err)

	persisted, ok, err := catalogue.Record("work")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []string{"first"}, persisted.TabNames)
	sess.mu.Lock()
	require.Equal(t, "first", tb.name)
	sess.mu.Unlock()
}

func TestLatestMetadataFailureRollsBackRename(t *testing.T) {
	failure := errors.New("metadata write failed")
	sess := newSnapshotTestSession(t, "work", false, "/work")
	catalogue, blocking := newBlockingCatalogue(sess, failure)
	d := newTestDaemon(t, nil, stubClock{})
	d.catalogue = blocking
	d.sessions[sess.id] = sess

	done := make(chan error, 1)
	go func() { done <- d.renameTab(sess, sess.tabs[0], "failed") }()
	select {
	case <-blocking.firstStarted:
	case <-time.After(testWaitTimeout):
		t.Fatal("metadata write did not block")
	}
	close(blocking.releaseFirst)
	require.ErrorIs(t, <-done, failure)

	persisted, ok, err := catalogue.Record("work")
	require.NoError(t, err)
	require.True(t, ok)
	require.Empty(t, persisted.TabNames)
	sess.mu.Lock()
	require.Empty(t, sess.tabs[0].name)
	sess.mu.Unlock()
}

func TestBlockedCwdWriteCannotOverwriteLaterTabMetadata(t *testing.T) {
	sess := newSnapshotTestSession(t, "work", false, "/work")
	catalogue, blocking := newBlockingCatalogue(sess, nil)
	d := newTestDaemon(t, nil, stubClock{})
	d.catalogue = blocking
	d.sessions[sess.id] = sess
	d.procCwd = func(int) (string, error) { return "/later-cwd", nil }

	cwdDone := make(chan struct{})
	go func() {
		d.refreshSessionCwd(sess)
		close(cwdDone)
	}()
	select {
	case <-blocking.firstStarted:
	case <-time.After(testWaitTimeout):
		t.Fatal("cwd metadata write did not block")
	}

	renameDone := make(chan error, 1)
	go func() { renameDone <- d.renameTab(sess, sess.tabs[0], "later-tab") }()
	require.Eventually(t, func() bool {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return sess.tabs[0].name == "later-tab"
	}, testWaitTimeout, time.Millisecond)

	close(blocking.releaseFirst)
	select {
	case <-cwdDone:
	case <-time.After(testWaitTimeout):
		t.Fatal("cwd refresh deadlocked")
	}
	require.NoError(t, <-renameDone)

	persisted, ok, err := catalogue.Record("work")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "/later-cwd", persisted.Cwd)
	require.Equal(t, []string{"later-tab"}, persisted.TabNames)
}

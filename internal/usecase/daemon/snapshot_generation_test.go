package daemon

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

func TestSnapshotMultipleMutationsPublishNextRepositoryGeneration(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	repository := portsmocks.NewMockSnapshotRepository(t)
	WithSnapshotRepository(repository)(d)
	startSnapshotEncodeWorker(t, d)

	publications := make(chan ports.SnapshotPublication, 1)
	repository.EXPECT().Publish(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, publication ports.SnapshotPublication) error {
		publications <- publication
		return nil
	}).Once()

	sess := newSnapshotTestSession(t, "work", false, "/work")
	markSnapshotDirty(sess)
	markSnapshotDirty(sess)
	markSnapshotDirty(sess)
	require.True(t, d.scheduleSnapshot(sess))

	select {
	case publication := <-publications:
		require.Equal(t, uint64(1), publication.Generation)
	case <-time.After(testWaitTimeout):
		t.Fatal("snapshot was not published")
	}
	awaitSnapshotClean(t, sess)
}

func TestSnapshotRetryKeepsRepositoryGeneration(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	repository := portsmocks.NewMockSnapshotRepository(t)
	WithSnapshotRepository(repository)(d)
	startSnapshotEncodeWorker(t, d)

	publications := make(chan ports.SnapshotPublication, 2)
	var attempts atomic.Int32
	repository.EXPECT().Publish(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, publication ports.SnapshotPublication) error {
		publications <- publication
		if attempts.Add(1) == 1 {
			return errors.New("temporary repository failure")
		}
		return nil
	}).Twice()

	sess := newSnapshotTestSession(t, "work", false, "/work")
	markSnapshotDirty(sess)
	require.True(t, d.scheduleSnapshot(sess))
	select {
	case publication := <-publications:
		require.Equal(t, uint64(1), publication.Generation)
	case <-time.After(testWaitTimeout):
		t.Fatal("first snapshot was not published")
	}
	awaitSnapshotIdle(t, sess)
	require.True(t, sess.snapDirty.Load())

	require.True(t, d.scheduleFinalSnapshot(sess))
	select {
	case publication := <-publications:
		require.Equal(t, uint64(1), publication.Generation)
	case <-time.After(testWaitTimeout):
		t.Fatal("retry snapshot was not published")
	}
	awaitSnapshotClean(t, sess)
}

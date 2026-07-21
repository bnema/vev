package daemon

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

func TestSnapshotCoordinatorQuarantineCancelsPublicationBeforeDelete(t *testing.T) {
	sess := newSnapshotTestSession(t, "work", false, "/work")
	ctx, cancel := snapshotCoordinatorContext(sess)
	done := quarantineSnapshotCoordinator(sess)

	select {
	case <-ctx.Done():
	case <-time.After(testWaitTimeout):
		t.Fatal("quarantine did not cancel publication")
	}
	select {
	case <-done:
	case <-time.After(testWaitTimeout):
		t.Fatal("quarantine did not join idle coordinator")
	}
	cancel()
}

func TestSnapshotCoordinatorQuarantineJoinsInFlightPublication(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	repository := portsmocks.NewMockSnapshotRepository(t)
	WithSnapshotRepository(repository, nil)(d)
	startSnapshotEncodeWorker(t, d)

	started := make(chan struct{})
	release := make(chan struct{})
	repository.EXPECT().Publish(mock.Anything, mock.Anything).RunAndReturn(func(context.Context, ports.SnapshotPublication) error {
		close(started)
		<-release // deliberately ignore cancellation
		return nil
	}).Once()
	sess := newSnapshotTestSession(t, "work", false, "/work")
	markSnapshotDirty(sess)
	require.True(t, d.scheduleSnapshot(sess))
	select {
	case <-started:
	case <-time.After(testWaitTimeout):
		t.Fatal("publication did not start")
	}

	done := quarantineSnapshotCoordinator(sess)
	select {
	case <-done:
		t.Fatal("quarantine joined before publication returned")
	default:
	}
	close(release)
	select {
	case <-done:
	case <-time.After(testWaitTimeout):
		t.Fatal("quarantine did not join publication")
	}
}

func TestRenameKeepsSnapshotCoordinatorQuarantinedUntilNewIdentityCommits(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	store, state := newMockStore(t)
	WithStore(store)(d)
	repository := portsmocks.NewMockSnapshotRepository(t)
	WithSnapshotRepository(repository, nil)(d)
	startSnapshotEncodeWorker(t, d)

	deleteEntered := make(chan struct{})
	allowDelete := make(chan struct{})
	published := make(chan ports.SnapshotPublication, 2)
	repository.EXPECT().Delete(mock.Anything, "work").RunAndReturn(func(context.Context, string) error {
		close(deleteEntered)
		<-allowDelete
		return nil
	}).Once()
	repository.EXPECT().Publish(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, publication ports.SnapshotPublication) error {
		published <- publication
		return nil
	}).Maybe()

	pty, release := newBlockingPTY(t)
	defer release()
	d.ptys = newFactory(t, pty)
	sess, err := d.createSessionLocked("work", false, "/work", domain.Size{Cols: 80, Rows: 24}, terminalEnv{}, d.baseEnv)
	require.NoError(t, err)
	markSnapshotDirty(sess)

	renameDone := make(chan error, 1)
	go func() { renameDone <- d.renameSession(sess, "renamed") }()
	select {
	case <-deleteEntered:
	case <-time.After(testWaitTimeout):
		t.Fatal("rename did not begin deleting the old snapshot identity")
	}

	// Hold the daemon identity commit after Delete has returned. Scheduler work
	// during this window must not capture or publish the old name.
	d.mu.Lock()
	close(allowDelete)
	require.Eventually(t, func() bool { return !state.has("work") }, testWaitTimeout, time.Millisecond)
	sess.snapshotMu.Lock()
	quarantined := sess.snapshotQuarantined
	sess.snapshotMu.Unlock()
	require.True(t, quarantined, "old Delete returned before the coordinator resumed")

	schedulerCtx, cancelScheduler := context.WithCancel(context.Background())
	defer cancelScheduler()
	go d.snapshotRepositorySaver(schedulerCtx)
	select {
	case d.snapshotWake <- struct{}{}:
	default:
	}
	require.True(t, d.scheduleSnapshot(sess))
	select {
	case publication := <-published:
		t.Fatalf("published %q after old Delete before identity commit", publication.Name)
	case <-time.After(50 * time.Millisecond):
	}
	d.mu.Unlock()

	require.NoError(t, <-renameDone)
	select {
	case publication := <-published:
		require.Equal(t, "renamed", publication.Name)
		require.Equal(t, uint64(1), publication.Generation)
	case <-time.After(testWaitTimeout):
		t.Fatal("new identity was not published")
	}
}

func TestRenameRollbackAfterOldDeleteLeavesOldCoordinatorStopped(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	store, state := newMockStore(t)
	WithStore(store)(d)
	repository := portsmocks.NewMockSnapshotRepository(t)
	WithSnapshotRepository(repository, nil)(d)
	startSnapshotEncodeWorker(t, d)

	deleteOldRecordErr := errors.New("old record delete failed")
	state.mu.Lock()
	state.deleteErr = func(key string) error {
		if key == "work" {
			return deleteOldRecordErr
		}
		return nil
	}
	state.mu.Unlock()
	repository.EXPECT().Delete(mock.Anything, "work").Return(nil).Once()
	repository.EXPECT().Publish(mock.Anything, mock.Anything).Run(func(context.Context, ports.SnapshotPublication) {
		t.Fatal("rollback resurrected the deleted old snapshot identity")
	}).Maybe()

	pty, release := newBlockingPTY(t)
	defer release()
	d.ptys = newFactory(t, pty)
	sess, err := d.createSessionLocked("work", false, "/work", domain.Size{Cols: 80, Rows: 24}, terminalEnv{}, d.baseEnv)
	require.NoError(t, err)
	markSnapshotDirty(sess)

	require.ErrorIs(t, d.renameSession(sess, "renamed"), deleteOldRecordErr)
	sess.mu.Lock()
	require.Equal(t, "work", sess.name)
	require.False(t, sess.ephemeral)
	require.False(t, sess.renameInProgress)
	sess.mu.Unlock()
	require.True(t, state.has("work"))
	require.False(t, state.has("renamed"))
	sess.snapshotMu.Lock()
	quarantined := sess.snapshotQuarantined
	sess.snapshotMu.Unlock()
	require.True(t, quarantined)

	schedulerCtx, cancelScheduler := context.WithCancel(context.Background())
	defer cancelScheduler()
	go d.snapshotRepositorySaver(schedulerCtx)
	select {
	case d.snapshotWake <- struct{}{}:
	default:
	}
	select {
	case <-time.After(50 * time.Millisecond):
	}
}

func TestRoutineSnapshotEligibilityStartsAtCompletionAndForcedDoesNotMoveIt(t *testing.T) {
	clock := &snapshotSchedulerClock{now: time.Unix(100, 0)}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)
	repository := portsmocks.NewMockSnapshotRepository(t)
	WithSnapshotRepository(repository, nil)(d)
	startSnapshotEncodeWorker(t, d)

	var publishes atomic.Int32
	repository.EXPECT().Publish(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, _ ports.SnapshotPublication) error {
		publishes.Add(1)
		return nil
	}).Times(2)

	sess := newSnapshotTestSession(t, "work", false, "/work")
	markSnapshotDirty(sess)
	require.True(t, d.scheduleSnapshot(sess))
	awaitSnapshotClean(t, sess)

	sess.snapshotMu.Lock()
	eligibleAt := sess.snapshotNextEligibleAt
	sess.snapshotMu.Unlock()
	require.Equal(t, time.Unix(100, 0).Add(snapshotInterval), eligibleAt)

	markSnapshotDirty(sess)
	require.True(t, d.scheduleSnapshot(sess))
	require.Eventually(t, func() bool { return publishes.Load() == 1 }, time.Second, time.Millisecond)

	require.True(t, d.scheduleFinalSnapshot(sess))
	awaitSnapshotClean(t, sess)
	sess.snapshotMu.Lock()
	require.Equal(t, eligibleAt, sess.snapshotNextEligibleAt)
	sess.snapshotMu.Unlock()
}

type snapshotSchedulerClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *snapshotSchedulerClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (*snapshotSchedulerClock) NewTimer(time.Duration) ports.Timer { return stubTimer{} }

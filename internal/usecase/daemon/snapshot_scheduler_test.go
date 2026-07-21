package daemon

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

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

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

func TestForcedSnapshotSchedulesSuccessorAfterRoutineInFlight(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	repository := portsmocks.NewMockSnapshotRepository(t)
	WithSnapshotRepository(repository, nil)(d)
	startSnapshotEncodeWorker(t, d)

	started := make(chan struct{})
	release := make(chan struct{})
	publications := make(chan ports.SnapshotPublication, 2)
	repository.EXPECT().Publish(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, publication ports.SnapshotPublication) error {
		publications <- publication
		if publication.Generation == 1 {
			close(started)
			<-release
		}
		return nil
	}).Twice()

	sess := newSnapshotTestSession(t, "work", false, "/work")
	markSnapshotDirty(sess)
	require.True(t, d.scheduleSnapshot(sess))
	select {
	case <-started:
	case <-time.After(testWaitTimeout):
		t.Fatal("routine publication did not start")
	}

	// The terminal state changed after the routine capture, so its publication
	// cannot satisfy the forced checkpoint.
	sess.tabs[0].panes["pane-1"].screen.Write([]byte(" terminal"))
	markSnapshotDirty(sess)
	require.True(t, d.scheduleFinalSnapshot(sess))
	close(release)

	awaitSnapshotClean(t, sess)
	require.Equal(t, uint64(1), (<-publications).Generation)
	require.Equal(t, uint64(2), (<-publications).Generation)
}

func TestForcedSnapshotSchedulesSuccessorForRoutineCaptureQueuedBehindWorker(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	repository := portsmocks.NewMockSnapshotRepository(t)
	WithSnapshotRepository(repository, nil)(d)
	startSnapshotEncodeWorker(t, d)

	blockerStarted := make(chan struct{})
	releaseBlocker := make(chan struct{})
	workPublications := make(chan ports.SnapshotPublication, 2)
	repository.EXPECT().Publish(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, publication ports.SnapshotPublication) error {
		if publication.Name == "blocker" {
			close(blockerStarted)
			<-releaseBlocker
			return nil
		}
		workPublications <- publication
		return nil
	}).Times(3)

	blocker := newSnapshotTestSession(t, "blocker", false, "/work")
	markSnapshotDirty(blocker)
	require.True(t, d.scheduleSnapshot(blocker))
	select {
	case <-blockerStarted:
	case <-time.After(testWaitTimeout):
		t.Fatal("blocking publication did not start")
	}

	sess := newSnapshotTestSession(t, "work", false, "/work")
	markSnapshotDirty(sess)
	require.True(t, d.scheduleSnapshot(sess), "routine capture should occupy the bounded queue")
	sess.tabs[0].panes["pane-1"].screen.Write([]byte(" terminal"))
	markSnapshotDirty(sess)
	require.True(t, d.scheduleFinalSnapshot(sess))
	close(releaseBlocker)

	awaitSnapshotClean(t, sess)
	require.Equal(t, uint64(1), (<-workPublications).Generation)
	require.Equal(t, uint64(2), (<-workPublications).Generation)
}

func TestMultipleForcedSnapshotsCoalesceToOneSuccessor(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	repository := portsmocks.NewMockSnapshotRepository(t)
	WithSnapshotRepository(repository, nil)(d)
	startSnapshotEncodeWorker(t, d)

	started := make(chan struct{})
	release := make(chan struct{})
	publications := make(chan ports.SnapshotPublication, 2)
	repository.EXPECT().Publish(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, publication ports.SnapshotPublication) error {
		publications <- publication
		if publication.Generation == 1 {
			close(started)
			<-release
		}
		return nil
	}).Twice()

	sess := newSnapshotTestSession(t, "work", false, "/work")
	markSnapshotDirty(sess)
	require.True(t, d.scheduleSnapshot(sess))
	select {
	case <-started:
	case <-time.After(testWaitTimeout):
		t.Fatal("routine publication did not start")
	}
	markSnapshotDirty(sess)
	require.True(t, d.scheduleFinalSnapshot(sess))
	markSnapshotDirty(sess)
	require.True(t, d.scheduleFinalSnapshot(sess))
	close(release)

	awaitSnapshotClean(t, sess)
	require.Equal(t, uint64(1), (<-publications).Generation)
	require.Equal(t, uint64(3), (<-publications).Generation)
	sess.snapshotMu.Lock()
	require.False(t, sess.snapshotPending)
	require.Nil(t, sess.snapshotQueuedCapture)
	require.Nil(t, sess.snapshotInFlightCapture)
	sess.snapshotMu.Unlock()
}

func TestForcedSnapshotShutdownTimeoutRetainsRetryableStateAndNotice(t *testing.T) {
	clock := newSnapshotDeadlineClock()
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)
	repository := portsmocks.NewMockSnapshotRepository(t)
	WithSnapshotRepository(repository, nil)(d)
	notices := portsmocks.NewMockNoticeStore(t)
	WithNoticeStore(notices)(d)
	startSnapshotEncodeWorker(t, d)

	blockerStarted := make(chan struct{})
	releaseBlocker := make(chan struct{})
	defer close(releaseBlocker)
	repository.EXPECT().Publish(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, publication ports.SnapshotPublication) error {
		if publication.Name == "blocker" {
			close(blockerStarted)
			<-releaseBlocker
		}
		return nil
	}).Maybe()
	notices.EXPECT().Append(mock.MatchedBy(func(notification domain.Notification) bool {
		return notification.Code == domain.NoticeSnapshotWrite && notification.Severity == domain.NoticeError
	})).Return(nil).Once()

	blocker := newSnapshotTestSession(t, "blocker", false, "/work")
	markSnapshotDirty(blocker)
	require.True(t, d.scheduleSnapshot(blocker))
	select {
	case <-blockerStarted:
	case <-time.After(testWaitTimeout):
		t.Fatal("blocking publication did not start")
	}

	sess := newSnapshotTestSession(t, "work", false, "/work")
	pty := sess.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
	pty.EXPECT().Close().Return(nil).Maybe()
	d.sessions[sess.id] = sess
	markSnapshotDirty(sess)
	require.True(t, d.scheduleSnapshot(sess), "routine capture should occupy the bounded queue")

	killed := make(chan error, 1)
	go func() { killed <- d.killSession(sess, ports.ReasonServerShutdown, false) }()
	timer := clock.nextTimer(t)
	timer.fire()
	require.Error(t, <-killed)

	sess.snapshotMu.Lock()
	require.True(t, sess.snapDirty.Load())
	require.Greater(t, sess.snapshotForcedGeneration, sess.snapshotPublishedGeneration)
	require.NotNil(t, sess.snapshotQueuedCapture, "the one routine capture remains retryable")
	sess.snapshotMu.Unlock()
	d.snapshotWorkerMu.Lock()
	require.LessOrEqual(t, len(d.snapshotJobs), snapshotQueueCapacity)
	d.snapshotWorkerMu.Unlock()
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

type snapshotDeadlineClock struct {
	timers chan *snapshotDeadlineTimer
}

func newSnapshotDeadlineClock() *snapshotDeadlineClock {
	return &snapshotDeadlineClock{timers: make(chan *snapshotDeadlineTimer, 4)}
}

func (*snapshotDeadlineClock) Now() time.Time { return time.Unix(100, 0) }
func (c *snapshotDeadlineClock) NewTimer(time.Duration) ports.Timer {
	timer := &snapshotDeadlineTimer{ch: make(chan time.Time, 1)}
	c.timers <- timer
	return timer
}

func (c *snapshotDeadlineClock) nextTimer(t *testing.T) *snapshotDeadlineTimer {
	t.Helper()
	select {
	case timer := <-c.timers:
		return timer
	case <-time.After(testWaitTimeout):
		t.Fatal("snapshot deadline timer was not created")
		return nil
	}
}

type snapshotDeadlineTimer struct{ ch chan time.Time }

func (t *snapshotDeadlineTimer) C() <-chan time.Time    { return t.ch }
func (*snapshotDeadlineTimer) Reset(time.Duration) bool { return true }
func (*snapshotDeadlineTimer) Stop() bool               { return true }
func (t *snapshotDeadlineTimer) fire()                  { t.ch <- time.Unix(101, 0) }

// deterministicSnapshotClock records timer resets and advances only when a
// test explicitly fires its timer. It deliberately has no wall-clock behavior.
type deterministicSnapshotClock struct {
	mu     sync.Mutex
	now    time.Time
	timers chan *deterministicSnapshotTimer
}

func newDeterministicSnapshotClock(now time.Time) *deterministicSnapshotClock {
	return &deterministicSnapshotClock{now: now, timers: make(chan *deterministicSnapshotTimer, 1)}
}

func (c *deterministicSnapshotClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *deterministicSnapshotClock) advance(now time.Time) {
	c.mu.Lock()
	c.now = now
	c.mu.Unlock()
}

func (c *deterministicSnapshotClock) NewTimer(time.Duration) ports.Timer {
	timer := &deterministicSnapshotTimer{clock: c, ch: make(chan time.Time, 1), resets: make(chan time.Duration, 16)}
	c.timers <- timer
	return timer
}

func (c *deterministicSnapshotClock) timer() *deterministicSnapshotTimer { return <-c.timers }

type deterministicSnapshotTimer struct {
	clock  *deterministicSnapshotClock
	ch     chan time.Time
	resets chan time.Duration
}

func (t *deterministicSnapshotTimer) C() <-chan time.Time { return t.ch }
func (t *deterministicSnapshotTimer) Reset(delay time.Duration) bool {
	t.resets <- delay
	return true
}
func (*deterministicSnapshotTimer) Stop() bool { return true }
func (t *deterministicSnapshotTimer) fire()    { t.ch <- t.clock.Now() }

func awaitSnapshotCleanSignal(sess *session) {
	for {
		sess.snapshotMu.Lock()
		clean := !sess.snapDirty.Load()
		changed := sess.snapshotChangeLocked()
		sess.snapshotMu.Unlock()
		if clean {
			return
		}
		<-changed
	}
}

func TestSnapshotCompletionSchedulesRoutineRetriesFromCompletion(t *testing.T) {
	now := time.Unix(100, 0)
	clock := newDeterministicSnapshotClock(now)
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)
	repository := portsmocks.NewMockSnapshotRepository(t)
	WithSnapshotRepository(repository, nil)(d)

	for _, tt := range []struct {
		name             string
		kind             snapshotAttemptKind
		succeeded        bool
		existingEligible time.Time
		wantDirty        bool
		wantEligible     time.Time
	}{
		{name: "routine success", kind: snapshotAttemptRoutine, succeeded: true, wantEligible: now.Add(snapshotInterval)},
		{name: "routine failure", kind: snapshotAttemptRoutine, wantDirty: true, wantEligible: now.Add(snapshotInterval)},
		{name: "forced completion preserves routine eligibility", kind: snapshotAttemptForced, succeeded: true, existingEligible: now.Add(time.Minute), wantEligible: now.Add(time.Minute)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sess := newSnapshotTestSession(t, "work", false, "/work")
			sess.snapshotGeneration = 1
			sess.snapshotNextEligibleAt = tt.existingEligible
			sess.snapDirty.Store(true)
			d.finishSnapshotCapture(&snapshotCapture{session: sess, generation: 1, attemptKind: tt.kind}, tt.succeeded)

			require.Equal(t, tt.wantDirty, sess.snapDirty.Load())
			sess.snapshotMu.Lock()
			require.Equal(t, tt.wantEligible, sess.snapshotNextEligibleAt)
			sess.snapshotMu.Unlock()
		})
	}
}

func TestSnapshotSchedulerImmediateEligibilityAndStaleCapturesRemainDirty(t *testing.T) {
	now := time.Unix(100, 0)
	clock := newDeterministicSnapshotClock(now)
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)
	WithSnapshotRepository(portsmocks.NewMockSnapshotRepository(t), nil)(d)

	t.Run("no prior attempt is immediately eligible", func(t *testing.T) {
		sess := newSnapshotTestSession(t, "immediate", false, "/work")
		sess.snapshotWake = d.snapshotWake
		sess.snapDirty.Store(true)
		d.sessions[sess.id] = sess

		require.Equal(t, snapshotInterval, d.scheduleEligibleRepositorySnapshots())
		sess.snapshotMu.Lock()
		require.True(t, sess.snapshotAttempted)
		require.Equal(t, uint64(0), sess.snapshotCapturedGeneration)
		require.Equal(t, now.Add(snapshotInterval), sess.snapshotNextEligibleAt)
		sess.snapshotMu.Unlock()
	})

	t.Run("queued and in-flight stale successes cannot clear newer generation", func(t *testing.T) {
		for _, state := range []struct {
			name string
			set  func(*session, *snapshotCapture)
		}{
			{name: "queued", set: func(sess *session, capture *snapshotCapture) { sess.snapshotQueuedCapture = capture }},
			{name: "in flight", set: func(sess *session, capture *snapshotCapture) { sess.snapshotInFlightCapture = capture }},
		} {
			t.Run(state.name, func(t *testing.T) {
				sess := newSnapshotTestSession(t, state.name, false, "/work")
				sess.snapshotGeneration = 1
				sess.snapDirty.Store(true)
				capture := &snapshotCapture{session: sess, generation: 1, attemptKind: snapshotAttemptRoutine}
				sess.snapshotPendingCaptures = 1
				sess.snapshotPending = true
				state.set(sess, capture)

				markSnapshotDirty(sess)
				d.finishSnapshotCapture(capture, true)

				require.True(t, sess.snapDirty.Load())
				require.Equal(t, uint64(1), sess.snapshotPublishedGeneration)
				require.Equal(t, now.Add(snapshotInterval), sess.snapshotNextEligibleAt)
			})
		}
	})
}

func TestSnapshotWorkerQueueIsBoundedAndForcedCapturesCoalesce(t *testing.T) {
	now := time.Unix(100, 0)
	clock := newDeterministicSnapshotClock(now)
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)
	repository := portsmocks.NewMockSnapshotRepository(t)
	WithSnapshotRepository(repository, nil)(d)
	startSnapshotEncodeWorker(t, d)

	started := make(chan struct{})
	release := make(chan struct{})
	publications := make(chan ports.SnapshotPublication, 3)
	var blockFirst sync.Once
	repository.EXPECT().Publish(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, publication ports.SnapshotPublication) error {
		publications <- publication
		if publication.Name == "in-flight" {
			blockFirst.Do(func() {
				close(started)
				<-release
			})
		}
		return nil
	}).Times(3)

	inFlight := newSnapshotTestSession(t, "in-flight", false, "/work")
	markSnapshotDirty(inFlight)
	require.True(t, d.scheduleSnapshot(inFlight))
	<-started

	queued := newSnapshotTestSession(t, "queued", false, "/work")
	markSnapshotDirty(queued)
	require.True(t, d.scheduleSnapshot(queued))

	d.snapshotWorkerMu.Lock()
	require.NotNil(t, d.snapshotWorkerInFlight)
	require.Len(t, d.snapshotJobs, snapshotQueueCapacity)
	d.snapshotWorkerMu.Unlock()

	saturated := newSnapshotTestSession(t, "saturated", false, "/work")
	markSnapshotDirty(saturated)
	require.False(t, d.scheduleSnapshot(saturated), "a saturated queue must not block the producer")
	require.True(t, saturated.snapDirty.Load(), "the rejected capture remains retryable")
	saturated.snapshotMu.Lock()
	require.Equal(t, now.Add(snapshotInterval), saturated.snapshotNextEligibleAt)
	saturated.snapshotMu.Unlock()

	// A forced request bypasses the routine rate limit and repeated requests
	// coalesce to one successor after the in-flight capture completes.
	markSnapshotDirty(inFlight)
	require.True(t, d.scheduleFinalSnapshot(inFlight))
	markSnapshotDirty(inFlight)
	require.True(t, d.scheduleFinalSnapshot(inFlight))
	close(release)

	gotGenerations := []uint64{(<-publications).Generation, (<-publications).Generation, (<-publications).Generation}
	require.ElementsMatch(t, []uint64{1, 1, 3}, gotGenerations, "one forced successor bypasses the routine limit while queued work remains bounded")
	awaitSnapshotCleanSignal(inFlight)
}

func TestSnapshotRepositorySaverUsesEarliestDeadlineAndRecomputes(t *testing.T) {
	now := time.Unix(100, 0)
	clock := newDeterministicSnapshotClock(now)
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)
	repository := portsmocks.NewMockSnapshotRepository(t)
	WithSnapshotRepository(repository, nil)(d)
	startSnapshotEncodeWorker(t, d)

	published := make(chan ports.SnapshotPublication, 1)
	repository.EXPECT().Publish(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, publication ports.SnapshotPublication) error {
		published <- publication
		return nil
	}).Once()

	later := newSnapshotTestSession(t, "later", false, "/work")
	later.snapshotWake = d.snapshotWake
	later.snapDirty.Store(true)
	later.snapshotNextEligibleAt = now.Add(10 * time.Minute)
	d.sessions[later.id] = later

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.snapshotRepositorySaver(ctx)
		close(done)
	}()
	timer := clock.timer()
	require.Equal(t, 10*time.Minute, <-timer.resets)

	earlier := newSnapshotTestSession(t, "earlier", false, "/work")
	earlier.snapshotWake = d.snapshotWake
	earlier.snapDirty.Store(true)
	earlier.snapshotNextEligibleAt = now.Add(5 * time.Minute)
	d.sessions[earlier.id] = earlier
	d.snapshotWake <- struct{}{}
	require.Equal(t, 5*time.Minute, <-timer.resets)

	clock.advance(now.Add(5 * time.Minute))
	timer.fire()
	// The saver first recomputes immediately after queueing the due capture.
	require.Equal(t, 5*time.Minute, <-timer.resets)
	publication := <-published
	require.Equal(t, "earlier", publication.Name, "timer fires exactly at the earliest eligibility")
	for {
		earlier.snapshotMu.Lock()
		clean := !earlier.snapDirty.Load()
		changed := earlier.snapshotChangeLocked()
		earlier.snapshotMu.Unlock()
		if clean {
			break
		}
		<-changed
	}
	// The later dirty session is still the next deadline after completion.
	require.Equal(t, 5*time.Minute, <-timer.resets)

	delete(d.sessions, later.id)
	d.snapshotWake <- struct{}{}
	require.Equal(t, 24*time.Hour, <-timer.resets)

	cancel()
	<-done
}

package daemon

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func TestSnapshotCoordinatorQuarantineCancelsPublicationBeforeDelete(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), newStubClock(t))
		repository := portsmocks.NewMockSnapshotRepository(t)
		WithSnapshotRepository(repository)(d)
		startSnapshotEncodeWorker(t, d)

		started := make(chan struct{})
		repository.EXPECT().Publish(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, _ ports.SnapshotPublication) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}).Once()
		sess := newSnapshotTestSession(t, "work", false, "/work")
		markSnapshotDirty(sess)
		require.True(t, d.scheduleSnapshot(sess))
		requireReceived(t, started, "publication did not start")

		done := quarantineSnapshotCoordinator(sess)
		requireReceived(t, done, "quarantine did not join canceled publication")
	})
}

func TestSnapshotCoordinatorQuarantineJoinsInFlightPublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), newStubClock(t))
		repository := portsmocks.NewMockSnapshotRepository(t)
		WithSnapshotRepository(repository)(d)
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
		requireReceived(t, started, "publication did not start")

		done := quarantineSnapshotCoordinator(sess)
		select {
		case <-done:
			t.Fatal("quarantine joined before publication returned")
		default:
		}
		close(release)
		requireReceived(t, done, "quarantine did not join publication")
	})
}

// TestQuarantineDiscardDoesNotDoubleDecrementPendingCaptures proves the pending
// capture accounting is exact across a quarantine discard and a rollback
// replacement: a queued capture the quarantine discarded already dropped its own
// pending count, so the worker-side finish that rejects its stale identity must
// not decrement a replacement scheduled after the rollback.
func TestQuarantineDiscardDoesNotDoubleDecrementPendingCaptures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), newStubClock(t))
		repository := portsmocks.NewMockSnapshotRepository(t)
		WithSnapshotRepository(repository)(d)
		startSnapshotEncodeWorker(t, d)

		blockerStarted := make(chan struct{})
		releaseBlocker := make(chan struct{})
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(releaseBlocker) }) }
		t.Cleanup(release)
		repository.EXPECT().Publish(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, publication ports.SnapshotPublication) error {
			if publication.Name == "blocker" {
				close(blockerStarted)
				<-releaseBlocker
			}
			return nil
		}).Maybe()

		// Occupy the single worker so the test session's capture stays queued.
		blocker := newSnapshotTestSession(t, "blocker", false, "/work")
		markSnapshotDirty(blocker)
		require.True(t, d.scheduleSnapshot(blocker))
		requireReceived(t, blockerStarted, "the blocking publication did not start")

		sess := newSnapshotTestSession(t, "work", false, "/work")
		markSnapshotDirty(sess)
		require.True(t, d.scheduleSnapshot(sess), "the test capture should queue behind the blocked worker")
		sess.snapshotMu.Lock()
		first := sess.snapshotQueuedCapture
		firstCount := sess.snapshotPendingCaptures
		sess.snapshotMu.Unlock()
		require.NotNil(t, first)
		require.Equal(t, uint(1), firstCount)

		// Quarantine discards the queued capture and drops its pending count exactly
		// once.
		<-quarantineSnapshotCoordinator(sess)
		sess.snapshotMu.Lock()
		discardedCount := sess.snapshotPendingCaptures
		sess.snapshotMu.Unlock()
		require.Equal(t, uint(0), discardedCount, "quarantine must consume the discarded capture's count")
		require.True(t, first.coordinatorDiscarded)

		// Model the worker having already dequeued the stale capture: the global
		// bounded queue still holds it, so free the slot before the replacement.
		select {
		case dequeued := <-d.snapshots.jobs:
			require.Same(t, first, dequeued)
		default:
			t.Fatal("the discarded capture must still be queued in the global worker queue")
		}

		// The rollback restores scheduling, and the replacement capture owns its own
		// pending count.
		rollbackSnapshotQuarantine(sess, true)
		require.True(t, d.scheduleSnapshot(sess), "the rolled-back session must schedule a replacement capture")
		sess.snapshotMu.Lock()
		replacement := sess.snapshotQueuedCapture
		replacementCount := sess.snapshotPendingCaptures
		sess.snapshotMu.Unlock()
		require.NotNil(t, replacement)
		require.NotSame(t, first, replacement)
		require.Equal(t, uint(1), replacementCount)

		// The worker finishing the stale discarded capture must not decrement the
		// live replacement's pending count.
		d.finishSnapshotCapture(first, false)
		sess.snapshotMu.Lock()
		afterFinishCount := sess.snapshotPendingCaptures
		afterFinishPending := sess.snapshotPending
		sess.snapshotMu.Unlock()
		require.Equal(t, uint(1), afterFinishCount, "the discarded capture must not decrement the replacement")
		require.True(t, afterFinishPending)

		// Once the worker drains both captures only the replacement publishes, and
		// the accounting returns to zero.
		release()
		requireSnapshotClean(t, sess)
		sess.snapshotMu.Lock()
		finalCount := sess.snapshotPendingCaptures
		finalPending := sess.snapshotPending
		sess.snapshotMu.Unlock()
		require.Equal(t, uint(0), finalCount)
		require.False(t, finalPending)
	})
}

func TestForcedSnapshotSchedulesSuccessorAfterRoutineInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), newStubClock(t))
		repository := portsmocks.NewMockSnapshotRepository(t)
		WithSnapshotRepository(repository)(d)
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
		requireReceived(t, started, "routine publication did not start")

		// The terminal state changed after the routine capture, so its publication
		// cannot satisfy the forced checkpoint.
		sess.tabs[0].panes["pane-1"].screen.Write([]byte(" terminal"))
		markSnapshotDirty(sess)
		require.True(t, d.scheduleFinalSnapshot(sess))
		close(release)

		requireSnapshotClean(t, sess)
		require.Equal(t, uint64(1), (<-publications).Generation)
		require.Equal(t, uint64(2), (<-publications).Generation)
	})
}

func TestForcedSnapshotSchedulesSuccessorForRoutineCaptureQueuedBehindWorker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), newStubClock(t))
		repository := portsmocks.NewMockSnapshotRepository(t)
		WithSnapshotRepository(repository)(d)
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
		requireReceived(t, blockerStarted, "blocking publication did not start")

		sess := newSnapshotTestSession(t, "work", false, "/work")
		markSnapshotDirty(sess)
		require.True(t, d.scheduleSnapshot(sess), "routine capture should occupy the bounded queue")
		sess.tabs[0].panes["pane-1"].screen.Write([]byte(" terminal"))
		markSnapshotDirty(sess)
		require.True(t, d.scheduleFinalSnapshot(sess))
		close(releaseBlocker)

		requireSnapshotClean(t, sess)
		require.Equal(t, uint64(1), (<-workPublications).Generation)
		require.Equal(t, uint64(2), (<-workPublications).Generation)
	})
}

func TestMultipleForcedSnapshotsCoalesceToOneSuccessor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), newStubClock(t))
		repository := portsmocks.NewMockSnapshotRepository(t)
		WithSnapshotRepository(repository)(d)
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
		requireReceived(t, started, "routine publication did not start")
		markSnapshotDirty(sess)
		require.True(t, d.scheduleFinalSnapshot(sess))
		markSnapshotDirty(sess)
		require.True(t, d.scheduleFinalSnapshot(sess))
		close(release)

		requireSnapshotClean(t, sess)
		require.Equal(t, uint64(1), (<-publications).Generation)
		require.Equal(t, uint64(2), (<-publications).Generation)
		sess.snapshotMu.Lock()
		require.False(t, sess.snapshotPending)
		require.Nil(t, sess.snapshotQueuedCapture)
		require.Nil(t, sess.snapshotInFlightCapture)
		sess.snapshotMu.Unlock()
	})
}

func TestForcedSnapshotShutdownTimeoutRetainsRetryableStateAndNotice(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock, deadlineTimers := newSignalClock(t, 4)
		d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)
		repository := portsmocks.NewMockSnapshotRepository(t)
		WithSnapshotRepository(repository)(d)
		notices := portsmocks.NewMockNoticeStore(t)
		WithNoticeStore(notices)(d)
		startSnapshotEncodeWorker(t, d)

		blockerStarted := make(chan struct{})
		releaseBlocker := make(chan struct{})
		var releaseBlockerOnce sync.Once
		release := func() { releaseBlockerOnce.Do(func() { close(releaseBlocker) }) }
		// Register this after newTestDaemon's worker cleanup so the uncooperative
		// repository call is always released before cleanup asks the worker to join.
		t.Cleanup(release)
		repository.EXPECT().Publish(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, publication ports.SnapshotPublication) error {
			require.Equal(t, "blocker", publication.Name)
			close(blockerStarted)
			<-releaseBlocker
			return nil
		}).Once()
		notices.EXPECT().Append(mock.MatchedBy(func(notification domain.Notification) bool {
			return notification.Code == domain.NoticeSnapshotWrite && notification.Severity == domain.NoticeError
		})).Return(nil).Once()

		blocker := newSnapshotTestSession(t, "blocker", false, "/work")
		markSnapshotDirty(blocker)
		require.True(t, d.scheduleSnapshot(blocker))
		requireReceived(t, blockerStarted, "blocking publication did not start")

		sess := newSnapshotTestSession(t, "work", false, "/work")
		pty, ok := sess.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
		require.True(t, ok, "snapshot test pane must use MockPTY")
		pty.EXPECT().Close().Return(nil).Maybe()
		d.sessions[sess.id] = sess
		markSnapshotDirty(sess)
		require.True(t, d.scheduleSnapshot(sess), "routine capture should occupy the bounded queue")

		killed := make(chan error, 1)
		go func() { killed <- d.killSession(sess, protocol.ReasonServerShutdown, false) }()
		timer := requireReceived(t, deadlineTimers, "snapshot deadline timer was not created")
		timer.fire()
		require.Error(t, <-killed)

		func() {
			sess.snapshotMu.Lock()
			defer sess.snapshotMu.Unlock()
			require.True(t, sess.snapDirty.Load())
			require.Greater(t, sess.snapshotForcedGeneration, sess.snapshotPublishedGeneration)
			require.True(t, sess.snapshotPending)
			require.Equal(t, uint(1), sess.snapshotPendingCaptures)
			require.NotNil(t, sess.snapshotQueuedCapture, "the one routine capture remains retryable")
			require.Nil(t, sess.snapshotInFlightCapture)
		}()
		d.snapshots.mu.Lock()
		require.LessOrEqual(t, len(d.snapshots.jobs), snapshotQueueCapacity)
		d.snapshots.mu.Unlock()

		// The retained capture is not published after quarantine, but releasing the
		// unrelated blocked call lets the worker discard it and lets cleanup join.
		release()
		for {
			sess.snapshotMu.Lock()
			complete := !sess.snapshotPending && sess.snapshotQueuedCapture == nil
			changed := sess.snapshotChangeLocked()
			sess.snapshotMu.Unlock()
			if complete {
				break
			}
			requireReceived(t, changed, "quarantined capture was not discarded")
		}
	})
}

func TestServeShutdownDeadlineStillJoinsRetainedSessionWriter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pty, releasePTY := newBlockingPTY(t)
		defer releasePTY()
		clock := newServeShutdownClock(t)
		d := newTestDaemon(t, newFactory(t, pty), clock.mock)
		repository := portsmocks.NewMockSnapshotRepository(t)
		WithSnapshotRepository(repository)(d)

		publicationStarted := make(chan struct{})
		releasePublication := make(chan struct{})
		var releasePublicationOnce sync.Once
		releaseSnapshot := func() { releasePublicationOnce.Do(func() { close(releasePublication) }) }
		repository.EXPECT().Publish(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, publication ports.SnapshotPublication) error {
			require.Equal(t, uint64(1), publication.Generation)
			close(publicationStarted)
			<-releasePublication
			return nil
		}).Once()

		tr, sends, _ := newConn(t, mustHello(protocol.IntentNew, "work", domain.Size{Cols: 80, Rows: 24}))
		listener := serveSnapshotListener(t, tr)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		served := make(chan error, 1)
		go func() { served <- d.Serve(ctx, listener) }()

		requireQueuedFrame(t, sends, "Welcome")
		sess := firstSession(d)
		require.NotNil(t, sess)

		// A retained-session teardown still publishes a terminal snapshot. It must
		// own teardown before Serve's context cancellation enters its competing
		// shutdownAll path.
		go func() { _ = d.killSession(sess, protocol.ReasonServerShutdown, false) }()
		requireReceived(t, publicationStarted, "pane EOF teardown did not publish its terminal snapshot")
		// The pane-exit owner has an independent final-snapshot timer. Keep it
		// unfired until cleanup so Serve must use, and remain bounded by, its shared
		// deadline.
		ownerDeadlineTimer := clock.nextFinalTimer(t)

		// Cleanup fires the pane-exit owner's independent budget and joins the real
		// pane reader. Serve itself must remain owned until the repository writer is
		// released below, even after its checkpoint deadline expires.
		t.Cleanup(func() {
			releaseSnapshot()
			ownerDeadlineTimer.fire()
			d.sessWg.Wait()
		})

		cancel()
		shutdownDeadlineTimer := clock.nextFinalTimer(t)
		shutdownDeadlineTimer.fire()

		select {
		case err := <-served:
			t.Fatalf("Serve returned with a durable writer alive: %v", err)
		default:
		}
		releaseSnapshot()
		err := requireReceived(t, served, "Serve did not return after the durable writer exited")
		require.NoError(t, err)

		sess.snapshotMu.Lock()
		generation := sess.snapshotGeneration
		sess.snapshotMu.Unlock()
		require.Equal(t, uint64(1), generation, "Serve shutdown must not create a competing terminal generation")
		require.Equal(t, int64(snapshotFinalFlushTimeout), clock.finalBudget.Load(), "Serve must spend only its shared snapshot deadline")
	})
}

// awaitTeardownWaiters waits for the teardown waiter count by its change
// channel under one bounded real-time guard: the waiters block on mutexes, which
// a synctest bubble cannot observe.
//
//testpolicy:realtime mutex-blocked waiters are not durably blocked
func awaitTeardownWaiters(t *testing.T, sess *session, want uint) {
	t.Helper()
	for {
		sess.teardownMu.Lock()
		waiters := sess.teardownWaiters
		changed := sess.teardownChangeLocked()
		sess.teardownMu.Unlock()
		if waiters == want {
			return
		}
		select {
		case <-changed:
		case <-time.After(testWaitTimeout):
			t.Fatalf("teardown ownership waiters = %d, want %d", waiters, want)
		}
	}
}

//testpolicy:realtime duplicate teardown callers block on mutexes, invisible to synctest
func TestConcurrentSessionTeardownPublishesOneFinalSnapshot(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), newStubClock(t))
	repository := portsmocks.NewMockSnapshotRepository(t)
	WithSnapshotRepository(repository)(d)
	startSnapshotEncodeWorker(t, d)

	publicationStarted := make(chan struct{})
	releasePublication := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releasePublication) }) })
	repository.EXPECT().Publish(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, _ ports.SnapshotPublication) error {
		close(publicationStarted)
		<-releasePublication
		return nil
	}).Once()

	sess := newSnapshotTestSession(t, "work", false, "/work")
	pty, ok := sess.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
	require.True(t, ok)
	pty.EXPECT().Close().Return(nil).Maybe()
	d.sessions[sess.id] = sess

	first := make(chan error, 1)
	go func() { first <- d.killSession(sess, protocol.ReasonSessionKilled, false) }()
	receiveBounded(t, publicationStarted, "ordinary teardown did not publish its terminal snapshot")

	sess.teardownMu.Lock()
	require.True(t, sess.teardownActive)
	require.NotNil(t, sess.teardownDone)
	sess.teardownMu.Unlock()

	const duplicateCallers = 8
	duplicates := make(chan error, duplicateCallers)
	for range duplicateCallers {
		go func() {
			duplicates <- d.killSession(sess, protocol.ReasonSessionKilled, false)
		}()
	}
	// Synchronize on the ownership state itself: every duplicate is waiting
	// behind the distinct ordinary teardown before its publication is released.
	awaitTeardownWaiters(t, sess, duplicateCallers)

	sess.snapshotMu.Lock()
	generation := sess.snapshotGeneration
	sess.snapshotMu.Unlock()
	require.Equal(t, uint64(1), generation, "duplicate teardown callers must not create synthetic terminal generations")

	releaseOnce.Do(func() { close(releasePublication) })
	err := receiveBounded(t, first, "ordinary teardown did not finish")
	require.NoError(t, err)
	for range duplicateCallers {
		err := receiveBounded(t, duplicates, "duplicate teardown did not observe the completed registry transition")
		require.NoError(t, err)
	}
	sess.snapshotMu.Lock()
	generation = sess.snapshotGeneration
	sess.snapshotMu.Unlock()
	require.Equal(t, uint64(1), generation, "completed duplicate teardowns must leave the terminal generation unchanged")
}

func TestServeShutdownCheckpointsBeforeStoppingSnapshotWorker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, releasePTY := newBlockingPTY(t)
		defer releasePTY()
		d := newTestDaemon(t, newFactory(t, p), newStubClock(t))
		repository := portsmocks.NewMockSnapshotRepository(t)
		WithSnapshotRepository(repository)(d)

		published := make(chan ports.SnapshotPublication, 2)
		allowTerminalPublication := make(chan struct{})
		repository.EXPECT().Publish(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, publication ports.SnapshotPublication) error {
			published <- publication
			if publication.Generation == 2 {
				<-allowTerminalPublication
			}
			return nil
		}).Twice()

		tr, sends, _ := newConn(t, mustHello(protocol.IntentNew, "work", domain.Size{Cols: 80, Rows: 24}))
		l := serveSnapshotListener(t, tr)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		served := make(chan error, 1)
		go func() { served <- d.Serve(ctx, l) }()

		requireQueuedFrame(t, sends, "Welcome")
		markSnapshotDirty(firstSession(d))
		d.snapshots.wake <- struct{}{}
		first := <-published
		require.Equal(t, uint64(1), first.Generation)

		cancel()
		terminal := <-published
		require.Equal(t, uint64(2), terminal.Generation, "shutdown must publish a forced terminal checkpoint")
		close(allowTerminalPublication)

		err := requireReceived(t, served, "Serve did not return after the terminal checkpoint completed")
		require.NoError(t, err)
	})
}

func TestServeShutdownDeadlineStillJoinsUncooperativeSnapshotRepository(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, releasePTY := newBlockingPTY(t)
		defer releasePTY()
		clock := newServeShutdownClock(t)
		d := newTestDaemon(t, newFactory(t, p), clock.mock)
		repository := portsmocks.NewMockSnapshotRepository(t)
		WithSnapshotRepository(repository)(d)
		noticeStore := portsmocks.NewMockNoticeStore(t)
		WithNoticeStore(noticeStore)(d)
		noticeStore.EXPECT().Claim().Return(nil, nil).Maybe()
		noticeStore.EXPECT().Ack().Return(nil).Maybe()
		noticePersisted := make(chan struct{})
		noticeStore.EXPECT().Append(mock.MatchedBy(func(n domain.Notification) bool {
			return n.Code == domain.NoticeSnapshotWrite && n.Severity == domain.NoticeError
		})).Run(func(domain.Notification) { close(noticePersisted) }).Return(nil).Once()

		firstPublicationCompleted := make(chan struct{})
		terminalPublicationStarted := make(chan struct{})
		releaseTerminalPublication := make(chan struct{})
		var lastValid atomic.Uint64
		repository.EXPECT().Publish(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, publication ports.SnapshotPublication) error {
			if publication.Generation == 1 {
				lastValid.Store(publication.Generation)
				close(firstPublicationCompleted)
				return nil
			}
			close(terminalPublicationStarted)
			<-releaseTerminalPublication // Deliberately ignores the worker cancellation context.
			return nil
		}).Twice()

		tr, sends, _ := newConn(t, mustHello(protocol.IntentNew, "work", domain.Size{Cols: 80, Rows: 24}))
		l := serveSnapshotListener(t, tr)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		served := make(chan error, 1)
		go func() { served <- d.Serve(ctx, l) }()

		requireQueuedFrame(t, sends, "Welcome")
		markSnapshotDirty(firstSession(d))
		d.snapshots.wake <- struct{}{}
		requireReceived(t, firstPublicationCompleted, "initial checkpoint did not complete")

		cancel()
		requireReceived(t, terminalPublicationStarted, "shutdown terminal checkpoint did not reach the worker")
		d.snapshots.mu.Lock()
		workerDone := d.snapshots.done
		d.snapshots.mu.Unlock()
		require.NotNil(t, workerDone)
		clock.nextFinalTimer(t).fire()
		requireReceived(t, noticePersisted, "shutdown timeout notice was not persisted before the writer join")

		select {
		case err := <-served:
			t.Fatalf("Serve returned with an uncooperative durable writer alive: %v", err)
		default:
		}
		close(releaseTerminalPublication)
		requireReceived(t, workerDone, "snapshot worker leaked after its repository call was released")
		err := requireReceived(t, served, "Serve did not return after the uncooperative writer exited")
		require.NoError(t, err)
		require.LessOrEqual(t, clock.finalBudget.Load(), int64(snapshotFinalFlushTimeout), "shutdown must consume at most one snapshot deadline")
		require.Equal(t, uint64(1), lastValid.Load(), "the last valid checkpoint remains available after the terminal attempt times out")
	})
}

// serveShutdownClock is the plain state behind a generated clock mock: it
// records the final-flush deadline timers and the budget their firing spends.
type serveShutdownClock struct {
	mock        *portsmocks.MockClock
	finalTimers chan *budgetTimer
	finalBudget atomic.Int64
}

// budgetTimer is a deadline timer whose firing spends its delay from the clock's
// budget. It is a value holder around a testTimer, not a ports.Timer.
type budgetTimer struct {
	timer *testTimer
	spend func()
}

func (b *budgetTimer) fire() {
	b.spend()
	b.timer.fire()
}

func newServeShutdownClock(t testing.TB) *serveShutdownClock {
	c := &serveShutdownClock{finalTimers: make(chan *budgetTimer, 2)}
	clock, timers := newSignalClock(t, 4)
	stub := newStubTimer(t)
	c.mock = portsmocks.NewMockClock(t)
	c.mock.EXPECT().Now().Return(time.Unix(100, 0)).Maybe()
	c.mock.EXPECT().NewTimer(mock.Anything).RunAndReturn(func(delay time.Duration) ports.Timer {
		if delay != snapshotFinalFlushTimeout {
			return stub
		}
		timer := clock.NewTimer(delay)
		tt := <-timers
		c.finalTimers <- &budgetTimer{timer: tt, spend: func() { c.finalBudget.Add(int64(delay)) }}
		return timer
	}).Maybe()
	return c
}

func (c *serveShutdownClock) nextFinalTimer(t *testing.T) *budgetTimer {
	t.Helper()
	return requireReceived(t, c.finalTimers, "shutdown checkpoint deadline timer was not created")
}

func serveSnapshotListener(t *testing.T, tr wire.Transport) *mockServerListener {
	t.Helper()
	l := newMockServerListener(t)
	connections := make(chan wire.Transport, 1)
	connections <- tr
	closed := make(chan struct{})
	var once sync.Once
	l.EXPECT().Accept().RunAndReturn(func() (wire.Transport, error) {
		select {
		case conn := <-connections:
			return conn, nil
		case <-closed:
			return nil, io.EOF
		}
	}).Maybe()
	l.EXPECT().Close().RunAndReturn(func() error {
		once.Do(func() { close(closed) })
		return nil
	}).Maybe()
	l.EXPECT().Addr().Return("mock").Maybe()
	return l
}

func TestRoutineSnapshotEligibilityStartsAtCompletionAndForcedDoesNotMoveIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := newFixedSnapshotClock(t, time.Unix(100, 0))
		d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)
		repository := portsmocks.NewMockSnapshotRepository(t)
		WithSnapshotRepository(repository)(d)
		startSnapshotEncodeWorker(t, d)

		var publishes atomic.Int32
		repository.EXPECT().Publish(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, _ ports.SnapshotPublication) error {
			publishes.Add(1)
			return nil
		}).Times(2)

		sess := newSnapshotTestSession(t, "work", false, "/work")
		markSnapshotDirty(sess)
		require.True(t, d.scheduleSnapshot(sess))
		requireSnapshotClean(t, sess)

		sess.snapshotMu.Lock()
		eligibleAt := sess.snapshotNextEligibleAt
		sess.snapshotMu.Unlock()
		require.Equal(t, time.Unix(100, 0).Add(snapshotInterval), eligibleAt)

		markSnapshotDirty(sess)
		require.True(t, d.scheduleSnapshot(sess))
		require.Equal(t, int32(1), publishes.Load(), "rate-limited routine work must not publish")

		require.True(t, d.scheduleFinalSnapshot(sess))
		requireSnapshotClean(t, sess)
		sess.snapshotMu.Lock()
		require.Equal(t, eligibleAt, sess.snapshotNextEligibleAt)
		sess.snapshotMu.Unlock()
	})
}

// newFixedSnapshotClock reports a constant time and never fires its timers.
func newFixedSnapshotClock(t testing.TB, now time.Time) *portsmocks.MockClock {
	t.Helper()
	clock := portsmocks.NewMockClock(t)
	clock.EXPECT().Now().Return(now).Maybe()
	clock.EXPECT().NewTimer(mock.Anything).Return(newStubTimer(t)).Maybe()
	return clock
}

// deterministicSnapshotClock is the plain state behind a generated clock mock
// that records timer resets and advances only when a test says so.
type deterministicSnapshotClock struct {
	mock   *portsmocks.MockClock
	mu     sync.Mutex
	now    time.Time
	timers chan *deterministicSnapshotTimer
}

func newDeterministicSnapshotClock(t testing.TB, now time.Time) *deterministicSnapshotClock {
	c := &deterministicSnapshotClock{now: now, timers: make(chan *deterministicSnapshotTimer, 1)}
	c.mock = portsmocks.NewMockClock(t)
	c.mock.EXPECT().Now().RunAndReturn(c.current).Maybe()
	c.mock.EXPECT().NewTimer(mock.Anything).RunAndReturn(func(time.Duration) ports.Timer {
		timer := &deterministicSnapshotTimer{clock: c, ch: make(chan time.Time, 1), resets: make(chan time.Duration, 16)}
		m := portsmocks.NewMockTimer(t)
		m.EXPECT().C().Return((<-chan time.Time)(timer.ch)).Maybe()
		m.EXPECT().Reset(mock.Anything).RunAndReturn(func(delay time.Duration) bool {
			timer.resets <- delay
			return true
		}).Maybe()
		m.EXPECT().Stop().Return(true).Maybe()
		c.timers <- timer
		return m
	}).Maybe()
	return c
}

func (c *deterministicSnapshotClock) current() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *deterministicSnapshotClock) advance(now time.Time) {
	c.mu.Lock()
	c.now = now
	c.mu.Unlock()
}

func (c *deterministicSnapshotClock) timer() *deterministicSnapshotTimer { return <-c.timers }

// deterministicSnapshotTimer is the observable side of a generated timer mock.
type deterministicSnapshotTimer struct {
	clock  *deterministicSnapshotClock
	ch     chan time.Time
	resets chan time.Duration
}

func (t *deterministicSnapshotTimer) fire() { t.ch <- t.clock.current() }

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
			synctest.Test(t, func(t *testing.T) {
				clock := newDeterministicSnapshotClock(t, now)
				d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock.mock)
				WithSnapshotRepository(portsmocks.NewMockSnapshotRepository(t))(d)
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
		})
	}
}

func TestSnapshotSchedulerImmediateEligibilityAndStaleCapturesRemainDirty(t *testing.T) {
	now := time.Unix(100, 0)
	newDaemon := func(t *testing.T) *Daemon {
		clock := newDeterministicSnapshotClock(t, now)
		d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock.mock)
		WithSnapshotRepository(portsmocks.NewMockSnapshotRepository(t))(d)
		return d
	}

	t.Run("no prior attempt is immediately eligible", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			d := newDaemon(t)
			published := make(chan ports.SnapshotPublication, 1)
			repository := portsmocks.NewMockSnapshotRepository(t)
			WithSnapshotRepository(repository)(d)
			startSnapshotEncodeWorker(t, d)
			repository.EXPECT().Publish(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, publication ports.SnapshotPublication) error {
				published <- publication
				return nil
			}).Once()

			sess := newSnapshotTestSession(t, "immediate", false, "/work")
			sess.snapshotWake = d.snapshots.wake
			sess.snapDirty.Store(true)
			d.sessions[sess.id] = sess

			d.scheduleEligibleRepositorySnapshots()
			publication := requireReceived(t, published, "immediately eligible snapshot was not published")
			require.Equal(t, "immediate", publication.Name)
			requireSnapshotClean(t, sess)
		})
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
				synctest.Test(t, func(t *testing.T) {
					d := newDaemon(t)
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
			})
		}
	})
}

func TestSnapshotWorkerQueueIsBoundedAndForcedCapturesCoalesce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Unix(100, 0)
		clock := newDeterministicSnapshotClock(t, now)
		d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock.mock)
		repository := portsmocks.NewMockSnapshotRepository(t)
		WithSnapshotRepository(repository)(d)
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

		d.snapshots.mu.Lock()
		require.NotNil(t, d.snapshots.inFlight)
		require.Len(t, d.snapshots.jobs, snapshotQueueCapacity)
		d.snapshots.mu.Unlock()

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
		require.ElementsMatch(t, []uint64{1, 1, 2}, gotGenerations, "one forced successor bypasses the routine limit while queued work remains bounded")
		awaitSnapshotCleanSignal(inFlight)
	})
}

func TestSnapshotRepositorySaverUsesEarliestDeadlineAndRecomputes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Unix(100, 0)
		clock := newDeterministicSnapshotClock(t, now)
		d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock.mock)
		repository := portsmocks.NewMockSnapshotRepository(t)
		WithSnapshotRepository(repository)(d)
		startSnapshotEncodeWorker(t, d)

		published := make(chan ports.SnapshotPublication, 1)
		repository.EXPECT().Publish(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, publication ports.SnapshotPublication) error {
			published <- publication
			return nil
		}).Once()

		later := newSnapshotTestSession(t, "later", false, "/work")
		later.snapshotWake = d.snapshots.wake
		later.snapDirty.Store(true)
		later.snapshotNextEligibleAt = now.Add(10 * time.Minute)
		d.sessions[later.id] = later

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() {
			d.snapshotRepositorySaver(ctx)
			close(done)
		}()
		timer := clock.timer()
		require.Equal(t, 10*time.Minute, <-timer.resets)

		earlier := newSnapshotTestSession(t, "earlier", false, "/work")
		earlier.snapshotWake = d.snapshots.wake
		earlier.snapDirty.Store(true)
		earlier.snapshotNextEligibleAt = now.Add(5 * time.Minute)
		d.sessions[earlier.id] = earlier
		d.snapshots.wake <- struct{}{}
		require.Equal(t, 5*time.Minute, <-timer.resets)

		clock.advance(now.Add(5 * time.Minute))
		timer.fire()
		// The saver first recomputes immediately after queueing the due capture.
		require.Equal(t, 5*time.Minute, <-timer.resets)
		publication := <-published
		require.Equal(t, "earlier", publication.Name, "timer fires exactly at the earliest eligibility")
		requireSnapshotClean(t, earlier)
		// The later dirty session is still the next deadline after completion.
		require.Equal(t, 5*time.Minute, <-timer.resets)

		delete(d.sessions, later.id)
		d.snapshots.wake <- struct{}{}
		require.Equal(t, 24*time.Hour, <-timer.resets)

		cancel()
		<-done
	})
}

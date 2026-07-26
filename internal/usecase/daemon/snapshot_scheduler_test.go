package daemon

import (
	"context"
	"io"
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
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	repository := portsmocks.NewMockSnapshotRepository(t)
	WithSnapshotRepository(repository, nil)(d)
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
	select {
	case <-started:
	case <-time.After(testWaitTimeout):
		t.Fatal("publication did not start")
	}

	done := quarantineSnapshotCoordinator(sess)
	select {
	case <-done:
	case <-time.After(testWaitTimeout):
		t.Fatal("quarantine did not join canceled publication")
	}
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
	require.Equal(t, uint64(2), (<-publications).Generation)
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
	select {
	case <-blockerStarted:
	case <-time.After(testWaitTimeout):
		t.Fatal("blocking publication did not start")
	}

	sess := newSnapshotTestSession(t, "work", false, "/work")
	pty, ok := sess.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
	require.True(t, ok, "snapshot test pane must use MockPTY")
	pty.EXPECT().Close().Return(nil).Maybe()
	d.sessions[sess.id] = sess
	markSnapshotDirty(sess)
	require.True(t, d.scheduleSnapshot(sess), "routine capture should occupy the bounded queue")

	killed := make(chan error, 1)
	go func() { killed <- d.killSession(sess, ports.ReasonServerShutdown, false) }()
	timer := clock.nextTimer(t)
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
	d.snapshotWorkerMu.Lock()
	require.LessOrEqual(t, len(d.snapshotJobs), snapshotQueueCapacity)
	d.snapshotWorkerMu.Unlock()

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
		select {
		case <-changed:
		case <-time.After(testWaitTimeout):
			t.Fatal("quarantined capture was not discarded")
		}
	}
}

func TestServeShutdownDeadlineBoundsPaneExitTeardownOwner(t *testing.T) {
	pty, releasePTY := newBlockingPTY(t)
	clock := newServeShutdownClock()
	d := newTestDaemon(t, newFactory(t, pty), clock)
	repository := portsmocks.NewMockSnapshotRepository(t)
	WithSnapshotRepository(repository, nil)(d)

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

	tr, sends, _ := newConn(t, mustHello(ports.IntentNew, "work", domain.Size{Cols: 80, Rows: 24}))
	listener := serveSnapshotListener(t, tr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- d.Serve(ctx, listener) }()

	awaitFrame(t, sends, ports.MsgWelcome)
	sess := firstSession(d)
	require.NotNil(t, sess)

	// Releasing the real pane reader drives EOF through ptyReader, reapPane, and
	// closePane. That ordinary lifecycle path must own teardown before Serve's
	// context cancellation enters its competing shutdownAll path.
	releasePTY()
	select {
	case <-publicationStarted:
	case <-time.After(testWaitTimeout):
		t.Fatal("pane EOF teardown did not publish its terminal snapshot")
	}
	// The pane-exit owner has an independent final-snapshot timer. Keep it
	// unfired until cleanup so Serve must use, and remain bounded by, its shared
	// deadline.
	ownerDeadlineTimer := clock.nextFinalTimer(t)

	// Cleanup is the only place that releases the competing owner. It also fires
	// that owner's independent budget and joins the real pane reader, proving no
	// teardown goroutine is leaked without making Serve depend on repository I/O.
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
		require.NoError(t, err)
	case <-time.After(testWaitTimeout):
		t.Fatal("Serve did not return after its shared deadline while the pane-exit owner remained blocked")
	}

	sess.snapshotMu.Lock()
	generation := sess.snapshotGeneration
	sess.snapshotMu.Unlock()
	require.Equal(t, uint64(1), generation, "Serve shutdown must not create a competing terminal generation")
	require.Equal(t, int64(snapshotFinalFlushTimeout), clock.finalBudget.Load(), "Serve must spend only its shared snapshot deadline")
}

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

func TestConcurrentSessionTeardownPublishesOneFinalSnapshot(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	repository := portsmocks.NewMockSnapshotRepository(t)
	WithSnapshotRepository(repository, nil)(d)
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
	go func() { first <- d.killSession(sess, ports.ReasonSessionKilled, false) }()
	select {
	case <-publicationStarted:
	case <-time.After(testWaitTimeout):
		t.Fatal("ordinary teardown did not publish its terminal snapshot")
	}

	sess.teardownMu.Lock()
	require.True(t, sess.teardownActive)
	require.NotNil(t, sess.teardownDone)
	sess.teardownMu.Unlock()

	const duplicateCallers = 8
	duplicates := make(chan error, duplicateCallers)
	for range duplicateCallers {
		go func() {
			duplicates <- d.killSession(sess, ports.ReasonSessionKilled, false)
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
	select {
	case err := <-first:
		require.NoError(t, err)
	case <-time.After(testWaitTimeout):
		t.Fatal("ordinary teardown did not finish")
	}
	for range duplicateCallers {
		select {
		case err := <-duplicates:
			require.NoError(t, err)
		case <-time.After(testWaitTimeout):
			t.Fatal("duplicate teardown did not observe the completed registry transition")
		}
	}
	sess.snapshotMu.Lock()
	generation = sess.snapshotGeneration
	sess.snapshotMu.Unlock()
	require.Equal(t, uint64(1), generation, "completed duplicate teardowns must leave the terminal generation unchanged")
}

func TestServeShutdownCheckpointsBeforeStoppingSnapshotWorker(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d := newTestDaemon(t, newFactory(t, p), stubClock{})
	repository := portsmocks.NewMockSnapshotRepository(t)
	WithSnapshotRepository(repository, nil)(d)

	published := make(chan ports.SnapshotPublication, 2)
	allowTerminalPublication := make(chan struct{})
	repository.EXPECT().Publish(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, publication ports.SnapshotPublication) error {
		published <- publication
		if publication.Generation == 2 {
			<-allowTerminalPublication
		}
		return nil
	}).Twice()

	tr, sends, _ := newConn(t, mustHello(ports.IntentNew, "work", domain.Size{Cols: 80, Rows: 24}))
	l := serveSnapshotListener(t, tr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- d.Serve(ctx, l) }()

	awaitFrame(t, sends, ports.MsgWelcome)
	markSnapshotDirty(firstSession(d))
	d.snapshotWake <- struct{}{}
	first := <-published
	require.Equal(t, uint64(1), first.Generation)

	cancel()
	terminal := <-published
	require.Equal(t, uint64(2), terminal.Generation, "shutdown must publish a forced terminal checkpoint")
	close(allowTerminalPublication)

	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(testWaitTimeout):
		t.Fatal("Serve did not return after the terminal checkpoint completed")
	}
}

func TestServeShutdownDeadlineDoesNotWaitTwiceForUncooperativeSnapshotRepository(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	clock := newServeShutdownClock()
	d := newTestDaemon(t, newFactory(t, p), clock)
	repository := portsmocks.NewMockSnapshotRepository(t)
	WithSnapshotRepository(repository, nil)(d)
	noticeStore := portsmocks.NewMockNoticeStore(t)
	WithNoticeStore(noticeStore)(d)
	noticeStore.EXPECT().Claim().Return(nil, nil).Maybe()
	noticeStore.EXPECT().Ack().Return(nil).Maybe()
	noticeStore.EXPECT().Append(mock.MatchedBy(func(n domain.Notification) bool {
		return n.Code == domain.NoticeSnapshotWrite && n.Severity == domain.NoticeError
	})).Return(nil).Once()

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

	tr, sends, _ := newConn(t, mustHello(ports.IntentNew, "work", domain.Size{Cols: 80, Rows: 24}))
	l := serveSnapshotListener(t, tr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- d.Serve(ctx, l) }()

	awaitFrame(t, sends, ports.MsgWelcome)
	markSnapshotDirty(firstSession(d))
	d.snapshotWake <- struct{}{}
	select {
	case <-firstPublicationCompleted:
	case <-time.After(testWaitTimeout):
		t.Fatal("initial checkpoint did not complete")
	}

	cancel()
	select {
	case <-terminalPublicationStarted:
	case <-time.After(testWaitTimeout):
		t.Fatal("shutdown terminal checkpoint did not reach the worker")
	}
	d.snapshotWorkerMu.Lock()
	workerDone := d.snapshotWorkerDone
	d.snapshotWorkerMu.Unlock()
	require.NotNil(t, workerDone)
	defer func() {
		close(releaseTerminalPublication)
		select {
		case <-workerDone:
		case <-time.After(testWaitTimeout):
			t.Error("snapshot worker leaked after its repository call was released")
		}
	}()
	clock.nextFinalTimer(t).fire()

	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(testWaitTimeout):
		t.Fatal("Serve waited beyond the terminal checkpoint deadline")
	}
	require.LessOrEqual(t, clock.finalBudget.Load(), int64(snapshotFinalFlushTimeout), "shutdown must consume at most one snapshot deadline")
	require.Equal(t, uint64(1), lastValid.Load(), "the last valid checkpoint remains available after the terminal attempt times out")
}

type serveShutdownClock struct {
	finalTimers chan *snapshotDeadlineTimer
	finalBudget atomic.Int64
}

func newServeShutdownClock() *serveShutdownClock {
	return &serveShutdownClock{finalTimers: make(chan *snapshotDeadlineTimer, 2)}
}

func (*serveShutdownClock) Now() time.Time { return time.Unix(100, 0) }
func (c *serveShutdownClock) NewTimer(delay time.Duration) ports.Timer {
	if delay != snapshotFinalFlushTimeout {
		return stubTimer{}
	}
	timer := &snapshotDeadlineTimer{ch: make(chan time.Time, 1), onFire: func() {
		c.finalBudget.Add(int64(delay))
	}}
	c.finalTimers <- timer
	return timer
}

func (c *serveShutdownClock) nextFinalTimer(t *testing.T) *snapshotDeadlineTimer {
	t.Helper()
	select {
	case timer := <-c.finalTimers:
		return timer
	case <-time.After(testWaitTimeout):
		t.Fatal("shutdown checkpoint deadline timer was not created")
		return nil
	}
}

func serveSnapshotListener(t *testing.T, tr ports.Transport) *portsmocks.MockListener {
	t.Helper()
	l := portsmocks.NewMockListener(t)
	connections := make(chan ports.Transport, 1)
	connections <- tr
	closed := make(chan struct{})
	var once sync.Once
	l.EXPECT().Accept().RunAndReturn(func() (ports.Transport, error) {
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
	require.Equal(t, int32(1), publishes.Load(), "rate-limited routine work must not publish")

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

type snapshotDeadlineTimer struct {
	ch     chan time.Time
	onFire func()
}

func (t *snapshotDeadlineTimer) C() <-chan time.Time    { return t.ch }
func (*snapshotDeadlineTimer) Reset(time.Duration) bool { return true }
func (*snapshotDeadlineTimer) Stop() bool               { return true }
func (t *snapshotDeadlineTimer) fire() {
	if t.onFire != nil {
		t.onFire()
	}
	t.ch <- time.Unix(101, 0)
}

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
		published := make(chan ports.SnapshotPublication, 1)
		repository := portsmocks.NewMockSnapshotRepository(t)
		WithSnapshotRepository(repository, nil)(d)
		startSnapshotEncodeWorker(t, d)
		repository.EXPECT().Publish(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, publication ports.SnapshotPublication) error {
			published <- publication
			return nil
		}).Once()

		sess := newSnapshotTestSession(t, "immediate", false, "/work")
		sess.snapshotWake = d.snapshotWake
		sess.snapDirty.Store(true)
		d.sessions[sess.id] = sess

		d.scheduleEligibleRepositorySnapshots()
		select {
		case publication := <-published:
			require.Equal(t, "immediate", publication.Name)
		case <-time.After(testWaitTimeout):
			t.Fatal("immediately eligible snapshot was not published")
		}
		awaitSnapshotClean(t, sess)
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
	require.ElementsMatch(t, []uint64{1, 1, 2}, gotGenerations, "one forced successor bypasses the routine limit while queued work remains bounded")
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

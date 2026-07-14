package daemon

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/bnema/vev/internal/usecase/layout"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

func TestSnapshotEncodeWorkerDefersMarshalUntilAfterCapture(t *testing.T) {
	store := &channelSnapshotStore{writes: make(chan []byte, 1)}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotStore(store)(d)
	t.Cleanup(d.stopSnapshotEncodeWorker)
	d.startSnapshotEncodeWorker()

	entered := make(chan struct{})
	release := make(chan struct{})
	d.snapshotMarshal = func(s snapcodec.Session) ([]byte, error) {
		close(entered)
		<-release
		return snapcodec.Marshal(s)
	}
	sess := newSnapshotTestSession(t, "work", false, "/work")
	markSnapshotDirty(sess)
	require.True(t, d.scheduleSnapshot(sess))
	<-entered

	paneLocked := make(chan struct{})
	releasePane := make(chan struct{})
	go func() {
		p := sess.tabs[0].panes["pane-1"]
		p.mu.Lock()
		close(paneLocked)
		<-releasePane
		p.mu.Unlock()
	}()
	<-paneLocked
	close(releasePane)
	close(release)
	<-store.writes
}

func TestSnapshotEncodeWorkerDoesNotBlockPTYOrCoordinatorRender(t *testing.T) {
	exerciseBlockedSnapshotWorker(t, true)
}

func TestSnapshotStoreWorkerDoesNotBlockPTYOrCoordinatorRender(t *testing.T) {
	exerciseBlockedSnapshotWorker(t, false)
}

func exerciseBlockedSnapshotWorker(t *testing.T, blockEncode bool) {
	t.Helper()
	store := newGatedSnapshotStore()
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotStore(store)(d)
	d.startSnapshotEncodeWorker()
	t.Cleanup(func() {
		store.unblock()
		d.stopSnapshotEncodeWorker()
	})
	if blockEncode {
		d.snapshotMarshal = func(s snapcodec.Session) ([]byte, error) {
			store.enter()
			<-store.release
			return snapcodec.Marshal(s)
		}
	}

	sess := newSnapshotTestSession(t, "live", false, "/live")
	d.sessions[sess.id] = sess
	sess.tabs[0].size = domain.Size{Cols: 80, Rows: 24}
	p := sess.tabs[0].panes["pane-1"]
	tr, sends, _ := newConn(t, ports.Frame{})
	ac := &attachedClient{tr: tr, output: newOutputStateStream(), size: domain.Size{Cols: 8, Rows: 3}}
	ac.setSession(sess)
	sess.mu.Lock()
	sess.client = ac
	sess.mu.Unlock()
	rendered := make(chan struct{}, 1)
	rc := newRenderCoordinator(renderCoordinatorOptions{
		clock: stubClock{},
		onInvalidate: func(renderInvalidation) {
			d.paint(sess, ac, true, nil)
			rendered <- struct{}{}
		},
	})
	sess.installRenderCoordinator(rc)
	rc.attach(ac)
	require.True(t, rc.markAttachmentReady(rc.attachmentLease(ac)))
	t.Cleanup(func() { rc.beginSessionTeardown().finish(); rc.waitForTimerWorkers() })

	markSnapshotDirty(sess)
	require.True(t, d.scheduleSnapshot(sess))
	<-store.entered

	paneLocked := make(chan struct{})
	releasePane := make(chan struct{})
	go func() {
		p.mu.Lock()
		close(paneLocked)
		<-releasePane
		p.mu.Unlock()
	}()
	<-paneLocked
	close(releasePane)

	reader := portsmocks.NewMockPTY(t)
	var reads atomic.Int32
	reader.EXPECT().Read(mock.Anything).RunAndReturn(func(buf []byte) (int, error) {
		if reads.Add(1) == 1 {
			return copy(buf, []byte("ok")), nil
		}
		return 0, io.EOF
	}).Twice()
	reader.EXPECT().Write(mock.Anything).Return(0, nil).Maybe()
	p.mu.Lock()
	p.pty = reader
	p.onExit = func() {}
	p.mu.Unlock()
	d.sessWg.Add(1)
	go d.ptyReader(sess, sess.tabs[0], p)
	d.sessWg.Wait()
	<-rendered
	_ = awaitFrame(t, sends, ports.MsgOutput)
	p.mu.Lock()
	require.Contains(t, screenLineText(p.screen, 0), "ok")
	p.mu.Unlock()

	store.unblock()
	awaitSnapshotIdle(t, sess)
	require.True(t, sess.snapDirty.Load(), "PTY output published while blocked must remain retryable")
}

func TestFinalSnapshotFlushPersistsNewestNamedSessionBeforeWorkerTeardown(t *testing.T) {
	clock := newFinalFlushClock()
	store := &channelSnapshotStore{writes: make(chan []byte, 1)}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)
	WithSnapshotStore(store)(d)
	d.startSnapshotEncodeWorker()

	entered := make(chan struct{})
	release := make(chan struct{})
	d.snapshotMarshal = func(s snapcodec.Session) ([]byte, error) {
		close(entered)
		<-release
		return snapcodec.Marshal(s)
	}
	sess := newSnapshotTestSession(t, "final", false, "/work")
	pty, ok := sess.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
	require.True(t, ok)
	pty.EXPECT().Close().Return(nil).Once()
	sess.tabs[0].panes["pane-1"].screen.Write([]byte("\rnewest"))
	d.sessions[sess.id] = sess

	require.NoError(t, d.killSession(sess, 0, false))
	<-entered
	stopped := make(chan struct{})
	go func() {
		d.stopSnapshotEncodeWorker()
		close(stopped)
	}()
	close(release)
	<-stopped
	awaitSnapshotIdle(t, sess)

	select {
	case data := <-store.writes:
		snap, err := snapcodec.Unmarshal(data)
		require.NoError(t, err)
		frame, err := vt.UnmarshalVisible(snap.Tabs[0].Panes[0].Visible)
		require.NoError(t, err)
		require.Contains(t, rowText(frame.Row(0)), "newest")
	default:
		t.Fatal("final snapshot was dropped during worker teardown")
	}
}

func TestFinalSnapshotSupersedesPendingCaptureBeforeWorkerTeardown(t *testing.T) {
	clock := newFinalFlushClock()
	store := &channelSnapshotStore{writes: make(chan []byte, 2)}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)
	WithSnapshotStore(store)(d)
	d.startSnapshotEncodeWorker()

	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	d.snapshotMarshal = func(s snapcodec.Session) ([]byte, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return snapcodec.Marshal(s)
	}
	sess := newSnapshotTestSession(t, "pending-final", false, "/work")
	markSnapshotDirty(sess)
	require.True(t, d.scheduleSnapshot(sess))
	<-entered

	pane := sess.tabs[0].panes["pane-1"]
	pane.mu.Lock()
	pane.screen.Write([]byte("\rterminal"))
	pane.mu.Unlock()
	markSnapshotDirty(sess)
	require.True(t, d.scheduleFinalSnapshot(sess))

	stopped := make(chan struct{})
	go func() {
		d.stopSnapshotEncodeWorker()
		close(stopped)
	}()
	close(release)
	<-stopped

	require.Len(t, store.writes, 2, "teardown must flush the newer terminal capture")
	var latest snapcodec.Session
	for range 2 {
		snap, err := snapcodec.Unmarshal(<-store.writes)
		require.NoError(t, err)
		latest = snap
	}
	frame, err := vt.UnmarshalVisible(latest.Tabs[0].Panes[0].Visible)
	require.NoError(t, err)
	require.Contains(t, rowText(frame.Row(0)), "terminal")
}

func TestFinalSnapshotFallbackCoalescesRepeatedTerminalCaptures(t *testing.T) {
	store := discardSnapshotStore{}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotStore(store)(d)
	d.startSnapshotEncodeWorker()

	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWorker := func() { releaseOnce.Do(func() { close(release) }) }
	defer func() {
		releaseWorker()
		d.stopSnapshotEncodeWorker()
	}()
	var calls atomic.Int32
	d.snapshotMarshal = func(s snapcodec.Session) ([]byte, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return snapcodec.Marshal(s)
	}
	sess := newSnapshotTestSession(t, "coalesced-final", false, "/work")
	markSnapshotDirty(sess)
	require.True(t, d.scheduleSnapshot(sess))
	<-entered

	for generation := uint64(1); generation <= 32; generation++ {
		capture, ok := d.captureSnapshotState(sess, generation)
		require.True(t, ok)
		require.True(t, d.enqueueFinalSnapshotCapture(capture))
	}

	d.snapshotWorkerMu.Lock()
	captures := make([]*snapshotCapture, 0, len(d.snapshotFinalJobs))
	for _, capture := range d.snapshotFinalJobs {
		captures = append(captures, capture)
	}
	d.snapshotWorkerMu.Unlock()
	require.Len(t, captures, 1, "blocked workers retain only the latest terminal capture per session")
	require.Equal(t, uint64(32), captures[0].generation)
	releaseWorker()
}

func TestFinalSnapshotFallbackBoundsDistinctTerminalSessions(t *testing.T) {
	store := discardSnapshotStore{}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotStore(store)(d)
	d.startSnapshotEncodeWorker()

	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	d.snapshotMarshal = func(s snapcodec.Session) ([]byte, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return snapcodec.Marshal(s)
	}
	defer func() {
		close(release)
		d.stopSnapshotEncodeWorker()
	}()

	active := newSnapshotTestSession(t, "active", false, "/work")
	queued := newSnapshotTestSession(t, "queued", false, "/work")
	markSnapshotDirty(active)
	markSnapshotDirty(queued)
	require.True(t, d.scheduleSnapshot(active))
	<-entered
	require.True(t, d.scheduleSnapshot(queued), "fill the regular bounded queue before terminal retirement")

	var superseded *session
	for i := range snapshotFinalQueueCapacity {
		sess := newSnapshotTestSession(t, fmt.Sprintf("terminal-%d", i), false, "/work")
		markSnapshotDirty(sess)
		require.True(t, d.scheduleFinalSnapshot(sess))
		if i == 0 {
			superseded = sess
		}
	}
	markSnapshotDirty(superseded)
	require.True(t, d.scheduleFinalSnapshot(superseded), "a newer generation for a retained session must supersede at capacity")

	rejected := newSnapshotTestSession(t, "terminal-rejected", false, "/work")
	d.mu.Lock()
	d.sessions[rejected.id] = rejected
	d.mu.Unlock()
	markSnapshotDirty(rejected)
	err := d.killSession(rejected, ports.ReasonSessionKilled, false)
	require.ErrorContains(t, err, "snapshot worker unavailable or saturated")
	d.mu.Lock()
	require.Same(t, rejected, d.sessions[rejected.id], "rejected terminal state must remain registered for retry")
	d.mu.Unlock()

	d.snapshotWorkerMu.Lock()
	retained := len(d.snapshotFinalJobs)
	owners := len(d.snapshotFinalOrder)
	supersedingCapture := d.snapshotFinalJobs[superseded]
	d.snapshotWorkerMu.Unlock()
	require.Equal(t, snapshotFinalQueueCapacity, retained, "distinct terminal captures must have a global bound")
	require.Equal(t, snapshotFinalQueueCapacity, owners, "retention order must not retain extra session owners")
	require.NotNil(t, supersedingCapture)
	require.Equal(t, uint64(2), supersedingCapture.generation, "retained session must keep its newest terminal generation")
	require.NotNil(t, rejected)
	require.True(t, rejected.snapEligible.Load())
	require.True(t, rejected.snapDirty.Load(), "a rejected terminal capture must remain retryable")
	awaitSnapshotIdle(t, rejected)
}

func TestStopSnapshotEncodeWorkerDoesNotWaitForUncancellableWrite(t *testing.T) {
	store := newNeverReturningSnapshotStore()
	clock := newFinalFlushClock()
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)
	WithSnapshotStore(store)(d)
	d.startSnapshotEncodeWorker()

	sess := newSnapshotTestSession(t, "stuck-write", false, "/work")
	require.True(t, d.captureSession(sess))
	<-store.entered

	stopped := make(chan struct{})
	go func() {
		d.stopSnapshotEncodeWorker()
		close(stopped)
	}()
	timer := <-clock.timers
	timer.ch <- time.Time{}
	<-stopped
	awaitSnapshotIdle(t, sess)
	require.True(t, sess.snapDirty.Load(), "an abandoned write must remain retryable")
	require.NotPanics(t, func() { require.False(t, d.scheduleSnapshot(sess)) })
}

func TestRejectedSnapshotCaptureLeavesSessionRetryable(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotStore(&channelSnapshotStore{writes: make(chan []byte, 1)})(d)
	sess := newSnapshotTestSession(t, "rejected", false, "/work")
	sess.name = ""
	sess.snapEligible.Store(true)

	markSnapshotDirty(sess)
	require.False(t, d.scheduleSnapshot(sess))
	awaitSnapshotIdle(t, sess)
	require.True(t, sess.snapDirty.Load())
}

func TestStoppedSnapshotWorkerLeavesCaptureRetryable(t *testing.T) {
	store := &channelSnapshotStore{writes: make(chan []byte, 1)}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotStore(store)(d)
	d.startSnapshotEncodeWorker()
	d.stopSnapshotEncodeWorker()

	sess := newSnapshotTestSession(t, "stopped", false, "/work")
	markSnapshotDirty(sess)
	require.False(t, d.scheduleSnapshot(sess), "a stopped worker must not accept a capture")
	awaitSnapshotIdle(t, sess)
	require.True(t, sess.snapDirty.Load(), "an unsubmitted capture must remain retryable")
}

func TestSnapshotQueueSaturationLeavesCaptureRetryable(t *testing.T) {
	store := &channelSnapshotStore{writes: make(chan []byte, 3)}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotStore(store)(d)
	startSnapshotEncodeWorker(t, d)
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	d.snapshotMarshal = func(s snapcodec.Session) ([]byte, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return snapcodec.Marshal(s)
	}
	first := newSnapshotTestSession(t, "first", false, "/work")
	second := newSnapshotTestSession(t, "second", false, "/work")
	third := newSnapshotTestSession(t, "third", false, "/work")
	markSnapshotDirty(first)
	markSnapshotDirty(second)
	markSnapshotDirty(third)
	require.True(t, d.scheduleSnapshot(first))
	<-entered
	require.True(t, d.scheduleSnapshot(second), "one capture may wait in the bounded queue")
	require.False(t, d.scheduleSnapshot(third), "full queue must not block the producer")
	require.True(t, third.snapDirty.Load())

	close(release)
	awaitSnapshotClean(t, first)
	awaitSnapshotClean(t, second)
	require.True(t, d.scheduleSnapshot(third))
	<-store.writes
	awaitSnapshotClean(t, third)
}

func TestNamedFinalSnapshotSurvivesSaturatedQueue(t *testing.T) {
	for _, tc := range []struct {
		name string
		stop func(t *testing.T, d *Daemon, sess *session)
	}{
		{
			name: "kill session",
			stop: func(t *testing.T, d *Daemon, sess *session) {
				t.Helper()
				require.NoError(t, d.killSession(sess, ports.ReasonSessionKilled, false))
			},
		},
		{
			name: "shutdown all",
			stop: func(t *testing.T, d *Daemon, _ *session) {
				t.Helper()
				d.shutdownAll(ports.ReasonServerShutdown)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &channelSnapshotStore{writes: make(chan []byte, 3)}
			d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
			WithSnapshotStore(store)(d)
			startSnapshotEncodeWorker(t, d)

			entered := make(chan struct{})
			release := make(chan struct{})
			var calls atomic.Int32
			d.snapshotMarshal = func(s snapcodec.Session) ([]byte, error) {
				if calls.Add(1) == 1 {
					close(entered)
					<-release
				}
				return snapcodec.Marshal(s)
			}
			first := newSnapshotTestSession(t, "first", false, "/work")
			second := newSnapshotTestSession(t, "second", false, "/work")
			final := newSnapshotTestSession(t, "final", false, "/work")
			finalPTY, ok := final.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
			require.True(t, ok)
			finalPTY.EXPECT().Close().Return(nil).Once()
			markSnapshotDirty(first)
			markSnapshotDirty(second)
			require.True(t, d.scheduleSnapshot(first))
			<-entered
			require.True(t, d.scheduleSnapshot(second))
			d.sessions[final.id] = final

			tc.stop(t, d, final)
			require.NotContains(t, d.sessions, final.id, "final capture must survive session removal")
			close(release)

			written := make(map[string]bool, 3)
			deadline := time.NewTimer(time.Second)
			defer deadline.Stop()
			for len(written) < 3 {
				select {
				case data := <-store.writes:
					snap, err := snapcodec.Unmarshal(data)
					require.NoError(t, err)
					written[snap.Name] = true
				case <-deadline.C:
					t.Fatalf("final snapshot was dropped: wrote %v", written)
				}
			}
			require.True(t, written[final.name])
		})
	}
}

func TestSnapshotCompletionSignalsWaiters(t *testing.T) {
	store := &channelSnapshotStore{writes: make(chan []byte, 1)}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotStore(store)(d)
	startSnapshotEncodeWorker(t, d)

	entered := make(chan struct{})
	release := make(chan struct{})
	d.snapshotMarshal = func(s snapcodec.Session) ([]byte, error) {
		close(entered)
		<-release
		return snapcodec.Marshal(s)
	}
	sess := newSnapshotTestSession(t, "completion", false, "/work")
	require.True(t, d.captureSession(sess))
	<-entered

	sess.snapshotMu.Lock()
	changed := sess.snapshotChangeLocked()
	sess.snapshotMu.Unlock()
	close(release)
	<-changed
	awaitSnapshotClean(t, sess)
}

func TestSnapshotSuccessDoesNotClearNewerGeneration(t *testing.T) {
	store := &channelSnapshotStore{writes: make(chan []byte, 2)}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotStore(store)(d)
	startSnapshotEncodeWorker(t, d)
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	d.snapshotMarshal = func(s snapcodec.Session) ([]byte, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return snapcodec.Marshal(s)
	}
	sess := newSnapshotTestSession(t, "generation", false, "/work")

	markSnapshotDirty(sess)
	require.True(t, d.scheduleSnapshot(sess))
	<-entered
	markSnapshotDirty(sess)
	close(release)
	<-store.writes
	awaitSnapshotIdle(t, sess)
	require.True(t, sess.snapDirty.Load(), "a stale successful capture must not clear newer state")

	require.True(t, d.scheduleSnapshot(sess))
	<-store.writes
	awaitSnapshotClean(t, sess)
}

func TestSnapshotEncodeFailureLeavesSessionDirtyAndRetries(t *testing.T) {
	store := &channelSnapshotStore{writes: make(chan []byte, 1)}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotStore(store)(d)
	startSnapshotEncodeWorker(t, d)
	sess := newSnapshotTestSession(t, "retry-encode", false, "/work")
	failed := make(chan struct{}, 1)
	var calls atomic.Int32
	d.snapshotMarshal = func(s snapcodec.Session) ([]byte, error) {
		if calls.Add(1) == 1 {
			failed <- struct{}{}
			return nil, errors.New("encode failed")
		}
		return snapcodec.Marshal(s)
	}

	require.True(t, d.captureSession(sess))
	<-failed
	awaitSnapshotIdle(t, sess)
	require.True(t, sess.snapDirty.Load())
	require.True(t, d.scheduleSnapshot(sess))
	<-store.writes
	awaitSnapshotClean(t, sess)
}

func TestSnapshotSaverWritesDirtyNamedSessionsOnly(t *testing.T) {
	clock := newManualSnapshotClock()
	store := portsmocks.NewMockSnapshotStore(t)
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)
	WithSnapshotStore(store)(d)
	d.procCwd = func(int) (string, error) { return "/pane", nil }
	startSnapshotEncodeWorker(t, d)

	dirty := newSnapshotTestSession(t, "dirty", false, "/fallback")
	clean := newSnapshotTestSession(t, "clean", false, "/fallback")
	ephemeral := newSnapshotTestSession(t, "0", true, "/fallback")
	dirty.snapDirty.Store(true)
	ephemeral.snapDirty.Store(true)
	d.sessions[dirty.id] = dirty
	d.sessions[clean.id] = clean
	d.sessions[ephemeral.id] = ephemeral

	wrote := make(chan struct{}, 1)
	store.EXPECT().Write("dirty", mock.Anything).RunAndReturn(func(_ string, data []byte) error {
		snap, err := snapcodec.Unmarshal(data)
		require.NoError(t, err)
		require.Equal(t, "dirty", snap.Name)
		wrote <- struct{}{}
		return nil
	}).Once()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go d.snapshotSaver(ctx)
	clock.fireNext(t)
	select {
	case <-wrote:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dirty snapshot write")
	}
	awaitSnapshotClean(t, dirty)

	clock.fireNext(t)
	select {
	case <-wrote:
		t.Fatal("clean second interval wrote unexpectedly")
	case <-time.After(25 * time.Millisecond):
	}
}

func TestSnapshotCaptureExcludesFloatingRuntime(t *testing.T) {
	store := portsmocks.NewMockSnapshotStore(t)
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotStore(store)(d)
	startSnapshotEncodeWorker(t, d)
	sess := newSnapshotTestSession(t, "work", false, "/fallback")
	tb := sess.tabs[0]
	floatingPTY, _ := newBlockingPTY(t)
	floating := newPane(layout.PaneID("floating"), floatingPTY, domain.Size{Cols: 20, Rows: 8})
	tb.mu.Lock()
	generation := tb.beginFloatingWarmLocked(false)
	require.True(t, tb.installFloatingLocked(floating, generation))
	tb.mu.Unlock()

	store.EXPECT().Write("work", mock.Anything).RunAndReturn(func(_ string, data []byte) error {
		snap, err := snapcodec.Unmarshal(data)
		require.NoError(t, err)
		require.Len(t, snap.Tabs, 1)
		require.Len(t, snap.Tabs[0].Panes, 1, "floating runtime must not be persisted")
		require.Equal(t, layout.PaneID("pane-1"), snap.Tabs[0].Panes[0].ID)
		return nil
	}).Once()
	require.True(t, d.captureSession(sess))
	awaitSnapshotClean(t, sess)
}

func TestSnapshotCaptureStoresForegroundProcessMetadata(t *testing.T) {
	store := portsmocks.NewMockSnapshotStore(t)
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotStore(store)(d)
	startSnapshotEncodeWorker(t, d)
	d.procGroupArgv = func(pgid int, shellPid int) ([]string, error) {
		require.Equal(t, 4321, pgid)
		require.Equal(t, 1234, shellPid)
		return []string{"claude", "--session-id", "agent-123"}, nil
	}

	sess := newSnapshotTestSession(t, "work", false, "/fallback")
	pty := snapshotProcessPTY(t, 4321)
	sess.tabs[0].panes["pane-1"].pty = pty

	store.EXPECT().Write("work", mock.Anything).RunAndReturn(func(_ string, data []byte) error {
		snap, err := snapcodec.Unmarshal(data)
		require.NoError(t, err)
		proc := snap.Tabs[0].Panes[0].Process
		require.NotNil(t, proc)
		require.Equal(t, []string{"claude", "--session-id", "agent-123"}, proc.Argv)
		require.Equal(t, processStrategyClaude, proc.Strategy)
		require.Equal(t, "agent-123", proc.Opts.AgentSessionID)
		return nil
	}).Once()

	require.True(t, d.captureSession(sess))
	awaitSnapshotClean(t, sess)
}

func TestSnapshotCaptureProcessFailureDoesNotBlockSnapshot(t *testing.T) {
	store := portsmocks.NewMockSnapshotStore(t)
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotStore(store)(d)
	startSnapshotEncodeWorker(t, d)
	d.procGroupArgv = func(int, int) ([]string, error) { return nil, errors.New("inspect failed") }

	sess := newSnapshotTestSession(t, "work", false, "/fallback")
	pty := snapshotProcessPTY(t, 4321)
	sess.tabs[0].panes["pane-1"].pty = pty

	store.EXPECT().Write("work", mock.Anything).RunAndReturn(func(_ string, data []byte) error {
		snap, err := snapcodec.Unmarshal(data)
		require.NoError(t, err)
		require.Nil(t, snap.Tabs[0].Panes[0].Process)
		return nil
	}).Once()

	require.True(t, d.captureSession(sess))
	awaitSnapshotClean(t, sess)
}

func TestSnapshotCaptureSkipsBareShellProcess(t *testing.T) {
	store := portsmocks.NewMockSnapshotStore(t)
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotStore(store)(d)
	startSnapshotEncodeWorker(t, d)
	d.procGroupArgv = func(int, int) ([]string, error) {
		t.Fatal("bare shell should not inspect argv")
		return nil, nil
	}

	sess := newSnapshotTestSession(t, "work", false, "/fallback")
	pty := snapshotProcessPTY(t, 1234)
	sess.tabs[0].panes["pane-1"].pty = pty

	store.EXPECT().Write("work", mock.Anything).RunAndReturn(func(_ string, data []byte) error {
		snap, err := snapcodec.Unmarshal(data)
		require.NoError(t, err)
		require.Nil(t, snap.Tabs[0].Panes[0].Process)
		return nil
	}).Once()

	require.True(t, d.captureSession(sess))
	awaitSnapshotClean(t, sess)
}

func TestKillSessionSnapshotsNamedSessionBeforeClosingPanes(t *testing.T) {
	store := portsmocks.NewMockSnapshotStore(t)
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotStore(store)(d)
	startSnapshotEncodeWorker(t, d)
	d.procCwd = func(int) (string, error) { return "/live", nil }

	sess := newSnapshotTestSession(t, "work", false, "/fallback")
	d.sessions[sess.id] = sess

	var closed atomic.Bool
	store.EXPECT().Write("work", mock.Anything).RunAndReturn(func(_ string, data []byte) error {
		snap, err := snapcodec.Unmarshal(data)
		require.NoError(t, err)
		require.Equal(t, "work", snap.Name)
		require.Equal(t, "/live", snap.Tabs[0].Panes[0].Cwd)
		require.True(t, closed.Load(), "snapshot persistence must not delay pane close")
		return nil
	}).Once()
	mockPTY, ok := sess.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
	require.True(t, ok)
	mockPTY.EXPECT().Close().RunAndReturn(func() error {
		closed.Store(true)
		return nil
	}).Once()

	require.NoError(t, d.killSession(sess, 0, false))
	awaitSnapshotClean(t, sess)
}

func TestPTYReaderSynchronizedUpdateFlushMarksSnapshotDirty(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	p := portsmocks.NewMockPTY(t)
	chunks := [][]byte{[]byte("\x1b[?2026hhello\x1b[?2026l")}
	p.EXPECT().Read(mock.Anything).RunAndReturn(func(buf []byte) (int, error) {
		if len(chunks) == 0 {
			return 0, io.EOF
		}
		n := copy(buf, chunks[0])
		chunks = chunks[1:]
		return n, nil
	})
	p.EXPECT().Close().Return(nil).Maybe()

	sctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tb := newTestTabWithContext(p, sctx, cancel)
	sess := &session{id: "sync", name: "sync", tabs: []*tab{tb}, ctx: sctx, cancel: cancel}
	sess.snapEligible.Store(true)
	d.sessions[sess.id] = sess
	d.sessWg.Add(1)
	d.ptyReader(sess, tb, tb.focusedPane())

	require.True(t, sess.snapDirty.Load())
}

func TestPTYReaderSameReadSynchronizedOutputUsesUrgentCoordinatorDeadline(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	p := chunkReadPTY(t, []byte("\x1b[?2026hcomplete batch\x1b[?2026l"))
	sctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tb := newTestTabWithContext(p, sctx, cancel)
	sess := &session{id: "sync-urgent", name: "sync-urgent", tabs: []*tab{tb}, client: &attachedClient{}, ctx: sctx, cancel: cancel}
	invs := make(chan renderInvalidation, 1)
	clock := newCoordinatorMockClock(t, 2)
	sess.installRenderCoordinator(newRenderCoordinator(renderCoordinatorOptions{
		clock:        clock.clock,
		onInvalidate: func(inv renderInvalidation) { invs <- inv },
	}))
	d.sessions[sess.id] = sess
	d.sessWg.Add(1)
	d.ptyReader(sess, tb, tb.focusedPane())

	inv := awaitInvalidation(t, invs)
	require.Equal(t, invalidateUrgent, inv.class, "a complete same-read synchronized batch must flush urgently")
	timer := awaitCoordinatorScheduledTimer(t, clock)
	require.Equal(t, urgentRenderDeadline, timer.duration)
}

func TestSnapshotSaverKeepsDirtyWhenWriteFails(t *testing.T) {
	clock := newManualSnapshotClock()
	store := portsmocks.NewMockSnapshotStore(t)
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)
	WithSnapshotStore(store)(d)
	startSnapshotEncodeWorker(t, d)

	sess := newSnapshotTestSession(t, "retry", false, "/fallback")
	sess.snapDirty.Store(true)
	d.sessions[sess.id] = sess

	failed := make(chan struct{}, 1)
	store.EXPECT().Write("retry", mock.Anything).RunAndReturn(func(string, []byte) error {
		failed <- struct{}{}
		return errors.New("boom")
	}).Once()
	wrote := make(chan struct{}, 1)
	store.EXPECT().Write("retry", mock.Anything).RunAndReturn(func(string, []byte) error {
		wrote <- struct{}{}
		return nil
	}).Once()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go d.snapshotSaver(ctx)
	clock.fireNext(t)
	<-failed
	awaitSnapshotIdle(t, sess)
	require.True(t, sess.snapDirty.Load())
	clock.fireNext(t)
	select {
	case <-wrote:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for retry snapshot write")
	}
	awaitSnapshotClean(t, sess)
	require.False(t, sess.snapDirty.Load())
}

func TestKillSessionPurgeDeletesSnapshot(t *testing.T) {
	store := portsmocks.NewMockSnapshotStore(t)
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotStore(store)(d)

	sess := newSnapshotTestSession(t, "work", false, "/fallback")
	d.sessions[sess.id] = sess

	store.EXPECT().Delete("work").Return(nil).Once()
	mockPTY, ok := sess.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
	require.True(t, ok)
	mockPTY.EXPECT().Close().Return(nil).Once()

	require.NoError(t, d.killSession(sess, 0, true))
}

func TestKillSessionPurgeReturnsSnapshotDeleteError(t *testing.T) {
	store := portsmocks.NewMockSnapshotStore(t)
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotStore(store)(d)

	sess := newSnapshotTestSession(t, "work", false, "/fallback")
	d.sessions[sess.id] = sess

	store.EXPECT().Delete("work").Return(errors.New("delete failed")).Once()
	mockPTY, ok := sess.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
	require.True(t, ok)
	mockPTY.EXPECT().Close().Return(nil).Once()

	err := d.killSession(sess, 0, true)
	require.ErrorContains(t, err, "delete failed")
	require.NotContains(t, d.sessions, sess.id)
	require.NotContains(t, d.stopped, "work")
}

func TestRefreshSessionCwdMarksSnapshotDirty(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	d.procCwd = func(int) (string, error) { return "/new", nil }
	sess := newSnapshotTestSession(t, "work", false, "/old")
	d.sessions[sess.id] = sess

	d.refreshSessionCwd(sess)
	require.Equal(t, "/new", sess.cwd)
	require.True(t, sess.snapDirty.Load())
}

func TestStoppedSessionKillPurgesSnapshot(t *testing.T) {
	store := portsmocks.NewMockSnapshotStore(t)
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotStore(store)(d)
	d.stopped["work"] = stoppedSession{name: "work", cwd: "/work", createdAt: 1}

	store.EXPECT().Delete("work").Return(nil).Once()
	tr, _, _ := newConn(t, ports.Frame{Type: ports.MsgKill, Payload: ports.MarshalKill(ports.Kill{Name: "work"})})

	d.handleKill(tr, ports.Frame{Type: ports.MsgKill, Payload: ports.MarshalKill(ports.Kill{Name: "work"})})

	require.NotContains(t, d.stopped, "work")
}

func TestStoppedSessionKillSnapshotDeleteFailureKeepsMetadata(t *testing.T) {
	store := portsmocks.NewMockSnapshotStore(t)
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotStore(store)(d)
	d.stopped["work"] = stoppedSession{name: "work", cwd: "/work", createdAt: 1}

	store.EXPECT().Delete("work").Return(errors.New("delete failed")).Once()
	tr, sends, _ := newConn(t, ports.Frame{Type: ports.MsgKill, Payload: ports.MarshalKill(ports.Kill{Name: "work"})})

	d.handleKill(tr, ports.Frame{Type: ports.MsgKill, Payload: ports.MarshalKill(ports.Kill{Name: "work"})})

	require.Contains(t, d.stopped, "work")
	frame := awaitFrame(t, sends, ports.MsgError)
	msg, err := ports.UnmarshalErrorMsg(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ErrInternal, msg.Code)
}

func TestRenameSessionDeletesOldSnapshotAndMarksDirty(t *testing.T) {
	store := portsmocks.NewMockSnapshotStore(t)
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotStore(store)(d)
	sess := newSnapshotTestSession(t, "old", false, "/fallback")
	d.sessions[sess.id] = sess

	store.EXPECT().Delete("old").Return(nil).Once()
	require.NoError(t, d.renameSession(sess, "new"))

	require.True(t, sess.snapDirty.Load())
	require.Nil(t, d.findByNameLocked("old"))
}

func TestSnapshotSaverCapturesLayoutOnlyDirtySave(t *testing.T) {
	clock := newManualSnapshotClock()
	store := portsmocks.NewMockSnapshotStore(t)
	factory := portsmocks.NewMockPTYFactory(t)
	d := newTestDaemon(t, factory, clock)
	WithSnapshotStore(store)(d)
	startSnapshotEncodeWorker(t, d)

	sess := newSnapshotTestSession(t, "work", false, "/work")
	tctx, tcancel := context.WithCancel(context.Background())
	t.Cleanup(tcancel)
	sess.ctx = tctx
	sess.cancel = tcancel
	sess.tabs[0].ctx = tctx
	sess.tabs[0].cancel = tcancel
	for _, p := range sess.tabs[0].panes {
		p.ctx = tctx
		p.cancel = tcancel
	}
	sess.tabs[0].size = domain.Size{Cols: 41, Rows: 10}
	d.sessions[sess.id] = sess
	newPTY := portsmocks.NewMockPTY(t)
	newPTY.EXPECT().Pid().Return(2345).Maybe()
	newPTY.EXPECT().Read(mock.Anything).RunAndReturn(blockingRead(t)).Maybe()
	newPTY.EXPECT().Write(mock.Anything).Return(0, errors.New("unused")).Maybe()
	newPTY.EXPECT().Resize(mock.Anything).Return(nil).Maybe()
	newPTY.EXPECT().ForegroundPgid().Return(0, nil).Maybe()
	factory.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, "/work", domain.Size{Cols: 20, Rows: 10}).Return(newPTY, nil).Once()

	require.NoError(t, d.splitPane(sess, nil, layout.Right))
	require.True(t, sess.snapDirty.Load())

	wrote := make(chan struct{}, 1)
	store.EXPECT().Write("work", mock.Anything).RunAndReturn(func(_ string, data []byte) error {
		snap, err := snapcodec.Unmarshal(data)
		require.NoError(t, err)
		require.Len(t, snap.Tabs, 1)
		require.Len(t, snap.Tabs[0].Panes, 2)
		wrote <- struct{}{}
		return nil
	}).Once()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go d.snapshotSaver(ctx)
	clock.fireNext(t)
	select {
	case <-wrote:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for layout-only dirty snapshot write")
	}
}

func snapshotProcessPTY(t *testing.T, pgid int) *portsmocks.MockPTY {
	t.Helper()
	pty := portsmocks.NewMockPTY(t)
	pty.EXPECT().Pid().Return(1234).Maybe()
	pty.EXPECT().Read(mock.Anything).Return(0, errors.New("unused")).Maybe()
	pty.EXPECT().Write(mock.Anything).Return(0, errors.New("unused")).Maybe()
	pty.EXPECT().Resize(mock.Anything).Return(nil).Maybe()
	pty.EXPECT().ForegroundPgid().Return(pgid, nil).Once()
	return pty
}

func newSnapshotTestSession(t *testing.T, name string, ephemeral bool, cwd string) *session {
	t.Helper()
	pty := portsmocks.NewMockPTY(t)
	pty.EXPECT().Pid().Return(1234).Maybe()
	pty.EXPECT().Read(mock.Anything).Return(0, errors.New("unused")).Maybe()
	pty.EXPECT().Write(mock.Anything).Return(0, errors.New("unused")).Maybe()
	pty.EXPECT().Resize(mock.Anything).Return(nil).Maybe()
	pty.EXPECT().ForegroundPgid().Return(0, nil).Maybe()

	tb := newTab(pty, domain.Size{Cols: 8, Rows: 3})
	p := tb.panes["pane-1"]
	p.screen.Write([]byte("hello"))
	p.history.Append([]renderer.Cell{{Rune: 'h'}, {Rune: 'i'}})
	sess := &session{
		id:        domain.SessionID("sess-" + name),
		name:      name,
		ephemeral: ephemeral,
		ctx:       context.Background(),
		cancel:    func() {},
		tabs:      []*tab{tb},
		active:    0,
		cwd:       cwd,
		createdAt: 42,
	}
	sess.snapEligible.Store(!ephemeral && name != "")
	return sess
}

func awaitSnapshotIdle(t testing.TB, sess *session) {
	t.Helper()
	for {
		sess.snapshotMu.Lock()
		pending := sess.snapshotPending
		changed := sess.snapshotChangeLocked()
		sess.snapshotMu.Unlock()
		if !pending {
			return
		}
		<-changed
	}
}

func awaitSnapshotClean(t *testing.T, sess *session) {
	t.Helper()
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

func startSnapshotEncodeWorker(t *testing.T, d *Daemon) {
	t.Helper()
	d.startSnapshotEncodeWorker()
	t.Cleanup(d.stopSnapshotEncodeWorker)
}

type discardSnapshotStore struct{}

func (discardSnapshotStore) Write(string, []byte) error          { return nil }
func (discardSnapshotStore) Load() ([]ports.SnapshotBlob, error) { return nil, nil }
func (discardSnapshotStore) Delete(string) error                 { return nil }

type gatedSnapshotStore struct {
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
	releaseOnce sync.Once
}

func newGatedSnapshotStore() *gatedSnapshotStore {
	return &gatedSnapshotStore{entered: make(chan struct{}), release: make(chan struct{})}
}

func (s *gatedSnapshotStore) enter()   { s.once.Do(func() { close(s.entered) }) }
func (s *gatedSnapshotStore) unblock() { s.releaseOnce.Do(func() { close(s.release) }) }

func (s *gatedSnapshotStore) Write(_ string, _ []byte) error {
	s.enter()
	<-s.release
	return nil
}
func (*gatedSnapshotStore) Load() ([]ports.SnapshotBlob, error) { return nil, nil }
func (*gatedSnapshotStore) Delete(string) error                 { return nil }

type neverReturningSnapshotStore struct {
	entered chan struct{}
	once    sync.Once
	block   chan struct{}
}

func newNeverReturningSnapshotStore() *neverReturningSnapshotStore {
	return &neverReturningSnapshotStore{entered: make(chan struct{}), block: make(chan struct{})}
}

func (s *neverReturningSnapshotStore) Write(string, []byte) error {
	s.once.Do(func() { close(s.entered) })
	<-s.block
	return nil
}
func (*neverReturningSnapshotStore) Load() ([]ports.SnapshotBlob, error) { return nil, nil }
func (*neverReturningSnapshotStore) Delete(string) error                 { return nil }

type channelSnapshotStore struct {
	writes chan []byte
}

func (s *channelSnapshotStore) Write(_ string, data []byte) error {
	s.writes <- append([]byte(nil), data...)
	return nil
}
func (*channelSnapshotStore) Load() ([]ports.SnapshotBlob, error) { return nil, nil }
func (*channelSnapshotStore) Delete(string) error                 { return nil }

type manualSnapshotClock struct {
	mu     sync.Mutex
	timers []*manualSnapshotTimer
}

func newManualSnapshotClock() *manualSnapshotClock { return &manualSnapshotClock{} }

func (c *manualSnapshotClock) Now() time.Time { return time.Unix(0, 0) }

func (c *manualSnapshotClock) NewTimer(time.Duration) ports.Timer {
	t := &manualSnapshotTimer{ch: make(chan time.Time, 1)}
	c.mu.Lock()
	c.timers = append(c.timers, t)
	c.mu.Unlock()
	return t
}

func (c *manualSnapshotClock) fireNext(t *testing.T) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		c.mu.Lock()
		if len(c.timers) > 0 {
			timer := c.timers[0]
			c.mu.Unlock()
			timer.ch <- time.Unix(0, 0)
			return
		}
		c.mu.Unlock()
		select {
		case <-deadline:
			t.Fatal("timed out waiting for snapshot timer")
		case <-time.After(time.Millisecond):
		}
	}
}

type finalFlushClock struct{ timers chan *manualSnapshotTimer }

func newFinalFlushClock() *finalFlushClock {
	return &finalFlushClock{timers: make(chan *manualSnapshotTimer, 1)}
}

func (c *finalFlushClock) Now() time.Time { return time.Unix(0, 0) }
func (c *finalFlushClock) NewTimer(time.Duration) ports.Timer {
	t := &manualSnapshotTimer{ch: make(chan time.Time, 1)}
	c.timers <- t
	return t
}

type manualSnapshotTimer struct{ ch chan time.Time }

func (t *manualSnapshotTimer) C() <-chan time.Time      { return t.ch }
func (t *manualSnapshotTimer) Reset(time.Duration) bool { return true }
func (t *manualSnapshotTimer) Stop() bool               { return true }

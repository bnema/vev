package daemon

import (
	"context"
	"errors"
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
)

func TestSnapshotSaverWritesDirtyNamedSessionsOnly(t *testing.T) {
	clock := newManualSnapshotClock()
	store := portsmocks.NewMockSnapshotStore(t)
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)
	WithSnapshotStore(store)(d)
	d.procCwd = func(int) (string, error) { return "/pane", nil }

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

	clock.fireNext(t)
	select {
	case <-wrote:
		t.Fatal("clean second interval wrote unexpectedly")
	case <-time.After(25 * time.Millisecond):
	}
}

func TestKillSessionSnapshotsNamedSessionBeforeClosingPanes(t *testing.T) {
	store := portsmocks.NewMockSnapshotStore(t)
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotStore(store)(d)
	d.procCwd = func(int) (string, error) { return "/live", nil }

	sess := newSnapshotTestSession(t, "work", false, "/fallback")
	d.sessions[sess.id] = sess

	var wrote atomic.Bool
	store.EXPECT().Write("work", mock.Anything).RunAndReturn(func(_ string, data []byte) error {
		snap, err := snapcodec.Unmarshal(data)
		require.NoError(t, err)
		require.Equal(t, "work", snap.Name)
		require.Equal(t, "/live", snap.Tabs[0].Panes[0].Cwd)
		wrote.Store(true)
		return nil
	}).Once()
	mockPTY, ok := sess.tabs[0].panes["pane-1"].pty.(*portsmocks.MockPTY)
	require.True(t, ok)
	mockPTY.EXPECT().Close().RunAndReturn(func() error {
		require.True(t, wrote.Load(), "snapshot Write must happen before pane Close")
		return nil
	}).Once()

	require.NoError(t, d.killSession(sess, 0, false))
	require.True(t, wrote.Load())
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

func TestSnapshotSaverKeepsDirtyWhenWriteFails(t *testing.T) {
	clock := newManualSnapshotClock()
	store := portsmocks.NewMockSnapshotStore(t)
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), clock)
	WithSnapshotStore(store)(d)

	sess := newSnapshotTestSession(t, "retry", false, "/fallback")
	sess.snapDirty.Store(true)
	d.sessions[sess.id] = sess

	store.EXPECT().Write("retry", mock.Anything).Return(errors.New("boom")).Once()
	wrote := make(chan struct{}, 1)
	store.EXPECT().Write("retry", mock.Anything).RunAndReturn(func(string, []byte) error {
		wrote <- struct{}{}
		return nil
	}).Once()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go d.snapshotSaver(ctx)
	clock.fireNext(t)
	require.Eventually(t, func() bool { return sess.snapDirty.Load() }, time.Second, time.Millisecond)
	clock.fireNext(t)
	select {
	case <-wrote:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for retry snapshot write")
	}
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
	factory.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, "/work", domain.Size{Cols: 20, Rows: 10}).Return(newPTY, nil).Once()

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
	p.scrollback.Append([]renderer.Cell{{Rune: 'h'}, {Rune: 'i'}})
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

type manualSnapshotTimer struct{ ch chan time.Time }

func (t *manualSnapshotTimer) C() <-chan time.Time      { return t.ch }
func (t *manualSnapshotTimer) Reset(time.Duration) bool { return true }
func (t *manualSnapshotTimer) Stop() bool               { return true }

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
	"github.com/bnema/vev/internal/protocol"
)

type contextAwareFloatingPTY struct {
	ctx           context.Context
	readerStarted chan struct{}
	closed        chan struct{}
	readerOnce    sync.Once
	closeOnce     sync.Once
	closeCount    int
}

func newContextAwareFloatingPTY() *contextAwareFloatingPTY {
	return &contextAwareFloatingPTY{
		readerStarted: make(chan struct{}),
		closed:        make(chan struct{}),
	}
}

func (p *contextAwareFloatingPTY) Read([]byte) (int, error) {
	p.readerOnce.Do(func() { close(p.readerStarted) })
	<-p.ctx.Done()
	return 0, p.ctx.Err()
}

func (*contextAwareFloatingPTY) Write(b []byte) (int, error)  { return len(b), nil }
func (*contextAwareFloatingPTY) Resize(domain.Geometry) error { return nil }
func (*contextAwareFloatingPTY) Pid() int                     { return 1 }
func (*contextAwareFloatingPTY) ForegroundPgid() (int, error) {
	return 1, nil
}

func (p *contextAwareFloatingPTY) Close() error {
	p.closeOnce.Do(func() {
		p.closeCount++
		close(p.closed)
	})
	return nil
}

type contextAwareFloatingFactory struct {
	pty    *contextAwareFloatingPTY
	opened chan context.Context
	err    error
	onOpen func()
}

func (f *contextAwareFloatingFactory) Open(ctx context.Context, _ string, _ []string, _ []string, _ string, _ domain.Geometry) (ports.PTY, error) {
	if f.pty != nil {
		f.pty.ctx = ctx
	}
	f.opened <- ctx
	if f.onOpen != nil {
		f.onOpen()
	}
	return f.pty, f.err
}

func TestFloatingSuccessfulOpenTransfersContextOwnershipToPane(t *testing.T) {
	teardowns := []struct {
		name           string
		desiredVisible bool
		teardown       func(*Daemon, *session, *tab)
	}{
		{
			name: "hidden floating pane",
			teardown: func(d *Daemon, _ *session, tb *tab) {
				d.teardownFloating(tb, nil)
			},
		},
		{
			name:           "visible tab",
			desiredVisible: true,
			teardown: func(_ *Daemon, _ *session, tb *tab) {
				tb.closeAllPanes()
			},
		},
		{
			name:           "visible session",
			desiredVisible: true,
			teardown: func(d *Daemon, sess *session, _ *tab) {
				require.NoError(t, d.killSession(sess, protocol.ReasonSessionKilled, false))
			},
		},
	}
	for _, tt := range teardowns {
		t.Run(tt.name, func(t *testing.T) {
			pty := newContextAwareFloatingPTY()
			factory := &contextAwareFloatingFactory{pty: pty, opened: make(chan context.Context, 1)}
			d := newTestDaemon(t, factory, stubClock{})
			tb := newFloatingTestTab(t)
			tabCtx, cancelTab := context.WithCancel(t.Context())
			tb.ctx, tb.cancel = tabCtx, cancelTab
			sessCtx, cancelSession := context.WithCancel(t.Context())
			sess := &session{sessionCore: sessionCore{id: "context-" + domain.SessionID(tt.name), name: "work"}, tabs: []*tab{tb}, ctx: sessCtx, cancel: cancelSession}
			d.mu.Lock()
			d.sessions[sess.id] = sess
			d.mu.Unlock()
			tb.mu.Lock()
			generation := tb.beginFloatingWarmLocked(tt.desiredVisible)
			tb.mu.Unlock()

			d.openAndInstallFloating(sess, tb, floatingLaunchSpec{
				sessionName:  "work",
				size:         domain.Size{Cols: 20, Rows: 8},
				geometry:     floatingGeometry{Inner: domain.Rect{Width: 20, Height: 8}},
				paneStableID: "p_floating",
				parentCtx:    tabCtx,
			}, generation)

			openCtx := <-factory.opened
			require.NoError(t, openCtx.Err(), "the PTY context must outlive the launch worker")
			tb.mu.Lock()
			floating := tb.floating.pane
			tb.mu.Unlock()
			require.NotNil(t, floating)
			sess.mu.Lock()
			sess.registerAttachmentLocked(&attachedClient{})
			sess.mu.Unlock()
			require.Equal(t, tt.desiredVisible, d.paneRenderable(sess, tb, floating), "installed popup renderability must follow its retained visibility")
			sess.mu.Lock()
			clearAttachmentsForTestLocked(sess)
			sess.mu.Unlock()
			select {
			case <-pty.readerStarted:
			case <-time.After(time.Second):
				t.Fatal("floating reader did not start")
			}

			tt.teardown(d, sess, tb)
			select {
			case <-openCtx.Done():
			case <-time.After(time.Second):
				t.Fatal("floating PTY context was not cancelled by teardown")
			}
			select {
			case <-pty.closed:
			case <-time.After(time.Second):
				t.Fatal("floating PTY was not closed by teardown")
			}
			d.sessWg.Wait()
			require.Equal(t, 1, pty.closeCount, "floating PTY must close exactly once")
		})
	}
}

type failedOpenPTY struct {
	ctx          context.Context
	readerStarts atomic.Int32
	closes       atomic.Int32
	closed       chan struct{}
	closeOnce    sync.Once
	onClose      func()
}

func newFailedOpenPTY() *failedOpenPTY {
	return &failedOpenPTY{closed: make(chan struct{})}
}

func (p *failedOpenPTY) Read([]byte) (int, error) {
	p.readerStarts.Add(1)
	select {
	case <-p.ctx.Done():
		return 0, p.ctx.Err()
	case <-p.closed:
		return 0, io.EOF
	}
}

func (*failedOpenPTY) Write(b []byte) (int, error)  { return len(b), nil }
func (*failedOpenPTY) Resize(domain.Geometry) error { return nil }
func (*failedOpenPTY) Pid() int                     { return 1 }
func (*failedOpenPTY) ForegroundPgid() (int, error) {
	return 1, nil
}

func (p *failedOpenPTY) Close() error {
	p.closes.Add(1)
	if p.onClose != nil {
		p.onClose()
	}
	p.closeOnce.Do(func() { close(p.closed) })
	return nil
}

func TestFloatingOpenErrorReleasesReturnedPTYBeforePublishingFailure(t *testing.T) {
	tests := []struct {
		name     string
		userOpen bool
	}{
		{name: "user open", userOpen: true},
		{name: "background prewarm", userOpen: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cause := io.ErrUnexpectedEOF
			pty := newFailedOpenPTY()
			factory := portsmocks.NewMockPTYFactory(t)
			cancelCallbacks := atomic.Int32{}
			cancelled := make(chan struct{})
			factory.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
				func(ctx context.Context, _ string, _ []string, _ []string, _ string, _ domain.Geometry) (ports.PTY, error) {
					pty.ctx = ctx
					context.AfterFunc(ctx, func() {
						cancelCallbacks.Add(1)
						close(cancelled)
					})
					return pty, cause
				}).Once()
			d := newTestDaemon(t, factory, stubClock{})
			tb := newFloatingTestTab(t)
			tr, _ := newCapturingTransport(t)
			ac := &attachedClient{tr: tr, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
			ac.initOverlays()
			sessCtx, cancelSession := context.WithCancel(t.Context())
			sess := &session{sessionCore: sessionCore{id: "floating-open-error", name: "work", attachments: map[*attachedClient]struct{}{ac: {}}}, tabs: []*tab{tb}, ctx: sessCtx, cancel: cancelSession}
			ac.setSession(sess)

			// Close must be safe to run before failure publication and without
			// either ownership lock held.
			var unpublishedAtClose atomic.Bool
			pty.onClose = func() {
				tb.mu.Lock()
				launchUnpublished := tb.floating.pane == nil && tb.floating.state == floatingWarming
				tb.mu.Unlock()
				sess.mu.Lock()
				sessionName := sess.name
				sess.mu.Unlock()
				unpublishedAtClose.Store(launchUnpublished && sessionName == "work")
			}

			d.startFloating(sess, tb, tt.userOpen)
			d.sessWg.Wait()
			select {
			case <-cancelled:
			case <-time.After(time.Second):
				t.Fatal("failed floating Open process context was not cancelled")
			}

			// Exercise later cancellation/stale cleanup paths; neither may find
			// ownership to release a second time.
			cancelSession()
			d.teardownFloating(tb, ac)
			require.Equal(t, int32(1), cancelCallbacks.Load(), "process context must be released exactly once")
			require.Equal(t, int32(1), pty.closes.Load(), "returned PTY must be closed exactly once")
			require.True(t, unpublishedAtClose.Load(), "PTY must close before any failure publication")
			require.Zero(t, pty.readerStarts.Load(), "failed Open must not start a reader")

			tb.mu.Lock()
			floating := tb.floating.pane
			publishedFailedPTY := false
			for _, published := range tb.panes {
				publishedFailedPTY = publishedFailedPTY || published.pty == pty
			}
			tb.mu.Unlock()
			require.Nil(t, floating, "failed Open must not publish the floating slot")
			require.False(t, publishedFailedPTY, "failed Open must not publish a pane or owner")
			if tt.userOpen {
				toasts := awaitToastCount(t, ac, 1)
				require.Equal(t, domain.NoticeFloatingSpawn, toasts[0].Code)
				require.Equal(t, "couldn't open floating pane: command failed to start", toasts[0].Message)
			} else {
				toasts, _ := visibleToasts(ac)
				require.Empty(t, toasts, "background prewarm failure must stay silent")
			}
		})
	}
}

func TestInstallFloatingRejectsNilPublicationInputs(t *testing.T) {
	tests := []struct {
		name       string
		nilSession bool
		nilTab     bool
		nilPane    bool
		wantCloses int
	}{
		{name: "nil session", nilSession: true, wantCloses: 1},
		{name: "nil tab", nilTab: true, wantCloses: 1},
		{name: "nil pane", nilPane: true},
		{name: "all nil", nilSession: true, nilTab: true, nilPane: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &Daemon{}
			sess := &session{}
			tb := newFloatingTestTab(t)
			tb.mu.Lock()
			generation := tb.beginFloatingWarmLocked(true)
			tb.mu.Unlock()
			pty := newContextAwareFloatingPTY()
			p := newPane("floating", pty, domain.Size{Cols: 20, Rows: 8})

			if tt.nilSession {
				sess = nil
			}
			if tt.nilTab {
				tb = nil
			}
			if tt.nilPane {
				p = nil
			}

			require.NotPanics(t, func() { d.installFloating(sess, tb, p, generation) })
			require.Equal(t, tt.wantCloses, pty.closeCount)
		})
	}
}

func TestFloatingFailedAndStaleOpenCancelContext(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*tab, *contextAwareFloatingFactory)
	}{
		{
			name: "failed Open",
			setup: func(_ *tab, factory *contextAwareFloatingFactory) {
				factory.err = context.Canceled
			},
		},
		{
			name: "stale install",
			setup: func(tb *tab, factory *contextAwareFloatingFactory) {
				factory.onOpen = func() {
					tb.mu.Lock()
					tb.takeFloatingLocked()
					tb.mu.Unlock()
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pty := newContextAwareFloatingPTY()
			factory := &contextAwareFloatingFactory{pty: pty, opened: make(chan context.Context, 1)}
			d := newTestDaemon(t, factory, stubClock{})
			tb := newFloatingTestTab(t)
			sess := &session{sessionCore: sessionCore{name: "work"}, tabs: []*tab{tb}, ctx: t.Context()}
			tb.mu.Lock()
			generation := tb.beginFloatingWarmLocked(true)
			tb.mu.Unlock()
			tt.setup(tb, factory)

			d.openAndInstallFloating(sess, tb, floatingLaunchSpec{
				sessionName:  "work",
				size:         domain.Size{Cols: 20, Rows: 8},
				geometry:     floatingGeometry{Inner: domain.Rect{Width: 20, Height: 8}},
				paneStableID: "p_floating",
				parentCtx:    tb.ctx,
			}, generation)

			openCtx := <-factory.opened
			select {
			case <-openCtx.Done():
			case <-time.After(time.Second):
				t.Fatal("failed or stale floating Open retained its context")
			}
			tb.mu.Lock()
			require.Nil(t, tb.floating.pane)
			tb.mu.Unlock()
			if tt.name == "stale install" {
				select {
				case <-pty.closed:
				case <-time.After(time.Second):
					t.Fatal("stale floating PTY was not closed")
				}
				require.Equal(t, 1, pty.closeCount)
			}
		})
	}
}

func TestFloatingOpenCancellationBoundsSessionTeardownAndClosesLatePTY(t *testing.T) {
	factory := portsmocks.NewMockPTYFactory(t)
	opened := make(chan struct{})
	latePTY := portsmocks.NewMockPTY(t)
	closed := make(chan struct{})
	latePTY.EXPECT().Close().RunAndReturn(func() error { close(closed); return nil }).Once()
	factory.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, _ string, _ []string, _ []string, _ string, _ domain.Geometry) (ports.PTY, error) {
			close(opened)
			<-ctx.Done()
			return latePTY, nil
		}).Once()
	d := newTestDaemon(t, factory, stubClock{})
	tb := newFloatingTestTab(t)
	ctx, cancel := context.WithCancel(t.Context())
	sess := &session{sessionCore: sessionCore{id: "blocked-open", name: "work"}, tabs: []*tab{tb}, ctx: ctx, cancel: cancel}
	d.mu.Lock()
	d.sessions[sess.id] = sess
	d.mu.Unlock()
	d.startFloating(sess, tb, true)
	select {
	case <-opened:
	case <-time.After(time.Second):
		t.Fatal("floating Open did not begin")
	}

	killed := make(chan error, 1)
	go func() { killed <- d.killSession(sess, protocol.ReasonSessionKilled, false) }()
	select {
	case err := <-killed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("session teardown waited for blocked Open")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("late Open PTY was not closed")
	}
	tb.mu.Lock()
	require.Nil(t, tb.floating.pane, "late PTY must not be installed")
	tb.mu.Unlock()
}

func TestFloatingOpensFromUnrelatedSessionsAreConcurrent(t *testing.T) {
	factory := portsmocks.NewMockPTYFactory(t)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var once sync.Once
	factory.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _ string, _ []string, _ []string, _ string, _ domain.Geometry) (ports.PTY, error) {
			entered <- struct{}{}
			<-release
			return newQuietPTY(), nil
		}).Twice()
	d := newTestDaemon(t, factory, stubClock{})
	tabs := make([]*tab, 0, 2)
	cancels := make([]context.CancelFunc, 0, 2)
	for _, id := range []domain.SessionID{"one", "two"} {
		tb := newFloatingTestTab(t)
		ctx, cancel := context.WithCancel(t.Context())
		sess := &session{sessionCore: sessionCore{id: id, name: string(id)}, tabs: []*tab{tb}, ctx: ctx, cancel: cancel}
		tabs = append(tabs, tb)
		cancels = append(cancels, cancel)
		d.startFloating(sess, tb, false)
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("unrelated floating launch was serialized")
		}
	}
	once.Do(func() { close(release) })
	for i, cancel := range cancels {
		cancel()
		d.teardownFloating(tabs[i], nil)
	}
	d.sessWg.Wait()
}

package daemon

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/domain/layout"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

// newLifetimePTYFactory returns a factory whose PTYs read until their Open
// context ends or they are closed.
func newLifetimePTYFactory(t testing.TB) *portsmocks.MockPTYFactory {
	t.Helper()
	factory := portsmocks.NewMockPTYFactory(t)
	factory.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, _ string, _ []string, _ []string, _ string, _ domain.Geometry) (ports.PTY, error) {
			closed := make(chan struct{})
			var once sync.Once
			p := portsmocks.NewMockPTY(t)
			p.EXPECT().Read(mock.Anything).RunAndReturn(func([]byte) (int, error) {
				select {
				case <-ctx.Done():
					return 0, ctx.Err()
				case <-closed:
					return 0, io.EOF
				}
			}).Maybe()
			p.EXPECT().Write(mock.Anything).RunAndReturn(func(b []byte) (int, error) { return len(b), nil }).Maybe()
			p.EXPECT().Resize(mock.Anything).Return(nil).Maybe()
			p.EXPECT().Pid().Return(1).Maybe()
			p.EXPECT().ForegroundPgid().Return(1, nil).Maybe()
			p.EXPECT().Close().RunAndReturn(func() error { once.Do(func() { close(closed) }); return nil }).Maybe()
			return p, nil
		}).Maybe()
	return factory
}

func TestPaneOpenErrorsReleaseReturnedPTYWithoutPublication(t *testing.T) {
	type openCase struct {
		name     string
		wantCode domain.NoticeCode
		run      func(*testing.T, *Daemon) ([]*tab, func(), error)
	}
	tests := []openCase{
		{
			name:     "initial pane",
			wantCode: domain.NoticeSessionSpawn,
			run: func(_ *testing.T, d *Daemon) ([]*tab, func(), error) {
				sess, err := createSessionForTest(d, "work", true, "/tmp", domain.Size{Cols: 80, Rows: 24}, terminalEnv{}, nil)
				if sess != nil {
					return sess.tabs, func() { closeTabs(sess.tabs) }, err
				}
				return nil, d.serveCancel, err
			},
		},
		{
			name:     "created tab",
			wantCode: domain.NoticeTabSpawn,
			run: func(t *testing.T, d *Daemon) ([]*tab, func(), error) {
				sessCtx, cancelSession := context.WithCancel(t.Context())
				base := newTab(newQuietPTY(t), domain.Size{Cols: 80, Rows: 22})
				base.ctx, base.cancel = context.WithCancel(sessCtx)
				sess := &session{sessionCore: sessionCore{id: "work", name: "work"}, ctx: sessCtx, cancel: cancelSession, tabs: []*tab{base}}
				d.mu.Lock()
				d.sessions[sess.id] = sess
				d.mu.Unlock()
				err := d.createTab(sess, domain.Size{Cols: 80, Rows: 24})
				return sess.tabs, func() {
					cancelSession()
					closeTabs(sess.tabs)
				}, err
			},
		},
		{
			name:     "split pane",
			wantCode: domain.NoticePaneSpawn,
			run: func(t *testing.T, d *Daemon) ([]*tab, func(), error) {
				sessCtx, cancelSession := context.WithCancel(t.Context())
				tb := newTab(newQuietPTY(t), domain.Size{Cols: 41, Rows: 10})
				tb.ctx, tb.cancel = context.WithCancel(sessCtx)
				sess := &session{sessionCore: sessionCore{id: "work", name: "work"}, cwd: "/tmp", ctx: sessCtx, cancel: cancelSession, tabs: []*tab{tb}}
				_, err := d.splitPaneAt(sess, tb, tb.focusedPane(), layout.Right)
				return []*tab{tb}, func() {
					cancelSession()
					tb.closeAllPanes()
				}, err
			},
		},
		{
			name: "restored pane",
			run: func(t *testing.T, d *Daemon) ([]*tab, func(), error) {
				restoreCtx, cancelRestore := context.WithCancel(t.Context())
				sessCtx, cancelSession := context.WithCancel(t.Context())
				tabs, err := d.restoreSnapshotTabs(restoreCtx, sessCtx, restoreAcceptanceSession(t, "work"))
				return tabs, func() {
					cancelRestore()
					cancelSession()
					closeRestoredTabs(tabs)
				}, err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cause := errors.New("open failed")
				ptyMock, pty := newFailedOpenPTY(t)
				cancelCallbacks := atomic.Int32{}
				cancelled := make(chan struct{})
				openFactory := portsmocks.NewMockPTYFactory(t)
				openFactory.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
					func(ctx context.Context, _ string, _ []string, _ []string, _ string, _ domain.Geometry) (ports.PTY, error) {
						pty.ctx = ctx
						context.AfterFunc(ctx, func() {
							cancelCallbacks.Add(1)
							close(cancelled)
						})
						return ptyMock, cause
					}).Maybe()
				d := newTestDaemon(t, openFactory, newStubClock(t))

				tabs, cleanup, err := tt.run(t, d)
				require.ErrorIs(t, err, cause)
				if tt.wantCode != 0 {
					var userErr *domain.UserError
					require.ErrorAs(t, err, &userErr)
					require.Equal(t, tt.wantCode, userErr.Code)
				}
				requireReceived(t, cancelled, "failed pane Open process context was not cancelled")

				for _, tb := range tabs {
					if tb == nil {
						continue
					}
					tb.mu.Lock()
					floating := tb.floating.pane
					var floatingPTY ports.PTY
					if floating != nil {
						floating.mu.Lock()
						floatingPTY = floating.pty
						floating.mu.Unlock()
					}
					publishedFailedPTY := false
					for _, published := range tb.panes {
						publishedFailedPTY = publishedFailedPTY || published.pty == ptyMock
					}
					tb.mu.Unlock()
					require.NotEqual(t, ptyMock, floatingPTY)
					require.False(t, publishedFailedPTY, "failed Open must not publish a pane or owner")
				}
				require.Zero(t, pty.readerStarts.Load(), "failed Open must not start a reader")
				require.Equal(t, int32(1), pty.closes.Load(), "failed Open PTY must close exactly once")
				require.Equal(t, int32(1), cancelCallbacks.Load(), "failed Open context must cancel exactly once")

				cleanup()
				require.Equal(t, int32(1), pty.closes.Load(), "later teardown must not close an unpublished PTY again")
				require.Equal(t, int32(1), cancelCallbacks.Load(), "later teardown must not recancel an unpublished process")
			})
		})
	}
}

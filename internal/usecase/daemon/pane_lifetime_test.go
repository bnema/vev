package daemon

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
)

type lifetimePTY struct {
	ctx       context.Context
	closed    chan struct{}
	closeOnce sync.Once
	closes    atomic.Int32
}

func newLifetimePTY() *lifetimePTY {
	return &lifetimePTY{closed: make(chan struct{})}
}

func (p *lifetimePTY) Read([]byte) (int, error) {
	select {
	case <-p.ctx.Done():
		return 0, p.ctx.Err()
	case <-p.closed:
		return 0, io.EOF
	}
}

func (*lifetimePTY) Write(b []byte) (int, error)  { return len(b), nil }
func (*lifetimePTY) Resize(domain.Geometry) error { return nil }
func (*lifetimePTY) Pid() int                     { return 1 }
func (*lifetimePTY) ForegroundPgid() (int, error) {
	return 1, nil
}
func (p *lifetimePTY) Close() error {
	p.closes.Add(1)
	p.closeOnce.Do(func() { close(p.closed) })
	return nil
}

type lifetimePTYFactory struct {
	mu       sync.Mutex
	ptys     []*lifetimePTY
	contexts []context.Context
}

func (f *lifetimePTYFactory) Open(ctx context.Context, _ string, _ []string, _ []string, _ string, _ domain.Geometry) (ports.PTY, error) {
	p := newLifetimePTY()
	p.ctx = ctx
	f.mu.Lock()
	f.ptys = append(f.ptys, p)
	f.contexts = append(f.contexts, ctx)
	f.mu.Unlock()
	return p, nil
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
				base := newTab(newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
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
				tb := newTab(newQuietPTY(), domain.Size{Cols: 41, Rows: 10})
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
			cause := errors.New("open failed")
			pty := newFailedOpenPTY()
			cancelCallbacks := atomic.Int32{}
			cancelled := make(chan struct{})
			openFactory := paneErrorFactory{
				pty: pty,
				err: cause,
				onOpen: func(ctx context.Context) {
					pty.ctx = ctx
					context.AfterFunc(ctx, func() {
						cancelCallbacks.Add(1)
						close(cancelled)
					})
				},
			}
			d := newTestDaemon(t, &openFactory, stubClock{})

			tabs, cleanup, err := tt.run(t, d)
			require.ErrorIs(t, err, cause)
			if tt.wantCode != 0 {
				var userErr *domain.UserError
				require.ErrorAs(t, err, &userErr)
				require.Equal(t, tt.wantCode, userErr.Code)
			}
			select {
			case <-cancelled:
			case <-time.After(time.Second):
				t.Fatal("failed pane Open process context was not cancelled")
			}

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
					publishedFailedPTY = publishedFailedPTY || published.pty == pty
				}
				tb.mu.Unlock()
				require.NotEqual(t, pty, floatingPTY)
				require.False(t, publishedFailedPTY, "failed Open must not publish a pane or owner")
			}
			require.Zero(t, pty.readerStarts.Load(), "failed Open must not start a reader")
			require.Equal(t, int32(1), pty.closes.Load(), "failed Open PTY must close exactly once")
			require.Equal(t, int32(1), cancelCallbacks.Load(), "failed Open context must cancel exactly once")

			cleanup()
			require.Equal(t, int32(1), pty.closes.Load(), "later teardown must not close an unpublished PTY again")
			require.Equal(t, int32(1), cancelCallbacks.Load(), "later teardown must not recancel an unpublished process")
		})
	}
}

type paneErrorFactory struct {
	pty    ports.PTY
	err    error
	onOpen func(context.Context)
}

func (f *paneErrorFactory) Open(ctx context.Context, _ string, _ []string, _ []string, _ string, _ domain.Geometry) (ports.PTY, error) {
	if f.onOpen != nil {
		f.onOpen(ctx)
	}
	return f.pty, f.err
}

package daemon

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// spareOpen records one PTY Open issued for a floating shell.
type spareOpen struct {
	cwd      string
	geometry domain.Geometry
	pty      *sparePTY
}

type sparePTY struct {
	ctx     context.Context
	mu      sync.Mutex
	resizes []domain.Geometry
	closed  chan struct{}
	once    sync.Once
}

func (p *sparePTY) Read([]byte) (int, error) {
	select {
	case <-p.ctx.Done():
		return 0, p.ctx.Err()
	case <-p.closed:
		return 0, io.EOF
	}
}
func (*sparePTY) Write(b []byte) (int, error) { return len(b), nil }
func (p *sparePTY) Resize(g domain.Geometry) error {
	p.mu.Lock()
	p.resizes = append(p.resizes, g)
	p.mu.Unlock()
	return nil
}
func (*sparePTY) Pid() int                     { return 1 }
func (*sparePTY) ForegroundPgid() (int, error) { return 1, nil }
func (p *sparePTY) Close() error               { p.once.Do(func() { close(p.closed) }); return nil }
func (p *sparePTY) isClosed() bool {
	select {
	case <-p.closed:
		return true
	default:
		return false
	}
}

type spareFactory struct {
	mu    sync.Mutex
	opens []spareOpen
}

func (f *spareFactory) Open(ctx context.Context, _ string, _ []string, _ []string, cwd string, g domain.Geometry) (ports.PTY, error) {
	p := &sparePTY{ctx: ctx, closed: make(chan struct{})}
	f.mu.Lock()
	f.opens = append(f.opens, spareOpen{cwd: cwd, geometry: g, pty: p})
	f.mu.Unlock()
	return p, nil
}

func (f *spareFactory) snapshot() []spareOpen {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]spareOpen(nil), f.opens...)
}

// liveShells counts opened floating PTYs that are still running.
func (f *spareFactory) liveShells() int {
	n := 0
	for _, open := range f.snapshot() {
		if !open.pty.isClosed() {
			n++
		}
	}
	return n
}

func waitSpareReady(t *testing.T, sess *session) {
	t.Helper()
	require.Eventually(t, func() bool {
		sess.floatingLaunchMu.Lock()
		defer sess.floatingLaunchMu.Unlock()
		return sess.floatingSpare != nil && !sess.floatingSpareStarting
	}, time.Second, time.Millisecond)
}

func waitFloatingVisible(t *testing.T, tb *tab) *pane {
	t.Helper()
	var p *pane
	require.Eventually(t, func() bool {
		tb.mu.Lock()
		defer tb.mu.Unlock()
		p = tb.floating.pane
		return tb.floating.state == floatingVisible && p != nil
	}, time.Second, time.Millisecond)
	return p
}

func TestFloatingSpareIsSharedPerSession(t *testing.T) {
	cfg := domain.FloatingConfig{Width: 80, Height: 80}
	tests := []struct {
		name       string
		claimCwd   string
		wantReused bool
	}{
		{name: "same cwd claims the spare", claimCwd: "/work", wantReused: true},
		{name: "other cwd launches a fresh shell", claimCwd: "/other", wantReused: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := &spareFactory{}
			d := newTestDaemon(t, factory, stubClock{})
			d.ApplyConfig(domain.Config{Floating: cfg})
			cwds := map[int]string{1: "/work", 2: tt.claimCwd}
			d.procCwd = func(pid int) (string, error) { return cwds[pid], nil }
			d.dirOrHome = func(cwd string) string { return cwd }

			first := newTab(&pidPTY{pid: 1}, domain.Size{Cols: 120, Rows: 40})
			second := newTab(&pidPTY{pid: 2}, domain.Size{Cols: 60, Rows: 20})
			for _, tb := range []*tab{first, second} {
				tb.ctx, tb.cancel = context.WithCancel(t.Context())
			}
			sess := &session{sessionCore: sessionCore{name: "work"}, tabs: []*tab{first, second}, ctx: t.Context()}

			// Activating both tabs starts exactly one session spare.
			d.ensureFloatingWarm(sess, first)
			d.ensureFloatingWarm(sess, second)
			waitSpareReady(t, sess)
			require.Len(t, factory.snapshot(), 1)
			require.Equal(t, "/work", factory.snapshot()[0].cwd)

			// The second (smaller) tab opens its floating pane and claims the
			// spare that the first tab started.
			second.mu.Lock()
			generation := second.beginFloatingWarmLocked(true)
			second.mu.Unlock()
			d.launchFloating(sess, second, cfg, generation, true)
			claimed := waitFloatingVisible(t, second)
			waitSpareReady(t, sess)

			want := calculateContentFloatingGeometry(domain.Size{Cols: 60, Rows: 20}, cfg)
			claimed.mu.Lock()
			require.Equal(t, want.ptyRect(), claimed.rect)
			require.Equal(t, rectSize(want.ptyRect()), claimed.geometry.Size)
			require.Equal(t, rectSize(want.ptyRect()).Cols, claimed.screen.Columns())
			claimed.mu.Unlock()

			opens := factory.snapshot()
			if tt.wantReused {
				require.Same(t, opens[0].pty, claimed.pty, "the claim must reuse the pre-started shell")
				require.Equal(t, []domain.Geometry{claimed.geometry}, opens[0].pty.resizes, "the claim resizes the spare to the claiming tab")
				require.Len(t, opens, 2, "one claimed shell plus one new spare")
			} else {
				require.True(t, opens[0].pty.isClosed(), "a spare from another cwd is discarded")
				require.Equal(t, tt.claimCwd, opens[1].cwd)
				require.Same(t, opens[1].pty, claimed.pty)
				require.Len(t, opens, 3, "a fresh shell plus one new spare")
			}
			// Panes visible as floating plus exactly one idle spare.
			require.Equal(t, 2, factory.liveShells())

			sess.stopInMemoryLifecycle()
			d.teardownFloating(second, nil)
			d.sessWg.Wait()
			require.Zero(t, factory.liveShells(), "teardown closes the spare")
		})
	}
}

type pidPTY struct {
	transactionalResizePTY
	pid int
}

func (p *pidPTY) Pid() int { return p.pid }

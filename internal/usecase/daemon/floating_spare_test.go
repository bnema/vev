package daemon

import (
	"context"
	"io"
	"sync"
	"testing"
	"testing/synctest"

	renderer "github.com/bnema/vev-vt"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	themeui "github.com/bnema/vev/internal/usecase/theme"
)

// spareOpen records one PTY Open issued for a floating shell.
type spareOpen struct {
	cwd      string
	geometry domain.Geometry
	pty      *spareShell
}

// spareShell is the observable state behind a floating-shell PTY mock.
type spareShell struct {
	mock    *portsmocks.MockPTY
	mu      sync.Mutex
	resizes []domain.Geometry
	closed  chan struct{}
	once    sync.Once
}

func (s *spareShell) isClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

func (s *spareShell) recordedResizes() []domain.Geometry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]domain.Geometry(nil), s.resizes...)
}

// newSpareShell builds a PTY mock that reads until ctx ends or it is closed.
// read, when non-nil, replaces the default Read; write likewise for Write.
func newSpareShell(t testing.TB, ctx context.Context, pid int, read func(b []byte, wait func() (int, error)) (int, error), write func([]byte)) *spareShell {
	t.Helper()
	s := &spareShell{mock: portsmocks.NewMockPTY(t), closed: make(chan struct{})}
	wait := func() (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-s.closed:
			return 0, io.EOF
		}
	}
	s.mock.EXPECT().Read(mock.Anything).RunAndReturn(func(b []byte) (int, error) {
		if read != nil {
			return read(b, wait)
		}
		return wait()
	}).Maybe()
	s.mock.EXPECT().Write(mock.Anything).RunAndReturn(func(b []byte) (int, error) {
		if write != nil {
			write(b)
		}
		return len(b), nil
	}).Maybe()
	s.mock.EXPECT().Resize(mock.Anything).RunAndReturn(func(g domain.Geometry) error {
		s.mu.Lock()
		s.resizes = append(s.resizes, g)
		s.mu.Unlock()
		return nil
	}).Maybe()
	s.mock.EXPECT().Pid().Return(pid).Maybe()
	s.mock.EXPECT().ForegroundPgid().Return(1, nil).Maybe()
	s.mock.EXPECT().Close().RunAndReturn(func() error { s.once.Do(func() { close(s.closed) }); return nil }).Maybe()
	return s
}

// spareOpens records every Open issued through newSpareFactory.
type spareOpens struct {
	mu    sync.Mutex
	opens []spareOpen
}

func newSpareFactory(t testing.TB) (*portsmocks.MockPTYFactory, *spareOpens) {
	t.Helper()
	rec := &spareOpens{}
	factory := portsmocks.NewMockPTYFactory(t)
	factory.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, _ string, _ []string, _ []string, cwd string, g domain.Geometry) (ports.PTY, error) {
			shell := newSpareShell(t, ctx, 1, nil, nil)
			rec.mu.Lock()
			rec.opens = append(rec.opens, spareOpen{cwd: cwd, geometry: g, pty: shell})
			rec.mu.Unlock()
			return shell.mock, nil
		}).Maybe()
	return factory, rec
}

func (f *spareOpens) snapshot() []spareOpen {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]spareOpen(nil), f.opens...)
}

// liveShells counts opened floating PTYs that are still running.
func (f *spareOpens) liveShells() int {
	n := 0
	for _, open := range f.snapshot() {
		if !open.pty.isClosed() {
			n++
		}
	}
	return n
}

// waitSpareReady runs inside a bubble: after quiescence the spare is ready.
func waitSpareReady(t *testing.T, sess *session) {
	t.Helper()
	synctest.Wait()
	sess.floatingLaunchMu.Lock()
	defer sess.floatingLaunchMu.Unlock()
	require.True(t, sess.floatingSpare != nil && !sess.floatingSpareStarting, "floating spare was not ready at quiescence")
}

// waitFloatingVisible runs inside a bubble: after quiescence the popup is visible.
func waitFloatingVisible(t *testing.T, tb *tab) *pane {
	t.Helper()
	synctest.Wait()
	tb.mu.Lock()
	defer tb.mu.Unlock()
	p := tb.floating.pane
	require.True(t, tb.floating.state == floatingVisible && p != nil, "floating pane was not visible at quiescence")
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
			synctest.Test(t, func(t *testing.T) {
				ptys, factory := newSpareFactory(t)
				d := newTestDaemon(t, ptys, newStubClock(t))
				d.ApplyConfig(domain.Config{Floating: cfg})
				cwds := map[int]string{1: "/work", 2: tt.claimCwd}
				WithProcessInspector(&processInspectorFake{cwd: func(pid int) (string, error) { return cwds[pid], nil }})(d)
				d.dirOrHome = func(cwd string) string { return cwd }

				first := newTab(newPidPTY(t, 1), domain.Size{Cols: 120, Rows: 40})
				second := newTab(newPidPTY(t, 2), domain.Size{Cols: 60, Rows: 20})
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
					require.Same(t, opens[0].pty.mock, claimed.pty, "the claim must reuse the pre-started shell")
					require.Equal(t, []domain.Geometry{claimed.geometry}, opens[0].pty.recordedResizes(), "the claim resizes the spare to the claiming tab")
					require.Len(t, opens, 2, "one claimed shell plus one new spare")
				} else {
					require.True(t, opens[0].pty.isClosed(), "a spare from another cwd is discarded")
					require.Equal(t, tt.claimCwd, opens[1].cwd)
					require.Same(t, opens[1].pty.mock, claimed.pty)
					require.Len(t, opens, 3, "a fresh shell plus one new spare")
				}
				// Panes visible as floating plus exactly one idle spare.
				require.Equal(t, 2, factory.liveShells())

				sess.stopInMemoryLifecycle()
				d.teardownFloating(second, nil)
				d.sessWg.Wait()
				require.Zero(t, factory.liveShells(), "teardown closes the spare")
			})
		})
	}
}

// newPidPTY is a quiet PTY reporting pid, for cwd lookups of the active tab.
func newPidPTY(t testing.TB, pid int) *portsmocks.MockPTY {
	t.Helper()
	p := newQuietPTY(t)
	p.EXPECT().Pid().Unset()
	p.EXPECT().Pid().Return(pid).Maybe()
	return p
}

func TestFloatingSpareAnswersStartupQueriesBeforeClaim(t *testing.T) {
	for _, tt := range []struct {
		name  string
		query string
		want  string
	}{
		{name: "primary device attributes", query: "\x1b[c", want: "\x1b[?"},
		{name: "background color from the session theme", query: "\x1b]11;?\x07", want: "\x1b]11;rgb:"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				replied := make(chan []byte, 4)
				ptys := portsmocks.NewMockPTYFactory(t)
				ptys.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
					func(ctx context.Context, _ string, _ []string, _ []string, _ string, _ domain.Geometry) (ports.PTY, error) {
						// A floating shell that sends one terminal query at
						// startup and records what the daemon writes back.
						sent := false
						shell := newSpareShell(t, ctx, 1,
							func(b []byte, wait func() (int, error)) (int, error) {
								if sent {
									return wait()
								}
								sent = true
								return copy(b, tt.query), nil
							},
							func(b []byte) { replied <- append([]byte(nil), b...) })
						return shell.mock, nil
					}).Maybe()
				d := newTestDaemon(t, ptys, newStubClock(t))
				d.ApplyConfig(domain.Config{Floating: domain.FloatingConfig{Width: 80, Height: 80}})
				tb := newTab(newPidPTY(t, 1), domain.Size{Cols: 80, Rows: 24})
				tb.ctx, tb.cancel = context.WithCancel(t.Context())
				ac := &attachedClient{}
				ac.setClientTheme(themeui.Theme{Known: true, HasFG: true, HasBG: true,
					Foreground: renderer.RGB{R: 0xee, G: 0xee, B: 0xee}, Background: renderer.RGB{R: 0x11, G: 0x11, B: 0x11}})
				sess := &session{sessionCore: sessionCore{name: "work", attachments: map[*attachedClient]struct{}{ac: {}}}, tabs: []*tab{tb}, ctx: t.Context()}

				d.ensureFloatingWarm(sess, tb)
				reply := requireReceived(t, replied, "the unclaimed spare never answered its shell's startup query")
				require.Contains(t, string(reply), tt.want, "the unclaimed spare must answer its shell's startup query")

				sess.stopInMemoryLifecycle()
				d.sessWg.Wait()
			})
		})
	}
}

func TestFloatingSpareExitBeforeClaimDropsIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ptys, factory := newSpareFactory(t)
		d := newTestDaemon(t, ptys, newStubClock(t))
		d.ApplyConfig(domain.Config{Floating: domain.FloatingConfig{Width: 80, Height: 80}})
		tb := newTab(newPidPTY(t, 1), domain.Size{Cols: 80, Rows: 24})
		tb.ctx, tb.cancel = context.WithCancel(t.Context())
		sess := &session{sessionCore: sessionCore{name: "work"}, tabs: []*tab{tb}, ctx: t.Context()}

		d.ensureFloatingWarm(sess, tb)
		waitSpareReady(t, sess)
		spare := sess.floatingSparePane()
		require.NotNil(t, spare)
		require.NoError(t, factory.snapshot()[0].pty.mock.Close(), "the spare's shell exits")
		synctest.Wait()
		require.Nil(t, sess.floatingSparePane(),
			"an exited unclaimed spare must be dropped so the next claim launches a fresh shell")

		sess.stopInMemoryLifecycle()
		d.sessWg.Wait()
	})
}

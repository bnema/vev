package daemon

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/layout"
)

func TestFloatingSlotTransitions(t *testing.T) {
	first := &pane{}
	second := &pane{}
	tests := []struct {
		name string
		run  func(t *testing.T, tb *tab)
	}{
		{
			name: "warming double toggle retains hidden completion",
			run: func(t *testing.T, tb *tab) {
				tb.mu.Lock()
				generation := tb.beginFloatingWarmLocked(false)
				start, gotGeneration := tb.toggleFloatingLocked()
				if start || gotGeneration != generation || !tb.floating.desiredVisible {
					t.Fatalf("first warming toggle = start %v, generation %d, desired %v", start, gotGeneration, tb.floating.desiredVisible)
				}
				start, gotGeneration = tb.toggleFloatingLocked()
				if start || gotGeneration != generation || tb.floating.desiredVisible {
					t.Fatalf("second warming toggle = start %v, generation %d, desired %v", start, gotGeneration, tb.floating.desiredVisible)
				}
				if !tb.installFloatingLocked(first, generation) || tb.floating.state != floatingHidden || tb.floating.pane != first {
					t.Fatal("warming completion did not retain a hidden pane")
				}
				tb.mu.Unlock()
			},
		},
		{
			name: "open and hide retain same pane",
			run: func(t *testing.T, tb *tab) {
				tb.mu.Lock()
				generation := tb.beginFloatingWarmLocked(false)
				if !tb.installFloatingLocked(first, generation) {
					t.Fatal("install failed")
				}
				start, _ := tb.toggleFloatingLocked()
				if start || tb.floating.state != floatingVisible || tb.floating.pane != first {
					t.Fatal("hidden open did not retain pane")
				}
				tb.toggleFloatingLocked()
				if tb.floating.state != floatingHidden || tb.floating.pane != first {
					t.Fatal("visible hide did not retain pane")
				}
				tb.mu.Unlock()
			},
		},
		{
			name: "current launch failure clears desired visibility",
			run: func(t *testing.T, tb *tab) {
				tb.mu.Lock()
				generation := tb.beginFloatingWarmLocked(true)
				if !tb.failFloatingLocked(generation) || tb.floating.state != floatingUninitialized || tb.floating.desiredVisible || tb.floating.pane != nil {
					t.Fatal("current launch failure did not clear slot")
				}
				tb.mu.Unlock()
			},
		},
		{
			name: "current exit clears and stale completion is ignored",
			run: func(t *testing.T, tb *tab) {
				tb.mu.Lock()
				generation := tb.beginFloatingWarmLocked(true)
				if !tb.installFloatingLocked(first, generation) {
					t.Fatal("install failed")
				}
				if !tb.clearFloatingLocked(first, generation) || tb.floating.state != floatingUninitialized || tb.floating.pane != nil {
					t.Fatal("current exit did not clear slot")
				}
				newGeneration := tb.beginFloatingWarmLocked(true)
				if tb.installFloatingLocked(second, generation) {
					t.Fatal("stale completion installed")
				}
				if tb.floating.state != floatingWarming || tb.floating.generation != newGeneration || tb.floating.pane != nil {
					t.Fatal("stale completion changed newer warming state")
				}
				tb.mu.Unlock()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { tt.run(t, &tab{}) })
	}
}

func TestFloatingLifecycleCapturesLaunchBeforeOpenAndDoesNotHoldTabLock(t *testing.T) {
	pty := portsmocks.NewMockPTY(t)
	readerStarted := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	pty.EXPECT().Read(mock.Anything).RunAndReturn(func([]byte) (int, error) {
		close(readerStarted)
		<-release
		return 0, context.Canceled
	}).Once()
	pty.EXPECT().Close().RunAndReturn(func() error { unblock(); return nil }).Once()
	factory := portsmocks.NewMockPTYFactory(t)
	opened := make(chan struct{})
	allowOpen := make(chan struct{})
	var gotCommand, gotDir string
	var gotArgs []string
	var gotSize domain.Size
	factory.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(command string, args, _ []string, dir string, size domain.Size) (ports.PTY, error) {
			gotCommand, gotArgs, gotDir, gotSize = command, append([]string(nil), args...), dir, size
			close(opened)
			<-allowOpen
			return pty, nil
		}).Once()
	d := newTestDaemon(t, factory, stubClock{})
	d.shell = "/bin/custom-shell"
	cwd := t.TempDir()
	tb := newTabWithStableID("t_stable", "p_normal", newBlockingPanePTY(t), domain.Size{Cols: 100, Rows: 40})
	tb.ctx, tb.cancel = context.WithCancel(t.Context())
	sess := &session{name: "work", cwd: cwd, tabs: []*tab{tb}, ctx: t.Context()}
	d.ApplyConfig(domain.Config{Floating: domain.FloatingConfig{Command: "btop --utf", Width: 50, Height: 50}})
	d.ensureFloatingWarm(sess, tb)
	// Open has started while this goroutine owns tab.mu: an external factory
	// call under that lock would deadlock this channel-controlled test.
	tb.mu.Lock()
	select {
	case <-opened:
	case <-time.After(time.Second):
		t.Fatal("floating Open waited for tab.mu")
	}
	tb.mu.Unlock()
	d.ApplyConfig(domain.Config{Floating: domain.FloatingConfig{Command: "later", Width: 90, Height: 90}})
	close(allowOpen)
	select {
	case <-readerStarted: // install started exactly one reader after the async Open
	case <-time.After(time.Second):
		t.Fatal("floating pane was not installed")
	}
	require.Equal(t, "/bin/custom-shell", gotCommand)
	require.Equal(t, []string{"-lc", "btop --utf"}, gotArgs)
	require.Equal(t, cwd, gotDir)
	require.Equal(t, domain.Size{Cols: 48, Rows: 18}, gotSize)
	d.teardownFloating(tb)
	unblock()
	d.sessWg.Wait()
}

func TestFloatingLaunchOwnershipJoinsShutdown(t *testing.T) {
	floatingPTY := portsmocks.NewMockPTY(t)
	readerStarted := make(chan struct{})
	readerRelease := make(chan struct{})
	var releaseOnce sync.Once
	releaseReader := func() { releaseOnce.Do(func() { close(readerRelease) }) }
	var closeCount int
	floatingPTY.EXPECT().Read(mock.Anything).RunAndReturn(func([]byte) (int, error) {
		close(readerStarted)
		<-readerRelease
		return 0, io.EOF
	}).Once()
	floatingPTY.EXPECT().Close().RunAndReturn(func() error {
		closeCount++
		releaseReader()
		return nil
	}).Once()
	factory := portsmocks.NewMockPTYFactory(t)
	opened := make(chan struct{})
	releaseOpen := make(chan struct{})
	factory.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(string, []string, []string, string, domain.Size) (ports.PTY, error) {
			close(opened)
			<-releaseOpen
			return floatingPTY, nil
		}).Once()
	d := newTestDaemon(t, factory, stubClock{})
	tb := newFloatingTestTab(t)
	sessCtx, cancel := context.WithCancel(t.Context())
	sess := &session{id: "floating-ownership", name: "work", tabs: []*tab{tb}, ctx: sessCtx, cancel: cancel}
	d.mu.Lock()
	d.sessions[sess.id] = sess
	d.mu.Unlock()

	// Keep a known worker alive while the waiter starts. Releasing it after
	// Open is blocked deterministically exposes an uncounted launch worker.
	d.sessWg.Add(1)
	joined := waitGroupDone(&d.sessWg)
	d.startFloating(sess, tb, true)
	select {
	case <-opened:
	case <-time.After(time.Second):
		t.Fatal("floating Open did not start")
	}
	d.sessWg.Done()
	// Waiting while Open is blocked must retain the launch worker. Otherwise
	// daemon shutdown can finish and a late install can Add reader goroutines.
	launchUnaccounted := false
	select {
	case <-joined:
		launchUnaccounted = true
	case <-time.After(time.Second):
	}
	close(releaseOpen)
	select {
	case <-readerStarted:
	case <-time.After(time.Second):
		t.Fatal("floating reader did not start")
	}
	require.NoError(t, d.killSession(sess, ports.ReasonSessionKilled, false))
	select {
	case <-waitGroupDone(&d.sessWg):
	case <-time.After(time.Second):
		t.Fatal("floating launch, reader, and scheduler did not join")
	}
	tb.mu.Lock()
	require.Nil(t, tb.floating.pane, "shutdown leaked an installed floating pane")
	require.Equal(t, floatingUninitialized, tb.floating.state)
	tb.mu.Unlock()
	require.False(t, launchUnaccounted, "blocked floating launch was not accounted by sessWg")
	require.Equal(t, 1, closeCount, "floating PTY must close exactly once")
}

func TestFloatingLifecycleStaleSuccessAndOldExitCannotReplaceCurrentSlot(t *testing.T) {
	pty := portsmocks.NewMockPTY(t)
	closed := make(chan struct{})
	pty.EXPECT().Close().RunAndReturn(func() error { close(closed); return nil }).Once()
	factory := portsmocks.NewMockPTYFactory(t)
	opened := make(chan struct{})
	allowOpen := make(chan struct{})
	factory.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(string, []string, []string, string, domain.Size) (ports.PTY, error) {
			close(opened)
			<-allowOpen
			return pty, nil
		}).Once()
	d := newTestDaemon(t, factory, stubClock{})
	tb := newFloatingTestTab(t)
	sess := &session{name: "work", tabs: []*tab{tb}, ctx: t.Context()}
	d.ensureFloatingWarm(sess, tb)
	<-opened
	d.teardownFloating(tb) // invalidate before the delayed Open returns
	close(allowOpen)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("stale completion did not close its PTY")
	}
	d.sessWg.Wait()

	first := &pane{}
	second := &pane{}
	tb.mu.Lock()
	g1 := tb.beginFloatingWarmLocked(true)
	require.True(t, tb.installFloatingLocked(first, g1))
	g2 := tb.clearFloatingLocked(first, g1)
	require.True(t, g2)
	generation := tb.beginFloatingWarmLocked(true)
	require.True(t, tb.installFloatingLocked(second, generation))
	tb.mu.Unlock()
	d.reapFloating(sess, tb, first, g1)
	tb.mu.Lock()
	require.Same(t, second, tb.floating.pane, "old EOF must not clear replacement")
	tb.mu.Unlock()
}

func TestFloatingReapAndTeardownCloseMatchingPaneOnce(t *testing.T) {
	pty, _ := newBlockingPTY(t)
	tb := newFloatingTestTab(t)
	p := newPane(layout.PaneID("floating"), pty, domain.Size{Cols: 10, Rows: 10})
	tb.mu.Lock()
	g := tb.beginFloatingWarmLocked(true)
	require.True(t, tb.installFloatingLocked(p, g))
	tb.mu.Unlock()
	d := newTestDaemon(t, nil, stubClock{})
	d.reapFloating(&session{}, tb, p, g)
	d.teardownFloating(tb)
	pty.AssertNumberOfCalls(t, "Close", 1)
}

func TestFloatingTeardownClosesInstalledPaneOnce(t *testing.T) {
	pty, _ := newBlockingPTY(t)
	tb := newFloatingTestTab(t)
	p := newPane(layout.PaneID("floating"), pty, domain.Size{Cols: 10, Rows: 10})
	tb.mu.Lock()
	g := tb.beginFloatingWarmLocked(false)
	require.True(t, tb.installFloatingLocked(p, g))
	tb.mu.Unlock()
	d := newTestDaemon(t, nil, stubClock{})
	d.teardownFloating(tb)
	d.teardownFloating(tb)
	pty.AssertNumberOfCalls(t, "Close", 1)
}

func TestFloatingEOFRepaintsVisibleSlotOnly(t *testing.T) {
	newCase := func(t *testing.T, state floatingState) (*Daemon, *session, *attachedClient, chan ports.Frame, *tab, *pane, func()) {
		t.Helper()
		normalPTY, releaseNormal := newBlockingPTY(t)
		d, sess, ac, sends := newManualSessionWithPTYs(t, normalPTY)
		tb := sess.activeTab()
		require.NotNil(t, tb)
		tb.mu.Lock()
		normal := tb.focusedPane()
		normal.mu.Lock()
		normal.screen.Write([]byte("underlying-cell"))
		normal.mu.Unlock()

		floatingPTY := portsmocks.NewMockPTY(t)
		floatingPTY.EXPECT().Close().Return(nil).Maybe()
		releaseEOF := make(chan struct{})
		floatingPTY.EXPECT().Read(mock.Anything).RunAndReturn(func([]byte) (int, error) {
			<-releaseEOF
			return 0, io.EOF
		}).Once()
		floating := newPane(layout.PaneID("floating"), floatingPTY, domain.Size{Cols: 20, Rows: 8})
		generation := tb.beginFloatingWarmLocked(state == floatingVisible)
		require.True(t, tb.installFloatingLocked(floating, generation))
		if state == floatingHidden {
			require.Equal(t, floatingHidden, tb.floating.state)
		}
		tb.mu.Unlock()

		// Establish a renderer shadow first. The EOF repaint must reset it and
		// redraw the underlying cell rather than depending on popup damage.
		d.paint(sess, ac, true)
		baseline := awaitFrame(t, sends, ports.MsgOutput)
		baselineOutput, err := ports.UnmarshalOutput(baseline.Payload)
		require.NoError(t, err)
		require.Contains(t, string(baselineOutput.Data), "underlying-cell")

		reaped := make(chan struct{})
		floating.onExit = func() {
			d.reapFloating(sess, tb, floating, generation)
			close(reaped)
		}
		d.sessWg.Add(1)
		go d.ptyReader(sess, tb, floating)
		return d, sess, ac, sends, tb, floating, func() {
			close(releaseEOF)
			<-reaped
			d.sessWg.Wait()
			releaseNormal()
		}
	}

	t.Run("visible matching EOF clears and fully repaints underlying content", func(t *testing.T) {
		_, _, _, sends, tb, floating, exit := newCase(t, floatingVisible)
		exit()

		tb.mu.Lock()
		require.Equal(t, floatingUninitialized, tb.floating.state)
		require.Nil(t, tb.floating.pane)
		tb.mu.Unlock()
		repaint := awaitFrame(t, sends, ports.MsgOutput)
		output, err := ports.UnmarshalOutput(repaint.Payload)
		require.NoError(t, err)
		require.Zero(t, output.BaseStateNum, "visible EOF must force a dependency-free full repaint")
		require.Contains(t, string(output.Data), "underlying-cell", "repaint must restore cells previously covered by the popup")
		require.NotNil(t, floating)
	})

	t.Run("hidden matching EOF clears without repaint", func(t *testing.T) {
		_, _, _, sends, tb, _, exit := newCase(t, floatingHidden)
		exit()

		tb.mu.Lock()
		require.Equal(t, floatingUninitialized, tb.floating.state)
		require.Nil(t, tb.floating.pane)
		tb.mu.Unlock()
		select {
		case frame := <-sends:
			t.Fatalf("hidden EOF unexpectedly repainted: %#v", frame)
		default:
		}
	})

	t.Run("stale EOF is ignored without repaint", func(t *testing.T) {
		_, _, _, sends, tb, stale, exit := newCase(t, floatingHidden)
		tb.mu.Lock()
		generation := tb.floating.generation
		require.True(t, tb.clearFloatingLocked(stale, generation))
		current := newPane(layout.PaneID("replacement"), newBlockingPanePTY(t), domain.Size{Cols: 20, Rows: 8})
		currentGeneration := tb.beginFloatingWarmLocked(true)
		require.True(t, tb.installFloatingLocked(current, currentGeneration))
		tb.mu.Unlock()
		exit()

		tb.mu.Lock()
		require.Same(t, current, tb.floating.pane)
		require.Equal(t, floatingVisible, tb.floating.state)
		tb.mu.Unlock()
		select {
		case frame := <-sends:
			t.Fatalf("stale EOF unexpectedly repainted: %#v", frame)
		default:
		}
	})
}

func newFloatingTestTab(t *testing.T) *tab {
	t.Helper()
	tb := newTabWithStableID("t_test", "p_test", newBlockingPanePTY(t), domain.Size{Cols: 80, Rows: 24})
	tb.ctx, tb.cancel = context.WithCancel(t.Context())
	return tb
}

func newBlockingPanePTY(t *testing.T) *portsmocks.MockPTY {
	t.Helper()
	p, _ := newBlockingPTY(t)
	return p
}

func TestFloatingLaunchUsesLiveOrValidatedSessionCwd(t *testing.T) {
	tests := []struct {
		name       string
		sessionCwd string
		liveCwd    string
		wantCwd    string
	}{
		{name: "valid session cwd", sessionCwd: "/valid", wantCwd: "/valid"},
		{name: "invalid session cwd falls home", sessionCwd: "/invalid", wantCwd: "/home/test"},
		{name: "live focused cwd", sessionCwd: "/invalid", liveCwd: "/live", wantCwd: "/live"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := portsmocks.NewMockPTYFactory(t)
			opened := make(chan string, 1)
			factory.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
				func(_ string, _ []string, _ []string, cwd string, _ domain.Size) (ports.PTY, error) {
					opened <- cwd
					return nil, io.ErrUnexpectedEOF
				}).Once()
			d := newTestDaemon(t, factory, stubClock{})
			d.dirOrHome = func(cwd string) string {
				if cwd == "/invalid" {
					return "/home/test"
				}
				return cwd
			}
			if tt.liveCwd != "" {
				d.procCwd = func(int) (string, error) { return tt.liveCwd, nil }
			}
			tb := newFloatingTestTab(t)
			sess := &session{name: "work", cwd: tt.sessionCwd, tabs: []*tab{tb}, ctx: t.Context()}
			d.startFloating(sess, tb, false)
			select {
			case got := <-opened:
				require.Equal(t, tt.wantCwd, got)
			case <-time.After(time.Second):
				t.Fatal("floating PTY was not opened")
			}
			d.sessWg.Wait()
		})
	}
}

type floatingLogHandler struct {
	slog.Handler
	handled chan struct{}
}

func (h floatingLogHandler) Handle(ctx context.Context, r slog.Record) error {
	err := h.Handler.Handle(ctx, r)
	close(h.handled)
	return err
}

func TestFloatingLaunchFailureCapturesSessionNameBeforeConcurrentRename(t *testing.T) {
	factory := portsmocks.NewMockPTYFactory(t)
	opened := make(chan struct{})
	release := make(chan struct{})
	factory.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(string, []string, []string, string, domain.Size) (ports.PTY, error) {
			close(opened)
			<-release
			return nil, io.ErrUnexpectedEOF
		}).Once()
	var logs bytes.Buffer
	handled := make(chan struct{})
	d := newTestDaemon(t, factory, stubClock{})
	d.log = slog.New(floatingLogHandler{Handler: slog.NewTextHandler(&logs, nil), handled: handled})
	tb := newFloatingTestTab(t)
	sess := &session{name: "captured", tabs: []*tab{tb}, ctx: t.Context()}
	d.startFloating(sess, tb, false)
	<-opened
	stopRename := make(chan struct{})
	renameDone := make(chan struct{})
	go func() {
		defer close(renameDone)
		for {
			select {
			case <-stopRename:
				return
			default:
			}
			sess.mu.Lock()
			sess.name = "renamed"
			sess.mu.Unlock()
		}
	}()
	close(release)
	select {
	case <-handled:
	case <-time.After(time.Second):
		t.Fatal("floating launch failure was not logged")
	}
	close(stopRename)
	<-renameDone
	d.sessWg.Wait()
	require.True(t, strings.Contains(logs.String(), "session=captured"), logs.String())
}

func TestFloatingInstallLogsSessionNameSafelyDuringRename(t *testing.T) {
	floatingPTY, _ := newBlockingPTY(t)
	factory := portsmocks.NewMockPTYFactory(t)
	opened := make(chan struct{})
	allowOpen := make(chan struct{})
	factory.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(string, []string, []string, string, domain.Size) (ports.PTY, error) {
			close(opened)
			<-allowOpen
			return floatingPTY, nil
		}).Once()
	var logs bytes.Buffer
	handled := make(chan struct{})
	d := newTestDaemon(t, factory, stubClock{})
	d.log = slog.New(floatingLogHandler{Handler: slog.NewTextHandler(&logs, nil), handled: handled})
	tb := newFloatingTestTab(t)
	sess := &session{name: "captured", tabs: []*tab{tb}, ctx: t.Context()}

	d.startFloating(sess, tb, false)
	<-opened
	renamed := make(chan struct{})
	stopRename := make(chan struct{})
	renameDone := make(chan struct{})
	go func() {
		defer close(renameDone)
		sess.mu.Lock()
		sess.name = "renamed"
		sess.mu.Unlock()
		close(renamed)
		for {
			select {
			case <-stopRename:
				return
			default:
			}
			sess.mu.Lock()
			sess.name = "renamed"
			sess.mu.Unlock()
		}
	}()
	<-renamed
	close(allowOpen)
	select {
	case <-handled:
	case <-time.After(time.Second):
		t.Fatal("installed floating pane was not logged")
	}
	close(stopRename)
	<-renameDone
	require.Contains(t, logs.String(), "session=renamed", logs.String())

	d.teardownFloating(tb)
	d.sessWg.Wait()
}

func TestFloatingToggleUsesVisibleTarget(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	tb := newFloatingTestTab(t)
	sess := &session{tabs: []*tab{tb}, ctx: t.Context()}
	tb.mu.Lock()
	g := tb.beginFloatingWarmLocked(false)
	floating := newPane(layout.PaneID("floating"), nil, domain.Size{Cols: 20, Rows: 8})
	require.True(t, tb.installFloatingLocked(floating, g))
	tb.mu.Unlock()
	require.NoError(t, d.toggleFloating(sess, nil))
	tb.mu.Lock()
	require.Same(t, floating, tb.terminalTargetLocked())
	tb.mu.Unlock()
}

func TestFloatingInnerSize(t *testing.T) {
	tests := []struct {
		name string
		tab  domain.Size
		cfg  domain.FloatingConfig
		want domain.Size
	}{
		{name: "one percent", tab: domain.Size{Cols: 100, Rows: 100}, cfg: domain.FloatingConfig{Width: 1, Height: 1}, want: domain.Size{Cols: 1, Rows: 1}},
		{name: "full size reserves borders", tab: domain.Size{Cols: 100, Rows: 80}, cfg: domain.FloatingConfig{Width: 100, Height: 100}, want: domain.Size{Cols: 98, Rows: 78}},
		{name: "tiny tab omits borders", tab: domain.Size{Cols: 2, Rows: 1}, cfg: domain.FloatingConfig{Width: 100, Height: 100}, want: domain.Size{Cols: 2, Rows: 1}},
		{name: "three cells leaves one inner cell", tab: domain.Size{Cols: 3, Rows: 3}, cfg: domain.FloatingConfig{Width: 100, Height: 100}, want: domain.Size{Cols: 1, Rows: 1}},
		{name: "invalid tab has no pty size", tab: domain.Size{}, cfg: domain.FloatingConfig{Width: 80, Height: 80}, want: domain.Size{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := floatingInnerSize(tt.tab, tt.cfg); got != tt.want {
				t.Fatalf("floatingInnerSize(%+v, %+v) = %+v, want %+v", tt.tab, tt.cfg, got, tt.want)
			}
		})
	}
}

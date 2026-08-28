package daemon

import (
	"bytes"
	"context"
	"errors"
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
	"github.com/bnema/vev/internal/protocol"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/internal/usecase/layout"
	themeui "github.com/bnema/vev/internal/usecase/theme"
)

type gatedFloatingOpen struct {
	command string
	args    []string
	env     []string
	dir     string
	size    domain.Size
}

func newGatedOpenFactory(t *testing.T, result ports.PTY, openErr error) (*portsmocks.MockPTYFactory, <-chan gatedFloatingOpen, func()) {
	t.Helper()
	factory := portsmocks.NewMockPTYFactory(t)
	opened := make(chan gatedFloatingOpen, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseOpen := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseOpen)
	factory.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, command string, args, env []string, dir string, geometry domain.Geometry) (ports.PTY, error) {
			opened <- gatedFloatingOpen{command: command, args: append([]string(nil), args...), env: append([]string(nil), env...), dir: dir, size: geometry.Size}
			<-release
			return result, openErr
		}).Once()
	return factory, opened, releaseOpen
}

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

func TestFloatingTransferabilityRejectsOnlyWarmingLaunch(t *testing.T) {
	tests := []struct {
		name  string
		state floatingState
		want  bool
	}{
		{name: "uninitialized", state: floatingUninitialized, want: true},
		{name: "warming", state: floatingWarming, want: false},
		{name: "hidden installed", state: floatingHidden, want: true},
		{name: "visible installed", state: floatingVisible, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tb := &tab{floating: floatingSlot{state: tt.state, generation: 7}}
			tb.mu.Lock()
			got := tb.floatingTransferableLocked()
			tb.mu.Unlock()
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.state, tb.floating.state, "validation must not change launch state")
			require.Equal(t, uint64(7), tb.floating.generation, "validation must preserve retry generation")
		})
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
	factory, opened, allowOpen := newGatedOpenFactory(t, pty, nil)
	d := newTestDaemon(t, factory, stubClock{})
	cwd := t.TempDir()
	tb := newTabWithStableID("t_stable", "p_normal", newBlockingPanePTY(t), domain.Size{Cols: 100, Rows: 40})
	tb.ctx, tb.cancel = context.WithCancel(t.Context())
	sess := &session{sessionCore: sessionCore{name: "work"}, cwd: cwd, env: []string{"ORDINARY=preserved", "DUP=first", "DUP=second", "PAIR=a=b", "SHELL=/bin/first", "SHELL=/bin/custom-shell", "TERM=old", "COLORTERM=old", "TERM_PROGRAM=old", "VEV=old"}, tabs: []*tab{tb}, ctx: t.Context()}
	d.ApplyConfig(domain.Config{Theme: domain.ThemeDark, Floating: domain.FloatingConfig{Command: "btop --utf", Width: 50, Height: 50}})
	d.ensureFloatingWarm(sess, tb)
	// Open has started while this goroutine owns tab.mu: an external factory
	// call under that lock would deadlock this channel-controlled test.
	tb.mu.Lock()
	var openCall gatedFloatingOpen
	select {
	case openCall = <-opened:
	case <-time.After(time.Second):
		t.Fatal("floating Open waited for tab.mu")
	}
	tb.mu.Unlock()
	d.ApplyConfig(domain.Config{Theme: domain.ThemeDark, Floating: domain.FloatingConfig{Command: "later", Width: 90, Height: 90}})
	allowOpen()
	select {
	case <-readerStarted: // install started exactly one reader after the async Open
	case <-time.After(time.Second):
		t.Fatal("floating pane was not installed")
	}
	tb.mu.Lock()
	floating := tb.floating.pane
	tb.mu.Unlock()
	require.NotNil(t, floating)
	require.Equal(t, []string{"ORDINARY=preserved", "DUP=first", "DUP=second", "PAIR=a=b", "SHELL=/bin/first", "SHELL=/bin/custom-shell", "TERM=xterm-256color", "COLORTERM=truecolor", "TERM_PROGRAM=vev", "VEV=session=work,tab=" + tb.stableID + ",pane=" + floating.stableID}, openCall.env)
	assertPaneDefaultColors(t, floating, themeui.BuiltinDark.Foreground, themeui.BuiltinDark.Background)
	assertPaneColorScheme(t, floating, false)
	require.Equal(t, "/bin/custom-shell", openCall.command)
	require.Equal(t, []string{"-lc", "btop --utf"}, openCall.args)
	require.Equal(t, cwd, openCall.dir)
	require.Equal(t, domain.Size{Cols: 48, Rows: 18}, openCall.size)
	d.teardownFloating(tb, nil)
	unblock()
	d.sessWg.Wait()
}

func TestFloatingBlockedOpenReconcilesLatestResponsiveSize(t *testing.T) {
	initial := domain.Size{Cols: 80, Rows: 25}
	latest := domain.Size{Cols: 79, Rows: 25}
	cfg := domain.FloatingConfig{Width: 100, Height: 100}
	latestGeometry := calculateContentFloatingGeometry(tabSize(latest), cfg)

	readerRelease := make(chan struct{})
	readerStarted := make(chan struct{})
	resized := make(chan struct{})
	floatingPTY := portsmocks.NewMockPTY(t)
	floatingPTY.EXPECT().Read(mock.Anything).RunAndReturn(func([]byte) (int, error) {
		close(readerStarted)
		<-readerRelease
		return 0, io.EOF
	}).Once()
	floatingPTY.EXPECT().Resize(domain.Geometry{Size: rectSize(latestGeometry.Inner)}).Run(func(domain.Geometry) { close(resized) }).Return(nil).Once()
	floatingPTY.EXPECT().Close().RunAndReturn(func() error {
		select {
		case <-readerRelease:
		default:
			close(readerRelease)
		}
		return nil
	}).Once()
	factory, opened, allowOpen := newGatedOpenFactory(t, floatingPTY, nil)
	d := newTestDaemon(t, factory, stubClock{})
	d.ApplyConfig(domain.Config{Floating: cfg})
	tabCtx, cancelTab := context.WithCancel(t.Context())
	t.Cleanup(cancelTab)
	tb := newTab(&transactionalResizePTY{}, tabSize(initial))
	tb.ctx, tb.cancel = tabCtx, cancelTab
	sess := &session{sessionCore: sessionCore{id: "responsive-open", name: "work"}, tabs: []*tab{tb}, ctx: t.Context()}

	d.startFloating(sess, tb, true)
	openCall := <-opened
	require.Equal(t, rectSize(calculateContentFloatingGeometry(tabSize(initial), cfg).Inner), openCall.size)
	require.True(t, sess.geometry.requestResize(d, sess, nil, latest, true))
	allowOpen()
	select {
	case <-resized:
	case <-time.After(time.Second):
		t.Fatal("floating install did not reconcile the resize committed while Open was blocked")
	}
	select {
	case <-readerStarted:
	case <-time.After(time.Second):
		t.Fatal("floating reader did not start")
	}

	require.Eventually(t, func() bool {
		tb.mu.Lock()
		p := tb.floating.pane
		visible := tb.floating.state == floatingVisible
		tb.mu.Unlock()
		if !visible || p == nil {
			return false
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.popupGeometry == latestGeometry && p.rect == latestGeometry.Inner
	}, time.Second, time.Millisecond, "floating install retained the stale Open geometry")
	d.teardownFloating(tb, nil)
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
	factory, opened, releaseOpen := newGatedOpenFactory(t, floatingPTY, nil)
	d := newTestDaemon(t, factory, stubClock{})
	tb := newFloatingTestTab(t)
	sessCtx, cancel := context.WithCancel(t.Context())
	sess := &session{sessionCore: sessionCore{id: "floating-ownership", name: "work"}, tabs: []*tab{tb}, ctx: sessCtx, cancel: cancel}
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
	releaseOpen()
	select {
	case <-readerStarted:
	case <-time.After(time.Second):
		t.Fatal("floating reader did not start")
	}
	require.NoError(t, d.killSession(sess, protocol.ReasonSessionKilled, false))
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
	factory, opened, allowOpen := newGatedOpenFactory(t, pty, nil)
	d := newTestDaemon(t, factory, stubClock{})
	tb := newFloatingTestTab(t)
	sess := &session{sessionCore: sessionCore{name: "work"}, tabs: []*tab{tb}, ctx: t.Context()}
	d.ensureFloatingWarm(sess, tb)
	<-opened
	d.teardownFloating(tb, nil) // invalidate before the delayed Open returns
	allowOpen()
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
	publishPaneOwner(first, sess, tb, g1)
	require.True(t, tb.installFloatingLocked(first, g1))
	clearSucceeded := tb.clearFloatingLocked(first, g1)
	require.True(t, clearSucceeded)
	generation := tb.beginFloatingWarmLocked(true)
	require.True(t, tb.installFloatingLocked(second, generation))
	tb.mu.Unlock()
	d.reapInstalledFloating(first)
	tb.mu.Lock()
	require.Same(t, second, tb.floating.pane, "old EOF must not clear replacement")
	tb.mu.Unlock()
}

func TestFloatingExitUsesCurrentOwnerAfterTabTransfer(t *testing.T) {
	factory := &lifetimePTYFactory{}
	d := newTestDaemon(t, factory, stubClock{})
	tb := newFloatingTestTab(t)
	source := &session{sessionCore: sessionCore{id: "source", name: "source"}, tabs: []*tab{tb}, ctx: t.Context()}
	destination := &session{sessionCore: sessionCore{id: "destination", name: "destination"}, tabs: []*tab{tb}, ctx: t.Context()}

	tb.mu.Lock()
	generation := tb.beginFloatingWarmLocked(false)
	tb.mu.Unlock()
	d.openAndInstallFloating(source, tb, floatingLaunchSpec{
		sessionName:  source.name,
		size:         domain.Size{Cols: 20, Rows: 8},
		geometry:     floatingGeometry{Inner: domain.Rect{Width: 20, Height: 8}},
		paneStableID: "p_floating",
		parentCtx:    tb.ctx,
	}, generation)

	tb.mu.Lock()
	floating := tb.floating.pane
	tb.mu.Unlock()
	require.NotNil(t, floating)
	tb.mu.Lock()
	floating.mu.Lock()
	floating.publishOwnerLocked(destination, tb, generation)
	floating.mu.Unlock()
	tb.mu.Unlock()

	sourceClient := &attachedClient{captureFrames: map[*pane]capturedPaneRenderState{floating: {}}}
	sourceClient.initOverlays()
	destinationClient := &attachedClient{captureFrames: map[*pane]capturedPaneRenderState{floating: {}}}
	destinationClient.initOverlays()
	source.registerAttachment(sourceClient)
	destination.registerAttachment(destinationClient)

	floating.onExit()
	d.sessWg.Wait()

	tb.mu.Lock()
	require.Equal(t, floatingUninitialized, tb.floating.state)
	require.Nil(t, tb.floating.pane)
	tb.mu.Unlock()
	sourceClient.sendMu.Lock()
	_, sourceRetained := sourceClient.captureFrames[floating]
	sourceClient.sendMu.Unlock()
	destinationClient.sendMu.Lock()
	_, destinationRetained := destinationClient.captureFrames[floating]
	destinationClient.sendMu.Unlock()
	require.True(t, sourceRetained, "exit must not clean up the retired source owner")
	require.False(t, destinationRetained, "exit must clean up the current destination owner")
}

func TestFloatingReapAndTeardownCloseMatchingPaneOnce(t *testing.T) {
	pty, _ := newBlockingPTY(t)
	tb := newFloatingTestTab(t)
	p := newPane(layout.PaneID("floating"), pty, domain.Size{Cols: 10, Rows: 10})
	sess := &session{}
	tb.mu.Lock()
	g := tb.beginFloatingWarmLocked(true)
	publishPaneOwner(p, sess, tb, g)
	require.True(t, tb.installFloatingLocked(p, g))
	tb.mu.Unlock()
	d := newTestDaemon(t, nil, stubClock{})
	d.reapInstalledFloating(p)
	d.teardownFloating(tb, nil)
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
	d.teardownFloating(tb, nil)
	d.teardownFloating(tb, nil)
	pty.AssertNumberOfCalls(t, "Close", 1)
}

func TestFloatingTeardownClearsCapturedCopyMode(t *testing.T) {
	pty, _ := newBlockingPTY(t)
	tb := newFloatingTestTab(t)
	p := newPane(layout.PaneID("floating"), pty, domain.Size{Cols: 10, Rows: 10})
	tb.mu.Lock()
	generation := tb.beginFloatingWarmLocked(true)
	require.True(t, tb.installFloatingLocked(p, generation))
	tb.mu.Unlock()
	ac := &attachedClient{}
	ac.initOverlays()
	ac.overlays.copyMu.Lock()
	ac.overlays.copyMode = &scopy.Mode{}
	ac.overlays.copyPane = p
	ac.overlays.copyMu.Unlock()
	d := newTestDaemon(t, nil, stubClock{})

	d.teardownFloating(tb, ac)

	require.False(t, ac.overlays.copyActive())
	ac.overlays.copyMu.Lock()
	require.Nil(t, ac.overlays.copyPane)
	ac.overlays.copyMu.Unlock()
}

func TestFloatingEOFRepaintsVisibleSlotOnly(t *testing.T) {
	newCase := func(t *testing.T, state floatingState) (*Daemon, *session, *attachedClient, chan ports.Frame, *tab, *pane, func()) {
		t.Helper()
		normalPTY, releaseNormal := newBlockingPTY(t)
		d, sess, ac, sends := newManualSessionWithPTYs(t, normalPTY)
		tb := testAttachmentTab(sess)
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
		publishPaneOwner(floating, sess, tb, generation)
		require.True(t, tb.installFloatingLocked(floating, generation))
		if state == floatingHidden {
			require.Equal(t, floatingHidden, tb.floating.state)
		}
		tb.mu.Unlock()

		// Establish a renderer shadow first. The EOF repaint must reset it and
		// redraw the underlying cell rather than depending on popup damage.
		d.paint(sess, ac, true, nil)
		baseline := awaitFrame(t, sends, ports.MsgOutput)
		baselineOutput, err := ports.UnmarshalOutput(baseline.Payload)
		require.NoError(t, err)
		require.Contains(t, string(baselineOutput.Data), "underlying-cell")

		reaped := make(chan struct{})
		floating.onExit = func() {
			d.reapInstalledFloating(floating)
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
		require.Zero(t, output.Base, "visible EOF must force a dependency-free full repaint")
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
			factory.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
				func(_ context.Context, _ string, _ []string, _ []string, cwd string, _ domain.Geometry) (ports.PTY, error) {
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
			sess := &session{sessionCore: sessionCore{name: "work"}, cwd: tt.sessionCwd, tabs: []*tab{tb}, ctx: t.Context()}
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
	mu      sync.Mutex
}

func (h *floatingLogHandler) Handle(ctx context.Context, r slog.Record) error {
	err := h.Handler.Handle(ctx, r)
	h.mu.Lock()
	select {
	case <-h.handled:
	default:
		close(h.handled)
	}
	h.mu.Unlock()
	return err
}

func TestFloatingLaunchFailureCapturesSessionNameBeforeConcurrentRename(t *testing.T) {
	factory, opened, release := newGatedOpenFactory(t, nil, io.ErrUnexpectedEOF)
	var logs bytes.Buffer
	handled := make(chan struct{})
	d := newTestDaemon(t, factory, stubClock{})
	d.log = slog.New(&floatingLogHandler{Handler: slog.NewTextHandler(&logs, nil), handled: handled})
	tb := newFloatingTestTab(t)
	sess := &session{sessionCore: sessionCore{name: "captured"}, tabs: []*tab{tb}, ctx: t.Context()}
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
	release()
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
	factory, opened, allowOpen := newGatedOpenFactory(t, floatingPTY, nil)
	var logs bytes.Buffer
	handled := make(chan struct{})
	d := newTestDaemon(t, factory, stubClock{})
	d.log = slog.New(&floatingLogHandler{Handler: slog.NewTextHandler(&logs, nil), handled: handled})
	tb := newFloatingTestTab(t)
	sess := &session{sessionCore: sessionCore{name: "captured"}, tabs: []*tab{tb}, ctx: t.Context()}

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
	allowOpen()
	select {
	case <-handled:
	case <-time.After(time.Second):
		t.Fatal("installed floating pane was not logged")
	}
	close(stopRename)
	<-renameDone
	require.Contains(t, logs.String(), "session=renamed", logs.String())

	d.teardownFloating(tb, nil)
	d.sessWg.Wait()
}

// TestFloatingAsyncSpawnFailureToastsOnlyForUserOpen drives a failed
// asynchronous PTY open through failFloatingLaunch. A user-initiated open
// must surface a toast; a background prewarm must stay silent (the user
// never asked for it). The failure completes on a worker goroutine, so the
// test waits on d.sessWg (which the launch worker joins) before asserting on
// the test goroutine -- no require inside the worker, no sleeps.
func TestFloatingAsyncSpawnFailureToastsOnlyForUserOpen(t *testing.T) {
	tests := []struct {
		name     string
		userOpen bool
	}{
		{name: "user-initiated open toasts", userOpen: true},
		{name: "background prewarm stays silent", userOpen: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spawnErr := errors.New("fork/exec: no such file or directory")
			factory := portsmocks.NewMockPTYFactory(t)
			factory.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
				Return(nil, spawnErr).Once()
			d := newTestDaemon(t, factory, stubClock{})
			tb := newFloatingTestTab(t)
			tr, _ := newCapturingTransport(t)
			ac := &attachedClient{tr: tr, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
			ac.initOverlays()
			sess := &session{sessionCore: sessionCore{id: "floating-spawn", name: "work", attachments: map[*attachedClient]struct{}{ac: {}}}, tabs: []*tab{tb}, ctx: t.Context()}
			ac.setSession(sess)

			d.startFloating(sess, tb, tc.userOpen)

			// The launch worker (registered via d.sessWg.Go in launchFloating)
			// runs failFloatingLaunch synchronously before returning, so
			// waiting on the group is a deterministic completion signal.
			d.sessWg.Wait()

			if tc.userOpen {
				toasts := awaitToastCount(t, ac, 1)
				require.Equal(t, domain.NoticeFloatingSpawn, toasts[0].Code)
				require.Equal(t, "couldn't open floating pane: command failed to start", toasts[0].Message)
			} else {
				ns, _ := visibleToasts(ac)
				require.Empty(t, ns, "background prewarm failure must not toast")
			}
		})
	}
}

func TestToggleFloatingStructuralErrorsAreUserErrors(t *testing.T) {
	tests := []struct {
		name    string
		d       *Daemon
		sess    *session
		wantMsg string
		cause   error
	}{
		{"nil session", newTestDaemon(t, nil, stubClock{}), nil, "couldn't open floating pane: no active session", nil},
		{"no active tab", newTestDaemon(t, nil, stubClock{}), &session{}, "couldn't open floating pane: no active tab", layout.ErrNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.d.toggleFloating(tc.sess, nil)
			require.Error(t, err)

			var ue *domain.UserError
			require.ErrorAs(t, err, &ue)
			require.Equal(t, domain.NoticeFloatingSpawn, ue.Code)
			require.Equal(t, domain.NoticeError, ue.Severity)
			require.Equal(t, tc.wantMsg, ue.Msg)
			if tc.cause != nil {
				require.NotContains(t, ue.Msg, tc.cause.Error())
				require.ErrorIs(t, ue, tc.cause)
			}
		})
	}
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

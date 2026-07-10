package daemon

import (
	"context"
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
	launch := domain.FloatingConfig{Command: "top", Width: 80, Height: 80}

	tests := []struct {
		name string
		run  func(t *testing.T, tb *tab)
	}{
		{
			name: "warming double toggle retains hidden completion",
			run: func(t *testing.T, tb *tab) {
				tb.mu.Lock()
				generation := tb.beginFloatingWarmLocked(launch, false)
				start, gotGeneration := tb.toggleFloatingLocked(launch)
				if start || gotGeneration != generation || !tb.floating.desiredVisible {
					t.Fatalf("first warming toggle = start %v, generation %d, desired %v", start, gotGeneration, tb.floating.desiredVisible)
				}
				start, gotGeneration = tb.toggleFloatingLocked(launch)
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
				generation := tb.beginFloatingWarmLocked(launch, false)
				if !tb.installFloatingLocked(first, generation) {
					t.Fatal("install failed")
				}
				start, _ := tb.toggleFloatingLocked(launch)
				if start || tb.floating.state != floatingVisible || tb.floating.pane != first {
					t.Fatal("hidden open did not retain pane")
				}
				tb.toggleFloatingLocked(launch)
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
				generation := tb.beginFloatingWarmLocked(launch, true)
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
				generation := tb.beginFloatingWarmLocked(launch, true)
				if !tb.installFloatingLocked(first, generation) {
					t.Fatal("install failed")
				}
				if !tb.clearFloatingLocked(first, generation) || tb.floating.state != floatingUninitialized || tb.floating.pane != nil {
					t.Fatal("current exit did not clear slot")
				}
				newGeneration := tb.beginFloatingWarmLocked(launch, true)
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
	tb := newTabWithStableID("t_stable", "p_normal", newBlockingPanePTY(t), domain.Size{Cols: 100, Rows: 40})
	tb.ctx, tb.cancel = context.WithCancel(t.Context())
	sess := &session{name: "work", cwd: "/captured", tabs: []*tab{tb}, ctx: t.Context()}
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
	require.Equal(t, "/captured", gotDir)
	require.Equal(t, domain.Size{Cols: 48, Rows: 18}, gotSize)
	d.teardownFloating(tb)
	unblock()
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

	first := &pane{}
	second := &pane{}
	tb.mu.Lock()
	g1 := tb.beginFloatingWarmLocked(domain.FloatingConfig{}, true)
	require.True(t, tb.installFloatingLocked(first, g1))
	g2 := tb.clearFloatingLocked(first, g1)
	require.True(t, g2)
	generation := tb.beginFloatingWarmLocked(domain.FloatingConfig{}, true)
	require.True(t, tb.installFloatingLocked(second, generation))
	tb.mu.Unlock()
	d.reapFloating(sess, tb, first, g1)
	tb.mu.Lock()
	require.Same(t, second, tb.floating.pane, "old EOF must not clear replacement")
	tb.mu.Unlock()
}

func TestFloatingTeardownClosesInstalledPaneOnce(t *testing.T) {
	pty, _ := newBlockingPTY(t)
	tb := newFloatingTestTab(t)
	p := newPane(layout.PaneID("floating"), pty, domain.Size{Cols: 10, Rows: 10})
	tb.mu.Lock()
	g := tb.beginFloatingWarmLocked(domain.FloatingConfig{}, false)
	require.True(t, tb.installFloatingLocked(p, g))
	tb.mu.Unlock()
	d := newTestDaemon(t, nil, stubClock{})
	d.teardownFloating(tb)
	d.teardownFloating(tb)
	pty.AssertNumberOfCalls(t, "Close", 1)
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

func TestFloatingToggleUsesVisibleTarget(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	tb := newFloatingTestTab(t)
	sess := &session{tabs: []*tab{tb}, ctx: t.Context()}
	tb.mu.Lock()
	g := tb.beginFloatingWarmLocked(domain.FloatingConfig{}, false)
	floating := &pane{}
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

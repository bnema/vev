package daemon

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/layout"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/pkg/renderer"
)

// --- test doubles -----------------------------------------------------------

// stubClock returns timers whose channel never fires, so a scheduler under it
// blocks in its debounce loop until the session context is cancelled. Used by

func TestPTYWriteErrorIsLogged(t *testing.T) {
	var logs bytes.Buffer
	d := New(nil, stubClock{}, slog.New(slog.NewTextHandler(&logs, nil)))
	errBoom := errors.New("boom")
	p := portsmocks.NewMockPTY(t)
	p.EXPECT().Write([]byte("input")).Return(0, errBoom).Once()
	win := newTab(p, domain.Size{Cols: 80, Rows: 23})
	sess := &session{id: "manual", name: "work", tabs: []*tab{win}}
	ac := &attachedClient{}
	ac.setSession(sess)

	daemonKeyHandler{d: d, ac: ac}.Forward([]byte("input"))

	got := logs.String()
	if !strings.Contains(got, "pty write failed") || !strings.Contains(got, "boom") || !strings.Contains(got, "work") {
		t.Fatalf("log output %q does not contain PTY write failure details", got)
	}
}

func TestAltXClosesNonFinalTabScheduler(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	clk := &signalClock{called: make(chan struct{})}
	d, sess, ac, _ := newManualSessionWithPTYs(t, p1, p2)
	d.clock = clk
	defer releasePTY1()
	defer releasePTY2()

	d.sessWg.Add(1)
	go d.scheduler(sess, sess.tabs[1], sess.tabs[1].focusedPane())
	sess.tabs[1].focusedPane().dirty <- struct{}{}
	<-clk.called

	d.handleInput(sess, ac, []byte("\x1b2"))
	d.handleInput(sess, ac, []byte("\x1b "))
	d.handleInput(sess, ac, []byte("CLT\r"))

	select {
	case <-waitGroupDone(&d.sessWg):
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler for removed tab did not exit")
	}
	require.Equal(t, 1, sessionCount(d))
}

func TestPTYEOFClosesNonFinalTabScheduler(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	clk := &signalClock{called: make(chan struct{})}
	d, sess, _, _ := newManualSessionWithPTYs(t, p1, p2)
	d.clock = clk
	defer releasePTY1()
	defer releasePTY2()

	d.sessWg.Add(2)
	go d.scheduler(sess, sess.tabs[1], sess.tabs[1].focusedPane())
	go d.ptyReader(sess, sess.tabs[1], sess.tabs[1].focusedPane())
	sess.tabs[1].focusedPane().dirty <- struct{}{}
	<-clk.called
	releasePTY2()

	select {
	case <-waitGroupDone(&d.sessWg):
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler for EOF-removed tab did not exit")
	}
	require.Equal(t, 1, sessionCount(d))
}

func TestAltXClosesFinalTabAndDetaches(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 1)
	defer releases[0]()

	d.handleInput(sess, ac, []byte("\x1b "))
	d.handleInput(sess, ac, []byte("CLT\r"))

	require.Equal(t, 0, sessionCount(d))
	f := awaitFrame(t, sends, ports.MsgDetached)
	det, err := ports.UnmarshalDetached(f.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ReasonSessionKilled, det.Reason)
}

func TestPTYEOFClosesActiveNonFinalTabAndRepaintsRemaining(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	d, sess, _, sends := newManualSessionWithPTYs(t, p1, p2)
	defer releasePTY2()
	sess.active = 0
	sess.tabs[1].focusedPane().screen.Write([]byte("remaining"))

	d.sessWg.Add(1)
	go d.ptyReader(sess, sess.tabs[0], sess.tabs[0].focusedPane())
	releasePTY1()

	require.Eventually(t, func() bool { return tabCount(sess) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, 1, sessionCount(d))
	require.Equal(t, 0, activeTabIndex(sess))
	f := awaitFrame(t, sends, ports.MsgOutput)
	out, err := ports.UnmarshalOutput(f.Payload)
	require.NoError(t, err)
	data := string(out.Data)
	require.Contains(t, data, "remaining")
	require.Contains(t, data, "work")
	require.Contains(t, data, ";7m")

	d.sessWg.Wait()
}

func TestPTYEOFClosesInactiveNonFinalTabAndRepaintsStatus(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	d, sess, _, sends := newManualSessionWithPTYs(t, p1, p2)
	defer releasePTY1()
	sess.active = 0
	sess.tabs[0].focusedPane().screen.Write([]byte("active"))

	d.sessWg.Add(1)
	go d.ptyReader(sess, sess.tabs[1], sess.tabs[1].focusedPane())
	releasePTY2()

	require.Eventually(t, func() bool { return tabCount(sess) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, 1, sessionCount(d))
	require.Equal(t, 0, activeTabIndex(sess))
	f := awaitFrame(t, sends, ports.MsgOutput)
	out, err := ports.UnmarshalOutput(f.Payload)
	require.NoError(t, err)
	data := string(out.Data)
	require.Contains(t, data, "active")
	require.Contains(t, data, "work")
	require.NotContains(t, data, "  2 ")

	d.sessWg.Wait()
}

func TestPTYEOFFinalTabKillsSessionAndDetaches(t *testing.T) {
	d, sess, _, sends, releases := newManualTabSession(t, 1)

	d.sessWg.Add(1)
	go d.ptyReader(sess, sess.tabs[0], sess.tabs[0].focusedPane())
	releases[0]()

	require.Eventually(t, func() bool { return sessionCount(d) == 0 }, 2*time.Second, 5*time.Millisecond)
	f := awaitFrame(t, sends, ports.MsgDetached)
	det, err := ports.UnmarshalDetached(f.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ReasonSessionKilled, det.Reason)

	d.sessWg.Wait()
}

func TestSchedulerDebounceCoalesces(t *testing.T) {
	mc := portsmocks.NewMockClock(t)
	mc.EXPECT().Now().Return(time.Now()).Maybe()
	mt := portsmocks.NewMockTimer(t)
	timerCh := make(chan time.Time, 1)
	newTimerCalled := make(chan struct{}, 4)

	mc.EXPECT().NewTimer(mock.Anything).RunAndReturn(func(time.Duration) ports.Timer {
		select {
		case newTimerCalled <- struct{}{}:
		default:
		}
		return mt
	}).Maybe()
	mt.EXPECT().C().Return(timerCh).Maybe()
	mt.EXPECT().Stop().Return(true).Maybe()

	var outputs atomic.Int32
	gotOutput := make(chan struct{}, 1)
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Close().Return(nil).Maybe()
	tr.EXPECT().Send(mock.Anything).RunAndReturn(func(f ports.Frame) error {
		if f.Type == ports.MsgOutput {
			outputs.Add(1)
			select {
			case gotOutput <- struct{}{}:
			default:
			}
		}
		return nil
	}).Maybe()

	d := New(nil, mc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	sctx, cancel := context.WithCancel(context.Background())
	win := newTab(newScriptPTY(nil), domain.Size{Cols: 20, Rows: 5})
	ac := &attachedClient{tr: tr, rend: renderer.New(renderer.Capabilities{})}
	sess := &session{id: "s", name: "s", tabs: []*tab{win}, ctx: sctx, cancel: cancel, client: ac}
	ac.setSession(sess)

	win.mu.Lock()
	win.focusedPane().screen.Write([]byte("hi"))
	win.mu.Unlock()

	d.sessWg.Add(1)
	go d.scheduler(sess, win, win.focusedPane())

	// First dirty opens the debounce window.
	win.focusedPane().dirty <- struct{}{}
	<-newTimerCalled

	// A burst of further dirties inside the window must be absorbed.
	for range 5 {
		select {
		case win.focusedPane().dirty <- struct{}{}:
		default:
		}
	}

	// Fire the timer once: exactly one render.
	timerCh <- time.Now()
	<-gotOutput

	cancel()
	d.sessWg.Wait()
	require.Equal(t, int32(1), outputs.Load(), "N dirties in one tab must render exactly once")
}

func TestSchedulerAdaptiveDebounceFloodAndIdle(t *testing.T) {
	delay := nextDebounceDelay(minDebounceInterval, 3)
	require.Greater(t, delay, minDebounceInterval, "sustained flood should widen debounce")
	delay = nextDebounceDelay(delay, 3)
	require.Greater(t, delay, minDebounceInterval+debounceStep, "continued flood should keep adapting")
}

func TestSchedulerAdaptiveDebounceResetsAfterQuietPeriod(t *testing.T) {
	delay := nextDebounceDelay(minDebounceInterval, 2)
	require.Greater(t, delay, minDebounceInterval)
	require.Equal(t, minDebounceInterval, nextDebounceDelay(delay, 0), "isolated update after quiet window should restore idle latency")
}

// --- resize ordering --------------------------------------------------------

func TestResizePreservesLiveContentAndEvictsScrollback(t *testing.T) {
	p := portsmocks.NewMockPTY(t)
	p.EXPECT().Resize(domain.Size{Cols: 4, Rows: 2}).Return(nil).Once()
	p.EXPECT().Resize(domain.Size{Cols: 6, Rows: 4}).Return(nil).Once()

	win := newTab(p, domain.Size{Cols: 4, Rows: 4})
	for y, text := range []string{"0000", "1111", "2222", "3333"} {
		copy(win.focusedPane().screen.Frame.Row(y), testRow(text))
	}
	win.focusedPane().screen.Row = 3

	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Close().Return(nil).Maybe()
	tr.EXPECT().Send(mock.Anything).Return(nil).Maybe()

	d := New(nil, stubClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ac := &attachedClient{tr: tr, rend: renderer.New(renderer.Capabilities{})}
	sess := &session{id: "s", name: "s", tabs: []*tab{win}, client: ac}
	ac.setSession(sess)

	// Client rows are one more than the equivalent case in a single-bar
	// layout: tabSize reserves 2 chrome rows (top + bottom bar) here, not 1,
	// so a client height of 4 (not 3) is what yields the same 2-row tab.
	d.resize(sess, ac, domain.Size{Cols: 4, Rows: 4})
	require.Equal(t, "2222", frameRowString(win.focusedPane().screen.Frame, 0))
	require.Equal(t, "3333", frameRowString(win.focusedPane().screen.Frame, 1))
	require.Equal(t, 2, win.focusedPane().scrollback.Len())
	require.Equal(t, "0000", cellsString(win.focusedPane().scrollback.Row(0)))
	require.Equal(t, "1111", cellsString(win.focusedPane().scrollback.Row(1)))

	d.resize(sess, ac, domain.Size{Cols: 6, Rows: 6})
	require.Equal(t, "2222  ", frameRowString(win.focusedPane().screen.Frame, 0))
	require.Equal(t, "3333  ", frameRowString(win.focusedPane().screen.Frame, 1))
	require.Equal(t, 2, win.focusedPane().scrollback.Len())
}

func frameRowString(f renderer.Frame, y int) string {
	return cellsString(f.Row(y))
}

func cellsString(row []renderer.Cell) string {
	runes := make([]rune, len(row))
	for i, c := range row {
		runes[i] = c.Rune
	}
	return string(runes)
}

func TestTabSizeReservesTopAndBottomChromeRows(t *testing.T) {
	require.Equal(t, domain.Size{Cols: 80, Rows: 22}, tabSize(domain.Size{Cols: 80, Rows: 24}))
	require.Equal(t, domain.Size{Cols: 80, Rows: 1}, tabSize(domain.Size{Cols: 80, Rows: 2}))
}

func TestOffsetDamageShiftsScreenDamageBelowTopBar(t *testing.T) {
	damage := offsetDamage([]renderer.Damage{
		{Kind: renderer.DamageText, X: 2, Y: 3, Width: 4, Height: 1},
		renderer.FullRedraw(),
	})
	require.Equal(t, []renderer.Damage{
		{Kind: renderer.DamageText, X: 2, Y: 4, Width: 4, Height: 1},
		renderer.FullRedraw(),
	}, damage)
}

func TestTranslateDamageShiftsXYAndPreservesFullRedraw(t *testing.T) {
	damage := translateDamage([]renderer.Damage{
		{Kind: renderer.DamageText, X: 2, Y: 3, Width: 4, Height: 1},
		renderer.FullRedraw(),
	}, 5, 7)
	require.Equal(t, []renderer.Damage{
		{Kind: renderer.DamageText, X: 7, Y: 10, Width: 4, Height: 1},
		renderer.FullRedraw(),
	}, damage)
}

func TestTranslatePaneDamagePreservesFullWidthScrollFastPathOnly(t *testing.T) {
	tests := []struct {
		name    string
		content domain.Rect
		area    domain.Rect
		in      renderer.Damage
		want    []renderer.Damage
	}{
		{
			name:    "half width scroll becomes pane text damage",
			content: domain.Rect{X: 21, Y: 0, Width: 20, Height: 4},
			area:    domain.Rect{Width: 41, Height: 4},
			in:      renderer.Damage{Kind: renderer.DamageScrollUp, X: 0, Y: 0, Width: 20, Height: 4, Count: 1},
			want:    []renderer.Damage{{Kind: renderer.DamageText, X: 21, Y: 0, Width: 20, Height: 4}},
		},
		{
			name:    "full width scroll keeps translated scroll damage",
			content: domain.Rect{X: 0, Y: 5, Width: 80, Height: 10},
			area:    domain.Rect{Width: 80, Height: 24},
			in:      renderer.Damage{Kind: renderer.DamageScrollUp, X: 0, Y: 0, Width: 80, Height: 10, Count: 1},
			want:    []renderer.Damage{{Kind: renderer.DamageScrollUp, X: 0, Y: 5, Width: 80, Height: 10, Count: 1}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, translatePaneDamage(tt.in, tt.content, tt.area))
		})
	}
}

func TestRenderClearsDamageOnEmittingPaneWhenDetached(t *testing.T) {
	tb := newTab(nil, domain.Size{Cols: 10, Rows: 3})
	focused := tb.focusedPane()
	emitting := newPane("pane-2", nil, domain.Size{Cols: 10, Rows: 3})
	tb.panes[emitting.id] = emitting
	tb.tree.Root = &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf(focused.id), layout.NewLeaf(emitting.id)}}
	tb.tree.Focus = focused.id
	sess := &session{id: "s", name: "work", tabs: []*tab{tb}, active: 0}
	d := newTestDaemon(t, nil, stubClock{})

	focused.screen.Write([]byte("a"))
	emitting.screen.Write([]byte("b"))
	d.render(sess, tb, emitting)

	require.NotEmpty(t, focused.screen.Damage(), "focused pane damage should be untouched by detached render for another pane")
	require.Empty(t, emitting.screen.Damage(), "emitting pane damage should be drained")
}

func TestComposeTabFrameTwoPaneSplitBlitsDividersDimsAndTranslatesDamage(t *testing.T) {
	win := newTab(nil, domain.Size{Cols: 41, Rows: 4})
	left := win.focusedPane()
	left.screen.ClearDamage()
	left.screen.Write([]byte("L"))
	left.screen.ClearDamage()

	right := newPane("pane-2", nil, domain.Size{Cols: 20, Rows: 4})
	right.screen.Write([]byte("R"))
	right.screen.ClearDamage()
	right.screen.Write([]byte("x"))

	win.tree.Root = &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf(left.id), layout.NewLeaf(right.id)}}
	win.tree.Focus = right.id
	win.panes[right.id] = right

	theme := themeui.Theme{Known: true, TrueColor: true, HasFG: true, HasBG: true, Foreground: renderer.RGB{R: 200, G: 200, B: 200}, Background: renderer.RGB{R: 10, G: 10, B: 10}}
	frame, damage := composeTabFrame(win, domain.Rect{Width: 41, Height: 4}, theme)

	require.Equal(t, 'L', frame.At(0, 0).Rune)
	require.Equal(t, '│', frame.At(20, 0).Rune)
	require.Equal(t, 'R', frame.At(21, 0).Rune)
	require.True(t, frame.At(0, 0).Style.HasForegroundRGB, "unfocused left pane should be dimmed during blit")
	require.False(t, left.screen.Frame.At(0, 0).Style.HasForegroundRGB, "dimming must not mutate vt.Screen")
	require.Contains(t, damage, renderer.Damage{Kind: renderer.DamageText, X: 22, Y: 0, Width: 1, Height: 1, Count: 1})
}

func TestComposeTabFrameStackDrawsTitleBarsAndDimsCollapsed(t *testing.T) {
	win := newTab(nil, domain.Size{Cols: 20, Rows: 5})
	p1 := win.focusedPane()
	p1.title = "one"
	p1.screen.ClearDamage()
	p2 := newPane("pane-2", nil, domain.Size{Cols: 20, Rows: 3})
	p2.title = "two"
	p2.screen.Write([]byte("T"))
	p2.screen.ClearDamage()

	win.tree.Root = &layout.Node{Kind: layout.Stack, Children: []*layout.Node{layout.NewLeaf(p1.id), layout.NewLeaf(p2.id)}, Expanded: p2.id}
	win.tree.Focus = p2.id
	win.panes[p2.id] = p2

	theme := themeui.Theme{Known: true, TrueColor: true, HasFG: true, HasBG: true, Foreground: renderer.RGB{R: 200, G: 200, B: 200}, Background: renderer.RGB{R: 10, G: 10, B: 10}}
	frame, _ := composeTabFrame(win, domain.Rect{Width: 20, Height: 5}, theme)

	require.Equal(t, "one", rowText(frame.Row(0))[:3])
	require.Equal(t, "two", rowText(frame.Row(1))[:3])
	require.Equal(t, 'T', frame.At(0, 2).Rune)
	require.True(t, frame.At(0, 0).Style.HasForegroundRGB, "collapsed title bar should use dimmed chrome")
	require.True(t, frame.At(0, 1).Style.Inverse || frame.At(0, 1).Style.HasBackgroundRGB, "focused title bar should use accent chrome")
}

func TestComposeClientFrameBarCacheSkipsUnchangedBars(t *testing.T) {
	sess, win := newBarCacheTestSession()
	var cache barCache

	_, damage := composeClientFrame(sess, win, true, "", &cache)
	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, damage)
	win.focusedPane().screen.ClearDamage()
	win.focusedPane().screen.Write([]byte("x"))

	_, damage = composeClientFrame(sess, win, false, "", &cache)

	require.NotEmpty(t, damage)
	for _, d := range damage {
		require.NotEqual(t, 0, d.Y, "unchanged top bar should not be damaged")
		require.NotEqual(t, win.focusedPane().screen.Frame.Height+1, d.Y, "unchanged bottom bar should not be damaged")
	}
}

func TestComposeClientFrameBarCacheDamagesChangedBottomOnly(t *testing.T) {
	sess, win := newBarCacheTestSession()
	var cache barCache
	composeClientFrame(sess, win, true, "", &cache)
	win.focusedPane().screen.ClearDamage()

	sess.name = "renamed"
	_, damage := composeClientFrame(sess, win, false, "", &cache)

	require.Equal(t, []renderer.Damage{{Kind: renderer.DamageText, X: 0, Y: win.focusedPane().screen.Frame.Height + 1, Width: win.focusedPane().screen.Frame.Width, Height: 1}}, damage)
}

func TestComposeClientFrameFullRedrawPrimesBarCache(t *testing.T) {
	sess, win := newBarCacheTestSession()
	var cache barCache
	cache.top = []renderer.Cell{renderer.BlankCell()}
	cache.bottom = []renderer.Cell{renderer.BlankCell()}

	_, damage := composeClientFrame(sess, win, true, "", &cache)
	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, damage)
	win.focusedPane().screen.ClearDamage()

	_, damage = composeClientFrame(sess, win, false, "", &cache)
	require.Empty(t, damage)
}

func newBarCacheTestSession() (*session, *tab) {
	win := newTab(newScriptPTY(nil), domain.Size{Cols: 20, Rows: 3})
	sess := &session{id: "s", name: "work", tabs: []*tab{win}}
	return sess, win
}

func TestResizeOrdersPTYBeforeScreen(t *testing.T) {
	newSize := domain.Size{Cols: 100, Rows: 30}

	p := portsmocks.NewMockPTY(t)
	var screenWidthAtResize int
	win := newTab(newScriptPTY(nil), domain.Size{Cols: 80, Rows: 24})
	p.EXPECT().Resize(domain.Size{Cols: 100, Rows: 28}).RunAndReturn(func(sz domain.Size) error {
		// The screen must not yet be resized when the PTY is: proves order.
		screenWidthAtResize = win.focusedPane().screen.Frame.Width
		return nil
	}).Once()
	win.focusedPane().pty = p

	var gotOutput atomic.Bool
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Close().Return(nil).Maybe()
	tr.EXPECT().Send(mock.Anything).RunAndReturn(func(f ports.Frame) error {
		if f.Type == ports.MsgOutput {
			gotOutput.Store(true)
		}
		return nil
	}).Maybe()

	d := New(nil, stubClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ac := &attachedClient{tr: tr, rend: renderer.New(renderer.Capabilities{})}
	sess := &session{id: "s", name: "s", tabs: []*tab{win}, client: ac}
	ac.setSession(sess)

	d.resize(sess, ac, newSize)

	require.Equal(t, 80, screenWidthAtResize, "pty.Resize must run before screen.Resize")
	require.Equal(t, 100, win.focusedPane().screen.Frame.Width, "screen resized after pty")
	require.Equal(t, 28, win.focusedPane().screen.Frame.Height, "screen reserves top and bottom chrome rows")
	require.True(t, gotOutput.Load(), "resize forces a full redraw output")
}

// --- reader EOF -> registry-empty shutdown ----------------------------------

func TestSendErrorKillsEphemeral(t *testing.T) {
	p := portsmocks.NewMockPTY(t)
	p.EXPECT().Close().Return(nil).Maybe()

	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(mock.Anything).Return(io.ErrClosedPipe).Maybe()
	tr.EXPECT().Close().Return(nil).Maybe()

	d := newTestDaemon(t, nil, stubClock{})
	win := newTab(p, domain.Size{Cols: 20, Rows: 5})
	sctx, cancel := context.WithCancel(context.Background())
	ac := &attachedClient{tr: tr, rend: renderer.New(renderer.Capabilities{})}
	sess := &session{id: "e", name: "0", ephemeral: true, tabs: []*tab{win}, ctx: sctx, cancel: cancel, client: ac}
	ac.setSession(sess)
	d.sessions[sess.id] = sess

	win.mu.Lock()
	win.focusedPane().screen.Write([]byte("x"))
	win.mu.Unlock()

	d.paint(sess, ac, true) // send fails -> detach -> ephemeral killed

	require.Equal(t, 0, sessionCount(d), "ephemeral session must die when its client's send fails")
	d.waitNotifies()
}

func TestSchedulerDefersPendingDirtyTimerDuringSynchronizedUpdate(t *testing.T) {
	mc := portsmocks.NewMockClock(t)
	mc.EXPECT().Now().Return(time.Now()).Maybe()
	mt := portsmocks.NewMockTimer(t)
	timerCh := make(chan time.Time, 1)
	mc.EXPECT().NewTimer(mock.Anything).Return(mt).Maybe()
	mt.EXPECT().C().Return(timerCh).Maybe()
	mt.EXPECT().Stop().Return(true).Maybe()

	var outputs atomic.Int32
	gotOutput := make(chan struct{}, 1)
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Close().Return(nil).Maybe()
	tr.EXPECT().Send(mock.Anything).RunAndReturn(func(f ports.Frame) error {
		if f.Type == ports.MsgOutput {
			outputs.Add(1)
			select {
			case gotOutput <- struct{}{}:
			default:
			}
		}
		return nil
	}).Maybe()

	d := New(nil, mc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	sctx, cancel := context.WithCancel(context.Background())
	win := newTab(nil, domain.Size{Cols: 20, Rows: 5})
	win.ctx, win.cancel = sctx, cancel
	p := win.focusedPane()
	p.ctx, p.cancel = sctx, cancel
	ac := &attachedClient{tr: tr, rend: renderer.New(renderer.Capabilities{})}
	sess := &session{id: "sync", name: "sync", tabs: []*tab{win}, ctx: sctx, cancel: cancel, client: ac}
	ac.setSession(sess)

	p.mu.Lock()
	p.screen.Write([]byte("before"))
	p.mu.Unlock()

	d.sessWg.Add(1)
	go d.scheduler(sess, win, p)
	p.dirty <- struct{}{}

	p.mu.Lock()
	p.screen.Write([]byte("\x1b[?2026hafter"))
	p.mu.Unlock()
	timerCh <- time.Now()

	select {
	case <-gotOutput:
		t.Fatal("scheduler rendered while synchronized update was active")
	case <-time.After(20 * time.Millisecond):
	}
	require.Equal(t, int32(0), outputs.Load())

	p.mu.Lock()
	p.screen.Write([]byte(" done\x1b[?2026l"))
	p.mu.Unlock()
	p.flush <- struct{}{}
	select {
	case <-gotOutput:
	case <-time.After(time.Second):
		t.Fatal("sync end did not flush deferred damage")
	}

	cancel()
	d.sessWg.Wait()
}

func TestPTYQueryGetsResponseWrittenBackToPTY(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	p := portsmocks.NewMockPTY(t)
	chunks := [][]byte{[]byte("\x1b[6n")}
	writes := make(chan []byte, 1)
	p.EXPECT().Read(mock.Anything).RunAndReturn(func(buf []byte) (int, error) {
		if len(chunks) == 0 {
			return 0, io.EOF
		}
		n := copy(buf, chunks[0])
		chunks = chunks[1:]
		return n, nil
	})
	p.EXPECT().Write(mock.Anything).RunAndReturn(func(b []byte) (int, error) {
		writes <- append([]byte(nil), b...)
		return len(b), nil
	}).Once()
	p.EXPECT().Close().Return(nil).Maybe()

	tr, sends := newCapturingTransport(t)
	sctx, cancel := context.WithCancel(context.Background())
	win := newTestTabWithContext(p, sctx, cancel)
	ac := &attachedClient{tr: tr, rend: renderer.New(renderer.Capabilities{})}
	sess := &session{id: "query", name: "query", tabs: []*tab{win}, ctx: sctx, cancel: cancel, client: ac}
	ac.setSession(sess)
	d.sessions[sess.id] = sess
	d.sessWg.Add(1)
	d.ptyReader(sess, win, win.focusedPane())

	select {
	case got := <-writes:
		require.Equal(t, []byte("\x1b[1;1R"), got)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for PTY response write")
	}
	select {
	case f := <-sends:
		require.NotEqual(t, ports.MsgOutput, f.Type)
	default:
	}
}

func TestPTYReaderSameReadSynchronizedUpdateStartEndFlushesImmediately(t *testing.T) {
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
	win := newTestTabWithContext(p, sctx, cancel)
	sess := &session{id: "sync", name: "sync", tabs: []*tab{win}, ctx: sctx, cancel: cancel}
	d.sessions[sess.id] = sess
	d.sessWg.Add(1)
	d.ptyReader(sess, win, win.focusedPane())

	select {
	case <-win.focusedPane().dirty:
		t.Fatal("same-read synchronized update end should request flush, not dirty")
	default:
	}
	select {
	case <-win.focusedPane().flush:
	case <-time.After(time.Second):
		t.Fatal("same-read synchronized update end did not request immediate flush")
	}
}

func TestPTYReaderDefersDirtyDuringSynchronizedUpdateAndFlushesAtEnd(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	p := portsmocks.NewMockPTY(t)
	chunks := [][]byte{[]byte("\x1b[?2026hhello"), []byte(" world\x1b[?2026l")}
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
	win := newTestTabWithContext(p, sctx, cancel)
	sess := &session{id: "sync", name: "sync", tabs: []*tab{win}, ctx: sctx, cancel: cancel}
	d.sessions[sess.id] = sess
	d.sessWg.Add(1)
	d.ptyReader(sess, win, win.focusedPane())

	select {
	case <-win.focusedPane().dirty:
		t.Fatal("dirty signaled while synchronized update was active")
	default:
	}
	select {
	case <-win.focusedPane().flush:
	case <-time.After(time.Second):
		t.Fatal("sync end did not request immediate flush")
	}
}

func TestSyncUpdateWatchdogAbandonedSyncRenders(t *testing.T) {
	clk := &signalClock{timers: make(chan *signalTimer, 1)}
	d := newTestDaemon(t, nil, clk)
	p := portsmocks.NewMockPTY(t)
	release := make(chan struct{})
	read := 0
	p.EXPECT().Read(mock.Anything).RunAndReturn(func(buf []byte) (int, error) {
		if read == 0 {
			read++
			return copy(buf, []byte("\x1b[?2026habandoned")), nil
		}
		<-release
		return 0, io.EOF
	})
	p.EXPECT().Close().Return(nil).Maybe()

	tr, sends := newCapturingTransport(t)
	sctx, cancel := context.WithCancel(context.Background())
	win := newTestTabWithContext(p, sctx, cancel)
	ac := &attachedClient{tr: tr, rend: renderer.New(renderer.Capabilities{})}
	sess := &session{id: "sync", name: "sync", tabs: []*tab{win}, ctx: sctx, cancel: cancel, client: ac}
	ac.setSession(sess)
	d.sessions[sess.id] = sess
	d.sessWg.Add(2)
	go d.scheduler(sess, win, win.focusedPane())
	go d.ptyReader(sess, win, win.focusedPane())

	timer := <-clk.timers
	timer.ch <- time.Now()
	awaitFrame(t, sends, ports.MsgOutput)

	win.mu.Lock()
	active := win.focusedPane().screen.SyncUpdateActive()
	got := screenLineText(win.focusedPane().screen, 0)
	win.mu.Unlock()
	require.False(t, active)
	require.Contains(t, got, "abandoned")

	close(release)
	cancel()
	d.sessWg.Wait()
}

func TestSyncUpdateWatchdogStaleGenerationNoopAfterEnd(t *testing.T) {
	clk := &signalClock{timers: make(chan *signalTimer, 1)}
	d := newTestDaemon(t, nil, clk)
	p := portsmocks.NewMockPTY(t)
	release := make(chan struct{})
	chunks := [][]byte{[]byte("\x1b[?2026hhello"), []byte(" world\x1b[?2026l")}
	p.EXPECT().Read(mock.Anything).RunAndReturn(func(buf []byte) (int, error) {
		if len(chunks) > 0 {
			n := copy(buf, chunks[0])
			chunks = chunks[1:]
			return n, nil
		}
		<-release
		return 0, io.EOF
	})
	p.EXPECT().Close().Return(nil).Maybe()

	tr, sends := newCapturingTransport(t)
	sctx, cancel := context.WithCancel(context.Background())
	win := newTestTabWithContext(p, sctx, cancel)
	ac := &attachedClient{tr: tr, rend: renderer.New(renderer.Capabilities{})}
	sess := &session{id: "sync", name: "sync", tabs: []*tab{win}, ctx: sctx, cancel: cancel, client: ac}
	ac.setSession(sess)
	d.sessions[sess.id] = sess
	d.sessWg.Add(2)
	go d.scheduler(sess, win, win.focusedPane())
	go d.ptyReader(sess, win, win.focusedPane())

	timer := <-clk.timers
	awaitFrame(t, sends, ports.MsgOutput)
	timer.ch <- time.Now()

	select {
	case f := <-sends:
		if f.Type == ports.MsgOutput {
			t.Fatal("stale synchronized update watchdog produced an extra render")
		}
	case <-time.After(50 * time.Millisecond):
	}
	win.mu.Lock()
	active := win.focusedPane().screen.SyncUpdateActive()
	win.mu.Unlock()
	require.False(t, active)

	close(release)
	cancel()
	d.sessWg.Wait()
}

func TestCursorTailVisibleHideAndMoveOnly(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	win := sess.tabs[0]
	win.focusedPane().screen.Write([]byte("A"))

	d.paint(sess, ac, true)
	data := mustOutputData(t, sends)
	require.Contains(t, string(data), "\x1b[5 q")
	require.Contains(t, string(data), "\x1b[?25h")

	win.focusedPane().screen.Write([]byte("\x1b[2;3H"))
	d.paint(sess, ac, false)
	data = mustOutputData(t, sends)
	require.Contains(t, string(data), "\x1b[3;3H")
	require.Contains(t, string(data), "\x1b[?25h")

	win.focusedPane().screen.Write([]byte("\x1b[?25l"))
	d.paint(sess, ac, false)
	data = mustOutputData(t, sends)
	require.Contains(t, string(data), "\x1b[?25l")
}

func TestCursorTailUsesFocusedPanePlacement(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
	win := sess.tabs[0]
	win.size = domain.Size{Cols: 80, Rows: 23}
	left := win.focusedPane()
	right := newPane("pane-2", nil, domain.Size{Cols: 39, Rows: 23})
	win.panes[right.id] = right
	win.tree = layout.NewTree(left.id)
	require.NoError(t, win.tree.Split(left.id, layout.Right, true, right.id, domain.Rect{Width: 80, Height: 23}))
	win.tree.Focus = right.id
	right.screen.Write([]byte("\x1b[2;3H"))
	placements, ok := layout.Solve(win.tree.Root, domain.Rect{Width: 80, Height: 23})
	require.True(t, ok)
	rightContent := placementContent(placements, right.id)

	d.paint(sess, ac, true)
	data := mustOutputData(t, sends)
	want := cursorCSI(rightContent.Y+right.screen.CursorRow()+2, rightContent.X+right.screen.CursorCol()+1)
	require.Contains(t, string(data), want)
}

func TestCursorTailUsesExpandedStackContentPlacement(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
	win := sess.tabs[0]
	win.size = domain.Size{Cols: 80, Rows: 23}
	one := win.focusedPane()
	two := newPane("pane-2", nil, domain.Size{Cols: 80, Rows: 20})
	three := newPane("pane-3", nil, domain.Size{Cols: 80, Rows: 20})
	win.panes[two.id] = two
	win.panes[three.id] = three
	win.tree = &layout.Tree{
		Root: &layout.Node{Kind: layout.Stack, Children: []*layout.Node{
			layout.NewLeaf(one.id),
			layout.NewLeaf(two.id),
			layout.NewLeaf(three.id),
		}, Expanded: two.id},
		Focus: two.id,
	}
	two.screen.Write([]byte("\x1b[2;3H"))
	placements, ok := layout.Solve(win.tree.Root, domain.Rect{Width: 80, Height: 23})
	require.True(t, ok)
	twoContent := placementContent(placements, two.id)
	require.Greater(t, twoContent.Y, 0, "stack title bars should offset content")

	d.paint(sess, ac, true)
	data := mustOutputData(t, sends)
	want := cursorCSI(twoContent.Y+two.screen.CursorRow()+2, twoContent.X+two.screen.CursorCol()+1)
	require.Contains(t, string(data), want)
}

func cursorCSI(row, col int) string {
	return "\x1b[" + strconv.Itoa(row) + ";" + strconv.Itoa(col) + "H"
}

package daemon

import (
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/picker"
)

// --- test doubles -----------------------------------------------------------

// stubClock returns timers whose channel never fires, so a scheduler under it
// blocks in its debounce loop until the session context is cancelled. Used by

func TestAltDigitSwitchesBetweenThreeTabs(t *testing.T) {
	writes1 := make(chan []byte, 1)
	writes2 := make(chan []byte, 1)
	writes3 := make(chan []byte, 1)
	p1, releasePTY1 := newBlockingPTYWithWrites(t, writes1)
	p2, releasePTY2 := newBlockingPTYWithWrites(t, writes2)
	p3, releasePTY3 := newBlockingPTYWithWrites(t, writes3)
	d := newTestDaemon(t, newFactorySeq(t, p1, p2, p3), stubClock{})
	tr, sends, releaseConn := newConn(t,
		mustHello(ports.IntentNew, "work", domain.Size{Cols: 80, Rows: 24}),
		frameInput([]byte("\x1b ")),
		frameInput([]byte("CNT\r")),
		frameInput([]byte("\x1b ")),
		frameInput([]byte("CNT\r")),
		frameInput([]byte("\x1b1")),
		frameInput([]byte("A")),
		frameInput([]byte("\x1b2")),
		frameInput([]byte("B")),
		frameInput([]byte("\x1b3")),
		frameInput([]byte("C")),
	)

	var hg sync.WaitGroup
	hg.Go(func() { d.handleConn(tr) })
	awaitFrame(t, sends, ports.MsgWelcome)
	awaitFrame(t, sends, ports.MsgOutput)

	require.Eventually(t, func() bool {
		sessions := listSessions(t, d)
		return len(sessions.Sessions) == 1 && sessions.Sessions[0].Tabs == 3
	}, 2*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool { return len(writes1) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, []byte("A"), <-writes1)
	require.Eventually(t, func() bool { return len(writes2) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, []byte("B"), <-writes2)
	require.Eventually(t, func() bool { return len(writes3) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, []byte("C"), <-writes3)

	d.mu.Lock()
	var sess *session
	for _, candidate := range d.sessions {
		sess = candidate
		break
	}
	d.mu.Unlock()
	require.NotNil(t, sess)
	sess.mu.Lock()
	tabs := append([]*tab(nil), sess.tabs...)
	sess.mu.Unlock()
	require.Len(t, tabs, 3)
	for _, tb := range tabs {
		requireFloatingInitialized(t, tb)
	}

	releaseConn()
	releasePTY1()
	releasePTY2()
	releasePTY3()
	hg.Wait()
	d.sessWg.Wait()
}

func TestAltCForwardsToPTY(t *testing.T) {
	writes := make(chan []byte, 2)
	p, releasePTY := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)

	d.handleInput(sess, ac, []byte("\x1bc"))

	require.Equal(t, []byte("\x1bc"), <-writes)
	releasePTY()
}

func requireFloatingInitialized(t *testing.T, tb *tab) {
	t.Helper()
	tb.mu.Lock()
	defer tb.mu.Unlock()
	require.NotEqual(t, floatingUninitialized, tb.floating.state)
}

func installTestFloating(tb *tab, p *pane, visible bool) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.floating.pane = p
	tb.floating.generation++
	if visible {
		tb.floating.state = floatingVisible
	} else {
		tb.floating.state = floatingHidden
	}
}

func TestFloatingKeyboardRoutesVisibleAndHiddenInput(t *testing.T) {
	for _, tc := range []struct {
		name    string
		visible bool
		input   []byte
	}{
		{name: "visible ordinary bytes", visible: true, input: []byte("hello")},
		{name: "visible escape", visible: true, input: []byte("\x1b")},
		{name: "hidden ordinary bytes", visible: false, input: []byte("hello")},
		{name: "hidden escape", visible: false, input: []byte("\x1b")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			normalWrites := make(chan []byte, 1)
			floatingWrites := make(chan []byte, 1)
			normal, releaseNormal := newBlockingPTYWithWrites(t, normalWrites)
			floating, releaseFloating := newBlockingPTYWithWrites(t, floatingWrites)
			defer releaseNormal()
			defer releaseFloating()
			d, sess, ac, _ := newManualSessionWithPTYs(t, normal)
			installTestFloating(sess.activeTab(), newPane("floating", floating, domain.Size{Cols: 20, Rows: 5}), tc.visible)

			daemonKeyHandler{d: d, ac: ac}.Forward(tc.input)
			if tc.visible {
				requirePTYWrite(t, floatingWrites, tc.input)
				requireNoPTYWrite(t, normalWrites)
			} else {
				requirePTYWrite(t, normalWrites, tc.input)
				requireNoPTYWrite(t, floatingWrites)
			}
		})
	}
}

func TestFloatingBracketedPasteAndGlobalActions(t *testing.T) {
	normalWrites := make(chan []byte, 1)
	floatingWrites := make(chan []byte, 1)
	normal, releaseNormal := newBlockingPTYWithWrites(t, normalWrites)
	floating, releaseFloating := newBlockingPTYWithWrites(t, floatingWrites)
	defer releaseNormal()
	defer releaseFloating()
	d, sess, ac, _ := newManualSessionWithPTYs(t, normal)
	installTestFloating(sess.activeTab(), newPane("floating", floating, domain.Size{Cols: 20, Rows: 5}), true)

	paste := []byte("\x1b[200~paste\x1bf\x1b[201~")
	d.handleInput(sess, ac, paste)
	requirePTYWrite(t, floatingWrites, paste)
	d.handleInput(sess, ac, []byte("\x1b "))
	require.True(t, ac.overlays.paletteActive(), "global palette action must still intercept")
	requireNoPTYWrite(t, normalWrites)
}

func TestFloatingStaysTerminalTargetAfterDirectionalFocus(t *testing.T) {
	writes := make(chan []byte, 1)
	floatingWrites := make(chan []byte, 1)
	normal, releaseNormal := newBlockingPTYWithWrites(t, writes)
	floating, releaseFloating := newBlockingPTYWithWrites(t, floatingWrites)
	defer releaseNormal()
	defer releaseFloating()
	d, sess, ac, _ := newManualSessionWithPTYs(t, normal)
	tb := sess.activeTab()
	second := newPane("pane-2", nil, domain.Size{Cols: 40, Rows: 5})
	tb.mu.Lock()
	tb.size = domain.Size{Cols: 81, Rows: 5}
	tb.tree.Root = &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}
	tb.panes["pane-2"] = second
	tb.mu.Unlock()
	installTestFloating(tb, newPane("floating", floating, domain.Size{Cols: 20, Rows: 5}), true)

	daemonKeyHandler{d: d, ac: ac}.Action(keys.ActionFocusPaneRight)
	require.Equal(t, layout.PaneID("pane-2"), tb.tree.Focus)
	daemonKeyHandler{d: d, ac: ac}.Forward([]byte("x"))
	requirePTYWrite(t, floatingWrites, []byte("x"))
}

func TestFloatingVisibilityRemainsIndependentAcrossTabSwitches(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, nil)
	first := sess.activeTab()
	second := newTab(nil, first.size)
	installTestFloating(first, newPane("floating", nil, domain.Size{Cols: 20, Rows: 5}), true)
	installTestFloating(second, newPane("floating", nil, domain.Size{Cols: 20, Rows: 5}), false)
	sess.mu.Lock()
	sess.tabs = append(sess.tabs, second)
	sess.mu.Unlock()

	daemonKeyHandler{d: d, ac: ac}.Action(keys.ActionSwitchTab2)
	second.mu.Lock()
	require.Equal(t, floatingHidden, second.floating.state)
	second.mu.Unlock()

	daemonKeyHandler{d: d, ac: ac}.Action(keys.ActionSwitchTab1)
	first.mu.Lock()
	require.Equal(t, floatingVisible, first.floating.state)
	first.mu.Unlock()
}

func TestFloatingMouseTranslatesSGRToInnerCoordinates(t *testing.T) {
	normal := portsmocks.NewMockPTY(t)
	floatingPTY := portsmocks.NewMockPTY(t)
	floatingPTY.EXPECT().Write([]byte("\x1b[<0;1;1M")).Return(len("\x1b[<0;1;1M"), nil).Once()
	d, sess, ac, _ := newManualSessionWithPTYs(t, normal)
	tb := sess.activeTab()
	tb.mu.Lock()
	tb.size = domain.Size{Cols: 100, Rows: 20}
	tb.mu.Unlock()
	floating := newPane("floating", floatingPTY, domain.Size{Cols: 78, Rows: 14})
	floating.screen.Write([]byte("\x1b[?1000h\x1b[?1006h"))
	installTestFloating(tb, floating, true)
	geometry := calculateContentFloatingGeometry(domain.Size{Cols: 100, Rows: 20}, d.currentFloatingConfig())
	raw := []byte(fmt.Sprintf("\x1b[<0;%d;%dM", geometry.Inner.X+1, geometry.Inner.Y+2))

	d.handleInput(sess, ac, raw)
}

func TestFloatingMouseIgnoresBorderAndOutside(t *testing.T) {
	normal := portsmocks.NewMockPTY(t)
	floatingPTY := portsmocks.NewMockPTY(t)
	d, sess, ac, _ := newManualSessionWithPTYs(t, normal)
	tb := sess.activeTab()
	tb.mu.Lock()
	tb.size = domain.Size{Cols: 100, Rows: 20}
	tb.mu.Unlock()
	floating := newPane("floating", floatingPTY, domain.Size{Cols: 78, Rows: 14})
	floating.screen.Write([]byte("\x1b[?1000h\x1b[?1006h"))
	installTestFloating(tb, floating, true)
	geometry := calculateContentFloatingGeometry(domain.Size{Cols: 100, Rows: 20}, d.currentFloatingConfig())

	border := []byte(fmt.Sprintf("\x1b[<0;%d;%dM", geometry.Bounds.X+1, geometry.Bounds.Y+2))
	outside := []byte("\x1b[<0;1;2M")
	d.handleInput(sess, ac, border)
	d.handleInput(sess, ac, outside)
}

func TestBracketedMultilinePasteForwardsDelimitersAndNewlines(t *testing.T) {
	writes := make(chan []byte, 1)
	p, releasePTY := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	pane := sess.tabs[0].focusedPane()
	pane.screen.Write([]byte("\x1b[?2004h"))
	require.True(t, pane.screen.BracketedPasteMode())

	paste := []byte("\x1b[200~first line\nsecond line\n\x1b[201~")
	d.handleInput(sess, ac, paste)

	require.Equal(t, paste, <-writes)
	releasePTY()
}

func TestOverlayInputPrecedence(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T, d *Daemon, sess *session, ac *attachedClient, sends chan ports.Frame)
		input []byte
		check func(t *testing.T, ac *attachedClient, writes chan []byte)
	}{
		{
			name: "prompt before palette picker copy and normal",
			setup: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient, sends chan ports.Frame) {
				t.Helper()
				d.enterCopyMode(sess, ac)
				d.enterPicker(sess, ac)
				d.enterPalette(sess, ac)
				d.enterPrompt(sess, ac, "Rename", "", func(string) error { return nil })
			},
			input: []byte("x"),
			check: func(t *testing.T, ac *attachedClient, writes chan []byte) {
				t.Helper()
				require.Equal(t, "x", ac.overlays.prompt.Value())
				require.Equal(t, "", ac.overlays.palette.Query())
				require.True(t, ac.overlays.pickerActive())
				require.True(t, ac.overlays.copyActive())
				requireNoPTYWrite(t, writes)
			},
		},
		{
			name: "palette before picker copy and normal",
			setup: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient, sends chan ports.Frame) {
				t.Helper()
				d.enterCopyMode(sess, ac)
				d.enterPicker(sess, ac)
				d.enterPalette(sess, ac)
			},
			input: []byte("x"),
			check: func(t *testing.T, ac *attachedClient, writes chan []byte) {
				t.Helper()
				require.Equal(t, "x", ac.overlays.palette.Query())
				require.True(t, ac.overlays.pickerActive())
				require.True(t, ac.overlays.copyActive())
				requireNoPTYWrite(t, writes)
			},
		},
		{
			name: "picker before copy and normal",
			setup: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient, sends chan ports.Frame) {
				t.Helper()
				d.enterCopyMode(sess, ac)
				d.enterPicker(sess, ac)
			},
			input: []byte("j"),
			check: func(t *testing.T, ac *attachedClient, writes chan []byte) {
				t.Helper()
				target, ok := ac.overlays.picker.Selected()
				require.True(t, ok)
				require.Equal(t, 1, target.TabIndex)
				require.True(t, ac.overlays.copyActive())
				requireNoPTYWrite(t, writes)
			},
		},
		{
			name: "copy before normal",
			setup: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient, sends chan ports.Frame) {
				t.Helper()
				d.enterCopyMode(sess, ac)
			},
			input: []byte("q"),
			check: func(t *testing.T, ac *attachedClient, writes chan []byte) {
				t.Helper()
				require.False(t, ac.overlays.copyActive())
				requireNoPTYWrite(t, writes)
			},
		},
		{
			name:  "normal keys when no overlay is active",
			input: []byte("Z"),
			check: func(t *testing.T, ac *attachedClient, writes chan []byte) {
				t.Helper()
				require.Equal(t, []byte("Z"), <-writes)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writes := make(chan []byte, 1)
			p, releasePTY := newBlockingPTYWithWrites(t, writes)
			p2, releasePTY2 := newBlockingPTY(t)
			d, sess, ac, sends := newManualSessionWithPTYs(t, p, p2)
			defer releasePTY()
			defer releasePTY2()
			if tc.setup != nil {
				tc.setup(t, d, sess, ac, sends)
			}

			d.handleInput(sess, ac, tc.input)

			tc.check(t, ac, writes)
		})
	}
}

func requirePTYWrite(t *testing.T, writes chan []byte, want []byte) {
	t.Helper()
	select {
	case got := <-writes:
		require.Equal(t, want, got)
	case <-time.After(time.Second):
		t.Fatalf("PTY did not receive %q", want)
	}
}

func requireNoPTYWrite(t *testing.T, writes chan []byte) {
	t.Helper()
	select {
	case got := <-writes:
		t.Fatalf("input forwarded to PTY: %q", got)
	default:
	}
}

func TestPaletteNextPreviousSwitchActiveTab(t *testing.T) {
	cases := []struct {
		name      string
		start     int
		query     []byte
		wantIndex int
	}{
		{name: "next advances", start: 0, query: []byte("NXT\r"), wantIndex: 1},
		{name: "next wraps", start: 2, query: []byte("NXT\r"), wantIndex: 0},
		{name: "previous moves back", start: 2, query: []byte("PVT\r"), wantIndex: 1},
		{name: "previous wraps", start: 0, query: []byte("PVT\r"), wantIndex: 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, sess, ac, _, releases := newManualTabSession(t, 3)
			defer func() {
				for _, release := range releases {
					release()
				}
			}()
			sess.active = tc.start

			d.handleInput(sess, ac, []byte("\x1b "))
			d.handleInput(sess, ac, tc.query)

			require.Equal(t, tc.wantIndex, activeTabIndex(sess))
		})
	}
}

func TestPaletteBackSessionTogglesPreviousSession(t *testing.T) {
	d, current, ac, _, releases := newRecentNavigationTestSessions(t)
	defer releaseAll(releases)
	recent := d.sessions[domain.SessionID("recent")]

	// Picker-style successful transition records its origin.
	d.switchToTarget(current, ac, picker.Target{Session: recent.id, TabIndex: -1})
	for _, sess := range []*session{current, recent, current} {
		runPaletteCommand(t, d, ac.currentSession(), ac, "BSK")
		require.Same(t, sess, ac.currentSession())
	}
}

func TestPaletteBackSessionDoesNotMoveWithoutValidTarget(t *testing.T) {
	d, current, ac, _, releases := newRecentNavigationTestSessions(t)
	defer releaseAll(releases)
	for _, tc := range []struct {
		name    string
		prepare func()
	}{
		{name: "no target", prepare: func() {}},
		{name: "stale target", prepare: func() { ac.previousSession.Set(&session{id: "gone"}) }},
		{name: "same session", prepare: func() { ac.previousSession.Set(current) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ac.clearPreviousSession()
			tc.prepare()
			runPaletteCommand(t, d, current, ac, "BSK")
			require.Same(t, current, ac.currentSession())
		})
	}
}

func newRecentNavigationTestSessions(t *testing.T) (*Daemon, *session, *attachedClient, chan ports.Frame, []func()) {
	t.Helper()
	p1, release1 := newBlockingPTY(t)
	p2, release2 := newBlockingPTY(t)
	p3, release3 := newBlockingPTY(t)
	d, current, ac, sends := newManualSessionWithPTYs(t, p1)
	current.id = "current"
	delete(d.sessions, domain.SessionID("manual"))
	d.sessions[current.id] = current
	recent := &session{id: "recent", name: "recent", ctx: current.ctx, cancel: func() {}, tabs: []*tab{newTab(p2, domain.Size{Cols: 80, Rows: 23})}}
	older := &session{id: "older", name: "older", ctx: current.ctx, cancel: func() {}, tabs: []*tab{newTab(p3, domain.Size{Cols: 80, Rows: 23})}}
	d.sessions[recent.id] = recent
	d.sessions[older.id] = older
	current.mruAt.Store(30)
	recent.mruAt.Store(20)
	older.mruAt.Store(10)
	return d, current, ac, sends, []func(){release1, release2, release3}
}

func runPaletteCommand(t *testing.T, d *Daemon, sess *session, ac *attachedClient, code string) {
	t.Helper()
	d.handleInput(sess, ac, []byte("\x1b "))
	d.handleInput(sess, ac, []byte(code+"\r"))
}

func releaseAll(releases []func()) {
	for _, release := range releases {
		release()
	}
}

func TestAltXClosesActiveTabAndSelectsRemaining(t *testing.T) {
	writes := make(chan []byte, 1)
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	p3, releasePTY3 := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p1, p2, p3)
	sess.active = 1

	d.handleInput(sess, ac, []byte("\x1b "))
	d.handleInput(sess, ac, []byte("CLT\r"))

	require.Equal(t, 1, sessionCount(d))
	require.Len(t, sess.tabs, 2)
	require.Equal(t, 1, activeTabIndex(sess), "closing middle tab selects the next remaining tab")
	d.handleInput(sess, ac, []byte("Z"))
	require.Eventually(t, func() bool { return len(writes) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, []byte("Z"), <-writes)

	releasePTY1()
	releasePTY2()
	releasePTY3()
}

func TestAltDDetachesCurrentClient(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 1)
	defer releases[0]()

	d.handleInput(sess, ac, []byte("\x1b "))
	d.handleInput(sess, ac, []byte("DET\r"))

	require.Nil(t, sess.client)
	f := awaitFrame(t, sends, ports.MsgDetached)
	det, err := ports.UnmarshalDetached(f.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ReasonDetach, det.Reason)
}

func TestRNSOpensPromptAndEnterPromotesEphemeralSession(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 1)
	defer releases[0]()
	sess.ephemeral = true
	sess.name = "0"

	d.handleInput(sess, ac, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("RNS\r"))
	awaitFrame(t, sends, ports.MsgOutput)
	require.True(t, ac.overlays.promptActive())

	d.handleInput(sess, ac, []byte("\r"))
	awaitFrame(t, sends, ports.MsgOutput)

	require.False(t, ac.overlays.promptActive())
	require.False(t, sess.ephemeral)
	require.Equal(t, "0", sess.name)
}

func TestRNTOpensPromptAndRenamesActiveTab(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 2)
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	sess.active = 1

	d.handleInput(sess, ac, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("RNT\r"))
	out := awaitFrame(t, sends, ports.MsgOutput)
	require.True(t, ac.overlays.promptActive())
	msg, err := ports.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	require.Contains(t, string(msg.Data), "> 2")

	d.handleInput(sess, ac, []byte("\x7flogs\r"))
	awaitFrame(t, sends, ports.MsgOutput)

	require.False(t, ac.overlays.promptActive())
	require.Equal(t, "logs", sess.tabs[1].name)
	require.Empty(t, sess.tabs[0].name)
}

func TestMouseWheelEntersScrollbackModeAndExitsAtBottom(t *testing.T) {
	writes := make(chan []byte, 4)
	p, _ := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	win := sess.tabs[0]
	win.focusedPane().screen.Write([]byte("live"))

	d.handleInput(sess, ac, []byte("\x1b[<64;1;1M"))
	data := mustOutputData(t, sends)
	require.NotNil(t, ac.overlays.copyMode)
	require.Equal(t, 19, ac.overlays.copyMode.Cursor)
	require.Contains(t, string(data), "[SCROLL]")

	d.handleInput(sess, ac, []byte("\x1b[<65;1;1M"))
	mustOutputData(t, sends)
	require.Nil(t, ac.overlays.copyMode)

	d.handleInput(sess, ac, []byte("\x1b[<64;1;1Mq"))
	mustOutputData(t, sends)
	mustOutputData(t, sends)
	require.Nil(t, ac.overlays.copyMode, "q after wheel in same input must be routed after copy mode is entered")
	select {
	case got := <-writes:
		t.Fatalf("mouse/copy input forwarded to PTY: %q", got)
	default:
	}
}

func TestMouseAltScreenWheelMapsToArrows(t *testing.T) {
	writes := make(chan []byte, 2)
	p, _ := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	sess.tabs[0].focusedPane().screen.Write([]byte("\x1b[?1049h"))

	d.handleInput(sess, ac, []byte("\x1b[<64;1;1M"))
	d.handleInput(sess, ac, []byte("\x1b[<65;1;1M"))

	require.Equal(t, []byte("\x1b[A\x1b[A\x1b[A"), <-writes)
	require.Equal(t, []byte("\x1b[B\x1b[B\x1b[B"), <-writes)
	require.Nil(t, ac.overlays.copyMode)
}

func TestSGRRowOffset(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		raw   []byte
		delta int
		want  []byte
	}{
		{name: "decrements press row", raw: []byte("\x1b[<0;2;3M"), delta: -1, want: []byte("\x1b[<0;2;2M")},
		{name: "increments release row", raw: []byte("\x1b[<0;2;3m"), delta: 2, want: []byte("\x1b[<0;2;5m")},
		{name: "leaves empty unchanged", raw: []byte(""), delta: -1, want: []byte("")},
		{name: "leaves non sgr unchanged", raw: []byte("abc"), delta: -1, want: []byte("abc")},
		{name: "leaves malformed fields unchanged", raw: []byte("\x1b[<0;2M"), delta: -1, want: []byte("\x1b[<0;2M")},
		{name: "leaves non numeric row unchanged", raw: []byte("\x1b[<0;2;xM"), delta: -1, want: []byte("\x1b[<0;2;xM")},
		{name: "leaves invalid shifted row unchanged", raw: []byte("\x1b[<0;2;1M"), delta: -1, want: []byte("\x1b[<0;2;1M")},
		{name: "handles digit width increase", raw: []byte("\x1b[<0;2;9M"), delta: 1, want: []byte("\x1b[<0;2;10M")},
		{name: "handles digit width decrease", raw: []byte("\x1b[<0;2;10m"), delta: -1, want: []byte("\x1b[<0;2;9m")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, sgrRowOffset(tc.raw, tc.delta))
		})
	}
}

func TestMouseChildForwardingStatusDropAndPressDrop(t *testing.T) {
	writes := make(chan []byte, 4)
	p, _ := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)

	d.handleInput(sess, ac, []byte("\x1b[<0;1;1M"))
	select {
	case got := <-writes:
		t.Fatalf("press without child mouse mode forwarded: %q", got)
	default:
	}
	require.Nil(t, ac.overlays.copyMode, "press alone must not enter visual mode")

	sess.tabs[0].focusedPane().screen.Write([]byte("\x1b[?1000h"))
	raw := []byte("\x1b[<0;2;3M")
	d.handleInput(sess, ac, raw)
	select {
	case got := <-writes:
		t.Fatalf("SGR report forwarded to child without SGR mode: %q", got)
	default:
	}

	sess.tabs[0].focusedPane().screen.Write([]byte("\x1b[?1006h"))
	d.handleInput(sess, ac, raw)
	require.Equal(t, []byte("\x1b[<0;2;2M"), <-writes)

	d.handleInput(sess, ac, []byte("\x1b[<0;1;1M"))
	select {
	case got := <-writes:
		t.Fatalf("top-row mouse report forwarded: %q", got)
	default:
	}

	statusRowReport := []byte("\x1b[<0;1;25M")
	d.handleInput(sess, ac, statusRowReport)
	select {
	case got := <-writes:
		t.Fatalf("status-row mouse report forwarded: %q", got)
	default:
	}

	sess.tabs[0].focusedPane().screen.Write([]byte("\x1b[?1006l\x1b[?1000l"))
	d.handleInput(sess, ac, []byte("\x1b[<0;1;1M"))
	require.Nil(t, ac.overlays.copyMode, "press alone must still not enter visual mode")
	d.handleInput(sess, ac, []byte("\x1b[<32;1;3M"))
	mustOutputData(t, sends)
	require.NotNil(t, ac.overlays.copyMode)
	lo, hi, ok := ac.overlays.copyMode.SelectedBounds()
	require.True(t, ok)
	require.Equal(t, 0, lo)
	require.Equal(t, 2, hi)
}

func TestCopyModeMouseDragYanksOSC52AndExits(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	copy(sess.tabs[0].focusedPane().screen.Frame.Row(0), testRow("alpha"))
	copy(sess.tabs[0].focusedPane().screen.Frame.Row(1), testRow("bravo"))
	copy(sess.tabs[0].focusedPane().screen.Frame.Row(2), testRow("charlie"))

	d.enterCopyMode(sess, ac)
	mustOutputData(t, sends)
	d.handleInput(sess, ac, []byte("\x1b[<0;1;1M\x1b[<32;1;2M"))
	mustOutputData(t, sends)
	require.NotNil(t, ac.overlays.copyMode)
	lo, hi, ok := ac.overlays.copyMode.SelectedBounds()
	require.True(t, ok)
	require.Equal(t, 0, lo)
	require.Equal(t, 1, hi)

	d.handleInput(sess, ac, []byte("y"))
	data := ""
	require.Eventually(t, func() bool {
		data = string(mustOutputData(t, sends))
		return strings.HasPrefix(data, "\x1b]52;c;")
	}, 2*time.Second, 5*time.Millisecond, "OSC52 output = %q", data)
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSuffix(strings.TrimPrefix(data, "\x1b]52;c;"), "\a"))
	require.NoError(t, err)
	require.Equal(t, "alpha\nbravo", string(decoded))
	require.Nil(t, ac.overlays.copyMode)

	exitPaint := string(mustOutputData(t, sends))
	require.NotContains(t, exitPaint, "[SELECT]")
}

// TestMouseNormalScreenStatusRowClearsStalePressState covers a regression
// where a Press or Release landing on the status row (row >= childRows)
// returned before the inner event-type switch ran, leaving
// normalMousePressValid untouched. A later Motion on a content row would
// then resurrect the stale anchor and start a selection out of thin air.
// The fix requires Release to always clear the press state and Press on the
// status row to clear it too, so a following Motion is a no-op.
func TestMouseNormalScreenStatusRowClearsStalePressState(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	require.Equal(t, 23, sess.tabs[0].focusedPane().screen.Frame.Height, "fixture assumption: status row is wire row 24")

	// Press on a content row establishes a (soon to be stale) anchor.
	d.handleInput(sess, ac, []byte("\x1b[<0;1;1M"))
	require.Nil(t, ac.overlays.copyMode)

	// Release lands on the status row: must still clear the press state,
	// regardless of the row it landed on.
	d.handleInput(sess, ac, []byte("\x1b[<0;1;24m"))

	// A brand new button hold starts with a Press on the status row: must
	// clear (not silently preserve) any prior press state.
	d.handleInput(sess, ac, []byte("\x1b[<0;1;24M"))

	// Motion onto a content row must not resurrect a stale anchor.
	d.handleInput(sess, ac, []byte("\x1b[<32;1;3M"))

	require.Nil(t, ac.overlays.copyMode, "stale press state must not resurrect a selection")
}

// TestCopyModeStatusRowPressClearsDragState covers the copy-mode counterpart
// of the same bug class: copyMouse ignored any event whose row landed on the
// status row before updating copyPressRow/copyPressRowValid/copyDragging, so
// a Press on the status row left the previous drag state in place and a
// following Motion on a content row silently extended the old selection
// instead of being a no-op.
func TestCopyModeStatusRowPressClearsDragState(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	copy(sess.tabs[0].focusedPane().screen.Frame.Row(0), testRow("alpha"))
	copy(sess.tabs[0].focusedPane().screen.Frame.Row(1), testRow("bravo"))
	copy(sess.tabs[0].focusedPane().screen.Frame.Row(2), testRow("charlie"))
	require.Equal(t, 23, sess.tabs[0].focusedPane().screen.Frame.Height, "fixture assumption: status row is wire row 24")

	d.enterCopyMode(sess, ac)
	mustOutputData(t, sends)

	// Full drag: press row0, motion to row1 -> selection [0,1].
	d.handleInput(sess, ac, []byte("\x1b[<0;1;1M\x1b[<32;1;2M"))
	mustOutputData(t, sends)
	require.NotNil(t, ac.overlays.copyMode)
	lo, hi, ok := ac.overlays.copyMode.SelectedBounds()
	require.True(t, ok)
	require.Equal(t, 0, lo)
	require.Equal(t, 1, hi)

	// Release, then a new button hold starting with Press on the status row.
	d.handleInput(sess, ac, []byte("\x1b[<0;1;2m"))
	d.handleInput(sess, ac, []byte("\x1b[<0;1;24M"))

	// Motion onto a content row must be a no-op: the status-row press must
	// have cleared copyPressRowValid/copyDragging.
	d.handleInput(sess, ac, []byte("\x1b[<32;1;3M"))

	require.NotNil(t, ac.overlays.copyMode)
	lo, hi, ok = ac.overlays.copyMode.SelectedBounds()
	require.True(t, ok)
	require.Equal(t, 0, lo, "anchor must not have moved")
	require.Equal(t, 1, hi, "status-row press must invalidate the drag so the motion is a no-op")
}

// TestMouseNormalScreenDragExtendsToCurrentScrollbackOffset covers a
// regression where the first drag Motion extended the selection using the
// scrollback length captured at Press time instead of the current
// scrollback length, so if lines were evicted into scrollback between the
// Press and the first Motion, the extend target landed on the wrong row.
func TestMouseNormalScreenDragExtendsToCurrentScrollbackOffset(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	sess.tabs[0].focusedPane().scrollback = scopy.NewScrollback(50)
	require.Equal(t, 23, sess.tabs[0].focusedPane().screen.Frame.Height, "fixture assumption: status row is wire row 24")

	// Press on a content row while scrollback is empty: the anchor is
	// content-stable at pressTop(0)+pressRow(0) = row 0.
	d.handleInput(sess, ac, []byte("\x1b[<0;1;1M"))

	// Simulate 5 lines evicted into scrollback between the Press and the
	// first Motion (e.g. the child kept producing output).
	for range 5 {
		sess.tabs[0].focusedPane().scrollback.Append(testRow("evicted"))
	}

	// Motion lands on wire row 3 (0-based row 2 of the *current* screen).
	// The extend target must track the pointer's current content row:
	// current scrollback length (5) + ev.Row (2) = 7 -- not the stale
	// pressTop(0)+ev.Row(2) = 2.
	d.handleInput(sess, ac, []byte("\x1b[<32;1;3M"))

	require.NotNil(t, ac.overlays.copyMode)
	lo, hi, ok := ac.overlays.copyMode.SelectedBounds()
	require.True(t, ok)
	require.Equal(t, 0, lo, "anchor stays content-stable at the row under the pointer at press time")
	require.Equal(t, 7, hi, "extend target must track the pointer's current content row, not a stale scrollback offset")
}

func TestMouseSplitReportPreservesOrder(t *testing.T) {
	writes := make(chan []byte, 2)
	p, _ := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	sess.tabs[0].focusedPane().screen.Write([]byte("live"))

	d.handleInput(sess, ac, []byte("\x1b[<64;"))
	d.handleInput(sess, ac, []byte("1;1Mq"))

	mustOutputData(t, sends)
	mustOutputData(t, sends)
	require.Nil(t, ac.overlays.copyMode)
	select {
	case got := <-writes:
		t.Fatalf("split mouse/copy bytes forwarded to PTY: %q", got)
	default:
	}
}

func TestMouseWheelOverUnfocusedPaneDoesNotFocusAndForwardsChildMouse(t *testing.T) {
	p1 := portsmocks.NewMockPTY(t)
	p1.EXPECT().Resize(domain.Size{Cols: 20, Rows: 5}).Return(nil).Maybe()
	p2 := portsmocks.NewMockPTY(t)
	p2.EXPECT().Resize(domain.Size{Cols: 20, Rows: 5}).Return(nil).Maybe()
	p2.EXPECT().Write([]byte("\x1b[<64;1;1M")).Return(len("\x1b[<64;1;1M"), nil).Once()
	d, sess, ac, _ := newManualSessionWithPTYs(t, p1)
	d.procComm = nil
	tb := sess.activeTab()
	p2pane := newPane("pane-2", p2, domain.Size{Cols: 20, Rows: 5})
	p2pane.screen.Write([]byte("\x1b[?1000h\x1b[?1006h"))
	tb.mu.Lock()
	tb.size = domain.Size{Cols: 41, Rows: 5}
	tb.tree.Root = &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}
	tb.tree.Focus = "pane-1"
	tb.panes["pane-2"] = p2pane
	tb.mu.Unlock()

	d.handleInput(sess, ac, []byte("\x1b[<64;22;2M"))

	require.Equal(t, layout.PaneID("pane-1"), tb.tree.Focus)
}

func TestMouseDividerAndTitleBarDoNotForwardBogusCoordinates(t *testing.T) {
	cases := []struct {
		name string
		raw  []byte
		root *layout.Node
		size domain.Size
	}{
		{
			name: "divider",
			raw:  []byte("\x1b[<0;21;2M"),
			root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}},
			size: domain.Size{Cols: 41, Rows: 5},
		},
		{
			name: "expanded title bar",
			raw:  []byte("\x1b[<0;1;1M"),
			root: &layout.Node{Kind: layout.Stack, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}, Expanded: "pane-1"},
			size: domain.Size{Cols: 20, Rows: 5},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p1 := portsmocks.NewMockPTY(t)
			p1.EXPECT().Resize(mock.Anything).Return(nil).Maybe()
			p2 := portsmocks.NewMockPTY(t)
			p2.EXPECT().Resize(mock.Anything).Return(nil).Maybe()
			d, sess, ac, _ := newManualSessionWithPTYs(t, p1)
			d.procComm = nil
			tb := sess.activeTab()
			tb.focusedPane().screen.Write([]byte("\x1b[?1000h\x1b[?1006h"))
			tb.mu.Lock()
			tb.size = tc.size
			tb.tree.Root = tc.root
			tb.tree.Focus = "pane-1"
			tb.panes["pane-2"] = newPane("pane-2", p2, tc.size)
			tb.mu.Unlock()

			d.handleInput(sess, ac, tc.raw)

			require.Equal(t, layout.PaneID("pane-1"), tb.tree.Focus)
		})
	}
}

func TestCopyModeDragOutsideSplitPaneClampsToPaneContent(t *testing.T) {
	p1 := portsmocks.NewMockPTY(t)
	p1.EXPECT().Resize(domain.Size{Cols: 20, Rows: 10}).Return(nil).Maybe()
	p2 := portsmocks.NewMockPTY(t)
	p2.EXPECT().Resize(domain.Size{Cols: 20, Rows: 10}).Return(nil).Maybe()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p1)
	d.procComm = nil
	tb := sess.activeTab()
	for i := range 10 {
		copy(tb.focusedPane().screen.Frame.Row(i), testRow(string(rune('a'+i))))
	}
	tb.mu.Lock()
	tb.size = domain.Size{Cols: 41, Rows: 10}
	tb.tree.Root = &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}
	tb.tree.Focus = "pane-1"
	tb.panes["pane-2"] = newPane("pane-2", p2, domain.Size{Cols: 20, Rows: 10})
	tb.mu.Unlock()
	d.enterCopyMode(sess, ac)
	mustOutputData(t, sends)

	d.handleInput(sess, ac, []byte("\x1b[<0;1;1M\x1b[<32;1;12M"))
	mustOutputData(t, sends)

	lo, hi, ok := ac.overlays.copyMode.SelectedBounds()
	require.True(t, ok)
	require.Equal(t, 0, lo)
	require.Equal(t, 9, hi)
}

func TestMouseHitTestFocusesPaneAndTranslatesSGRColumns(t *testing.T) {
	p1 := portsmocks.NewMockPTY(t)
	p1.EXPECT().Resize(domain.Size{Cols: 20, Rows: 5}).Return(nil).Maybe()
	p2 := portsmocks.NewMockPTY(t)
	p2.EXPECT().Resize(domain.Size{Cols: 20, Rows: 5}).Return(nil).Maybe()
	p2.EXPECT().Write([]byte("\x1b[<0;1;1M")).Return(len("\x1b[<0;1;1M"), nil).Once()
	d, sess, ac, _ := newManualSessionWithPTYs(t, p1)
	d.procComm = nil
	tb := sess.activeTab()
	p2pane := newPane("pane-2", p2, domain.Size{Cols: 20, Rows: 5})
	p2pane.screen.Write([]byte("\x1b[?1000h\x1b[?1006h"))
	tb.mu.Lock()
	tb.size = domain.Size{Cols: 41, Rows: 5}
	tb.tree.Root = &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}
	tb.tree.Focus = "pane-1"
	tb.panes["pane-2"] = p2pane
	tb.mu.Unlock()

	d.handleInput(sess, ac, []byte("\x1b[<0;22;2M"))

	require.Equal(t, layout.PaneID("pane-2"), tb.tree.Focus)
}

func TestMouseCollapsedStackBarExpandsAndFocuses(t *testing.T) {
	p1 := portsmocks.NewMockPTY(t)
	p1.EXPECT().Resize(domain.Size{Cols: 20, Rows: 3}).Return(nil).Maybe()
	p2 := portsmocks.NewMockPTY(t)
	p2.EXPECT().Resize(domain.Size{Cols: 20, Rows: 3}).Return(nil).Maybe()
	d, sess, ac, _ := newManualSessionWithPTYs(t, p1)
	d.procComm = nil
	tb := sess.activeTab()
	p2pane := newPane("pane-2", p2, domain.Size{Cols: 20, Rows: 3})
	tb.mu.Lock()
	tb.size = domain.Size{Cols: 20, Rows: 5}
	tb.tree.Root = &layout.Node{Kind: layout.Stack, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}, Expanded: "pane-1"}
	tb.tree.Focus = "pane-1"
	tb.panes["pane-2"] = p2pane
	tb.mu.Unlock()

	d.handleInput(sess, ac, []byte("\x1b[<0;1;6M"))

	require.Equal(t, layout.PaneID("pane-2"), tb.tree.Focus)
	require.Equal(t, layout.PaneID("pane-2"), tb.tree.Root.Expanded)
}

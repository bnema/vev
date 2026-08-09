package daemon

import (
	"context"
	"encoding/base64"
	"errors"
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
	"github.com/bnema/vev/internal/usecase/mouse"
	"github.com/bnema/vev/internal/usecase/picker"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/vt"
)

// --- test doubles -----------------------------------------------------------

type blockingInputActionRunner struct {
	admitted chan<- struct{}
	release  <-chan struct{}
	mu       *sync.Mutex
	order    *[]string
}

func (r *blockingInputActionRunner) Run(daemonActionRequest) error {
	close(r.admitted)
	<-r.release
	r.mu.Lock()
	*r.order = append(*r.order, "keyboard")
	r.mu.Unlock()
	return nil
}

func TestAttachedKeyboardMutationSharesDispatchBoundaryWithCommand(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "t_work", "p_work")
	ac := &attachedClient{}
	ac.setSession(sess)

	var orderMu sync.Mutex
	var order []string
	admitted := make(chan struct{})
	release := make(chan struct{})
	runner := &blockingInputActionRunner{admitted: admitted, release: release, mu: &orderMu, order: &order}
	handler := daemonKeyHandler{d: d, ac: ac, actions: runner}
	keyboardDone := make(chan struct{})
	go func() {
		handler.Action(keys.ActionGrowPaneWidth, nil)
		close(keyboardDone)
	}()
	<-admitted
	require.False(t, sess.dispatchMu.TryLock(), "keyboard mutation must hold the session dispatch boundary")

	commandDone := make(chan struct{})
	go func() {
		_ = sess.runMutation(func() error {
			orderMu.Lock()
			order = append(order, "command")
			orderMu.Unlock()
			return nil
		})
		close(commandDone)
	}()
	close(release)
	<-keyboardDone
	<-commandDone

	require.Equal(t, []string{"keyboard", "command"}, order)
}

func TestConfiguredConsumeOrExpelActionsRouteThroughDaemonInput(t *testing.T) {
	tests := []struct {
		name       string
		binding    string
		key        byte
		focus      layout.PaneID
		wantTarget layout.PaneID
	}{
		{name: "left", binding: "consume-or-expel-pane-left", key: 'H', focus: "pane-3", wantTarget: "pane-3"},
		{name: "right", binding: "consume-or-expel-pane-right", key: 'L', focus: "pane-1", wantTarget: "pane-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newPaneRearrangeHarness(t, domain.Size{Cols: 80, Rows: 22}, threeColumnTree())
			h.tab.mu.Lock()
			h.tab.tree.Focus = tt.focus
			beforeGeneration := h.tab.layoutGeneration
			h.tab.mu.Unlock()
			bindings, warnings := keys.BuildBindings(map[string]string{tt.binding: "alt+" + string(tt.key)})
			require.Empty(t, warnings)
			h.daemon.bindings.Store(bindings)
			ac := &attachedClient{}
			ac.setSession(h.session)
			ac.keys = keys.NewRouter(h.daemon.clock, daemonKeyHandler{d: h.daemon, ac: ac}, &h.daemon.bindings)
			h.session.mu.Lock()
			h.session.registerAttachmentLocked(ac)
			h.session.mu.Unlock()
			invalidations := make(chan renderInvalidation, 1)
			rc := newRenderCoordinator(renderCoordinatorOptions{onInvalidate: func(inv renderInvalidation) { invalidations <- inv }})
			rc.attach(ac)
			h.session.installRenderCoordinator(rc)

			h.daemon.handleInput(h.session, ac, []byte{keys.ESC, tt.key})

			h.tab.mu.Lock()
			require.Equal(t, tt.wantTarget, h.tab.tree.Focus)
			require.Equal(t, beforeGeneration+1, h.tab.layoutGeneration)
			require.Len(t, h.tab.tree.Root.Children, 2)
			h.tab.mu.Unlock()
			awaitInvalidation(t, invalidations)
			requireNoInvalidation(t, invalidations)
		})
	}
}

func TestConfiguredConsumeOrExpelEdgeActionIsSilent(t *testing.T) {
	h := newPaneRearrangeHarness(t, domain.Size{Cols: 80, Rows: 22}, threeColumnTree())
	bindings, warnings := keys.BuildBindings(map[string]string{"consume-or-expel-pane-left": "alt+H"})
	require.Empty(t, warnings)
	h.daemon.bindings.Store(bindings)
	ac := &attachedClient{}
	ac.setSession(h.session)
	ac.keys = keys.NewRouter(h.daemon.clock, daemonKeyHandler{d: h.daemon, ac: ac}, &h.daemon.bindings)
	h.session.mu.Lock()
	h.session.registerAttachmentLocked(ac)
	h.session.mu.Unlock()
	invalidations := make(chan renderInvalidation, 1)
	rc := newRenderCoordinator(renderCoordinatorOptions{onInvalidate: func(inv renderInvalidation) { invalidations <- inv }})
	rc.attach(ac)
	h.session.installRenderCoordinator(rc)
	before := h.snapshot()

	h.daemon.handleInput(h.session, ac, []byte{keys.ESC, 'H'})

	require.Equal(t, before, h.snapshot())
	require.Empty(t, h.daemon.notices.history())
	requireNoInvalidation(t, invalidations)
}

func TestConsumeOrExpelKeyActionPreservesRearrangementWarning(t *testing.T) {
	tests := []struct {
		name   string
		action keys.Action
	}{
		{name: "left", action: keys.ActionConsumeOrExpelPaneLeft},
		{name: "right", action: keys.ActionConsumeOrExpelPaneRight},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDaemon(t, nil, stubClock{})
			sess := addControlSession(d, "work", "t_work", "p_work")
			ac := &attachedClient{}
			ac.setSession(sess)
			runner := &actionRunnerSpy{err: domain.UserWarn(
				domain.NoticeLayoutTooSmall,
				"not enough space to rearrange pane",
				layout.ErrTooSmall,
			)}

			daemonKeyHandler{d: d, ac: ac, actions: runner}.Action(tt.action, nil)

			history := d.notices.history()
			require.Len(t, history, 1)
			require.Equal(t, domain.NoticeLayoutTooSmall, history[0].Code)
			require.Equal(t, domain.NoticeWarn, history[0].Severity)
			require.Equal(t, "not enough space to rearrange pane", history[0].Message)
		})
	}
}

// stubClock returns timers whose channel never fires, so a scheduler under it
// blocks in its debounce loop until the session context is cancelled. Used by

func TestSwitchTabFirstFrameDoesNotReuseSamePaneIDCapture(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, nil, nil)
	source := sess.tabs[0].focusedPane()
	target := sess.tabs[1].focusedPane()
	require.Equal(t, source.id, target.id, "tabs deliberately reuse pane-1")

	source.mu.Lock()
	source.screen.Write([]byte("source"))
	source.mu.Unlock()
	d.paint(sess, ac, true, nil)
	first := awaitFrame(t, sends, ports.MsgOutput)
	firstOutput, err := ports.UnmarshalOutput(first.Payload)
	require.NoError(t, err)
	terminal := vt.NewScreen(ac.size.Cols, ac.size.Rows)
	terminal.Write(firstOutput.Data)

	// A clean pane relies on the attachment capture cache for its retained
	// snapshot. Both tabs deliberately use the tab-local default pane ID.
	target.mu.Lock()
	target.screen.ClearDamage()
	target.mu.Unlock()
	// Use the real key action: it requests the mandatory complete repaint for
	// the first target-tab frame.
	daemonKeyHandler{d: d, ac: ac}.Action(keys.ActionSwitchTab2, nil)
	require.Equal(t, 1, testAttachmentTabIndex(sess))

	second := awaitFrame(t, sends, ports.MsgOutput)
	secondOutput, err := ports.UnmarshalOutput(second.Payload)
	require.NoError(t, err)
	require.Zero(t, secondOutput.Base, "tab switch must emit the complete target frame first")
	terminal.Write(secondOutput.Data)
	require.NotContains(t, strings.Join(frameRows(terminal.Frame), "\n"), "source", "the first target-tab frame must not retain source pane cells")
}

func TestClosePanePrunesAttachedCaptureFrameAndKeepsSurvivor(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, nil)
	tb := sess.tabs[0]
	survivor := tb.panes["pane-1"]
	closed := newPane("pane-2", nil, domain.Size{Cols: 40, Rows: 23})
	tb.mu.Lock()
	tb.tree = &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}, Focus: "pane-2"}
	tb.panes[closed.id] = closed
	tb.mu.Unlock()

	cachedSurvivor := capturedPaneRenderState{title: "survivor"}
	ac.captureFrames = map[*pane]capturedPaneRenderState{
		survivor: cachedSurvivor,
		closed:   {title: "closed"},
	}

	require.NoError(t, d.closePane(sess, tb, closed.id, ac, false))
	require.NotContains(t, ac.captureFrames, closed, "closed pane must not remain strongly retained by its attachment")
	require.Equal(t, cachedSurvivor, ac.captureFrames[survivor], "surviving pane keeps its incremental capture")
}

func TestCloseTabPrunesAttachedCaptureFrames(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, nil, nil)
	closedTab, survivorTab := sess.tabs[0], sess.tabs[1]
	closed := closedTab.panes["pane-1"]
	survivor := survivorTab.panes["pane-1"]
	cachedSurvivor := capturedPaneRenderState{title: "survivor"}
	ac.captureFrames = map[*pane]capturedPaneRenderState{
		closed:   {title: "closed"},
		survivor: cachedSurvivor,
	}

	require.NoError(t, d.closeTab(sess, closedTab, false))
	require.NotContains(t, ac.captureFrames, closed, "closed tab panes must not remain strongly retained by its attachment")
	require.Equal(t, cachedSurvivor, ac.captureFrames[survivor], "surviving tab pane keeps its incremental capture")
}

func TestAltCForwardsToPTY(t *testing.T) {
	writes := make(chan []byte, 2)
	p, releasePTY := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)

	d.handleInput(sess, ac, []byte("\x1bc"))

	requirePTYWrite(t, writes, []byte("\x1bc"))
	releasePTY()
}

// TestWriteToPaneFailureNotifiesDroppedInput drives forwarded keystrokes into
// a pane whose pty.Write always fails (a wedged pty, or a dead child). The
// user's typing must not silently vanish: each failed write records a
// NoticeInputDropped notice, and repeated failures on the same code coalesce
// into a single toast with a rising count rather than spamming one per key.
func TestWriteToPaneFailureNotifiesDroppedInput(t *testing.T) {
	writeErr := errors.New("write /dev/ptmx: input/output error")
	p, releasePTY := newBlockingPTYWithFailingWrite(t, writeErr)
	defer releasePTY()
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)

	for range 5 {
		d.handleInput(sess, ac, []byte("x"))
	}

	toasts := awaitToastCount(t, ac, 1)
	require.Equal(t, domain.NoticeInputDropped, toasts[0].Code)
	require.Equal(t, domain.NoticeError, toasts[0].Severity)
	require.Equal(t, "input not delivered to pane", toasts[0].Message)
	require.Equal(t, 5, toasts[0].Count, "5 failed writes must coalesce into one toast, not one per keystroke")

	hist := d.notices.history()
	require.Len(t, hist, 5, "coalescing is display-only; history keeps every occurrence")
}

func TestAltFToggleRetainedFloatingPaneRepaintsImmediately(t *testing.T) {
	normal, releaseNormal := newBlockingPTY(t)
	floatingPTY, releaseFloating := newBlockingPTY(t)
	defer releaseNormal()
	defer releaseFloating()
	d, sess, ac, sends := newManualSessionWithPTYs(t, normal)
	client := vt.NewScreen(80, 25)
	floatingCtx, cancelFloating := context.WithCancel(sess.ctx)
	defer cancelFloating()
	floating := newPane("floating", floatingPTY, domain.Size{Cols: 20, Rows: 5})
	floating.ctx = floatingCtx
	floating.screen.Write([]byte("popup-content"))
	installTestFloating(testAttachmentTab(sess), floating, false)

	// Establish the client shadow while the retained popup is hidden.
	d.paint(sess, ac, true, nil)
	mustApplyOutput(t, client, awaitFrame(t, sends, ports.MsgOutput))

	// Route the real Alt+F binding. Showing a retained pane must paint once.
	d.handleInput(sess, ac, []byte("\x1bf"))
	mustApplyOutput(t, client, awaitFrame(t, sends, ports.MsgOutput))
	require.Contains(t, strings.Join(frameRows(client.Frame), "\n"), "popup-content")
	require.Contains(t, strings.Join(frameRows(client.Frame), "\n"), "┌")
	select {
	case extra := <-sends:
		t.Fatalf("show emitted duplicate output: %#v", extra)
	default:
	}

	// The second real key must repaint immediately, rather than waiting for
	// unrelated output, while retaining the existing PTY and context.
	d.handleInput(sess, ac, []byte("\x1bf"))
	mustApplyOutput(t, client, awaitFrame(t, sends, ports.MsgOutput))
	require.NotContains(t, strings.Join(frameRows(client.Frame), "\n"), "popup-content")
	tb := testAttachmentTab(sess)
	tb.mu.Lock()
	require.Equal(t, floatingHidden, tb.floating.state)
	require.Same(t, floating, tb.floating.pane)
	generation := tb.floating.generation
	tb.mu.Unlock()
	require.Same(t, floatingPTY, floating.pty)
	select {
	case <-floating.ctx.Done():
		t.Fatal("hiding retained popup cancelled its context")
	default:
	}

	// A third show uses the installed pane; it must not launch or emit a second
	// resize/start frame.
	d.handleInput(sess, ac, []byte("\x1bf"))
	mustApplyOutput(t, client, awaitFrame(t, sends, ports.MsgOutput))
	require.Contains(t, strings.Join(frameRows(client.Frame), "\n"), "popup-content")
	tb.mu.Lock()
	require.Equal(t, floatingVisible, tb.floating.state)
	require.Same(t, floating, tb.floating.pane)
	require.Equal(t, generation, tb.floating.generation)
	tb.mu.Unlock()
	select {
	case extra := <-sends:
		t.Fatalf("retained show emitted duplicate output: %#v", extra)
	default:
	}
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
			installTestFloating(testAttachmentTab(sess), newPane("floating", floating, domain.Size{Cols: 20, Rows: 5}), tc.visible)

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
	installTestFloating(testAttachmentTab(sess), newPane("floating", floating, domain.Size{Cols: 20, Rows: 5}), true)

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
	tb := testAttachmentTab(sess)
	second := newPane("pane-2", nil, domain.Size{Cols: 40, Rows: 5})
	tb.mu.Lock()
	tb.size = domain.Size{Cols: 81, Rows: 5}
	tb.tree.Root = &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}
	tb.panes["pane-2"] = second
	tb.mu.Unlock()
	installTestFloating(tb, newPane("floating", floating, domain.Size{Cols: 20, Rows: 5}), true)

	daemonKeyHandler{d: d, ac: ac}.Action(keys.ActionFocusPaneRight, nil)
	require.Equal(t, layout.PaneID("pane-2"), tb.tree.Focus)
	daemonKeyHandler{d: d, ac: ac}.Forward([]byte("x"))
	requirePTYWrite(t, floatingWrites, []byte("x"))
}

// TestActionFocusPaneAtEdgeStaysSilent drives a directional focus move with
// no pane on that side (the session has a single, unsplit pane). This is
// routine navigation, not a failure, and must never produce a notice.
func TestActionFocusPaneAtEdgeStaysSilent(t *testing.T) {
	d, _, ac, _ := newManualSessionWithPTYs(t, nil)

	daemonKeyHandler{d: d, ac: ac}.Action(keys.ActionFocusPaneLeft, nil)

	require.Empty(t, d.notices.history(), "no-neighbor focus move must stay silent")
}

// TestActionFocusPaneGenuineErrorReportsNoticeInternal drives a directional
// focus move against a tab with no layout tree at all — a genuine structural
// error distinct from "no pane in that direction" — and asserts it reaches
// the user as a notice instead of being silently discarded.
func TestActionFocusPaneGenuineErrorReportsNoticeInternal(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, nil)
	tb := testAttachmentTab(sess)
	tb.mu.Lock()
	tb.tree = nil
	tb.mu.Unlock()

	daemonKeyHandler{d: d, ac: ac}.Action(keys.ActionFocusPaneLeft, nil)

	history := d.notices.history()
	require.Len(t, history, 2, "the layout error and its failed direct display update must both surface")
	require.Equal(t, domain.NoticeInternal, history[0].Code)
	require.Equal(t, "display update failed", history[0].Message)
	require.Equal(t, domain.NoticeInternal, history[1].Code)
	require.Equal(t, "internal error", history[1].Message)
}

func TestFloatingVisibilityRemainsIndependentAcrossTabSwitches(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, nil)
	first := testAttachmentTab(sess)
	second := newTab(nil, first.size)
	installTestFloating(first, newPane("floating", nil, domain.Size{Cols: 20, Rows: 5}), true)
	installTestFloating(second, newPane("floating", nil, domain.Size{Cols: 20, Rows: 5}), false)
	sess.mu.Lock()
	sess.tabs = append(sess.tabs, second)
	sess.mu.Unlock()

	daemonKeyHandler{d: d, ac: ac}.Action(keys.ActionSwitchTab2, nil)
	second.mu.Lock()
	require.Equal(t, floatingHidden, second.floating.state)
	second.mu.Unlock()

	daemonKeyHandler{d: d, ac: ac}.Action(keys.ActionSwitchTab1, nil)
	first.mu.Lock()
	require.Equal(t, floatingVisible, first.floating.state)
	first.mu.Unlock()
}

func TestFloatingMouseTranslatesSGRToInnerCoordinates(t *testing.T) {
	normal := portsmocks.NewMockPTY(t)
	floatingPTY := portsmocks.NewMockPTY(t)
	floatingPTY.EXPECT().Write([]byte("\x1b[<0;1;1M")).Return(len("\x1b[<0;1;1M"), nil).Once()
	d, sess, ac, _ := newManualSessionWithPTYs(t, normal)
	tb := testAttachmentTab(sess)
	tb.mu.Lock()
	tb.size = domain.Size{Cols: 100, Rows: 20}
	tb.mu.Unlock()
	floating := newPane("floating", floatingPTY, domain.Size{Cols: 78, Rows: 14})
	floating.screen.Write([]byte("\x1b[?1000h\x1b[?1006h"))
	installTestFloating(tb, floating, true)
	geometry := calculateContentFloatingGeometry(domain.Size{Cols: 100, Rows: 20}, d.currentFloatingConfig())
	raw := fmt.Appendf(nil, "\x1b[<0;%d;%dM", geometry.Inner.X+1, geometry.Inner.Y+clientTopBarRows+1)

	d.handleInput(sess, ac, raw)
}

func TestFloatingMouseIgnoresBorderAndOutside(t *testing.T) {
	normal := portsmocks.NewMockPTY(t)
	floatingPTY := portsmocks.NewMockPTY(t)
	d, sess, ac, _ := newManualSessionWithPTYs(t, normal)
	tb := testAttachmentTab(sess)
	tb.mu.Lock()
	tb.size = domain.Size{Cols: 100, Rows: 20}
	tb.mu.Unlock()
	floating := newPane("floating", floatingPTY, domain.Size{Cols: 78, Rows: 14})
	floating.screen.Write([]byte("\x1b[?1000h\x1b[?1006h"))
	installTestFloating(tb, floating, true)
	geometry := calculateContentFloatingGeometry(domain.Size{Cols: 100, Rows: 20}, d.currentFloatingConfig())

	border := fmt.Appendf(nil, "\x1b[<0;%d;%dM", geometry.Bounds.X+1, geometry.Bounds.Y+clientTopBarRows+1)
	outside := []byte("\x1b[<0;1;2M")
	d.handleInput(sess, ac, border)
	d.handleInput(sess, ac, outside)
}

func TestResponsiveDrawerFloatingMouseRoutesOnlyInnerClientCells(t *testing.T) {
	normal := portsmocks.NewMockPTY(t)
	floatingPTY := portsmocks.NewMockPTY(t)
	floatingPTY.EXPECT().Write([]byte("\x1b[<0;1;1M")).Return(len("\x1b[<0;1;1M"), nil).Once()
	d, sess, ac, _ := newManualSessionWithPTYs(t, normal)
	tb := testAttachmentTab(sess)
	complete := domain.Size{Cols: 79, Rows: 25}
	content := tabSize(complete)
	geometry := calculateContentFloatingGeometry(content, d.currentFloatingConfig())
	require.Equal(t, ui.PresentationDrawer, geometry.Mode)
	tb.mu.Lock()
	tb.size = content
	tb.mu.Unlock()
	floating := newPane("floating", floatingPTY, rectSize(geometry.Inner))
	floating.popupGeometry = geometry
	floating.screen.Write([]byte("\x1b[?1000h\x1b[?1006h"))
	installTestFloating(tb, floating, true)

	firstInner := fmt.Appendf(nil, "\x1b[<0;%d;%dM", geometry.Inner.X+1, geometry.Inner.Y+clientTopBarRows+1)
	d.handleInput(sess, ac, firstInner)
	for _, frameRow := range []int{0, 1, 2, geometry.Bounds.Y + clientTopBarRows, complete.Rows - 1} {
		d.handleInput(sess, ac, fmt.Appendf(nil, "\x1b[<0;1;%dM", frameRow+1))
	}
}

func TestResponsiveDrawerCopyMouseMapsInnerAndPinsDragAwayFromChrome(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	tb := testAttachmentTab(sess)
	complete := domain.Size{Cols: 79, Rows: 25}
	content := tabSize(complete)
	geometry := calculateContentFloatingGeometry(content, d.currentFloatingConfig())
	tb.mu.Lock()
	tb.size = content
	tb.mu.Unlock()
	floating := newPane("floating", nil, rectSize(geometry.Inner))
	floating.popupGeometry = geometry
	floating.screen.Write([]byte("drawer row zero\r\nsecond row"))
	installTestFloating(tb, floating, true)

	for _, frameRow := range []int{0, 1, 2, geometry.Bounds.Y + clientTopBarRows, complete.Rows - 1} {
		d.handleInput(sess, ac, fmt.Appendf(nil, "\x1b[<0;1;%dM", frameRow+1))
		ac.overlays.copyMu.Lock()
		require.False(t, ac.overlays.copyPointer.valid, "frame row %d is drawer chrome, not copy content", frameRow)
		ac.overlays.copyMu.Unlock()
	}

	firstInner := fmt.Appendf(nil, "\x1b[<0;%d;%dM", geometry.Inner.X+1, geometry.Inner.Y+clientTopBarRows+1)
	d.handleInput(sess, ac, firstInner)
	ac.overlays.copyMu.Lock()
	pointer := ac.overlays.copyPointer
	ac.overlays.copyMu.Unlock()
	require.True(t, pointer.valid)
	require.Same(t, floating, pointer.pane)
	require.Equal(t, scopy.Pos{Row: 0, Col: 0}, pointer.press)
	require.Equal(t, copyContentToClientRect(geometry.Inner), pointer.geometry.content)

	// Motion onto the protected top row clamps against the press-owned drawer
	// geometry instead of retargeting the underlying tiled terminal.
	d.handleInput(sess, ac, []byte("\x1b[<32;79;1M"))
	ac.overlays.copyMu.Lock()
	require.NotNil(t, ac.overlays.copyMode)
	require.Same(t, floating, ac.overlays.copyPane)
	require.Same(t, floating, ac.overlays.copyPointer.pane)
	require.Equal(t, pointer.geometry, ac.overlays.copyPointer.geometry)
	selection := ac.overlays.copyMode.Selection()
	ac.overlays.copyMu.Unlock()
	require.Equal(t, scopy.Pos{Row: 0, Col: 0}, selection.Anchor)
	require.Equal(t, 0, selection.Active.Row, "drag above the drawer must stay pinned to its first document row")
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

	requirePTYWrite(t, writes, paste)
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
				requirePTYWrite(t, writes, []byte("Z"))
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
			selectTestAttachmentTab(sess, tc.start)

			d.handleInput(sess, ac, []byte("\x1b "))
			d.handleInput(sess, ac, tc.query)

			require.Equal(t, tc.wantIndex, testAttachmentTabIndex(sess))
		})
	}
}

func TestPaletteBackSessionSendsClientPreviousRouteAction(t *testing.T) {
	d, current, ac, sends, releases := newRecentNavigationTestSessions(t)
	defer releaseAll(releases)
	transport := ac.transport()
	rc := d.attachCoordinator(current, nil, ac, true)
	token := current.attachmentToken(ac, transport)
	token.lease = rc.attachmentLease(ac)
	ac.publishAttachmentCapability(token)
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	defer effect.End()
	token.effect = effect
	ac.setRouteSnapshot(ports.RecentRouteSnapshot{
		Generation: 3,
		Active:     ports.RouteRef{Key: 3, Generation: 3},
		Previous:   ports.RouteRef{Key: 2, Generation: 2},
		Entries:    []ports.RecentRouteEntry{{Key: 2, Generation: 2, Name: "recent", Kind: ports.RouteKindLocal}},
	})

	require.NoError(t, d.backSessionForAttachment(token))
	frame := awaitFrame(t, sends, ports.MsgNavigateRecentRoute)
	action, err := ports.UnmarshalRouteNavigationAction(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.RouteNavigationAction{SnapshotGeneration: 3, Key: 2, Generation: 2}, action)
}

func TestPaletteBackSessionWithoutClientHistoryIsNoop(t *testing.T) {
	d, current, ac, _, releases := newRecentNavigationTestSessions(t)
	defer releaseAll(releases)

	d.backSession(current, ac)

	require.Same(t, current, ac.currentSession())
}

func TestBackSessionWithoutSnapshotDoesNotReportDaemonHandoffFailure(t *testing.T) {
	d, current, ac, _, releases := newRecentNavigationTestSessions(t)
	defer releaseAll(releases)

	d.backSession(current, ac)

	require.Same(t, current, ac.currentSession())
	require.Empty(t, d.notices.history())
}

func TestBackSessionWithoutSnapshotDoesNotPublishFrames(t *testing.T) {
	d, current, ac, sends, releases := newRecentNavigationTestSessions(t)
	defer releaseAll(releases)

	d.backSession(current, ac)

	require.Same(t, current, ac.currentSession())
	requireNoOutputFrame(t, sends)
}

func TestSwitchSourceDoesNotOwnPreviousRoute(t *testing.T) {
	d, current, ac, _, releases := newRecentNavigationTestSessions(t)
	defer releaseAll(releases)
	recent := mustLocalSession(t, d.sessions[domain.SessionID("recent")])

	require.NoError(t, d.switchToTarget(current, ac, picker.Target{Session: recent.id, TabIndex: -1}))
	require.Same(t, recent, ac.currentSession())
	require.Equal(t, uint64(0), ac.routeSnapshotCopy().Generation, "route ownership starts in the client")
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
	recent := &session{sessionCore: sessionCore{id: "recent", name: "recent"}, ctx: current.ctx, cancel: func() {}, tabs: []*tab{newTab(p2, domain.Size{Cols: 80, Rows: 23})}}
	older := &session{sessionCore: sessionCore{id: "older", name: "older"}, ctx: current.ctx, cancel: func() {}, tabs: []*tab{newTab(p3, domain.Size{Cols: 80, Rows: 23})}}
	d.sessions[recent.id] = recent
	d.sessions[older.id] = older
	current.mruAt.Store(30)
	recent.mruAt.Store(20)
	older.mruAt.Store(10)
	return d, current, ac, sends, []func(){release1, release2, release3}
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
	selectTestAttachmentTab(sess, 1)

	d.handleInput(sess, ac, []byte("\x1b "))
	d.handleInput(sess, ac, []byte("CLT\r"))

	require.Equal(t, 1, sessionCount(d))
	require.Len(t, sess.tabs, 2)
	require.Equal(t, 1, testAttachmentTabIndex(sess), "closing middle tab selects the next remaining tab")
	d.handleInput(sess, ac, []byte("Z"))
	require.Eventually(t, func() bool { return len(writes) == 1 }, 2*time.Second, 5*time.Millisecond)
	requirePTYWrite(t, writes, []byte("Z"))

	releasePTY1()
	releasePTY2()
	releasePTY3()
}

func TestAltDDetachesCurrentClient(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 1)
	defer releases[0]()

	d.handleInput(sess, ac, []byte("\x1b "))
	d.handleInput(sess, ac, []byte("DET\r"))

	require.Empty(t, sess.snapshotAttachments())
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
	selectTestAttachmentTab(sess, 1)

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
	require.Equal(t, 19, ac.overlays.copyMode.Cursor().Row)
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

	requirePTYWrite(t, writes, []byte("\x1b[A\x1b[A\x1b[A"))
	requirePTYWrite(t, writes, []byte("\x1b[B\x1b[B\x1b[B"))
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
	requirePTYWrite(t, writes, []byte("\x1b[<0;2;2M"))

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
	d.handleInput(sess, ac, []byte("\x1b[<0;1;2M"))
	require.Nil(t, ac.overlays.copyMode, "press alone must still not enter visual mode")
	d.handleInput(sess, ac, []byte("\x1b[<32;1;4M"))
	mustOutputData(t, sends)
	require.NotNil(t, ac.overlays.copyMode)
	selection := ac.overlays.copyMode.Selection()
	require.True(t, selection.Enabled)
	lo, hi := selection.Anchor.Row, selection.Active.Row
	if lo > hi {
		lo, hi = hi, lo
	}
	require.Equal(t, 0, lo)
	require.Equal(t, 2, hi)
}

func TestCopyModeMousePressReleaseWithoutMotionEmitsNoOSC52(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	copy(sess.tabs[0].focusedPane().screen.Frame.Row(0), testRow("alpha"))
	d.enterCopyMode(sess, ac)
	mustOutputData(t, sends)

	d.handleInput(sess, ac, []byte("\x1b[<0;1;2M\x1b[<0;1;2m"))
	d.handleInput(sess, ac, []byte("y"))

	for range 2 { // press repaint, then copy-mode exit repaint
		require.NotContains(t, string(mustOutputData(t, sends)), "\x1b]52;c;", "a click without motion has no selection to copy")
	}
	require.False(t, ac.overlays.copyActive())
}

func TestCopyModeMouseHorizontalReverseDragUsesExactOSC52(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	copy(sess.tabs[0].focusedPane().screen.Frame.Row(0), testRow("alpha"))
	d.enterCopyMode(sess, ac)
	mustOutputData(t, sends)

	// SGR is one based: Cy2 is the first body row. Dragging from column 3
	// back to column 1 is the same stream as the forward endpoints 1..3.
	d.handleInput(sess, ac, []byte("\x1b[<0;4;2M\x1b[<32;2;2M"))
	mustOutputData(t, sends)
	d.handleInput(sess, ac, []byte("\x1b[<0;2;2m"))
	d.handleInput(sess, ac, []byte("y"))
	var msg ports.Output
	require.Eventually(t, func() bool {
		frame := awaitFrame(t, sends, ports.MsgOutput)
		var err error
		msg, err = ports.UnmarshalOutput(frame.Payload)
		return err == nil && string(msg.Data) == string(scopy.OSC52("lph")[0])
	}, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, scopy.OSC52("lph")[0], msg.Data)
	require.Nil(t, ac.overlays.copyPointer.pane)
}

func TestCopyModeMouseDragYanksOSC52AndExits(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	copy(sess.tabs[0].focusedPane().screen.Frame.Row(0), testRow("alpha"))
	copy(sess.tabs[0].focusedPane().screen.Frame.Row(1), testRow("bravo"))
	copy(sess.tabs[0].focusedPane().screen.Frame.Row(2), testRow("charlie"))

	d.enterCopyMode(sess, ac)
	mustOutputData(t, sends)
	d.handleInput(sess, ac, []byte("\x1b[<0;1;2M\x1b[<32;1;3M"))
	mustOutputData(t, sends)
	require.NotNil(t, ac.overlays.copyMode)
	selection := ac.overlays.copyMode.Selection()
	require.True(t, selection.Enabled)
	lo, hi := selection.Anchor.Row, selection.Active.Row
	if lo > hi {
		lo, hi = hi, lo
	}
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
	require.Equal(t, "alpha\nb", string(decoded))
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
	d.handleInput(sess, ac, []byte("\x1b[<0;1;2M"))
	require.Nil(t, ac.overlays.copyMode)

	// Release lands on the status row: must still clear the press state,
	// regardless of the row it landed on.
	d.handleInput(sess, ac, []byte("\x1b[<0;1;25m"))

	// A brand new button hold starts with a Press on the status row: must
	// clear (not silently preserve) any prior press state.
	d.handleInput(sess, ac, []byte("\x1b[<0;1;25M"))

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
func TestCopyModeRejectedReleaseClearsPointerAndClickBeforeMotion(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	d.enterCopyMode(sess, ac)
	mustOutputData(t, sends)

	d.handleInput(sess, ac, []byte("\x1b[<0;1;2M"))
	ac.overlays.copyMu.Lock()
	require.True(t, ac.overlays.copyPointer.valid)
	ac.overlays.copyMu.Unlock()

	// Wire row 25 is below the focused pane content, so the release is not a
	// valid endpoint and must not leave a drag that later motion can revive.
	d.handleInput(sess, ac, []byte("\x1b[<0;1;25m\x1b[<32;1;3M"))
	ac.overlays.copyMu.Lock()
	require.False(t, ac.overlays.copyPointer.valid)
	selection := ac.overlays.copyMode.Selection()
	require.False(t, selection.Enabled)
	ac.overlays.copyMu.Unlock()
}

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
	d.handleInput(sess, ac, []byte("\x1b[<0;1;2M\x1b[<32;1;3M"))
	mustOutputData(t, sends)
	require.NotNil(t, ac.overlays.copyMode)
	selection := ac.overlays.copyMode.Selection()
	require.True(t, selection.Enabled)
	lo, hi := selection.Anchor.Row, selection.Active.Row
	if lo > hi {
		lo, hi = hi, lo
	}
	require.Equal(t, 0, lo)
	require.Equal(t, 1, hi)

	// Release, then a new button hold starting with Press on the status row.
	d.handleInput(sess, ac, []byte("\x1b[<0;1;3m"))
	d.handleInput(sess, ac, []byte("\x1b[<0;1;25M"))

	// Motion onto a content row must be a no-op: the status-row press must
	// have cleared copyPressRowValid/copyDragging.
	d.handleInput(sess, ac, []byte("\x1b[<32;1;3M"))

	require.NotNil(t, ac.overlays.copyMode)
	selection = ac.overlays.copyMode.Selection()
	require.True(t, selection.Enabled)
	lo, hi = selection.Anchor.Row, selection.Active.Row
	if lo > hi {
		lo, hi = hi, lo
	}
	require.Equal(t, 0, lo, "anchor must not have moved")
	require.Equal(t, 1, hi, "status-row press must invalidate the drag so the motion is a no-op")
}

// TestMouseNormalScreenDragUsesPressOwnedDocumentAfterOutputEviction verifies
// that output eviction after Press does not change endpoint mapping: the
// immutable press-owned Document keeps pointer rows 0→1 mapped to rows 0→1.
func TestMouseNormalScreenDragUsesPressOwnedDocumentAfterOutputEviction(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	installTestHistory(sess.tabs[0].focusedPane(), vt.HistoryConfig{MaxRows: 50})
	require.Equal(t, 23, sess.tabs[0].focusedPane().screen.Frame.Height, "fixture assumption: status row is wire row 24")

	// Press anchors document row 0 before output eviction.
	d.handleInput(sess, ac, []byte("\x1b[<0;1;2M"))

	// Output evicts five lines after Press; the Document remains immutable.
	for range 5 {
		appendHistoryRow(t, sess.tabs[0].focusedPane().history, testRow("evicted"))
	}

	// Motion maps the next endpoint through the press-owned Document.
	d.handleInput(sess, ac, []byte("\x1b[<32;1;3M"))

	require.NotNil(t, ac.overlays.copyMode)
	selection := ac.overlays.copyMode.Selection()
	require.True(t, selection.Enabled)
	lo, hi := selection.Anchor.Row, selection.Active.Row
	if lo > hi {
		lo, hi = hi, lo
	}
	require.Equal(t, 0, lo, "anchor stays content-stable at the row under the pointer at press time")
	require.Equal(t, 1, hi, "the press-owned immutable document keeps the mapped endpoint stable")
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
	tb := testAttachmentTab(sess)
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

func TestRejectedLeftReleaseInvalidatesFreshPointerBeforeMotion(t *testing.T) {
	for _, tc := range []struct {
		name   string
		setup  func(t *testing.T, d *Daemon, sess *session, ac *attachedClient, press *[]byte) []byte
		reject []byte
	}{
		{
			name: "split divider",
			setup: func(t *testing.T, d *Daemon, sess *session, _ *attachedClient, _ *[]byte) []byte {
				tb := testAttachmentTab(sess)
				p2 := newPane("pane-2", nil, domain.Size{Cols: 20, Rows: 10})
				tb.mu.Lock()
				tb.size = domain.Size{Cols: 41, Rows: 10}
				tb.tree.Root = &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}
				tb.panes["pane-2"] = p2
				tb.mu.Unlock()
				return []byte("\x1b[<0;21;2m")
			},
		},
		{
			name: "split title bar",
			setup: func(t *testing.T, d *Daemon, sess *session, _ *attachedClient, _ *[]byte) []byte {
				tb := testAttachmentTab(sess)
				p2 := newPane("pane-2", nil, domain.Size{Cols: 20, Rows: 10})
				tb.mu.Lock()
				tb.size = domain.Size{Cols: 41, Rows: 10}
				tb.tree.Root = &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}
				tb.panes["pane-2"] = p2
				tb.mu.Unlock()
				return []byte("\x1b[<0;1;1m")
			},
		},
		{
			name: "floating exterior",
			setup: func(t *testing.T, d *Daemon, sess *session, _ *attachedClient, press *[]byte) []byte {
				tb := testAttachmentTab(sess)
				floating := newPane("floating", nil, domain.Size{Cols: 20, Rows: 5})
				installTestFloating(tb, floating, true)
				tb.mu.Lock()
				_, geometry, visible := tb.visibleFloatingSnapshotLocked(d.currentFloatingConfig())
				tb.mu.Unlock()
				require.True(t, visible)
				*press = fmt.Appendf(nil, "\x1b[<0;%d;%dM", geometry.Inner.X+1, geometry.Inner.Y+2)
				return []byte("\x1b[<0;1;1m")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, release := newBlockingPTY(t)
			defer release()
			d, sess, ac, _ := newManualSessionWithPTYs(t, p)
			press := []byte("\x1b[<0;1;2M")
			reject := tc.setup(t, d, sess, ac, &press)

			// A content press is fresh state. The rejected release must clear it
			// before a following motion has an opportunity to publish copy mode.
			d.handleInput(sess, ac, press)
			ac.overlays.copyMu.Lock()
			require.True(t, ac.overlays.copyPointer.valid)
			ac.overlays.copyMu.Unlock()

			d.handleInput(sess, ac, reject)
			d.handleInput(sess, ac, []byte("\x1b[<32;1;3M"))

			ac.overlays.copyMu.Lock()
			require.False(t, ac.overlays.copyPointer.valid)
			require.Nil(t, ac.overlays.copyMode, "rejected release followed by motion must not publish")
			ac.overlays.copyMu.Unlock()
		})
	}
}

func TestRejectedLeftPressAndInactiveReleaseInvalidateStalePointer(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)

	d.handleInput(sess, ac, []byte("\x1b[<0;1;2M"))
	d.handleInput(sess, ac, []byte("\x1b[<0;1;1M")) // a new rejected title-bar press
	ac.overlays.copyMu.Lock()
	require.False(t, ac.overlays.copyPointer.valid)
	ac.overlays.copyMu.Unlock()

	d.handleInput(sess, ac, []byte("\x1b[<0;1;2M"))
	ac.setSession(nil)
	d.handleMouse(ac, mouse.Event{Button: mouse.Left, Type: mouse.Release})
	ac.overlays.copyMu.Lock()
	require.False(t, ac.overlays.copyPointer.valid)
	ac.overlays.copyMu.Unlock()
}

func TestPressAtFormerStackTitleRowIsTreatedAsExpandedContent(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	tb := testAttachmentTab(sess)
	p2 := newPane("pane-2", nil, domain.Size{Cols: 20, Rows: 9})
	p3 := newPane("pane-3", nil, domain.Size{Cols: 20, Rows: 1})
	tb.mu.Lock()
	tb.size = domain.Size{Cols: 41, Rows: 10}
	tb.tree.Root = &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{
		layout.NewLeaf("pane-1"),
		{Kind: layout.Stack, Children: []*layout.Node{layout.NewLeaf("pane-2"), layout.NewLeaf("pane-3")}, Expanded: "pane-2"},
	}}
	tb.tree.Focus = "pane-2"
	tb.panes["pane-2"] = p2
	tb.panes["pane-3"] = p3
	tb.mu.Unlock()

	// pane-2 is the stack's expanded member, so it draws no title bar and its
	// content now starts at the row that used to hold that title bar (row 0 of
	// the stack, screen row 1, i.e. cy=2 in 1-based SGR coordinates). A content
	// press gives pane-2 a fresh candidate.
	d.handleInput(sess, ac, []byte("\x1b[<0;22;3M"))
	ac.overlays.copyMu.Lock()
	require.True(t, ac.overlays.copyPointer.valid)
	epoch := ac.overlays.copyPointerEpoch
	ac.overlays.copyMu.Unlock()

	// Pressing the row that used to be the title bar must be routed as an
	// ordinary content press on the already-focused pane-2, not a title-bar
	// hit: it replaces the candidate with a fresh one rather than leaving a
	// stale/invalidated pointer, and focus is unchanged since pane-2 was
	// already focused.
	d.handleInput(sess, ac, []byte("\x1b[<0;22;2M"))
	ac.overlays.copyMu.Lock()
	require.True(t, ac.overlays.copyPointer.valid)
	require.Greater(t, ac.overlays.copyPointerEpoch, epoch)
	ac.overlays.copyMu.Unlock()
	require.Equal(t, layout.PaneID("pane-2"), tb.tree.Focus, "press on already-focused pane's content must preserve focus")

	// A drag from that press is a legitimate content selection now (there is
	// no title bar to have blocked it), so it publishes copy mode.
	d.handleInput(sess, ac, []byte("\x1b[<32;22;4M"))
	ac.overlays.copyMu.Lock()
	require.NotNil(t, ac.overlays.copyMode, "drag from a content press must publish copy mode")
	ac.overlays.copyMu.Unlock()
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
			tb := testAttachmentTab(sess)
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
	tb := testAttachmentTab(sess)
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

	d.handleInput(sess, ac, []byte("\x1b[<0;1;2M\x1b[<32;1;12M"))
	mustOutputData(t, sends)

	selection := ac.overlays.copyMode.Selection()
	require.True(t, selection.Enabled)
	lo, hi := selection.Anchor.Row, selection.Active.Row
	if lo > hi {
		lo, hi = hi, lo
	}
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
	tb := testAttachmentTab(sess)
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

// TestMouseGatedWhileNoticesOverlayActive guards against a click on the
// notifications modal falling through to pane hit-testing underneath it: the
// modal has no mouse handler of its own, so without the gate a click would
// silently retarget focus and forward SGR bytes to whichever pane sits under
// the modal's coordinates.
func TestMouseGatedWhileNoticesOverlayActive(t *testing.T) {
	p1 := portsmocks.NewMockPTY(t)
	p1.EXPECT().Resize(domain.Size{Cols: 20, Rows: 5}).Return(nil).Maybe()
	p2 := portsmocks.NewMockPTY(t)
	p2.EXPECT().Resize(domain.Size{Cols: 20, Rows: 5}).Return(nil).Maybe()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p1)
	d.procComm = nil
	tb := testAttachmentTab(sess)
	p2pane := newPane("pane-2", p2, domain.Size{Cols: 20, Rows: 5})
	p2pane.screen.Write([]byte("\x1b[?1000h\x1b[?1006h"))
	tb.mu.Lock()
	tb.size = domain.Size{Cols: 41, Rows: 5}
	tb.tree.Root = &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}
	tb.tree.Focus = "pane-1"
	tb.panes["pane-2"] = p2pane
	tb.mu.Unlock()

	d.notices.record(domain.Notification{Code: domain.NoticePaneSpawn, Message: "m", Time: time.Unix(1, 0)})
	d.enterNotices(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	require.True(t, ac.overlays.noticesActive())

	// Same coordinates that TestMouseHitTestFocusesPaneAndTranslatesSGRColumns
	// proves would otherwise focus pane-2 and forward the SGR report to it.
	d.handleInput(sess, ac, []byte("\x1b[<0;22;2M"))

	require.Equal(t, layout.PaneID("pane-1"), tb.tree.Focus, "mouse must not reach pane hit-testing while the notices overlay is open")
	require.True(t, ac.overlays.noticesActive(), "mouse click must not close the notices overlay")
}

func TestMouseCollapsedStackBarExpandsAndFocuses(t *testing.T) {
	p1 := portsmocks.NewMockPTY(t)
	p1.EXPECT().Resize(domain.Size{Cols: 20, Rows: 4}).Return(nil).Maybe()
	p2 := portsmocks.NewMockPTY(t)
	p2.EXPECT().Resize(domain.Size{Cols: 20, Rows: 4}).Return(nil).Maybe()
	d, sess, ac, _ := newManualSessionWithPTYs(t, p1)
	d.procComm = nil
	tb := testAttachmentTab(sess)
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

func TestActiveCopyPressOnOtherPaneFocusesAndExitsBeforeFutureInput(t *testing.T) {
	p1, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, _ := newManualSessionWithPTYs(t, p1)
	tb := testAttachmentTab(sess)
	p2 := newPane("pane-2", nil, domain.Size{Cols: 20, Rows: 10})
	tb.mu.Lock()
	tb.size = domain.Size{Cols: 41, Rows: 10}
	tb.tree.Root = &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}
	tb.panes["pane-2"] = p2
	tb.mu.Unlock()

	d.enterCopyMode(sess, ac)
	require.True(t, ac.overlays.copyActive())
	d.handleInput(sess, ac, []byte("\x1b[<0;1;2M"))
	ac.overlays.copyMu.Lock()
	require.True(t, ac.overlays.copyPointer.valid)
	require.True(t, ac.overlays.copyClick.valid)
	ac.overlays.copyMu.Unlock()

	// A press in the right split must focus it, then leave the old pane's copy
	// snapshot behind. The following press is therefore a fresh p2 candidate.
	d.handleInput(sess, ac, []byte("\x1b[<0;22;2M"))
	require.Equal(t, layout.PaneID("pane-2"), tb.tree.Focus)
	require.False(t, ac.overlays.copyActive())
	ac.overlays.copyMu.Lock()
	require.False(t, ac.overlays.copyPointer.valid)
	require.False(t, ac.overlays.copyClick.valid)
	ac.overlays.copyMu.Unlock()

	d.handleInput(sess, ac, []byte("\x1b[<0;22;2M"))
	ac.overlays.copyMu.Lock()
	pointer := ac.overlays.copyPointer
	ac.overlays.copyMu.Unlock()
	require.True(t, pointer.valid)
	require.Same(t, p2, pointer.pane)
}

func TestCopyModeReleaseMapsFinalEndpointAndInvalidatesPointer(t *testing.T) {
	d, sess, ac, _ := mouseCopyHarness(t, "alpha bravo")

	// Motion starts an active drag; Release then arrives farther away without
	// another Motion and must still extend its final endpoint.
	copyMouseInput(d, sess, ac, "\x1b[<0;1;2M\x1b[<32;5;2M")
	copyMouseInput(d, sess, ac, "\x1b[<0;8;2m")

	ac.overlays.copyMu.Lock()
	selection := ac.overlays.copyMode.Selection()
	pointerValid := ac.overlays.copyPointer.valid
	ac.overlays.copyMu.Unlock()
	require.True(t, selection.Enabled)
	require.Equal(t, scopy.Pos{Row: 0, Col: 0}, selection.Anchor)
	require.Equal(t, scopy.Pos{Row: 0, Col: 7}, selection.Active)
	require.False(t, pointerValid)
}

func TestCopySearchReleaseMapsFinalEndpointAndInvalidatesPointer(t *testing.T) {
	for _, tc := range []struct {
		name    string
		release string
		wantCol int
	}{
		{name: "in pane", release: "\x1b[<0;8;2m", wantCol: 7},
		{name: "outside pane clamps", release: "\x1b[<0;99;2m", wantCol: 79},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, sess, ac, _ := mouseCopyHarness(t, "alpha bravo")

			copyMouseInput(d, sess, ac, "\x1b[<0;1;2M\x1b[<32;5;2M")
			copyMouseInput(d, sess, ac, "/")
			require.True(t, ac.overlays.copySearchActive())

			// Search owns ordinary mouse input, but the release still completes
			// the active drag against its press-owned geometry.
			copyMouseInput(d, sess, ac, tc.release)
			copyMouseInput(d, sess, ac, "\x03")

			ac.overlays.copyMu.Lock()
			selection := ac.overlays.copyMode.Selection()
			pointerValid := ac.overlays.copyPointer.valid
			ac.overlays.copyMu.Unlock()
			require.True(t, selection.Enabled)
			require.Equal(t, scopy.Pos{Row: 0, Col: 0}, selection.Anchor)
			require.Equal(t, scopy.Pos{Row: 0, Col: tc.wantCol}, selection.Active)
			require.False(t, pointerValid)
			require.False(t, ac.overlays.copySearchActive())
		})
	}
}

// TestActiveCopyMouseIgnoresStalePointerResets makes the mapping/reset race
// deterministic. Each path captures a copy-input snapshot, yields while that
// snapshot is outside copyMu, then starts a new pointer. The old event must
// never invalidate the new interaction.
func TestActiveCopyMouseIgnoresStalePointerResets(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) mouse.Event
	}{
		{
			name: "press-owned release rejects mapping",
			prepare: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) mouse.Event {
				t.Helper()
				p := testAttachmentTab(sess).focusedPane()
				tb := testAttachmentTab(sess)
				tb.mu.Lock()
				geometry, ok := hitTestCopyMouseGeometryLocked(tb, d.currentFloatingConfig(), 1, 2)
				tb.mu.Unlock()
				require.True(t, ok)
				ac.overlays.copyMu.Lock()
				ac.overlays.beginCopyPointerLocked(copyPointerState{pane: p, document: ac.overlays.copyDocument, geometry: geometry})
				ac.overlays.copyMu.Unlock()
				return mouse.Event{Button: mouse.Left, Type: mouse.Release, Col: geometry.content.X - 1, Row: geometry.content.Y}
			},
		},
		{
			name: "current geometry disappears",
			prepare: func(t *testing.T, _ *Daemon, sess *session, _ *attachedClient) mouse.Event {
				t.Helper()
				tb := testAttachmentTab(sess)
				tb.mu.Lock()
				tb.tree = nil
				tb.mu.Unlock()
				return mouse.Event{Button: mouse.Left, Type: mouse.Press, Col: 1, Row: 2}
			},
		},
		{
			name: "fresh press rejects document mapping",
			prepare: func(t *testing.T, _ *Daemon, sess *session, _ *attachedClient) mouse.Event {
				t.Helper()
				tb := testAttachmentTab(sess)
				tb.mu.Lock()
				tb.size.Rows = 40 // valid layout coordinate, outside the old document.
				tb.mu.Unlock()
				return mouse.Event{Button: mouse.Left, Type: mouse.Press, Col: 1, Row: 30}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, release := newBlockingPTY(t)
			defer release()
			d, sess, ac, sends := newManualSessionWithPTYs(t, p)
			d.enterCopyMode(sess, ac)
			mustOutputData(t, sends)
			ev := tc.prepare(t, d, sess, ac)

			var want copyPointerState
			d.beforeCopyMouseMap = func() {
				ac.overlays.copyMu.Lock()
				ac.overlays.beginCopyPointerLocked(copyPointerState{
					pane:     ac.overlays.copyPane,
					document: ac.overlays.copyDocument,
					press:    scopy.Pos{Row: 7, Col: 7},
				})
				want = ac.overlays.copyPointer
				ac.overlays.copyMu.Unlock()
			}
			defer func() { d.beforeCopyMouseMap = nil }()

			d.handleActiveCopyMouse(sess, ac, testAttachmentTab(sess), ev)

			ac.overlays.copyMu.Lock()
			defer ac.overlays.copyMu.Unlock()
			require.True(t, want.valid, "test seam must start the newer pointer")
			require.True(t, ac.overlays.copyPointer.valid)
			require.Equal(t, want.epoch, ac.overlays.copyPointerEpoch)
			require.Equal(t, want.epoch, ac.overlays.copyPointer.epoch)
			require.Same(t, want.pane, ac.overlays.copyPointer.pane)
			require.Same(t, want.document, ac.overlays.copyPointer.document)
			require.Equal(t, want.press, ac.overlays.copyPointer.press)
		})
	}
}

func TestActiveCopyMouseRejectsViewportChangeAfterMappingSnapshot(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	pane := testAttachmentTab(sess).focusedPane()
	for range 8 {
		appendHistoryRow(t, pane.history, testRow("history"))
	}
	d.enterCopyMode(sess, ac)
	mustOutputData(t, sends)

	ac.overlays.copyMu.Lock()
	before := ac.overlays.copyMode.Cursor()
	ac.overlays.copyMu.Unlock()
	d.beforeCopyMouseMap = func() {
		ac.overlays.copyMu.Lock()
		ac.overlays.copyMode.ViewportTop--
		ac.overlays.copyMu.Unlock()
	}
	defer func() { d.beforeCopyMouseMap = nil }()

	// The seam changes ViewportTop after the map snapshot but before copyMouse
	// revalidates it. The stale mapped row must not move the selection cursor.
	d.handleInput(sess, ac, []byte("\x1b[<0;1;2M"))
	ac.overlays.copyMu.Lock()
	require.Equal(t, before, ac.overlays.copyMode.Cursor())
	require.False(t, ac.overlays.copyPointer.valid)
	ac.overlays.copyMu.Unlock()
}

func TestFreshCopyPressUsesFrameAbsoluteHitTestGeometry(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, d *Daemon, sess *session) (*pane, mouse.Event)
	}{
		{
			name: "mono",
			setup: func(_ *testing.T, _ *Daemon, sess *session) (*pane, mouse.Event) {
				return testAttachmentTab(sess).focusedPane(), mouse.Event{Button: mouse.Left, Type: mouse.Press, Col: 1, Row: 1}
			},
		},
		{
			name: "split",
			setup: func(_ *testing.T, _ *Daemon, sess *session) (*pane, mouse.Event) {
				tb := testAttachmentTab(sess)
				p2 := newPane("pane-2", nil, domain.Size{Cols: 20, Rows: 10})
				tb.mu.Lock()
				tb.size = domain.Size{Cols: 41, Rows: 10}
				tb.tree.Root = &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}
				tb.panes["pane-2"] = p2
				tb.mu.Unlock()
				return p2, mouse.Event{Button: mouse.Left, Type: mouse.Press, Col: 22, Row: 3}
			},
		},
		{
			name: "floating",
			setup: func(t *testing.T, d *Daemon, sess *session) (*pane, mouse.Event) {
				p := newPane("floating", nil, domain.Size{Cols: 20, Rows: 5})
				installTestFloating(testAttachmentTab(sess), p, true)
				tb := testAttachmentTab(sess)
				tb.mu.Lock()
				_, geometry, visible := tb.visibleFloatingSnapshotLocked(d.currentFloatingConfig())
				tb.mu.Unlock()
				require.True(t, visible)
				return p, mouse.Event{Button: mouse.Left, Type: mouse.Press, Col: geometry.Inner.X + 1, Row: geometry.Inner.Y + 2}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, release := newBlockingPTY(t)
			defer release()
			d, sess, ac, _ := newManualSessionWithPTYs(t, p)
			wantPane, ev := tc.setup(t, d, sess)
			tb := testAttachmentTab(sess)
			tb.mu.Lock()
			wantGeometry, ok := hitTestCopyMouseGeometryLocked(tb, d.currentFloatingConfig(), ev.Col, ev.Row)
			tb.mu.Unlock()
			require.True(t, ok)
			require.Same(t, wantPane, wantGeometry.pane)

			d.handleMouse(ac, ev)
			ac.overlays.copyMu.Lock()
			pointer := ac.overlays.copyPointer
			ac.overlays.copyMu.Unlock()
			require.True(t, pointer.valid)
			require.Same(t, wantPane, pointer.pane)
			require.Equal(t, wantGeometry, pointer.geometry)
			mapped, ok := mapCopyMouse(ev, wantGeometry, max(pointer.document.Len()-pointer.document.Height(), 0), pointer.document, false)
			require.True(t, ok)
			require.Equal(t, mapped.pos, pointer.press)
		})
	}
}

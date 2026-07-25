package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/mouse"
	"github.com/stretchr/testify/require"
)

func TestPaletteAndControlShareResizeAction(t *testing.T) {
	tests := []struct {
		name    string
		palette func(paletteExec) error
		control func(controlExec) error
		kind    daemonActionKind
		axis    layout.Axis
		delta   int
	}{
		{"grow width", func(e paletteExec) error { return e.GrowPaneWidth() }, func(e controlExec) error { return e.GrowPaneWidth() }, daemonActionResizePane, layout.Width, 2},
		{"shrink width", func(e paletteExec) error { return e.ShrinkPaneWidth() }, func(e controlExec) error { return e.ShrinkPaneWidth() }, daemonActionResizePane, layout.Width, -2},
		{"grow height", func(e paletteExec) error { return e.GrowPaneHeight() }, func(e controlExec) error { return e.GrowPaneHeight() }, daemonActionResizePane, layout.Height, 1},
		{"shrink height", func(e paletteExec) error { return e.ShrinkPaneHeight() }, func(e controlExec) error { return e.ShrinkPaneHeight() }, daemonActionResizePane, layout.Height, -1},
		{"equalize", func(e paletteExec) error { return e.EqualizePanes() }, func(e controlExec) error { return e.EqualizePanes() }, daemonActionEqualizePanes, layout.Width, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &actionRunnerSpy{}
			c := &actionRunnerSpy{}
			require.NoError(t, tt.palette(paletteExec{actions: p}))
			require.NoError(t, tt.control(controlExec{actions: c}))
			require.Len(t, p.requests, 1)
			require.Equal(t, p.requests, c.requests)
			require.Equal(t, tt.kind, p.requests[0].kind)
			require.Equal(t, tt.axis, p.requests[0].axis)
			require.Equal(t, tt.delta, p.requests[0].delta)
		})
	}
}

func TestResizeControlHeadlessAndErrors(t *testing.T) {
	factory := &controlPTYFactory{}
	d := newTestDaemon(t, factory, stubClock{})
	t.Cleanup(func() { factory.close(); d.sessWg.Wait() })
	sess := addControlSession(d, "work", "t_work", "p_work")

	tooEarly := sendCommand(t, d, ports.CommandRequest{Slug: "grow-pane-width", TargetSession: "work"})
	require.False(t, tooEarly.OK)
	require.Equal(t, ports.ErrNoSuchTarget, tooEarly.Code)
	require.Equal(t, "pane is not in a split", tooEarly.Text)

	require.True(t, sendCommand(t, d, ports.CommandRequest{Slug: "split-right", TargetSession: "work"}).OK)
	tb := sess.activeTab()
	tb.mu.Lock()
	beforeFocus := tb.tree.Focus
	beforeGeneration := tb.layoutGeneration
	var target *pane
	for id, candidate := range tb.panes {
		if id != beforeFocus {
			target = candidate
			break
		}
	}
	tb.mu.Unlock()
	require.NotNil(t, target)

	require.NoError(t, (daemonActions{d: d}).Run(daemonActionRequest{
		kind:   daemonActionResizePane,
		target: daemonActionTarget{session: sess, tab: tb, pane: target},
		axis:   layout.Width,
		delta:  resizeStepCols,
	}))
	tb.mu.Lock()
	require.Equal(t, beforeFocus, tb.tree.Focus, "exact pane targeting must not refocus the tab")
	require.Equal(t, beforeGeneration+1, tb.layoutGeneration)
	tb.mu.Unlock()

	equalized := sendCommand(t, d, ports.CommandRequest{Slug: "equalize-panes", TargetSession: "work"})
	require.True(t, equalized.OK, equalized.Text)
	require.Empty(t, equalized.Output)
}

func TestEnterResizeModeRejectsPaneOutsideSplit(t *testing.T) {
	factory := &controlPTYFactory{}
	d := newTestDaemon(t, factory, stubClock{})
	t.Cleanup(func() { factory.close(); d.sessWg.Wait() })
	sess := addControlSession(d, "work", "t_work", "p_work")
	ac := &attachedClient{output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	ac.initOverlays()
	ac.setSession(sess)

	err := d.enterResizeMode(sess, ac)
	require.ErrorIs(t, err, layout.ErrNotInSplit)
	require.False(t, ac.overlays.resizeModeActive())
}

func TestResizeModeStateRenderingAndMouseConsumption(t *testing.T) {
	factory := &controlPTYFactory{}
	d := newTestDaemon(t, factory, stubClock{})
	t.Cleanup(func() { factory.close(); d.sessWg.Wait() })
	sess := addControlSession(d, "work", "t_work", "p_work")
	require.True(t, sendCommand(t, d, ports.CommandRequest{Slug: "split-right", TargetSession: "work"}).OK)

	ac := &attachedClient{output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	ac.initOverlays()
	ac.setSession(sess)
	require.NoError(t, d.enterResizeMode(sess, ac))
	require.True(t, ac.overlays.resizeModeActive())
	d.handleResizeInput(ac, []byte("\x1b"))
	require.True(t, ac.overlays.resizeModeActive(), "a bare ESC read must wait for a possible arrow suffix")
	d.handleResizeInput(ac, []byte("[D"))
	require.True(t, ac.overlays.resizeModeActive())

	snapshot := ac.overlays.SnapshotForRender()
	require.True(t, snapshot.resizeActive)
	snapshot.Unlock()
	require.False(t, (capturedOverlayRenderState{resizeActive: true}).active(), "resize must not create a modal render layer")

	tb := sess.activeTab()
	tb.mu.Lock()
	focused := tb.focusedPane()
	tb.mu.Unlock()
	focused.mu.Lock()
	cursor := captureCursorInputsLocked(focused, domain.Rect{Width: 40, Height: 10}, capturedOverlayRenderState{resizeActive: true})
	focused.mu.Unlock()
	require.False(t, cursor.hiddenByOverlay, "resize must leave the pane cursor live")

	ac.overlays.copyMu.Lock()
	ac.overlays.copyPointer.valid = true
	ac.overlays.copyMu.Unlock()
	d.handleMouse(ac, mouse.Event{Button: mouse.Left, Type: mouse.Release})
	ac.overlays.copyMu.Lock()
	require.True(t, ac.overlays.copyPointer.valid, "resize mode must consume mouse input before copy/terminal routing")
	ac.overlays.copyMu.Unlock()

	ac.overlays.copyMu.Lock()
	ac.overlays.copyMode = &scopy.Mode{}
	ac.overlays.copyMu.Unlock()
	require.True(t, ac.overlays.HandleInput(d, []byte("q")))
	require.False(t, ac.overlays.resizeModeActive())
	require.True(t, ac.overlays.copyActive(), "resize input must take priority over copy mode")
}

func TestRouteOverlayDecodesHorizontalArrows(t *testing.T) {
	var left, right int
	pending := []byte(nil)
	events := overlayEvents{left: func() { left++ }, right: func() { right++ }}
	routeOverlayBytes([]byte("\x1b["), &pending, events)
	require.Equal(t, []byte("\x1b["), pending)
	routeOverlayBytes([]byte("D\x1bOC"), &pending, events)
	require.Equal(t, 1, left)
	require.Equal(t, 1, right)
	require.Empty(t, pending)
}

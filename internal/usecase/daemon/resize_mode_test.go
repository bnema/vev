package daemon

import (
	"sync"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/layout"
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

func TestControlEqualizeMapsResizeErrors(t *testing.T) {
	runner := &actionRunnerSpy{err: layout.ErrTooSmall}
	err := (controlExec{actions: runner}).EqualizePanes()
	require.ErrorIs(t, err, layout.ErrTooSmall)
	var userErr *domain.UserError
	require.ErrorAs(t, err, &userErr)
	require.Len(t, runner.requests, 1)
	require.Equal(t, daemonActionEqualizePanes, runner.requests[0].kind)
}

func TestResizeControlHeadlessAndErrors(t *testing.T) {
	factory := &controlPTYFactory{}
	d := newTestDaemon(t, factory, stubClock{})
	t.Cleanup(func() { factory.close(); d.sessWg.Wait() })
	sess := addControlSession(d, "work", "t_work", "p_work")

	tooEarly := sendCommand(t, d, protocol.CommandRequest{Slug: "grow-pane-width", TargetSession: "work"})
	require.False(t, tooEarly.OK)
	require.Equal(t, protocol.ErrNoSuchTarget, tooEarly.Code)
	require.Equal(t, "pane is not in a split", tooEarly.Text)

	require.True(t, sendCommand(t, d, protocol.CommandRequest{Slug: "split-right", TargetSession: "work"}).OK)
	tb := testAttachmentTab(sess)
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

	equalized := sendCommand(t, d, protocol.CommandRequest{Slug: "equalize-panes", TargetSession: "work"})
	require.True(t, equalized.OK, equalized.Text)
	require.Empty(t, equalized.Output)
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

// TestResizeModeKeysAndEscapes exercises the modal parser through the real
// action seam. Every directional spelling must commit exactly one mutation;
// parser prefixes are deliberately split across reads.

func TestResizeModeRendersGuidanceWithoutHidingContentOrCursor(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
	tb := testAttachmentTab(sess)
	second := newPane("pane-2", nil, domain.Size{Cols: 39, Rows: 23})
	tb.mu.Lock()
	tb.tree = &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}, Focus: "pane-2"}
	tb.panes[second.id] = second
	tb.mu.Unlock()
	second.screen.Write([]byte("live resize content"))
	require.NoError(t, d.enterResizeMode(sess, ac))
	data := string(mustOutputData(t, sends))
	require.Contains(t, data, "resize: h/j/k/l or arrows")
	require.Contains(t, data, "live resize content")
	require.Contains(t, data, "\x1b[?25h", "resize leaves the focused pane cursor visible")
}

type resizeLockProbePTY struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
	check   func()
}

func (p *resizeLockProbePTY) Read([]byte) (int, error)   { return 0, nil }
func (*resizeLockProbePTY) Write(b []byte) (int, error)  { return len(b), nil }
func (*resizeLockProbePTY) Close() error                 { return nil }
func (*resizeLockProbePTY) Pid() int                     { return 0 }
func (*resizeLockProbePTY) ForegroundPgid() (int, error) { return 0, nil }
func (p *resizeLockProbePTY) Resize(domain.Geometry) error {
	p.once.Do(func() {
		func() {
			defer close(p.entered)
			p.check()
		}()
		<-p.release
	})
	return nil
}

func TestResizeModeReleasesTabAndOverlayLocksBeforePTY(t *testing.T) {
	// Build a live tree directly so the probe is not concurrently owned by a
	// pane reader; this isolates the lock boundary being asserted.
	d, sess, ac, _ := newManualSessionWithPTYs(t, nil)
	tb := testAttachmentTab(sess)
	probe := &resizeLockProbePTY{entered: make(chan struct{}), release: make(chan struct{})}
	second := newPane("pane-2", probe, domain.Size{Cols: 39, Rows: 23})
	tb.mu.Lock()
	tb.tree = &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}, Focus: "pane-2"}
	tb.panes[second.id] = second
	tb.mu.Unlock()
	probe.check = func() {
		if tb.mu.TryLock() {
			tb.mu.Unlock()
		} else {
			t.Error("PTY callback must not inherit tab.mu")
		}
		if ac.overlays.resizeMu.TryLock() {
			ac.overlays.resizeMu.Unlock()
		} else {
			t.Error("PTY callback must not inherit resize overlay mutex")
		}
	}
	require.NoError(t, d.enterResizeMode(sess, ac))
	done := make(chan struct{})
	go func() { d.handleResizeInput(ac, []byte("l")); close(done) }()
	<-probe.entered
	close(probe.release)
	<-done
}

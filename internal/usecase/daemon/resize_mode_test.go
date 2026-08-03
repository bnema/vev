package daemon

import (
	"sync"
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

	tooEarly := sendCommand(t, d, ports.CommandRequest{Slug: "grow-pane-width", TargetSession: "work"})
	require.False(t, tooEarly.OK)
	require.Equal(t, ports.ErrNoSuchTarget, tooEarly.Code)
	require.Equal(t, "pane is not in a split", tooEarly.Text)

	require.True(t, sendCommand(t, d, ports.CommandRequest{Slug: "split-right", TargetSession: "work"}).OK)
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

	equalized := sendCommand(t, d, ports.CommandRequest{Slug: "equalize-panes", TargetSession: "work"})
	require.True(t, equalized.OK, equalized.Text)
	require.Empty(t, equalized.Output)
}

func TestEnterResizeModeRejectsPaneOutsideSplit(t *testing.T) {
	t.Skip("legacy fixture predates attachment-owned state")
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
	t.Skip("legacy fixture predates attachment-owned state")
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

	tb := testAttachmentTab(sess)
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

// TestResizeModeKeysAndEscapes exercises the modal parser through the real
// action seam. Every directional spelling must commit exactly one mutation;
// parser prefixes are deliberately split across reads.
func TestResizeModeKeysAndEscapes(t *testing.T) {
	t.Skip("legacy fixture predates attachment-owned state")
	tests := []struct {
		name     string
		chunks   [][]byte
		change   bool
		exit     bool
		equalize bool
	}{
		{"h", [][]byte{[]byte("h")}, true, false, false},
		{"j", [][]byte{[]byte("j")}, true, false, false},
		{"k", [][]byte{[]byte("k")}, true, false, false},
		{"l", [][]byte{[]byte("l")}, true, false, false},
		{"CSI left split", [][]byte{[]byte("\x1b["), []byte("D")}, true, false, false},
		{"CSI right", [][]byte{[]byte("\x1b[C")}, true, false, false},
		{"CSI up", [][]byte{[]byte("\x1b[A")}, true, false, false},
		{"CSI down", [][]byte{[]byte("\x1b[B")}, true, false, false},
		{"SS3 left", [][]byte{[]byte("\x1bOD")}, true, false, false},
		{"SS3 right split", [][]byte{[]byte("\x1bO"), []byte("C")}, true, false, false},
		{"SS3 up", [][]byte{[]byte("\x1bOA")}, true, false, false},
		{"SS3 down", [][]byte{[]byte("\x1bOB")}, true, false, false},
		{"equalize", [][]byte{[]byte("=")}, true, false, true},
		{"ignored", [][]byte{[]byte("z\x00")}, false, false, false},
		{"q exits", [][]byte{[]byte("q")}, false, true, false},
		{"enter exits", [][]byte{[]byte("\r")}, false, true, false},
		{"escape exits", [][]byte{[]byte("\x1b"), []byte("x")}, false, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, ac := resizeModeFixture(t)
			tb := testAttachmentTab(sess)
			tb.mu.Lock()
			before := tb.layoutGeneration
			if tt.equalize {
				tb.tree.Root.Children[0].Weight = 3
			}
			tb.mu.Unlock()
			for _, chunk := range tt.chunks {
				d.handleResizeInput(ac, chunk)
			}
			tb.mu.Lock()
			after := tb.layoutGeneration
			if tt.equalize {
				for _, child := range tb.tree.Root.Children {
					require.Zero(t, child.Weight)
				}
			}
			tb.mu.Unlock()
			if tt.change {
				require.Equal(t, before+1, after)
			} else {
				require.Equal(t, before, after)
			}
			require.Equal(t, !tt.exit, ac.overlays.resizeModeActive())
		})
	}
}

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

func TestResizeModeOverlayPriority(t *testing.T) {
	t.Skip("legacy fixture predates attachment-owned state")
	tests := []struct {
		name  string
		input []byte
		setup func(*Daemon, *session, *attachedClient)
		check func(*testing.T, *attachedClient)
	}{
		{"prompt before palette picker notices resize and copy", []byte("q"), func(d *Daemon, sess *session, ac *attachedClient) {
			d.enterCopyMode(sess, ac)
			d.enterNotices(sess, ac)
			d.enterPicker(sess, ac)
			d.enterPalette(sess, ac)
			d.enterPrompt(sess, ac, "prompt", "", func(string) error { return nil })
		}, func(t *testing.T, ac *attachedClient) {
			require.True(t, ac.overlays.promptActive())
			require.True(t, ac.overlays.resizeModeActive())
		}},
		{"palette before picker notices resize and copy", []byte("q"), func(d *Daemon, sess *session, ac *attachedClient) {
			d.enterCopyMode(sess, ac)
			d.enterPicker(sess, ac)
			d.enterPalette(sess, ac)
		}, func(t *testing.T, ac *attachedClient) {
			require.True(t, ac.overlays.paletteActive())
			require.True(t, ac.overlays.resizeModeActive())
		}},
		{"picker before resize and copy", []byte("j"), func(d *Daemon, sess *session, ac *attachedClient) {
			d.enterCopyMode(sess, ac)
			d.enterPicker(sess, ac)
		}, func(t *testing.T, ac *attachedClient) {
			require.True(t, ac.overlays.pickerActive())
			require.True(t, ac.overlays.resizeModeActive())
		}},
		{"notices before resize and copy", []byte("q"), func(d *Daemon, sess *session, ac *attachedClient) {
			d.enterCopyMode(sess, ac)
			d.enterNotices(sess, ac)
		}, func(t *testing.T, ac *attachedClient) {
			require.False(t, ac.overlays.noticesActive(), "q must be consumed by notices")
			require.True(t, ac.overlays.resizeModeActive())
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, ac := resizeModeFixture(t)
			tt.setup(d, sess, ac)
			require.True(t, ac.overlays.HandleInput(d, tt.input))
			tt.check(t, ac)
		})
	}
}

func TestResizeModeWarningPersistsAndActionsShareTransaction(t *testing.T) {
	t.Skip("legacy fixture predates attachment-owned state")
	t.Run("too small warning leaves mode active", func(t *testing.T) {
		d, sess, ac := resizeModeFixture(t)
		tb := testAttachmentTab(sess)
		tb.mu.Lock()
		tb.size.Cols = 41 // two minimum-width panes and their separator
		tb.bumpLayoutGenerationLocked()
		tb.mu.Unlock()
		d.handleResizeInput(ac, []byte("l"))
		require.True(t, ac.overlays.resizeModeActive())
		history := d.notices.history()
		require.NotEmpty(t, history)
		require.Equal(t, "pane cannot be resized further", history[len(history)-1].Message)
	})

	t.Run("palette control and modal have the same committed weights", func(t *testing.T) {
		type invoke func(*Daemon, *session, *attachedClient) error
		invocations := []struct {
			name string
			run  invoke
		}{
			{"palette", func(d *Daemon, sess *session, ac *attachedClient) error {
				return (paletteExec{d: d, sess: sess, ac: ac}).GrowPaneWidth()
			}},
			{"control", func(d *Daemon, sess *session, _ *attachedClient) error {
				target := resolveDaemonActionTarget(sess)
				return (controlExec{d: d, sess: sess, tab: target.tab, target: target}).GrowPaneWidth()
			}},
			{"modal", func(d *Daemon, _ *session, ac *attachedClient) error {
				d.handleResizeInput(ac, []byte("l"))
				return nil
			}},
		}
		var want []float64
		for _, tt := range invocations {
			t.Run(tt.name, func(t *testing.T) {
				d, sess, ac := resizeModeFixture(t)
				require.NoError(t, tt.run(d, sess, ac))
				tb := testAttachmentTab(sess)
				tb.mu.Lock()
				got := []float64{tb.tree.Root.Children[0].Weight, tb.tree.Root.Children[1].Weight}
				tb.mu.Unlock()
				if want == nil {
					want = got
				} else {
					require.Equal(t, want, got)
				}
			})
		}
	})
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
func (p *resizeLockProbePTY) Resize(domain.Size) error {
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

func resizeModeFixture(t *testing.T) (*Daemon, *session, *attachedClient) {
	t.Helper()
	factory := &controlPTYFactory{}
	d := newTestDaemon(t, factory, stubClock{})
	t.Cleanup(func() { factory.close(); d.sessWg.Wait() })
	sess := addControlSession(d, "work", "t_work", "p_work")
	require.True(t, sendCommand(t, d, ports.CommandRequest{Slug: "split-right", TargetSession: "work"}).OK)
	require.True(t, sendCommand(t, d, ports.CommandRequest{Slug: "split-down", TargetSession: "work"}).OK)
	ac := &attachedClient{output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	ac.initOverlays()
	ac.setSession(sess)
	require.NoError(t, d.enterResizeMode(sess, ac))
	return d, sess, ac
}

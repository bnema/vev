package daemon

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/picker"
	"github.com/bnema/vev/pkg/vt"
)

// --- test doubles -----------------------------------------------------------

func newTestTabWithContext(p ports.PTY, ctx context.Context, cancel context.CancelFunc) *tab {
	tb := newTab(p, domain.Size{Cols: 80, Rows: 23})
	tb.ctx, tb.cancel = ctx, cancel
	for _, pane := range tb.panes {
		pane.ctx, pane.cancel = ctx, cancel
	}
	return tb
}

// stubClock returns timers whose channel never fires, so a scheduler under it
// blocks in its debounce loop until the session context is cancelled. Used by

func TestAltTForwardsToPTY(t *testing.T) {
	writes := make(chan []byte, 2)
	p, releasePTY := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	defer releasePTY()

	d.handleInput(sess, ac, []byte("\x1bt"))

	require.False(t, ac.overlays.pickerActive())
	require.Equal(t, []byte("\x1bt"), <-writes)
}

func TestPickerViewsAddsBellSuffixForAttention(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	ctx, cancel := context.WithCancel(d.serveCtx)
	defer cancel()
	current := &session{id: "s1", name: "alpha", ctx: ctx, cancel: cancel, tabs: []*tab{{}, {}}}
	ringing := &session{id: "s2", name: "beta", ctx: ctx, cancel: cancel, tabs: []*tab{{name: "shell"}, {name: "logs"}}}
	ringing.mu.Lock()
	ringing.tabs[1].attention = true
	ringing.tabs[1].attentionAt = time.Unix(10, 0)
	ringing.mu.Unlock()
	d.sessions[current.id] = current
	d.sessions[ringing.id] = ringing

	views, curTab := d.pickerViews(current)

	require.Equal(t, 0, curTab)
	require.Len(t, views, 2)
	require.Equal(t, "alpha", views[0].Name)
	require.Equal(t, []picker.TabEntry{{Name: "1"}, {Name: "2"}}, views[0].Tabs)
	require.Equal(t, "beta ", views[1].Name)
	require.Equal(t, []picker.TabEntry{{Name: "shell"}, {Name: "logs", Attention: true}}, views[1].Tabs)
}

func TestPickerViewsComposesFocusedPaneTitleWithAttentionSuffix(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	defer releasePTY1()
	defer releasePTY2()
	d, sess, _, _ := newManualSessionWithPTYs(t, p1, p2)
	d.shell = "/bin/sh"
	sess.tabs[1].name = "logs"

	pane0 := sess.tabs[0].focusedPane()
	pane0.mu.Lock()
	pane0.title.processName = "vim"
	pane0.title.processNameValid = true
	pane0.mu.Unlock()

	sess.mu.Lock()
	sess.tabs[1].attention = true
	sess.mu.Unlock()

	views, _ := d.pickerViews(sess)

	require.Len(t, views, 1)
	require.Equal(t, []picker.TabEntry{{Name: "1", Detail: " (vim)"}, {Name: "logs", Detail: " (sh)", Attention: true}}, views[0].Tabs)
}

func TestPickerViewsOmitsTerminalTitleWhenTabsConfigDisabled(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	defer releasePTY1()
	defer releasePTY2()
	d, sess, _, _ := newManualSessionWithPTYs(t, p1, p2)
	d.shell = "/bin/sh"
	sess.tabs[1].name = "logs"

	pane0 := sess.tabs[0].focusedPane()
	pane0.mu.Lock()
	pane0.title.processName = "vim"
	pane0.title.processNameValid = true
	pane0.title.terminalTitle = "server.go — vev"
	pane0.mu.Unlock()

	d.ApplyConfig(domain.Config{Tabs: domain.TabsConfig{TerminalTitle: false}})

	views, _ := d.pickerViews(sess)

	require.Len(t, views, 1)
	require.Equal(t, []picker.TabEntry{{Name: "1", Detail: " (vim)"}, {Name: "logs", Detail: " (sh)"}}, views[0].Tabs)
}

func TestPickerResumesStoppedSessionWithPersistedTabNames(t *testing.T) {
	p1, release1 := newBlockingPTY(t)
	p2, release2 := newBlockingPTY(t)
	p3, release3 := newBlockingPTY(t)
	defer release1()
	defer release2()
	defer release3()
	d, from, ac, sends := newManualSessionWithPTYs(t, p1)
	d.ptys = newFactorySeq(t, p2, p3)
	d.stopped["work"] = stoppedSession{name: "work", cwd: "/tmp/work", createdAt: 7, tabNames: []string{"shell", "logs"}}

	d.resumeStoppedAndSwitch(from, ac, picker.Target{Name: "work", Stopped: true})
	awaitFrame(t, sends, ports.MsgOutput)

	target := ac.currentSession()
	require.NotNil(t, target)
	require.Equal(t, "work", target.name)
	require.Len(t, target.tabs, 2)
	require.Equal(t, "shell", target.tabs[0].name)
	require.Equal(t, "logs", target.tabs[1].name)
}

func TestPickerSameSessionNavigationSwitchAndEscClose(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 2)
	sess.mu.Lock()
	sess.client = ac
	sess.mu.Unlock()
	d.ptys = newBlockingOpenFactory(t, d)
	defer func() {
		for _, release := range releases {
			release()
		}
	}()

	d.enterPicker(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("j"))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("\r"))

	require.Equal(t, 1, activeTabIndex(sess))
	requireFloatingInitialized(t, sess.activeTab())
	awaitFrame(t, sends, ports.MsgOutput)
	d.enterPicker(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	clk := &signalClock{timers: make(chan *signalTimer, 1)}
	d.clock = clk
	d.handleInput(sess, ac, []byte("\x1b"))
	timer := <-clk.timers
	require.True(t, ac.overlays.pickerActive())
	timer.ch <- time.Now()
	require.Eventually(t, func() bool { return !ac.overlays.pickerActive() }, time.Second, 5*time.Millisecond)
	awaitFrame(t, sends, ports.MsgOutput)
}

func TestPickerSplitArrowNavigatesWithoutExiting(t *testing.T) {
	cases := []struct {
		name       string
		input      [][]byte
		wantActive int
	}{
		{name: "escape then down arrow", input: [][]byte{[]byte("\x1b"), []byte("[B")}, wantActive: 1},
		{name: "escape then up arrow", input: [][]byte{[]byte("j"), []byte("\x1b"), []byte("[A")}, wantActive: 0},
		{name: "split down arrow", input: [][]byte{[]byte("\x1b["), []byte("B")}, wantActive: 1},
		{name: "split SS3 down arrow", input: [][]byte{[]byte("\x1bO"), []byte("B")}, wantActive: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, sess, ac, sends, releases := newManualTabSession(t, 2)
			defer func() {
				for _, release := range releases {
					release()
				}
			}()

			d.enterPicker(sess, ac)
			awaitFrame(t, sends, ports.MsgOutput)
			for _, input := range tc.input {
				d.handleInput(sess, ac, input)
			}
			require.True(t, ac.overlays.pickerActive())
			d.handleInput(sess, ac, []byte("\r"))
			require.Equal(t, tc.wantActive, activeTabIndex(sess))
		})
	}
}

func TestPickerLoneEscapeExitsAfterDelay(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 1)
	defer releases[0]()

	d.enterPicker(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	clk := &signalClock{timers: make(chan *signalTimer, 1)}
	d.clock = clk
	d.handleInput(sess, ac, []byte("\x1b"))
	timer := <-clk.timers
	require.True(t, ac.overlays.pickerActive())
	timer.ch <- time.Now()
	require.Eventually(t, func() bool { return !ac.overlays.pickerActive() }, time.Second, 5*time.Millisecond)
}

func TestBackSessionFirstResetDoesNotReuseSamePaneIDCapture(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	defer releasePTY1()
	defer releasePTY2()
	d, source, ac, sends := newManualSessionWithPTYs(t, p1)
	source.id, source.name = "source", "source"
	delete(d.sessions, domain.SessionID("manual"))
	d.sessions[source.id] = source
	target := &session{
		id: "target", name: "target", ctx: source.ctx, cancel: func() {},
		tabs: []*tab{newTab(p2, domain.Size{Cols: 80, Rows: 22})},
	}
	d.sessions[target.id] = target

	sourcePane := source.tabs[0].focusedPane()
	targetPane := target.tabs[0].focusedPane()
	require.Equal(t, layout.PaneID("pane-1"), sourcePane.id, "source uses reusable pane-1")
	require.Equal(t, sourcePane.id, targetPane.id, "target deliberately reuses pane-1")
	sourcePane.screen.Write([]byte("SOURCE"))
	client := vt.NewScreen(80, 25)
	d.firstPaint(source, ac, ac.size)
	sourceOutput := mustApplyOutput(t, client, awaitFrame(t, sends, ports.MsgOutput))
	ac.ackOutputState(sourceOutput.NewStateNum)

	targetPane.screen.Write([]byte("TARGET"))
	targetPane.screen.ClearDamage() // TARGET is already rendered and has no pending VT damage.
	require.Empty(t, targetPane.screen.Damage(), "target deliberately has no pending VT damage")
	ac.previousSession.Set(target)

	// Exercise the user-facing previous-session route, which delegates to the
	// real switchToTarget hand-off and immediately emits its required reset.
	d.backSession(source, ac)
	firstReset := awaitFrame(t, sends, ports.MsgOutput)
	out := mustApplyOutput(t, client, firstReset)
	require.Zero(t, out.BaseStateNum, "the first target frame must be the reset, not an eventual repair")
	frame := strings.Join(frameRows(client.Frame), "\n")
	require.NotContains(t, frame, "SOURCE", "first target reset must not reuse source capture")
	require.Contains(t, frame, "TARGET", "first target reset must immediately show clean target VT state")
}

func TestPickerCrossSessionSwitchDetachesExistingClient(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	defer releasePTY1()
	defer releasePTY2()
	d := newTestDaemon(t, nil, stubClock{})
	tr1, sends1 := newCapturingTransport(t)
	tr2, sends2 := newCapturingTransport(t)
	ac1 := &attachedClient{tr: tr1, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	ac1.initOverlays()
	ac2 := &attachedClient{tr: tr2, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	ac2.initOverlays()
	sctx1, cancel1 := context.WithCancel(d.serveCtx)
	sctx2, cancel2 := context.WithCancel(d.serveCtx)
	defer cancel1()
	defer cancel2()
	sess1 := &session{id: "s1", name: "alpha", ephemeral: true, ctx: sctx1, cancel: cancel1, tabs: []*tab{newTestTabWithContext(p1, sctx1, cancel1)}, client: ac1}
	sess2 := &session{id: "s2", name: "beta", ctx: sctx2, cancel: cancel2, tabs: []*tab{newTestTabWithContext(p2, sctx2, cancel2)}, client: ac2}
	ac1.setSession(sess1)
	ac2.setSession(sess2)
	ac1.keys = keys.NewRouter(d.clock, daemonKeyHandler{d: d, ac: ac1}, nil)
	ac2.keys = keys.NewRouter(d.clock, daemonKeyHandler{d: d, ac: ac2}, nil)
	d.sessions[sess1.id] = sess1
	d.sessions[sess2.id] = sess2

	d.enterPicker(sess1, ac1)
	awaitFrame(t, sends1, ports.MsgOutput)
	d.handleInput(sess1, ac1, []byte("j"))
	awaitFrame(t, sends1, ports.MsgOutput)
	d.handleInput(sess1, ac1, []byte("\r"))

	require.Same(t, sess2, ac1.currentSession())
	require.Same(t, ac1, sess2.client)
	require.Nil(t, sess1.client)
	require.Equal(t, 2, sessionCount(d), "old ephemeral session remains alive after picker switch")
	det := awaitFrame(t, sends2, ports.MsgDetached)
	dm, err := ports.UnmarshalDetached(det.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ReasonDetach, dm.Reason)
	awaitFrame(t, sends1, ports.MsgOutput)
}

func TestPickerDisplacementCancelsSupersededResize(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	defer releasePTY1()
	defer releasePTY2()
	clock := &signalClock{timers: make(chan *signalTimer, 2)}
	d := newTestDaemon(t, nil, clock)
	tr1, _ := newCapturingTransport(t)
	tr2, _ := newCapturingTransport(t)
	ac1 := &attachedClient{tr: tr1, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	ac2 := &attachedClient{tr: tr2, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	sctx1, cancel1 := context.WithCancel(d.serveCtx)
	sctx2, cancel2 := context.WithCancel(d.serveCtx)
	defer cancel1()
	defer cancel2()
	sess1 := &session{id: "s1", name: "alpha", ctx: sctx1, cancel: cancel1, tabs: []*tab{newTestTabWithContext(p1, sctx1, cancel1)}, client: ac1}
	sess2 := &session{id: "s2", name: "beta", ctx: sctx2, cancel: cancel2, tabs: []*tab{newTestTabWithContext(p2, sctx2, cancel2)}, client: ac2}
	ac1.setSession(sess1)
	ac2.setSession(sess2)
	d.sessions[sess1.id], d.sessions[sess2.id] = sess1, sess2

	d.resize(sess2, ac2, domain.Size{Cols: 100, Rows: 24})
	timer := <-clock.timers
	before := sess2.renderCoordinator().resizeSnapshot().epoch

	require.Same(t, ac2, d.stealClientForTarget(sess1, ac1, sess2, picker.Target{Session: sess2.id}))
	// The target coordinator invalidates its scheduled epoch during handoff;
	// no attachment-local timer survives the transfer.
	require.Equal(t, before, sess2.renderCoordinator().resizeSnapshot().epoch)
	timer.ch <- time.Time{}
}

func TestPickerStalePaintAfterSessionSwitchSendsNoFrame(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	defer releasePTY1()
	defer releasePTY2()
	d := newTestDaemon(t, nil, stubClock{})
	tr1, sends1 := newCapturingTransport(t)
	tr2, _ := newCapturingTransport(t)
	ac1 := &attachedClient{tr: tr1, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	ac1.initOverlays()
	ac2 := &attachedClient{tr: tr2, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	ac2.initOverlays()
	sctx1, cancel1 := context.WithCancel(d.serveCtx)
	sctx2, cancel2 := context.WithCancel(d.serveCtx)
	defer cancel1()
	defer cancel2()
	sess1 := &session{id: "s1", name: "alpha", ephemeral: true, ctx: sctx1, cancel: cancel1, tabs: []*tab{newTestTabWithContext(p1, sctx1, cancel1)}, client: ac1}
	sess2 := &session{id: "s2", name: "beta", ctx: sctx2, cancel: cancel2, tabs: []*tab{newTestTabWithContext(p2, sctx2, cancel2)}, client: ac2}
	ac1.setSession(sess1)
	ac2.setSession(sess2)
	d.sessions[sess1.id] = sess1
	d.sessions[sess2.id] = sess2

	d.firstPaint(sess1, ac1, ac1.size)
	awaitFrame(t, sends1, ports.MsgOutput)
	d.stealClientForTarget(sess1, ac1, sess2, picker.Target{Session: sess2.id})
	d.firstPaint(sess2, ac1, ac1.size)
	awaitFrame(t, sends1, ports.MsgOutput)
	require.Same(t, sess2, ac1.currentSession())
	for len(sends1) > 0 {
		<-sends1
	}
	oldPane := sess1.tabs[0].focusedPane()
	oldPane.screen.Write([]byte("stale-damage"))
	require.NotEmpty(t, oldPane.screen.Damage())

	d.paint(sess1, ac1, false, nil)

	require.Zero(t, len(sends1), "stale paint from old session sent a frame")
	require.NotEmpty(t, oldPane.screen.Damage(), "stale paint from old session consumed damage")
}

func TestPickerSessionSwitchWaitsForInFlightPaintSend(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	defer releasePTY1()
	defer releasePTY2()
	d := newTestDaemon(t, nil, stubClock{})
	enteredSend := make(chan struct{})
	releaseSend := make(chan struct{})
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(mock.Anything).RunAndReturn(func(ports.Frame) error {
		close(enteredSend)
		<-releaseSend
		return nil
	}).Once()
	tr.EXPECT().Close().Return(nil).Maybe()
	ac := &attachedClient{tr: tr, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	ac.initOverlays()
	sctx1, cancel1 := context.WithCancel(d.serveCtx)
	sctx2, cancel2 := context.WithCancel(d.serveCtx)
	defer cancel1()
	defer cancel2()
	sess1 := &session{id: "s1", name: "alpha", ctx: sctx1, cancel: cancel1, tabs: []*tab{newTestTabWithContext(p1, sctx1, cancel1)}, client: ac}
	sess2 := &session{id: "s2", name: "beta", ctx: sctx2, cancel: cancel2, tabs: []*tab{newTestTabWithContext(p2, sctx2, cancel2)}}
	ac.setSession(sess1)

	sess1.tabs[0].focusedPane().screen.Write([]byte("paint while switching"))
	paintDone := make(chan struct{})
	go func() {
		d.paint(sess1, ac, false, nil)
		close(paintDone)
	}()
	<-enteredSend

	switchDone := make(chan struct{})
	go func() {
		ac.setSession(sess2)
		close(switchDone)
	}()
	select {
	case <-switchDone:
		t.Fatal("session switch completed while paint Send was still in flight")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseSend)
	select {
	case <-paintDone:
	case <-time.After(time.Second):
		t.Fatal("paint did not finish after Send was released")
	}
	select {
	case <-switchDone:
	case <-time.After(time.Second):
		t.Fatal("session switch did not complete after paint finished")
	}
	require.Same(t, sess2, ac.currentSession())
}

func TestPickerCrossSessionSwitchCopiesTerminalEnvForFutureTabs(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	p3, releasePTY3 := newBlockingPTY(t)
	defer releasePTY1()
	defer releasePTY2()
	defer releasePTY3()
	var openedEnv []string
	f := portsmocks.NewMockPTYFactory(t)
	f.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _ string, _ []string, env []string, _ string, sz domain.Size) (ports.PTY, error) {
			if sz != (domain.Size{Cols: 80, Rows: 22}) {
				return newQuietPTY(), nil
			}
			openedEnv = append([]string(nil), env...)
			return p3, nil
		},
	).Maybe()
	d := newTestDaemon(t, f, stubClock{})
	tr1, _ := newCapturingTransport(t)
	tr2, _ := newCapturingTransport(t)
	ac1 := &attachedClient{tr: tr1, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	ac2 := &attachedClient{tr: tr2, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	sctx1, cancel1 := context.WithCancel(d.serveCtx)
	sctx2, cancel2 := context.WithCancel(d.serveCtx)
	defer cancel1()
	defer cancel2()
	sess1 := &session{id: "s1", name: "alpha", cwd: t.TempDir(), ctx: sctx1, cancel: cancel1, tabs: []*tab{newTestTabWithContext(p1, sctx1, cancel1)}, client: ac1, terminal: terminalEnv{}}
	sess2 := &session{id: "s2", name: "beta", cwd: t.TempDir(), ctx: sctx2, cancel: cancel2, tabs: []*tab{newTestTabWithContext(p2, sctx2, cancel2)}, client: ac2, terminal: terminalEnv{TrueColor: true}}
	ac1.setSession(sess1)
	ac2.setSession(sess2)
	d.sessions[sess1.id] = sess1
	d.sessions[sess2.id] = sess2

	old := d.stealClientForTarget(sess1, ac1, sess2, picker.Target{Session: sess2.id})

	require.Same(t, ac2, old)
	require.False(t, sess2.terminal.TrueColor)
	require.NoError(t, d.createTab(sess2, domain.Size{Cols: 80, Rows: 24}))
	require.Contains(t, openedEnv, "TERM=xterm-256color")
	require.NotContains(t, openedEnv, "COLORTERM=truecolor")
	require.NotContains(t, openedEnv, "TERM=xterm-direct")
}

func TestPickerPreviewSinglePaneSnapshotsFocusedPane(t *testing.T) {
	tb := newTab(nil, domain.Size{Cols: 10, Rows: 3})
	p := tb.focusedPane()
	p.screen.Write([]byte("focused"))

	preview := snapshotPickerPreview(tb)

	require.Equal(t, 10, preview.Width)
	require.Equal(t, 3, preview.Height)
	require.Equal(t, 'f', preview.Rows[0][0].Rune)
	require.Equal(t, 'o', preview.Rows[0][1].Rune)
}

func TestPickerPreviewMultiPaneComposesTabFrame(t *testing.T) {
	tb := newTab(nil, domain.Size{Cols: 41, Rows: 5})
	left := tb.focusedPane()
	left.title.processName = "one"
	left.screen.Write([]byte("L"))
	rightTop := newPane("pane-2", nil, domain.Size{Cols: 20, Rows: 3})
	rightTop.title.processName = "two"
	rightBottom := newPane("pane-3", nil, domain.Size{Cols: 20, Rows: 2})
	rightBottom.title.processName = "three"
	rightBottom.screen.Write([]byte("R"))

	tb.mu.Lock()
	tb.panes[rightTop.id] = rightTop
	tb.panes[rightBottom.id] = rightBottom
	tb.tree.Root = &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{
		layout.NewLeaf(left.id),
		{Kind: layout.Stack, Children: []*layout.Node{layout.NewLeaf(rightTop.id), layout.NewLeaf(rightBottom.id)}, Expanded: rightBottom.id},
	}}
	tb.tree.Focus = rightBottom.id
	tb.mu.Unlock()

	preview := snapshotPickerPreview(tb)

	require.Equal(t, 41, preview.Width)
	require.Equal(t, 5, preview.Height)
	require.Equal(t, 'L', preview.Rows[0][0].Rune, "left pane content should remain visible")
	require.Equal(t, '│', preview.Rows[0][20].Rune, "split divider should be included")
	require.Equal(t, "two", rowText(preview.Rows[0][21:24]), "collapsed stack title bar should be included")
	require.Equal(t, "three", rowText(preview.Rows[1][21:26]), "expanded stack title bar should be included")
	require.Equal(t, 'R', preview.Rows[2][21].Rune, "expanded stacked pane content should be included")
}

func TestPickerModalGeometry(t *testing.T) {
	base := domain.Size{Cols: 100, Rows: 40}

	require.Equal(t, domain.Rect{X: 10, Y: 4, Width: 80, Height: 32}, pickerModal.Bounds(base))
	require.Equal(t, domain.AnchorCenter, pickerModal.Anchor)
}

func TestPickerResizeRecomposesModal(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	defer releasePTY()

	d.enterPicker(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.resize(sess, ac, domain.Size{Cols: 100, Rows: 30})
	// Picker recomposition is the relevant event here; do not depend on a real
	// resize-idle timer in this synchronous rendering test.
	d.paint(sess, ac, false, nil)

	out := awaitFrame(t, sends, ports.MsgOutput)
	msg, err := ports.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	require.Contains(t, string(msg.Data), "┌")
	require.Contains(t, string(msg.Data), "Sessions")
}

func TestResumeStoppedAndSwitchInheritsTerminalEnv(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	p1, release1 := newBlockingPTY(t)
	defer release1()
	p2, release2 := newBlockingPTY(t)
	defer release2()
	var opens [][]string
	f := portsmocks.NewMockPTYFactory(t)
	normalSize := domain.Size{Cols: sz.Cols, Rows: sz.Rows - 2}
	f.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, normalSize).RunAndReturn(
		func(_ context.Context, _ string, _ []string, env []string, _ string, _ domain.Size) (ports.PTY, error) {
			opens = append(opens, append([]string(nil), env...))
			if len(opens) == 1 {
				return p1, nil
			}
			return p2, nil
		},
	).Twice()
	floating := newQuietPTY()
	f.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(got domain.Size) bool {
		return got != normalSize && got.Valid()
	})).Return(floating, nil).Once()
	d := newTestDaemon(t, f, stubClock{})
	d.stopped["old"] = stoppedSession{name: "old", cwd: t.TempDir(), createdAt: 1}
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(mock.Anything).Return(nil).Maybe()
	tr.EXPECT().Close().Return(nil).Maybe()
	sess, ac, err := d.route(ports.Hello{Version: ports.ProtocolVersion, Intent: ports.IntentNew, Name: "work", Size: sz, TrueColor: true}, tr)
	require.NoError(t, err)
	d.resumeStoppedAndSwitch(sess, ac, picker.Target{Name: "old", Stopped: true})
	got := ac.sess.Get()
	require.NotNil(t, got)
	got.mu.Lock()
	require.True(t, got.terminal.TrueColor)
	got.mu.Unlock()
	require.Len(t, opens, 2)
	require.Contains(t, opens[1], "TERM=xterm-direct")
	require.Contains(t, opens[1], "COLORTERM=truecolor")
	require.Contains(t, opens[1], "TERM_PROGRAM=vev")
	_ = d.killSession(got, ports.ReasonSessionKilled, false)
	release1()
	release2()
	d.sessWg.Wait()
	select {
	case <-floating.done:
	default:
		t.Fatal("floating prewarm PTY was not closed")
	}
}

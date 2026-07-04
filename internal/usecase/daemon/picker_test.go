package daemon

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/pkg/renderer"
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

	require.False(t, ac.pickerActive())
	require.Equal(t, []byte("\x1bt"), <-writes)
}

func TestPickerViewsAddsBellSuffixForAttention(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	ctx, cancel := context.WithCancel(d.serveCtx)
	defer cancel()
	current := &session{id: "s1", name: "alpha", ctx: ctx, cancel: cancel, tabs: []*tab{{}, {}}}
	ringing := &session{id: "s2", name: "beta", ctx: ctx, cancel: cancel, tabs: []*tab{{}, {}}}
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
	require.Equal(t, []string{"1", "2"}, views[0].Tabs)
	require.Equal(t, "beta ", views[1].Name)
	require.Equal(t, []string{"1", "2 "}, views[1].Tabs)
}

func TestPickerSameSessionNavigationSwitchAndEscClose(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 2)
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
	awaitFrame(t, sends, ports.MsgOutput)
	d.enterPicker(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	clk := &signalClock{timers: make(chan *signalTimer, 1)}
	d.clock = clk
	d.handleInput(sess, ac, []byte("\x1b"))
	timer := <-clk.timers
	require.True(t, ac.pickerActive())
	timer.ch <- time.Now()
	require.Eventually(t, func() bool { return !ac.pickerActive() }, time.Second, 5*time.Millisecond)
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
			require.True(t, ac.pickerActive())
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
	require.True(t, ac.pickerActive())
	timer.ch <- time.Now()
	require.Eventually(t, func() bool { return !ac.pickerActive() }, time.Second, 5*time.Millisecond)
}

func TestPickerCrossSessionSwitchDetachesExistingClient(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	defer releasePTY1()
	defer releasePTY2()
	d := newTestDaemon(t, nil, stubClock{})
	tr1, sends1 := newCapturingTransport(t)
	tr2, sends2 := newCapturingTransport(t)
	ac1 := &attachedClient{tr: tr1, rend: renderer.New(renderer.Capabilities{}), size: domain.Size{Cols: 80, Rows: 24}}
	ac2 := &attachedClient{tr: tr2, rend: renderer.New(renderer.Capabilities{}), size: domain.Size{Cols: 80, Rows: 24}}
	sctx1, cancel1 := context.WithCancel(d.serveCtx)
	sctx2, cancel2 := context.WithCancel(d.serveCtx)
	defer cancel1()
	defer cancel2()
	sess1 := &session{id: "s1", name: "alpha", ephemeral: true, ctx: sctx1, cancel: cancel1, tabs: []*tab{newTestTabWithContext(p1, sctx1, cancel1)}, client: ac1}
	sess2 := &session{id: "s2", name: "beta", ctx: sctx2, cancel: cancel2, tabs: []*tab{newTestTabWithContext(p2, sctx2, cancel2)}, client: ac2}
	ac1.setSession(sess1)
	ac2.setSession(sess2)
	ac1.keys = keys.NewRouter(d.clock, daemonKeyHandler{d: d, ac: ac1})
	ac2.keys = keys.NewRouter(d.clock, daemonKeyHandler{d: d, ac: ac2})
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
	left.title = "one"
	left.screen.Write([]byte("L"))
	rightTop := newPane("pane-2", nil, domain.Size{Cols: 20, Rows: 3})
	rightTop.title = "two"
	rightBottom := newPane("pane-3", nil, domain.Size{Cols: 20, Rows: 2})
	rightBottom.title = "three"
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

func TestPickerLivePreviewRepaintsInactiveTab(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 2)
	defer func() {
		for _, release := range releases {
			release()
		}
	}()

	d.enterPicker(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("j"))
	awaitFrame(t, sends, ports.MsgOutput)

	previewTab := sess.tabs[1]
	previewTab.mu.Lock()
	previewTab.focusedPane().screen.Write([]byte("inactive-preview-live"))
	previewTab.mu.Unlock()
	d.render(sess, previewTab, previewTab.focusedPane())

	previewOut := awaitFrame(t, sends, ports.MsgOutput)
	previewMsg, err := ports.UnmarshalOutput(previewOut.Payload)
	require.NoError(t, err)
	require.Contains(t, string(previewMsg.Data), "inactive-preview-live")
}

func TestPickerLivePreviewRepaintsCrossSessionTab(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	defer releasePTY1()
	defer releasePTY2()
	d := newTestDaemon(t, nil, stubClock{})
	tr1, sends1 := newCapturingTransport(t)
	tr2, _ := newCapturingTransport(t)
	ac1 := &attachedClient{tr: tr1, rend: renderer.New(renderer.Capabilities{}), size: domain.Size{Cols: 80, Rows: 24}}
	ac2 := &attachedClient{tr: tr2, rend: renderer.New(renderer.Capabilities{}), size: domain.Size{Cols: 80, Rows: 24}}
	sctx1, cancel1 := context.WithCancel(d.serveCtx)
	sctx2, cancel2 := context.WithCancel(d.serveCtx)
	defer cancel1()
	defer cancel2()
	sess1 := &session{id: "s1", name: "alpha", ctx: sctx1, cancel: cancel1, tabs: []*tab{newTestTabWithContext(p1, sctx1, cancel1)}, client: ac1}
	sess2 := &session{id: "s2", name: "beta", ctx: sctx2, cancel: cancel2, tabs: []*tab{newTestTabWithContext(p2, sctx2, cancel2)}, client: ac2}
	ac1.setSession(sess1)
	ac2.setSession(sess2)
	ac1.keys = keys.NewRouter(d.clock, daemonKeyHandler{d: d, ac: ac1})
	ac2.keys = keys.NewRouter(d.clock, daemonKeyHandler{d: d, ac: ac2})
	d.sessions[sess1.id] = sess1
	d.sessions[sess2.id] = sess2

	d.enterPicker(sess1, ac1)
	awaitFrame(t, sends1, ports.MsgOutput)
	d.handleInput(sess1, ac1, []byte("j"))
	awaitFrame(t, sends1, ports.MsgOutput)

	previewTab := sess2.tabs[0]
	previewTab.mu.Lock()
	previewTab.focusedPane().screen.Write([]byte("cross-session-preview-live"))
	previewTab.mu.Unlock()
	d.render(sess2, previewTab, previewTab.focusedPane())

	previewOut := awaitFrame(t, sends1, ports.MsgOutput)
	previewMsg, err := ports.UnmarshalOutput(previewOut.Payload)
	require.NoError(t, err)
	require.Contains(t, string(previewMsg.Data), "cross-session-preview-live")
}

func TestPickerOpenCloseNavigationConcurrentWithRenderRace(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 3)
	defer func() {
		for _, release := range releases {
			release()
		}
	}()

	done := make(chan struct{})
	var drain sync.WaitGroup
	drain.Go(func() {
		for {
			select {
			case <-sends:
			case <-done:
				return
			}
		}
	})

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Go(func() {
			d.enterPicker(sess, ac)
		})
		wg.Go(func() {
			d.handlePickerInput(ac, []byte("j"))
			d.handlePickerInput(ac, []byte("k"))
		})
		wg.Go(func() {
			d.closePicker(ac)
		})
		tb := sess.tabs[i%len(sess.tabs)]
		wg.Go(func() {
			tb.mu.Lock()
			p := tb.focusedPane()
			tb.mu.Unlock()
			if p != nil {
				p.mu.Lock()
				p.screen.Write([]byte("render-race"))
				p.mu.Unlock()
			}
			d.render(sess, tb, p)
		})
	}
	wg.Wait()
	close(done)
	drain.Wait()
}

func TestPickerResizeRecomposesModal(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	defer releasePTY()

	d.enterPicker(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.resize(sess, ac, domain.Size{Cols: 100, Rows: 30})

	out := awaitFrame(t, sends, ports.MsgOutput)
	msg, err := ports.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	require.Contains(t, string(msg.Data), "┌")
	require.Contains(t, string(msg.Data), "Sessions")
}

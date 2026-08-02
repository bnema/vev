package daemon

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
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
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

// --- test doubles -----------------------------------------------------------

func TestBoundedSendOutputErrTransportReturnsTransportUsedBySend(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	ac := &attachedClient{output: newOutputStateStream()}
	ac.initOverlays()
	replacement := &closeTrackingTransport{}
	sendErr := errors.New("send failed")
	failed := &swapErrorTransport{ac: ac, replacement: replacement, err: sendErr}
	ac.replaceTransport(failed)

	used, err := d.boundedSendOutputErrTransport(ac, []byte("copy"))

	require.ErrorIs(t, err, sendErr)
	require.Same(t, failed, used)
	require.Same(t, replacement, ac.transport())
}

func TestOwnedSynchronousSendReturnsCapturedTransportAcrossReplacement(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	replacement := &closeTrackingTransport{}
	sendErr := errors.New("owned send failed")
	ac := &attachedClient{output: newOutputStateStream()}
	failed := &ownedSwapErrorTransport{ac: ac, replacement: replacement, err: sendErr, sent: make(chan ports.Frame, 1)}
	ac.replaceTransport(failed)
	sess := &session{sessionCore: sessionCore{name: "work", client: ac}}
	ac.setSession(sess)

	used, err := d.boundedSendOutputErrTransport(ac, []byte("copy"))

	require.ErrorIs(t, err, sendErr)
	require.Same(t, failed, used)
	require.Same(t, replacement, ac.transport())
	out, decodeErr := ports.UnmarshalOutput((<-failed.sent).Payload)
	require.NoError(t, decodeErr)
	require.Equal(t, []byte("copy"), out.Data)
	d.detachOnSendError(sess, ac, used)
	require.Same(t, ac, sess.client)
	require.False(t, replacement.Closed())
}

type swapErrorTransport struct {
	ac          *attachedClient
	replacement ports.Transport
	err         error
}

func (t *swapErrorTransport) Send(ports.Frame) error {
	t.ac.replaceTransport(t.replacement)
	return t.err
}

func (t *swapErrorTransport) Recv() (ports.Frame, error) { return ports.Frame{}, io.EOF }
func (t *swapErrorTransport) Close() error               { return nil }

type ownedSwapErrorTransport struct {
	ac          *attachedClient
	replacement ports.Transport
	err         error
	sent        chan ports.Frame
}

func (t *ownedSwapErrorTransport) Send(f ports.Frame) error { return t.SendSynchronous(f) }
func (t *ownedSwapErrorTransport) SendSynchronous(f ports.Frame) error {
	t.sent <- f
	t.ac.replaceTransport(t.replacement)
	return t.err
}
func (t *ownedSwapErrorTransport) Recv() (ports.Frame, error) { return ports.Frame{}, io.EOF }
func (t *ownedSwapErrorTransport) Close() error               { return nil }

// stubClock returns timers whose channel never fires, so a scheduler under it
// blocks in its debounce loop until the session context is cancelled. Used by

func TestCopySearchModalGeometry(t *testing.T) {
	base := domain.Size{Cols: 100, Rows: 40}

	require.Equal(t, domain.Rect{X: 0, Y: 28, Width: 100, Height: 11}, copySearchModal.Resolve(base).Bounds)
	require.Equal(t, domain.AnchorBottom, copySearchModal.Anchor)
	require.Equal(t, ui.Margins{Bottom: 1}, copySearchModal.Margins)
}

func TestCopyModeDocumentCarriesPaneRowIDs(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	pane := sess.activeTab().focusedPane()
	appendHistoryRow(t, pane.history, testRow("history"))
	pane.screen.Write([]byte("live"))
	historyID := pane.history.View().RowID(0)
	liveID := pane.screen.RowID(0)

	d.enterCopyMode(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	doc := ac.overlays.copyDocument

	require.Equal(t, 0, doc.FindRowID(historyID))
	require.Equal(t, pane.history.Len(), doc.FindRowID(liveID))
	require.Equal(t, liveID, doc.RowID(pane.history.Len()))
}

func TestComposeCopyClientFrameConcurrentPaneOutput(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	_, sess, _, _ := newManualSessionWithPTYs(t, p)
	tb := sess.activeTab()
	pane := tb.focusedPane()
	pane.mu.Lock()
	snap := scopy.NewSnapshot(pane.history, pane.screen.Frame, pane.screen.LineBounds(), nil)
	pane.mu.Unlock()
	mode := scopy.NewMode(scopy.NewDocument(snap, domain.DefaultWordSeparators))

	base := renderer.NewFrame(80, 25)
	target := domain.Rect{X: 0, Y: 1, Width: 80, Height: 23}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 200 {
			pane.mu.Lock()
			pane.screen.Write([]byte("output\n"))
			pane.mu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		for range 200 {
			_, _ = composeCopyClientFrame(mode, target, base, resolveStyles(nil))
		}
	}()
	wg.Wait()
}

func TestCopyModeFrameIncludesTopAndBottomChrome(t *testing.T) {
	p, release := newBlockingPTY(t)
	_, sess, _, _ := newManualSessionWithPTYs(t, p)
	defer release()
	sess.name = "work"
	tb := sess.activeTab()
	tb.focusedPane().screen = vt.NewScreen(12, 3)
	tb.focusedPane().screen.Write([]byte("live"))
	snap := scopy.NewSnapshot(tb.focusedPane().history, tb.focusedPane().screen.Frame, tb.focusedPane().screen.LineBounds(), nil)
	mode := scopy.NewMode(scopy.NewDocument(snap, domain.DefaultWordSeparators))

	bars := barState{status: sess.statusSegments(true)}
	content := domain.Rect{Width: tb.size.Cols, Height: tb.size.Rows}
	base := composeFrame(capturedRenderState{
		reset:  true,
		layout: capturedTabLayout{area: content, focus: tb.focusedPane().id, valid: true},
		panes: []capturedPaneRenderState{{
			id: tb.focusedPane().id, frame: tb.focusedPane().screen.Frame.Clone(), placement: layout.Placement{ID: tb.focusedPane().id, Content: content}, focused: true, damage: []renderer.Damage{renderer.FullRedraw()},
		}},
		bars: bars,
	}, composeCacheInput{}).frame
	frame, damage := composeCopyClientFrame(mode, domain.Rect{X: 0, Y: 1, Width: 12, Height: 3}, base, resolveStyles(nil))

	require.Equal(t, 80, frame.Width)
	require.Equal(t, 25, frame.Height)
	require.Equal(t, " 1 (sh)", strings.TrimRight(rowText(frame.Row(0)), " "))
	require.Contains(t, rowText(frame.Row(1)), "live")
	require.Contains(t, rowText(frame.Row(24)), "[SCROLL]")
	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, damage)
}

func TestCopyModePaletteCommandEntersAndDoesNotForward(t *testing.T) {
	writes := make(chan []byte, 1)
	p, _ := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	installTestHistory(sess.tabs[0].focusedPane(), vt.HistoryConfig{MaxRows: 4})
	sess.tabs[0].focusedPane().screen.Write([]byte("live"))

	d.handleInput(sess, ac, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("VIS\r"))

	if ac.overlays.copyMode == nil {
		t.Fatal("scrollback mode not entered")
	}
	select {
	case got := <-writes:
		t.Fatalf("scrollback binding forwarded to PTY: %q", got)
	default:
	}
	out := awaitFrame(t, sends, ports.MsgOutput)
	msg, err := ports.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	if got := string(msg.Data); !strings.Contains(got, "[SCROLL]") || strings.Contains(got, "[SELECT]") || strings.Contains(got, "[COPY]") {
		t.Fatalf("passive scrollback paint = %q, want [SCROLL] without [SELECT]/[COPY]", got)
	}

	d.handleInput(sess, ac, []byte(" "))
	out = awaitFrame(t, sends, ports.MsgOutput)
	msg, err = ports.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	if got := string(msg.Data); !strings.Contains(got, "[SELECT]") || strings.Contains(got, "[SCROLL]") {
		t.Fatalf("visual selection paint = %q, want [SELECT] without [SCROLL]", got)
	}
}

func TestCopyModeSearchModalRoutesBatchedInputAfterSlash(t *testing.T) {
	writes := make(chan []byte, 1)
	p, _ := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	pane := sess.tabs[0].focusedPane()
	copy(pane.screen.Frame.Row(0), testRow("alpha"))
	copy(pane.screen.Frame.Row(1), testRow("beta alpha"))

	d.enterCopyMode(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("/alpha\r"))
	awaitFrame(t, sends, ports.MsgOutput)

	require.Nil(t, ac.overlays.copySearch)
	require.NotNil(t, ac.overlays.copyMode)
	require.Equal(t, 0, ac.overlays.copyMode.Cursor().Row)
	select {
	case got := <-writes:
		t.Fatalf("batched visual search input forwarded to PTY: %q", got)
	default:
	}
}

func TestCopyModeSearchModalJumpsAndKeepsNavigation(t *testing.T) {
	writes := make(chan []byte, 1)
	p, _ := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	pane := sess.tabs[0].focusedPane()
	copy(pane.screen.Frame.Row(0), testRow("alpha"))
	copy(pane.screen.Frame.Row(1), testRow("beta alpha"))
	copy(pane.screen.Frame.Row(2), testRow("gamma"))

	d.enterCopyMode(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("/"))
	out := awaitFrame(t, sends, ports.MsgOutput)
	msg, err := ports.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	require.Contains(t, string(msg.Data), "Search")
	require.NotNil(t, ac.overlays.copySearch)

	d.handleInput(sess, ac, []byte("alpha"))
	out = awaitFrame(t, sends, ports.MsgOutput)
	msg, err = ports.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	require.Contains(t, string(msg.Data), "/alpha")
	require.Contains(t, string(msg.Data), "1:1  alpha")
	require.Contains(t, string(msg.Data), "2:6  beta alpha")

	d.handleInput(sess, ac, []byte("\r"))
	awaitFrame(t, sends, ports.MsgOutput)
	require.Nil(t, ac.overlays.copySearch)
	require.Equal(t, 0, ac.overlays.copyMode.Cursor().Row)

	d.handleInput(sess, ac, []byte("n"))
	awaitFrame(t, sends, ports.MsgOutput)
	require.Equal(t, 1, ac.overlays.copyMode.Cursor().Row)
	d.handleInput(sess, ac, []byte("N"))
	awaitFrame(t, sends, ports.MsgOutput)
	require.Equal(t, 0, ac.overlays.copyMode.Cursor().Row)

	select {
	case got := <-writes:
		t.Fatalf("visual search input forwarded to PTY: %q", got)
	default:
	}
}

func TestCopyModeSearchModalSelectionPreviewsBehindModal(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	pane := sess.tabs[0].focusedPane()
	copy(pane.screen.Frame.Row(0), testRow("alpha"))
	copy(pane.screen.Frame.Row(1), testRow("beta alpha"))
	copy(pane.screen.Frame.Row(2), testRow("gamma"))

	d.enterCopyMode(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)

	d.handleInput(sess, ac, []byte("/"))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("alpha"))
	awaitFrame(t, sends, ports.MsgOutput)
	require.NotNil(t, ac.overlays.copySearch)
	require.Equal(t, 0, ac.overlays.copyMode.Cursor().Row, "typing a query previews the selected first result behind the modal")

	d.handleInput(sess, ac, []byte{0x0e})
	awaitFrame(t, sends, ports.MsgOutput)
	require.NotNil(t, ac.overlays.copySearch)
	require.Equal(t, 1, ac.overlays.copyMode.Cursor().Row, "moving modal selection previews that result without Enter")
}

func TestCopyModeSearchModalCapturesMouseAndClearsOnExit(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	pane := sess.tabs[0].focusedPane()
	installTestHistory(pane, vt.HistoryConfig{MaxRows: 4})
	appendHistoryRow(t, pane.history, testRow("old alpha"))
	copy(pane.screen.Frame.Row(0), testRow("live alpha"))

	d.enterCopyMode(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("/alpha"))
	awaitFrame(t, sends, ports.MsgOutput)
	require.NotNil(t, ac.overlays.copySearch)
	cursor := ac.overlays.copyMode.Cursor().Row

	d.handleInput(sess, ac, []byte("\x1b[<64;1;1M"))
	require.NotNil(t, ac.overlays.copySearch)
	require.Equal(t, cursor, ac.overlays.copyMode.Cursor().Row)

	d.handleInput(sess, ac, []byte("\x1b"))
	awaitFrame(t, sends, ports.MsgOutput)
	require.Nil(t, ac.overlays.copySearch)
	require.NotNil(t, ac.overlays.copyMode)
	d.handleInput(sess, ac, []byte("q"))
	awaitFrame(t, sends, ports.MsgOutput)
	require.Nil(t, ac.overlays.copySearch)
	require.Nil(t, ac.overlays.copyMode)
}

func TestCopyModeInputNotForwardedAndOSC52Copy(t *testing.T) {
	writes := make(chan []byte, 1)
	p, _ := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	installTestHistory(sess.tabs[0].focusedPane(), vt.HistoryConfig{MaxRows: 4})
	appendHistoryRow(t, sess.tabs[0].focusedPane().history, testRow("old1    "))
	appendHistoryRow(t, sess.tabs[0].focusedPane().history, testRow("old2    "))
	sess.tabs[0].focusedPane().screen.Write([]byte("live"))

	d.enterCopyMode(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte{'g', ' ', 'j', 'y'})

	select {
	case got := <-writes:
		t.Fatalf("copy-mode navigation forwarded to PTY: %q", got)
	default:
	}
	out := awaitFrame(t, sends, ports.MsgOutput)
	msg, err := ports.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	if got, want := string(msg.Data), "\x1b]52;c;b2xkMQpvbGQy\x07"; got != want {
		t.Fatalf("OSC52 = %q, want %q", got, want)
	}
	if ac.overlays.copyMode != nil {
		t.Fatal("copy mode still active after yank")
	}
	live := awaitFrame(t, sends, ports.MsgOutput)
	liveMsg, err := ports.UnmarshalOutput(live.Payload)
	require.NoError(t, err)
	if strings.Contains(string(liveMsg.Data), "[COPY]") || strings.Contains(string(liveMsg.Data), "[SCROLL]") {
		t.Fatalf("live repaint still contains copy/scroll status: %q", string(liveMsg.Data))
	}
	if !strings.Contains(string(liveMsg.Data), "copied 9 chars to clipboard") {
		t.Fatalf("live repaint = %q, want copy success feedback", string(liveMsg.Data))
	}
	if !strings.Contains(string(liveMsg.Data), "live") {
		t.Fatalf("live repaint = %q, want live screen", string(liveMsg.Data))
	}

	d.paint(sess, ac, true, nil)
	followup := awaitFrame(t, sends, ports.MsgOutput)
	followupMsg, err := ports.UnmarshalOutput(followup.Payload)
	require.NoError(t, err)
	if strings.Contains(string(followupMsg.Data), "copied 9 chars to clipboard") {
		t.Fatalf("copy feedback persisted after next repaint: %q", string(followupMsg.Data))
	}
}

func TestScrollbackEvictionFeedsCopyModeYank(t *testing.T) {
	p := portsmocks.NewMockPTY(t)
	reads := make(chan []byte, 64)
	readDone := make(chan struct{})
	var closeOnce sync.Once
	p.EXPECT().Read(mock.Anything).RunAndReturn(func(b []byte) (int, error) {
		select {
		case data := <-reads:
			return copy(b, data), nil
		case <-readDone:
			return 0, io.EOF
		}
	}).Maybe()
	p.EXPECT().Write(mock.Anything).Return(0, errors.New("unexpected PTY write")).Maybe()
	p.EXPECT().Resize(mock.Anything).Return(nil).Maybe()
	p.EXPECT().Close().RunAndReturn(func() error { closeOnce.Do(func() { close(readDone) }); return nil }).Maybe()
	p.EXPECT().Pid().Return(4242).Maybe()

	// Rendering is coordinator-driven: advance a controllable clock through the
	// retained resize timer and its resulting coordinator wake rather than
	// relying on wall-clock debounce delivery.
	clk := newCoordinatorMockClock(t, 16)
	d := newTestDaemon(t, newFactory(t, p), clk.clock)
	tr, sends, releaseConn := newConn(t, mustHello(ports.IntentNew, "work", domain.Size{Cols: 16, Rows: 5}))
	advanceRender := func() {
		deadline := time.NewTimer(2 * time.Second)
		defer deadline.Stop()
		select {
		case timer := <-clk.timers:
			timer.ch <- time.Time{}
		case <-deadline.C:
			t.Fatal("coordinator did not arm a controllable render timer")
		}
	}
	var hg sync.WaitGroup
	hg.Go(func() { d.handleConn(tr) })
	awaitFrame(t, sends, ports.MsgWelcome)
	advanceRender() // initial coordinator invalidation
	awaitFrame(t, sends, ports.MsgOutput)

	for i := range 12 {
		reads <- fmt.Appendf(nil, "line-%02d\r\n", i)
	}
	require.Eventually(t, func() bool {
		sess := firstSession(d)
		if sess == nil {
			return false
		}
		win := sess.activeTab()
		if win == nil {
			return false
		}
		win.mu.Lock()
		p := win.focusedPane()
		win.mu.Unlock()
		if p == nil {
			return false
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		return scopy.NewSnapshot(p.history, p.screen.Frame, p.screen.LineBounds(), nil).Len() >= 12
	}, 2*time.Second, 5*time.Millisecond)

	sess := firstSession(d)
	require.NotNil(t, sess)
	ac := sess.client
	require.NotNil(t, ac)
	d.handleInput(sess, ac, []byte("\x1b "))
	awaitCoordinatorOutput(t, sends, clk.timers, "while advancing render clock", "controllable timers did not produce an output frame")
	d.handleInput(sess, ac, []byte("VIS\r"))
	awaitCoordinatorOutput(t, sends, clk.timers, "while advancing render clock", "controllable timers did not produce an output frame")
	d.handleInput(sess, ac, []byte{'g', ' ', 'G', 'y'})

	var payload string
	require.Eventually(t, func() bool {
		out := awaitFrame(t, sends, ports.MsgOutput)
		msg, err := ports.UnmarshalOutput(out.Payload)
		require.NoError(t, err)
		payload = string(msg.Data)
		return strings.HasPrefix(payload, "\x1b]52;c;")
	}, 2*time.Second, 5*time.Millisecond)
	require.True(t, strings.HasPrefix(payload, "\x1b]52;c;"), "OSC52 payload = %q", payload)
	require.True(t, strings.HasSuffix(payload, "\a"), "OSC52 payload = %q", payload)
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSuffix(strings.TrimPrefix(payload, "\x1b]52;c;"), "\a"))
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(string(decoded), "line-00"), "decoded yank = %q", string(decoded))

	releaseConn()
	closeOnce.Do(func() { close(readDone) })
	hg.Wait()
	d.sessWg.Wait()
}

func TestCopyModeEscapeRestoresLiveFullRepaint(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	installTestHistory(sess.tabs[0].focusedPane(), vt.HistoryConfig{MaxRows: 4})
	sess.tabs[0].focusedPane().screen.Write([]byte("live"))

	d.enterCopyMode(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("q"))

	if ac.overlays.copyMode != nil {
		t.Fatal("copy mode still active after q")
	}
	out := awaitFrame(t, sends, ports.MsgOutput)
	msg, err := ports.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	if strings.Contains(string(msg.Data), "[COPY]") || !strings.Contains(string(msg.Data), "live") {
		t.Fatalf("exit repaint = %q, want live full repaint without copy status", string(msg.Data))
	}
}

func TestCopyModeSplitArrowDoesNotExit(t *testing.T) {
	cases := []struct {
		name       string
		input      [][]byte
		wantCursor int
	}{
		{name: "escape then up arrow", input: [][]byte{[]byte("\x1b"), []byte("[A")}, wantCursor: 23},
		{name: "escape then down arrow", input: [][]byte{[]byte("g"), []byte("\x1b"), []byte("[B")}, wantCursor: 1},
		{name: "split up arrow", input: [][]byte{[]byte("\x1b["), []byte("A")}, wantCursor: 23},
		{name: "split SS3 up arrow", input: [][]byte{[]byte("\x1bO"), []byte("A")}, wantCursor: 23},
		{name: "split page down", input: [][]byte{[]byte("g"), []byte("\x1b[6"), []byte("~")}, wantCursor: 23},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := newBlockingPTY(t)
			d, sess, ac, sends := newManualSessionWithPTYs(t, p)
			installTestHistory(sess.tabs[0].focusedPane(), vt.HistoryConfig{MaxRows: 4})
			appendHistoryRow(t, sess.tabs[0].focusedPane().history, testRow("old1    "))
			appendHistoryRow(t, sess.tabs[0].focusedPane().history, testRow("old2    "))
			sess.tabs[0].focusedPane().screen.Write([]byte("live"))

			d.enterCopyMode(sess, ac)
			awaitFrame(t, sends, ports.MsgOutput)
			for _, input := range tc.input {
				d.handleInput(sess, ac, input)
			}

			require.NotNil(t, ac.overlays.copyMode)
			require.Equal(t, tc.wantCursor, ac.overlays.copyMode.Cursor().Row)
		})
	}
}

func TestCopyModeOversizedYankShowsTooLargeFeedback(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	installTestHistory(sess.tabs[0].focusedPane(), vt.HistoryConfig{MaxRows: 1})
	longLine := strings.Repeat("x", scopy.OSC52MaxPayloadBytes+1)
	sess.tabs[0].focusedPane().screen.Frame = renderer.NewFrame(len(longLine), 1)
	copy(sess.tabs[0].focusedPane().screen.Frame.Row(0), testRow(longLine))

	d.enterCopyMode(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte{' ', 'y'})

	out := awaitFrame(t, sends, ports.MsgOutput)
	msg, err := ports.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	if strings.Contains(string(msg.Data), "\x1b]52;") {
		t.Fatalf("oversized yank emitted OSC52: %q", string(msg.Data))
	}
	if !strings.Contains(string(msg.Data), "selection too large to copy") {
		t.Fatalf("oversized yank repaint = %q, want too-large feedback", string(msg.Data))
	}
}

func TestCopyModeLoneEscapeExitsAfterDelay(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	clk := &signalClock{timers: make(chan *signalTimer, 1)}
	d.clock = clk
	installTestHistory(sess.tabs[0].focusedPane(), vt.HistoryConfig{MaxRows: 4})
	sess.tabs[0].focusedPane().screen.Write([]byte("live"))

	d.enterCopyMode(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("\x1b"))
	timer := <-clk.timers
	ac.overlays.copyMu.Lock()
	require.NotNil(t, ac.overlays.copyMode)
	ac.overlays.copyMu.Unlock()
	timer.ch <- time.Now()
	require.Eventually(t, func() bool {
		ac.overlays.copyMu.Lock()
		defer ac.overlays.copyMu.Unlock()
		return ac.overlays.copyMode == nil
	}, time.Second, 5*time.Millisecond)
}

func TestCopyModePendingEscapeDoesNotCloseNewMode(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	clk := &signalClock{timers: make(chan *signalTimer, 1)}
	d.clock = clk
	installTestHistory(sess.tabs[0].focusedPane(), vt.HistoryConfig{MaxRows: 4})
	sess.tabs[0].focusedPane().screen.Write([]byte("live"))

	d.enterCopyMode(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("\x1b"))
	timer := <-clk.timers
	d.enterCopyMode(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)

	ac.overlays.copyMu.Lock()
	require.Nil(t, ac.overlays.copyESC.timer)
	require.Nil(t, ac.overlays.copyESC.done)
	require.Empty(t, ac.overlays.copyPending)
	ac.overlays.copyMu.Unlock()
	d.handleInput(sess, ac, []byte("x"))
	require.True(t, ac.overlays.copyActive(), "pending input from the replaced mode affected the new mode")

	timer.ch <- time.Now()
	require.Never(t, func() bool {
		ac.overlays.copyMu.Lock()
		defer ac.overlays.copyMu.Unlock()
		return ac.overlays.copyMode == nil
	}, 50*time.Millisecond, 5*time.Millisecond)
	ac.overlays.copyMu.Lock()
	require.Nil(t, ac.overlays.copyESC.timer)
	require.Nil(t, ac.overlays.copyESC.done)
	require.Empty(t, ac.overlays.copyPending)
	ac.overlays.copyMu.Unlock()
}

func TestCopyModeEmptyYankDoesNotClearClipboard(t *testing.T) {
	cases := []struct {
		name  string
		input []byte
	}{
		{name: "yank", input: []byte("y")},
		{name: "enter", input: []byte("\r")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := newBlockingPTY(t)
			d, sess, ac, sends := newManualSessionWithPTYs(t, p)
			installTestHistory(sess.tabs[0].focusedPane(), vt.HistoryConfig{MaxRows: 4})
			sess.tabs[0].focusedPane().screen.Write([]byte("live"))

			d.enterCopyMode(sess, ac)
			awaitFrame(t, sends, ports.MsgOutput)
			d.handleInput(sess, ac, tc.input)

			if ac.overlays.copyMode != nil {
				t.Fatal("copy mode still active after empty yank")
			}
			out := awaitFrame(t, sends, ports.MsgOutput)
			msg, err := ports.UnmarshalOutput(out.Payload)
			require.NoError(t, err)
			if strings.Contains(string(msg.Data), "\x1b]52;") {
				t.Fatalf("empty yank emitted OSC52 clipboard clear: %q", string(msg.Data))
			}
			if strings.Contains(string(msg.Data), "[COPY]") || !strings.Contains(string(msg.Data), "live") {
				t.Fatalf("empty yank repaint = %q, want live full repaint", string(msg.Data))
			}
		})
	}
}

func TestHandleCopyInputUsesImmutableSnapshotWithoutPaneLock(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	pane := sess.activeTab().focusedPane()
	pane.screen.Write([]byte("live"))
	d.enterCopyMode(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)

	pane.mu.Lock()
	defer pane.mu.Unlock()
	done := make(chan struct{})
	go func() {
		d.handleCopyInput(ac, []byte("x"))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("immutable copy input waited for the live pane lock")
	}
}

func TestCopyModeEnterExitConcurrentWithPaintRace(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	installTestHistory(sess.tabs[0].focusedPane(), vt.HistoryConfig{MaxRows: 4})
	sess.tabs[0].focusedPane().screen.Write([]byte("live"))

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
	for range 100 {
		wg.Go(func() {
			d.enterCopyMode(sess, ac)
			d.handleInput(sess, ac, []byte("q"))
		})
		wg.Go(func() {
			d.paint(sess, ac, true, nil)
		})
	}
	wg.Wait()
	close(done)
	drain.Wait()
}

func TestCursorTailHidesInCopyMode(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	sess.tabs[0].focusedPane().screen.Write([]byte("live"))

	d.enterCopyMode(sess, ac)
	data := mustOutputData(t, sends)
	require.Contains(t, string(data), "\x1b[?25l")
}

func installFloatingCopyFixture(t *testing.T, sess *session, size domain.Size) *pane {
	t.Helper()
	floatingPTY, releaseFloating := newBlockingPTY(t)
	t.Cleanup(releaseFloating)
	fp := newPane("floating", floatingPTY, size)
	installTestFloating(sess.activeTab(), fp, true)
	return fp
}

func TestCopyModeCapturesSourceAndRetainsItAcrossFocusMove(t *testing.T) {
	cases := []struct {
		name        string
		useFloating bool
		wantText    string
	}{
		{name: "floating source", useFloating: true, wantText: "flt-old\nflt-live"},
		{name: "normal source", useFloating: false, wantText: "one-old\none-live"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			normalWrites := make(chan []byte, 1)
			normal, releaseNormal := newBlockingPTYWithWrites(t, normalWrites)
			defer releaseNormal()
			d, sess, ac, sends := newManualSessionWithPTYs(t, normal)
			tb := sess.activeTab()
			second := newPane("pane-2", nil, domain.Size{Cols: 40, Rows: 5})
			tb.mu.Lock()
			tb.tree.Root = &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}
			tb.panes["pane-2"] = second
			tb.mu.Unlock()
			main := tb.focusedPane()
			var want *pane
			if tc.useFloating {
				want = installFloatingCopyFixture(t, sess, domain.Size{Cols: 20, Rows: 3})
				appendHistoryRow(t, want.history, testRow("flt-old"))
				want.screen.Write([]byte("flt-live"))
			} else {
				want = main
				appendHistoryRow(t, want.history, testRow("one-old"))
				want.screen.Write([]byte("one-live"))
			}

			d.enterCopyMode(sess, ac)
			awaitFrame(t, sends, ports.MsgOutput)
			ac.overlays.copyMu.Lock()
			captured := ac.overlays.copyPane
			ac.overlays.copyMu.Unlock()
			require.Same(t, want, captured)

			tb.mu.Lock()
			focusPlacementLocked(tb, "pane-2")
			tb.mu.Unlock()

			d.handleInput(sess, ac, []byte{'g', ' ', 'j', 'y'})
			out := awaitFrame(t, sends, ports.MsgOutput)
			msg, err := ports.UnmarshalOutput(out.Payload)
			require.NoError(t, err)
			wantOSC := "\x1b]52;c;" + base64.StdEncoding.EncodeToString([]byte(tc.wantText)) + "\x07"
			require.Equal(t, wantOSC, string(msg.Data))
			require.Nil(t, ac.overlays.copyMode)
			select {
			case got := <-normalWrites:
				t.Fatalf("copy-mode input forwarded to normal PTY: %q", got)
			default:
			}
		})
	}
}

func TestFloatingCopyModeWheelUsesCapturedSnapshot(t *testing.T) {
	normal, releaseNormal := newBlockingPTY(t)
	defer releaseNormal()
	d, sess, ac, sends := newManualSessionWithPTYs(t, normal)
	fp := installFloatingCopyFixture(t, sess, domain.Size{Cols: 20, Rows: 3})
	for i := range 30 {
		appendHistoryRow(t, fp.history, testRow(fmt.Sprintf("old-%02d", i)))
	}
	fp.screen.Write([]byte("live"))
	fp.mu.Lock()
	total := scopy.NewSnapshot(fp.history, fp.screen.Frame, fp.screen.LineBounds(), nil).Len()
	fp.mu.Unlock()

	d.enterCopyMode(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	require.Equal(t, total-1, ac.overlays.copyMode.Cursor().Row)

	d.copyWheel(sess, ac, -3)
	awaitFrame(t, sends, ports.MsgOutput)
	require.Equal(t, total-4, ac.overlays.copyMode.Cursor().Row)

	d.copyWheel(sess, ac, 3)
	awaitFrame(t, sends, ports.MsgOutput)
	require.Nil(t, ac.overlays.copyMode, "wheel down reaching the captured bottom exits copy mode")
}

func TestFloatingCopyModeMouseSelectsFloatingRows(t *testing.T) {
	normal, releaseNormal := newBlockingPTY(t)
	defer releaseNormal()
	d, sess, ac, sends := newManualSessionWithPTYs(t, normal)
	fp := installFloatingCopyFixture(t, sess, domain.Size{Cols: 20, Rows: 3})
	for i := range 5 {
		appendHistoryRow(t, fp.history, testRow(fmt.Sprintf("old-%d", i)))
	}
	fp.screen.Write([]byte("live"))

	d.enterCopyMode(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	ac.overlays.copyMu.Lock()
	viewportTop := ac.overlays.copyMode.ViewportTop
	ac.overlays.copyMu.Unlock()
	inner := calculateContentFloatingGeometry(domain.Size{Cols: 80, Rows: 23}, d.currentFloatingConfig()).Inner

	// Copy body row k renders at wire row inner.Y+k+2 (top bar + content
	// offset); clicking a row must select exactly that row.
	press := fmt.Sprintf("\x1b[<0;%d;%dM", inner.X+3, inner.Y+3)
	d.handleInput(sess, ac, []byte(press))
	awaitFrame(t, sends, ports.MsgOutput)
	require.Equal(t, viewportTop+1, ac.overlays.copyMode.Cursor().Row)

	motion := fmt.Sprintf("\x1b[<32;%d;%dM", inner.X+3, inner.Y+4)
	d.handleInput(sess, ac, []byte(motion))
	awaitFrame(t, sends, ports.MsgOutput)
	selection := ac.overlays.copyMode.Selection()
	require.True(t, selection.Enabled)
	require.Equal(t, viewportTop+1, selection.Anchor.Row)
	require.Equal(t, viewportTop+2, selection.Active.Row)
}

func TestFloatingExitClearsCopyModeBeforeRepaint(t *testing.T) {
	normal, releaseNormal := newBlockingPTY(t)
	defer releaseNormal()
	d, sess, ac, sends := newManualSessionWithPTYs(t, normal)
	tb := sess.activeTab()
	fp := installFloatingCopyFixture(t, sess, domain.Size{Cols: 20, Rows: 3})
	fp.screen.Write([]byte("flt-live"))

	d.enterCopyMode(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	tb.mu.Lock()
	generation := tb.floating.generation
	publishPaneOwner(fp, sess, tb, generation)
	tb.mu.Unlock()

	d.reapInstalledFloating(fp)

	ac.overlays.copyMu.Lock()
	require.Nil(t, ac.overlays.copyMode)
	require.Nil(t, ac.overlays.copyPane)
	ac.overlays.copyMu.Unlock()
	data := mustOutputData(t, sends)
	require.NotContains(t, string(data), "[SCROLL]")
	require.NotContains(t, string(data), "flt-live")
}

func TestMouseDragCopyEntryCapturesSourceForYank(t *testing.T) {
	pty, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, pty)
	pane := sess.tabs[0].focusedPane()
	copy(pane.screen.Frame.Row(0), testRow("alpha"))
	copy(pane.screen.Frame.Row(1), testRow("bravo"))

	d.handleInput(sess, ac, []byte("\x1b[<0;1;2M\x1b[<32;1;3M"))
	mustOutputData(t, sends)
	require.NotNil(t, ac.overlays.copyMode)
	ac.overlays.copyMu.Lock()
	captured := ac.overlays.copyPane
	ac.overlays.copyMu.Unlock()
	require.Same(t, pane, captured, "drag entry must capture the copy source pane")

	d.handleInput(sess, ac, []byte("y"))
	out := awaitFrame(t, sends, ports.MsgOutput)
	msg, err := ports.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	want := "\x1b]52;c;" + base64.StdEncoding.EncodeToString([]byte("alpha\nb")) + "\x07"
	require.Equal(t, want, string(msg.Data))
}

func TestComposeCopyClientFrameStylesStatusContentAndSurround(t *testing.T) {
	for _, tt := range []struct {
		name       string
		selectMode bool
		want       string
	}{
		{name: "scroll", want: "[SCROLL]"},
		{name: "selection", selectMode: true, want: "[SELECT]"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			base := renderer.NewFrame(20, 8)
			pane := newPane("split", nil, domain.Size{Cols: 12, Rows: 2})
			pane.screen.Write([]byte("copy"))
			mode := scopy.NewMode(scopy.NewDocument(scopy.NewSnapshot(pane.history, pane.screen.Frame, pane.screen.LineBounds(), nil), domain.DefaultWordSeparators))
			if tt.selectMode {
				mode.ToggleLineSelection()
			}
			styles := resolveStyles(nil)
			styles.SurfaceBar = renderer.Style{Foreground: 7, Background: 3}
			styles.CopyStatus = renderer.Style{Foreground: 5, Background: 4}

			frame, _ := composeCopyClientFrame(mode, domain.Rect{X: 0, Y: 1, Width: 12, Height: 2}, base, styles)
			status := frame.Row(frame.Height - 1)
			for x := range 12 {
				require.Truef(t, status[x].Style.Equal(styles.CopyStatus), "status content cell %d must use CopyStatus", x)
			}
			for x := 12; x < len(status); x++ {
				require.Truef(t, status[x].Style.Equal(styles.SurfaceBar), "surrounding status cell %d must use SurfaceBar", x)
			}
			require.Contains(t, rowText(status), tt.want)
		})
	}
}

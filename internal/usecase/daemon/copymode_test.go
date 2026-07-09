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
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

// --- test doubles -----------------------------------------------------------

func TestBoundedSendOutputErrTransportReturnsTransportUsedBySend(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	ac := &attachedClient{}
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

// stubClock returns timers whose channel never fires, so a scheduler under it
// blocks in its debounce loop until the session context is cancelled. Used by

func TestComposeCopyClientFrameConcurrentPaneOutput(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	_, sess, _, _ := newManualSessionWithPTYs(t, p)
	tb := sess.activeTab()
	pane := tb.focusedPane()
	pane.mu.Lock()
	snap := scopy.NewSnapshot(pane.scrollback, pane.screen.Frame)
	pane.mu.Unlock()
	mode := scopy.NewMode(snap)
	bars := barState{status: sess.statusSegments()}

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
			tb.mu.Lock()
			_, _ = composeCopyClientFrame(mode, tb, bars)
			tb.mu.Unlock()
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
	snap := scopy.NewSnapshot(tb.focusedPane().scrollback, tb.focusedPane().screen.Frame)
	mode := scopy.NewMode(snap)

	frame, damage := composeCopyClientFrame(mode, tb, barState{status: sess.statusSegments()})

	require.Equal(t, 12, frame.Width)
	require.Equal(t, 5, frame.Height)
	require.Equal(t, " 1          ", rowText(frame.Row(0)))
	require.Contains(t, rowText(frame.Row(4)), "[SCROLL]")
	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, damage)
}

func TestCopyModePaletteCommandEntersAndDoesNotForward(t *testing.T) {
	writes := make(chan []byte, 1)
	p, _ := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	sess.tabs[0].focusedPane().scrollback = scopy.NewScrollback(4)
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
	require.Equal(t, 0, ac.overlays.copyMode.Cursor)
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
	require.Equal(t, 0, ac.overlays.copyMode.Cursor)

	d.handleInput(sess, ac, []byte("n"))
	awaitFrame(t, sends, ports.MsgOutput)
	require.Equal(t, 1, ac.overlays.copyMode.Cursor)
	d.handleInput(sess, ac, []byte("N"))
	awaitFrame(t, sends, ports.MsgOutput)
	require.Equal(t, 0, ac.overlays.copyMode.Cursor)

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
	require.Equal(t, 0, ac.overlays.copyMode.Cursor, "typing a query previews the selected first result behind the modal")

	d.handleInput(sess, ac, []byte{0x0e})
	awaitFrame(t, sends, ports.MsgOutput)
	require.NotNil(t, ac.overlays.copySearch)
	require.Equal(t, 1, ac.overlays.copyMode.Cursor, "moving modal selection previews that result without Enter")
}

func TestCopyModeSearchModalCapturesMouseAndClearsOnExit(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	pane := sess.tabs[0].focusedPane()
	pane.scrollback = scopy.NewScrollback(4)
	pane.scrollback.Append(testRow("old alpha"))
	copy(pane.screen.Frame.Row(0), testRow("live alpha"))

	d.enterCopyMode(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("/alpha"))
	awaitFrame(t, sends, ports.MsgOutput)
	require.NotNil(t, ac.overlays.copySearch)
	cursor := ac.overlays.copyMode.Cursor

	d.handleInput(sess, ac, []byte("\x1b[<64;1;1M"))
	require.NotNil(t, ac.overlays.copySearch)
	require.Equal(t, cursor, ac.overlays.copyMode.Cursor)

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
	sess.tabs[0].focusedPane().scrollback = scopy.NewScrollback(4)
	sess.tabs[0].focusedPane().scrollback.Append(testRow("old1    "))
	sess.tabs[0].focusedPane().scrollback.Append(testRow("old2    "))
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

	d.paint(sess, ac, true)
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

	d := newTestDaemon(t, newFactory(t, p), stubClock{})
	tr, sends, releaseConn := newConn(t, mustHello(ports.IntentNew, "work", domain.Size{Cols: 16, Rows: 5}))
	var hg sync.WaitGroup
	hg.Go(func() { d.handleConn(tr) })
	awaitFrame(t, sends, ports.MsgWelcome)
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
		return len(scopy.NewSnapshot(p.scrollback, p.screen.Frame).Rows) >= 12
	}, 2*time.Second, 5*time.Millisecond)

	sess := firstSession(d)
	require.NotNil(t, sess)
	ac := sess.client
	require.NotNil(t, ac)
	d.handleInput(sess, ac, []byte("\x1b "))
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("VIS\r"))
	awaitFrame(t, sends, ports.MsgOutput)
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
	sess.tabs[0].focusedPane().scrollback = scopy.NewScrollback(4)
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
			sess.tabs[0].focusedPane().scrollback = scopy.NewScrollback(4)
			sess.tabs[0].focusedPane().scrollback.Append(testRow("old1    "))
			sess.tabs[0].focusedPane().scrollback.Append(testRow("old2    "))
			sess.tabs[0].focusedPane().screen.Write([]byte("live"))

			d.enterCopyMode(sess, ac)
			awaitFrame(t, sends, ports.MsgOutput)
			for _, input := range tc.input {
				d.handleInput(sess, ac, input)
			}

			require.NotNil(t, ac.overlays.copyMode)
			require.Equal(t, tc.wantCursor, ac.overlays.copyMode.Cursor)
		})
	}
}

func TestCopyModeOversizedYankShowsTooLargeFeedback(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	sess.tabs[0].focusedPane().scrollback = scopy.NewScrollback(1)
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
	sess.tabs[0].focusedPane().scrollback = scopy.NewScrollback(4)
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
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	clk := &signalClock{timers: make(chan *signalTimer, 1)}
	d.clock = clk
	sess.tabs[0].focusedPane().scrollback = scopy.NewScrollback(4)
	sess.tabs[0].focusedPane().screen.Write([]byte("live"))

	d.enterCopyMode(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("\x1b"))
	timer := <-clk.timers
	d.enterCopyMode(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)

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
			sess.tabs[0].focusedPane().scrollback = scopy.NewScrollback(4)
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

func TestCopyModeEnterExitConcurrentWithPaintRace(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	sess.tabs[0].focusedPane().scrollback = scopy.NewScrollback(4)
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
			d.paint(sess, ac, true)
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

package daemon

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	vt "github.com/bnema/vev-vt"
	renderer "github.com/bnema/vev-vt/ansi"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/domain/layout"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/internal/usecase/ui"
)

// --- test doubles -----------------------------------------------------------

func TestBoundedSendOutputErrTransportReturnsTransportUsedBySend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newTestDaemon(t, nil, newStubClock(t))
		ac := &attachedClient{output: newOutputStateStream()}
		ac.initOverlays()
		stopOverlayTimersAtCleanup(t, ac)
		replacement := newQuietConnection(t)
		sendErr := errors.New("send failed")
		failed, _ := newTrackedConnection(t, withSendFunc(func(wire.Envelope, *connLog) error {
			ac.replaceTransport(replacement)
			return sendErr
		}))
		ac.replaceTransport(failed)

		used, err := d.boundedSendOutputErrTransport(ac, []byte("copy"))

		require.ErrorIs(t, err, sendErr)
		require.Same(t, failed, used)
		require.Same(t, replacement, ac.transport())
	})
}

func TestOwnedSynchronousSendReturnsCapturedTransportAcrossReplacement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newTestDaemon(t, nil, newStubClock(t))
		replacement, replacementLog := newTrackedConnection(t)
		sendErr := errors.New("owned send failed")
		ac := &attachedClient{output: newOutputStateStream()}
		failed, failedFrames := newCapabilityConnection(t, ownedSynchronousCapabilities(), func(c *portsmocks.MockServerConnection, frames *typedFrames) {
			c.EXPECT().SendOutputSynchronous(mock.Anything).RunAndReturn(func(output protocol.Output) error {
				envelope, err := testServerEnvelope(output)
				if err != nil {
					return err
				}
				frames.sends <- envelope
				ac.replaceTransport(replacement)
				return sendErr
			}).Maybe()
		})
		ac.replaceTransport(failed)
		sess := &session{sessionCore: sessionCore{name: "work", attachments: map[*attachedClient]struct{}{ac: {}}}}
		ac.setSession(sess)

		used, err := d.boundedSendOutputErrTransport(ac, []byte("copy"))

		require.ErrorIs(t, err, sendErr)
		require.Same(t, failed, used)
		require.Same(t, replacement, ac.transport())
		out := unmarshalTestOutput(t, (<-failedFrames.sends).Payload)
		require.Equal(t, []byte("copy"), out.Data)
		d.detachOnSendError(sess, ac, used)
		require.True(t, slices.Contains(sess.snapshotAttachments(), ac))
		require.False(t, replacementLog.Closed())
	})
}

// stubClock returns timers whose channel never fires, so a scheduler under it
// blocks in its debounce loop until the session context is cancelled. Used by

func TestCopySearchModalGeometry(t *testing.T) {
	base := domain.Size{Cols: 100, Rows: 40}

	require.Equal(t, domain.Rect{X: 0, Y: 28, Width: 100, Height: 11}, copySearchModal.Resolve(base).Bounds)
	require.Equal(t, domain.AnchorBottom, copySearchModal.Anchor)
	require.Equal(t, ui.Margins{Bottom: 1}, copySearchModal.Margins)
}

func TestCopyModeDocumentCarriesPaneRowIDs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, _ := newBlockingPTY(t)
		d, sess, ac, sends := newManualSessionWithPTYs(t, p)
		stopOverlayTimersAtCleanup(t, ac)
		pane := testAttachmentTab(sess).focusedPane()
		appendHistoryRow(t, pane.history, testRow("history"))
		pane.screen.Write([]byte("live"))
		historyID := pane.history.View().RowID(0)
		liveID := pane.screen.RowID(0)
		require.NotEqual(t, historyID, liveID)

		d.enterCopyMode(sess, ac)
		requireFrame(t, sends, "Output")
		doc := ac.overlays.copyDocument

		require.Equal(t, 0, doc.FindRowID(historyID))
		require.Equal(t, pane.history.Len(), doc.FindRowID(liveID))
		require.Equal(t, liveID, doc.RowID(pane.history.Len()))
	})
}

func TestComposeCopyClientFrameConcurrentPaneOutput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, release := newBlockingPTY(t)
		defer release()
		_, sess, _, _ := newManualSessionWithPTYs(t, p)
		tb := testAttachmentTab(sess)
		pane := tb.focusedPane()
		pane.mu.Lock()
		snap := scopy.NewSnapshot(pane.history, pane.screen, pane.screen.LineBounds(), nil)
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
	})
}

func TestCopyModeFrameIncludesTopAndBottomChrome(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, release := newBlockingPTY(t)
		_, sess, _, _ := newManualSessionWithPTYs(t, p)
		defer release()
		sess.name = "work"
		tb := testAttachmentTab(sess)
		tb.focusedPane().screen = vt.NewScreen(12, 3)
		tb.focusedPane().screen.Write([]byte("live"))
		snap := scopy.NewSnapshot(tb.focusedPane().history, tb.focusedPane().screen, tb.focusedPane().screen.LineBounds(), nil)
		mode := scopy.NewMode(scopy.NewDocument(snap, domain.DefaultWordSeparators))

		bars := barState{status: sess.statusSegments(true)}
		content := domain.Rect{Width: tb.size.Cols, Height: tb.size.Rows}
		base := composeFrame(capturedRenderState{
			reset:  true,
			layout: capturedTabLayout{area: content, focus: tb.focusedPane().id, valid: true},
			panes: []capturedPaneRenderState{{
				id: tb.focusedPane().id, frame: captureTestFrame(tb.focusedPane().screen), placement: layout.Placement{ID: tb.focusedPane().id, Content: content}, focused: true, damage: []renderer.Damage{renderer.FullRedraw()},
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
	})
}

func TestCopyModePaletteCommandEntersAndDoesNotForward(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writes := make(chan []byte, 1)
		p, _ := newBlockingPTYWithWrites(t, writes)
		d, sess, ac, sends := newManualSessionWithPTYs(t, p)
		stopOverlayTimersAtCleanup(t, ac)
		installTestHistory(sess.tabs[0].focusedPane(), vt.HistoryConfig{MaxRows: 4})
		sess.tabs[0].focusedPane().screen.Write([]byte("live"))

		d.handleInput(sess, ac, []byte("\x1b "))
		requireFrame(t, sends, "Output")
		d.handleInput(sess, ac, []byte("VIS\r"))

		if ac.overlays.copyMode == nil {
			t.Fatal("scrollback mode not entered")
		}
		select {
		case got := <-writes:
			t.Fatalf("scrollback binding forwarded to PTY: %q", got)
		default:
		}
		out := requireFrame(t, sends, "Output")
		msg := unmarshalTestOutput(t, out.Payload)
		if got := string(msg.Data); !strings.Contains(got, "[SCROLL]") || strings.Contains(got, "[SELECT]") || strings.Contains(got, "[COPY]") {
			t.Fatalf("passive scrollback paint = %q, want [SCROLL] without [SELECT]/[COPY]", got)
		}

		d.handleInput(sess, ac, []byte(" "))
		out = requireFrame(t, sends, "Output")
		msg = unmarshalTestOutput(t, out.Payload)
		if got := string(msg.Data); !strings.Contains(got, "[SELECT]") || strings.Contains(got, "[SCROLL]") {
			t.Fatalf("visual selection paint = %q, want [SELECT] without [SCROLL]", got)
		}
	})
}

func TestCopyModeSearchModalRoutesBatchedInputAfterSlash(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writes := make(chan []byte, 1)
		p, _ := newBlockingPTYWithWrites(t, writes)
		d, sess, ac, sends := newManualSessionWithPTYs(t, p)
		stopOverlayTimersAtCleanup(t, ac)
		pane := sess.tabs[0].focusedPane()
		writeTestRow(pane.screen, 0, "alpha")
		writeTestRow(pane.screen, 1, "beta alpha")

		d.enterCopyMode(sess, ac)
		requireFrame(t, sends, "Output")
		d.handleInput(sess, ac, []byte("/alpha\r"))
		requireFrame(t, sends, "Output")

		require.Nil(t, ac.overlays.copySearch)
		require.NotNil(t, ac.overlays.copyMode)
		require.Equal(t, 0, ac.overlays.copyMode.Cursor().Row)
		select {
		case got := <-writes:
			t.Fatalf("batched visual search input forwarded to PTY: %q", got)
		default:
		}
	})
}

func TestCopyModeSearchModalJumpsAndKeepsNavigation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writes := make(chan []byte, 1)
		p, _ := newBlockingPTYWithWrites(t, writes)
		d, sess, ac, sends := newManualSessionWithPTYs(t, p)
		stopOverlayTimersAtCleanup(t, ac)
		pane := sess.tabs[0].focusedPane()
		writeTestRow(pane.screen, 0, "alpha")
		writeTestRow(pane.screen, 1, "beta alpha")
		writeTestRow(pane.screen, 2, "gamma")

		d.enterCopyMode(sess, ac)
		requireFrame(t, sends, "Output")
		d.handleInput(sess, ac, []byte("/"))
		out := requireFrame(t, sends, "Output")
		msg := unmarshalTestOutput(t, out.Payload)
		require.Contains(t, string(msg.Data), "Search")
		require.NotNil(t, ac.overlays.copySearch)

		d.handleInput(sess, ac, []byte("alpha"))
		out = requireFrame(t, sends, "Output")
		msg = unmarshalTestOutput(t, out.Payload)
		require.Contains(t, string(msg.Data), "/alpha")
		require.Contains(t, string(msg.Data), "1:1  alpha")
		require.Contains(t, string(msg.Data), "2:6  beta alpha")

		d.handleInput(sess, ac, []byte("\r"))
		requireFrame(t, sends, "Output")
		require.Nil(t, ac.overlays.copySearch)
		require.Equal(t, 0, ac.overlays.copyMode.Cursor().Row)

		d.handleInput(sess, ac, []byte("n"))
		requireFrame(t, sends, "Output")
		require.Equal(t, 1, ac.overlays.copyMode.Cursor().Row)
		d.handleInput(sess, ac, []byte("N"))
		requireFrame(t, sends, "Output")
		require.Equal(t, 0, ac.overlays.copyMode.Cursor().Row)

		select {
		case got := <-writes:
			t.Fatalf("visual search input forwarded to PTY: %q", got)
		default:
		}
	})
}

func TestCopyModeSearchModalSelectionPreviewsBehindModal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, _ := newBlockingPTY(t)
		d, sess, ac, sends := newManualSessionWithPTYs(t, p)
		stopOverlayTimersAtCleanup(t, ac)
		pane := sess.tabs[0].focusedPane()
		writeTestRow(pane.screen, 0, "alpha")
		writeTestRow(pane.screen, 1, "beta alpha")
		writeTestRow(pane.screen, 2, "gamma")

		d.enterCopyMode(sess, ac)
		requireFrame(t, sends, "Output")

		d.handleInput(sess, ac, []byte("/"))
		requireFrame(t, sends, "Output")
		d.handleInput(sess, ac, []byte("alpha"))
		requireFrame(t, sends, "Output")
		require.NotNil(t, ac.overlays.copySearch)
		require.Equal(t, 0, ac.overlays.copyMode.Cursor().Row, "typing a query previews the selected first result behind the modal")

		d.handleInput(sess, ac, []byte{0x0e})
		requireFrame(t, sends, "Output")
		require.NotNil(t, ac.overlays.copySearch)
		require.Equal(t, 1, ac.overlays.copyMode.Cursor().Row, "moving modal selection previews that result without Enter")
	})
}

func TestCopyModeSearchModalCapturesMouseAndClearsOnExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, _ := newBlockingPTY(t)
		d, sess, ac, sends := newManualSessionWithPTYs(t, p)
		stopOverlayTimersAtCleanup(t, ac)
		pane := sess.tabs[0].focusedPane()
		installTestHistory(pane, vt.HistoryConfig{MaxRows: 4})
		appendHistoryRow(t, pane.history, testRow("old alpha"))
		writeTestRow(pane.screen, 0, "live alpha")

		d.enterCopyMode(sess, ac)
		requireFrame(t, sends, "Output")
		d.handleInput(sess, ac, []byte("/alpha"))
		requireFrame(t, sends, "Output")
		require.NotNil(t, ac.overlays.copySearch)
		cursor := ac.overlays.copyMode.Cursor().Row

		d.handleInput(sess, ac, []byte("\x1b[<64;1;1M"))
		require.NotNil(t, ac.overlays.copySearch)
		require.Equal(t, cursor, ac.overlays.copyMode.Cursor().Row)

		d.handleInput(sess, ac, []byte("\x1b"))
		requireFrame(t, sends, "Output")
		require.Nil(t, ac.overlays.copySearch)
		require.NotNil(t, ac.overlays.copyMode)
		d.handleInput(sess, ac, []byte("q"))
		requireFrame(t, sends, "Output")
		require.Nil(t, ac.overlays.copySearch)
		require.Nil(t, ac.overlays.copyMode)
	})
}

func TestCopyModeInputNotForwardedAndOSC52Copy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writes := make(chan []byte, 1)
		p, _ := newBlockingPTYWithWrites(t, writes)
		d, sess, ac, sends := newManualSessionWithPTYs(t, p)
		stopOverlayTimersAtCleanup(t, ac)
		installTestHistory(sess.tabs[0].focusedPane(), vt.HistoryConfig{MaxRows: 4})
		appendHistoryRow(t, sess.tabs[0].focusedPane().history, testRow("old1    "))
		appendHistoryRow(t, sess.tabs[0].focusedPane().history, testRow("old2    "))
		sess.tabs[0].focusedPane().screen.Write([]byte("live"))

		d.enterCopyMode(sess, ac)
		requireFrame(t, sends, "Output")
		d.handleInput(sess, ac, []byte{'g', ' ', 'j', 'y'})

		select {
		case got := <-writes:
			t.Fatalf("copy-mode navigation forwarded to PTY: %q", got)
		default:
		}
		out := requireFrame(t, sends, "Output")
		msg := unmarshalTestOutput(t, out.Payload)
		if got, want := string(msg.Data), "\x1b]52;c;b2xkMQpvbGQy\x07"; got != want {
			t.Fatalf("OSC52 = %q, want %q", got, want)
		}
		if ac.overlays.copyMode != nil {
			t.Fatal("copy mode still active after yank")
		}
		live := requireFrame(t, sends, "Output")
		liveMsg := unmarshalTestOutput(t, live.Payload)
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
		followup := requireFrame(t, sends, "Output")
		followupMsg := unmarshalTestOutput(t, followup.Payload)
		if strings.Contains(string(followupMsg.Data), "copied 9 chars to clipboard") {
			t.Fatalf("copy feedback persisted after next repaint: %q", string(followupMsg.Data))
		}
	})
}

func TestScrollbackEvictionFeedsCopyModeYank(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
		tr, sends, releaseConn := newConn(t, mustHello(protocol.IntentNew, "work", domain.Size{Cols: 16, Rows: 5}))
		advanceRender := func() {
			timer := requireReceived(t, clk.timers, "coordinator did not arm a controllable render timer")
			timer.ch <- time.Time{}
		}
		var hg sync.WaitGroup
		hg.Go(func() { d.handleConn(tr) })
		requireFrame(t, sends, "Welcome")
		advanceRender() // initial coordinator invalidation
		initial := unmarshalTestOutput(t, requireFrame(t, sends, "Output").Payload)
		require.NotNil(t, initial.Context)
		require.NotEmpty(t, initial.Context.FocusedPaneID, "a small viewport must retain its input target even when layout cannot place its cells")

		for i := range 12 {
			reads <- fmt.Appendf(nil, "line-%02d\r\n", i)
		}
		synctest.Wait()
		{
			sess := firstSession(d)
			require.NotNil(t, sess)
			win := testAttachmentTab(sess)
			require.NotNil(t, win)
			win.mu.Lock()
			p := win.focusedPane()
			win.mu.Unlock()
			require.NotNil(t, p)
			p.mu.Lock()
			held := scopy.NewSnapshot(p.history, p.screen, p.screen.LineBounds(), nil).Len()
			p.mu.Unlock()
			require.GreaterOrEqual(t, held, 12, "pane reader did not ingest every line at quiescence")
		}

		sess := firstSession(d)
		require.NotNil(t, sess)
		ac := sess.snapshotAttachments()[0]
		require.NotNil(t, ac)
		d.handleInput(sess, ac, []byte("\x1b "))
		requireCoordinatorOutput(t, sends, clk.timers, "while advancing render clock")
		d.handleInput(sess, ac, []byte("VIS\r"))
		requireCoordinatorOutput(t, sends, clk.timers, "while advancing render clock")
		d.handleInput(sess, ac, []byte{'g', ' ', 'G', 'y'})

		var payload string
		for !strings.HasPrefix(payload, "\x1b]52;c;") {
			out := requireFrame(t, sends, "Output")
			payload = string(unmarshalTestOutput(t, out.Payload).Data)
		}
		require.True(t, strings.HasPrefix(payload, "\x1b]52;c;"), "OSC52 payload = %q", payload)
		require.True(t, strings.HasSuffix(payload, "\a"), "OSC52 payload = %q", payload)
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimSuffix(strings.TrimPrefix(payload, "\x1b]52;c;"), "\a"))
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(string(decoded), "line-00"), "decoded yank = %q", string(decoded))

		releaseConn()
		closeOnce.Do(func() { close(readDone) })
		hg.Wait()
		require.NoError(t, d.killSession(sess, protocol.ReasonSessionKilled, true))
		d.sessWg.Wait()
	})
}

func TestCopyModeEscapeRestoresLiveFullRepaint(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, _ := newBlockingPTY(t)
		d, sess, ac, sends := newManualSessionWithPTYs(t, p)
		stopOverlayTimersAtCleanup(t, ac)
		installTestHistory(sess.tabs[0].focusedPane(), vt.HistoryConfig{MaxRows: 4})
		sess.tabs[0].focusedPane().screen.Write([]byte("live"))

		d.enterCopyMode(sess, ac)
		requireFrame(t, sends, "Output")
		d.handleInput(sess, ac, []byte("q"))

		if ac.overlays.copyMode != nil {
			t.Fatal("copy mode still active after q")
		}
		out := requireFrame(t, sends, "Output")
		msg := unmarshalTestOutput(t, out.Payload)
		if strings.Contains(string(msg.Data), "[COPY]") || !strings.Contains(string(msg.Data), "live") {
			t.Fatalf("exit repaint = %q, want live full repaint without copy status", string(msg.Data))
		}
	})
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
			synctest.Test(t, func(t *testing.T) {
				p, _ := newBlockingPTY(t)
				d, sess, ac, sends := newManualSessionWithPTYs(t, p)
				stopOverlayTimersAtCleanup(t, ac)
				installTestHistory(sess.tabs[0].focusedPane(), vt.HistoryConfig{MaxRows: 4})
				appendHistoryRow(t, sess.tabs[0].focusedPane().history, testRow("old1    "))
				appendHistoryRow(t, sess.tabs[0].focusedPane().history, testRow("old2    "))
				sess.tabs[0].focusedPane().screen.Write([]byte("live"))

				d.enterCopyMode(sess, ac)
				requireFrame(t, sends, "Output")
				for _, input := range tc.input {
					d.handleInput(sess, ac, input)
				}

				require.NotNil(t, ac.overlays.copyMode)
				require.Equal(t, tc.wantCursor, ac.overlays.copyMode.Cursor().Row)
			})
		})
	}
}

func TestCopyModeOversizedYankShowsTooLargeFeedback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, _ := newBlockingPTY(t)
		d, sess, ac, sends := newManualSessionWithPTYs(t, p)
		stopOverlayTimersAtCleanup(t, ac)
		installTestHistory(sess.tabs[0].focusedPane(), vt.HistoryConfig{MaxRows: 1})
		longLine := strings.Repeat("x", scopy.OSC52MaxPayloadBytes+1)
		sess.tabs[0].focusedPane().screen.Resize(len(longLine), 1)
		writeTestRow(sess.tabs[0].focusedPane().screen, 0, longLine)

		d.enterCopyMode(sess, ac)
		requireFrame(t, sends, "Output")
		d.handleInput(sess, ac, []byte{' ', 'y'})

		out := requireFrame(t, sends, "Output")
		msg := unmarshalTestOutput(t, out.Payload)
		if strings.Contains(string(msg.Data), "\x1b]52;") {
			t.Fatalf("oversized yank emitted OSC52: %q", string(msg.Data))
		}
		if !strings.Contains(string(msg.Data), "selection too large to copy") {
			t.Fatalf("oversized yank repaint = %q, want too-large feedback", string(msg.Data))
		}
	})
}

func TestCopyModeLoneEscapeExitsAfterDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, _ := newBlockingPTY(t)
		d, sess, ac, sends := newManualSessionWithPTYs(t, p)
		stopOverlayTimersAtCleanup(t, ac)
		clk, clkTimers := newSignalClock(t, 1)
		d.clock = clk
		installTestHistory(sess.tabs[0].focusedPane(), vt.HistoryConfig{MaxRows: 4})
		sess.tabs[0].focusedPane().screen.Write([]byte("live"))

		d.enterCopyMode(sess, ac)
		requireFrame(t, sends, "Output")
		d.handleInput(sess, ac, []byte("\x1b"))
		timer := requireReceived(t, clkTimers, "copy escape did not arm its timer")
		ac.overlays.copyMu.Lock()
		require.NotNil(t, ac.overlays.copyMode)
		ac.overlays.copyMu.Unlock()
		timer.fire()
		synctest.Wait()
		ac.overlays.copyMu.Lock()
		require.Nil(t, ac.overlays.copyMode, "the pending escape did not close copy mode")
		ac.overlays.copyMu.Unlock()
	})
}

func TestCopyModePendingEscapeDoesNotCloseNewMode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, release := newBlockingPTY(t)
		defer release()
		d, sess, ac, sends := newManualSessionWithPTYs(t, p)
		stopOverlayTimersAtCleanup(t, ac)
		clk, clkTimers := newSignalClock(t, 1)
		d.clock = clk
		installTestHistory(sess.tabs[0].focusedPane(), vt.HistoryConfig{MaxRows: 4})
		sess.tabs[0].focusedPane().screen.Write([]byte("live"))

		d.enterCopyMode(sess, ac)
		requireFrame(t, sends, "Output")
		d.handleInput(sess, ac, []byte("\x1b"))
		timer := requireReceived(t, clkTimers, "copy escape did not arm its timer")
		d.enterCopyMode(sess, ac)
		requireFrame(t, sends, "Output")

		ac.overlays.copyMu.Lock()
		require.Nil(t, ac.overlays.copyESC.timer)
		require.Nil(t, ac.overlays.copyESC.done)
		require.Empty(t, ac.overlays.copyPending)
		ac.overlays.copyMu.Unlock()
		d.handleInput(sess, ac, []byte("x"))
		require.True(t, ac.overlays.copyActive(), "pending input from the replaced mode affected the new mode")

		timer.fire()
		synctest.Wait()
		require.True(t, ac.overlays.copyActive(), "a stale pending escape closed the new copy mode")
		ac.overlays.copyMu.Lock()
		require.Nil(t, ac.overlays.copyESC.timer)
		require.Nil(t, ac.overlays.copyESC.done)
		require.Empty(t, ac.overlays.copyPending)
		ac.overlays.copyMu.Unlock()
	})
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
			synctest.Test(t, func(t *testing.T) {
				p, _ := newBlockingPTY(t)
				d, sess, ac, sends := newManualSessionWithPTYs(t, p)
				stopOverlayTimersAtCleanup(t, ac)
				installTestHistory(sess.tabs[0].focusedPane(), vt.HistoryConfig{MaxRows: 4})
				sess.tabs[0].focusedPane().screen.Write([]byte("live"))

				d.enterCopyMode(sess, ac)
				requireFrame(t, sends, "Output")
				d.handleInput(sess, ac, tc.input)

				if ac.overlays.copyMode != nil {
					t.Fatal("copy mode still active after empty yank")
				}
				out := requireFrame(t, sends, "Output")
				msg := unmarshalTestOutput(t, out.Payload)
				if strings.Contains(string(msg.Data), "\x1b]52;") {
					t.Fatalf("empty yank emitted OSC52 clipboard clear: %q", string(msg.Data))
				}
				if strings.Contains(string(msg.Data), "[COPY]") || !strings.Contains(string(msg.Data), "live") {
					t.Fatalf("empty yank repaint = %q, want live full repaint", string(msg.Data))
				}
			})
		})
	}
}

func TestHandleCopyInputUsesImmutableSnapshotWithoutPaneLock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, release := newBlockingPTY(t)
		defer release()
		d, sess, ac, sends := newManualSessionWithPTYs(t, p)
		stopOverlayTimersAtCleanup(t, ac)
		pane := testAttachmentTab(sess).focusedPane()
		pane.screen.Write([]byte("live"))
		d.enterCopyMode(sess, ac)
		requireFrame(t, sends, "Output")

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
	})
}

func TestCopyModeEnterExitConcurrentWithPaintRace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, _ := newBlockingPTY(t)
		d, sess, ac, sends := newManualSessionWithPTYs(t, p)
		stopOverlayTimersAtCleanup(t, ac)
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
	})
}

func TestCursorTailHidesInCopyMode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, _ := newBlockingPTY(t)
		d, sess, ac, sends := newManualSessionWithPTYs(t, p)
		stopOverlayTimersAtCleanup(t, ac)
		sess.tabs[0].focusedPane().screen.Write([]byte("live"))

		d.enterCopyMode(sess, ac)
		data := mustOutputData(t, sends)
		require.Contains(t, string(data), "\x1b[?25l")
	})
}

func installFloatingCopyFixture(t *testing.T, sess *session, size domain.Size) *pane {
	t.Helper()
	floatingPTY, releaseFloating := newBlockingPTY(t)
	t.Cleanup(releaseFloating)
	fp := newPane("floating", floatingPTY, size)
	installTestFloating(testAttachmentTab(sess), fp, true)
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
			synctest.Test(t, func(t *testing.T) {
				normalWrites := make(chan []byte, 1)
				normal, releaseNormal := newBlockingPTYWithWrites(t, normalWrites)
				defer releaseNormal()
				d, sess, ac, sends := newManualSessionWithPTYs(t, normal)
				stopOverlayTimersAtCleanup(t, ac)
				tb := testAttachmentTab(sess)
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
				requireFrame(t, sends, "Output")
				ac.overlays.copyMu.Lock()
				captured := ac.overlays.copyPane
				ac.overlays.copyMu.Unlock()
				require.Same(t, want, captured)

				tb.mu.Lock()
				focusPlacementLocked(tb, "pane-2")
				tb.mu.Unlock()

				d.handleInput(sess, ac, []byte{'g', ' ', 'j', 'y'})
				out := requireFrame(t, sends, "Output")
				msg := unmarshalTestOutput(t, out.Payload)
				wantOSC := "\x1b]52;c;" + base64.StdEncoding.EncodeToString([]byte(tc.wantText)) + "\x07"
				require.Equal(t, wantOSC, string(msg.Data))
				require.Nil(t, ac.overlays.copyMode)
				select {
				case got := <-normalWrites:
					t.Fatalf("copy-mode input forwarded to normal PTY: %q", got)
				default:
				}
			})
		})
	}
}

func TestFloatingCopyModeWheelUsesCapturedSnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		normal, releaseNormal := newBlockingPTY(t)
		defer releaseNormal()
		d, sess, ac, sends := newManualSessionWithPTYs(t, normal)
		stopOverlayTimersAtCleanup(t, ac)
		fp := installFloatingCopyFixture(t, sess, domain.Size{Cols: 20, Rows: 3})
		for i := range 30 {
			appendHistoryRow(t, fp.history, testRow(fmt.Sprintf("old-%02d", i)))
		}
		fp.screen.Write([]byte("live"))
		fp.mu.Lock()
		total := scopy.NewSnapshot(fp.history, fp.screen, fp.screen.LineBounds(), nil).Len()
		fp.mu.Unlock()

		d.enterCopyMode(sess, ac)
		requireFrame(t, sends, "Output")
		require.Equal(t, total-1, ac.overlays.copyMode.Cursor().Row)

		d.copyWheel(sess, ac, -3)
		requireFrame(t, sends, "Output")
		require.Equal(t, total-4, ac.overlays.copyMode.Cursor().Row)

		d.copyWheel(sess, ac, 3)
		requireFrame(t, sends, "Output")
		require.Nil(t, ac.overlays.copyMode, "wheel down reaching the captured bottom exits copy mode")
	})
}

func TestFloatingCopyModeMouseSelectsFloatingRows(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		normal, releaseNormal := newBlockingPTY(t)
		defer releaseNormal()
		d, sess, ac, sends := newManualSessionWithPTYs(t, normal)
		stopOverlayTimersAtCleanup(t, ac)
		fp := installFloatingCopyFixture(t, sess, domain.Size{Cols: 20, Rows: 3})
		for i := range 5 {
			appendHistoryRow(t, fp.history, testRow(fmt.Sprintf("old-%d", i)))
		}
		fp.screen.Write([]byte("live"))

		d.enterCopyMode(sess, ac)
		requireFrame(t, sends, "Output")
		ac.overlays.copyMu.Lock()
		viewportTop := ac.overlays.copyMode.ViewportTop
		ac.overlays.copyMu.Unlock()
		inner := calculateContentFloatingGeometry(domain.Size{Cols: 80, Rows: 23}, d.currentFloatingConfig()).Inner

		// Copy body row k renders at wire row inner.Y+k+2 (top bar + content
		// offset); clicking a row must select exactly that row.
		press := fmt.Sprintf("\x1b[<0;%d;%dM", inner.X+3, inner.Y+3)
		d.handleInput(sess, ac, []byte(press))
		requireFrame(t, sends, "Output")
		require.Equal(t, viewportTop+1, ac.overlays.copyMode.Cursor().Row)

		motion := fmt.Sprintf("\x1b[<32;%d;%dM", inner.X+3, inner.Y+4)
		d.handleInput(sess, ac, []byte(motion))
		requireFrame(t, sends, "Output")
		selection := ac.overlays.copyMode.Selection()
		require.True(t, selection.Enabled)
		require.Equal(t, viewportTop+1, selection.Anchor.Row)
		require.Equal(t, viewportTop+2, selection.Active.Row)
	})
}

func TestFloatingExitClearsCopyModeBeforeRepaint(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		normal, releaseNormal := newBlockingPTY(t)
		defer releaseNormal()
		d, sess, ac, sends := newManualSessionWithPTYs(t, normal)
		stopOverlayTimersAtCleanup(t, ac)
		tb := testAttachmentTab(sess)
		fp := installFloatingCopyFixture(t, sess, domain.Size{Cols: 20, Rows: 3})
		fp.screen.Write([]byte("flt-live"))

		d.enterCopyMode(sess, ac)
		requireFrame(t, sends, "Output")
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
	})
}

func TestMouseDragCopyEntryCapturesSourceForYank(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pty, _ := newBlockingPTY(t)
		d, sess, ac, sends := newManualSessionWithPTYs(t, pty)
		stopOverlayTimersAtCleanup(t, ac)
		pane := sess.tabs[0].focusedPane()
		writeTestRow(pane.screen, 0, "alpha")
		writeTestRow(pane.screen, 1, "bravo")

		d.handleInput(sess, ac, []byte("\x1b[<0;1;2M\x1b[<32;1;3M"))
		mustOutputData(t, sends)
		require.NotNil(t, ac.overlays.copyMode)
		ac.overlays.copyMu.Lock()
		captured := ac.overlays.copyPane
		ac.overlays.copyMu.Unlock()
		require.Same(t, pane, captured, "drag entry must capture the copy source pane")

		d.handleInput(sess, ac, []byte("y"))
		out := requireFrame(t, sends, "Output")
		msg := unmarshalTestOutput(t, out.Payload)
		want := "\x1b]52;c;" + base64.StdEncoding.EncodeToString([]byte("alpha\nb")) + "\x07"
		require.Equal(t, want, string(msg.Data))
	})
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
			mode := scopy.NewMode(scopy.NewDocument(scopy.NewSnapshot(pane.history, pane.screen, pane.screen.LineBounds(), nil), domain.DefaultWordSeparators))
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

// requireCoordinatorOutput is the bubble form of awaitCoordinatorOutput: it
// drives coordinator timers and returns the next Output frame, ignoring
// RoutePosition frames.
func requireCoordinatorOutput(t *testing.T, sends <-chan wire.Envelope, timers <-chan *coordinatorMockTimer, frameContext string) wire.Envelope {
	t.Helper()
	for {
		synctest.Wait()
		select {
		case frame := <-sends:
			name := envelopeMessageName(t, frame.Payload)
			if name == "RoutePosition" {
				continue
			}
			if name != "Output" {
				t.Fatalf("unexpected frame type %s %s", name, frameContext)
			}
			return frame
		default:
		}
		select {
		case timer := <-timers:
			timer.ch <- time.Time{}
		default:
			t.Fatal("controllable timers did not produce an output frame")
		}
	}
}

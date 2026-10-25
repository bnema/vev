package daemon

import (
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	vt "github.com/bnema/vev-vt"
	renderer "github.com/bnema/vev-vt/ansi"
	"github.com/bnema/vev/internal/domain"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

func TestDatagramAttachPipelinesRendererBeforeAck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, releasePTY := newBlockingPTY(t)
		defer releasePTY()
		d, sess, _, _ := newManualSessionWithPTYs(t, p)
		tr, trSends, trRecv := newChannelConnection(t)
		routed, ac, err := d.route(protocol.Hello{
			Version:           protocol.Version,
			Intent:            protocol.IntentAttach,
			Name:              sess.name,
			Size:              domain.Size{Cols: 80, Rows: 24},
			MaxOutputInFlight: 8,
		}, tr)
		require.NoError(t, err)
		require.Same(t, sess, routed)

		// route publishes membership only; mark the attachment ready before the
		// direct pipeline paints so coordinator admission is deterministic.
		rc := sess.renderCoordinator()
		require.NotNil(t, rc)
		require.True(t, rc.markAttachmentReady(rc.attachmentLease(ac)))

		sess.tabs[0].focusedPane().screen.Write([]byte("A"))
		d.paint(sess, ac, false, nil)
		first := requireFrame(t, trSends, "Output")
		out := unmarshalTestOutput(t, first.Payload)
		require.Equal(t, uint64(1), out.New)

		sess.tabs[0].focusedPane().screen.Write([]byte("\rB"))
		d.paint(sess, ac, false, nil)
		updated := requireFrame(t, trSends, "Output")
		out = unmarshalTestOutput(t, updated.Payload)
		require.Equal(t, uint64(2), out.New)
		// Both states were emitted before the Ack; the ordered dependency chain
		// must therefore remain valid without waiting for renderer acknowledgement.
		trRecv <- mustClientEnvelope(protocol.Ack{Epoch: out.Epoch, State: out.New})
		require.NoError(t, tr.Close())
		d.runConnLoop(ac)

		// The production Ack path retires retained states without moving the
		// renderer baseline backward.
		d.paint(sess, ac, false, nil)
		require.Empty(t, trSends)
		// runConnLoop parked the attachment; its expiry watcher waits on the
		// stub timer forever, so retire the parked credential to end the bubble.
		d.shutdownAll(protocol.ReasonServerShutdown)
	})
}

func TestPaintExplicitlyUsesAsyncTransportCapability(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, releasePTY := newBlockingPTY(t)
		defer releasePTY()
		d, sess, ac, _ := newManualSessionWithPTYs(t, p)
		tr, frames := newCapabilityConnection(t, asyncCapabilities())
		ac.replaceTransport(tr)
		sess.tabs[0].focusedPane().screen.Write([]byte("A"))

		d.paint(sess, ac, false, nil)

		requireFrame(t, frames.async, "Output")
		select {
		case frame := <-frames.sends:
			t.Fatalf("paint used synchronous Send: %+v", envelopeMessageName(t, frame.Payload))
		default:
		}
	})
}

func TestDatagramMultipleUnackedScrollPaintsMatchLatestFrame(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, releasePTY := newBlockingPTY(t)
		defer releasePTY()
		d, sess, _, _ := newManualSessionWithPTYs(t, p)
		tr, trSends, _ := newChannelConnection(t)
		_, ac, err := d.route(protocol.Hello{
			Version:           protocol.Version,
			Intent:            protocol.IntentAttach,
			Name:              sess.name,
			Size:              domain.Size{Cols: 80, Rows: 24},
			MaxOutputInFlight: 8,
		}, tr)
		require.NoError(t, err)

		pane := sess.tabs[0].focusedPane()
		initial := renderer.NewFrame(pane.screen.Columns(), pane.screen.Rows())
		for y := range initial.Height {
			cell := renderer.Cell{Rune: rune('A' + y), Style: renderer.DefaultStyle()}
			initial.FillRow(y, 0, initial.Width, cell)
		}
		writeTestFrame(t, pane.screen, initial)
		pane.screen.ClearDamage()
		client := vt.NewScreen(80, 25)
		d.paint(sess, ac, true, nil)
		first := mustApplyOutput(t, client, requireFrame(t, trSends, "Output"))
		ac.ackOutputState(first.Epoch, first.New)

		// Preserve the frame after one scroll while inducing a real VT scroll
		// damage event before each production paint. On main, both unacknowledged
		// paints are generated from the ACKed frame and apply the scroll twice to
		// the client. The ordered stream instead renders the second paint from the
		// preceding emitted frame and overwrites the incompatible repeated damage.
		desired := captureTestFrame(pane.screen)
		desired.ScrollUp(0, desired.Height-1, 1)
		for x := range desired.Width {
			desired.Set(x, desired.Height-1, renderer.Cell{Rune: 'z', Style: renderer.DefaultStyle()})
		}
		for range 2 {
			writeTestFrame(t, pane.screen, desired)
			pane.screen.Row = pane.screen.Rows() - 1
			pane.screen.Col = 0
			pane.screen.Write([]byte("\nq"))
			writeTestFrame(t, pane.screen, desired)
			d.paint(sess, ac, false, nil)
			mustApplyOutput(t, client, requireFrame(t, trSends, "Output"))
		}

		require.Equal(t, 'B', client.Cell(0, 1).Rune,
			"client-visible content must equal the latest daemon-composed frame after pipelined scroll paints")
	})
}

func fillOutputStateRows(frame renderer.Frame, rows []string) {
	for y, row := range rows {
		for x, r := range row {
			frame.Set(x, y, renderer.Cell{Rune: r, Style: renderer.DefaultStyle()})
		}
	}
}

func outputStateRow(row []renderer.Cell) string {
	runes := make([]rune, len(row))
	for i, cell := range row {
		runes[i] = cell.Rune
	}
	return string(runes)
}

func mustApplyOutput(t *testing.T, screen *vt.Screen, frame wire.Envelope) protocol.Output {
	t.Helper()
	out := unmarshalTestOutput(t, frame.Payload)
	screen.Write(out.Data)
	return out
}

func TestLocalAttachStillAdvancesRendererOnSend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, releasePTY := newBlockingPTY(t)
		defer releasePTY()
		d, sess, ac, sends := newManualSessionWithPTYs(t, p)

		sess.tabs[0].focusedPane().screen.Write([]byte("A"))
		d.paint(sess, ac, false, nil)
		requireFrame(t, sends, "Output")

		// The first paint updates the renderer shadow without waiting for Ack.
		d.paint(sess, ac, false, nil)
		requireNoOutputFrame(t, sends)
	})
}

func TestLocalOutputAckDoesNotMoveRendererShadowBackward(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, releasePTY := newBlockingPTY(t)
		defer releasePTY()
		d, sess, ac, sends := newManualSessionWithPTYs(t, p)
		pane := sess.tabs[0].focusedPane()

		pane.screen.Write([]byte("A"))
		d.paint(sess, ac, false, nil)
		first := requireFrame(t, sends, "Output")
		out := unmarshalTestOutput(t, first.Payload)

		pane.screen.Write([]byte("\rB"))
		d.paint(sess, ac, false, nil)
		requireFrame(t, sends, "Output")

		ac.ackOutputState(out.Epoch, out.New)
		d.paint(sess, ac, false, nil)
		requireNoOutputFrame(t, sends)
	})
}

func TestRawTerminalSideEffectDoesNotEnterFullOutputWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := newTestDaemon(t, nil, newStubClock(t))
		tr, trLog := newTrackedConnection(t)
		ac := &attachedClient{tr: tr, output: newOutputStateStream()}
		ac.output.next = maxUnackedOutputStates

		require.NoError(t, d.boundedSendOutputErr(ac, []byte("\x1b]52;c;YQ==\x07")))
		require.Equal(t, uint64(maxUnackedOutputStates), ac.output.next)
		require.Equal(t, uint64(maxUnackedOutputStates), ac.output.outstanding())
		sends := trLog.Sends()
		require.Len(t, sends, 1)
		out := unmarshalTestOutput(t, sends[0].Payload)
		require.Zero(t, out.Base)
		require.Zero(t, out.New)
	})
}

func TestOwnedSynchronousSideEffectSkipsOuterWatchdog(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// An unexpected NewTimer call fails the mock: no daemon watchdog may be armed.
		clock := portsmocks.NewMockClock(t)
		clock.EXPECT().Now().Return(time.Time{}).Maybe()
		d := newTestDaemon(t, nil, clock)
		var sent []wire.Envelope
		tr := portsmocks.NewMockServerConnection(t)
		tr.EXPECT().Capabilities().Return(protocol.ConnectionCapabilities{OutputDataLimit: protocol.MaxOutputDataLen, PreferredOutputWindow: protocol.MaxOutputWindow, OwnedSynchronousSend: true}).Maybe()
		tr.EXPECT().SendOutputSynchronous(mock.Anything).RunAndReturn(func(output protocol.Output) error {
			envelope, err := testServerEnvelope(output)
			sent = append(sent, envelope)
			return err
		}).Maybe()
		ac := &attachedClient{tr: tr, output: newOutputStateStream()}

		require.NoError(t, d.boundedSendOutputErr(ac, make([]byte, 100*1024)))
		require.Len(t, sent, 1)
	})
}

func TestFailedPaintSendRollsBackOutputState(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	ac.replaceTransport(newFailingConnection(t))
	sess.tabs[0].focusedPane().screen.Write([]byte("A"))

	d.paint(sess, ac, false, nil)

	require.Zero(t, ac.output.next)
	require.Zero(t, ac.output.acked)
}

func TestResizeGrowthFirstFrameIncludesConcurrentPTYRedraw(t *testing.T) {
	for _, tc := range []struct {
		name   string
		window uint64
	}{
		{name: "datagram window", window: 1},
		{name: "stream window", window: maxUnackedOutputStates},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// resizeReaderPTY delivers the child's SIGWINCH redraw through the
				// actual ptyReader. Its Resize callback does not return until Read has
				// accepted the bytes, at which point resizeApplying is still true.
				p := newResizeReaderPTY(t, []byte("\x1b[1;81H"+strings.Repeat("B", 40)))
				d, sess, ac, sends := newManualSessionWithPTYs(t, p)
				ac.output.maxOutstanding = tc.window
				pane := testAttachmentTab(sess).focusedPane()
				p.applying = func() bool {
					pane.mu.Lock()
					defer pane.mu.Unlock()
					return pane.resizeApplying
				}
				pane.screen.Write([]byte(strings.Repeat("A", 79)))
				d.paint(sess, ac, true, nil)
				client := vt.NewScreen(80, 24)
				initial := mustApplyOutput(t, client, requireFrame(t, sends, "Output"))
				ac.ackOutputState(initial.Epoch, initial.New)

				d.sessWg.Add(1)
				pane.onExit = func() {}
				go d.ptyReader(sess, testAttachmentTab(sess), pane)
				defer func() { p.close(); d.sessWg.Wait() }()

				// The resize deadline drives coordinator prepare/apply/commit. No test
				// writes the grown columns directly.
				clock, clockTimers := newSignalClock(t, 2)
				d.clock = clock
				d.resize(sess, ac, domain.Size{Cols: 120, Rows: 24})
				timer := <-clockTimers
				done := captureResizeCallbackDone(t, sess.renderCoordinator())
				timer.ch <- time.Time{}
				requireReceived(t, done, "resize callback did not complete")
				require.True(t, p.deliveredWhileApplying(), "redraw must arrive while PTY.Resize owns the apply gate")
				require.Equal(t, 'B', pane.screen.Cell(100, 0).Rune, "replay must parse the redraw before commit")

				client.Resize(120, 24)
				mustApplyOutput(t, client, requireFrame(t, sends, "Output"))
				require.Equal(t, 'B', client.Cell(100, 1).Rune,
					"first grown frame exposed stale pre-SIGWINCH pane content")
				select {
				case extra := <-sends:
					t.Fatalf("resize emitted more than one frame: %#v", extra)
				default:
				}
			})
		})
	}
}

func TestResizeWithoutPTYOutputFlushesOneFullFrameAtDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, releasePTY := newBlockingPTY(t)
		defer releasePTY()
		d, sess, ac, sends := newManualSessionWithPTYs(t, p)
		clk, clkTimers := newSignalClock(t, 1)
		d.clock = clk

		d.resize(sess, ac, domain.Size{Cols: 120, Rows: 24})
		var timer *testTimer
		select {
		case timer = <-clkTimers:
		case <-time.After(time.Second):
			t.Fatal("resize did not schedule a bounded paint")
		}
		requireNoOutputFrame(t, sends)

		done := captureResizeCallbackDone(t, sess.renderCoordinator())
		timer.ch <- time.Now()
		requireReceived(t, done, "resize callback did not complete")
		frame := requireFrame(t, sends, "Output")
		client := vt.NewScreen(120, 24)
		out := mustApplyOutput(t, client, frame)
		require.Zero(t, out.Base)
		require.Contains(t, screenLineText(client, 0), "1")
		require.Contains(t, screenLineText(client, 23), "work")
		require.Equal(t, domain.Size{Cols: 120, Rows: 24}, ac.geometry.Size)
		requireNoOutputFrame(t, sends)
	})
}

func TestResizeBurstFlushesOnlyLatestGeometry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, releasePTY := newBlockingPTY(t)
		defer releasePTY()
		d, sess, ac, sends := newManualSessionWithPTYs(t, p)
		clk, clkTimers := newSignalClock(t, 2)
		d.clock = clk

		d.resize(sess, ac, domain.Size{Cols: 100, Rows: 24})
		var first *testTimer
		select {
		case first = <-clkTimers:
		case <-time.After(time.Second):
			t.Fatal("first resize did not schedule a bounded paint")
		}
		firstDone := captureResizeCallbackDone(t, sess.renderCoordinator())
		d.resize(sess, ac, domain.Size{Cols: 120, Rows: 24})
		var latest *testTimer
		select {
		case latest = <-clkTimers:
		case <-time.After(time.Second):
			t.Fatal("latest resize did not replace the bounded paint")
		}

		latestDone := captureResizeCallbackDone(t, sess.renderCoordinator())
		first.ch <- time.Now()
		requireReceived(t, firstDone, "obsolete resize callback did not complete")
		requireNoOutputFrame(t, sends)
		latest.ch <- time.Now()
		requireReceived(t, latestDone, "latest resize callback did not complete")
		requireFrame(t, sends, "Output")
		require.Equal(t, domain.Size{Cols: 120, Rows: 24}, ac.geometry.Size)
		require.Equal(t, 120, testAttachmentTab(sess).focusedPane().screen.Columns())
		requireNoOutputFrame(t, sends)
	})
}

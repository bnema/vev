package daemon

import (
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	vt "github.com/bnema/vev-vt"
	renderer "github.com/bnema/vev-vt/ansi"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

type datagramTestTransport struct {
	sends chan ports.Frame
	recv  chan ports.Frame
	once  sync.Once
}

type failingOutputTransport struct{}

func (failingOutputTransport) Send(ports.Frame) error     { return errors.New("send failed") }
func (failingOutputTransport) Recv() (ports.Frame, error) { return ports.Frame{}, io.EOF }
func (failingOutputTransport) Close() error               { return nil }

type asyncPaintTransport struct {
	syncSends  chan ports.Frame
	asyncSends chan ports.Frame
}

func (t *asyncPaintTransport) Send(f ports.Frame) error      { t.syncSends <- f; return nil }
func (t *asyncPaintTransport) SendAsync(f ports.Frame) error { t.asyncSends <- f; return nil }
func (t *asyncPaintTransport) Recv() (ports.Frame, error)    { return ports.Frame{}, io.EOF }
func (t *asyncPaintTransport) Close() error                  { return nil }

type timedSideEffectTransport struct {
	closeTrackingTransport
}

func (t *timedSideEffectTransport) SendSynchronous(f ports.Frame) error {
	return t.Send(f)
}

type noWatchdogClock struct{}

func (noWatchdogClock) Now() time.Time { return time.Time{} }
func (noWatchdogClock) NewTimer(time.Duration) ports.Timer {
	panic("owned synchronous send must not install daemon watchdog")
}

func newDatagramTestTransport() *datagramTestTransport {
	return &datagramTestTransport{sends: make(chan ports.Frame, 64), recv: make(chan ports.Frame, 64)}
}

func (t *datagramTestTransport) Send(f ports.Frame) error { t.sends <- f; return nil }
func (t *datagramTestTransport) Recv() (ports.Frame, error) {
	f, ok := <-t.recv
	if !ok {
		return ports.Frame{}, io.EOF
	}
	return f, nil
}
func (t *datagramTestTransport) Close() error { t.once.Do(func() { close(t.recv) }); return nil }

func TestDatagramAttachPipelinesRendererBeforeAck(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d, sess, _, _ := newManualSessionWithPTYs(t, p)
	tr := newDatagramTestTransport()
	routed, ac, err := d.route(ports.Hello{
		Version:           ports.ProtocolVersion,
		Intent:            ports.IntentAttach,
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
	first := awaitFrame(t, tr.sends, ports.MsgOutput)
	out, err := ports.UnmarshalOutput(first.Payload)
	require.NoError(t, err)
	require.Equal(t, uint64(1), out.New)

	sess.tabs[0].focusedPane().screen.Write([]byte("\rB"))
	d.paint(sess, ac, false, nil)
	updated := awaitFrame(t, tr.sends, ports.MsgOutput)
	out, err = ports.UnmarshalOutput(updated.Payload)
	require.NoError(t, err)
	require.Equal(t, uint64(2), out.New)
	// Both states were emitted before the MsgAck; the ordered dependency chain
	// must therefore remain valid without waiting for renderer acknowledgement.
	tr.recv <- ports.Frame{Type: ports.MsgAck, Payload: mustMarshalAck(ports.Ack{Epoch: out.Epoch, State: out.New})}
	require.NoError(t, tr.Close())
	d.runConnLoop(ac)

	// The production MsgAck path retires retained states without moving the
	// renderer baseline backward.
	d.paint(sess, ac, false, nil)
	require.Empty(t, tr.sends)
}

func TestPaintExplicitlyUsesAsyncTransportCapability(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	tr := &asyncPaintTransport{syncSends: make(chan ports.Frame, 1), asyncSends: make(chan ports.Frame, 1)}
	ac.replaceTransport(tr)
	sess.tabs[0].focusedPane().screen.Write([]byte("A"))

	d.paint(sess, ac, false, nil)

	awaitFrame(t, tr.asyncSends, ports.MsgOutput)
	select {
	case frame := <-tr.syncSends:
		t.Fatalf("paint used synchronous Send: %+v", frame)
	default:
	}
}

func TestDatagramMultipleUnackedScrollPaintsMatchLatestFrame(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d, sess, _, _ := newManualSessionWithPTYs(t, p)
	tr := newDatagramTestTransport()
	_, ac, err := d.route(ports.Hello{
		Version:           ports.ProtocolVersion,
		Intent:            ports.IntentAttach,
		Name:              sess.name,
		Size:              domain.Size{Cols: 80, Rows: 24},
		MaxOutputInFlight: 8,
	}, tr)
	require.NoError(t, err)

	pane := sess.tabs[0].focusedPane()
	for y := range pane.screen.Frame.Height {
		cell := renderer.Cell{Rune: rune('A' + y), Style: renderer.DefaultStyle()}
		for x := range pane.screen.Frame.Width {
			pane.screen.Frame.Set(x, y, cell)
		}
	}
	pane.screen.ClearDamage()
	client := vt.NewScreen(80, 25)
	d.paint(sess, ac, true, nil)
	first := mustApplyOutput(t, client, awaitFrame(t, tr.sends, ports.MsgOutput))
	ac.ackOutputState(first.Epoch, first.New)

	// Preserve the frame after one scroll while inducing a real VT scroll
	// damage event before each production paint. On main, both unacknowledged
	// paints are generated from the ACKed frame and apply the scroll twice to
	// the client. The ordered stream instead renders the second paint from the
	// preceding emitted frame and overwrites the incompatible repeated damage.
	desired := pane.screen.Frame.Clone()
	desired.ScrollUp(0, desired.Height-1, 1)
	for x := range desired.Width {
		desired.Set(x, desired.Height-1, renderer.Cell{Rune: 'z', Style: renderer.DefaultStyle()})
	}
	for range 2 {
		pane.screen.Frame = desired.Clone()
		pane.screen.Row = pane.screen.Frame.Height - 1
		pane.screen.Col = 0
		pane.screen.Write([]byte("\nq"))
		pane.screen.Frame = desired.Clone()
		d.paint(sess, ac, false, nil)
		mustApplyOutput(t, client, awaitFrame(t, tr.sends, ports.MsgOutput))
	}

	require.Equal(t, 'B', client.Frame.At(0, 1).Rune,
		"client-visible content must equal the latest daemon-composed frame after pipelined scroll paints")
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

func mustApplyOutput(t *testing.T, screen *vt.Screen, frame ports.Frame) ports.Output {
	t.Helper()
	out, err := ports.UnmarshalOutput(frame.Payload)
	require.NoError(t, err)
	screen.Write(out.Data)
	return out
}

func TestLocalAttachStillAdvancesRendererOnSend(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)

	sess.tabs[0].focusedPane().screen.Write([]byte("A"))
	d.paint(sess, ac, false, nil)
	awaitFrame(t, sends, ports.MsgOutput)

	// The first paint updates the renderer shadow without waiting for MsgAck.
	d.paint(sess, ac, false, nil)
	requireNoOutputFrame(t, sends)
}

func TestLocalOutputAckDoesNotMoveRendererShadowBackward(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	pane := sess.tabs[0].focusedPane()

	pane.screen.Write([]byte("A"))
	d.paint(sess, ac, false, nil)
	first := awaitFrame(t, sends, ports.MsgOutput)
	out, err := ports.UnmarshalOutput(first.Payload)
	require.NoError(t, err)

	pane.screen.Write([]byte("\rB"))
	d.paint(sess, ac, false, nil)
	awaitFrame(t, sends, ports.MsgOutput)

	ac.ackOutputState(out.Epoch, out.New)
	d.paint(sess, ac, false, nil)
	requireNoOutputFrame(t, sends)
}

func TestRawTerminalSideEffectDoesNotEnterFullOutputWindow(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	tr := &closeTrackingTransport{}
	ac := &attachedClient{tr: tr, output: newOutputStateStream()}
	ac.output.next = maxUnackedOutputStates

	require.NoError(t, d.boundedSendOutputErr(ac, []byte("\x1b]52;c;YQ==\x07")))
	require.Equal(t, uint64(maxUnackedOutputStates), ac.output.next)
	require.Equal(t, uint64(maxUnackedOutputStates), ac.output.outstanding())
	sends := tr.Sends()
	require.Len(t, sends, 1)
	out, err := ports.UnmarshalOutput(sends[0].Payload)
	require.NoError(t, err)
	require.Zero(t, out.Base)
	require.Zero(t, out.New)
}

func TestOwnedSynchronousSideEffectSkipsOuterWatchdog(t *testing.T) {
	d := newTestDaemon(t, nil, noWatchdogClock{})
	tr := &timedSideEffectTransport{}
	ac := &attachedClient{tr: tr, output: newOutputStateStream()}

	require.NoError(t, d.boundedSendOutputErr(ac, make([]byte, 100*1024)))
	require.Len(t, tr.Sends(), 1)
}

func TestFailedPaintSendRollsBackOutputState(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	ac.replaceTransport(failingOutputTransport{})
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
			// resizeReaderPTY delivers the child's SIGWINCH redraw through the
			// actual ptyReader. Its Resize callback does not return until Read has
			// accepted the bytes, at which point resizeApplying is still true.
			p := newResizeReaderPTY([]byte("\x1b[1;81H" + strings.Repeat("B", 40)))
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
			initial := mustApplyOutput(t, client, awaitFrame(t, sends, ports.MsgOutput))
			ac.ackOutputState(initial.Epoch, initial.New)

			d.sessWg.Add(1)
			pane.onExit = func() {}
			go d.ptyReader(sess, testAttachmentTab(sess), pane)
			defer func() { p.close(); d.sessWg.Wait() }()

			// The resize deadline drives coordinator prepare/apply/commit. No test
			// writes the grown columns directly.
			clock := &signalClock{timers: make(chan *signalTimer, 2)}
			d.clock = clock
			d.resize(sess, ac, domain.Size{Cols: 120, Rows: 24})
			timer := <-clock.timers
			done := captureResizeCallbackDone(t, sess.renderCoordinator())
			timer.ch <- time.Time{}
			awaitTestCompletion(t, done, "resize callback did not complete")
			require.True(t, p.deliveredWhileApplying(), "redraw must arrive while PTY.Resize owns the apply gate")
			require.Equal(t, 'B', pane.screen.Frame.At(100, 0).Rune, "replay must parse the redraw before commit")

			client.Resize(120, 24)
			mustApplyOutput(t, client, awaitFrame(t, sends, ports.MsgOutput))
			require.Equal(t, 'B', client.Frame.At(100, 1).Rune,
				"first grown frame exposed stale pre-SIGWINCH pane content")
			select {
			case extra := <-sends:
				t.Fatalf("resize emitted more than one frame: %#v", extra)
			default:
			}
		})
	}
}

func TestResizeWithoutPTYOutputFlushesOneFullFrameAtDeadline(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	clk := &signalClock{timers: make(chan *signalTimer, 1)}
	d.clock = clk

	d.resize(sess, ac, domain.Size{Cols: 120, Rows: 24})
	var timer *signalTimer
	select {
	case timer = <-clk.timers:
	case <-time.After(time.Second):
		t.Fatal("resize did not schedule a bounded paint")
	}
	requireNoOutputFrame(t, sends)

	done := captureResizeCallbackDone(t, sess.renderCoordinator())
	timer.ch <- time.Now()
	awaitTestCompletion(t, done, "resize callback did not complete")
	frame := awaitFrame(t, sends, ports.MsgOutput)
	client := vt.NewScreen(120, 24)
	out := mustApplyOutput(t, client, frame)
	require.Zero(t, out.Base)
	require.Contains(t, screenLineText(client, 0), "1")
	require.Contains(t, screenLineText(client, 23), "work")
	require.Equal(t, domain.Size{Cols: 120, Rows: 24}, ac.size)
	requireNoOutputFrame(t, sends)
}

func TestResizeBurstFlushesOnlyLatestGeometry(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	clk := &signalClock{timers: make(chan *signalTimer, 2)}
	d.clock = clk

	d.resize(sess, ac, domain.Size{Cols: 100, Rows: 24})
	var first *signalTimer
	select {
	case first = <-clk.timers:
	case <-time.After(time.Second):
		t.Fatal("first resize did not schedule a bounded paint")
	}
	firstDone := captureResizeCallbackDone(t, sess.renderCoordinator())
	d.resize(sess, ac, domain.Size{Cols: 120, Rows: 24})
	var latest *signalTimer
	select {
	case latest = <-clk.timers:
	case <-time.After(time.Second):
		t.Fatal("latest resize did not replace the bounded paint")
	}

	latestDone := captureResizeCallbackDone(t, sess.renderCoordinator())
	first.ch <- time.Now()
	awaitTestCompletion(t, firstDone, "obsolete resize callback did not complete")
	requireNoOutputFrame(t, sends)
	latest.ch <- time.Now()
	awaitTestCompletion(t, latestDone, "latest resize callback did not complete")
	awaitFrame(t, sends, ports.MsgOutput)
	require.Equal(t, domain.Size{Cols: 120, Rows: 24}, ac.size)
	require.Equal(t, 120, testAttachmentTab(sess).focusedPane().screen.Frame.Width)
	requireNoOutputFrame(t, sends)
}

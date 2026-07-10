package daemon

import (
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
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

	sess.tabs[0].focusedPane().screen.Write([]byte("A"))
	d.paint(sess, ac, false)
	first := awaitFrame(t, tr.sends, ports.MsgOutput)
	out, err := ports.UnmarshalOutput(first.Payload)
	require.NoError(t, err)
	require.Equal(t, uint64(1), out.NewStateNum)

	// Before the MsgAck, the renderer has already advanced along the ordered
	// output dependency chain, so an unchanged repaint is a no-op.
	d.paint(sess, ac, false)
	requireNoOutputFrame(t, tr.sends)

	tr.recv <- ports.Frame{Type: ports.MsgAck, Payload: ports.MarshalAck(ports.Ack{AckedStateNum: out.NewStateNum})}
	require.NoError(t, tr.Close())
	d.runConnLoop(ac)

	// The production MsgAck path retires retained states without moving the
	// renderer baseline backward.
	d.paint(sess, ac, false)
	requireNoOutputFrame(t, tr.sends)
}

func TestPaintExplicitlyUsesAsyncTransportCapability(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	tr := &asyncPaintTransport{syncSends: make(chan ports.Frame, 1), asyncSends: make(chan ports.Frame, 1)}
	ac.replaceTransport(tr)
	sess.tabs[0].focusedPane().screen.Write([]byte("A"))

	d.paint(sess, ac, false)

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
	d.paint(sess, ac, true)
	first := mustApplyOutput(t, client, awaitFrame(t, tr.sends, ports.MsgOutput))
	ac.ackOutputState(first.NewStateNum)

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
		d.paint(sess, ac, false)
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
	d.paint(sess, ac, false)
	awaitFrame(t, sends, ports.MsgOutput)

	// The first paint updates the renderer shadow without waiting for MsgAck.
	d.paint(sess, ac, false)
	requireNoOutputFrame(t, sends)
}

func TestLocalOutputAckDoesNotMoveRendererShadowBackward(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	pane := sess.tabs[0].focusedPane()

	pane.screen.Write([]byte("A"))
	d.paint(sess, ac, false)
	first := awaitFrame(t, sends, ports.MsgOutput)
	out, err := ports.UnmarshalOutput(first.Payload)
	require.NoError(t, err)

	pane.screen.Write([]byte("\rB"))
	d.paint(sess, ac, false)
	awaitFrame(t, sends, ports.MsgOutput)

	ac.ackOutputState(out.NewStateNum)
	d.paint(sess, ac, false)
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
	require.Zero(t, out.BaseStateNum)
	require.Zero(t, out.NewStateNum)
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

	d.paint(sess, ac, false)

	require.Zero(t, ac.output.next)
	require.Zero(t, ac.output.acked)
}

func TestDatagramWindowOneCoalescesPaintsUntilMsgAck(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d, sess, _, _ := newManualSessionWithPTYs(t, p)
	tr := newDatagramTestTransport()
	_, ac, err := d.route(ports.Hello{
		Version:           ports.ProtocolVersion,
		Intent:            ports.IntentAttach,
		Name:              sess.name,
		Size:              domain.Size{Cols: 80, Rows: 24},
		MaxOutputInFlight: 1,
	}, tr)
	require.NoError(t, err)

	pane := sess.tabs[0].focusedPane()
	pane.screen.Write([]byte("A"))
	d.paint(sess, ac, false)
	first := awaitFrame(t, tr.sends, ports.MsgOutput)
	firstOutput, err := ports.UnmarshalOutput(first.Payload)
	require.NoError(t, err)
	require.Equal(t, uint64(1), firstOutput.NewStateNum)
	for range 100 {
		pane.screen.Write([]byte("x"))
		d.paint(sess, ac, false)
	}
	pane.screen.Write([]byte("LATEST"))
	d.paint(sess, ac, false)
	requireNoOutputFrame(t, tr.sends)
	require.Equal(t, uint64(1), ac.output.outstanding(), "only one state-bearing datagram output may be unacknowledged")

	// Side-effect output is control-like: it must pass while the state window is
	// full without consuming another state number.
	require.NoError(t, d.boundedSendOutputErr(ac, []byte("side-effect")))
	sideEffect := awaitFrame(t, tr.sends, ports.MsgOutput)
	sideEffectOutput, err := ports.UnmarshalOutput(sideEffect.Payload)
	require.NoError(t, err)
	require.Zero(t, sideEffectOutput.BaseStateNum)
	require.Zero(t, sideEffectOutput.NewStateNum)
	require.Equal(t, uint64(1), ac.output.outstanding())

	tr.recv <- ports.Frame{Type: ports.MsgAck, Payload: ports.MarshalAck(ports.Ack{AckedStateNum: firstOutput.NewStateNum})}
	require.NoError(t, tr.Close())
	d.runConnLoop(ac)
	second := awaitFrame(t, tr.sends, ports.MsgOutput)
	secondOutput, err := ports.UnmarshalOutput(second.Payload)
	require.NoError(t, err)
	require.Equal(t, firstOutput.NewStateNum, secondOutput.BaseStateNum)
	require.Equal(t, uint64(2), secondOutput.NewStateNum)
	require.Contains(t, string(secondOutput.Data), "LATEST", "cumulative ACK must release the latest coalesced state")
	require.Equal(t, uint64(1), ac.output.outstanding())
}

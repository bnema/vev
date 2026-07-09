package daemon

import (
	"io"
	"sync"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

type datagramTestTransport struct {
	sends chan ports.Frame
	recv  chan ports.Frame
	once  sync.Once
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

func TestDatagramAttachAdvancesRendererOnlyOnAck(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d, sess, _, _ := newManualSessionWithPTYs(t, p)
	tr := newDatagramTestTransport()
	routed, ac, err := d.route(ports.Hello{
		Version:   ports.ProtocolVersion,
		Intent:    ports.IntentAttach,
		Name:      sess.name,
		Size:      domain.Size{Cols: 80, Rows: 24},
		AckOutput: true,
	}, tr)
	require.NoError(t, err)
	require.Same(t, sess, routed)

	sess.tabs[0].focusedPane().screen.Write([]byte("A"))
	d.paint(sess, ac, false)
	first := awaitFrame(t, tr.sends, ports.MsgOutput)
	out, err := ports.UnmarshalOutput(first.Payload)
	require.NoError(t, err)
	require.Equal(t, uint64(1), out.NewStateNum)

	// Before the MsgAck, AdvanceOnAck must keep rediffing from the unacked
	// shadow even if the composed screen is unchanged.
	d.paint(sess, ac, false)
	awaitFrame(t, tr.sends, ports.MsgOutput)

	tr.recv <- ports.Frame{Type: ports.MsgAck, Payload: ports.MarshalAck(ports.Ack{AckedStateNum: out.NewStateNum})}
	require.NoError(t, tr.Close())
	d.runConnLoop(ac)

	// After the production MsgAck path advances the renderer shadow to state 1,
	// an unchanged repaint is a no-op.
	d.paint(sess, ac, false)
	requireNoOutputFrame(t, tr.sends)
}

func TestLocalAttachStillAdvancesRendererOnSend(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)

	sess.tabs[0].focusedPane().screen.Write([]byte("A"))
	d.paint(sess, ac, false)
	awaitFrame(t, sends, ports.MsgOutput)

	// Local/default transports keep AdvanceOnSend: the first paint updates the
	// renderer shadow without waiting for MsgAck.
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

	require.NoError(t, ac.ackOutputState(out.NewStateNum))
	d.paint(sess, ac, false)
	requireNoOutputFrame(t, sends)
}

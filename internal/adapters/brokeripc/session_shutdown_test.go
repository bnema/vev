package brokeripc

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokerwire"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// Session shutdown ordering (GO-007). A bridged stream writes through its own
// carriage, whose writer is parked inside the outer carriage write while the
// peer is not reading. Settling a stream joins that writer, so shutdown must
// close the outer carriage first: otherwise the join waits on the peer and the
// session never releases its slot or done.

// rawPeer drives one broker peer over a bare net carriage, framing by hand
// instead of composing a transport. Consuming a length prefix and deliberately
// leaving the body unread is what parks the server's write on this non-reading
// peer deterministically.
type rawPeer struct {
	conn     net.Conn
	ceilings brokerwire.Ceilings
}

// send writes one encoded client frame.
func (p *rawPeer) send(t *testing.T, message brokerwire.ClientMessage) {
	t.Helper()
	payload, err := brokerwire.EncodeClient(message, p.ceilings.MaxReceiveEnvelopeBytes, p.ceilings.StreamChunkLimit)
	require.NoError(t, err)
	frame := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)
	_, err = p.conn.Write(frame)
	require.NoError(t, err)
}

// recv reads and decodes one whole server frame.
func (p *rawPeer) recv(t *testing.T) brokerwire.ServerMessage {
	t.Helper()
	var header [4]byte
	_, err := io.ReadFull(p.conn, header[:])
	require.NoError(t, err)
	payload := make([]byte, binary.BigEndian.Uint32(header[:]))
	_, err = io.ReadFull(p.conn, payload)
	require.NoError(t, err)
	message, err := brokerwire.DecodeServer(payload, p.ceilings.MaxReceiveEnvelopeBytes, p.ceilings.StreamChunkLimit)
	require.NoError(t, err)
	return message
}

// recvHeader consumes only the length prefix of the next server frame, leaving
// its body unread: the server's frame write is then provably still in flight on
// this peer, so the stream writer that issued it is parked inside it.
func (p *rawPeer) recvHeader(t *testing.T) uint32 {
	t.Helper()
	var header [4]byte
	_, err := io.ReadFull(p.conn, header[:])
	require.NoError(t, err)
	length := binary.BigEndian.Uint32(header[:])
	require.NotZero(t, length, "the parked frame must carry a payload")
	return length
}

// TestProtocolViolationWithParkedStreamWriterReleasesSession proves a protocol
// violation settles a session whose bridged stream writer is parked on a
// non-reading peer: the violation alone closes done, closes the admitted core
// service, and releases the listener's client slot, with no external session or
// listener Close.
func TestProtocolViolationWithParkedStreamWriterReleasesSession(t *testing.T) {
	const epoch = ports.BrokerEpoch(0x55)
	serverConn, peerConn := net.Pipe()
	t.Cleanup(func() { _ = peerConn.Close() })

	transport, ok := ipc.NewTransport(serverConn).(wire.BoundedTransport)
	require.True(t, ok, "the IPC carriage must be a bounded transport")
	core := newTestCore(epoch)
	released := make(chan struct{}, 1)
	session, err := newServerSession(epoch, transport, brokerwire.DefaultCeilings(), core, Config{}, func() { released <- struct{}{} })
	require.NoError(t, err)

	// The peer's Register already happened; the duplicate Register below is the
	// violation.
	require.NoError(t, session.conn.Register())
	go session.run()
	peer := &rawPeer{conn: peerConn, ceilings: brokerwire.DefaultCeilings()}

	session.startStream(brokerwire.OpenStream{
		Epoch: epoch, Connection: session.scope.Connection, Stream: 1,
		Purpose: ports.BrokerStreamControl, Local: true, Policy: testPolicy(),
	})
	opened, isOpened := peer.recv(t).(brokerwire.StreamOpened)
	require.True(t, isOpened, "the stream must be established before its writer can be parked")
	require.Equal(t, ports.BrokerStreamID(1), opened.Stream)

	// One daemon message reaches the stream's relay, and the peer consumes only
	// the length prefix of the frame it produces: the broker's carriage write can
	// no longer complete, so the stream writer is parked inside it.
	conn := core.logicalConn(1)
	require.NotNil(t, conn, "the broker core must have admitted the stream")
	conn.toClient <- protocol.Pong{}
	peer.recvHeader(t)

	peer.send(t, brokerwire.Register{})

	select {
	case <-session.done:
	case <-time.After(5 * time.Second):
		t.Fatal("a protocol violation must settle a session with a parked stream writer")
	}
	select {
	case <-released:
	case <-time.After(5 * time.Second):
		t.Fatal("a settled session must release its client slot")
	}
	require.ErrorIs(t, session.terminalErr(), ErrProtocol)
	select {
	case <-core.done:
	default:
		t.Fatal("a settled session must close its admitted core service")
	}
}

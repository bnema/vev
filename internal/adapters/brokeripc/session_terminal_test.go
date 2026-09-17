package brokeripc

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokerwire"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
)

// newRunningSession starts one registered server session over a real net.Pipe
// carriage so a test can drive its terminal outcome from the peer side.
func newRunningSession(t *testing.T, epoch ports.BrokerEpoch) (*serverSession, net.Conn) {
	t.Helper()
	serverConn, peerConn := net.Pipe()
	t.Cleanup(func() { _ = peerConn.Close() })
	transport, ok := ipc.NewTransport(serverConn).(wire.BoundedTransport)
	require.True(t, ok, "the IPC carriage must be a bounded transport")
	session, err := newServerSession(epoch, transport, brokerwire.DefaultCeilings(), newTestCore(epoch), Config{}, func() {}, time.Time{})
	require.NoError(t, err)
	require.NoError(t, session.conn.Register())
	go session.run()
	return session, peerConn
}

// awaitSessionDone waits for one session to reach its terminal state.
func awaitSessionDone(t *testing.T, session *serverSession) {
	t.Helper()
	select {
	case <-session.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("session did not become terminal")
	}
}

// TestSessionErrReportsOrderlyEndAsNil pins the server session Err contract: a
// peer disconnect and a local Close are orderly ends and report nil, while a
// protocol failure reports the typed cause that settled the session. Done is
// closed and Err stable in every case.
func TestSessionErrReportsOrderlyEndAsNil(t *testing.T) {
	t.Run("peer EOF is orderly", func(t *testing.T) {
		session, peer := newRunningSession(t, 0x61)
		require.NoError(t, peer.Close())
		awaitSessionDone(t, session)

		// The underlying cause is the peer's EOF, but Err presents it as an
		// orderly end so a watcher never mistakes a departure for a failure.
		require.Error(t, session.terminalErr(), "the peer EOF remains the recorded internal cause")
		require.True(t, orderlyDisconnect(session.terminalErr()))
		require.NoError(t, session.Err())
		require.NoError(t, session.Err(), "Err is stable after Done")
		require.NoError(t, session.Close())
	})

	t.Run("local Close is orderly", func(t *testing.T) {
		session, _ := newRunningSession(t, 0x62)
		require.NoError(t, session.Close())
		awaitSessionDone(t, session)
		require.NoError(t, session.Err())
	})

	t.Run("protocol failure is typed", func(t *testing.T) {
		session, peer := newRunningSession(t, 0x63)
		raw := &rawPeer{conn: peer, ceilings: brokerwire.DefaultCeilings()}
		// A second Register is a protocol-order violation and settles the
		// connection.
		raw.send(t, brokerwire.Register{})
		awaitSessionDone(t, session)

		require.Error(t, session.Err())
		require.ErrorIs(t, session.Err(), ErrProtocol)
		require.Error(t, session.Close(), "a protocol failure is not an orderly end")
	})
}

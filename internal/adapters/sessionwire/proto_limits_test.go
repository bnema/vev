package sessionwire

import (
	"bytes"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

func testOutput(data []byte) protocol.Output {
	return protocol.Output{Epoch: 1, New: 1, Full: true, Size: domain.Size{Cols: 80, Rows: 24}, Context: testUIOutputContext(), Data: data}
}

// TestServerSendEnforcesNegotiatedCeilings proves the normal, async, and
// owned-synchronous send paths all refuse envelopes and output data above
// the negotiated ceilings before any transport call.
func TestServerSendEnforcesNegotiatedCeilings(t *testing.T) {
	envelopeBound := protoCeilings{maxReceiveEnvelopeBytes: 8, outputDataLimit: uint64(protocol.MaxOutputDataLen)}
	outputBound := protoCeilings{maxReceiveEnvelopeBytes: wire.AbsoluteEnvelopeLimit, outputDataLimit: 4}

	t.Run("normal send", func(t *testing.T) {
		raw, rawState := newScriptedTransport(t)
		conn := &serverConnection{handshake: handshake{raw: raw, ceilings: envelopeBound}}
		require.ErrorIs(t, conn.SendServer(protocol.ErrorMsg{Code: protocol.ErrInternal, Text: "oversized"}), wire.ErrScanLength)
		require.Zero(t, rawState.sentLen())
		conn.ceilings = outputBound
		require.ErrorIs(t, conn.SendOutput(testOutput([]byte("12345"))), errOutputDataExceedsLimit)
		require.Zero(t, rawState.sentLen())
	})
	t.Run("async and owned synchronous send", func(t *testing.T) {
		raw, rawState := newAsyncScriptedTransport(t)
		conn := &serverConnection{handshake: handshake{raw: raw, ceilings: envelopeBound}}
		require.ErrorIs(t, conn.SendServerAsync(protocol.ErrorMsg{Code: protocol.ErrInternal, Text: "oversized"}), wire.ErrScanLength)
		require.ErrorIs(t, conn.SendServerSynchronous(protocol.ErrorMsg{Code: protocol.ErrInternal, Text: "oversized"}), wire.ErrScanLength)
		require.Zero(t, rawState.asyncLen())
		require.Zero(t, rawState.synchronousLen())
		conn.ceilings = outputBound
		require.ErrorIs(t, conn.SendOutputAsync(testOutput([]byte("12345"))), errOutputDataExceedsLimit)
		require.ErrorIs(t, conn.SendOutputSynchronous(testOutput([]byte("12345"))), errOutputDataExceedsLimit)
		require.Zero(t, rawState.asyncLen())
		require.Zero(t, rawState.synchronousLen())
	})
	t.Run("within ceilings", func(t *testing.T) {
		raw, rawState := newScriptedTransport(t)
		conn := &serverConnection{handshake: handshake{raw: raw, ceilings: defaultProtoCeilings()}}
		require.NoError(t, conn.SendServer(protocol.Pong{}))
		require.NoError(t, conn.SendOutput(testOutput([]byte("12345"))))
		require.Equal(t, 2, rawState.sentLen())
	})
}

// TestTypedReceiveEnforcesNegotiatedCeilings proves both typed receive paths
// apply the negotiated envelope ceiling before decode and the negotiated
// output-data ceiling before decompression.
func TestTypedReceiveEnforcesNegotiatedCeilings(t *testing.T) {
	largeInput := mustEncodeClient(t, protocol.Input{InputSeq: 1, Data: bytes.Repeat([]byte("x"), 64)})
	largeOutput := mustEncodeServer(t, testOutput(bytes.Repeat([]byte("x"), 64)))
	smallOutput := mustEncodeServer(t, testOutput([]byte("12345")))

	t.Run("server receive envelope ceiling", func(t *testing.T) {
		raw, _ := newScriptedTransport(t, withRecv([]wire.Envelope{{Payload: largeInput}}...))
		conn := &serverConnection{handshake: handshake{raw: raw, ceilings: protoCeilings{maxReceiveEnvelopeBytes: 16, outputDataLimit: uint64(protocol.MaxOutputDataLen)}}}
		conn.preambleOnce.Do(func() {})
		_, err := conn.ReceiveClient()
		var failure *protocol.DecodeFailure
		require.ErrorAs(t, err, &failure)
		require.ErrorIs(t, failure.Err, wire.ErrScanLength)
	})
	t.Run("client receive envelope ceiling", func(t *testing.T) {
		raw, _ := newScriptedTransport(t, withRecv([]wire.Envelope{{Payload: largeOutput}}...))
		conn := &clientConnection{handshake: handshake{raw: raw, ceilings: protoCeilings{maxReceiveEnvelopeBytes: 16, outputDataLimit: uint64(protocol.MaxOutputDataLen)}}}
		conn.preambleOnce.Do(func() {})
		_, err := conn.ReceiveServer()
		var failure *protocol.DecodeFailure
		require.ErrorAs(t, err, &failure)
		require.ErrorIs(t, failure.Err, wire.ErrScanLength)
	})
	t.Run("client receive output data ceiling", func(t *testing.T) {
		raw, _ := newScriptedTransport(t, withRecv([]wire.Envelope{{Payload: smallOutput}}...))
		conn := &clientConnection{handshake: handshake{raw: raw, ceilings: protoCeilings{maxReceiveEnvelopeBytes: wire.AbsoluteEnvelopeLimit, outputDataLimit: 4}}}
		conn.preambleOnce.Do(func() {})
		_, err := conn.ReceiveServer()
		var failure *protocol.DecodeFailure
		require.ErrorAs(t, err, &failure)
		require.ErrorIs(t, failure.Err, errOutputDataExceedsLimit)
	})
	t.Run("within ceilings decodes", func(t *testing.T) {
		serverRaw, _ := newScriptedTransport(t, withRecv([]wire.Envelope{{Payload: largeInput}}...))
		server := &serverConnection{handshake: handshake{raw: serverRaw, ceilings: defaultProtoCeilings()}}
		server.preambleOnce.Do(func() {})
		got, err := server.ReceiveClient()
		require.NoError(t, err)
		require.Equal(t, []byte(bytes.Repeat([]byte("x"), 64)), got.(protocol.Input).Data)

		clientRaw, _ := newScriptedTransport(t, withRecv([]wire.Envelope{{Payload: smallOutput}}...))
		client := &clientConnection{handshake: handshake{raw: clientRaw, ceilings: defaultProtoCeilings()}}
		client.preambleOnce.Do(func() {})
		decoded, err := client.ReceiveServer()
		require.NoError(t, err)
		require.Equal(t, []byte("12345"), decoded.(protocol.Output).Data)
	})
}

// TestClientSendAndCapabilitiesUseNegotiatedCeilings proves the client send
// path and advertised capabilities reflect the negotiated ceilings.
func TestClientSendAndCapabilitiesUseNegotiatedCeilings(t *testing.T) {
	raw, rawState := newScriptedTransport(t)
	conn := &clientConnection{handshake: handshake{raw: raw, ceilings: protoCeilings{maxReceiveEnvelopeBytes: 8, outputDataLimit: 1234}}}
	conn.preambleOnce.Do(func() {})
	require.ErrorIs(t, conn.SendClient(protocol.Input{InputSeq: 1, Data: []byte("hello")}), wire.ErrScanLength)
	require.Zero(t, rawState.sentLen())
	require.Equal(t, 1234, conn.Capabilities().OutputDataLimit)

	server := &serverConnection{handshake: handshake{raw: newBareTransport(t), ceilings: protoCeilings{maxReceiveEnvelopeBytes: 1, outputDataLimit: 4321}}}
	require.Equal(t, 4321, server.Capabilities().OutputDataLimit)
}

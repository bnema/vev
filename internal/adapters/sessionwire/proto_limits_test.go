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

// asyncScriptedTransport carries the async and owned-synchronous send
// capabilities so tests can prove every server send mode shares the
// negotiated ceiling checks.
type asyncScriptedTransport struct {
	*scriptedTransport
	async       []wire.Envelope
	synchronous []wire.Envelope
}

func (t *asyncScriptedTransport) SendAsync(envelope wire.Envelope) error {
	t.async = append(t.async, envelope)
	return nil
}

func (t *asyncScriptedTransport) SendSynchronous(envelope wire.Envelope) error {
	t.synchronous = append(t.synchronous, envelope)
	return nil
}

// TestServerSendEnforcesNegotiatedCeilings proves the normal, async, and
// owned-synchronous send paths all refuse envelopes and output data above
// the negotiated ceilings before any transport call.
func TestServerSendEnforcesNegotiatedCeilings(t *testing.T) {
	envelopeBound := protoCeilings{maxReceiveEnvelopeBytes: 8, outputDataLimit: uint64(protocol.MaxOutputDataLen)}
	outputBound := protoCeilings{maxReceiveEnvelopeBytes: wire.AbsoluteEnvelopeLimit, outputDataLimit: 4}

	t.Run("normal send", func(t *testing.T) {
		raw := &scriptedTransport{}
		conn := &serverConnection{raw: raw, ceilings: envelopeBound}
		require.ErrorIs(t, conn.SendServer(protocol.ErrorMsg{Code: protocol.ErrInternal, Text: "oversized"}), wire.ErrScanLength)
		require.Zero(t, raw.sentLen())
		conn.ceilings = outputBound
		require.ErrorIs(t, conn.SendOutput(testOutput([]byte("12345"))), errOutputDataExceedsLimit)
		require.Zero(t, raw.sentLen())
	})
	t.Run("async and owned synchronous send", func(t *testing.T) {
		raw := &asyncScriptedTransport{scriptedTransport: &scriptedTransport{}}
		conn := &serverConnection{raw: raw, ceilings: envelopeBound}
		require.ErrorIs(t, conn.SendServerAsync(protocol.ErrorMsg{Code: protocol.ErrInternal, Text: "oversized"}), wire.ErrScanLength)
		require.ErrorIs(t, conn.SendServerSynchronous(protocol.ErrorMsg{Code: protocol.ErrInternal, Text: "oversized"}), wire.ErrScanLength)
		require.Empty(t, raw.async)
		require.Empty(t, raw.synchronous)
		conn.ceilings = outputBound
		require.ErrorIs(t, conn.SendOutputAsync(testOutput([]byte("12345"))), errOutputDataExceedsLimit)
		require.ErrorIs(t, conn.SendOutputSynchronous(testOutput([]byte("12345"))), errOutputDataExceedsLimit)
		require.Empty(t, raw.async)
		require.Empty(t, raw.synchronous)
	})
	t.Run("within ceilings", func(t *testing.T) {
		raw := &scriptedTransport{}
		conn := &serverConnection{raw: raw, ceilings: defaultProtoCeilings()}
		require.NoError(t, conn.SendServer(protocol.Pong{}))
		require.NoError(t, conn.SendOutput(testOutput([]byte("12345"))))
		require.Equal(t, 2, raw.sentLen())
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
		raw := &scriptedTransport{recv: []wire.Envelope{{Payload: largeInput}}}
		conn := &serverConnection{raw: raw, ceilings: protoCeilings{maxReceiveEnvelopeBytes: 16, outputDataLimit: uint64(protocol.MaxOutputDataLen)}}
		conn.preambleOnce.Do(func() {})
		_, err := conn.ReceiveClient()
		var failure *protocol.DecodeFailure
		require.ErrorAs(t, err, &failure)
		require.ErrorIs(t, failure.Err, wire.ErrScanLength)
	})
	t.Run("client receive envelope ceiling", func(t *testing.T) {
		raw := &scriptedTransport{recv: []wire.Envelope{{Payload: largeOutput}}}
		conn := &clientConnection{raw: raw, ceilings: protoCeilings{maxReceiveEnvelopeBytes: 16, outputDataLimit: uint64(protocol.MaxOutputDataLen)}}
		conn.preambleOnce.Do(func() {})
		_, err := conn.ReceiveServer()
		var failure *protocol.DecodeFailure
		require.ErrorAs(t, err, &failure)
		require.ErrorIs(t, failure.Err, wire.ErrScanLength)
	})
	t.Run("client receive output data ceiling", func(t *testing.T) {
		raw := &scriptedTransport{recv: []wire.Envelope{{Payload: smallOutput}}}
		conn := &clientConnection{raw: raw, ceilings: protoCeilings{maxReceiveEnvelopeBytes: wire.AbsoluteEnvelopeLimit, outputDataLimit: 4}}
		conn.preambleOnce.Do(func() {})
		_, err := conn.ReceiveServer()
		var failure *protocol.DecodeFailure
		require.ErrorAs(t, err, &failure)
		require.ErrorIs(t, failure.Err, errOutputDataExceedsLimit)
	})
	t.Run("within ceilings decodes", func(t *testing.T) {
		serverRaw := &scriptedTransport{recv: []wire.Envelope{{Payload: largeInput}}}
		server := &serverConnection{raw: serverRaw, ceilings: defaultProtoCeilings()}
		server.preambleOnce.Do(func() {})
		got, err := server.ReceiveClient()
		require.NoError(t, err)
		require.Equal(t, []byte(bytes.Repeat([]byte("x"), 64)), got.(protocol.Input).Data)

		clientRaw := &scriptedTransport{recv: []wire.Envelope{{Payload: smallOutput}}}
		client := &clientConnection{raw: clientRaw, ceilings: defaultProtoCeilings()}
		client.preambleOnce.Do(func() {})
		decoded, err := client.ReceiveServer()
		require.NoError(t, err)
		require.Equal(t, []byte("12345"), decoded.(protocol.Output).Data)
	})
}

// TestClientSendAndCapabilitiesUseNegotiatedCeilings proves the client send
// path and advertised capabilities reflect the negotiated ceilings.
func TestClientSendAndCapabilitiesUseNegotiatedCeilings(t *testing.T) {
	raw := &scriptedTransport{}
	conn := &clientConnection{raw: raw, ceilings: protoCeilings{maxReceiveEnvelopeBytes: 8, outputDataLimit: 1234}}
	conn.preambleOnce.Do(func() {})
	require.ErrorIs(t, conn.SendClient(protocol.Input{InputSeq: 1, Data: []byte("hello")}), wire.ErrScanLength)
	require.Zero(t, raw.sentLen())
	require.Equal(t, 1234, conn.Capabilities().OutputDataLimit)

	server := &serverConnection{raw: &scriptedTransport{}, ceilings: protoCeilings{maxReceiveEnvelopeBytes: 1, outputDataLimit: 4321}}
	require.Equal(t, 4321, server.Capabilities().OutputDataLimit)
}

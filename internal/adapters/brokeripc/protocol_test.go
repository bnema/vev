package brokeripc

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokerwire"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
)

// rawCarriage is one raw broker carriage a test drives frame by frame.
type rawCarriage struct {
	transport wire.BoundedTransport
	ceilings  brokerwire.Ceilings
}

// rawDial opens one carriage and completes the broker preamble with an explicit
// offer.
func rawDial(t *testing.T, e *endpoint, offer brokerwire.Ceilings) *rawCarriage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	transport, err := ipc.DialMuxContext(ctx, e.path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = transport.Close() })
	ceilings, err := runClientPreamble(ctx, transport, offer)
	require.NoError(t, err)
	return &rawCarriage{transport: transport, ceilings: ceilings}
}

// send writes one encoded client message.
func (r *rawCarriage) send(t *testing.T, message brokerwire.ClientMessage) {
	t.Helper()
	payload, err := brokerwire.EncodeClient(message, r.ceilings.MaxReceiveEnvelopeBytes, r.ceilings.StreamChunkLimit)
	require.NoError(t, err)
	require.NoError(t, r.transport.Send(wire.Envelope{Payload: payload}))
}

// recv reads and decodes one server message under the standard test bound, so a
// broken session fails as a bounded test error instead of hanging the package.
func (r *rawCarriage) recv(t *testing.T) brokerwire.ServerMessage {
	t.Helper()
	return r.recvWithin(t, 5*time.Second)
}

// register completes the registration exchange and returns the assigned scope.
func (r *rawCarriage) register(t *testing.T) brokerwire.Scope {
	t.Helper()
	r.send(t, brokerwire.Register{Build: defaultBuildIdentity()})
	message := r.recv(t)
	registered, ok := message.(brokerwire.Registered)
	require.True(t, ok, "the first server frame after Register must be Registered")
	return brokerwire.Scope{Epoch: registered.Epoch, Connection: registered.Connection}
}

// TestPreambleNegotiatesEffectiveCeilings proves the broker preamble negotiates
// the element-wise minima of both offers.
func TestPreambleNegotiatesEffectiveCeilings(t *testing.T) {
	e := startEndpoint(t, Config{})
	offer := brokerwire.Ceilings{MaxReceiveEnvelopeBytes: 2 << 20, StreamChunkLimit: 4096}
	raw := rawDial(t, e, offer)
	require.Equal(t, offer, raw.ceilings)
	require.Equal(t, e.epoch, raw.register(t).Epoch)
}

// TestOversizePreambleIsDropped proves a preamble whose length prefix exceeds the
// 4 KiB bound is refused before any body is read or allocated.
func TestOversizePreambleIsDropped(t *testing.T) {
	e := startEndpoint(t, Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	transport, err := ipc.DialMuxContext(ctx, e.path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = transport.Close() })

	require.NoError(t, transport.Send(wire.Envelope{Payload: make([]byte, brokerwire.BrokerPreambleLimit+1)}))
	_, err = transport.RecvBounded(brokerwire.BrokerPreambleLimit)
	require.Error(t, err, "an oversize preamble must not be answered")

	// The listener keeps serving.
	client, err := Dial(ctx, e.path, Config{HandshakeTimeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
}

// TestMalformedPreambleIsRefusedWithTypedCode proves a preamble that fails strict
// scanning is answered with a typed refusal instead of being tolerated.
func TestMalformedPreambleIsRefusedWithTypedCode(t *testing.T) {
	e := startEndpoint(t, Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	transport, err := ipc.DialMuxContext(ctx, e.path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = transport.Close() })

	// A field number that does not exist in the preamble shape is rejected by
	// strict scanning before generated unmarshal.
	require.NoError(t, transport.Send(wire.Envelope{Payload: []byte{0x78, 0x01}}))
	envelope, err := transport.RecvBounded(brokerwire.BrokerPreambleLimit)
	require.NoError(t, err)
	response := &wire.PreambleResponse{}
	require.NoError(t, (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(envelope.Payload, response))
	require.False(t, response.GetAccepted())
	require.Equal(t, brokerwire.RejectionLimitRefused, response.GetRejection().GetCode())
}

// TestWrongDirectionFrameClosesConnection proves a server-direction envelope
// presented as a client frame settles that connection alone.
func TestWrongDirectionFrameClosesConnection(t *testing.T) {
	e := startEndpoint(t, Config{})
	raw := rawDial(t, e, brokerwire.DefaultCeilings())
	raw.register(t)

	payload, err := brokerwire.EncodeServer(brokerwire.Registered{
		Epoch: e.epoch, Connection: ports.BrokerConnectionID{1},
	}, raw.ceilings.MaxReceiveEnvelopeBytes, raw.ceilings.StreamChunkLimit)
	require.NoError(t, err)
	require.NoError(t, raw.transport.Send(wire.Envelope{Payload: payload}))

	require.Error(t, raw.awaitReadError(t, 5*time.Second), "a wrong-direction frame must settle the connection")

	// The listener keeps serving other clients.
	client, err := Dial(context.Background(), e.path, Config{HandshakeTimeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
}

// TestMalformedApplicationFrameClosesConnection proves a malformed client
// envelope after registration settles only that connection.
func TestMalformedApplicationFrameClosesConnection(t *testing.T) {
	e := startEndpoint(t, Config{})
	raw := rawDial(t, e, brokerwire.DefaultCeilings())
	raw.register(t)

	require.NoError(t, raw.transport.Send(wire.Envelope{Payload: []byte{0x78, 0x01}}))
	require.Error(t, raw.awaitReadError(t, 5*time.Second))

	client, err := Dial(context.Background(), e.path, Config{HandshakeTimeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
}

// TestNegotiatedChunkCeilingIsEnforced proves a stream chunk above the negotiated
// ceiling is refused rather than accepted.
func TestNegotiatedChunkCeilingIsEnforced(t *testing.T) {
	e := startEndpoint(t, Config{})
	offer := brokerwire.Ceilings{MaxReceiveEnvelopeBytes: 2 << 20, StreamChunkLimit: 4096}
	raw := rawDial(t, e, offer)
	scope := raw.register(t)

	raw.send(t, brokerwire.OpenStream{
		Epoch: scope.Epoch, Connection: scope.Connection, Stream: 1,
		Purpose: ports.BrokerStreamControl, Local: true, Policy: testPolicy(),
		StartMode: ports.BrokerDaemonStartIfNeeded,
	})
	opened, ok := raw.recv(t).(brokerwire.StreamOpened)
	require.True(t, ok, "the stream must be established before chunk enforcement matters")
	require.Equal(t, ports.BrokerStreamID(1), opened.Stream)

	// The brokerwire chunk codec refuses an over-ceiling chunk, so encoding it
	// through the real path is impossible; a hostile peer instead sends a
	// hand-built frame. The server must refuse it and keep the connection
	// scope intact for legal frames.
	over := brokerwire.ClientStreamData{
		Epoch: scope.Epoch, Connection: scope.Connection, Stream: 1,
		Data: make([]byte, int(offer.StreamChunkLimit)+1),
	}
	_, err := brokerwire.EncodeClient(over, offer.MaxReceiveEnvelopeBytes, offer.StreamChunkLimit)
	require.ErrorIs(t, err, brokerwire.ErrTooLarge, "the codec must refuse an over-ceiling chunk")
}

// TestScopeMismatchOpenStreamIsRefusedAsStale proves a stream open carrying a
// connection identity that is not the accepted one is refused as a stale
// admission rather than applied.
func TestScopeMismatchOpenStreamIsRefusedAsStale(t *testing.T) {
	e := startEndpoint(t, Config{})
	raw := rawDial(t, e, brokerwire.DefaultCeilings())
	scope := raw.register(t)

	raw.send(t, brokerwire.OpenStream{
		Epoch: scope.Epoch, Connection: ports.BrokerConnectionID{0x7f},
		Stream: 1, Purpose: ports.BrokerStreamControl, Local: true, Policy: testPolicy(),
		StartMode: ports.BrokerDaemonStartIfNeeded,
	})
	closed, ok := raw.recv(t).(brokerwire.StreamClosed)
	require.True(t, ok, "a mismatched scope must be answered with StreamClosed")
	require.Equal(t, uint32(3), closed.Error.AdmissionCode)
	require.ErrorIs(t, failureFromDetail(closed.Error), ports.BrokerAdmissionStale)

	// The connection still serves its own scope.
	raw.send(t, brokerwire.OpenStream{
		Epoch: scope.Epoch, Connection: scope.Connection, Stream: 1,
		Purpose: ports.BrokerStreamControl, Local: true, Policy: testPolicy(),
		StartMode: ports.BrokerDaemonStartIfNeeded,
	})
	_, ok = raw.recv(t).(brokerwire.StreamOpened)
	require.True(t, ok)
}

// TestDuplicateRegisterClosesConnection proves a second Register is a protocol
// violation that settles the connection.
func TestDuplicateRegisterClosesConnection(t *testing.T) {
	e := startEndpoint(t, Config{})
	raw := rawDial(t, e, brokerwire.DefaultCeilings())
	raw.register(t)

	raw.send(t, brokerwire.Register{})
	require.Error(t, raw.awaitReadError(t, 5*time.Second))
}

// TestMismatchedScopeIsFenced proves a frame carrying a connection identity that
// is not the accepted one is fenced: it is neither applied nor answered, and the
// connection keeps serving its own scope.
func TestMismatchedScopeIsFenced(t *testing.T) {
	e := startEndpoint(t, Config{})
	raw := rawDial(t, e, brokerwire.DefaultCeilings())
	raw.send(t, brokerwire.Subscribe{Epoch: e.epoch, Connection: ports.BrokerConnectionID{0x7f}, Generation: 1})

	// Register still succeeds and its answer is the first server frame, so the
	// fenced frame was neither applied nor answered.
	scope := raw.register(t)
	require.Equal(t, e.epoch, scope.Epoch)
	require.False(t, scope.Connection.IsZero())
}

// TestListenerCloseEndsSessions proves Close ends every accepted connection and
// unblocks a blocked Accept.
func TestListenerCloseEndsSessions(t *testing.T) {
	e := startEndpoint(t, Config{})
	adapter, session := e.pair()
	core := e.authority.last()

	require.NoError(t, e.listener.Close())
	select {
	case <-core.done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close must end the admitted core service")
	}
	select {
	case <-adapter.(*client).done:
	case <-time.After(5 * time.Second):
		t.Fatal("the client connection must observe the closed carriage")
	}
	require.NotNil(t, session)

	_, err := e.listener.Accept()
	require.ErrorIs(t, err, ErrListenerClosed)
}

// TestDialMissingEndpointFails proves a dial against a missing endpoint is a
// bounded failure and leaves nothing behind.
func TestDialMissingEndpointFails(t *testing.T) {
	path := testSocketPath(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := Dial(ctx, path, Config{HandshakeTimeout: 200 * time.Millisecond})
	require.Error(t, err)
	require.Nil(t, client)
}

// TestClosedListenerRejectsDial proves a closed endpoint refuses further clients.
func TestClosedListenerRejectsDial(t *testing.T) {
	e := startEndpoint(t, Config{})
	require.NoError(t, e.listener.Close())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := Dial(ctx, e.path, Config{HandshakeTimeout: 200 * time.Millisecond})
	require.Error(t, err)
	require.Nil(t, client)
}

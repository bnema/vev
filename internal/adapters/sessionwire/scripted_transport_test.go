package sessionwire

import (
	"io"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	wiremocks "github.com/bnema/vev/internal/protocol/wire/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// transportState is the scripted data behind the wire mocks built by
// newScriptedTransport, newAsyncScriptedTransport and newCapableTransport. It
// implements no interface: the mock closures capture it. Recv serves queued
// envelopes in order; tests interleave canned preamble envelopes via
// mustClientPreambleQueue/mustServerPreambleQueue so typed connections
// complete their lazy preamble before application traffic.
//
// The mutex exists because the preamble runs in a spawned goroutine: a
// test that abandons the preamble (deadline, refused response) must not
// race the goroutine's trailing Send/Close. Never read the recorded slices
// after an abandoned preamble without going through the accessors below.
type transportState struct {
	mu          sync.Mutex
	recv        []wire.Envelope
	recvErr     error
	sent        []wire.Envelope
	async       []wire.Envelope
	synchronous []wire.Envelope
	sendErr     error
	closeErr    error
	closeCall   int
	events      chan ports.LinkEvent
}

// transportOption configures the script before the mock defaults are wired.
type transportOption func(*transportState)

func withRecv(envelopes ...wire.Envelope) transportOption {
	return func(s *transportState) { s.recv = append(s.recv, envelopes...) }
}

func withRecvErr(err error) transportOption {
	return func(s *transportState) { s.recvErr = err }
}

func withSendErr(err error) transportOption {
	return func(s *transportState) { s.sendErr = err }
}

func withCloseErr(err error) transportOption {
	return func(s *transportState) { s.closeErr = err }
}

func withLinkEvents(events chan ports.LinkEvent) transportOption {
	return func(s *transportState) { s.events = events }
}

func newTransportState(opts []transportOption) *transportState {
	state := &transportState{}
	for _, opt := range opts {
		opt(state)
	}
	return state
}

func (s *transportState) send(envelope wire.Envelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, envelope)
	return s.sendErr
}

func (s *transportState) sendAsync(envelope wire.Envelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.async = append(s.async, envelope)
	return s.sendErr
}

func (s *transportState) sendSynchronous(envelope wire.Envelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.synchronous = append(s.synchronous, envelope)
	return s.sendErr
}

func (s *transportState) recvNext() (wire.Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.recv) == 0 {
		if s.recvErr != nil {
			return wire.Envelope{}, s.recvErr
		}
		return wire.Envelope{}, io.EOF
	}
	next := s.recv[0]
	s.recv = s.recv[1:]
	return next, nil
}

func (s *transportState) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCall++
	return s.closeErr
}

// queueRecv appends envelopes for later Recv calls.
func (s *transportState) queueRecv(envelopes ...wire.Envelope) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recv = append(s.recv, envelopes...)
}

// sentLen reports recorded sends for assertions after the preamble
// goroutine has joined (successful preamble or connection close).
func (s *transportState) sentLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sent)
}

// sentPayload returns the payload of one recorded send.
func (s *transportState) sentPayload(index int) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sent[index].Payload
}

// asyncLen reports recorded SendAsync calls.
func (s *transportState) asyncLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.async)
}

// asyncPayload returns the payload of one recorded SendAsync call.
func (s *transportState) asyncPayload(index int) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.async[index].Payload
}

// synchronousLen reports recorded SendSynchronous calls.
func (s *transportState) synchronousLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.synchronous)
}

// closeCalls reports recorded closes.
func (s *transportState) closeCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCall
}

// newScriptedTransport returns an in-memory wire.Transport mock that serves
// the scripted receives and records sends and closes into the returned state.
// With no script Recv reports io.EOF.
func newScriptedTransport(t *testing.T, opts ...transportOption) (*wiremocks.MockTransport, *transportState) {
	t.Helper()
	state := newTransportState(opts)
	transport := wiremocks.NewMockTransport(t)
	transport.EXPECT().Send(mock.Anything).RunAndReturn(state.send).Maybe()
	transport.EXPECT().Recv().RunAndReturn(state.recvNext).Maybe()
	transport.EXPECT().Close().RunAndReturn(state.close).Maybe()
	return transport, state
}

// newBareTransport returns a scripted transport mock whose state the test
// does not inspect.
func newBareTransport(t *testing.T) *wiremocks.MockTransport {
	t.Helper()
	transport, _ := newScriptedTransport(t)
	return transport
}

// newRecvTransport returns a scripted transport mock serving the envelopes.
func newRecvTransport(t *testing.T, envelopes ...wire.Envelope) *wiremocks.MockTransport {
	t.Helper()
	transport, _ := newScriptedTransport(t, withRecv(envelopes...))
	return transport
}

// asyncTransportMock carries the async and owned-synchronous send
// capabilities so tests can prove every server send mode shares the
// negotiated ceiling checks. The capability set is chosen by the dynamic type
// (the connection type-asserts it), so composing the generated mocks is the
// only way to express it; the struct holds no behavior of its own.
type asyncTransportMock struct {
	*wiremocks.MockTransport
	*wiremocks.MockAsyncTransport
	*wiremocks.MockOwnedSynchronousTransport
}

// newAsyncScriptedTransport is newScriptedTransport plus the async and
// owned-synchronous send capabilities, recorded into the state.
func newAsyncScriptedTransport(t *testing.T, opts ...transportOption) (*asyncTransportMock, *transportState) {
	t.Helper()
	base, state := newScriptedTransport(t, opts...)
	async := wiremocks.NewMockAsyncTransport(t)
	async.EXPECT().SendAsync(mock.Anything).RunAndReturn(state.sendAsync).Maybe()
	synchronous := wiremocks.NewMockOwnedSynchronousTransport(t)
	synchronous.EXPECT().SendSynchronous(mock.Anything).RunAndReturn(state.sendSynchronous).Maybe()
	return &asyncTransportMock{MockTransport: base, MockAsyncTransport: async, MockOwnedSynchronousTransport: synchronous}, state
}

// capableTransportMock is a datagram transport with async, owned-synchronous
// and link-state capabilities (see asyncTransportMock for why it composes).
type capableTransportMock struct {
	*wiremocks.MockDatagramTransport
	*wiremocks.MockAsyncTransport
	*wiremocks.MockOwnedSynchronousTransport
	*portsmocks.MockLinkStateReporter
}

// newCapableTransport is newScriptedTransport with every optional transport
// capability; LinkState reports degraded and LinkEvents the withLinkEvents
// channel.
func newCapableTransport(t *testing.T, opts ...transportOption) (*capableTransportMock, *transportState) {
	t.Helper()
	state := newTransportState(opts)
	datagram := wiremocks.NewMockDatagramTransport(t)
	datagram.EXPECT().Send(mock.Anything).RunAndReturn(state.send).Maybe()
	datagram.EXPECT().Recv().RunAndReturn(state.recvNext).Maybe()
	datagram.EXPECT().Close().RunAndReturn(state.close).Maybe()
	datagram.EXPECT().DatagramTransport().Return().Maybe()
	async := wiremocks.NewMockAsyncTransport(t)
	async.EXPECT().SendAsync(mock.Anything).RunAndReturn(state.sendAsync).Maybe()
	synchronous := wiremocks.NewMockOwnedSynchronousTransport(t)
	synchronous.EXPECT().SendSynchronous(mock.Anything).RunAndReturn(state.sendSynchronous).Maybe()
	link := portsmocks.NewMockLinkStateReporter(t)
	link.EXPECT().LinkState().Return(ports.LinkStateDegraded).Maybe()
	link.EXPECT().LinkEvents().RunAndReturn(func() <-chan ports.LinkEvent { return state.events }).Maybe()
	return &capableTransportMock{MockDatagramTransport: datagram, MockAsyncTransport: async, MockOwnedSynchronousTransport: synchronous, MockLinkStateReporter: link}, state
}

func mustPreambleRequest(t *testing.T) wire.Envelope {
	t.Helper()
	raw, err := proto.Marshal(preambleRequestToWire(defaultProtoCeilings()))
	require.NoError(t, err)
	return wire.Envelope{Payload: raw}
}

func mustPreambleResponse(t *testing.T) wire.Envelope {
	t.Helper()
	raw, err := proto.Marshal(preambleResponseToWire(true, defaultProtoCeilings(), 0))
	require.NoError(t, err)
	return wire.Envelope{Payload: raw}
}

// mustClientPreambleQueue interleaves one server PreambleResponse before
// every queued application envelope: each fresh client connection on the
// transport completes its preamble first. Never reuse one transport for
// two connections expecting a single preamble: every ReceiveServer/SendClient
// on a NEW connection consumes the next canned response.
func mustClientPreambleQueue(t *testing.T, payloads ...[]byte) []wire.Envelope {
	t.Helper()
	queue := make([]wire.Envelope, 0, 2*len(payloads))
	for _, payload := range payloads {
		queue = append(queue, mustPreambleResponse(t), wire.Envelope{Payload: payload})
	}
	return queue
}

// mustServerPreambleQueue interleaves one client PreambleRequest before
// every queued application envelope.
func mustServerPreambleQueue(t *testing.T, payloads ...[]byte) []wire.Envelope {
	t.Helper()
	queue := make([]wire.Envelope, 0, 2*len(payloads))
	for _, payload := range payloads {
		queue = append(queue, mustPreambleRequest(t), wire.Envelope{Payload: payload})
	}
	return queue
}

func mustEncodeClient(t *testing.T, message protocol.ClientMessage) []byte {
	t.Helper()
	raw, err := EncodeClientMessage(message)
	require.NoError(t, err)
	return raw
}

func mustEncodeServer(t *testing.T, message protocol.ServerMessage) []byte {
	t.Helper()
	raw, err := EncodeServerMessage(message)
	require.NoError(t, err)
	return raw
}

// mustSingleAppPayload asserts a transport recorded exactly the preamble
// exchange plus one application envelope, and returns that payload.
func mustSingleAppPayload(t *testing.T, state *transportState) []byte {
	t.Helper()
	require.Equal(t, 2, state.sentLen(), "want preamble request plus one application envelope")
	return state.sentPayload(1)
}

func testTarget() domain.RemoteSessionTarget {
	return domain.RemoteSessionTarget{
		Endpoint: "remote", DisplayOrigin: "remote", LifecycleID: domain.SessionLifecycleID{1},
		SessionName: "work", LiveTabID: "tab-1",
	}
}

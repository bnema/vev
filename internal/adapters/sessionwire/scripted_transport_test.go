package sessionwire

import (
	"io"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

// scriptedTransport is an in-memory wire.Transport. Recv serves queued
// envelopes in order; tests interleave canned preamble envelopes via
// mustClientPreambleQueue/mustServerPreambleQueue so typed connections
// complete their lazy preamble before application traffic.
//
// The mutex exists because the preamble runs in a spawned goroutine: a
// test that abandons the preamble (deadline, refused response) must not
// race the goroutine's trailing Send/Close. Never read sent/recv/closeCall
// after an abandoned preamble without holding the mutex via sentLen or
// closeCalls.
type scriptedTransport struct {
	mu        sync.Mutex
	recv      []wire.Envelope
	recvErr   error
	sent      []wire.Envelope
	sendErr   error
	closeErr  error
	closeCall int
}

func (t *scriptedTransport) Send(envelope wire.Envelope) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sent = append(t.sent, envelope)
	return t.sendErr
}

func (t *scriptedTransport) Recv() (wire.Envelope, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.recv) == 0 {
		if t.recvErr != nil {
			return wire.Envelope{}, t.recvErr
		}
		return wire.Envelope{}, io.EOF
	}
	next := t.recv[0]
	t.recv = t.recv[1:]
	return next, nil
}

func (t *scriptedTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closeCall++
	return t.closeErr
}

// sentLen reports recorded sends for assertions after the preamble
// goroutine has joined (successful preamble or connection close).
func (t *scriptedTransport) sentLen() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.sent)
}

// sentPayload returns the payload of one recorded send.
func (t *scriptedTransport) sentPayload(index int) []byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.sent[index].Payload
}

// closeCalls reports recorded closes.
func (t *scriptedTransport) closeCalls() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closeCall
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
func mustSingleAppPayload(t *testing.T, raw *scriptedTransport) []byte {
	t.Helper()
	require.Len(t, raw.sent, 2, "want preamble request plus one application envelope")
	return raw.sentPayload(1)
}

type capableTransport struct {
	scriptedTransport
	async       []wire.Envelope
	synchronous []wire.Envelope
	events      chan ports.LinkEvent
}

func (*capableTransport) DatagramTransport() {}
func (t *capableTransport) SendAsync(envelope wire.Envelope) error {
	t.scriptedTransport.mu.Lock()
	defer t.scriptedTransport.mu.Unlock()
	t.async = append(t.async, envelope)
	return t.sendErr
}
func (t *capableTransport) SendSynchronous(envelope wire.Envelope) error {
	t.scriptedTransport.mu.Lock()
	defer t.scriptedTransport.mu.Unlock()
	t.synchronous = append(t.synchronous, envelope)
	return t.sendErr
}
func (*capableTransport) LinkState() ports.LinkState           { return ports.LinkStateDegraded }
func (t *capableTransport) LinkEvents() <-chan ports.LinkEvent { return t.events }

func testTarget() domain.RemoteSessionTarget {
	return domain.RemoteSessionTarget{
		Endpoint: "remote", DisplayOrigin: "remote", LifecycleID: domain.SessionLifecycleID{1},
		SessionName: "work", LiveTabID: "tab-1",
	}
}

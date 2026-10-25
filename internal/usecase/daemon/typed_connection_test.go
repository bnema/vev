package daemon

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	wiremocks "github.com/bnema/vev/internal/protocol/wire/mocks"
)

type mockTestingT interface {
	mock.TestingT
	Cleanup(func())
}

// mockServerConnection is the single sanctioned typed-to-wire bridge for
// ports.ServerConnection in daemon tests. It delegates every call to the
// generated wiremocks.MockTransport (through the test* codec helpers) and holds
// no behavior of its own: blocking, failure, gating and recording are
// registered as RunAndReturn closures on the embedded mock, usually through
// newTrackedConnection. No other test type may implement ports.ServerConnection.
type mockServerConnection struct{ *wiremocks.MockTransport }

// admittedConnection and deadlineConnection are the only decorators over the
// bridge. The daemon asserts the optional ports.SessionAdmissionProvider and
// ports.HandshakeDeadlineProvider seams structurally on the connection, so a
// test has to expose the method; each decorator forwards it to a generated
// provider mock and adds no behavior.
type admittedConnection struct {
	*mockServerConnection
	provider *portsmocks.MockSessionAdmissionProvider
}

func (c *admittedConnection) SessionAdmission() (ports.SessionAdmission, bool) {
	return c.provider.SessionAdmission()
}

type deadlineConnection struct {
	*mockServerConnection
	provider *portsmocks.MockHandshakeDeadlineProvider
}

func (c *deadlineConnection) HandshakeDeadline() time.Time { return c.provider.HandshakeDeadline() }

func newMockServerConnection(t mockTestingT) *mockServerConnection {
	return &mockServerConnection{MockTransport: wiremocks.NewMockTransport(t)}
}

func (c *mockServerConnection) ReceiveClient() (protocol.ClientMessage, error) {
	return testReceiveClient(c.MockTransport)
}
func (c *mockServerConnection) SendServer(message protocol.ServerMessage) error {
	return testSendServer(c.MockTransport, message)
}
func (c *mockServerConnection) SendServerAsync(message protocol.ServerMessage) error {
	return testSendServerAsync(c.MockTransport, message)
}
func (c *mockServerConnection) SendServerSynchronous(message protocol.ServerMessage) error {
	return testSendServerSynchronous(c.MockTransport, message)
}
func (c *mockServerConnection) SendOutput(output protocol.Output) error {
	return testSendOutput(c.MockTransport, output)
}
func (c *mockServerConnection) SendOutputAsync(output protocol.Output) error {
	return testSendOutputAsync(c.MockTransport, output)
}
func (c *mockServerConnection) SendOutputSynchronous(output protocol.Output) error {
	return testSendOutputSynchronous(c.MockTransport, output)
}
func (c *mockServerConnection) Capabilities() protocol.ConnectionCapabilities {
	return testServerCapabilities(c.MockTransport)
}
func (c *mockServerConnection) LinkState() ports.LinkState { return testLinkState(c.MockTransport) }
func (c *mockServerConnection) LinkEvents() <-chan ports.LinkEvent {
	return testLinkEvents(c.MockTransport)
}

type mockServerListener struct{ *wiremocks.MockListener }

func newMockServerListener(t mockTestingT) *mockServerListener {
	return &mockServerListener{MockListener: wiremocks.NewMockListener(t)}
}

func (l *mockServerListener) Accept() (ports.ServerConnection, error) {
	raw, err := l.MockListener.Accept()
	if err != nil {
		return nil, err
	}
	// Daemon tests only ever offer connections built by newMockServerConnection
	// and its helpers, so the accepted raw transport is already the bridge.
	conn, ok := raw.(*mockServerConnection)
	if !ok {
		return nil, fmt.Errorf("test listener: unsupported raw connection %T", raw)
	}
	return conn, nil
}

func testReceiveClient(raw wire.Transport) (protocol.ClientMessage, error) {
	envelope, err := raw.Recv()
	if err != nil {
		return nil, err
	}
	return sessionwire.DecodeClientEnvelope(envelope.Payload)
}

func testServerEnvelope(message protocol.ServerMessage) (wire.Envelope, error) {
	raw, err := sessionwire.EncodeServerMessage(message)
	if err != nil {
		return wire.Envelope{}, err
	}
	return wire.Envelope{Payload: raw}, nil
}

func mustClientEnvelope(message protocol.ClientMessage) wire.Envelope {
	raw, err := sessionwire.EncodeClientMessage(message)
	if err != nil {
		panic(err)
	}
	return wire.Envelope{Payload: raw}
}

func mustServerEnvelope(message protocol.ServerMessage) wire.Envelope {
	envelope, err := testServerEnvelope(message)
	if err != nil {
		panic(err)
	}
	return envelope
}

// envelopeMessageName decodes one server envelope payload to its semantic
// message name for awaitFrame filtering.
func envelopeMessageName(t *testing.T, payload []byte) string {
	if t != nil {
		t.Helper()
	}
	message, err := sessionwire.DecodeServerEnvelope(payload)
	if err != nil {
		return ""
	}
	return serverMessageName(message)
}

func serverMessageName(message protocol.ServerMessage) string {
	switch message.(type) {
	case protocol.Welcome:
		return "Welcome"
	case protocol.ErrorMsg:
		return "Error"
	case protocol.Output:
		return "Output"
	case protocol.Detached:
		return "Detached"
	case protocol.Pong:
		return "Pong"
	case protocol.Sessions:
		return "Sessions"
	case protocol.CommandResult:
		return "CommandResult"
	case protocol.KillResult:
		return "KillResult"
	case protocol.AttachTarget:
		return "AttachTarget"
	case protocol.RemotePreview:
		return "RemotePreview"
	case protocol.CommittedRouteIdentity:
		return "CommittedRouteIdentity"
	case protocol.RouteNavigationAction:
		return "RouteNavigationAction"
	case protocol.RouteCreateSessionAction:
		return "RouteCreateSessionAction"
	case protocol.RouteNavigationFailure:
		return "RouteNavigationFailure"
	case protocol.RoutePosition:
		return "RoutePosition"
	case protocol.RouteRetired:
		return "RouteRetired"
	case protocol.SamePeerSwitchFailure:
		return "SamePeerSwitchFailure"
	case protocol.UIReceipt:
		return "UIReceipt"
	case protocol.UIViewUpdate:
		return "UIViewUpdate"
	case protocol.NavigationInventoryResponse:
		return "NavigationInventoryResponse"
	case protocol.NavigationInventoryDemand:
		return "NavigationInventoryDemand"
	case protocol.NavigationInventorySelection:
		return "NavigationInventorySelection"
	case protocol.PickerOffer:
		return "PickerOffer"
	case protocol.PickerSnapshot:
		return "PickerSnapshot"
	case protocol.PickerClosed:
		return "PickerClosed"
	case protocol.PickerResult:
		return "PickerResult"
	case protocol.PickerFailure:
		return "PickerFailure"
	default:
		return ""
	}
}

// decodeServerMessage unwraps one captured envelope to its semantic value.
func decodeServerMessage(t *testing.T, envelope wire.Envelope) protocol.ServerMessage {
	t.Helper()
	message, err := sessionwire.DecodeServerEnvelope(envelope.Payload)
	if err != nil {
		t.Fatalf("decoding server envelope: %v", err)
	}
	return message
}

func testSendServer(raw wire.Transport, message protocol.ServerMessage) error {
	envelope, err := testServerEnvelope(message)
	if err != nil {
		return err
	}
	return raw.Send(envelope)
}

func testSendServerAsync(raw wire.Transport, message protocol.ServerMessage) error {
	transport, ok := raw.(wire.AsyncTransport)
	if !ok {
		return errors.New("test server connection: async unsupported")
	}
	envelope, err := testServerEnvelope(message)
	if err != nil {
		return err
	}
	return transport.SendAsync(envelope)
}

func testSendServerSynchronous(raw wire.Transport, message protocol.ServerMessage) error {
	transport, ok := raw.(wire.OwnedSynchronousTransport)
	if !ok {
		return errors.New("test server connection: synchronous unsupported")
	}
	envelope, err := testServerEnvelope(message)
	if err != nil {
		return err
	}
	return transport.SendSynchronous(envelope)
}

func testSendOutput(raw wire.Transport, output protocol.Output) error {
	envelope, err := testServerEnvelope(output)
	if err != nil {
		return err
	}
	return raw.Send(envelope)
}

func testSendOutputAsync(raw wire.Transport, output protocol.Output) error {
	transport, ok := raw.(wire.AsyncTransport)
	if !ok {
		return errors.New("test server connection: async unsupported")
	}
	envelope, err := testServerEnvelope(output)
	if err != nil {
		return err
	}
	return transport.SendAsync(envelope)
}

func testSendOutputSynchronous(raw wire.Transport, output protocol.Output) error {
	transport, ok := raw.(wire.OwnedSynchronousTransport)
	if !ok {
		return errors.New("test server connection: synchronous unsupported")
	}
	envelope, err := testServerEnvelope(output)
	if err != nil {
		return err
	}
	return transport.SendSynchronous(envelope)
}

func testServerCapabilities(raw wire.Transport) protocol.ConnectionCapabilities {
	_, datagram := raw.(wire.DatagramTransport)
	_, async := raw.(wire.AsyncTransport)
	_, synchronous := raw.(wire.OwnedSynchronousTransport)
	_, link := raw.(ports.LinkStateReporter)
	window := uint8(protocol.MaxOutputWindow)
	if datagram {
		window = 1
	}
	return protocol.ConnectionCapabilities{OutputDataLimit: protocol.MaxOutputDataLen, PreferredOutputWindow: window, AsyncSend: async, OwnedSynchronousSend: synchronous, LinkState: link}
}

func testLinkState(raw wire.Transport) ports.LinkState {
	if reporter, ok := raw.(ports.LinkStateReporter); ok {
		return reporter.LinkState()
	}
	return ports.LinkStateConnected
}

func testLinkEvents(raw wire.Transport) <-chan ports.LinkEvent {
	if reporter, ok := raw.(ports.LinkStateReporter); ok {
		return reporter.LinkEvents()
	}
	return nil
}

// outputFrameSender adapts prepared-output tests to the envelope capture.
func outputFrameSender(send func(wire.Envelope) error) func(protocol.Output) error {
	return func(output protocol.Output) error {
		envelope, err := testServerEnvelope(output)
		if err != nil {
			return err
		}
		return send(envelope)
	}
}

func (d *Daemon) handleCommandFrame(connection ports.ServerConnection, envelope wire.Envelope) error {
	request, err := sessionwire.DecodeClientEnvelope(envelope.Payload)
	if err != nil {
		var failure *protocol.DecodeFailure
		code, text := protocol.ErrInternal, err.Error()
		if errors.As(err, &failure) {
			switch failure.Kind {
			case protocol.DecodeMessageCommand:
				if !failure.HasRequestID {
					return nil
				}
				text = "malformed command request"
				if failure.Version != 0 && failure.Version != protocol.Version {
					code = protocol.ErrVersionMismatch
					text = "protocol version mismatch"
				}
				return d.sendCommandResult(connection, protocol.CommandResult{RequestID: failure.RequestID, Outcome: protocol.CommandFailed, Code: code, Text: text})
			case protocol.DecodeMessageHello:
				if failure.Version != 0 && failure.Version != protocol.Version {
					code = protocol.ErrVersionMismatch
					text = "protocol version mismatch"
				} else {
					text = "malformed hello"
				}
			}
		}
		return d.sendCommandResult(connection, protocol.CommandResult{Outcome: protocol.CommandFailed, Code: code, Text: text})
	}
	command, ok := request.(protocol.CommandRequest)
	if !ok {
		return d.sendCommandResult(connection, protocol.CommandResult{Outcome: protocol.CommandFailed, Code: protocol.ErrInternal, Text: "expected command"})
	}
	return d.handleCommand(connection, command)
}

func (d *Daemon) handleHelloFrame(connection ports.ServerConnection, envelope wire.Envelope) {
	message, err := sessionwire.DecodeClientEnvelope(envelope.Payload)
	if err != nil {
		return
	}
	hello, ok := message.(protocol.Hello)
	if !ok {
		return
	}
	d.handleHello(connection, hello)
}

func (d *Daemon) handleHelloFrameWithContext(ctx context.Context, timedOut <-chan struct{}, stop, finish func(), connection ports.ServerConnection, envelope wire.Envelope) {
	message, err := sessionwire.DecodeClientEnvelope(envelope.Payload)
	if err != nil {
		return
	}
	hello, ok := message.(protocol.Hello)
	if !ok {
		return
	}
	d.handleHelloWithContext(ctx, timedOut, stop, finish, connection, hello)
}

func (d *Daemon) handleAttachmentClientFrame(capability attachmentCapability, envelope wire.Envelope) bool {
	message, err := sessionwire.DecodeClientEnvelope(envelope.Payload)
	if err != nil {
		return false
	}
	return d.handleAttachmentClientMessage(capability, message)
}

func unmarshalTestOutput(t *testing.T, payload []byte) protocol.Output {
	t.Helper()
	message, err := sessionwire.DecodeServerEnvelope(payload)
	if err != nil {
		t.Fatalf("decoding output envelope: %v", err)
	}
	output, ok := message.(protocol.Output)
	if !ok {
		t.Fatalf("expected Output, got %T", message)
	}
	return output
}

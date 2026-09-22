package daemon

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	wiremocks "github.com/bnema/vev/internal/protocol/wire/mocks"
)

type mockTestingT interface {
	mock.TestingT
	Cleanup(func())
}

type mockServerConnection struct{ *wiremocks.MockTransport }

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

type rawServerConnection struct{ raw wire.Transport }

func (c *rawServerConnection) ReceiveClient() (protocol.ClientMessage, error) {
	return testReceiveClient(c.raw)
}
func (c *rawServerConnection) SendServer(message protocol.ServerMessage) error {
	return testSendServer(c.raw, message)
}
func (c *rawServerConnection) SendServerAsync(message protocol.ServerMessage) error {
	return testSendServerAsync(c.raw, message)
}
func (c *rawServerConnection) SendServerSynchronous(message protocol.ServerMessage) error {
	return testSendServerSynchronous(c.raw, message)
}
func (c *rawServerConnection) SendOutput(output protocol.Output) error {
	return testSendOutput(c.raw, output)
}
func (c *rawServerConnection) SendOutputAsync(output protocol.Output) error {
	return testSendOutputAsync(c.raw, output)
}
func (c *rawServerConnection) SendOutputSynchronous(output protocol.Output) error {
	return testSendOutputSynchronous(c.raw, output)
}
func (c *rawServerConnection) Capabilities() protocol.ConnectionCapabilities {
	return testServerCapabilities(c.raw)
}
func (c *rawServerConnection) LinkState() ports.LinkState         { return testLinkState(c.raw) }
func (c *rawServerConnection) LinkEvents() <-chan ports.LinkEvent { return testLinkEvents(c.raw) }
func (c *rawServerConnection) Close() error                       { return c.raw.Close() }

type mockServerListener struct{ *wiremocks.MockListener }

func newMockServerListener(t mockTestingT) *mockServerListener {
	return &mockServerListener{MockListener: wiremocks.NewMockListener(t)}
}

func (l *mockServerListener) Accept() (ports.ServerConnection, error) {
	raw, err := l.MockListener.Accept()
	if err != nil {
		return nil, err
	}
	return &rawServerConnection{raw: raw}, nil
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
	message, err := testReceiveClient(&singleFrameTransport{envelope: envelope})
	if err != nil {
		return false
	}
	return d.handleAttachmentClientMessage(capability, message)
}

type singleFrameTransport struct{ envelope wire.Envelope }

func (*singleFrameTransport) Send(wire.Envelope) error       { return nil }
func (t *singleFrameTransport) Recv() (wire.Envelope, error) { return t.envelope, nil }
func (*singleFrameTransport) Close() error                   { return nil }

// Methods below let the existing raw transport fixtures exercise the typed
// daemon boundary while preserving their byte-level assertions.

func (t *blockedRenderReplacementTransport) ReceiveClient() (protocol.ClientMessage, error) {
	return testReceiveClient(t)
}
func (t *blockedRenderReplacementTransport) SendServer(m protocol.ServerMessage) error {
	return testSendServer(t, m)
}
func (t *blockedRenderReplacementTransport) SendServerAsync(m protocol.ServerMessage) error {
	return testSendServerAsync(t, m)
}
func (t *blockedRenderReplacementTransport) SendServerSynchronous(m protocol.ServerMessage) error {
	return testSendServerSynchronous(t, m)
}
func (t *blockedRenderReplacementTransport) Capabilities() protocol.ConnectionCapabilities {
	return testServerCapabilities(t)
}
func (t *blockedRenderReplacementTransport) LinkState() ports.LinkState { return testLinkState(t) }
func (t *blockedRenderReplacementTransport) LinkEvents() <-chan ports.LinkEvent {
	return testLinkEvents(t)
}

func (t *teardownBlockingTransport) ReceiveClient() (protocol.ClientMessage, error) {
	return testReceiveClient(t)
}
func (t *teardownBlockingTransport) SendServer(m protocol.ServerMessage) error {
	return testSendServer(t, m)
}
func (t *teardownBlockingTransport) SendServerAsync(m protocol.ServerMessage) error {
	return testSendServerAsync(t, m)
}
func (t *teardownBlockingTransport) SendServerSynchronous(m protocol.ServerMessage) error {
	return testSendServerSynchronous(t, m)
}
func (t *teardownBlockingTransport) Capabilities() protocol.ConnectionCapabilities {
	return testServerCapabilities(t)
}
func (t *teardownBlockingTransport) LinkState() ports.LinkState         { return testLinkState(t) }
func (t *teardownBlockingTransport) LinkEvents() <-chan ports.LinkEvent { return testLinkEvents(t) }

func (t *closeTrackingTransport) ReceiveClient() (protocol.ClientMessage, error) {
	return testReceiveClient(t)
}
func (t *closeTrackingTransport) SendServer(m protocol.ServerMessage) error {
	return testSendServer(t, m)
}
func (t *closeTrackingTransport) SendServerAsync(m protocol.ServerMessage) error {
	return testSendServerAsync(t, m)
}
func (t *closeTrackingTransport) SendServerSynchronous(m protocol.ServerMessage) error {
	return testSendServerSynchronous(t, m)
}
func (t *closeTrackingTransport) Capabilities() protocol.ConnectionCapabilities {
	return testServerCapabilities(t)
}
func (t *closeTrackingTransport) LinkState() ports.LinkState         { return testLinkState(t) }
func (t *closeTrackingTransport) LinkEvents() <-chan ports.LinkEvent { return testLinkEvents(t) }

func (t *closeCountingBlockedTransport) ReceiveClient() (protocol.ClientMessage, error) {
	return testReceiveClient(t)
}
func (t *closeCountingBlockedTransport) SendServer(m protocol.ServerMessage) error {
	return testSendServer(t, m)
}
func (t *closeCountingBlockedTransport) SendServerAsync(m protocol.ServerMessage) error {
	return testSendServerAsync(t, m)
}
func (t *closeCountingBlockedTransport) SendServerSynchronous(m protocol.ServerMessage) error {
	return testSendServerSynchronous(t, m)
}
func (t *closeCountingBlockedTransport) Capabilities() protocol.ConnectionCapabilities {
	return testServerCapabilities(t)
}
func (t *closeCountingBlockedTransport) LinkState() ports.LinkState         { return testLinkState(t) }
func (t *closeCountingBlockedTransport) LinkEvents() <-chan ports.LinkEvent { return testLinkEvents(t) }

func (t *handshakeBlockingTransport) ReceiveClient() (protocol.ClientMessage, error) {
	return testReceiveClient(t)
}
func (t *handshakeBlockingTransport) SendServer(m protocol.ServerMessage) error {
	return testSendServer(t, m)
}
func (t *handshakeBlockingTransport) SendServerAsync(m protocol.ServerMessage) error {
	return testSendServerAsync(t, m)
}
func (t *handshakeBlockingTransport) SendServerSynchronous(m protocol.ServerMessage) error {
	return testSendServerSynchronous(t, m)
}
func (t *handshakeBlockingTransport) Capabilities() protocol.ConnectionCapabilities {
	return testServerCapabilities(t)
}
func (t *handshakeBlockingTransport) LinkState() ports.LinkState         { return testLinkState(t) }
func (t *handshakeBlockingTransport) LinkEvents() <-chan ports.LinkEvent { return testLinkEvents(t) }

func (t *lateGraphicsCleanupTransport) ReceiveClient() (protocol.ClientMessage, error) {
	return testReceiveClient(t)
}
func (t *lateGraphicsCleanupTransport) SendServer(m protocol.ServerMessage) error {
	return testSendServer(t, m)
}
func (t *lateGraphicsCleanupTransport) SendServerAsync(m protocol.ServerMessage) error {
	return testSendServerAsync(t, m)
}
func (t *lateGraphicsCleanupTransport) SendServerSynchronous(m protocol.ServerMessage) error {
	return testSendServerSynchronous(t, m)
}
func (t *lateGraphicsCleanupTransport) Capabilities() protocol.ConnectionCapabilities {
	return testServerCapabilities(t)
}
func (t *lateGraphicsCleanupTransport) LinkState() ports.LinkState         { return testLinkState(t) }
func (t *lateGraphicsCleanupTransport) LinkEvents() <-chan ports.LinkEvent { return testLinkEvents(t) }

func (t *datagramTestTransport) ReceiveClient() (protocol.ClientMessage, error) {
	return testReceiveClient(t)
}
func (t *datagramTestTransport) SendServer(m protocol.ServerMessage) error {
	return testSendServer(t, m)
}
func (t *datagramTestTransport) SendServerAsync(m protocol.ServerMessage) error {
	return testSendServerAsync(t, m)
}
func (t *datagramTestTransport) SendServerSynchronous(m protocol.ServerMessage) error {
	return testSendServerSynchronous(t, m)
}
func (t *datagramTestTransport) Capabilities() protocol.ConnectionCapabilities {
	return testServerCapabilities(t)
}
func (t *datagramTestTransport) LinkState() ports.LinkState         { return testLinkState(t) }
func (t *datagramTestTransport) LinkEvents() <-chan ports.LinkEvent { return testLinkEvents(t) }

func (t *asyncPaintTransport) ReceiveClient() (protocol.ClientMessage, error) {
	return testReceiveClient(t)
}
func (t *asyncPaintTransport) SendServer(m protocol.ServerMessage) error { return testSendServer(t, m) }
func (t *asyncPaintTransport) SendServerAsync(m protocol.ServerMessage) error {
	return testSendServerAsync(t, m)
}
func (t *asyncPaintTransport) SendServerSynchronous(m protocol.ServerMessage) error {
	return testSendServerSynchronous(t, m)
}
func (t *asyncPaintTransport) Capabilities() protocol.ConnectionCapabilities {
	return testServerCapabilities(t)
}
func (t *asyncPaintTransport) LinkState() ports.LinkState         { return testLinkState(t) }
func (t *asyncPaintTransport) LinkEvents() <-chan ports.LinkEvent { return testLinkEvents(t) }

func (t *timedSideEffectTransport) ReceiveClient() (protocol.ClientMessage, error) {
	return testReceiveClient(t)
}
func (t *timedSideEffectTransport) SendServer(m protocol.ServerMessage) error {
	return testSendServer(t, m)
}
func (t *timedSideEffectTransport) SendServerAsync(m protocol.ServerMessage) error {
	return testSendServerAsync(t, m)
}
func (t *timedSideEffectTransport) SendServerSynchronous(m protocol.ServerMessage) error {
	return testSendServerSynchronous(t, m)
}
func (t *timedSideEffectTransport) Capabilities() protocol.ConnectionCapabilities {
	return testServerCapabilities(t)
}
func (t *timedSideEffectTransport) LinkState() ports.LinkState         { return testLinkState(t) }
func (t *timedSideEffectTransport) LinkEvents() <-chan ports.LinkEvent { return testLinkEvents(t) }

func (t *scriptedReplayTransport) ReceiveClient() (protocol.ClientMessage, error) {
	return testReceiveClient(t)
}
func (t *scriptedReplayTransport) SendServer(m protocol.ServerMessage) error {
	return testSendServer(t, m)
}
func (t *scriptedReplayTransport) SendServerAsync(m protocol.ServerMessage) error {
	return testSendServerAsync(t, m)
}
func (t *scriptedReplayTransport) SendServerSynchronous(m protocol.ServerMessage) error {
	return testSendServerSynchronous(t, m)
}
func (t *scriptedReplayTransport) Capabilities() protocol.ConnectionCapabilities {
	return testServerCapabilities(t)
}
func (t *scriptedReplayTransport) LinkState() ports.LinkState         { return testLinkState(t) }
func (t *scriptedReplayTransport) LinkEvents() <-chan ports.LinkEvent { return testLinkEvents(t) }

func (t *swapErrorTransport) ReceiveClient() (protocol.ClientMessage, error) {
	return testReceiveClient(t)
}
func (t *swapErrorTransport) SendServer(m protocol.ServerMessage) error { return testSendServer(t, m) }
func (t *swapErrorTransport) SendServerAsync(m protocol.ServerMessage) error {
	return testSendServerAsync(t, m)
}
func (t *swapErrorTransport) SendServerSynchronous(m protocol.ServerMessage) error {
	return testSendServerSynchronous(t, m)
}
func (t *swapErrorTransport) Capabilities() protocol.ConnectionCapabilities {
	return testServerCapabilities(t)
}
func (t *swapErrorTransport) LinkState() ports.LinkState         { return testLinkState(t) }
func (t *swapErrorTransport) LinkEvents() <-chan ports.LinkEvent { return testLinkEvents(t) }

func (t *ownedSwapErrorTransport) ReceiveClient() (protocol.ClientMessage, error) {
	return testReceiveClient(t)
}
func (t *ownedSwapErrorTransport) SendServer(m protocol.ServerMessage) error {
	return testSendServer(t, m)
}
func (t *ownedSwapErrorTransport) SendServerAsync(m protocol.ServerMessage) error {
	return testSendServerAsync(t, m)
}
func (t *ownedSwapErrorTransport) SendServerSynchronous(m protocol.ServerMessage) error {
	return testSendServerSynchronous(t, m)
}
func (t *ownedSwapErrorTransport) Capabilities() protocol.ConnectionCapabilities {
	return testServerCapabilities(t)
}
func (t *ownedSwapErrorTransport) LinkState() ports.LinkState         { return testLinkState(t) }
func (t *ownedSwapErrorTransport) LinkEvents() <-chan ports.LinkEvent { return testLinkEvents(t) }

func (t *blockingControlSendTransport) ReceiveClient() (protocol.ClientMessage, error) {
	return testReceiveClient(t)
}
func (t *blockingControlSendTransport) SendServer(m protocol.ServerMessage) error {
	return testSendServer(t, m)
}
func (t *blockingControlSendTransport) SendServerAsync(m protocol.ServerMessage) error {
	return testSendServerAsync(t, m)
}
func (t *blockingControlSendTransport) SendServerSynchronous(m protocol.ServerMessage) error {
	return testSendServerSynchronous(t, m)
}
func (t *blockingControlSendTransport) Capabilities() protocol.ConnectionCapabilities {
	return testServerCapabilities(t)
}
func (t *blockingControlSendTransport) LinkState() ports.LinkState         { return testLinkState(t) }
func (t *blockingControlSendTransport) LinkEvents() <-chan ports.LinkEvent { return testLinkEvents(t) }

func (t *commandSendErrorTransport) ReceiveClient() (protocol.ClientMessage, error) {
	return testReceiveClient(t)
}
func (t *commandSendErrorTransport) SendServer(m protocol.ServerMessage) error {
	return testSendServer(t, m)
}
func (t *commandSendErrorTransport) SendServerAsync(m protocol.ServerMessage) error {
	return testSendServerAsync(t, m)
}
func (t *commandSendErrorTransport) SendServerSynchronous(m protocol.ServerMessage) error {
	return testSendServerSynchronous(t, m)
}
func (t *commandSendErrorTransport) Capabilities() protocol.ConnectionCapabilities {
	return testServerCapabilities(t)
}
func (t *commandSendErrorTransport) LinkState() ports.LinkState         { return testLinkState(t) }
func (t *commandSendErrorTransport) LinkEvents() <-chan ports.LinkEvent { return testLinkEvents(t) }

func (t *fanoutBlockingTransport) ReceiveClient() (protocol.ClientMessage, error) {
	return testReceiveClient(t)
}
func (t *fanoutBlockingTransport) SendServer(m protocol.ServerMessage) error {
	return testSendServer(t, m)
}
func (t *fanoutBlockingTransport) SendServerAsync(m protocol.ServerMessage) error {
	return testSendServerAsync(t, m)
}
func (t *fanoutBlockingTransport) SendServerSynchronous(m protocol.ServerMessage) error {
	return testSendServerSynchronous(t, m)
}
func (t *fanoutBlockingTransport) Capabilities() protocol.ConnectionCapabilities {
	return testServerCapabilities(t)
}
func (t *fanoutBlockingTransport) LinkState() ports.LinkState         { return testLinkState(t) }
func (t *fanoutBlockingTransport) LinkEvents() <-chan ports.LinkEvent { return testLinkEvents(t) }

func (t *staleClipboardErrorTransport) ReceiveClient() (protocol.ClientMessage, error) {
	return testReceiveClient(t)
}
func (t *staleClipboardErrorTransport) SendServer(m protocol.ServerMessage) error {
	return testSendServer(t, m)
}
func (t *staleClipboardErrorTransport) SendServerAsync(m protocol.ServerMessage) error {
	return testSendServerAsync(t, m)
}
func (t *staleClipboardErrorTransport) SendServerSynchronous(m protocol.ServerMessage) error {
	return testSendServerSynchronous(t, m)
}
func (t *staleClipboardErrorTransport) Capabilities() protocol.ConnectionCapabilities {
	return testServerCapabilities(t)
}
func (t *staleClipboardErrorTransport) LinkState() ports.LinkState         { return testLinkState(t) }
func (t *staleClipboardErrorTransport) LinkEvents() <-chan ports.LinkEvent { return testLinkEvents(t) }

func (t *movingClipboardErrorTransport) ReceiveClient() (protocol.ClientMessage, error) {
	return testReceiveClient(t)
}
func (t *movingClipboardErrorTransport) SendServer(m protocol.ServerMessage) error {
	return testSendServer(t, m)
}
func (t *movingClipboardErrorTransport) SendServerAsync(m protocol.ServerMessage) error {
	return testSendServerAsync(t, m)
}
func (t *movingClipboardErrorTransport) SendServerSynchronous(m protocol.ServerMessage) error {
	return testSendServerSynchronous(t, m)
}
func (t *movingClipboardErrorTransport) Capabilities() protocol.ConnectionCapabilities {
	return testServerCapabilities(t)
}
func (t *movingClipboardErrorTransport) LinkState() ports.LinkState         { return testLinkState(t) }
func (t *movingClipboardErrorTransport) LinkEvents() <-chan ports.LinkEvent { return testLinkEvents(t) }

func (t *blockingClipboardTransport) ReceiveClient() (protocol.ClientMessage, error) {
	return testReceiveClient(t)
}
func (t *blockingClipboardTransport) SendServer(m protocol.ServerMessage) error {
	return testSendServer(t, m)
}
func (t *blockingClipboardTransport) SendServerAsync(m protocol.ServerMessage) error {
	return testSendServerAsync(t, m)
}
func (t *blockingClipboardTransport) SendServerSynchronous(m protocol.ServerMessage) error {
	return testSendServerSynchronous(t, m)
}
func (t *blockingClipboardTransport) Capabilities() protocol.ConnectionCapabilities {
	return testServerCapabilities(t)
}
func (t *blockingClipboardTransport) LinkState() ports.LinkState         { return testLinkState(t) }
func (t *blockingClipboardTransport) LinkEvents() <-chan ports.LinkEvent { return testLinkEvents(t) }

func (t *blockedAttachmentTransport) ReceiveClient() (protocol.ClientMessage, error) {
	return testReceiveClient(t)
}
func (t *blockedAttachmentTransport) SendServer(m protocol.ServerMessage) error {
	return testSendServer(t, m)
}
func (t *blockedAttachmentTransport) SendServerAsync(m protocol.ServerMessage) error {
	return testSendServerAsync(t, m)
}
func (t *blockedAttachmentTransport) SendServerSynchronous(m protocol.ServerMessage) error {
	return testSendServerSynchronous(t, m)
}
func (t *blockedAttachmentTransport) Capabilities() protocol.ConnectionCapabilities {
	return testServerCapabilities(t)
}
func (t *blockedAttachmentTransport) LinkState() ports.LinkState         { return testLinkState(t) }
func (t *blockedAttachmentTransport) LinkEvents() <-chan ports.LinkEvent { return testLinkEvents(t) }

func (t *resumeWelcomeFailureTransport) ReceiveClient() (protocol.ClientMessage, error) {
	return testReceiveClient(t)
}
func (t *resumeWelcomeFailureTransport) SendServer(m protocol.ServerMessage) error {
	return testSendServer(t, m)
}
func (t *resumeWelcomeFailureTransport) SendServerAsync(m protocol.ServerMessage) error {
	return testSendServerAsync(t, m)
}
func (t *resumeWelcomeFailureTransport) SendServerSynchronous(m protocol.ServerMessage) error {
	return testSendServerSynchronous(t, m)
}
func (t *resumeWelcomeFailureTransport) Capabilities() protocol.ConnectionCapabilities {
	return testServerCapabilities(t)
}
func (t *resumeWelcomeFailureTransport) LinkState() ports.LinkState         { return testLinkState(t) }
func (t *resumeWelcomeFailureTransport) LinkEvents() <-chan ports.LinkEvent { return testLinkEvents(t) }

func (t *committedIdentityErrorTransport) ReceiveClient() (protocol.ClientMessage, error) {
	return testReceiveClient(t)
}
func (t *committedIdentityErrorTransport) SendServer(m protocol.ServerMessage) error {
	return testSendServer(t, m)
}
func (t *committedIdentityErrorTransport) SendServerAsync(m protocol.ServerMessage) error {
	return testSendServerAsync(t, m)
}
func (t *committedIdentityErrorTransport) SendServerSynchronous(m protocol.ServerMessage) error {
	return testSendServerSynchronous(t, m)
}
func (t *committedIdentityErrorTransport) Capabilities() protocol.ConnectionCapabilities {
	return testServerCapabilities(t)
}
func (t *committedIdentityErrorTransport) LinkState() ports.LinkState { return testLinkState(t) }
func (t *committedIdentityErrorTransport) LinkEvents() <-chan ports.LinkEvent {
	return testLinkEvents(t)
}

func (t *countingOutputTransport) ReceiveClient() (protocol.ClientMessage, error) {
	return testReceiveClient(t)
}
func (t *countingOutputTransport) SendServer(m protocol.ServerMessage) error {
	return testSendServer(t, m)
}
func (t *countingOutputTransport) SendServerAsync(m protocol.ServerMessage) error {
	return testSendServerAsync(t, m)
}
func (t *countingOutputTransport) SendServerSynchronous(m protocol.ServerMessage) error {
	return testSendServerSynchronous(t, m)
}
func (t *countingOutputTransport) Capabilities() protocol.ConnectionCapabilities {
	return testServerCapabilities(t)
}
func (t *countingOutputTransport) LinkState() ports.LinkState         { return testLinkState(t) }
func (t *countingOutputTransport) LinkEvents() <-chan ports.LinkEvent { return testLinkEvents(t) }

func (t failingOutputTransport) ReceiveClient() (protocol.ClientMessage, error) {
	return testReceiveClient(t)
}
func (t failingOutputTransport) SendServer(m protocol.ServerMessage) error {
	return testSendServer(t, m)
}
func (t failingOutputTransport) SendServerAsync(m protocol.ServerMessage) error {
	return testSendServerAsync(t, m)
}
func (t failingOutputTransport) SendServerSynchronous(m protocol.ServerMessage) error {
	return testSendServerSynchronous(t, m)
}
func (t failingOutputTransport) Capabilities() protocol.ConnectionCapabilities {
	return testServerCapabilities(t)
}
func (t failingOutputTransport) LinkState() ports.LinkState         { return testLinkState(t) }
func (t failingOutputTransport) LinkEvents() <-chan ports.LinkEvent { return testLinkEvents(t) }

func (t cacheFailTransport) ReceiveClient() (protocol.ClientMessage, error) {
	return testReceiveClient(t)
}
func (t cacheFailTransport) SendServer(m protocol.ServerMessage) error { return testSendServer(t, m) }
func (t cacheFailTransport) SendServerAsync(m protocol.ServerMessage) error {
	return testSendServerAsync(t, m)
}
func (t cacheFailTransport) SendServerSynchronous(m protocol.ServerMessage) error {
	return testSendServerSynchronous(t, m)
}
func (t cacheFailTransport) Capabilities() protocol.ConnectionCapabilities {
	return testServerCapabilities(t)
}
func (t cacheFailTransport) LinkState() ports.LinkState         { return testLinkState(t) }
func (t cacheFailTransport) LinkEvents() <-chan ports.LinkEvent { return testLinkEvents(t) }

func (t *blockedRenderReplacementTransport) SendOutput(o protocol.Output) error {
	return testSendOutput(t, o)
}
func (t *blockedRenderReplacementTransport) SendOutputAsync(o protocol.Output) error {
	return testSendOutputAsync(t, o)
}
func (t *blockedRenderReplacementTransport) SendOutputSynchronous(o protocol.Output) error {
	return testSendOutputSynchronous(t, o)
}

func (t *teardownBlockingTransport) SendOutput(o protocol.Output) error { return testSendOutput(t, o) }
func (t *teardownBlockingTransport) SendOutputAsync(o protocol.Output) error {
	return testSendOutputAsync(t, o)
}
func (t *teardownBlockingTransport) SendOutputSynchronous(o protocol.Output) error {
	return testSendOutputSynchronous(t, o)
}

func (t *closeTrackingTransport) SendOutput(o protocol.Output) error { return testSendOutput(t, o) }
func (t *closeTrackingTransport) SendOutputAsync(o protocol.Output) error {
	return testSendOutputAsync(t, o)
}
func (t *closeTrackingTransport) SendOutputSynchronous(o protocol.Output) error {
	return testSendOutputSynchronous(t, o)
}

func (t *closeCountingBlockedTransport) SendOutput(o protocol.Output) error {
	return testSendOutput(t, o)
}
func (t *closeCountingBlockedTransport) SendOutputAsync(o protocol.Output) error {
	return testSendOutputAsync(t, o)
}
func (t *closeCountingBlockedTransport) SendOutputSynchronous(o protocol.Output) error {
	return testSendOutputSynchronous(t, o)
}

func (t *handshakeBlockingTransport) SendOutput(o protocol.Output) error { return testSendOutput(t, o) }
func (t *handshakeBlockingTransport) SendOutputAsync(o protocol.Output) error {
	return testSendOutputAsync(t, o)
}
func (t *handshakeBlockingTransport) SendOutputSynchronous(o protocol.Output) error {
	return testSendOutputSynchronous(t, o)
}

func (t *lateGraphicsCleanupTransport) SendOutput(o protocol.Output) error {
	return testSendOutput(t, o)
}
func (t *lateGraphicsCleanupTransport) SendOutputAsync(o protocol.Output) error {
	return testSendOutputAsync(t, o)
}
func (t *lateGraphicsCleanupTransport) SendOutputSynchronous(o protocol.Output) error {
	return testSendOutputSynchronous(t, o)
}

func (t *datagramTestTransport) SendOutput(o protocol.Output) error { return testSendOutput(t, o) }
func (t *datagramTestTransport) SendOutputAsync(o protocol.Output) error {
	return testSendOutputAsync(t, o)
}
func (t *datagramTestTransport) SendOutputSynchronous(o protocol.Output) error {
	return testSendOutputSynchronous(t, o)
}

func (t *asyncPaintTransport) SendOutput(o protocol.Output) error { return testSendOutput(t, o) }
func (t *asyncPaintTransport) SendOutputAsync(o protocol.Output) error {
	return testSendOutputAsync(t, o)
}
func (t *asyncPaintTransport) SendOutputSynchronous(o protocol.Output) error {
	return testSendOutputSynchronous(t, o)
}

func (t *timedSideEffectTransport) SendOutput(o protocol.Output) error { return testSendOutput(t, o) }
func (t *timedSideEffectTransport) SendOutputAsync(o protocol.Output) error {
	return testSendOutputAsync(t, o)
}
func (t *timedSideEffectTransport) SendOutputSynchronous(o protocol.Output) error {
	return testSendOutputSynchronous(t, o)
}

func (t *scriptedReplayTransport) SendOutput(o protocol.Output) error { return testSendOutput(t, o) }
func (t *scriptedReplayTransport) SendOutputAsync(o protocol.Output) error {
	return testSendOutputAsync(t, o)
}
func (t *scriptedReplayTransport) SendOutputSynchronous(o protocol.Output) error {
	return testSendOutputSynchronous(t, o)
}

func (t *swapErrorTransport) SendOutput(o protocol.Output) error { return testSendOutput(t, o) }
func (t *swapErrorTransport) SendOutputAsync(o protocol.Output) error {
	return testSendOutputAsync(t, o)
}
func (t *swapErrorTransport) SendOutputSynchronous(o protocol.Output) error {
	return testSendOutputSynchronous(t, o)
}

func (t *ownedSwapErrorTransport) SendOutput(o protocol.Output) error { return testSendOutput(t, o) }
func (t *ownedSwapErrorTransport) SendOutputAsync(o protocol.Output) error {
	return testSendOutputAsync(t, o)
}
func (t *ownedSwapErrorTransport) SendOutputSynchronous(o protocol.Output) error {
	return testSendOutputSynchronous(t, o)
}

func (t *blockingControlSendTransport) SendOutput(o protocol.Output) error {
	return testSendOutput(t, o)
}
func (t *blockingControlSendTransport) SendOutputAsync(o protocol.Output) error {
	return testSendOutputAsync(t, o)
}
func (t *blockingControlSendTransport) SendOutputSynchronous(o protocol.Output) error {
	return testSendOutputSynchronous(t, o)
}

func (t *commandSendErrorTransport) SendOutput(o protocol.Output) error { return testSendOutput(t, o) }
func (t *commandSendErrorTransport) SendOutputAsync(o protocol.Output) error {
	return testSendOutputAsync(t, o)
}
func (t *commandSendErrorTransport) SendOutputSynchronous(o protocol.Output) error {
	return testSendOutputSynchronous(t, o)
}

func (t *fanoutBlockingTransport) SendOutput(o protocol.Output) error { return testSendOutput(t, o) }
func (t *fanoutBlockingTransport) SendOutputAsync(o protocol.Output) error {
	return testSendOutputAsync(t, o)
}
func (t *fanoutBlockingTransport) SendOutputSynchronous(o protocol.Output) error {
	return testSendOutputSynchronous(t, o)
}

func (t *staleClipboardErrorTransport) SendOutput(o protocol.Output) error {
	return testSendOutput(t, o)
}
func (t *staleClipboardErrorTransport) SendOutputAsync(o protocol.Output) error {
	return testSendOutputAsync(t, o)
}
func (t *staleClipboardErrorTransport) SendOutputSynchronous(o protocol.Output) error {
	return testSendOutputSynchronous(t, o)
}

func (t *movingClipboardErrorTransport) SendOutput(o protocol.Output) error {
	return testSendOutput(t, o)
}
func (t *movingClipboardErrorTransport) SendOutputAsync(o protocol.Output) error {
	return testSendOutputAsync(t, o)
}
func (t *movingClipboardErrorTransport) SendOutputSynchronous(o protocol.Output) error {
	return testSendOutputSynchronous(t, o)
}

func (t *blockingClipboardTransport) SendOutput(o protocol.Output) error { return testSendOutput(t, o) }
func (t *blockingClipboardTransport) SendOutputAsync(o protocol.Output) error {
	return testSendOutputAsync(t, o)
}
func (t *blockingClipboardTransport) SendOutputSynchronous(o protocol.Output) error {
	return testSendOutputSynchronous(t, o)
}

func (t *blockedAttachmentTransport) SendOutput(o protocol.Output) error { return testSendOutput(t, o) }
func (t *blockedAttachmentTransport) SendOutputAsync(o protocol.Output) error {
	return testSendOutputAsync(t, o)
}
func (t *blockedAttachmentTransport) SendOutputSynchronous(o protocol.Output) error {
	return testSendOutputSynchronous(t, o)
}

func (t *resumeWelcomeFailureTransport) SendOutput(o protocol.Output) error {
	return testSendOutput(t, o)
}
func (t *resumeWelcomeFailureTransport) SendOutputAsync(o protocol.Output) error {
	return testSendOutputAsync(t, o)
}
func (t *resumeWelcomeFailureTransport) SendOutputSynchronous(o protocol.Output) error {
	return testSendOutputSynchronous(t, o)
}

func (t *committedIdentityErrorTransport) SendOutput(o protocol.Output) error {
	return testSendOutput(t, o)
}
func (t *committedIdentityErrorTransport) SendOutputAsync(o protocol.Output) error {
	return testSendOutputAsync(t, o)
}
func (t *committedIdentityErrorTransport) SendOutputSynchronous(o protocol.Output) error {
	return testSendOutputSynchronous(t, o)
}

func (t *countingOutputTransport) SendOutput(o protocol.Output) error { return testSendOutput(t, o) }
func (t *countingOutputTransport) SendOutputAsync(o protocol.Output) error {
	return testSendOutputAsync(t, o)
}
func (t *countingOutputTransport) SendOutputSynchronous(o protocol.Output) error {
	return testSendOutputSynchronous(t, o)
}

func (t failingOutputTransport) SendOutput(o protocol.Output) error { return testSendOutput(t, o) }
func (t failingOutputTransport) SendOutputAsync(o protocol.Output) error {
	return testSendOutputAsync(t, o)
}
func (t failingOutputTransport) SendOutputSynchronous(o protocol.Output) error {
	return testSendOutputSynchronous(t, o)
}

func (t cacheFailTransport) SendOutput(o protocol.Output) error { return testSendOutput(t, o) }
func (t cacheFailTransport) SendOutputAsync(o protocol.Output) error {
	return testSendOutputAsync(t, o)
}
func (t cacheFailTransport) SendOutputSynchronous(o protocol.Output) error {
	return testSendOutputSynchronous(t, o)
}

// unmarshalTestOutput decodes one captured output envelope in tests.
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

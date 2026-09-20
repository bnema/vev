package client

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/domain"
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

type mockClientConnection struct{ *wiremocks.MockTransport }

func newMockClientConnection(t mockTestingT) *mockClientConnection {
	return &mockClientConnection{MockTransport: wiremocks.NewMockTransport(t)}
}

func (c *mockClientConnection) SendClient(message protocol.ClientMessage) error {
	frame, err := testClientFrame(message)
	if err != nil {
		return err
	}
	return c.MockTransport.Send(frame)
}
func (c *mockClientConnection) ReceiveServer() (protocol.ServerMessage, error) {
	frame, err := c.MockTransport.Recv()
	if err != nil {
		return nil, err
	}
	return testServerMessage(frame)
}
func (c *mockClientConnection) Capabilities() protocol.ConnectionCapabilities {
	return testClientCapabilities(c.MockTransport)
}
func (c *mockClientConnection) LinkState() ports.LinkState {
	return testClientLinkState(c.MockTransport)
}
func (c *mockClientConnection) LinkEvents() <-chan ports.LinkEvent {
	return testClientLinkEvents(c.MockTransport)
}

type rawClientConnection struct{ raw wire.Transport }

func (c *rawClientConnection) SendClient(message protocol.ClientMessage) error {
	frame, err := testClientFrame(message)
	if err != nil {
		return err
	}
	return c.raw.Send(frame)
}
func (c *rawClientConnection) ReceiveServer() (protocol.ServerMessage, error) {
	frame, err := c.raw.Recv()
	if err != nil {
		return nil, err
	}
	return testServerMessage(frame)
}
func (c *rawClientConnection) Capabilities() protocol.ConnectionCapabilities {
	return testClientCapabilities(c.raw)
}
func (c *rawClientConnection) LinkState() ports.LinkState         { return testClientLinkState(c.raw) }
func (c *rawClientConnection) LinkEvents() <-chan ports.LinkEvent { return testClientLinkEvents(c.raw) }
func (c *rawClientConnection) Close() error                       { return c.raw.Close() }

type mockClientDialer struct{ *wiremocks.MockDialer }

func newMockClientDialer(t mockTestingT) *mockClientDialer {
	return &mockClientDialer{MockDialer: wiremocks.NewMockDialer(t)}
}
func (d *mockClientDialer) Dial(ctx context.Context) (ports.ClientConnection, error) {
	raw, err := d.MockDialer.Dial(ctx)
	if err != nil {
		return nil, err
	}
	return &rawClientConnection{raw: raw}, nil
}

func testClientFrame(message protocol.ClientMessage) (wire.Envelope, error) {
	raw, err := sessionwire.EncodeClientMessage(message)
	if err != nil {
		return wire.Envelope{}, err
	}
	return wire.Envelope{Payload: raw}, nil
}

func mustClientEnvelope(message protocol.ClientMessage) wire.Envelope {
	envelope, err := testClientFrame(message)
	if err != nil {
		panic(err)
	}
	return envelope
}

func mustServerEnvelope(message protocol.ServerMessage) wire.Envelope {
	if output, ok := message.(protocol.Output); ok && output.New != 0 && output.Context == nil {
		output.Context = &protocol.ViewContext{
			Publication: output.Epoch<<32 | output.New,
			Route:       protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "work"}},
			TabID:       "tab-1", FocusedPaneID: "pane-1",
		}
		message = output
	}
	raw, err := sessionwire.EncodeServerMessage(message)
	if err != nil {
		panic(err)
	}
	return wire.Envelope{Payload: raw}
}

func testServerMessage(envelope wire.Envelope) (protocol.ServerMessage, error) {
	return sessionwire.DecodeServerEnvelope(envelope.Payload)
}

func testClientCapabilities(raw wire.Transport) protocol.ConnectionCapabilities {
	_, dgram := raw.(wire.DatagramTransport)
	_, link := raw.(ports.LinkStateReporter)
	window := uint8(protocol.MaxOutputWindow)
	if dgram {
		window = 1
	}
	return protocol.ConnectionCapabilities{OutputDataLimit: protocol.MaxOutputDataLen, PreferredOutputWindow: window, LinkState: link}
}
func testClientLinkState(raw wire.Transport) ports.LinkState {
	if r, ok := raw.(ports.LinkStateReporter); ok {
		return r.LinkState()
	}
	return ports.LinkStateConnected
}
func testClientLinkEvents(raw wire.Transport) <-chan ports.LinkEvent {
	if r, ok := raw.(ports.LinkStateReporter); ok {
		return r.LinkEvents()
	}
	return nil
}

func TestRunRecvLogsIgnoredTypedBoundaryFailures(t *testing.T) {
	connection := portsmocks.NewMockClientConnection(t)
	connection.EXPECT().ReceiveServer().Return(nil, &protocol.DecodeFailure{Category: protocol.DecodeUnknownType, Type: 255}).Once()
	connection.EXPECT().ReceiveServer().Return(nil, &protocol.DecodeFailure{Category: protocol.DecodeWrongDirection}).Once()
	connection.EXPECT().ReceiveServer().Return(protocol.Pong{}, nil).Once()
	connection.EXPECT().ReceiveServer().Return(nil, io.EOF).Once()
	results := make(chan recvResult, 2)
	failed := make(chan struct{})
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	runRecv(context.Background(), connection, results, failed, log)
	first := <-results
	require.Equal(t, protocol.Pong{}, first.message)
	require.NoError(t, first.err)
	second := <-results
	require.ErrorIs(t, second.err, io.EOF)
	select {
	case <-failed:
	default:
		t.Fatal("terminal receive failure was not reported")
	}
	require.Contains(t, logs.String(), "ignoring rejected server message")
	require.Contains(t, logs.String(), "category=2")
	require.Contains(t, logs.String(), "category=3")
	require.Contains(t, logs.String(), "type=255")
	require.Contains(t, logs.String(), "type=2")
}

func (t *ackRecordingTransport) SendClient(m protocol.ClientMessage) error {
	f, e := testClientFrame(m)
	if e != nil {
		return e
	}
	return t.Send(f)
}
func (t *ackRecordingTransport) ReceiveServer() (protocol.ServerMessage, error) {
	f, e := t.Recv()
	if e != nil {
		return nil, e
	}
	return testServerMessage(f)
}
func (t *ackRecordingTransport) Capabilities() protocol.ConnectionCapabilities {
	return testClientCapabilities(t)
}
func (t *ackRecordingTransport) LinkState() ports.LinkState         { return ports.LinkStateConnected }
func (t *ackRecordingTransport) LinkEvents() <-chan ports.LinkEvent { return nil }

func (t *reconnectToastLinkTransport) SendClient(m protocol.ClientMessage) error {
	f, e := testClientFrame(m)
	if e != nil {
		return e
	}
	return t.Send(f)
}
func (t *reconnectToastLinkTransport) ReceiveServer() (protocol.ServerMessage, error) {
	f, e := t.Recv()
	if e != nil {
		return nil, e
	}
	return testServerMessage(f)
}
func (t *reconnectToastLinkTransport) Capabilities() protocol.ConnectionCapabilities {
	return testClientCapabilities(t)
}

func (t *reconnectToastRecordingTransport) SendClient(m protocol.ClientMessage) error {
	f, e := testClientFrame(m)
	if e != nil {
		return e
	}
	return t.Send(f)
}
func (t *reconnectToastRecordingTransport) ReceiveServer() (protocol.ServerMessage, error) {
	f, e := t.Recv()
	if e != nil {
		return nil, e
	}
	return testServerMessage(f)
}
func (t *reconnectToastRecordingTransport) Capabilities() protocol.ConnectionCapabilities {
	return testClientCapabilities(t)
}
func (t *reconnectToastRecordingTransport) LinkState() ports.LinkState {
	return ports.LinkStateConnected
}
func (t *reconnectToastRecordingTransport) LinkEvents() <-chan ports.LinkEvent {
	return nil
}

func (t *reconnectResetBlockingTransport) SendClient(m protocol.ClientMessage) error {
	f, e := testClientFrame(m)
	if e != nil {
		return e
	}
	return t.Send(f)
}
func (t *reconnectResetBlockingTransport) ReceiveServer() (protocol.ServerMessage, error) {
	f, e := t.Recv()
	if e != nil {
		return nil, e
	}
	return testServerMessage(f)
}
func (t *reconnectResetBlockingTransport) Capabilities() protocol.ConnectionCapabilities {
	return testClientCapabilities(t)
}
func (t *reconnectResetBlockingTransport) LinkState() ports.LinkState {
	return ports.LinkStateConnected
}
func (t *reconnectResetBlockingTransport) LinkEvents() <-chan ports.LinkEvent {
	return nil
}

func (t *reconnectToastBlockingSendTransport) SendClient(m protocol.ClientMessage) error {
	f, e := testClientFrame(m)
	if e != nil {
		return e
	}
	return t.Send(f)
}
func (t *reconnectToastBlockingSendTransport) ReceiveServer() (protocol.ServerMessage, error) {
	f, e := t.Recv()
	if e != nil {
		return nil, e
	}
	return testServerMessage(f)
}
func (t *reconnectToastBlockingSendTransport) Capabilities() protocol.ConnectionCapabilities {
	return testClientCapabilities(t)
}

func (t *paletteAttachTransport) SendClient(m protocol.ClientMessage) error {
	f, e := testClientFrame(m)
	if e != nil {
		return e
	}
	return t.Send(f)
}
func (t *paletteAttachTransport) ReceiveServer() (protocol.ServerMessage, error) {
	f, e := t.Recv()
	if e != nil {
		return nil, e
	}
	return testServerMessage(f)
}
func (t *paletteAttachTransport) Capabilities() protocol.ConnectionCapabilities {
	return testClientCapabilities(t)
}
func (t *paletteAttachTransport) LinkState() ports.LinkState         { return ports.LinkStateConnected }
func (t *paletteAttachTransport) LinkEvents() <-chan ports.LinkEvent { return nil }

func (t *attachPaletteTransport) SendClient(m protocol.ClientMessage) error {
	f, e := testClientFrame(m)
	if e != nil {
		return e
	}
	return t.Send(f)
}
func (t *attachPaletteTransport) ReceiveServer() (protocol.ServerMessage, error) {
	f, e := t.Recv()
	if e != nil {
		return nil, e
	}
	return testServerMessage(f)
}
func (t *attachPaletteTransport) Capabilities() protocol.ConnectionCapabilities {
	return testClientCapabilities(t)
}
func (t *attachPaletteTransport) LinkState() ports.LinkState         { return ports.LinkStateConnected }
func (t *attachPaletteTransport) LinkEvents() <-chan ports.LinkEvent { return nil }

func decodeClientMessageForTest(t *testing.T, envelope wire.Envelope) protocol.ClientMessage {
	t.Helper()
	message, err := sessionwire.DecodeClientEnvelope(envelope.Payload)
	if err != nil {
		t.Fatalf("decoding client envelope: %v", err)
	}
	return message
}

func serverMessageNameForTest(payload []byte) string {
	message, err := sessionwire.DecodeServerEnvelope(payload)
	if err != nil {
		return ""
	}
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
	case protocol.PickerPreview:
		return "PickerPreview"
	case protocol.PickerControlResponse:
		return "PickerControlResponse"
	default:
		return ""
	}
}

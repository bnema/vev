package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

type mockTestingT interface {
	mock.TestingT
	Cleanup(func())
}

type mockClientConnection struct{ *portsmocks.MockTransport }

func newMockClientConnection(t mockTestingT) *mockClientConnection {
	return &mockClientConnection{MockTransport: portsmocks.NewMockTransport(t)}
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

type rawClientConnection struct{ raw ports.Transport }

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

type mockClientDialer struct{ *portsmocks.MockDialer }

func newMockClientDialer(t mockTestingT) *mockClientDialer {
	return &mockClientDialer{MockDialer: portsmocks.NewMockDialer(t)}
}
func (d *mockClientDialer) Dial(ctx context.Context) (ports.ClientConnection, error) {
	raw, err := d.MockDialer.Dial(ctx)
	if err != nil {
		return nil, err
	}
	return &rawClientConnection{raw: raw}, nil
}

func testClientFrame(message protocol.ClientMessage) (wire.Frame, error) {
	switch m := message.(type) {
	case protocol.Hello:
		return wire.Frame{Type: wire.MsgHello, Payload: wire.MarshalHello(m)}, nil
	case protocol.Input:
		return wire.Frame{Type: wire.MsgInput, Payload: wire.MarshalInput(m)}, nil
	case protocol.Resize:
		p, e := wire.MarshalResize(m)
		return wire.Frame{Type: wire.MsgResize, Payload: p}, e
	case protocol.Detach:
		return wire.Frame{Type: wire.MsgDetach, Payload: wire.MarshalDetach(m)}, nil
	case protocol.Ping:
		return wire.Frame{Type: wire.MsgPing, Payload: wire.MarshalPing(m)}, nil
	case protocol.List:
		return wire.Frame{Type: wire.MsgList, Payload: wire.MarshalList(m)}, nil
	case protocol.Kill:
		return wire.Frame{Type: wire.MsgKill, Payload: wire.MarshalKill(m)}, nil
	case protocol.Theme:
		return wire.Frame{Type: wire.MsgTheme, Payload: wire.MarshalTheme(m)}, nil
	case protocol.Ack:
		p, e := wire.MarshalAck(m)
		return wire.Frame{Type: wire.MsgAck, Payload: p}, e
	case protocol.ImagePush:
		return wire.Frame{Type: wire.MsgImagePush, Payload: wire.MarshalImagePush(m)}, nil
	case protocol.ClientNotice:
		return wire.Frame{Type: wire.MsgClientNotice, Payload: wire.MarshalClientNotice(m)}, nil
	case protocol.CommandRequest:
		p, e := wire.MarshalCommandRequest(m)
		return wire.Frame{Type: wire.MsgCommand, Payload: p}, e
	case protocol.OutputResetRequest:
		return wire.Frame{Type: wire.MsgOutputResetRequest}, nil
	case protocol.RemotePreviewRequest:
		return wire.Frame{Type: wire.MsgRemotePreviewRequest, Payload: wire.MarshalRemotePreviewRequest(m)}, nil
	case protocol.RouteAttentionSubscription:
		p, e := wire.MarshalRouteAttentionSubscription(m)
		return wire.Frame{Type: wire.MsgRouteAttentionSubscription, Payload: p}, e
	case protocol.SamePeerSwitchRequest:
		p, e := wire.MarshalSamePeerSwitchRequest(m)
		return wire.Frame{Type: wire.MsgSamePeerSwitchRequest, Payload: p}, e
	case protocol.ParkedRouteRequest:
		return wire.Frame{Type: wire.MsgParkedRouteRequest, Payload: wire.MarshalParkedRouteRequest(m)}, nil
	case protocol.RecentRouteSnapshot:
		p, e := wire.MarshalRecentRouteSnapshot(m)
		return wire.Frame{Type: wire.MsgRecentRouteSnapshot, Payload: p}, e
	case protocol.RouteNavigationFailure:
		p, e := wire.MarshalRouteNavigationFailure(m)
		return wire.Frame{Type: wire.MsgRouteNavigationFailure, Payload: p}, e
	default:
		return wire.Frame{}, errors.New("test client connection: unsupported client message")
	}
}

func testServerMessage(frame wire.Frame) (protocol.ServerMessage, error) {
	switch frame.Type {
	case wire.MsgWelcome:
		return wire.UnmarshalWelcome(frame.Payload)
	case wire.MsgError:
		return wire.UnmarshalErrorMsg(frame.Payload)
	case wire.MsgOutput:
		return wire.UnmarshalOutput(frame.Payload)
	case wire.MsgDetached:
		return wire.UnmarshalDetached(frame.Payload)
	case wire.MsgPong:
		return wire.UnmarshalPong(frame.Payload)
	case wire.MsgSessions:
		return wire.UnmarshalSessions(frame.Payload)
	case wire.MsgCommandResult:
		return wire.UnmarshalCommandResult(frame.Payload)
	case wire.MsgNavigationAction:
		return wire.UnmarshalNavigationDirective(frame.Payload)
	case wire.MsgAttachTarget:
		return wire.UnmarshalAttachTarget(frame.Payload)
	case wire.MsgRemotePreviewResponse:
		return wire.UnmarshalRemotePreview(frame.Payload)
	case wire.MsgCommittedRouteIdentity:
		return wire.UnmarshalCommittedRouteIdentity(frame.Payload)
	case wire.MsgNavigateRecentRoute:
		return wire.UnmarshalRouteNavigationAction(frame.Payload)
	case wire.MsgRouteNavigationFailure:
		return wire.UnmarshalRouteNavigationFailure(frame.Payload)
	case wire.MsgRoutePosition:
		return wire.UnmarshalRoutePosition(frame.Payload)
	case wire.MsgSamePeerSwitchFailure:
		return wire.UnmarshalSamePeerSwitchFailure(frame.Payload)
	case wire.MsgParkedRouteResponse:
		return wire.UnmarshalParkedRouteResponse(frame.Payload)
	default:
		return nil, errors.New("test client connection: unsupported server frame")
	}
}

func testClientCapabilities(raw ports.Transport) protocol.ConnectionCapabilities {
	_, dgram := raw.(ports.DatagramTransport)
	_, link := raw.(ports.LinkStateReporter)
	window := uint8(protocol.MaxOutputWindow)
	if dgram {
		window = 1
	}
	return protocol.ConnectionCapabilities{OutputDataLimit: protocol.MaxOutputDataLen, PreferredOutputWindow: window, LinkState: link}
}
func testClientLinkState(raw ports.Transport) ports.LinkState {
	if r, ok := raw.(ports.LinkStateReporter); ok {
		return r.LinkState()
	}
	return ports.LinkStateConnected
}
func testClientLinkEvents(raw ports.Transport) <-chan ports.LinkEvent {
	if r, ok := raw.(ports.LinkStateReporter); ok {
		return r.LinkEvents()
	}
	return nil
}

func TestRunRecvLogsIgnoredTypedBoundaryFailures(t *testing.T) {
	connection := portsmocks.NewMockClientConnection(t)
	connection.EXPECT().ReceiveServer().Return(nil, &protocol.DecodeFailure{Category: protocol.DecodeUnknownType, Type: 255}).Once()
	connection.EXPECT().ReceiveServer().Return(nil, &protocol.DecodeFailure{Category: protocol.DecodeWrongDirection, Type: uint8(wire.MsgInput)}).Once()
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

package client_test

import (
	"context"
	"errors"

	"github.com/stretchr/testify/mock"

	"github.com/bnema/vev/internal/ports"
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
func (c *mockClientConnection) SendClient(m protocol.ClientMessage) error {
	f, e := externalClientFrame(m)
	if e != nil {
		return e
	}
	return c.Send(f)
}
func (c *mockClientConnection) ReceiveServer() (protocol.ServerMessage, error) {
	f, e := c.Recv()
	if e != nil {
		return nil, e
	}
	return externalServerMessage(f)
}
func (c *mockClientConnection) Capabilities() protocol.ConnectionCapabilities {
	return externalCapabilities(c.MockTransport)
}
func (c *mockClientConnection) LinkState() ports.LinkState         { return ports.LinkStateConnected }
func (c *mockClientConnection) LinkEvents() <-chan ports.LinkEvent { return nil }

type rawClientConnection struct{ raw wire.Transport }

func (c *rawClientConnection) SendClient(m protocol.ClientMessage) error {
	f, e := externalClientFrame(m)
	if e != nil {
		return e
	}
	return c.raw.Send(f)
}
func (c *rawClientConnection) ReceiveServer() (protocol.ServerMessage, error) {
	f, e := c.raw.Recv()
	if e != nil {
		return nil, e
	}
	return externalServerMessage(f)
}
func (c *rawClientConnection) Capabilities() protocol.ConnectionCapabilities {
	return externalCapabilities(c.raw)
}
func (c *rawClientConnection) LinkState() ports.LinkState {
	if r, ok := c.raw.(ports.LinkStateReporter); ok {
		return r.LinkState()
	}
	return ports.LinkStateConnected
}
func (c *rawClientConnection) LinkEvents() <-chan ports.LinkEvent {
	if r, ok := c.raw.(ports.LinkStateReporter); ok {
		return r.LinkEvents()
	}
	return nil
}
func (c *rawClientConnection) Close() error { return c.raw.Close() }

type mockClientDialer struct{ *wiremocks.MockDialer }

func newMockClientDialer(t mockTestingT) *mockClientDialer {
	return &mockClientDialer{MockDialer: wiremocks.NewMockDialer(t)}
}
func (d *mockClientDialer) Dial(ctx context.Context) (ports.ClientConnection, error) {
	raw, e := d.MockDialer.Dial(ctx)
	if e != nil {
		return nil, e
	}
	return &rawClientConnection{raw: raw}, nil
}

func externalCapabilities(raw wire.Transport) protocol.ConnectionCapabilities {
	_, dgram := raw.(wire.DatagramTransport)
	_, link := raw.(ports.LinkStateReporter)
	window := uint8(protocol.MaxOutputWindow)
	if dgram {
		window = 1
	}
	return protocol.ConnectionCapabilities{
		OutputDataLimit:       protocol.MaxOutputDataLen,
		PreferredOutputWindow: window,
		LinkState:             link,
	}
}
func externalClientFrame(m protocol.ClientMessage) (wire.Frame, error) {
	switch x := m.(type) {
	case protocol.Hello:
		return wire.Frame{Type: wire.MsgHello, Payload: wire.MarshalHello(x)}, nil
	case protocol.Input:
		return wire.Frame{Type: wire.MsgInput, Payload: wire.MarshalInput(x)}, nil
	case protocol.Resize:
		p, e := wire.MarshalResize(x)
		return wire.Frame{Type: wire.MsgResize, Payload: p}, e
	case protocol.Detach:
		return wire.Frame{Type: wire.MsgDetach}, nil
	case protocol.Theme:
		return wire.Frame{Type: wire.MsgTheme, Payload: wire.MarshalTheme(x)}, nil
	case protocol.Ack:
		p, e := wire.MarshalAck(x)
		return wire.Frame{Type: wire.MsgAck, Payload: p}, e
	case protocol.ImagePush:
		return wire.Frame{Type: wire.MsgImagePush, Payload: wire.MarshalImagePush(x)}, nil
	case protocol.ClientNotice:
		return wire.Frame{Type: wire.MsgClientNotice, Payload: wire.MarshalClientNotice(x)}, nil
	case protocol.OutputResetRequest:
		return wire.Frame{Type: wire.MsgOutputResetRequest}, nil
	case protocol.RecentRouteSnapshot:
		p, e := wire.MarshalRecentRouteSnapshot(x)
		return wire.Frame{Type: wire.MsgRecentRouteSnapshot, Payload: p}, e
	case protocol.RouteAttentionSubscription:
		p, e := wire.MarshalRouteAttentionSubscription(x)
		return wire.Frame{Type: wire.MsgRouteAttentionSubscription, Payload: p}, e
	case protocol.SamePeerSwitchRequest:
		p, e := wire.MarshalSamePeerSwitchRequest(x)
		return wire.Frame{Type: wire.MsgSamePeerSwitchRequest, Payload: p}, e
	case protocol.ParkedRouteRequest:
		return wire.Frame{Type: wire.MsgParkedRouteRequest, Payload: wire.MarshalParkedRouteRequest(x)}, nil
	case protocol.RouteNavigationFailure:
		p, e := wire.MarshalRouteNavigationFailure(x)
		return wire.Frame{Type: wire.MsgRouteNavigationFailure, Payload: p}, e
	case protocol.SessionCreationFailure:
		p, e := wire.MarshalSessionCreationFailure(x)
		return wire.Frame{Type: wire.MsgSessionCreationFailure, Payload: p}, e
	default:
		return wire.Frame{}, errors.New("test client: unsupported message")
	}
}
func externalServerMessage(f wire.Frame) (protocol.ServerMessage, error) {
	switch f.Type {
	case wire.MsgWelcome:
		return wire.UnmarshalWelcome(f.Payload)
	case wire.MsgError:
		return wire.UnmarshalErrorMsg(f.Payload)
	case wire.MsgOutput:
		return wire.UnmarshalOutput(f.Payload)
	case wire.MsgDetached:
		return wire.UnmarshalDetached(f.Payload)
	case wire.MsgPong:
		return wire.UnmarshalPong(f.Payload)
	case wire.MsgAttachTarget:
		return wire.UnmarshalAttachTarget(f.Payload)
	case wire.MsgNavigationAction:
		return wire.UnmarshalNavigationDirective(f.Payload)
	case wire.MsgParkedRouteResponse:
		return wire.UnmarshalParkedRouteResponse(f.Payload)
	case wire.MsgNavigateRecentRoute:
		return wire.UnmarshalRouteNavigationAction(f.Payload)
	case wire.MsgRouteCreateSession:
		return wire.UnmarshalRouteCreateSessionAction(f.Payload)
	case wire.MsgCommittedRouteIdentity:
		return wire.UnmarshalCommittedRouteIdentity(f.Payload)
	case wire.MsgSamePeerSwitchFailure:
		return wire.UnmarshalSamePeerSwitchFailure(f.Payload)
	case wire.MsgRoutePosition:
		return wire.UnmarshalRoutePosition(f.Payload)
	case wire.MsgRouteNavigationFailure:
		return wire.UnmarshalRouteNavigationFailure(f.Payload)
	default:
		return nil, errors.New("test client: unsupported server frame")
	}
}

func (t *markedDatagramTransport) SendClient(m protocol.ClientMessage) error {
	f, e := externalClientFrame(m)
	if e != nil {
		return e
	}
	return t.Send(f)
}
func (t *markedDatagramTransport) ReceiveServer() (protocol.ServerMessage, error) {
	f, e := t.Recv()
	if e != nil {
		return nil, e
	}
	return externalServerMessage(f)
}
func (t *markedDatagramTransport) Capabilities() protocol.ConnectionCapabilities {
	return externalCapabilities(t)
}
func (t *markedDatagramTransport) LinkState() ports.LinkState         { return ports.LinkStateConnected }
func (t *markedDatagramTransport) LinkEvents() <-chan ports.LinkEvent { return nil }

func (t *recordingTransport) SendClient(m protocol.ClientMessage) error {
	f, e := externalClientFrame(m)
	if e != nil {
		return e
	}
	return t.Send(f)
}
func (t *recordingTransport) ReceiveServer() (protocol.ServerMessage, error) {
	f, e := t.Recv()
	if e != nil {
		return nil, e
	}
	return externalServerMessage(f)
}
func (t *recordingTransport) Capabilities() protocol.ConnectionCapabilities {
	return externalCapabilities(t)
}
func (t *recordingTransport) LinkState() ports.LinkState         { return ports.LinkStateConnected }
func (t *recordingTransport) LinkEvents() <-chan ports.LinkEvent { return nil }

func (t *clipboardToastLifecycleTransport) SendClient(m protocol.ClientMessage) error {
	f, e := externalClientFrame(m)
	if e != nil {
		return e
	}
	return t.Send(f)
}
func (t *clipboardToastLifecycleTransport) ReceiveServer() (protocol.ServerMessage, error) {
	f, e := t.Recv()
	if e != nil {
		return nil, e
	}
	return externalServerMessage(f)
}
func (t *clipboardToastLifecycleTransport) Capabilities() protocol.ConnectionCapabilities {
	return externalCapabilities(t)
}
func (t *clipboardToastLifecycleTransport) LinkState() ports.LinkState {
	return ports.LinkStateConnected
}
func (t *clipboardToastLifecycleTransport) LinkEvents() <-chan ports.LinkEvent { return nil }

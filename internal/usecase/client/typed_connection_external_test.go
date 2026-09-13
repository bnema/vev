package client_test

import (
	"context"

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
func externalClientFrame(m protocol.ClientMessage) (wire.Envelope, error) {
	raw, err := sessionwire.EncodeClientMessage(m)
	if err != nil {
		return wire.Envelope{}, err
	}
	return wire.Envelope{Payload: raw}, nil
}

func externalServerMessage(f wire.Envelope) (protocol.ServerMessage, error) {
	return sessionwire.DecodeServerEnvelope(f.Payload)
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

// Package sessionwire adapts typed session messages to strict binary frames.
package sessionwire

import (
	"errors"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

var (
	ErrUnknownMessageType = errors.New("sessionwire: unknown message type")
	ErrWrongDirection     = errors.New("sessionwire: wrong message direction")
	ErrInvalidMessage     = errors.New("sessionwire: invalid message")
	ErrUnsupportedSend    = errors.New("sessionwire: send mode is unsupported")
)

type serverConnection struct{ raw wire.Transport }

var _ ports.ServerConnection = (*serverConnection)(nil)

// NewServerConnection wraps one raw connection incarnation exactly once.
func NewServerConnection(raw wire.Transport) ports.ServerConnection {
	if raw == nil {
		return nil
	}
	return &serverConnection{raw: raw}
}

func (c *serverConnection) ReceiveClient() (protocol.ClientMessage, error) {
	frame, err := c.raw.Recv()
	if err != nil {
		return nil, err
	}
	message, err := decodeClient(frame)
	if err == nil {
		return message, nil
	}
	failure := &protocol.DecodeFailure{Category: protocol.DecodeMalformed, Type: uint8(frame.Type), Err: err}
	if errors.Is(err, ErrUnknownMessageType) {
		failure.Category = protocol.DecodeUnknownType
	} else if errors.Is(err, ErrWrongDirection) {
		failure.Category = protocol.DecodeWrongDirection
	}
	switch frame.Type {
	case wire.MsgHello:
		failure.Kind = protocol.DecodeMessageHello
		failure.Version, _ = wire.PeekHelloVersion(frame.Payload)
	case wire.MsgCommand:
		failure.Kind = protocol.DecodeMessageCommand
		failure.Version, _ = wire.PeekCommandVersion(frame.Payload)
		failure.RequestID, failure.HasRequestID = wire.PeekCommandRequestID(frame.Payload)
	case wire.MsgKill:
		failure.Kind = protocol.DecodeMessageKill
	case wire.MsgRemotePreviewRequest:
		failure.Kind = protocol.DecodeMessageRemotePreview
	}
	return nil, failure
}

func (c *serverConnection) SendServer(message protocol.ServerMessage) error {
	frame, err := encodeServer(message)
	if err != nil {
		return err
	}
	return c.raw.Send(frame)
}

func (c *serverConnection) SendServerAsync(message protocol.ServerMessage) error {
	transport, ok := c.raw.(wire.AsyncTransport)
	if !ok {
		return ErrUnsupportedSend
	}
	frame, err := encodeServer(message)
	if err != nil {
		return err
	}
	return transport.SendAsync(frame)
}

func (c *serverConnection) SendServerSynchronous(message protocol.ServerMessage) error {
	transport, ok := c.raw.(wire.OwnedSynchronousTransport)
	if !ok {
		return ErrUnsupportedSend
	}
	frame, err := encodeServer(message)
	if err != nil {
		return err
	}
	return transport.SendSynchronous(frame)
}

func (c *serverConnection) SendOutput(output protocol.Output) error {
	frame, err := encodeOutput(output)
	if err != nil {
		return err
	}
	return c.raw.Send(frame)
}

func (c *serverConnection) SendOutputAsync(output protocol.Output) error {
	transport, ok := c.raw.(wire.AsyncTransport)
	if !ok {
		return ErrUnsupportedSend
	}
	frame, err := encodeOutput(output)
	if err != nil {
		return err
	}
	return transport.SendAsync(frame)
}

func (c *serverConnection) SendOutputSynchronous(output protocol.Output) error {
	transport, ok := c.raw.(wire.OwnedSynchronousTransport)
	if !ok {
		return ErrUnsupportedSend
	}
	frame, err := encodeOutput(output)
	if err != nil {
		return err
	}
	return transport.SendSynchronous(frame)
}

func encodeOutput(output protocol.Output) (wire.Frame, error) {
	payload, err := wire.MarshalOutput(output)
	return wire.Frame{Type: wire.MsgOutput, Payload: payload}, err
}

func (c *serverConnection) Capabilities() protocol.ConnectionCapabilities {
	return rawCapabilities(c.raw)
}

func (c *serverConnection) LinkState() ports.LinkState         { return rawLinkState(c.raw) }
func (c *serverConnection) LinkEvents() <-chan ports.LinkEvent { return rawLinkEvents(c.raw) }

func rawCapabilities(raw wire.Transport) protocol.ConnectionCapabilities {
	_, datagram := raw.(wire.DatagramTransport)
	_, async := raw.(wire.AsyncTransport)
	_, synchronous := raw.(wire.OwnedSynchronousTransport)
	_, linkState := raw.(ports.LinkStateReporter)
	window := uint8(protocol.MaxOutputWindow)
	if datagram {
		window = 1
	}
	return protocol.ConnectionCapabilities{
		OutputDataLimit:       protocol.MaxOutputDataLen,
		PreferredOutputWindow: window,
		AsyncSend:             async,
		OwnedSynchronousSend:  synchronous,
		LinkState:             linkState,
	}
}

func rawLinkState(raw wire.Transport) ports.LinkState {
	if reporter, ok := raw.(ports.LinkStateReporter); ok {
		return reporter.LinkState()
	}
	return ports.LinkStateConnected
}

func rawLinkEvents(raw wire.Transport) <-chan ports.LinkEvent {
	if reporter, ok := raw.(ports.LinkStateReporter); ok {
		return reporter.LinkEvents()
	}
	return nil
}

func (c *serverConnection) Close() error { return c.raw.Close() }

type serverListener struct{ raw wire.Listener }

var _ ports.ServerListener = (*serverListener)(nil)

// NewServerListener wraps every accepted raw connection in a stable typed adapter.
func NewServerListener(raw wire.Listener) ports.ServerListener {
	if raw == nil {
		return nil
	}
	return &serverListener{raw: raw}
}

func (l *serverListener) Accept() (ports.ServerConnection, error) {
	raw, err := l.raw.Accept()
	if err != nil {
		return nil, err
	}
	return NewServerConnection(raw), nil
}

func (l *serverListener) Close() error { return l.raw.Close() }
func (l *serverListener) Addr() string { return l.raw.Addr() }

func decodeClient(frame wire.Frame) (protocol.ClientMessage, error) {
	switch frame.Type {
	case wire.MsgHello:
		return wire.UnmarshalHello(frame.Payload)
	case wire.MsgInput:
		return wire.UnmarshalInput(frame.Payload)
	case wire.MsgResize:
		return wire.UnmarshalResize(frame.Payload)
	case wire.MsgDetach:
		return protocol.Detach{}, nil
	case wire.MsgPing:
		return protocol.Ping{}, nil
	case wire.MsgList:
		return protocol.List{}, nil
	case wire.MsgKill:
		return wire.UnmarshalKill(frame.Payload)
	case wire.MsgTheme:
		return wire.UnmarshalTheme(frame.Payload)
	case wire.MsgAck:
		return wire.UnmarshalAck(frame.Payload)
	case wire.MsgImagePush:
		return wire.UnmarshalImagePush(frame.Payload)
	case wire.MsgClientNotice:
		return wire.UnmarshalClientNotice(frame.Payload)
	case wire.MsgCommand:
		return wire.UnmarshalCommandRequest(frame.Payload)
	case wire.MsgOutputResetRequest:
		return wire.UnmarshalOutputResetRequest(frame.Payload)
	case wire.MsgRemotePreviewRequest:
		return wire.UnmarshalRemotePreviewRequest(frame.Payload)
	case wire.MsgRouteAttentionSubscription:
		return wire.UnmarshalRouteAttentionSubscription(frame.Payload)
	case wire.MsgSamePeerSwitchRequest:
		return wire.UnmarshalSamePeerSwitchRequest(frame.Payload)
	case wire.MsgParkedRouteRequest:
		return wire.UnmarshalParkedRouteRequest(frame.Payload)
	case wire.MsgRecentRouteSnapshot:
		return wire.UnmarshalRecentRouteSnapshot(frame.Payload)
	case wire.MsgRouteNavigationFailure:
		return wire.UnmarshalRouteNavigationFailure(frame.Payload)
	case wire.MsgSessionCreationFailure:
		return wire.UnmarshalSessionCreationFailure(frame.Payload)
	case wire.MsgWelcome, wire.MsgError, wire.MsgOutput, wire.MsgDetached, wire.MsgPong,
		wire.MsgSessions, wire.MsgCommandResult, wire.MsgNavigationAction,
		wire.MsgAttachTarget, wire.MsgRemotePreviewResponse,
		wire.MsgCommittedRouteIdentity, wire.MsgNavigateRecentRoute, wire.MsgRouteCreateSession,
		wire.MsgRoutePosition, wire.MsgSamePeerSwitchFailure, wire.MsgParkedRouteResponse:
		return nil, ErrWrongDirection
	default:
		return nil, ErrUnknownMessageType
	}
}

func encodeServer(message protocol.ServerMessage) (wire.Frame, error) {
	switch m := message.(type) {
	case protocol.Welcome:
		payload := wire.MarshalWelcome(m)
		if payload == nil {
			return wire.Frame{}, ErrInvalidMessage
		}
		return wire.Frame{Type: wire.MsgWelcome, Payload: payload}, nil
	case protocol.ErrorMsg:
		return wire.Frame{Type: wire.MsgError, Payload: wire.MarshalErrorMsg(m)}, nil
	case protocol.Output:
		payload, err := wire.MarshalOutput(m)
		return wire.Frame{Type: wire.MsgOutput, Payload: payload}, err
	case protocol.Detached:
		if err := protocol.ValidateDetached(m); err != nil {
			return wire.Frame{}, err
		}
		return wire.Frame{Type: wire.MsgDetached, Payload: wire.MarshalDetached(m)}, nil
	case protocol.Pong:
		return wire.Frame{Type: wire.MsgPong, Payload: wire.MarshalPong(m)}, nil
	case protocol.Sessions:
		return wire.Frame{Type: wire.MsgSessions, Payload: wire.MarshalSessions(m)}, nil
	case protocol.CommandResult:
		return wire.Frame{Type: wire.MsgCommandResult, Payload: wire.MarshalCommandResult(m)}, nil
	case protocol.NavigationDirective:
		payload := wire.MarshalNavigationDirective(m)
		if payload == nil {
			return wire.Frame{}, ErrInvalidMessage
		}
		return wire.Frame{Type: wire.MsgNavigationAction, Payload: payload}, nil
	case protocol.AttachTarget:
		payload := wire.MarshalAttachTarget(m)
		if payload == nil {
			return wire.Frame{}, ErrInvalidMessage
		}
		return wire.Frame{Type: wire.MsgAttachTarget, Payload: payload}, nil
	case protocol.RemotePreview:
		payload := wire.MarshalRemotePreview(m)
		if payload == nil {
			return wire.Frame{}, ErrInvalidMessage
		}
		return wire.Frame{Type: wire.MsgRemotePreviewResponse, Payload: payload}, nil
	case protocol.CommittedRouteIdentity:
		payload, err := wire.MarshalCommittedRouteIdentity(m)
		return wire.Frame{Type: wire.MsgCommittedRouteIdentity, Payload: payload}, err
	case protocol.RouteNavigationAction:
		payload, err := wire.MarshalRouteNavigationAction(m)
		return wire.Frame{Type: wire.MsgNavigateRecentRoute, Payload: payload}, err
	case protocol.RouteCreateSessionAction:
		payload, err := wire.MarshalRouteCreateSessionAction(m)
		return wire.Frame{Type: wire.MsgRouteCreateSession, Payload: payload}, err
	case protocol.RouteNavigationFailure:
		payload, err := wire.MarshalRouteNavigationFailure(m)
		return wire.Frame{Type: wire.MsgRouteNavigationFailure, Payload: payload}, err
	case protocol.RoutePosition:
		payload, err := wire.MarshalRoutePosition(m)
		return wire.Frame{Type: wire.MsgRoutePosition, Payload: payload}, err
	case protocol.SamePeerSwitchFailure:
		payload, err := wire.MarshalSamePeerSwitchFailure(m)
		return wire.Frame{Type: wire.MsgSamePeerSwitchFailure, Payload: payload}, err
	case protocol.ParkedRouteResponse:
		payload := wire.MarshalParkedRouteResponse(m)
		if payload == nil {
			return wire.Frame{}, ErrInvalidMessage
		}
		return wire.Frame{Type: wire.MsgParkedRouteResponse, Payload: payload}, nil
	case *protocol.Welcome:
		if m != nil {
			return encodeServer(*m)
		}
	case *protocol.ErrorMsg:
		if m != nil {
			return encodeServer(*m)
		}
	case *protocol.Output:
		if m != nil {
			return encodeServer(*m)
		}
	case *protocol.Detached:
		if m != nil {
			return encodeServer(*m)
		}
	case *protocol.Pong:
		if m != nil {
			return encodeServer(*m)
		}
	case *protocol.Sessions:
		if m != nil {
			return encodeServer(*m)
		}
	case *protocol.CommandResult:
		if m != nil {
			return encodeServer(*m)
		}
	case *protocol.NavigationDirective:
		if m != nil {
			return encodeServer(*m)
		}
	case *protocol.AttachTarget:
		if m != nil {
			return encodeServer(*m)
		}
	case *protocol.RemotePreview:
		if m != nil {
			return encodeServer(*m)
		}
	case *protocol.CommittedRouteIdentity:
		if m != nil {
			return encodeServer(*m)
		}
	case *protocol.RouteNavigationAction:
		if m != nil {
			return encodeServer(*m)
		}
	case *protocol.RouteCreateSessionAction:
		if m != nil {
			return encodeServer(*m)
		}
	case *protocol.RouteNavigationFailure:
		if m != nil {
			return encodeServer(*m)
		}
	case *protocol.RoutePosition:
		if m != nil {
			return encodeServer(*m)
		}
	case *protocol.SamePeerSwitchFailure:
		if m != nil {
			return encodeServer(*m)
		}
	case *protocol.ParkedRouteResponse:
		if m != nil {
			return encodeServer(*m)
		}
	default:
		return wire.Frame{}, ErrWrongDirection
	}
	return wire.Frame{}, ErrInvalidMessage
}

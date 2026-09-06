package sessionwire

import (
	"context"
	"errors"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

type clientConnection struct{ raw wire.Transport }

var _ ports.ClientConnection = (*clientConnection)(nil)

// NewClientConnection wraps one raw client connection incarnation.
func NewClientConnection(raw wire.Transport) ports.ClientConnection {
	if raw == nil {
		return nil
	}
	return &clientConnection{raw: raw}
}

func (c *clientConnection) SendClient(message protocol.ClientMessage) error {
	frame, err := encodeClient(message)
	if err != nil {
		return err
	}
	return c.raw.Send(frame)
}

func (c *clientConnection) ReceiveServer() (protocol.ServerMessage, error) {
	frame, err := c.raw.Recv()
	if err != nil {
		return nil, err
	}
	message, err := decodeServer(frame)
	if err == nil {
		return message, nil
	}
	failure := &protocol.DecodeFailure{Category: protocol.DecodeMalformed, Type: uint8(frame.Type), Err: err}
	if errors.Is(err, ErrUnknownMessageType) {
		failure.Category = protocol.DecodeUnknownType
	} else if errors.Is(err, ErrWrongDirection) {
		failure.Category = protocol.DecodeWrongDirection
	}
	return nil, failure
}

func (c *clientConnection) Capabilities() protocol.ConnectionCapabilities {
	return rawCapabilities(c.raw)
}

func (c *clientConnection) LinkState() ports.LinkState         { return rawLinkState(c.raw) }
func (c *clientConnection) LinkEvents() <-chan ports.LinkEvent { return rawLinkEvents(c.raw) }
func (c *clientConnection) Close() error                       { return c.raw.Close() }

type clientDialer struct{ raw wire.Dialer }

var _ ports.ClientDialer = (*clientDialer)(nil)

// NewClientDialer wraps every dialed raw connection in a stable typed adapter.
func NewClientDialer(raw wire.Dialer) ports.ClientDialer {
	if raw == nil {
		return nil
	}
	return &clientDialer{raw: raw}
}

func (d *clientDialer) Dial(ctx context.Context) (ports.ClientConnection, error) {
	raw, err := d.raw.Dial(ctx)
	if err != nil {
		return nil, err
	}
	return NewClientConnection(raw), nil
}

func decodeServer(frame wire.Frame) (protocol.ServerMessage, error) {
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
	case wire.MsgRouteCreateSession:
		return wire.UnmarshalRouteCreateSessionAction(frame.Payload)
	case wire.MsgRouteNavigationFailure:
		return wire.UnmarshalRouteNavigationFailure(frame.Payload)
	case wire.MsgRoutePosition:
		return wire.UnmarshalRoutePosition(frame.Payload)
	case wire.MsgSamePeerSwitchFailure:
		return wire.UnmarshalSamePeerSwitchFailure(frame.Payload)
	case wire.MsgParkedRouteResponse:
		return wire.UnmarshalParkedRouteResponse(frame.Payload)
	case wire.MsgUIReceipt:
		return wire.UnmarshalUIReceipt(frame.Payload)
	case wire.MsgUIViewUpdate:
		return wire.UnmarshalUIViewUpdate(frame.Payload)
	case wire.MsgHello, wire.MsgInput, wire.MsgResize, wire.MsgDetach, wire.MsgPing,
		wire.MsgList, wire.MsgKill, wire.MsgTheme, wire.MsgAck, wire.MsgImagePush,
		wire.MsgClientNotice, wire.MsgCommand, wire.MsgOutputResetRequest,
		wire.MsgRemotePreviewRequest, wire.MsgRouteAttentionSubscription,
		wire.MsgSamePeerSwitchRequest, wire.MsgParkedRouteRequest,
		wire.MsgRecentRouteSnapshot, wire.MsgSessionCreationFailure, wire.MsgUIFence:
		return nil, ErrWrongDirection
	default:
		return nil, ErrUnknownMessageType
	}
}

func encodeClient(message protocol.ClientMessage) (wire.Frame, error) {
	switch m := message.(type) {
	case protocol.Hello:
		payload := wire.MarshalHello(m)
		if payload == nil {
			return wire.Frame{}, ErrInvalidMessage
		}
		return wire.Frame{Type: wire.MsgHello, Payload: payload}, nil
	case protocol.Input:
		return wire.Frame{Type: wire.MsgInput, Payload: wire.MarshalInput(m)}, nil
	case protocol.Resize:
		payload, err := wire.MarshalResize(m)
		return wire.Frame{Type: wire.MsgResize, Payload: payload}, err
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
		payload, err := wire.MarshalAck(m)
		return wire.Frame{Type: wire.MsgAck, Payload: payload}, err
	case protocol.ImagePush:
		return wire.Frame{Type: wire.MsgImagePush, Payload: wire.MarshalImagePush(m)}, nil
	case protocol.ClientNotice:
		if err := protocol.ValidateClientNotice(m); err != nil {
			return wire.Frame{}, err
		}
		return wire.Frame{Type: wire.MsgClientNotice, Payload: wire.MarshalClientNotice(m)}, nil
	case protocol.CommandRequest:
		payload, err := wire.MarshalCommandRequest(m)
		return wire.Frame{Type: wire.MsgCommand, Payload: payload}, err
	case protocol.OutputResetRequest:
		return wire.Frame{Type: wire.MsgOutputResetRequest, Payload: wire.MarshalOutputResetRequest(m)}, nil
	case protocol.UIFence:
		payload, err := wire.MarshalUIFence(m)
		return wire.Frame{Type: wire.MsgUIFence, Payload: payload}, err
	case protocol.RemotePreviewRequest:
		payload := wire.MarshalRemotePreviewRequest(m)
		if payload == nil {
			return wire.Frame{}, ErrInvalidMessage
		}
		return wire.Frame{Type: wire.MsgRemotePreviewRequest, Payload: payload}, nil
	case protocol.RouteAttentionSubscription:
		payload, err := wire.MarshalRouteAttentionSubscription(m)
		return wire.Frame{Type: wire.MsgRouteAttentionSubscription, Payload: payload}, err
	case protocol.SamePeerSwitchRequest:
		payload, err := wire.MarshalSamePeerSwitchRequest(m)
		return wire.Frame{Type: wire.MsgSamePeerSwitchRequest, Payload: payload}, err
	case protocol.ParkedRouteRequest:
		payload := wire.MarshalParkedRouteRequest(m)
		if payload == nil {
			return wire.Frame{}, ErrInvalidMessage
		}
		return wire.Frame{Type: wire.MsgParkedRouteRequest, Payload: payload}, nil
	case protocol.RecentRouteSnapshot:
		payload, err := wire.MarshalRecentRouteSnapshot(m)
		return wire.Frame{Type: wire.MsgRecentRouteSnapshot, Payload: payload}, err
	case protocol.RouteNavigationFailure:
		payload, err := wire.MarshalRouteNavigationFailure(m)
		return wire.Frame{Type: wire.MsgRouteNavigationFailure, Payload: payload}, err
	case protocol.SessionCreationFailure:
		payload, err := wire.MarshalSessionCreationFailure(m)
		return wire.Frame{Type: wire.MsgSessionCreationFailure, Payload: payload}, err
	case *protocol.Hello:
		if m != nil {
			return encodeClient(*m)
		}
	case *protocol.Input:
		if m != nil {
			return encodeClient(*m)
		}
	case *protocol.Resize:
		if m != nil {
			return encodeClient(*m)
		}
	case *protocol.Detach:
		if m != nil {
			return encodeClient(*m)
		}
	case *protocol.Ping:
		if m != nil {
			return encodeClient(*m)
		}
	case *protocol.List:
		if m != nil {
			return encodeClient(*m)
		}
	case *protocol.Kill:
		if m != nil {
			return encodeClient(*m)
		}
	case *protocol.Theme:
		if m != nil {
			return encodeClient(*m)
		}
	case *protocol.Ack:
		if m != nil {
			return encodeClient(*m)
		}
	case *protocol.ImagePush:
		if m != nil {
			return encodeClient(*m)
		}
	case *protocol.ClientNotice:
		if m != nil {
			return encodeClient(*m)
		}
	case *protocol.CommandRequest:
		if m != nil {
			return encodeClient(*m)
		}
	case *protocol.OutputResetRequest:
		if m != nil {
			return encodeClient(*m)
		}
	case *protocol.UIFence:
		if m != nil {
			return encodeClient(*m)
		}
	case *protocol.RemotePreviewRequest:
		if m != nil {
			return encodeClient(*m)
		}
	case *protocol.RouteAttentionSubscription:
		if m != nil {
			return encodeClient(*m)
		}
	case *protocol.SamePeerSwitchRequest:
		if m != nil {
			return encodeClient(*m)
		}
	case *protocol.ParkedRouteRequest:
		if m != nil {
			return encodeClient(*m)
		}
	case *protocol.RecentRouteSnapshot:
		if m != nil {
			return encodeClient(*m)
		}
	case *protocol.RouteNavigationFailure:
		if m != nil {
			return encodeClient(*m)
		}
	case *protocol.SessionCreationFailure:
		if m != nil {
			return encodeClient(*m)
		}
	default:
		return wire.Frame{}, ErrWrongDirection
	}
	return wire.Frame{}, ErrInvalidMessage
}

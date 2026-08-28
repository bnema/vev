package sessionwire

import (
	"errors"
	"io"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

type scriptedTransport struct {
	recv      wire.Frame
	recvErr   error
	sent      wire.Frame
	sendErr   error
	closeErr  error
	closeCall int
}

func (t *scriptedTransport) Send(frame wire.Frame) error { t.sent = frame; return t.sendErr }
func (t *scriptedTransport) Recv() (wire.Frame, error)   { return t.recv, t.recvErr }
func (t *scriptedTransport) Close() error                { t.closeCall++; return t.closeErr }

type capableTransport struct {
	scriptedTransport
	async       wire.Frame
	synchronous wire.Frame
	events      chan ports.LinkEvent
}

func (*capableTransport) DatagramTransport() {}
func (t *capableTransport) SendAsync(frame wire.Frame) error {
	t.async = frame
	return t.sendErr
}
func (t *capableTransport) SendSynchronous(frame wire.Frame) error {
	t.synchronous = frame
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

func TestServerConnectionDecodesEveryClientMessage(t *testing.T) {
	target := testTarget()
	exact := protocol.ExactSessionTarget{LifecycleID: target.LifecycleID, SessionName: target.SessionName}
	lease := protocol.ParkedRouteLeaseID{1}
	commandPayload, err := wire.MarshalCommandRequest(protocol.CommandRequest{Version: protocol.Version, RequestID: 7, Slug: "list-sessions"})
	require.NoError(t, err)
	resizePayload, err := wire.MarshalResize(protocol.Resize{Size: domain.Size{Cols: 80, Rows: 24}})
	require.NoError(t, err)
	ackPayload, err := wire.MarshalAck(protocol.Ack{Epoch: 1, State: 2})
	require.NoError(t, err)
	attentionPayload, err := wire.MarshalRouteAttentionSubscription(protocol.RouteAttentionSubscription{})
	require.NoError(t, err)
	switchPayload, err := wire.MarshalSamePeerSwitchRequest(protocol.SamePeerSwitchRequest{RequestID: 3, Target: exact})
	require.NoError(t, err)
	snapshotPayload, err := wire.MarshalRecentRouteSnapshot(protocol.RecentRouteSnapshot{})
	require.NoError(t, err)
	failurePayload, err := wire.MarshalRouteNavigationFailure(protocol.RouteNavigationFailure{Key: 1, Generation: 1, Code: protocol.RouteFailureUnavailable})
	require.NoError(t, err)

	tests := []struct {
		name  string
		frame wire.Frame
		want  protocol.ClientMessage
	}{
		{name: "hello", frame: wire.Frame{Type: wire.MsgHello, Payload: wire.MarshalHello(protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}})}, want: protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}}},
		{name: "input", frame: wire.Frame{Type: wire.MsgInput, Payload: wire.MarshalInput(protocol.Input{InputSeq: 1, Data: []byte("x")})}, want: protocol.Input{InputSeq: 1, Data: []byte("x")}},
		{name: "resize", frame: wire.Frame{Type: wire.MsgResize, Payload: resizePayload}, want: protocol.Resize{Size: domain.Size{Cols: 80, Rows: 24}}},
		{name: "detach", frame: wire.Frame{Type: wire.MsgDetach}, want: protocol.Detach{}},
		{name: "ping", frame: wire.Frame{Type: wire.MsgPing}, want: protocol.Ping{}},
		{name: "list", frame: wire.Frame{Type: wire.MsgList}, want: protocol.List{}},
		{name: "kill", frame: wire.Frame{Type: wire.MsgKill, Payload: wire.MarshalKill(protocol.Kill{Name: "work"})}, want: protocol.Kill{Name: "work"}},
		{name: "theme", frame: wire.Frame{Type: wire.MsgTheme, Payload: wire.MarshalTheme(protocol.Theme{TrueColor: true})}, want: protocol.Theme{TrueColor: true}},
		{name: "ack", frame: wire.Frame{Type: wire.MsgAck, Payload: ackPayload}, want: protocol.Ack{Epoch: 1, State: 2}},
		{name: "image", frame: wire.Frame{Type: wire.MsgImagePush, Payload: wire.MarshalImagePush(protocol.ImagePush{InputSeq: 2, Mime: "image/png", Data: []byte{1}})}, want: protocol.ImagePush{InputSeq: 2, Mime: "image/png", Data: []byte{1}}},
		{name: "notice", frame: wire.Frame{Type: wire.MsgClientNotice, Payload: wire.MarshalClientNotice(protocol.ClientNotice{Action: protocol.ClientNoticeLinkConnected})}, want: protocol.ClientNotice{Action: protocol.ClientNoticeLinkConnected}},
		{name: "command", frame: wire.Frame{Type: wire.MsgCommand, Payload: commandPayload}, want: protocol.CommandRequest{Version: protocol.Version, RequestID: 7, Slug: "list-sessions"}},
		{name: "reset", frame: wire.Frame{Type: wire.MsgOutputResetRequest}, want: protocol.OutputResetRequest{}},
		{name: "preview", frame: wire.Frame{Type: wire.MsgRemotePreviewRequest, Payload: wire.MarshalRemotePreviewRequest(protocol.RemotePreviewRequest{Version: protocol.RemotePreviewSchemaVersion, Target: target, Width: 1, Height: 1})}, want: protocol.RemotePreviewRequest{Version: protocol.RemotePreviewSchemaVersion, Target: target, Width: 1, Height: 1}},
		{name: "attention", frame: wire.Frame{Type: wire.MsgRouteAttentionSubscription, Payload: attentionPayload}, want: protocol.RouteAttentionSubscription{Targets: []protocol.RouteAttentionTarget{}}},
		{name: "same peer", frame: wire.Frame{Type: wire.MsgSamePeerSwitchRequest, Payload: switchPayload}, want: protocol.SamePeerSwitchRequest{RequestID: 3, Target: exact}},
		{name: "parked", frame: wire.Frame{Type: wire.MsgParkedRouteRequest, Payload: wire.MarshalParkedRouteRequest(protocol.ParkedRouteRequest{RequestID: 4, LeaseID: lease, Action: protocol.ParkedRoutePrepare})}, want: protocol.ParkedRouteRequest{RequestID: 4, LeaseID: lease, Action: protocol.ParkedRoutePrepare}},
		{name: "snapshot", frame: wire.Frame{Type: wire.MsgRecentRouteSnapshot, Payload: snapshotPayload}, want: protocol.RecentRouteSnapshot{}},
		{name: "route failure", frame: wire.Frame{Type: wire.MsgRouteNavigationFailure, Payload: failurePayload}, want: protocol.RouteNavigationFailure{Key: 1, Generation: 1, Code: protocol.RouteFailureUnavailable}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := &scriptedTransport{recv: tt.frame}
			got, err := NewServerConnection(raw).ReceiveClient()
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestServerConnectionEncodesEveryServerMessage(t *testing.T) {
	exact := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "work"}
	lease := protocol.ParkedRouteLeaseID{1}
	tests := []struct {
		name    string
		message protocol.ServerMessage
		typeID  wire.MsgType
	}{
		{name: "welcome", message: protocol.Welcome{SessionID: "session"}, typeID: wire.MsgWelcome},
		{name: "error", message: protocol.ErrorMsg{Code: protocol.ErrInternal, Text: "error"}, typeID: wire.MsgError},
		{name: "output", message: protocol.Output{Epoch: 1, New: 1, Full: true, Size: domain.Size{Cols: 80, Rows: 24}}, typeID: wire.MsgOutput},
		{name: "detached", message: protocol.Detached{Reason: protocol.ReasonDetach}, typeID: wire.MsgDetached},
		{name: "pong", message: protocol.Pong{}, typeID: wire.MsgPong},
		{name: "sessions", message: protocol.Sessions{}, typeID: wire.MsgSessions},
		{name: "command result", message: protocol.CommandResult{RequestID: 1, OK: true}, typeID: wire.MsgCommandResult},
		{name: "navigation", message: protocol.NavigationDirective{Action: protocol.NavigationOpenHomePicker, LeaseID: lease}, typeID: wire.MsgNavigationAction},
		{name: "attach target", message: protocol.AttachTarget{Session: "work", Intent: protocol.IntentAttach}, typeID: wire.MsgAttachTarget},
		{name: "preview", message: protocol.RemotePreview{Version: protocol.RemotePreviewSchemaVersion, Status: protocol.RemotePreviewUnavailable}, typeID: wire.MsgRemotePreviewResponse},
		{name: "identity", message: protocol.CommittedRouteIdentity{Target: exact}, typeID: wire.MsgCommittedRouteIdentity},
		{name: "route action", message: protocol.RouteNavigationAction{SnapshotGeneration: 1, Key: 1, Generation: 1}, typeID: wire.MsgNavigateRecentRoute},
		{name: "route failure", message: protocol.RouteNavigationFailure{Key: 1, Generation: 1, Code: protocol.RouteFailureUnavailable}, typeID: wire.MsgRouteNavigationFailure},
		{name: "route position", message: protocol.RoutePosition{Target: exact, ActiveTabID: "tab-1"}, typeID: wire.MsgRoutePosition},
		{name: "switch failure", message: protocol.SamePeerSwitchFailure{RequestID: 1, Code: protocol.SamePeerSwitchUnavailable}, typeID: wire.MsgSamePeerSwitchFailure},
		{name: "parked response", message: protocol.ParkedRouteResponse{RequestID: 1, Status: protocol.ParkedRouteReady}, typeID: wire.MsgParkedRouteResponse},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := &scriptedTransport{}
			require.NoError(t, NewServerConnection(raw).SendServer(tt.message))
			require.Equal(t, tt.typeID, raw.sent.Type)
		})
	}
}

func TestServerConnectionPreservesPayloadAgnosticClientControls(t *testing.T) {
	for _, tt := range []struct {
		name  string
		frame wire.Frame
		want  protocol.ClientMessage
	}{
		{name: "list", frame: wire.Frame{Type: wire.MsgList, Payload: []byte{1}}, want: protocol.List{}},
		{name: "detach", frame: wire.Frame{Type: wire.MsgDetach, Payload: []byte{1}}, want: protocol.Detach{}},
		{name: "ping", frame: wire.Frame{Type: wire.MsgPing, Payload: []byte{1}}, want: protocol.Ping{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			message, err := NewServerConnection(&scriptedTransport{recv: tt.frame}).ReceiveClient()
			require.NoError(t, err)
			require.Equal(t, tt.want, message)
		})
	}
}

func TestServerConnectionClassifiesDecodeFailures(t *testing.T) {
	tests := []struct {
		name     string
		frame    wire.Frame
		category protocol.DecodeCategory
		version  uint16
		request  uint64
	}{
		{name: "malformed hello", frame: wire.Frame{Type: wire.MsgHello, Payload: []byte{0, 38}}, category: protocol.DecodeMalformed, version: 38},
		{name: "malformed command", frame: wire.Frame{Type: wire.MsgCommand, Payload: []byte{0, 37, 0, 0, 0, 0, 0, 0, 0, 9}}, category: protocol.DecodeMalformed, version: 37, request: 9},
		{name: "wrong direction", frame: wire.Frame{Type: wire.MsgOutput}, category: protocol.DecodeWrongDirection},
		{name: "unknown", frame: wire.Frame{Type: 255}, category: protocol.DecodeUnknownType},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewServerConnection(&scriptedTransport{recv: tt.frame}).ReceiveClient()
			var failure *protocol.DecodeFailure
			require.ErrorAs(t, err, &failure)
			require.Equal(t, tt.category, failure.Category)
			require.Equal(t, tt.version, failure.Version)
			require.Equal(t, tt.request, failure.RequestID)
		})
	}
}

func TestServerConnectionPreservesSendModesCapabilitiesAndErrors(t *testing.T) {
	sendErr := errors.New("send failed")
	events := make(chan ports.LinkEvent, 1)
	raw := &capableTransport{scriptedTransport: scriptedTransport{sendErr: sendErr}, events: events}
	connection := NewServerConnection(raw)

	caps := connection.Capabilities()
	require.Equal(t, uint8(1), caps.PreferredOutputWindow)
	require.Equal(t, protocol.MaxOutputDataLen, caps.OutputDataLimit)
	require.True(t, caps.AsyncSend)
	require.True(t, caps.OwnedSynchronousSend)
	require.True(t, caps.LinkState)
	require.Equal(t, ports.LinkStateDegraded, connection.LinkState())
	require.Equal(t, (<-chan ports.LinkEvent)(events), connection.LinkEvents())

	message := protocol.Pong{}
	require.ErrorIs(t, connection.SendServer(message), sendErr)
	require.Equal(t, wire.MsgPong, raw.sent.Type)
	require.ErrorIs(t, connection.SendServerAsync(message), sendErr)
	require.Equal(t, wire.MsgPong, raw.async.Type)
	require.ErrorIs(t, connection.SendServerSynchronous(message), sendErr)
	require.Equal(t, wire.MsgPong, raw.synchronous.Type)

	output := protocol.Output{Epoch: 1, New: 1, Full: true, Size: domain.Size{Cols: 80, Rows: 24}}
	require.ErrorIs(t, connection.SendOutput(output), sendErr)
	require.Equal(t, wire.MsgOutput, raw.sent.Type)
	require.ErrorIs(t, connection.SendOutputAsync(output), sendErr)
	require.Equal(t, wire.MsgOutput, raw.async.Type)
	require.ErrorIs(t, connection.SendOutputSynchronous(output), sendErr)
	require.Equal(t, wire.MsgOutput, raw.synchronous.Type)

	plain := NewServerConnection(&scriptedTransport{})
	require.ErrorIs(t, plain.SendServerAsync(message), ErrUnsupportedSend)
	require.ErrorIs(t, plain.SendServerSynchronous(message), ErrUnsupportedSend)
	require.ErrorIs(t, plain.SendOutputAsync(output), ErrUnsupportedSend)
	require.ErrorIs(t, plain.SendOutputSynchronous(output), ErrUnsupportedSend)
	require.Equal(t, uint8(8), plain.Capabilities().PreferredOutputWindow)
	require.Equal(t, ports.LinkStateConnected, plain.LinkState())
	require.Nil(t, plain.LinkEvents())
}

func TestServerConnectionForwardsReceiveAndCloseErrors(t *testing.T) {
	recvErr := errors.Join(io.EOF, errors.New("closed"))
	closeErr := errors.New("close failed")
	raw := &scriptedTransport{recvErr: recvErr, closeErr: closeErr}
	connection := NewServerConnection(raw)
	_, err := connection.ReceiveClient()
	require.ErrorIs(t, err, io.EOF)
	require.ErrorIs(t, connection.Close(), closeErr)
	require.Equal(t, 1, raw.closeCall)
}

func TestServerListenerWrapsEachAcceptedIncarnation(t *testing.T) {
	raw := &scriptedTransport{recv: wire.Frame{Type: wire.MsgPing}}
	listener := &scriptedListener{transport: raw}
	wrapped := NewServerListener(listener)
	first, err := wrapped.Accept()
	require.NoError(t, err)
	message, err := first.ReceiveClient()
	require.NoError(t, err)
	require.Equal(t, protocol.Ping{}, message)
	require.Equal(t, "test", wrapped.Addr())
	require.NoError(t, wrapped.Close())
}

type scriptedListener struct{ transport ports.Transport }

func (l *scriptedListener) Accept() (ports.Transport, error) { return l.transport, nil }
func (*scriptedListener) Close() error                       { return nil }
func (*scriptedListener) Addr() string                       { return "test" }

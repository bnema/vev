package sessionwire

import (
	"context"
	"errors"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

func TestClientConnectionEncodesEveryClientMessage(t *testing.T) {
	target := testTarget()
	exact := protocol.ExactSessionTarget{LifecycleID: target.LifecycleID, SessionName: target.SessionName}
	lease := protocol.ParkedRouteLeaseID{1}
	tests := []struct {
		name    string
		message protocol.ClientMessage
		typeID  wire.MsgType
	}{
		{name: "hello", message: protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}}, typeID: wire.MsgHello},
		{name: "input", message: protocol.Input{InputSeq: 1, Data: []byte("x")}, typeID: wire.MsgInput},
		{name: "resize", message: protocol.Resize{Size: domain.Size{Cols: 80, Rows: 24}}, typeID: wire.MsgResize},
		{name: "detach", message: protocol.Detach{}, typeID: wire.MsgDetach},
		{name: "ping", message: protocol.Ping{}, typeID: wire.MsgPing},
		{name: "list", message: protocol.List{}, typeID: wire.MsgList},
		{name: "kill", message: protocol.Kill{Name: "work"}, typeID: wire.MsgKill},
		{name: "theme", message: protocol.Theme{TrueColor: true}, typeID: wire.MsgTheme},
		{name: "ack", message: protocol.Ack{Epoch: 1, State: 1}, typeID: wire.MsgAck},
		{name: "image", message: protocol.ImagePush{InputSeq: 1, Mime: "image/png", Data: []byte{1}}, typeID: wire.MsgImagePush},
		{name: "notice", message: protocol.ClientNotice{Action: protocol.ClientNoticeLinkConnected}, typeID: wire.MsgClientNotice},
		{name: "command", message: protocol.CommandRequest{Version: protocol.Version, RequestID: 1, Slug: "list-sessions"}, typeID: wire.MsgCommand},
		{name: "reset", message: protocol.OutputResetRequest{}, typeID: wire.MsgOutputResetRequest},
		{name: "preview", message: protocol.RemotePreviewRequest{Version: protocol.RemotePreviewSchemaVersion, Target: target, Width: 1, Height: 1}, typeID: wire.MsgRemotePreviewRequest},
		{name: "attention", message: protocol.RouteAttentionSubscription{Targets: []protocol.RouteAttentionTarget{}}, typeID: wire.MsgRouteAttentionSubscription},
		{name: "same peer", message: protocol.SamePeerSwitchRequest{RequestID: 1, Target: exact}, typeID: wire.MsgSamePeerSwitchRequest},
		{name: "parked", message: protocol.ParkedRouteRequest{RequestID: 1, LeaseID: lease, Action: protocol.ParkedRoutePrepare}, typeID: wire.MsgParkedRouteRequest},
		{name: "snapshot", message: protocol.RecentRouteSnapshot{}, typeID: wire.MsgRecentRouteSnapshot},
		{name: "route failure", message: protocol.RouteNavigationFailure{Key: 1, Generation: 1, Code: protocol.RouteFailureUnavailable}, typeID: wire.MsgRouteNavigationFailure},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := &scriptedTransport{}
			require.NoError(t, NewClientConnection(raw).SendClient(tt.message))
			require.Equal(t, tt.typeID, raw.sent.Type)
			got, err := decodeClient(raw.sent)
			require.NoError(t, err)
			require.Equal(t, tt.message, got)
		})
	}
}

func TestClientConnectionDecodesEveryServerMessage(t *testing.T) {
	exact := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "work"}
	lease := protocol.ParkedRouteLeaseID{1}
	tests := []struct {
		name    string
		message protocol.ServerMessage
	}{
		{name: "welcome", message: protocol.Welcome{SessionID: "session"}},
		{name: "error", message: protocol.ErrorMsg{Code: protocol.ErrInternal, Text: "error"}},
		{name: "output", message: protocol.Output{Epoch: 1, New: 1, Full: true, Size: domain.Size{Cols: 80, Rows: 24}}},
		{name: "detached", message: protocol.Detached{Reason: protocol.ReasonDetach}},
		{name: "pong", message: protocol.Pong{}},
		{name: "sessions", message: protocol.Sessions{Sessions: []protocol.SessionInfo{}}},
		{name: "command result", message: protocol.CommandResult{RequestID: 1, OK: true}},
		{name: "navigation", message: protocol.NavigationDirective{Action: protocol.NavigationOpenHomePicker, LeaseID: lease}},
		{name: "attach target", message: protocol.AttachTarget{Session: "work", Intent: protocol.IntentAttach}},
		{name: "preview", message: protocol.RemotePreview{Version: protocol.RemotePreviewSchemaVersion, Status: protocol.RemotePreviewUnavailable}},
		{name: "identity", message: protocol.CommittedRouteIdentity{Target: exact}},
		{name: "route action", message: protocol.RouteNavigationAction{SnapshotGeneration: 1, Key: 1, Generation: 1}},
		{name: "route failure", message: protocol.RouteNavigationFailure{Key: 1, Generation: 1, Code: protocol.RouteFailureUnavailable}},
		{name: "route position", message: protocol.RoutePosition{Target: exact, ActiveTabID: "tab-1"}},
		{name: "switch failure", message: protocol.SamePeerSwitchFailure{RequestID: 1, Code: protocol.SamePeerSwitchUnavailable}},
		{name: "parked response", message: protocol.ParkedRouteResponse{RequestID: 1, Status: protocol.ParkedRouteReady}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame, err := encodeServer(tt.message)
			require.NoError(t, err)
			got, err := NewClientConnection(&scriptedTransport{recv: frame}).ReceiveServer()
			require.NoError(t, err)
			require.Equal(t, tt.message, got)
		})
	}
}

func TestClientConnectionClassifiesFailuresAndPreservesCapabilities(t *testing.T) {
	validError := wire.MarshalErrorMsg(protocol.ErrorMsg{Code: protocol.ErrInternal, Text: "error"})
	for _, tt := range []struct {
		name     string
		frame    wire.Frame
		category protocol.DecodeCategory
	}{
		{name: "wrong direction", frame: wire.Frame{Type: wire.MsgInput}, category: protocol.DecodeWrongDirection},
		{name: "truncated payload", frame: wire.Frame{Type: wire.MsgError, Payload: validError[:1]}, category: protocol.DecodeMalformed},
		{name: "trailing garbage", frame: wire.Frame{Type: wire.MsgError, Payload: append(append([]byte(nil), validError...), 0xff)}, category: protocol.DecodeMalformed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewClientConnection(&scriptedTransport{recv: tt.frame}).ReceiveServer()
			var failure *protocol.DecodeFailure
			require.ErrorAs(t, err, &failure)
			require.Equal(t, tt.category, failure.Category)
		})
	}

	events := make(chan ports.LinkEvent, 1)
	raw := &capableTransport{events: events}
	connection := NewClientConnection(raw)
	require.Equal(t, uint8(1), connection.Capabilities().PreferredOutputWindow)
	require.True(t, connection.Capabilities().LinkState)
	require.Equal(t, ports.LinkStateDegraded, connection.LinkState())
	require.Equal(t, (<-chan ports.LinkEvent)(events), connection.LinkEvents())
}

func TestClientDialerWrapsEachConnectionAndPreservesErrors(t *testing.T) {
	dialErr := errors.New("dial failed")
	dialer := NewClientDialer(&scriptedDialer{err: dialErr})
	_, err := dialer.Dial(context.Background())
	require.ErrorIs(t, err, dialErr)

	raw := &scriptedTransport{}
	dialer = NewClientDialer(&scriptedDialer{transport: raw})
	connection, err := dialer.Dial(context.Background())
	require.NoError(t, err)
	require.NoError(t, connection.SendClient(protocol.Ping{}))
	require.Equal(t, wire.MsgPing, raw.sent.Type)
}

type scriptedDialer struct {
	transport ports.Transport
	err       error
}

func (d *scriptedDialer) Dial(context.Context) (ports.Transport, error) {
	return d.transport, d.err
}

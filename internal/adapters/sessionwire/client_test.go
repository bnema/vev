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
	tests := []struct {
		name    string
		message protocol.ClientMessage
	}{
		{name: "hello", message: protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}}},
		{name: "input", message: protocol.Input{InputSeq: 1, Data: []byte("x")}},
		{name: "resize", message: protocol.Resize{Size: domain.Size{Cols: 80, Rows: 24}}},
		{name: "detach", message: protocol.Detach{}},
		{name: "ping", message: protocol.Ping{}},
		{name: "list", message: protocol.List{}},
		{name: "kill", message: protocol.Kill{Name: "work"}},
		{name: "theme", message: protocol.Theme{TrueColor: true}},
		{name: "ack", message: protocol.Ack{Epoch: 1, State: 1}},
		{name: "image", message: protocol.ImagePush{InputSeq: 1, Mime: "image/png", Data: []byte{1}}},
		{name: "notice", message: protocol.ClientNotice{Action: protocol.ClientNoticeLinkConnected}},
		{name: "command", message: protocol.CommandRequest{Version: protocol.Version, RequestID: 1, Slug: "list-sessions"}},
		{name: "reset", message: protocol.OutputResetRequest{}},
		{name: "UI fence", message: protocol.UIFence{ActionID: 7}},
		{name: "UI fence pointer", message: &protocol.UIFence{ActionID: 7}},
		{name: "preview", message: protocol.RemotePreviewRequest{Version: protocol.RemotePreviewSchemaVersion, Target: target, Width: 1, Height: 1}},
		{name: "attention", message: protocol.RouteAttentionSubscription{}},
		{name: "same peer", message: protocol.SamePeerSwitchRequest{RequestID: 1, Target: exact}},
		{name: "snapshot", message: protocol.RecentRouteSnapshot{}},
		{name: "route failure", message: protocol.RouteNavigationFailure{Key: 1, Generation: 1, Code: protocol.RouteFailureUnavailable}},
		{name: "creation failure", message: protocol.SessionCreationFailure{RequestID: 1, Code: protocol.RouteFailureUnavailable}},
		{name: "creation failure pointer", message: &protocol.SessionCreationFailure{RequestID: 1, Code: protocol.RouteFailureUnavailable}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := &scriptedTransport{recv: []wire.Envelope{mustPreambleResponse(t)}}
			require.NoError(t, NewClientConnection(raw).SendClient(tt.message))
			got, err := DecodeClientEnvelope(mustSingleAppPayload(t, raw))
			require.NoError(t, err)
			switch message := tt.message.(type) {
			case *protocol.SessionCreationFailure:
				require.Equal(t, *message, got)
			case *protocol.UIFence:
				require.Equal(t, *message, got)
			default:
				require.Equal(t, tt.message, got)
			}
		})
	}
}

func TestClientConnectionDecodesEveryServerMessage(t *testing.T) {
	exact := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "work"}
	tests := []struct {
		name    string
		message protocol.ServerMessage
	}{
		{name: "welcome", message: protocol.Welcome{SessionID: "session"}},
		{name: "error", message: protocol.ErrorMsg{Code: protocol.ErrInternal, Text: "error"}},
		{name: "output", message: protocol.Output{Epoch: 1, New: 1, Full: true, Size: domain.Size{Cols: 80, Rows: 24}, Context: testUIOutputContext()}},
		{name: "detached", message: protocol.Detached{Reason: protocol.ReasonDetach}},
		{name: "pong", message: protocol.Pong{}},
		{name: "sessions", message: protocol.Sessions{}},
		{name: "command result", message: protocol.CommandResult{RequestID: 1, OK: true}},
		{name: "attach target", message: protocol.AttachTarget{Session: "work", Intent: protocol.IntentAttach}},
		{name: "preview", message: protocol.RemotePreview{Version: protocol.RemotePreviewSchemaVersion, Status: protocol.RemotePreviewUnavailable}},
		{name: "identity", message: protocol.CommittedRouteIdentity{Target: exact}},
		{name: "route action", message: protocol.RouteNavigationAction{SnapshotGeneration: 1, Key: 1, Generation: 1}},
		{name: "route create", message: protocol.RouteCreateSessionAction{RequestID: 1, SnapshotGeneration: 1, Key: 1, Generation: 1, SessionName: "example"}},
		{name: "route failure", message: protocol.RouteNavigationFailure{Key: 1, Generation: 1, Code: protocol.RouteFailureUnavailable}},
		{name: "route position", message: protocol.RoutePosition{Target: exact, ActiveTabID: "tab-1"}},
		{name: "switch failure", message: protocol.SamePeerSwitchFailure{RequestID: 1, Code: protocol.SamePeerSwitchUnavailable}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := &scriptedTransport{recv: mustClientPreambleQueue(t, mustEncodeServer(t, tt.message))}
			got, err := NewClientConnection(raw).ReceiveServer()
			require.NoError(t, err)
			require.Equal(t, tt.message, got)
		})
	}
}

func TestClientConnectionClassifiesFailuresAndPreservesCapabilities(t *testing.T) {
	t.Run("wrong direction", func(t *testing.T) {
		// A client envelope on the server-to-client path is rejected
		// before any mutation: its fields are unknown to the server
		// envelope, so decoding reports the wrong-direction category.
		raw := &scriptedTransport{recv: mustClientPreambleQueue(t, mustEncodeClient(t, protocol.Input{InputSeq: 1, Data: []byte("x")}))}
		_, err := NewClientConnection(raw).ReceiveServer()
		var failure *protocol.DecodeFailure
		require.ErrorAs(t, err, &failure)
		require.Equal(t, protocol.DecodeUnknownType, failure.Category)
	})

	t.Run("truncated payload", func(t *testing.T) {
		valid := mustEncodeServer(t, protocol.ErrorMsg{Code: protocol.ErrInternal, Text: "error"})
		raw := &scriptedTransport{recv: mustClientPreambleQueue(t, valid[:1])}
		_, err := NewClientConnection(raw).ReceiveServer()
		var failure *protocol.DecodeFailure
		require.ErrorAs(t, err, &failure)
		require.Equal(t, protocol.DecodeMalformed, failure.Category)
	})

	t.Run("trailing garbage", func(t *testing.T) {
		valid := mustEncodeServer(t, protocol.ErrorMsg{Code: protocol.ErrInternal, Text: "error"})
		// A second top-level variant occurrence is trailing data, not
		// a longer message: the scanner rejects duplicate variants.
		duplicate := mustEncodeServer(t, protocol.Pong{})
		raw := &scriptedTransport{recv: mustClientPreambleQueue(t, append(append([]byte(nil), valid...), duplicate...))}
		_, err := NewClientConnection(raw).ReceiveServer()
		var failure *protocol.DecodeFailure
		require.ErrorAs(t, err, &failure)
		require.Equal(t, protocol.DecodeMalformed, failure.Category)
		require.ErrorIs(t, failure.Err, wire.ErrScanDuplicate)
	})

	t.Run("capabilities", func(t *testing.T) {
		events := make(chan ports.LinkEvent, 1)
		raw := &capableTransport{events: events}
		connection := NewClientConnection(raw)
		require.Equal(t, uint8(1), connection.Capabilities().PreferredOutputWindow)
		require.True(t, connection.Capabilities().LinkState)
		require.Equal(t, ports.LinkStateDegraded, connection.LinkState())
		require.Equal(t, (<-chan ports.LinkEvent)(events), connection.LinkEvents())
	})
}

func TestClientDialerWrapsEachConnectionAndPreservesErrors(t *testing.T) {
	dialErr := errors.New("dial failed")
	dialer := NewClientDialer(&scriptedDialer{err: dialErr})
	_, err := dialer.Dial(context.Background())
	require.ErrorIs(t, err, dialErr)

	raw := &scriptedTransport{recv: []wire.Envelope{mustPreambleResponse(t)}}
	dialer = NewClientDialer(&scriptedDialer{transport: raw})
	connection, err := dialer.Dial(context.Background())
	require.NoError(t, err)
	require.NoError(t, connection.SendClient(protocol.Ping{}))
	got, err := DecodeClientEnvelope(mustSingleAppPayload(t, raw))
	require.NoError(t, err)
	require.Equal(t, protocol.Ping{}, got)
}

type scriptedDialer struct {
	transport wire.Transport
	err       error
}

func (d *scriptedDialer) Dial(context.Context) (wire.Transport, error) {
	return d.transport, d.err
}

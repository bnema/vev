package sessionwire

import (
	"errors"
	"io"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

func TestServerConnectionDecodesEveryClientMessage(t *testing.T) {
	target := testTarget()
	exact := protocol.ExactSessionTarget{LifecycleID: target.LifecycleID, SessionName: target.SessionName}
	tests := []struct {
		name string
		want protocol.ClientMessage
	}{
		{name: "hello", want: protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}}},
		{name: "input", want: protocol.Input{InputSeq: 1, Data: []byte("x")}},
		{name: "resize", want: protocol.Resize{Size: domain.Size{Cols: 80, Rows: 24}}},
		{name: "detach", want: protocol.Detach{}},
		{name: "ping", want: protocol.Ping{}},
		{name: "list", want: protocol.List{}},
		{name: "kill", want: protocol.Kill{RequestID: 1, Name: "work"}},
		{name: "theme", want: protocol.Theme{TrueColor: true}},
		{name: "ack", want: protocol.Ack{Epoch: 1, State: 2}},
		{name: "image", want: protocol.ImagePush{InputSeq: 2, Mime: "image/png", Data: []byte{1}}},
		{name: "notice", want: protocol.ClientNotice{Action: protocol.ClientNoticeLinkConnected}},
		{name: "command", want: protocol.CommandRequest{Version: protocol.Version, RequestID: 7, Slug: "list-sessions"}},
		{name: "reset", want: protocol.OutputResetRequest{}},
		{name: "preview", want: protocol.RemotePreviewRequest{Version: protocol.RemotePreviewSchemaVersion, Target: target, Width: 1, Height: 1}},
		{name: "attention", want: protocol.RouteAttentionSubscription{}},
		{name: "same peer", want: protocol.SamePeerSwitchRequest{RequestID: 3, Target: exact}},
		{name: "snapshot", want: protocol.RecentRouteSnapshot{}},
		{name: "route failure", want: protocol.RouteNavigationFailure{Key: 1, Generation: 1, Code: protocol.RouteFailureUnavailable}},
		{name: "creation failure", want: protocol.SessionCreationFailure{RequestID: 1, Code: protocol.RouteFailureUnavailable}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := &scriptedTransport{recv: mustServerPreambleQueue(t, mustEncodeClient(t, tt.want))}
			got, err := NewServerConnection(raw).ReceiveClient()
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestServerConnectionEncodesEveryServerMessage(t *testing.T) {
	exact := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "work"}
	welcome := protocol.Welcome{SessionID: "session"}
	errorMessage := protocol.ErrorMsg{Code: protocol.ErrInternal, Text: "error"}
	output := protocol.Output{Epoch: 1, New: 1, Full: true, Size: domain.Size{Cols: 80, Rows: 24}, Context: testUIOutputContext()}
	receipt := protocol.UIReceipt{ActionID: 7, Epoch: 1, State: 1, ViewPublication: 1, Outcome: protocol.UIReceiptProcessed}
	viewUpdate := protocol.UIViewUpdate{Epoch: 1, State: 1, Context: *testUIOutputContext()}
	detached := protocol.Detached{Reason: protocol.ReasonDetach}
	pong := protocol.Pong{}
	sessions := protocol.Sessions{}
	commandResult := protocol.CommandResult{RequestID: 1, Outcome: protocol.CommandSucceeded}
	killResult := protocol.KillResult{RequestID: 1, Outcome: protocol.KillSucceeded}
	killFailure := protocol.KillResult{RequestID: 2, Outcome: protocol.KillFailed, Code: protocol.ErrInternal, Text: "partial", Failures: []protocol.KillFailure{{Class: "stopped", Name: "work", Text: "boom"}}}
	attachTarget := protocol.AttachTarget{Session: "work", Intent: protocol.IntentAttach}
	preview := protocol.RemotePreview{Version: protocol.RemotePreviewSchemaVersion, Status: protocol.RemotePreviewUnavailable}
	identity := protocol.CommittedRouteIdentity{Target: exact}
	routeAction := protocol.RouteNavigationAction{SnapshotGeneration: 1, Key: 1, Generation: 1}
	routeCreate := protocol.RouteCreateSessionAction{RequestID: 1, SnapshotGeneration: 1, Key: 1, Generation: 1, SessionName: "example"}
	routeFailure := protocol.RouteNavigationFailure{Key: 1, Generation: 1, Code: protocol.RouteFailureUnavailable}
	routePosition := protocol.RoutePosition{Target: exact, ActiveTabID: "tab-1"}
	switchFailure := protocol.SamePeerSwitchFailure{RequestID: 1, Code: protocol.SamePeerSwitchUnavailable}

	tests := []struct {
		name    string
		message protocol.ServerMessage
	}{
		{name: "welcome", message: welcome},
		{name: "error", message: errorMessage},
		{name: "output", message: output},
		{name: "receipt", message: receipt},
		{name: "receipt pointer", message: &receipt},
		{name: "view update", message: viewUpdate},
		{name: "view update pointer", message: &viewUpdate},
		{name: "detached", message: detached},
		{name: "pong", message: pong},
		{name: "sessions", message: sessions},
		{name: "command result", message: commandResult},
		{name: "kill result", message: killResult},
		{name: "kill result partial failures", message: killFailure},
		{name: "attach target", message: attachTarget},
		{name: "preview", message: preview},
		{name: "identity", message: identity},
		{name: "route action", message: routeAction},
		{name: "route create", message: routeCreate},
		{name: "route create pointer", message: &routeCreate},
		{name: "route failure", message: routeFailure},
		{name: "route position", message: routePosition},
		{name: "switch failure", message: switchFailure},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := &scriptedTransport{}
			require.NoError(t, NewServerConnection(raw).SendServer(tt.message))
			require.Equal(t, 1, raw.sentLen())
			got, err := DecodeServerEnvelope(raw.sentPayload(0))
			require.NoError(t, err)
			switch message := tt.message.(type) {
			case *protocol.UIReceipt:
				require.Equal(t, *message, got)
			case *protocol.UIViewUpdate:
				require.Equal(t, *message, got)
			case *protocol.RouteCreateSessionAction:
				require.Equal(t, *message, got)
			default:
				require.Equal(t, tt.message, got)
			}
		})
	}
}

func TestServerConnectionPreservesPayloadAgnosticClientControls(t *testing.T) {
	// Empty control envelopes decode without payload-specific validation.
	for _, tt := range []struct {
		name    string
		message protocol.ClientMessage
	}{
		{name: "list", message: protocol.List{}},
		{name: "detach", message: protocol.Detach{}},
		{name: "ping", message: protocol.Ping{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			raw := &scriptedTransport{recv: mustServerPreambleQueue(t, mustEncodeClient(t, tt.message))}
			message, err := NewServerConnection(raw).ReceiveClient()
			require.NoError(t, err)
			require.Equal(t, tt.message, message)
		})
	}
}

func helloWithVersion(t *testing.T, version uint32) []byte {
	t.Helper()
	converted, err := helloToWire(protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach})
	require.NoError(t, err)
	converted.Version = version
	raw, err := proto.Marshal(&wire.ClientEnvelope{
		Payload: &wire.ClientEnvelope_Hello{Hello: converted},
	})
	require.NoError(t, err)
	return raw
}

func TestServerConnectionClassifiesDecodeFailures(t *testing.T) {
	validCommand, err := commandRequestToWire(protocol.CommandRequest{Version: protocol.Version, RequestID: 9, Slug: "list-sessions"})
	require.NoError(t, err)
	validCommandRaw, err := proto.Marshal(&wire.ClientEnvelope{
		Payload: &wire.ClientEnvelope_CommandRequest{CommandRequest: validCommand},
	})
	require.NoError(t, err)
	tests := []struct {
		name       string
		payload    []byte
		category   protocol.DecodeCategory
		version    uint16
		request    uint64
		hasRequest bool
	}{
		{name: "malformed hello", payload: helloWithVersion(t, 38), category: protocol.DecodeMalformed, version: 38},
		{name: "truncated command", payload: validCommandRaw[:1], category: protocol.DecodeMalformed},

		{name: "unknown", payload: []byte{0xf8, 0x07}, category: protocol.DecodeUnknownType},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := &scriptedTransport{recv: mustServerPreambleQueue(t, tt.payload)}
			_, err := NewServerConnection(raw).ReceiveClient()
			var failure *protocol.DecodeFailure
			require.ErrorAs(t, err, &failure)
			require.Equal(t, tt.category, failure.Category)
			require.Equal(t, tt.version, failure.Version)
			require.Equal(t, tt.request, failure.RequestID)
			require.Equal(t, tt.hasRequest, failure.HasRequestID)
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
	require.Equal(t, 1, raw.sentLen())
	got, err := DecodeServerEnvelope(raw.sentPayload(0))
	require.NoError(t, err)
	require.Equal(t, message, got)
	require.ErrorIs(t, connection.SendServerAsync(message), sendErr)
	require.Equal(t, 1, len(raw.async))
	require.ErrorIs(t, connection.SendServerSynchronous(message), sendErr)
	require.Equal(t, 1, len(raw.synchronous))

	output := protocol.Output{Epoch: 1, New: 1, Full: true, Size: domain.Size{Cols: 80, Rows: 24}, Context: testUIOutputContext()}
	require.ErrorIs(t, connection.SendOutput(output), sendErr)
	require.ErrorIs(t, connection.SendOutputAsync(output), sendErr)
	require.ErrorIs(t, connection.SendOutputSynchronous(output), sendErr)

	plain := NewServerConnection(&scriptedTransport{})
	require.ErrorIs(t, plain.SendServerAsync(message), ErrUnsupportedSend)
	require.ErrorIs(t, plain.SendServerSynchronous(message), ErrUnsupportedSend)
	require.ErrorIs(t, plain.SendOutputAsync(output), ErrUnsupportedSend)
	require.ErrorIs(t, plain.SendOutputSynchronous(output), ErrUnsupportedSend)
	require.Equal(t, uint8(protocol.MaxOutputWindow), plain.Capabilities().PreferredOutputWindow)
	require.Equal(t, ports.LinkStateConnected, plain.LinkState())
	require.Nil(t, plain.LinkEvents())
}

func TestServerConnectionForwardsReceiveAndCloseErrors(t *testing.T) {
	recvErr := errors.Join(io.EOF, errors.New("closed"))
	closeErr := errors.New("close failed")
	// The queued preamble request is consumed, then the preamble
	// itself fails on the joined error; the failure surfaces as a
	// typed decode failure chaining io.EOF, and the failed attempt
	// closes the transport before the explicit Close.
	raw := &scriptedTransport{recv: mustServerPreambleQueue(t), recvErr: recvErr, closeErr: closeErr}
	connection := NewServerConnection(raw)
	_, err := connection.ReceiveClient()
	require.ErrorIs(t, err, io.EOF)
	var failure *protocol.DecodeFailure
	require.ErrorAs(t, err, &failure)
	require.Equal(t, protocol.DecodeMalformed, failure.Category)
	require.ErrorIs(t, connection.Close(), closeErr)
	require.Equal(t, 2, raw.closeCalls())
}

func TestServerListenerWrapsEachAcceptedIncarnation(t *testing.T) {
	raw := &scriptedTransport{recv: mustServerPreambleQueue(t, mustEncodeClient(t, protocol.Ping{}))}
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

type scriptedListener struct{ transport wire.Transport }

func (l *scriptedListener) Accept() (wire.Transport, error) { return l.transport, nil }
func (*scriptedListener) Close() error                      { return nil }
func (*scriptedListener) Addr() string                      { return "test" }

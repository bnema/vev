package client_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// mustServerEnvelope wraps one semantic server message in the envelope the
// wire.Transport exchanges. Replay-only output fixtures that predate the
// publication context still model valid v41 semantic publications, so the
// context is filled in before encoding.
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

// mustClientEnvelope wraps one semantic client message in the envelope the
// wire.Transport exchanges.
func mustClientEnvelope(message protocol.ClientMessage) wire.Envelope {
	raw, err := sessionwire.EncodeClientMessage(message)
	if err != nil {
		panic(err)
	}
	return wire.Envelope{Payload: raw}
}

// decodeClientMessageForTest unwraps one client envelope for assertions.
func decodeClientMessageForTest(t *testing.T, envelope wire.Envelope) protocol.ClientMessage {
	t.Helper()
	message, err := sessionwire.DecodeClientEnvelope(envelope.Payload)
	require.NoError(t, err)
	return message
}

// clientMessageName mirrors the internal package's naming table so external
// transport expectations can match typed client messages by kind.
func clientMessageName(t *testing.T, envelope wire.Envelope) string {
	if t != nil {
		t.Helper()
	}
	message, err := sessionwire.DecodeClientEnvelope(envelope.Payload)
	if err != nil {
		return ""
	}
	switch message.(type) {
	case protocol.SuspendAttachment:
		return "SuspendAttachment"
	case protocol.ActivateAttachment:
		return "ActivateAttachment"
	case protocol.Hello:
		return "Hello"
	case protocol.Input:
		return "Input"
	case protocol.Resize:
		return "Resize"
	case protocol.Detach:
		return "Detach"
	case protocol.Ping:
		return "Ping"
	case protocol.List:
		return "List"
	case protocol.Kill:
		return "Kill"
	case protocol.Theme:
		return "Theme"
	case protocol.Ack:
		return "Ack"
	case protocol.ImagePush:
		return "ImagePush"
	case protocol.ClientNotice:
		return "ClientNotice"
	case protocol.CommandRequest:
		return "CommandRequest"
	case protocol.OutputResetRequest:
		return "OutputResetRequest"
	case protocol.UIFence:
		return "UIFence"
	case protocol.RemotePreviewRequest:
		return "RemotePreviewRequest"
	case protocol.RouteAttentionSubscription:
		return "RouteAttentionSubscription"
	case protocol.SamePeerSwitchRequest:
		return "SamePeerSwitchRequest"
	case protocol.RecentRouteSnapshot:
		return "RecentRouteSnapshot"
	case protocol.RouteNavigationFailure:
		return "RouteNavigationFailure"
	case protocol.SessionCreationFailure:
		return "SessionCreationFailure"
	case protocol.NavigationInventoryRequest:
		return "NavigationInventoryRequest"
	case protocol.NavigationInventoryPublication:
		return "NavigationInventoryPublication"
	case protocol.NavigationInventoryFailure:
		return "NavigationInventoryFailure"
	case protocol.PickerBegin:
		return "PickerBegin"
	case protocol.PickerClose:
		return "PickerClose"
	case protocol.PickerSelection:
		return "PickerSelection"
	case protocol.PickerPreviewRequest:
		return "PickerPreviewRequest"
	case protocol.PickerControlRequest:
		return "PickerControlRequest"
	default:
		return ""
	}
}

// serverMessageNameForTest mirrors the internal package's naming table for
// server envelope payloads.
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

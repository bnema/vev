package wire

import (
	"testing"

	"github.com/bnema/vev/internal/protocol"
)

func TestProtocolVersion(t *testing.T) {
	if protocol.Version != 40 {
		t.Fatalf("ProtocolVersion = %d, want 40", protocol.Version)
	}
}

func TestControlMsgTypes(t *testing.T) {
	if MsgCommand != 12 {
		t.Fatalf("MsgCommand = %d, want 12", MsgCommand)
	}
	if MsgCommandResult != 22 {
		t.Fatalf("MsgCommandResult = %d, want 22", MsgCommandResult)
	}
	if MsgOutputResetRequest != 13 {
		t.Fatalf("MsgOutputResetRequest = %d, want 13", MsgOutputResetRequest)
	}
}

func TestMsgTypeUnique(t *testing.T) {
	if MsgNavigationAction != 23 {
		t.Fatalf("MsgNavigationAction = %d, want 23", MsgNavigationAction)
	}
	tests := []struct {
		name string
		typ  MsgType
	}{
		{"MsgHello", MsgHello}, {"MsgInput", MsgInput}, {"MsgResize", MsgResize},
		{"MsgDetach", MsgDetach}, {"MsgPing", MsgPing}, {"MsgList", MsgList},
		{"MsgKill", MsgKill}, {"MsgTheme", MsgTheme}, {"MsgAck", MsgAck},
		{"MsgImagePush", MsgImagePush}, {"MsgClientNotice", MsgClientNotice},
		{"MsgCommand", MsgCommand}, {"MsgOutputResetRequest", MsgOutputResetRequest}, {"MsgRemotePreviewRequest", MsgRemotePreviewRequest},
		{"MsgRouteAttentionSubscription", MsgRouteAttentionSubscription}, {"MsgSamePeerSwitchRequest", MsgSamePeerSwitchRequest}, {"MsgParkedRouteRequest", MsgParkedRouteRequest}, {"MsgSessionCreationFailure", MsgSessionCreationFailure},
		{"MsgWelcome", MsgWelcome}, {"MsgError", MsgError}, {"MsgOutput", MsgOutput},
		{"MsgDetached", MsgDetached}, {"MsgPong", MsgPong}, {"MsgSessions", MsgSessions},
		{"MsgCommandResult", MsgCommandResult},
		{"MsgRemotePreviewResponse", MsgRemotePreviewResponse}, {"MsgAttachTarget", MsgAttachTarget}, {"MsgNavigationAction", MsgNavigationAction},
		{"MsgCommittedRouteIdentity", MsgCommittedRouteIdentity}, {"MsgRecentRouteSnapshot", MsgRecentRouteSnapshot},
		{"MsgNavigateRecentRoute", MsgNavigateRecentRoute}, {"MsgRouteNavigationFailure", MsgRouteNavigationFailure},
		{"MsgRoutePosition", MsgRoutePosition}, {"MsgSamePeerSwitchFailure", MsgSamePeerSwitchFailure}, {"MsgParkedRouteResponse", MsgParkedRouteResponse}, {"MsgRouteCreateSession", MsgRouteCreateSession},
	}

	seen := make(map[MsgType]string, len(tests))
	for _, tt := range tests {
		if prev, ok := seen[tt.typ]; ok {
			t.Errorf("MsgType value %d used by both %s and %s", tt.typ, prev, tt.name)
		}
		seen[tt.typ] = tt.name
	}
	if len(seen) != len(tests) {
		t.Fatalf("expected %d distinct MsgType values, got %d", len(tests), len(seen))
	}
}

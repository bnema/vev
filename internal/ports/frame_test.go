package ports

import "testing"

func TestProtocolVersion(t *testing.T) {
	if ProtocolVersion != 26 {
		t.Fatalf("ProtocolVersion = %d, want 26", ProtocolVersion)
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
		{"MsgWelcome", MsgWelcome}, {"MsgError", MsgError}, {"MsgOutput", MsgOutput},
		{"MsgDetached", MsgDetached}, {"MsgPong", MsgPong}, {"MsgSessions", MsgSessions},
		{"MsgCommandResult", MsgCommandResult},
		{"MsgRemotePreviewResponse", MsgRemotePreviewResponse}, {"MsgAttachTarget", MsgAttachTarget}, {"MsgNavigationAction", MsgNavigationAction},
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

func TestNavigationActionFrame(t *testing.T) {
	frame := Frame{Type: MsgNavigationAction, Payload: MarshalNavigationAction(NavigationOpenHomePicker)}
	if frame.Type != MsgNavigationAction {
		t.Fatalf("frame type = %d, want %d", frame.Type, MsgNavigationAction)
	}
	action, err := UnmarshalNavigationAction(frame.Payload)
	if err != nil {
		t.Fatalf("UnmarshalNavigationAction() error = %v", err)
	}
	if action != NavigationOpenHomePicker {
		t.Fatalf("action = %d, want %d", action, NavigationOpenHomePicker)
	}
}

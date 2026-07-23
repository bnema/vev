package ports

import "testing"

func TestProtocolVersion(t *testing.T) {
	if ProtocolVersion != 18 {
		t.Fatalf("ProtocolVersion = %d, want 18", ProtocolVersion)
	}
}

func TestControlMsgTypes(t *testing.T) {
	if MsgCommand != 12 {
		t.Fatalf("MsgCommand = %d, want 12", MsgCommand)
	}
	if MsgCommandResult != 22 {
		t.Fatalf("MsgCommandResult = %d, want 22", MsgCommandResult)
	}
}

func TestMsgTypeUnique(t *testing.T) {
	tests := []struct {
		name string
		typ  MsgType
	}{
		{"MsgHello", MsgHello},
		{"MsgInput", MsgInput},
		{"MsgResize", MsgResize},
		{"MsgDetach", MsgDetach},
		{"MsgPing", MsgPing},
		{"MsgList", MsgList},
		{"MsgKill", MsgKill},
		{"MsgTheme", MsgTheme},
		{"MsgAck", MsgAck},
		{"MsgImagePush", MsgImagePush},
		{"MsgClientNotice", MsgClientNotice},
		{"MsgCommand", MsgCommand},
		{"MsgWelcome", MsgWelcome},
		{"MsgError", MsgError},
		{"MsgOutput", MsgOutput},
		{"MsgDetached", MsgDetached},
		{"MsgPong", MsgPong},
		{"MsgSessions", MsgSessions},
		{"MsgCommandResult", MsgCommandResult},
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

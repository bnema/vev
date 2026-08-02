package ports

import "testing"

func TestProtocolVersion(t *testing.T) {
	if ProtocolVersion != 21 {
		t.Fatalf("ProtocolVersion = %d, want 21", ProtocolVersion)
	}
}

func TestScreenUpdateMessageType(t *testing.T) {
	if MsgScreenUpdate != 24 {
		t.Fatalf("MsgScreenUpdate = %d, want 24", MsgScreenUpdate)
	}
	if MaxFrameLen != 16<<20 {
		t.Fatalf("MaxFrameLen = %d, want %d", MaxFrameLen, 16<<20)
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
	if MsgSessionMeta != 23 {
		t.Fatalf("MsgSessionMeta = %d, want 23", MsgSessionMeta)
	}
	if CapabilityProxied != 1<<3 {
		t.Fatalf("CapabilityProxied = %d, want 8", CapabilityProxied)
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
		{"MsgOutputResetRequest", MsgOutputResetRequest},
		{"MsgWelcome", MsgWelcome},
		{"MsgError", MsgError},
		{"MsgOutput", MsgOutput},
		{"MsgDetached", MsgDetached},
		{"MsgPong", MsgPong},
		{"MsgSessions", MsgSessions},
		{"MsgCommandResult", MsgCommandResult},
		{"MsgSessionMeta", MsgSessionMeta},
		{"MsgScreenUpdate", MsgScreenUpdate},
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

package ports

import "testing"

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
		{"MsgWelcome", MsgWelcome},
		{"MsgError", MsgError},
		{"MsgOutput", MsgOutput},
		{"MsgDetached", MsgDetached},
		{"MsgPong", MsgPong},
		{"MsgSessions", MsgSessions},
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

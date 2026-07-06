package app

import "testing"

func TestParseRemoteAttachTarget(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantTarget  string
		wantSession string
		wantOK      bool
	}{
		{name: "local plain", input: "work"},
		{name: "local with colon", input: "work:logs"},
		{name: "missing user", input: "@host"},
		{name: "missing host", input: "user@"},
		{name: "remote no session", input: "user@example.com", wantTarget: "user@example.com", wantOK: true},
		{name: "remote with session", input: "user@example.com:work", wantTarget: "user@example.com", wantSession: "work", wantOK: true},
		{name: "remote empty session allowed", input: "user@example.com:", wantTarget: "user@example.com", wantOK: true},
		{name: "bracketed ipv6 no session", input: "user@[2001:db8::1]", wantTarget: "user@[2001:db8::1]", wantOK: true},
		{name: "bracketed ipv6 with session", input: "user@[2001:db8::1]:work", wantTarget: "user@[2001:db8::1]", wantSession: "work", wantOK: true},
		{name: "malformed bracketed ipv6", input: "user@[2001:db8::1"},
		{name: "target with at in userinfo", input: "first@user@example.com:work", wantTarget: "first@user@example.com", wantSession: "work", wantOK: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTarget, gotSession, gotOK := parseRemoteAttachTarget(tt.input)
			if gotOK != tt.wantOK {
				t.Fatalf("ok = %v, want %v", gotOK, tt.wantOK)
			}
			if gotTarget != tt.wantTarget {
				t.Fatalf("target = %q, want %q", gotTarget, tt.wantTarget)
			}
			if gotSession != tt.wantSession {
				t.Fatalf("session = %q, want %q", gotSession, tt.wantSession)
			}
		})
	}
}

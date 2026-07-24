package app

import "testing"

func TestParseVEVEnv(t *testing.T) {
	tests := []struct {
		name    string
		env     string
		session string
		tab     string
		pane    string
		ok      bool
	}{
		{"normal", "session=dev,tab=t_abc,pane=p_def", "dev", "t_abc", "p_def", true},
		{"escaped components", "session=a%20b,tab=t%2Cx,pane=p%3Dy", "a b", "t,x", "p=y", true},
		{"empty", "", "", "", "", false},
		{"missing pane", "session=dev,tab=t_abc", "", "", "", false},
		{"garbage", "not-a-vev-value", "", "", "", false},
		{"unknown key", "session=dev,tab=t_abc,pane=p_def,extra=x", "", "", "", false},
		{"duplicate key", "session=dev,session=other,tab=t_abc,pane=p_def", "", "", "", false},
		{"bad escape", "session=dev%2,tab=t_abc,pane=p_def", "", "", "", false},
		{"lowercase escape is invalid", "session=dev%2fwork,tab=t_abc,pane=p_def", "", "", "", false},
		{"unescaped byte is invalid", "session=dev work,tab=t_abc,pane=p_def", "", "", "", false},
		{"raw UTF-8 is invalid", "session=dév,tab=t_abc,pane=p_def", "", "", "", false},
		{"empty component", "session=dev,tab=,pane=p_def", "", "", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := parseVEVEnv(tt.env)
			if ok != tt.ok {
				t.Fatalf("parseVEVEnv(%q) ok = %v, want %v", tt.env, ok, tt.ok)
			}
			if ok && (got.session != tt.session || got.tab != tt.tab || got.pane != tt.pane) {
				t.Fatalf("parseVEVEnv(%q) = %+v", tt.env, got)
			}
		})
	}
}

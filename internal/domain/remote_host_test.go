package domain

import (
	"slices"
	"testing"
)

func TestRemoteHostTargetValidation(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		wantErr bool
	}{
		{name: "alias", target: "arch"},
		{name: "user at host", target: "build@mule"},
		{name: "bracketed ipv6 with port", target: "user@[2001:db8::1]:2222"},
		{name: "empty", target: "", wantErr: true},
		{name: "leading whitespace", target: " arch", wantErr: true},
		{name: "trailing whitespace", target: "arch ", wantErr: true},
		{name: "internal whitespace", target: "build @mule", wantErr: true},
		{name: "newline", target: "arch\n", wantErr: true},
		{name: "tab", target: "arch\t", wantErr: true},
		{name: "control byte", target: "arch\x00", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRemoteHostTarget(tt.target)
			if tt.wantErr && err == nil {
				t.Fatalf("ValidateRemoteHostTarget(%q) = nil, want error", tt.target)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("ValidateRemoteHostTarget(%q) = %v, want nil", tt.target, err)
			}
		})
	}
}

func TestRemoteHostUniqueTargets(t *testing.T) {
	tests := []struct {
		name  string
		input []string
		want  []string
	}{
		{
			name:  "preserves first occurrence order",
			input: []string{"arch", "build@mule", "arch", "mule"},
			want:  []string{"arch", "build@mule", "mule"},
		},
		{
			name:  "empty input",
			input: nil,
			want:  []string{},
		},
		{
			name:  "no duplicates",
			input: []string{"alpha", "beta"},
			want:  []string{"alpha", "beta"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := UniqueRemoteHostTargets(tt.input)
			if got == nil {
				t.Fatalf("UniqueRemoteHostTargets(%v) = nil, want non-nil slice", tt.input)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("UniqueRemoteHostTargets(%v) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestRemoteHostConfigDefaults(t *testing.T) {
	cfg := Defaults()
	if !cfg.Remote.Enabled {
		t.Fatal("Defaults().Remote.Enabled = false, want true")
	}
	if !cfg.Remote.Remember {
		t.Fatal("Defaults().Remote.Remember = false, want true")
	}
	if cfg.Remote.Hosts == nil {
		t.Fatal("Defaults().Remote.Hosts = nil, want empty slice")
	}
	if len(cfg.Remote.Hosts) != 0 {
		t.Fatalf("Defaults().Remote.Hosts = %v, want empty", cfg.Remote.Hosts)
	}
}

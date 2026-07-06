package domain

import "testing"

func TestValidateSessionName(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{name: "a"},
		{name: "work"},
		{name: "Work_1.2-3"},
		{name: "a123456789012345678901234567890123456789012345678901234567890123"},
		{name: "", wantErr: true},
		{name: "my work", wantErr: true},
		{name: "-work", wantErr: true},
		{name: "_work", wantErr: true},
		{name: ".work", wantErr: true},
		{name: "work/one", wantErr: true},
		{name: "a1234567890123456789012345678901234567890123456789012345678901234", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSessionName(tt.name)
			if tt.wantErr && err == nil {
				t.Fatalf("ValidateSessionName(%q) = nil, want error", tt.name)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("ValidateSessionName(%q) = %v, want nil", tt.name, err)
			}
		})
	}
}

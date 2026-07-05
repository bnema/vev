package platform

import (
	"path/filepath"
	"testing"
)

func TestConfigPath(t *testing.T) {
	tests := []struct {
		name      string
		xdgConfig string
		home      string
	}{
		{
			name:      "xdg config home",
			xdgConfig: filepath.Join(t.TempDir(), "xdg-config"),
			home:      filepath.Join(t.TempDir(), "home"),
		},
		{
			name: "home fallback",
			home: filepath.Join(t.TempDir(), "home"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("XDG_CONFIG_HOME", tt.xdgConfig)
			t.Setenv("HOME", tt.home)

			want := filepath.Join(tt.xdgConfig, "vev", "config")
			if tt.xdgConfig == "" {
				want = filepath.Join(tt.home, ".config", "vev", "config")
			}
			if got := ConfigPath(); got != want {
				t.Fatalf("ConfigPath() = %q, want %q", got, want)
			}
		})
	}
}

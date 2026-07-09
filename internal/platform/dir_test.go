package platform

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStateDir(t *testing.T) {
	tests := []struct {
		name     string
		xdgState string
		home     string
		want     func(*testing.T, string, string) string
	}{
		{
			name:     "xdg state home",
			xdgState: filepath.Join(t.TempDir(), "xdg-state"),
			home:     filepath.Join(t.TempDir(), "home"),
			want: func(_ *testing.T, xdgState, _ string) string {
				return filepath.Join(xdgState, "vev")
			},
		},
		{
			name: "home fallback",
			home: filepath.Join(t.TempDir(), "home"),
			want: func(_ *testing.T, _, home string) string {
				return filepath.Join(home, ".local", "state", "vev")
			},
		},
		{
			name: "temp fallback without home",
			want: func(t *testing.T, _, _ string) string {
				t.Helper()
				return filepath.Join(os.TempDir(), "vev-state-")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("XDG_STATE_HOME", tt.xdgState)
			t.Setenv("HOME", tt.home)

			want := tt.want(t, tt.xdgState, tt.home)
			got := StateDir()
			if strings.HasSuffix(want, "vev-state-") {
				if !strings.HasPrefix(got, want) {
					t.Fatalf("StateDir() = %q, want prefix %q", got, want)
				}
				return
			}
			if got != want {
				t.Fatalf("StateDir() = %q, want %q", got, want)
			}
		})
	}
}

func TestConfigPath(t *testing.T) {
	tests := []struct {
		name      string
		xdgConfig string
		home      string
		want      func(*testing.T, string, string) string
	}{
		{
			name:      "xdg config home",
			xdgConfig: filepath.Join(t.TempDir(), "xdg-config"),
			home:      filepath.Join(t.TempDir(), "home"),
			want: func(_ *testing.T, xdgConfig, _ string) string {
				return filepath.Join(xdgConfig, "vev", "config")
			},
		},
		{
			name: "home fallback",
			home: filepath.Join(t.TempDir(), "home"),
			want: func(_ *testing.T, _, home string) string {
				return filepath.Join(home, ".config", "vev", "config")
			},
		},
		{
			name: "temp fallback without home",
			want: func(t *testing.T, _, _ string) string {
				t.Helper()
				return filepath.Join(os.TempDir(), "vev-config-")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("XDG_CONFIG_HOME", tt.xdgConfig)
			t.Setenv("HOME", tt.home)

			want := tt.want(t, tt.xdgConfig, tt.home)
			got := ConfigPath()
			if strings.HasSuffix(want, "vev-config-") {
				if !strings.HasPrefix(got, want) || filepath.Base(got) != "config" {
					t.Fatalf("ConfigPath() = %q, want prefix %q and base config", got, want)
				}
				return
			}
			if got != want {
				t.Fatalf("ConfigPath() = %q, want %q", got, want)
			}
		})
	}
}

func TestDirOrHome(t *testing.T) {
	existing := t.TempDir()
	home := filepath.Join(t.TempDir(), "home")
	nonexistent := filepath.Join(t.TempDir(), "missing")

	tests := []struct {
		name string
		dir  string
		home string
		want string
	}{
		{name: "existing dir", dir: existing, home: home, want: existing},
		{name: "non-existent dir falls back to home", dir: nonexistent, home: home, want: home},
		{name: "non-existent dir without home falls back to root", dir: nonexistent, want: "/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("HOME", tt.home)

			if got := DirOrHome(tt.dir); got != tt.want {
				t.Fatalf("DirOrHome(%q) = %q, want %q", tt.dir, got, tt.want)
			}
		})
	}
}

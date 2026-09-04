package platform

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestActivateDevelopmentEnvironment(t *testing.T) {
	originalCwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(originalCwd); err != nil {
			t.Errorf("restore working directory: %v", err)
		}
	})

	distinctCwd := t.TempDir()
	tests := []struct {
		name        string
		environment string
		cwd         string
		wantErr     bool
		check       func(*testing.T, string)
	}{
		{
			name: "unset preserves existing XDG roots",
			check: func(t *testing.T, _ string) {
				t.Helper()
				if got := os.Getenv("XDG_CONFIG_HOME"); got != "/existing/config" {
					t.Errorf("XDG_CONFIG_HOME = %q", got)
				}
				if got := os.Getenv("XDG_STATE_HOME"); got != "/existing/state" {
					t.Errorf("XDG_STATE_HOME = %q", got)
				}
				if got := os.Getenv("XDG_RUNTIME_DIR"); got != "/existing/runtime" {
					t.Errorf("XDG_RUNTIME_DIR = %q", got)
				}
			},
		},
		{
			name:        "valid name overrides XDG roots",
			environment: "dev",
			check: func(t *testing.T, cwd string) {
				t.Helper()
				root := filepath.Join(cwd, ".dev", "dev")
				if got := os.Getenv("XDG_CONFIG_HOME"); got != filepath.Join(root, "config") {
					t.Errorf("XDG_CONFIG_HOME = %q", got)
				}
				if got := os.Getenv("XDG_STATE_HOME"); got != filepath.Join(root, "state") {
					t.Errorf("XDG_STATE_HOME = %q", got)
				}
				if got := os.Getenv("XDG_RUNTIME_DIR"); got != filepath.Join(root, "runtime") {
					t.Errorf("XDG_RUNTIME_DIR = %q", got)
				}
				if got, want := os.Getenv("VEV_ENV_ROOT"), filepath.Join(cwd, ".dev"); got != want {
					t.Errorf("VEV_ENV_ROOT = %q, want %q", got, want)
				}
				if _, err := os.Stat(root); !os.IsNotExist(err) {
					t.Errorf("activation created root or returned unexpected error: %v", err)
				}
			},
		},
		{
			name:        "resolved root survives cwd change",
			environment: "persisted",
			check: func(t *testing.T, cwd string) {
				t.Helper()
				root := filepath.Join(cwd, ".dev", "persisted")
				other := t.TempDir()
				if err := os.Chdir(other); err != nil {
					t.Fatal(err)
				}
				if err := ActivateDevelopmentEnvironment(); err != nil {
					t.Fatal(err)
				}
				if got := os.Getenv("XDG_STATE_HOME"); got != filepath.Join(root, "state") {
					t.Errorf("XDG_STATE_HOME after cwd change = %q", got)
				}
			},
		},
		{name: "distinct name alpha", environment: "alpha", cwd: distinctCwd},
		{name: "distinct name beta", environment: "beta", cwd: distinctCwd},
		{name: "reject traversal", environment: "../live", wantErr: true},
		{name: "reject separator", environment: "a/b", wantErr: true},
		{name: "reject whitespace", environment: "two words", wantErr: true},
		{name: "reject leading punctuation", environment: ".hidden", wantErr: true},
		{name: "reject 65 characters", environment: strings.Repeat("a", 65), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cwd := tt.cwd
			if cwd == "" {
				cwd = t.TempDir()
			}
			if err := os.Chdir(cwd); err != nil {
				t.Fatal(err)
			}
			canonicalCwd, getwdErr := os.Getwd()
			if getwdErr != nil {
				t.Fatal(getwdErr)
			}
			t.Setenv("VEV_ENV", tt.environment)
			t.Setenv("VEV_ENV_ROOT", "")
			t.Setenv("XDG_CONFIG_HOME", "/existing/config")
			t.Setenv("XDG_STATE_HOME", "/existing/state")
			t.Setenv("XDG_RUNTIME_DIR", "/existing/runtime")

			err := ActivateDevelopmentEnvironment()
			if tt.wantErr {
				if err == nil || !strings.Contains(err.Error(), "VEV_ENV") {
					t.Fatalf("ActivateDevelopmentEnvironment() error = %v", err)
				}
				for name, want := range map[string]string{
					"XDG_CONFIG_HOME": "/existing/config",
					"XDG_STATE_HOME":  "/existing/state",
					"XDG_RUNTIME_DIR": "/existing/runtime",
					"VEV_ENV_ROOT":    "",
				} {
					if got := os.Getenv(name); got != want {
						t.Errorf("%s changed to %q, want %q", name, got, want)
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tt.check != nil {
				tt.check(t, canonicalCwd)
			}
		})
	}
}

func TestDevelopmentEnvironmentTempDir(t *testing.T) {
	absoluteBase := filepath.Join(t.TempDir(), ".dev")
	tests := []struct {
		name        string
		environment string
		base        string
		want        string
		wantActive  bool
	}{
		{name: "inactive without environment", base: absoluteBase},
		{name: "inactive without inherited base", environment: "work"},
		{
			name:        "active environment",
			environment: "work",
			base:        absoluteBase,
			want:        filepath.Join(absoluteBase, "work", "tmp"),
			wantActive:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("VEV_ENV", tt.environment)
			t.Setenv("VEV_ENV_ROOT", tt.base)

			got, active := DevelopmentEnvironmentTempDir()
			if got != tt.want || active != tt.wantActive {
				t.Fatalf("DevelopmentEnvironmentTempDir() = (%q, %v), want (%q, %v)", got, active, tt.want, tt.wantActive)
			}
			if active && !filepath.IsAbs(got) {
				t.Errorf("DevelopmentEnvironmentTempDir() = %q, want absolute path", got)
			}
		})
	}
}

func TestActivateDevelopmentEnvironmentRecomputesProfileFromInheritedBase(t *testing.T) {
	originalCwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(originalCwd); err != nil {
			t.Errorf("restore working directory: %v", err)
		}
	})

	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	invocationCwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("VEV_ENV", "alpha")
	t.Setenv("VEV_ENV_ROOT", "")
	if err := ActivateDevelopmentEnvironment(); err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(invocationCwd, ".dev")
	if got := os.Getenv("VEV_ENV_ROOT"); got != base {
		t.Fatalf("VEV_ENV_ROOT = %q, want %q", got, base)
	}

	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("VEV_ENV", "beta"); err != nil {
		t.Fatal(err)
	}
	if err := ActivateDevelopmentEnvironment(); err != nil {
		t.Fatal(err)
	}

	betaRoot := filepath.Join(base, "beta")
	for name, want := range map[string]string{
		"VEV_ENV_ROOT":    base,
		"XDG_CONFIG_HOME": filepath.Join(betaRoot, "config"),
		"XDG_STATE_HOME":  filepath.Join(betaRoot, "state"),
		"XDG_RUNTIME_DIR": filepath.Join(betaRoot, "runtime"),
	} {
		if got := os.Getenv(name); got != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
}

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

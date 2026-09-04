package platform

import (
	"fmt"
	"os"
	"path/filepath"
)

const devEnvironmentRootEnv = "VEV_ENV_ROOT"

// ActivateDevelopmentEnvironment redirects vev's XDG roots into an isolated
// development tree when VEV_ENV is set. The absolute .dev base is retained in
// VEV_ENV_ROOT so re-executed roles do not rebase after changing directory.
func ActivateDevelopmentEnvironment() error {
	name := os.Getenv("VEV_ENV")
	if name == "" {
		return nil
	}
	if !validDevelopmentEnvironmentName(name) {
		return fmt.Errorf("vev: invalid VEV_ENV %q (want one safe segment matching [A-Za-z0-9][A-Za-z0-9._-]{0,63})", name)
	}

	base := os.Getenv(devEnvironmentRootEnv)
	if base == "" {
		absolute, err := filepath.Abs(".dev")
		if err != nil {
			return fmt.Errorf("vev: resolve VEV_ENV %q root: %w", name, err)
		}
		base = absolute
	} else if !filepath.IsAbs(base) {
		return fmt.Errorf("vev: invalid %s %q (want an absolute path)", devEnvironmentRootEnv, base)
	}
	root := filepath.Join(base, name)

	values := map[string]string{
		devEnvironmentRootEnv: base,
		"XDG_CONFIG_HOME":     filepath.Join(root, "config"),
		"XDG_STATE_HOME":      filepath.Join(root, "state"),
		"XDG_RUNTIME_DIR":     filepath.Join(root, "runtime"),
	}
	type previousValue struct {
		value string
		set   bool
	}
	previous := make(map[string]previousValue, len(values))
	for key := range values {
		value, set := os.LookupEnv(key)
		previous[key] = previousValue{value: value, set: set}
	}
	for key, value := range values {
		if err := os.Setenv(key, value); err != nil {
			for rollbackKey, old := range previous {
				if old.set {
					_ = os.Setenv(rollbackKey, old.value)
				} else {
					_ = os.Unsetenv(rollbackKey)
				}
			}
			return fmt.Errorf("vev: activate VEV_ENV %q: %w", name, err)
		}
	}
	return nil
}

func validDevelopmentEnvironmentName(name string) bool {
	if len(name) == 0 || len(name) > 64 || !asciiAlphaNumeric(name[0]) {
		return false
	}
	for i := 1; i < len(name); i++ {
		if !asciiAlphaNumeric(name[i]) && name[i] != '.' && name[i] != '_' && name[i] != '-' {
			return false
		}
	}
	return true
}

func asciiAlphaNumeric(value byte) bool {
	return value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z' || value >= '0' && value <= '9'
}

// DevelopmentEnvironmentTempDir returns the private temporary-file directory
// for an activated development environment. Ordinary invocations use the
// existing os.TempDir fallback in the daemon.
func DevelopmentEnvironmentTempDir() (string, bool) {
	name := os.Getenv("VEV_ENV")
	base := os.Getenv(devEnvironmentRootEnv)
	if name == "" || base == "" {
		return "", false
	}
	return filepath.Join(base, name, "tmp"), true
}

// StateDir returns the directory vev writes state to: $XDG_STATE_HOME/vev if
// set, else ~/.local/state/vev.
func StateDir() string {
	if xdg := os.Getenv("XDG_STATE_HOME"); xdg != "" {
		return filepath.Join(xdg, "vev")
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		// Last resort: a per-uid temp path so state setup never aborts startup.
		return filepath.Join(os.TempDir(), fmt.Sprintf("vev-state-%d", os.Getuid()))
	}
	return filepath.Join(home, ".local", "state", "vev")
}

// ConfigPath returns the user config file path: $XDG_CONFIG_HOME/vev/config if
// set, else ~/.config/vev/config.
func ConfigPath() string {
	if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
		return filepath.Join(xdg, "vev", "config")
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		// Last resort: a per-uid temp path so config lookup never aborts startup.
		return filepath.Join(os.TempDir(), fmt.Sprintf("vev-config-%d", os.Getuid()), "config")
	}
	return filepath.Join(home, ".config", "vev", "config")
}

// DirOrHome returns dir when it names an existing directory. Otherwise it
// falls back to the current user's home directory, then to / as a last resort.
func DirOrHome(dir string) string {
	if dir != "" {
		if st, err := os.Stat(dir); err == nil && st.IsDir() {
			return dir
		}
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return home
	}
	return "/"
}

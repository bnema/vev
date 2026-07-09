package platform

import (
	"fmt"
	"os"
	"path/filepath"
)

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

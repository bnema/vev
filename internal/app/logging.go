package app

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// logFileName is the fixed name of vev's shared log file, used by both the
// daemon and the raw-attached client (which must never log to the console).
const logFileName = "vev.log"

// stateDir returns the directory vev writes its log to: $XDG_STATE_HOME/vev
// if set, else ~/.local/state/vev.
func stateDir() string {
	if xdg := os.Getenv("XDG_STATE_HOME"); xdg != "" {
		return filepath.Join(xdg, "vev")
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		// Last resort: a per-uid temp path so logging never aborts startup.
		return filepath.Join(os.TempDir(), fmt.Sprintf("vev-state-%d", os.Getuid()))
	}
	return filepath.Join(home, ".local", "state", "vev")
}

// parseLogLevel maps VEV_LOG to a slog level, defaulting to info.
func parseLogLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// setupLogging installs a JSON slog handler writing to the shared log file
// and returns the open file so the caller can close it on exit. The level
// comes from VEV_LOG (default info). Both the daemon and the client route
// diagnostics here — the client because console output while the terminal is
// raw would corrupt the display.
//
// No log rotation is performed for the MVP (future work).
func setupLogging() (*os.File, error) {
	dir := stateDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("vev: creating state dir: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, logFileName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("vev: opening log file: %w", err)
	}
	handler := slog.NewJSONHandler(f, &slog.HandlerOptions{Level: parseLogLevel(os.Getenv("VEV_LOG"))})
	slog.SetDefault(slog.New(handler))
	return f, nil
}

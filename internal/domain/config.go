package domain

// ThemeMode selects how vev derives its color scheme.
type ThemeMode int

const (
	// ThemeAuto follows the attached client's reported color scheme.
	ThemeAuto ThemeMode = iota
	// ThemeDark forces vev's built-in dark palette.
	ThemeDark
	// ThemeLight forces vev's built-in light palette.
	ThemeLight
)

// ConfigEntry preserves one parsed config key/value in file order.
type ConfigEntry struct {
	Key   string
	Value string
}

// SnapshotConfig contains user-configurable snapshot restore settings.
type SnapshotConfig struct {
	RestoreProcesses    []string
	RestoreProcessesSet bool
}

// defaultSnapshotRestoreProcesses is the default allowlist for process restore.
var defaultSnapshotRestoreProcesses = []string{
	"vi", "vim", "nvim", "emacs", "man", "less", "more", "tail", "top", "htop", "btop",
	"claude", "codex", "pi", "opencode",
}

// DefaultSnapshotRestoreProcesses returns a copy of the default process restore allowlist.
func DefaultSnapshotRestoreProcesses() []string {
	return append([]string(nil), defaultSnapshotRestoreProcesses...)
}

// Config is the user-editable vev configuration after parsing. Unknown binding
// keys are preserved here (in BindingEntries, in file order) so the usecase
// layer can decide which actions it understands.
type Config struct {
	Theme          ThemeMode
	BindingEntries []ConfigEntry
	Codes          map[string]string
	Snapshot       SnapshotConfig
}

// Warning describes a non-fatal config problem. Parsers and reloaders should
// warn and continue with defaults rather than aborting startup.
type Warning struct {
	Line int
	Msg  string
}

// Defaults returns vev's default configuration.
func Defaults() Config {
	return Config{
		Theme:          ThemeAuto,
		BindingEntries: []ConfigEntry{},
		Codes:          map[string]string{},
		Snapshot: SnapshotConfig{
			RestoreProcesses: DefaultSnapshotRestoreProcesses(),
		},
	}
}

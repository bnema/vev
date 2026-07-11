package domain

import "time"

// MinBarInterval is the minimum supported bar refresh interval.
const MinBarInterval = time.Second

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

// BarConfig contains user-configurable bar right-anchor settings.
type BarConfig struct {
	TopRight    string
	BottomRight string
	Interval    time.Duration
}

// PaletteConfig contains palette placement settings. A false AnchorSet uses the
// adaptive automatic placement.
type PaletteConfig struct {
	Anchor    Anchor
	AnchorSet bool
}

// FloatingConfig contains launch and sizing settings for per-tab floating panes.
type FloatingConfig struct {
	Command string
	Width   int
	Height  int
}

// TabsConfig contains tab label display settings.
type TabsConfig struct {
	// TerminalTitle includes the focused pane's OSC terminal title in tab
	// labels (top bar and session picker). The process name always shows.
	TerminalTitle bool
}

// Config is the user-editable vev configuration after parsing. Unknown binding
// keys are preserved here (in BindingEntries, in file order) so the usecase
// layer can decide which actions it understands.
type Config struct {
	Theme          ThemeMode
	Bar            BarConfig
	BindingEntries []ConfigEntry
	Codes          map[string]string
	Snapshot       SnapshotConfig
	Palette        PaletteConfig
	Floating       FloatingConfig
	Tabs           TabsConfig
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
		Theme: ThemeAuto,
		Bar: BarConfig{
			TopRight:    "vev-bar-top-right",
			BottomRight: "vev-bar-bottom-right",
			Interval:    5 * time.Second,
		},
		BindingEntries: []ConfigEntry{},
		Codes:          map[string]string{},
		Snapshot: SnapshotConfig{
			RestoreProcesses: DefaultSnapshotRestoreProcesses(),
		},
		Floating: FloatingConfig{
			Width:  80,
			Height: 80,
		},
		Tabs: TabsConfig{
			TerminalTitle: true,
		},
	}
}

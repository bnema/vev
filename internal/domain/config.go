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

// Config is the user-editable vev configuration after parsing. Unknown binding
// keys are preserved here (in BindingEntries, in file order) so the usecase
// layer can decide which actions it understands.
type Config struct {
	Theme          ThemeMode
	BindingEntries []ConfigEntry
	Codes          map[string]string
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
	}
}

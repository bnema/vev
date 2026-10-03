// Package terminalcap owns pure terminal capability values and detection policy.
package terminalcap

import (
	"fmt"
	"strings"
)

// ColorMode is the color output mode selected for one attachment.
type ColorMode uint8

const (
	// TrueColor is the zero value so manually constructed attachments retain the
	// historical renderer behavior; live Hello paths always use the detector.
	TrueColor ColorMode = iota
	ANSI256
	ANSI16
	Monochrome
)

// Valid reports whether m is one of the defined color modes.
func (m ColorMode) Valid() bool { return m <= Monochrome }

// ColorCapabilities is the color output capability selected for one attachment
// and how confidently it was selected.
type ColorCapabilities struct {
	Mode   ColorMode
	Source Source
}

// Valid reports whether both the mode and the source are defined values.
func (c ColorCapabilities) Valid() bool { return c.Mode.Valid() && c.Source.Valid() }

// RGB reports whether the attachment can receive RGB ANSI output.
func (c ColorCapabilities) RGB() bool { return c.Mode == TrueColor }

// Colors reports the number of colors the attachment can display.
func (c ColorCapabilities) Colors() int {
	switch c.Mode {
	case ANSI256:
		return 256
	case ANSI16:
		return 16
	case Monochrome:
		return 0
	default:
		return 1 << 24
	}
}

// Source records how confidently a terminal capability was selected.
type Source uint8

const (
	SourceUnknown Source = iota
	SourceHeuristic
	SourceDeclared
	// SourceForced is an explicit user override that detection must not
	// second-guess and that never warrants a downgrade notice.
	SourceForced
)

// Valid reports whether s is one of the defined sources.
func (s Source) Valid() bool { return s <= SourceForced }

// Application identifies a known terminal application when its environment
// provides a trustworthy origin signal.
type Application uint8

const (
	ApplicationUnknown Application = iota
	ApplicationKitty
)

// Capabilities describes the output features selected for one client attachment.
type Capabilities struct {
	Color         ColorCapabilities
	Application   Application
	KittyGraphics bool
}

// SupportsKittyGraphics reports whether the active outer-terminal probe
// accepted the Kitty graphics protocol. Environment detection never sets it.
func (c Capabilities) SupportsKittyGraphics() bool { return c.KittyGraphics }

// Detect derives conservative attachment capabilities from a client environment.
func Detect(env []string) Capabilities {
	values := environmentValues(env)
	term := strings.ToLower(strings.TrimSpace(values["TERM"]))
	colorTerm := strings.ToLower(strings.TrimSpace(values["COLORTERM"]))
	caps := Capabilities{Color: ColorCapabilities{Mode: ANSI256}}

	if values["KITTY_WINDOW_ID"] != "" || values["KITTY_PID"] != "" || values["KITTY_LISTEN_ON"] != "" {
		caps.Application = ApplicationKitty
	}

	kittyIdentity := term == "xterm-kitty" && caps.Application == ApplicationKitty
	switch colorTerm {
	case "truecolor", "24bit":
		caps.Color.Mode = TrueColor
		caps.Color.Source = SourceDeclared
	}
	if term == "xterm-direct" || strings.HasSuffix(term, "-direct") {
		caps.Color.Mode = TrueColor
		caps.Color.Source = SourceDeclared
	}
	if kittyIdentity {
		if caps.Color.Source == SourceUnknown {
			caps.Color.Source = SourceHeuristic
		}
		caps.Color.Mode = TrueColor
		return caps
	}
	if caps.Color.Source == SourceDeclared {
		return caps
	}
	if mode, ok := termColorMode(term); ok {
		caps.Color.Mode = mode
		caps.Color.Source = SourceDeclared
	}
	return caps
}

// termColorMode classifies a lowercase TERM value that carries no truecolor
// signal. It is deliberately conservative: only well-known terminfo names map
// to a constrained mode, and anything else reports ok=false so the caller keeps
// its unverified 256-color default. The rules apply in this order:
//
//   - monochrome: dumb, vt52, vt100, vt102, vt220, or a name ending in -m or -mono;
//   - 256 colors: any name containing "256color";
//   - 16 colors: linux, ansi, cons25, or a name ending in -16color or -color.
func termColorMode(term string) (ColorMode, bool) {
	switch term {
	case "dumb", "vt52", "vt100", "vt102", "vt220":
		return Monochrome, true
	case "linux", "ansi", "cons25":
		return ANSI16, true
	}
	switch {
	case strings.HasSuffix(term, "-m"), strings.HasSuffix(term, "-mono"):
		return Monochrome, true
	case strings.Contains(term, "256color"):
		return ANSI256, true
	case strings.HasSuffix(term, "-16color"), strings.HasSuffix(term, "-color"):
		return ANSI16, true
	}
	return 0, false
}

// ParseColorMode parses a user color override: auto, truecolor, 256, 16, or
// mono, case-insensitively and ignoring surrounding space. An empty value is
// auto. When auto is true the caller must use detection and mode is
// meaningless; an unknown value returns an error.
func ParseColorMode(value string) (mode ColorMode, auto bool, err error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "auto":
		return TrueColor, true, nil
	case "truecolor":
		return TrueColor, false, nil
	case "256":
		return ANSI256, false, nil
	case "16":
		return ANSI16, false, nil
	case "mono":
		return Monochrome, false, nil
	}
	return TrueColor, false, fmt.Errorf("invalid color mode %q (want auto, truecolor, 256, 16, or mono)", value)
}

func environmentValues(env []string) map[string]string {
	values := make(map[string]string, len(env))
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			values[key] = value
		}
	}
	return values
}

// Resolve selects attachment capabilities from the client environment and the
// client's color claim. A declared or forced client color is used verbatim. A
// heuristic truecolor claim upgrades a weaker daemon-side detection, because
// the daemon may not see the client environment that produced the inference.
// Any other claim falls back to environment detection.
func Resolve(env []string, declared ColorCapabilities) Capabilities {
	caps := Detect(env)
	switch {
	case declared.Source == SourceDeclared || declared.Source == SourceForced:
		caps.Color = declared
	case declared.Source == SourceHeuristic && declared.RGB() && !caps.Color.RGB():
		caps.Color = declared
	}
	return caps
}

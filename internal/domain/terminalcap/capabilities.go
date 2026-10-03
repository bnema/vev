// Package terminalcap owns pure terminal capability values and detection policy.
package terminalcap

import "strings"

// ColorMode is the color output mode selected for one attachment.
type ColorMode uint8

const (
	// TrueColor is the zero value so manually constructed attachments retain the
	// historical renderer behavior; live Hello paths always use the detector.
	TrueColor ColorMode = iota
	ANSI256
)

// ColorCapabilities is the color output capability selected for one attachment
// and how confidently it was selected.
type ColorCapabilities struct {
	Mode   ColorMode
	Source Source
}

// RGB reports whether the attachment can receive RGB ANSI output.
func (c ColorCapabilities) RGB() bool { return c.Mode == TrueColor }

// Colors reports the number of colors the attachment can display.
func (c ColorCapabilities) Colors() int {
	switch c.Mode {
	case ANSI256:
		return 256
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
)

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
	if strings.Contains(term, "256color") || term == "dumb" {
		caps.Color.Source = SourceDeclared
	}
	return caps
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
// client's explicit color declaration. A declared truecolor client upgrades a
// weaker environment detection to declared TrueColor.
func Resolve(env []string, declaredTrueColor bool) Capabilities {
	caps := Detect(env)
	if declaredTrueColor && !caps.Color.RGB() {
		caps.Color = ColorCapabilities{Mode: TrueColor, Source: SourceDeclared}
	}
	return caps
}

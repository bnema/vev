// Package terminalcap owns pure terminal capability values and detection policy.
package terminalcap

import "strings"

// ColorMode is the color output mode selected for one attachment.
type ColorMode uint8

const (
	// TrueColor is the zero value so manually constructed attachments retain the
	// historical renderer behavior; live Hello paths always use the detector.
	TrueColor ColorMode = iota
	Indexed256
)

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
	ColorMode     ColorMode
	ColorSource   Source
	Application   Application
	KittyGraphics bool
}

// SupportsKittyGraphics reports whether the active outer-terminal probe
// accepted the Kitty graphics protocol. Environment detection never sets it.
func (c Capabilities) SupportsKittyGraphics() bool { return c.KittyGraphics }

// TrueColor reports whether this attachment can receive RGB ANSI output.
func (c Capabilities) TrueColor() bool { return c.ColorMode == TrueColor }

// Detect derives conservative attachment capabilities from a client environment.
func Detect(env []string) Capabilities {
	values := environmentValues(env)
	term := strings.ToLower(strings.TrimSpace(values["TERM"]))
	colorTerm := strings.ToLower(strings.TrimSpace(values["COLORTERM"]))
	caps := Capabilities{ColorMode: Indexed256}

	if values["KITTY_WINDOW_ID"] != "" || values["KITTY_PID"] != "" || values["KITTY_LISTEN_ON"] != "" {
		caps.Application = ApplicationKitty
	}

	kittyIdentity := term == "xterm-kitty" && caps.Application == ApplicationKitty
	switch colorTerm {
	case "truecolor", "24bit":
		caps.ColorMode = TrueColor
		caps.ColorSource = SourceDeclared
	}
	if term == "xterm-direct" || strings.HasSuffix(term, "-direct") {
		caps.ColorMode = TrueColor
		caps.ColorSource = SourceDeclared
	}
	if kittyIdentity {
		if caps.ColorSource == SourceUnknown {
			caps.ColorSource = SourceHeuristic
		}
		caps.ColorMode = TrueColor
		return caps
	}
	if caps.ColorSource == SourceDeclared {
		return caps
	}
	if strings.Contains(term, "256color") || term == "dumb" {
		caps.ColorSource = SourceDeclared
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

// DetectTrueColor reports whether the supplied terminal environment advertises
// direct color support. Explicit TERM/COLORTERM values override env entries.
func DetectTrueColor(termEnv, colorTerm string, env []string) bool {
	detectionEnv := make([]string, 0, len(env)+2)
	detectionEnv = append(detectionEnv, env...)
	detectionEnv = append(detectionEnv, "TERM="+termEnv, "COLORTERM="+colorTerm)
	return Detect(detectionEnv).TrueColor()
}

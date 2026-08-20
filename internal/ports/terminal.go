package ports

import (
	"io"
	"strings"

	"github.com/bnema/vev/internal/domain"
)

// Terminal is the CLIENT-side controlling terminal.
type Terminal interface {
	EnterRaw() (restore func() error, err error)
	Geometry() (domain.Geometry, error)
	ResizeEvents() <-chan domain.Geometry
	In() io.Reader
	Out() io.Writer
	Flush() error
}

// TerminalColorMode is the color output mode selected for one attachment.
type TerminalColorMode uint8

const (
	// TrueColor is the zero value so manually constructed attachments retain the
	// historical renderer behavior; live Hello paths always use the detector.
	TerminalColorTrueColor TerminalColorMode = iota
	TerminalColorIndexed256
)

// TerminalCapabilitySource records how confidently a terminal capability was
// selected. Environment observations are advisory because nested multiplexers
// can retain values from an outer terminal.
type TerminalCapabilitySource uint8

const (
	TerminalCapabilityUnknown TerminalCapabilitySource = iota
	TerminalCapabilityHeuristic
	TerminalCapabilityDeclared
)

// TerminalApplication identifies a known terminal application when its
// environment provides a trustworthy origin signal. It does not imply support
// for any application-specific protocol on the current output path.
type TerminalApplication uint8

const (
	TerminalApplicationUnknown TerminalApplication = iota
	TerminalApplicationKitty
)

// TerminalCapabilities describes the output features selected for one client
// attachment. Pane processes must not derive their terminal environment from
// these capabilities.
type TerminalCapabilities struct {
	ColorMode   TerminalColorMode
	ColorSource TerminalCapabilitySource
	Application TerminalApplication
}

// TrueColor reports whether this attachment can receive RGB ANSI output.
func (c TerminalCapabilities) TrueColor() bool {
	return c.ColorMode == TerminalColorTrueColor
}

// DetectTerminalCapabilities derives conservative attachment capabilities from
// a client environment. Missing evidence selects indexed color; it never proves
// the physical terminal cannot display direct color.
func DetectTerminalCapabilities(env []string) TerminalCapabilities {
	values := environmentValues(env)
	term := strings.ToLower(strings.TrimSpace(values["TERM"]))
	colorTerm := strings.ToLower(strings.TrimSpace(values["COLORTERM"]))
	caps := TerminalCapabilities{ColorMode: TerminalColorIndexed256}

	if values["KITTY_WINDOW_ID"] != "" || values["KITTY_PID"] != "" || values["KITTY_LISTEN_ON"] != "" {
		caps.Application = TerminalApplicationKitty
	}

	switch colorTerm {
	case "truecolor", "24bit":
		caps.ColorMode = TerminalColorTrueColor
		caps.ColorSource = TerminalCapabilityDeclared
		return caps
	}
	if term == "xterm-direct" || strings.HasSuffix(term, "-direct") {
		caps.ColorMode = TerminalColorTrueColor
		caps.ColorSource = TerminalCapabilityDeclared
		return caps
	}
	if term == "xterm-kitty" && caps.Application == TerminalApplicationKitty {
		caps.ColorMode = TerminalColorTrueColor
		caps.ColorSource = TerminalCapabilityHeuristic
		return caps
	}
	if strings.Contains(term, "256color") || term == "dumb" {
		caps.ColorSource = TerminalCapabilityDeclared
	}
	return caps
}

func environmentValues(env []string) map[string]string {
	values := make(map[string]string, len(env))
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		values[key] = value
	}
	return values
}

// DetectTrueColor reports whether TERM/COLORTERM advertise direct color support.
func DetectTrueColor(termEnv, colorTerm string) bool {
	return DetectTerminalCapabilities([]string{"TERM=" + termEnv, "COLORTERM=" + colorTerm}).TrueColor()
}

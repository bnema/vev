package config

import (
	"strings"

	vt "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
)

// Spinner frame count bounds for a custom agents.spinner.
const (
	minSpinnerFrames = 2
	maxSpinnerFrames = 32
)

// spinnerPresets are the named agents.spinner values. "off" is handled by
// parseSpinner.
var spinnerPresets = map[string]string{
	"circle":    string(domain.DefaultAgentSpinner()),
	"jump":      "⢄⢂⢁⡁⡈⡐⡠",
	"minidot":   "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏",
	"dot":       "⣾⣽⣻⢿⡿⣟⣯⣷",
	"line":      `|/-\`,
	"hamburger": "☱☲☴☲",
	"star":      "✶✸✹✺✹✷",
}

// parseSpinner resolves a preset name (case-insensitive), "off" (no frames),
// or space-separated custom frames. Every frame must be one rune exactly one
// terminal cell wide.
func parseSpinner(value string) ([]rune, bool) {
	value, ok := parseConfigString(value)
	if !ok {
		return nil, false
	}
	name := strings.ToLower(strings.TrimSpace(value))
	if name == "off" {
		return []rune{}, true
	}
	if preset, ok := spinnerPresets[name]; ok {
		return []rune(preset), true
	}
	fields := strings.Fields(value)
	if len(fields) < minSpinnerFrames || len(fields) > maxSpinnerFrames {
		return nil, false
	}
	frames := make([]rune, 0, len(fields))
	for _, field := range fields {
		runes := []rune(field)
		if len(runes) != 1 || vt.RuneWidth(runes[0]) != 1 {
			return nil, false
		}
		frames = append(frames, runes[0])
	}
	return frames, true
}

// parseGlyphMode parses ui.glyphs case-insensitively. An invalid value
// returns auto and false.
func parseGlyphMode(value string) (domain.GlyphMode, bool) {
	value, ok := parseConfigString(value)
	if !ok {
		return domain.GlyphsAuto, false
	}
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "auto":
		return domain.GlyphsAuto, true
	case "nerd":
		return domain.GlyphsNerd, true
	case "unicode":
		return domain.GlyphsUnicode, true
	case "ascii":
		return domain.GlyphsASCII, true
	}
	return domain.GlyphsAuto, false
}

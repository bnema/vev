package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/domain/terminalcap"
)

func TestParseTerminalColors(t *testing.T) {
	for _, tt := range []struct {
		name     string
		input    string
		want     domain.TerminalConfig
		warnings int
	}{
		{"default is auto", "", domain.TerminalConfig{}, 0},
		{"auto", "terminal.colors = auto", domain.TerminalConfig{}, 0},
		{"truecolor", "terminal.colors = truecolor", domain.TerminalConfig{Colors: terminalcap.TrueColor, ColorsSet: true}, 0},
		{"256", "terminal.colors = 256", domain.TerminalConfig{Colors: terminalcap.ANSI256, ColorsSet: true}, 0},
		{"16", "terminal.colors = 16", domain.TerminalConfig{Colors: terminalcap.ANSI16, ColorsSet: true}, 0},
		{"mono", "terminal.colors = mono", domain.TerminalConfig{Colors: terminalcap.Monochrome, ColorsSet: true}, 0},
		{"case-insensitive", "terminal.colors = TrueColor", domain.TerminalConfig{Colors: terminalcap.TrueColor, ColorsSet: true}, 0},
		{"empty value is auto", "terminal.colors =", domain.TerminalConfig{}, 0},
		{"inline comment", "terminal.colors = 16 # linux console", domain.TerminalConfig{Colors: terminalcap.ANSI16, ColorsSet: true}, 0},
		{"invalid keeps default", "terminal.colors = rainbow", domain.TerminalConfig{}, 1},
		{"invalid keeps earlier valid", "terminal.colors = 16\nterminal.colors = rainbow", domain.TerminalConfig{Colors: terminalcap.ANSI16, ColorsSet: true}, 2},
		{"duplicate last wins with warning", "terminal.colors = 16\nterminal.colors = mono", domain.TerminalConfig{Colors: terminalcap.Monochrome, ColorsSet: true}, 1},
		{"later auto clears earlier override", "terminal.colors = 16\nterminal.colors = auto", domain.TerminalConfig{}, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg, warnings, err := Parse(strings.NewReader(tt.input))
			require.NoError(t, err)
			require.Equal(t, tt.want, cfg.Terminal)
			require.Len(t, warnings, tt.warnings)
			require.Empty(t, cfg.BindingEntries)
		})
	}
}

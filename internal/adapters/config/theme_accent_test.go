package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestParseThemeAccent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		want     domain.ThemeAccent
		warnings []domain.Warning
	}{
		{
			name:  "auto",
			input: "theme.accent = auto\n",
			want:  domain.ThemeAccent{Mode: domain.ThemeAccentAuto},
		},
		{
			name:  "slot zero is valid but warns",
			input: "theme.accent = 0\n",
			want:  domain.ThemeAccent{Mode: domain.ThemeAccentSlot, Slot: 0},
			warnings: []domain.Warning{
				{Line: 1, Msg: "theme.accent slot 0 is conventionally neutral"},
			},
		},
		{
			name:  "slot fifteen is valid but warns",
			input: "theme.accent = 15\n",
			want:  domain.ThemeAccent{Mode: domain.ThemeAccentSlot, Slot: 15},
			warnings: []domain.Warning{
				{Line: 1, Msg: "theme.accent slot 15 is conventionally neutral"},
			},
		},
		{
			name:  "chromatic slot",
			input: "theme.accent = 2\n",
			want:  domain.ThemeAccent{Mode: domain.ThemeAccentSlot, Slot: 2},
		},
		{
			name:  "negative slot falls back to auto",
			input: "theme.accent = -1\n",
			want:  domain.ThemeAccent{Mode: domain.ThemeAccentAuto},
			warnings: []domain.Warning{
				{Line: 1, Msg: "invalid theme.accent \"-1\""},
			},
		},
		{
			name:  "out of range slot falls back to auto",
			input: "theme.accent = 16\n",
			want:  domain.ThemeAccent{Mode: domain.ThemeAccentAuto},
			warnings: []domain.Warning{
				{Line: 1, Msg: "invalid theme.accent \"16\""},
			},
		},
		{
			name:  "non numeric value falls back to auto",
			input: "theme.accent = teal\n",
			want:  domain.ThemeAccent{Mode: domain.ThemeAccentAuto},
			warnings: []domain.Warning{
				{Line: 1, Msg: "invalid theme.accent \"teal\""},
			},
		},
		{
			name:  "non canonical slot falls back to auto",
			input: "theme.accent = 02\n",
			want:  domain.ThemeAccent{Mode: domain.ThemeAccentAuto},
			warnings: []domain.Warning{
				{Line: 1, Msg: "invalid theme.accent \"02\""},
			},
		},
		{
			name:  "case sensitive auto falls back to auto",
			input: "theme.accent = AUTO\n",
			want:  domain.ThemeAccent{Mode: domain.ThemeAccentAuto},
			warnings: []domain.Warning{
				{Line: 1, Msg: "invalid theme.accent \"AUTO\""},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, warnings, err := Parse(strings.NewReader(tt.input))
			require.NoError(t, err)
			require.Equal(t, tt.want, cfg.ThemeAccent)
			require.Equal(t, tt.warnings, warnings)
		})
	}
}

func TestDefaultsThemeAccent(t *testing.T) {
	t.Parallel()

	require.Equal(t, domain.ThemeAccent{Mode: domain.ThemeAccentAuto}, domain.Defaults().ThemeAccent)
}

// real I/O: reads and writes real files in temp directories.
func TestWatchThemeAccentInvalidReloadFallsBackToAuto(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config")
	require.NoError(t, os.WriteFile(path, []byte("theme.accent = 2\n"), 0o600))
	initial, warnings, err := Load(path)
	require.NoError(t, err)
	require.Empty(t, warnings)
	require.Equal(t, domain.ThemeAccent{Mode: domain.ThemeAccentSlot, Slot: 2}, initial.ThemeAccent)

	changes, stop := startWatch(t, path)
	require.NoError(t, os.WriteFile(path, []byte("theme.accent = invalid\n"), 0o600))

	got := nextChange(t, changes)
	require.Equal(t, domain.ThemeAccent{Mode: domain.ThemeAccentAuto}, got.cfg.ThemeAccent)
	require.Equal(t, []domain.Warning{{Line: 1, Msg: "invalid theme.accent \"invalid\""}}, got.warnings)
	stop()
}

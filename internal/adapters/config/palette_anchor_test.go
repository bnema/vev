package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestParsePaletteAnchor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		want     domain.PaletteConfig
		warnings []domain.Warning
	}{
		{name: "default is centered", want: domain.PaletteConfig{Anchor: domain.AnchorCenter, AnchorSet: true}},
		{name: "explicit auto is case insensitive", input: "palette.anchor = AuTo\n", want: domain.PaletteConfig{}},
		{name: "concrete anchors are trimmed and case insensitive", input: "palette.anchor = CENTER\npalette.anchor = top-left\npalette.anchor = Top\npalette.anchor = TOP-RIGHT\npalette.anchor = left\npalette.anchor = RIGHT\npalette.anchor = bottom-left\npalette.anchor = Bottom\npalette.anchor =  BOTTOM-RIGHT \n", want: domain.PaletteConfig{Anchor: domain.AnchorBottomRight, AnchorSet: true}, warnings: []domain.Warning{{Line: 2, Msg: "duplicate key \"palette.anchor\""}, {Line: 3, Msg: "duplicate key \"palette.anchor\""}, {Line: 4, Msg: "duplicate key \"palette.anchor\""}, {Line: 5, Msg: "duplicate key \"palette.anchor\""}, {Line: 6, Msg: "duplicate key \"palette.anchor\""}, {Line: 7, Msg: "duplicate key \"palette.anchor\""}, {Line: 8, Msg: "duplicate key \"palette.anchor\""}, {Line: 9, Msg: "duplicate key \"palette.anchor\""}}},
		{name: "invalid first preserves centered default", input: "palette.anchor = diagonal\n", want: domain.PaletteConfig{Anchor: domain.AnchorCenter, AnchorSet: true}, warnings: []domain.Warning{{Line: 1, Msg: "invalid palette.anchor \"diagonal\""}}},
		{name: "valid then invalid preserves valid", input: "palette.anchor = top\npalette.anchor = diagonal\n", want: domain.PaletteConfig{Anchor: domain.AnchorTop, AnchorSet: true}, warnings: []domain.Warning{{Line: 2, Msg: "duplicate key \"palette.anchor\""}, {Line: 2, Msg: "invalid palette.anchor \"diagonal\""}}},
		{name: "explicit then auto clears to auto", input: "palette.anchor = left\npalette.anchor = auto\n", want: domain.PaletteConfig{}, warnings: []domain.Warning{{Line: 2, Msg: "duplicate key \"palette.anchor\""}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, warnings, err := Parse(strings.NewReader(tt.input))
			require.NoError(t, err)
			require.Equal(t, tt.want, cfg.Palette)
			require.Equal(t, tt.warnings, warnings)
			require.Empty(t, cfg.BindingEntries)
		})
	}
}

// real I/O: reads and writes real files in temp directories.
func TestLoadMissingPaletteAnchorDefaultsToCenter(t *testing.T) {
	t.Parallel()
	cfg, warnings, err := Load(filepath.Join(t.TempDir(), "missing"))
	require.NoError(t, err)
	require.Empty(t, warnings)
	require.Equal(t, domain.PaletteConfig{Anchor: domain.AnchorCenter, AnchorSet: true}, cfg.Palette)
}

// real I/O: reads and writes real files in temp directories.
func TestWatchPaletteAnchorRestoresCenteredDefaultAfterFileRemoval(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config")
	require.NoError(t, os.WriteFile(path, []byte("palette.anchor = top-right\n"), 0o600))
	changes, stop := startWatch(t, path)

	require.NoError(t, os.WriteFile(path, []byte("palette.anchor = bottom-left\n"), 0o600))
	require.Equal(t, domain.AnchorBottomLeft, nextChange(t, changes).cfg.Palette.Anchor)

	require.NoError(t, os.Remove(path))
	got := nextChange(t, changes)
	require.Equal(t, domain.PaletteConfig{Anchor: domain.AnchorCenter, AnchorSet: true}, got.cfg.Palette)
	stop()
}

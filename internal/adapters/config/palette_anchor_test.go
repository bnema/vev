package config

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

func TestLoadMissingPaletteAnchorDefaultsToCenter(t *testing.T) {
	t.Parallel()
	cfg, warnings, err := Load(filepath.Join(t.TempDir(), "missing"))
	require.NoError(t, err)
	require.Empty(t, warnings)
	require.Equal(t, domain.PaletteConfig{Anchor: domain.AnchorCenter, AnchorSet: true}, cfg.Palette)
}

func TestWatchPaletteAnchorRestoresCenteredDefaultAfterFileRemoval(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config")
	require.NoError(t, os.WriteFile(path, []byte("palette.anchor = top-right\n"), 0o600))
	clk := newFakeClock(time.Unix(0, 0))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changes := make(chan domain.Config, 2)
	done := make(chan error, 1)
	go func() { done <- Watch(ctx, clk, path, func(cfg domain.Config, _ []domain.Warning) { changes <- cfg }) }()
	clk.waitForTimers(t, 1)

	require.NoError(t, os.WriteFile(path, []byte("palette.anchor = bottom-left\n"), 0o600))
	clk.advance(2 * time.Second)
	require.Equal(t, domain.AnchorBottomLeft, (<-changes).Palette.Anchor)

	require.NoError(t, os.Remove(path))
	clk.waitForTimers(t, 1)
	clk.advance(2 * time.Second)
	got := <-changes
	require.Equal(t, domain.PaletteConfig{Anchor: domain.AnchorCenter, AnchorSet: true}, got.Palette)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

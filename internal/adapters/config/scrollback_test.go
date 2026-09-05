package config

import (
	"strings"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestParseScrollbackPolicy(t *testing.T) {
	for _, tt := range []struct {
		name, input string
		want        domain.ScrollbackConfig
		warnings    int
	}{
		{"defaults", "", domain.DefaultScrollbackConfig(), 0},
		{"bytes only", "scrollback.megabytes = 20\nscrollback.lines = 0", domain.ScrollbackConfig{Megabytes: 20}, 0},
		{"disabled", "scrollback.megabytes = 0", domain.ScrollbackConfig{Lines: 10_000}, 0},
		{"both limits", "scrollback.megabytes = 10\nscrollback.lines = 100", domain.ScrollbackConfig{Megabytes: 10, Lines: 100}, 0},
		{"upper bounds", "scrollback.megabytes = 4096\nscrollback.lines = 1000000", domain.ScrollbackConfig{Megabytes: 4096, Lines: 1_000_000}, 0},
		{"negative bytes", "scrollback.megabytes = -1", domain.DefaultScrollbackConfig(), 1},
		{"negative lines", "scrollback.lines = -1", domain.DefaultScrollbackConfig(), 1},
		{"oversized bytes", "scrollback.megabytes = 4097", domain.DefaultScrollbackConfig(), 1},
		{"oversized lines", "scrollback.lines = 1000001", domain.DefaultScrollbackConfig(), 1},
		{"overflow", "scrollback.megabytes = 18446744073709551616", domain.DefaultScrollbackConfig(), 1},
		{"duplicate invalid keeps valid", "scrollback.megabytes = 20\nscrollback.megabytes = -1", domain.ScrollbackConfig{Megabytes: 20, Lines: 10_000}, 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg, warnings, err := Parse(strings.NewReader(tt.input))
			require.NoError(t, err)
			require.Equal(t, tt.want, cfg.Scrollback)
			require.Len(t, warnings, tt.warnings)
			require.Empty(t, cfg.BindingEntries)
		})
	}
}

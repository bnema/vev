package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseCopyReduceMotion(t *testing.T) {
	for _, tc := range []struct {
		name     string
		input    string
		want     bool
		warnings int
	}{
		{"default", "", false, 0},
		{"enabled", "copy.reduce-motion = on", true, 0},
		{"disabled", "copy.reduce-motion = off", false, 0},
		{"invalid", "copy.reduce-motion = maybe", false, 1},
		{"duplicate", "copy.reduce-motion = on\ncopy.reduce-motion = off", false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, warnings, err := Parse(strings.NewReader(tc.input))
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.Copy.ReduceMotion)
			require.Len(t, warnings, tc.warnings)
		})
	}
}

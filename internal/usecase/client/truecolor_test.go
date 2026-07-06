package client

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDetectTrueColor(t *testing.T) {
	tests := []struct {
		name      string
		termEnv   string
		colorTerm string
		want      bool
	}{
		{name: "COLORTERM truecolor", termEnv: "screen", colorTerm: "truecolor", want: true},
		{name: "COLORTERM 24bit", termEnv: "screen", colorTerm: "24bit", want: true},
		{name: "COLORTERM trims and ignores case", termEnv: "screen", colorTerm: " TRUECOLOR ", want: true},
		{name: "xterm-direct", termEnv: "xterm-direct", colorTerm: "", want: true},
		{name: "direct suffix", termEnv: "foot-direct", colorTerm: "", want: true},
		{name: "direct suffix ignores case and space", termEnv: " FOOT-DIRECT ", colorTerm: "", want: true},
		{name: "xterm 256 only", termEnv: "xterm-256color", colorTerm: "", want: false},
		{name: "empty", termEnv: "", colorTerm: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, detectTrueColor(tt.termEnv, tt.colorTerm))
		})
	}
}

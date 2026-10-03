package colorprofile

import (
	"testing"

	ansi "github.com/bnema/vev-vt/ansi"
	"github.com/bnema/vev/internal/domain/terminalcap"
	"github.com/stretchr/testify/require"
)

func TestProfile(t *testing.T) {
	tests := []struct {
		name string
		in   terminalcap.ColorCapabilities
		want ansi.ColorProfile
	}{
		{name: "zero value is truecolor", in: terminalcap.ColorCapabilities{}, want: ansi.ColorProfileTrueColor},
		{name: "truecolor", in: terminalcap.ColorCapabilities{Mode: terminalcap.TrueColor}, want: ansi.ColorProfileTrueColor},
		{name: "ansi256", in: terminalcap.ColorCapabilities{Mode: terminalcap.ANSI256}, want: ansi.ColorProfileANSI256},
		{name: "ansi16", in: terminalcap.ColorCapabilities{Mode: terminalcap.ANSI16}, want: ansi.ColorProfileANSI16},
		{name: "monochrome", in: terminalcap.ColorCapabilities{Mode: terminalcap.Monochrome}, want: ansi.ColorProfileMonochrome},
		{name: "forced source does not change mapping", in: terminalcap.ColorCapabilities{Mode: terminalcap.ANSI16, Source: terminalcap.SourceForced}, want: ansi.ColorProfileANSI16},
		{name: "unknown mode is conservative", in: terminalcap.ColorCapabilities{Mode: terminalcap.ColorMode(99)}, want: ansi.ColorProfileANSI256},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, Profile(tt.in))
		})
	}
}

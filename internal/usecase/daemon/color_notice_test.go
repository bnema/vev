package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain/terminalcap"
)

func TestColorDowngradeNotice(t *testing.T) {
	tests := []struct {
		name  string
		color terminalcap.ColorCapabilities
		want  string
	}{
		{name: "declared 256 colors", color: terminalcap.ColorCapabilities{Mode: terminalcap.ANSI256, Source: terminalcap.SourceDeclared}, want: "Terminal supports 256 colors; vev UI colors are reduced."},
		{name: "declared 16 colors", color: terminalcap.ColorCapabilities{Mode: terminalcap.ANSI16, Source: terminalcap.SourceDeclared}, want: "Terminal supports 16 colors; vev UI colors are reduced."},
		{name: "declared monochrome", color: terminalcap.ColorCapabilities{Mode: terminalcap.Monochrome, Source: terminalcap.SourceDeclared}, want: "Terminal has no color support; vev UI uses bold and reverse."},
		{name: "declared truecolor", color: terminalcap.ColorCapabilities{Mode: terminalcap.TrueColor, Source: terminalcap.SourceDeclared}},
		{name: "forced 256 colors", color: terminalcap.ColorCapabilities{Mode: terminalcap.ANSI256, Source: terminalcap.SourceForced}},
		{name: "forced 16 colors", color: terminalcap.ColorCapabilities{Mode: terminalcap.ANSI16, Source: terminalcap.SourceForced}},
		{name: "forced monochrome", color: terminalcap.ColorCapabilities{Mode: terminalcap.Monochrome, Source: terminalcap.SourceForced}},
		{name: "heuristic 256 colors", color: terminalcap.ColorCapabilities{Mode: terminalcap.ANSI256, Source: terminalcap.SourceHeuristic}},
		{name: "unknown 256 colors", color: terminalcap.ColorCapabilities{Mode: terminalcap.ANSI256}},
		{name: "zero value", color: terminalcap.ColorCapabilities{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := colorDowngradeNotice(tt.color)
			require.Equal(t, tt.want != "", ok)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestFinishAttachedClientColorToast drives the real attach completion path:
// only a declared reduced mode toasts, and a forced mode never does.
func TestFinishAttachedClientColorToast(t *testing.T) {
	tests := []struct {
		name      string
		color     terminalcap.ColorCapabilities
		wantToast string
	}{
		{name: "declared 256 colors toasts", color: terminalcap.ColorCapabilities{Mode: terminalcap.ANSI256, Source: terminalcap.SourceDeclared}, wantToast: "Terminal supports 256 colors; vev UI colors are reduced."},
		{name: "declared 16 colors toasts", color: terminalcap.ColorCapabilities{Mode: terminalcap.ANSI16, Source: terminalcap.SourceDeclared}, wantToast: "Terminal supports 16 colors; vev UI colors are reduced."},
		{name: "forced 16 colors is silent", color: terminalcap.ColorCapabilities{Mode: terminalcap.ANSI16, Source: terminalcap.SourceForced}},
		{name: "declared truecolor is silent", color: terminalcap.ColorCapabilities{Mode: terminalcap.TrueColor, Source: terminalcap.SourceDeclared}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pty, release := newBlockingPTY(t)
			defer release()
			d, sess, ac, _ := newManualSessionWithPTYs(t, pty)
			ac.terminalCapabilities = terminalcap.Capabilities{Color: tt.color}

			d.finishAttachedClient(sess, ac, attachClientOptions{})

			toasts, _ := visibleToasts(ac)
			if tt.wantToast == "" {
				require.Empty(t, toasts)
				return
			}
			require.Len(t, toasts, 1)
			require.Equal(t, tt.wantToast, toasts[0].Message)
		})
	}
}

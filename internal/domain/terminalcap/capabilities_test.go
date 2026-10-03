package terminalcap

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColorCapabilities(t *testing.T) {
	tests := []struct {
		name    string
		in      ColorCapabilities
		wantRGB bool
	}{
		{name: "zero value is truecolor", in: ColorCapabilities{}, wantRGB: true},
		{name: "truecolor", in: ColorCapabilities{Mode: TrueColor, Source: SourceDeclared}, wantRGB: true},
		{name: "ansi256", in: ColorCapabilities{Mode: ANSI256}, wantRGB: false},
		{name: "ansi16", in: ColorCapabilities{Mode: ANSI16, Source: SourceForced}, wantRGB: false},
		{name: "monochrome", in: ColorCapabilities{Mode: Monochrome, Source: SourceForced}, wantRGB: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.wantRGB, tt.in.RGB())
		})
	}
}

func TestColorCapabilitiesValid(t *testing.T) {
	tests := []struct {
		name string
		in   ColorCapabilities
		want bool
	}{
		{name: "zero value", in: ColorCapabilities{}, want: true},
		{name: "every mode and source", in: ColorCapabilities{Mode: Monochrome, Source: SourceForced}, want: true},
		{name: "unknown mode", in: ColorCapabilities{Mode: Monochrome + 1}},
		{name: "unknown source", in: ColorCapabilities{Source: SourceForced + 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.in.Valid())
		})
	}
}

func TestResolve(t *testing.T) {
	tests := []struct {
		name     string
		env      []string
		declared ColorCapabilities
		want     ColorCapabilities
		wantApp  Application
	}{
		{name: "unknown claim falls back to detection", env: []string{"TERM=xterm-256color"}, want: ColorCapabilities{Mode: ANSI256, Source: SourceDeclared}},
		{name: "unknown truecolor claim falls back to detection", env: []string{"TERM=xterm-256color"}, declared: ColorCapabilities{Mode: TrueColor}, want: ColorCapabilities{Mode: ANSI256, Source: SourceDeclared}},
		{name: "unknown claim with detected truecolor", env: []string{"TERM=xterm-256color", "COLORTERM=truecolor"}, want: ColorCapabilities{Mode: TrueColor, Source: SourceDeclared}},
		{name: "declared truecolor overrides indexed detection", env: []string{"TERM=xterm-256color"}, declared: ColorCapabilities{Mode: TrueColor, Source: SourceDeclared}, want: ColorCapabilities{Mode: TrueColor, Source: SourceDeclared}},
		{name: "declared truecolor on unknown terminal", env: []string{"TERM=unknown"}, declared: ColorCapabilities{Mode: TrueColor, Source: SourceDeclared}, want: ColorCapabilities{Mode: TrueColor, Source: SourceDeclared}},
		{name: "declared 256 overrides detected truecolor verbatim", env: []string{"TERM=xterm-256color", "COLORTERM=truecolor"}, declared: ColorCapabilities{Mode: ANSI256, Source: SourceDeclared}, want: ColorCapabilities{Mode: ANSI256, Source: SourceDeclared}},
		{name: "forced 16 colors", env: []string{"TERM=xterm-256color", "COLORTERM=truecolor"}, declared: ColorCapabilities{Mode: ANSI16, Source: SourceForced}, want: ColorCapabilities{Mode: ANSI16, Source: SourceForced}},
		{name: "forced monochrome", env: nil, declared: ColorCapabilities{Mode: Monochrome, Source: SourceForced}, want: ColorCapabilities{Mode: Monochrome, Source: SourceForced}},
		{name: "heuristic truecolor upgrades weaker detection", env: []string{"TERM=xterm-256color"}, declared: ColorCapabilities{Mode: TrueColor, Source: SourceHeuristic}, want: ColorCapabilities{Mode: TrueColor, Source: SourceHeuristic}},
		{name: "heuristic truecolor keeps stronger detection", env: []string{"TERM=xterm-kitty", "KITTY_WINDOW_ID=1"}, declared: ColorCapabilities{Mode: TrueColor, Source: SourceHeuristic}, want: ColorCapabilities{Mode: TrueColor, Source: SourceHeuristic}, wantApp: ApplicationKitty},
		{name: "heuristic 16 colors falls back to detection", env: []string{"TERM=xterm-256color"}, declared: ColorCapabilities{Mode: ANSI16, Source: SourceHeuristic}, want: ColorCapabilities{Mode: ANSI256, Source: SourceDeclared}},
		{name: "declared claim keeps detected application", env: []string{"TERM=tmux-256color", "KITTY_WINDOW_ID=1"}, declared: ColorCapabilities{Mode: TrueColor, Source: SourceDeclared}, want: ColorCapabilities{Mode: TrueColor, Source: SourceDeclared}, wantApp: ApplicationKitty},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Resolve(tt.env, tt.declared)
			require.Equal(t, tt.want, got.Color)
			require.Equal(t, tt.wantApp, got.Application)
			require.False(t, got.KittyGraphics)
		})
	}
}

func TestDetect(t *testing.T) {
	tests := []struct {
		name         string
		env          []string
		wantMode     ColorMode
		wantSource   Source
		wantApp      Application
		wantGraphics bool
	}{
		{name: "declared truecolor", env: []string{"TERM=xterm-256color", "COLORTERM=truecolor"}, wantMode: TrueColor, wantSource: SourceDeclared},
		{name: "declared 24bit", env: []string{"TERM=xterm-256color", "COLORTERM=24bit"}, wantMode: TrueColor, wantSource: SourceDeclared},
		{name: "direct terminfo entry", env: []string{"TERM=foot-direct"}, wantMode: TrueColor, wantSource: SourceDeclared},
		{name: "kitty signals infer truecolor", env: []string{"TERM=xterm-kitty", "KITTY_WINDOW_ID=1"}, wantMode: TrueColor, wantSource: SourceHeuristic, wantApp: ApplicationKitty},
		{name: "kitty environment with declared truecolor remains color-only", env: []string{"TERM=xterm-kitty", "COLORTERM=truecolor", "KITTY_WINDOW_ID=1"}, wantMode: TrueColor, wantSource: SourceDeclared, wantApp: ApplicationKitty},
		{name: "256 color terminal remains a constrained attachment", env: []string{"TERM=xterm-256color"}, wantMode: ANSI256, wantSource: SourceDeclared},
		{name: "unknown terminal is conservatively indexed", env: []string{"TERM=unknown"}, wantMode: ANSI256, wantSource: SourceUnknown},
		{name: "empty TERM is unknown", env: nil, wantMode: ANSI256, wantSource: SourceUnknown},
		{name: "TERM=screen is unknown", env: []string{"TERM=screen"}, wantMode: ANSI256, wantSource: SourceUnknown},
		{name: "TERM=xterm is unknown", env: []string{"TERM=xterm"}, wantMode: ANSI256, wantSource: SourceUnknown},
		{name: "screen-256color", env: []string{"TERM=screen-256color"}, wantMode: ANSI256, wantSource: SourceDeclared},
		{name: "tmux-256color", env: []string{"TERM=tmux-256color"}, wantMode: ANSI256, wantSource: SourceDeclared},
		{name: "TERM is case-insensitive and trimmed", env: []string{"TERM= LINUX "}, wantMode: ANSI16, wantSource: SourceDeclared},
		{name: "dumb is monochrome", env: []string{"TERM=dumb"}, wantMode: Monochrome, wantSource: SourceDeclared},
		{name: "vt52 is monochrome", env: []string{"TERM=vt52"}, wantMode: Monochrome, wantSource: SourceDeclared},
		{name: "vt100 is monochrome", env: []string{"TERM=vt100"}, wantMode: Monochrome, wantSource: SourceDeclared},
		{name: "vt102 is monochrome", env: []string{"TERM=vt102"}, wantMode: Monochrome, wantSource: SourceDeclared},
		{name: "vt220 is monochrome", env: []string{"TERM=vt220"}, wantMode: Monochrome, wantSource: SourceDeclared},
		{name: "-mono suffix is monochrome", env: []string{"TERM=xterm-mono"}, wantMode: Monochrome, wantSource: SourceDeclared},
		{name: "-m suffix is monochrome", env: []string{"TERM=rxvt-m"}, wantMode: Monochrome, wantSource: SourceDeclared},
		{name: "vt320 is not assumed monochrome", env: []string{"TERM=vt320"}, wantMode: ANSI256, wantSource: SourceUnknown},
		{name: "linux console is 16 colors", env: []string{"TERM=linux"}, wantMode: ANSI16, wantSource: SourceDeclared},
		{name: "ansi is 16 colors", env: []string{"TERM=ansi"}, wantMode: ANSI16, wantSource: SourceDeclared},
		{name: "cons25 is 16 colors", env: []string{"TERM=cons25"}, wantMode: ANSI16, wantSource: SourceDeclared},
		{name: "-16color suffix", env: []string{"TERM=xterm-16color"}, wantMode: ANSI16, wantSource: SourceDeclared},
		{name: "-color suffix", env: []string{"TERM=xterm-color"}, wantMode: ANSI16, wantSource: SourceDeclared},
		{name: "screen-color suffix", env: []string{"TERM=screen-color"}, wantMode: ANSI16, wantSource: SourceDeclared},
		{name: "256color is not the -color suffix", env: []string{"TERM=xterm-256color"}, wantMode: ANSI256, wantSource: SourceDeclared},
		{name: "COLORTERM truecolor beats linux", env: []string{"TERM=linux", "COLORTERM=truecolor"}, wantMode: TrueColor, wantSource: SourceDeclared},
		{name: "COLORTERM 24bit beats dumb", env: []string{"TERM=dumb", "COLORTERM=24bit"}, wantMode: TrueColor, wantSource: SourceDeclared},
		{name: "direct terminfo beats mono suffix", env: []string{"TERM=foot-direct"}, wantMode: TrueColor, wantSource: SourceDeclared},
		{name: "unrelated COLORTERM does not change TERM rules", env: []string{"TERM=linux", "COLORTERM=yes"}, wantMode: ANSI16, wantSource: SourceDeclared},
		{name: "kitty identity beats TERM rules", env: []string{"TERM=xterm-kitty", "KITTY_WINDOW_ID=1"}, wantMode: TrueColor, wantSource: SourceHeuristic, wantApp: ApplicationKitty},
		{name: "kitty environment behind a multiplexer is not graphics evidence", env: []string{"TERM=tmux-256color", "KITTY_WINDOW_ID=1"}, wantMode: ANSI256, wantSource: SourceDeclared, wantApp: ApplicationKitty},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Detect(tt.env)
			require.Equal(t, tt.wantMode, got.Color.Mode)
			require.Equal(t, tt.wantSource, got.Color.Source)
			require.Equal(t, tt.wantApp, got.Application)
			require.Equal(t, tt.wantGraphics, got.SupportsKittyGraphics())
		})
	}
}

func TestParseColorMode(t *testing.T) {
	tests := []struct {
		name     string
		in       string
		wantMode ColorMode
		wantAuto bool
		wantErr  bool
	}{
		{name: "empty is auto", in: "", wantAuto: true},
		{name: "blank is auto", in: "  \t", wantAuto: true},
		{name: "auto", in: "auto", wantAuto: true},
		{name: "auto is case-insensitive", in: " AUTO ", wantAuto: true},
		{name: "truecolor", in: "truecolor", wantMode: TrueColor},
		{name: "truecolor mixed case", in: "TrueColor", wantMode: TrueColor},
		{name: "256", in: "256", wantMode: ANSI256},
		{name: "16", in: " 16 ", wantMode: ANSI16},
		{name: "mono", in: "mono", wantMode: Monochrome},
		{name: "mono upper case", in: "MONO", wantMode: Monochrome},
		{name: "24bit is not accepted", in: "24bit", wantErr: true},
		{name: "256color is not accepted", in: "256color", wantErr: true},
		{name: "number out of set", in: "8", wantErr: true},
		{name: "monochrome spelled out is not accepted", in: "monochrome", wantErr: true},
		{name: "garbage", in: "rainbow", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mode, auto, err := ParseColorMode(tt.in)
			if tt.wantErr {
				require.Error(t, err)
				require.False(t, auto)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantAuto, auto)
			if !auto {
				require.Equal(t, tt.wantMode, mode)
			}
		})
	}
}

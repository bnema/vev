package ui

import (
	"testing"

	vt "bnema.dev/vev-vt"

	"github.com/bnema/vev/internal/domain"
)

func TestResolveGlyphs(t *testing.T) {
	tests := []struct {
		name    string
		mode    domain.GlyphMode
		console bool
		want    domain.GlyphMode
	}{
		{name: "auto", mode: domain.GlyphsAuto, want: domain.GlyphsNerd},
		{name: "auto console", mode: domain.GlyphsAuto, console: true, want: domain.GlyphsASCII},
		{name: "nerd on console", mode: domain.GlyphsNerd, console: true, want: domain.GlyphsNerd},
		{name: "unicode", mode: domain.GlyphsUnicode, console: true, want: domain.GlyphsUnicode},
		{name: "ascii", mode: domain.GlyphsASCII, want: domain.GlyphsASCII},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ResolveGlyphs(tt.mode, tt.console).Mode; got != tt.want {
				t.Fatalf("ResolveGlyphs = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestGlyphSetDoneGlyph(t *testing.T) {
	tests := []struct {
		name string
		set  GlyphSet
		want rune
	}{
		{name: "nerd md-bell_check", set: NerdGlyphs(), want: '\U000F11E5'},
		{name: "unicode", set: UnicodeGlyphs(), want: '✓'},
		{name: "ascii", set: ASCIIGlyphs(), want: '*'},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.set.Done != tt.want {
				t.Fatalf("Done = %q, want %q", tt.set.Done, tt.want)
			}
			if w := vt.RuneWidth(tt.want); w != 1 {
				t.Fatalf("%q is %d cells, want 1", tt.want, w)
			}
		})
	}
}

func TestGlyphSetsAreOneCell(t *testing.T) {
	for _, set := range []GlyphSet{NerdGlyphs(), UnicodeGlyphs(), ASCIIGlyphs()} {
		runes := append([]rune{set.Attention, set.Done}, set.Spinner...)
		for _, r := range runes {
			if vt.RuneWidth(r) != 1 {
				t.Errorf("set %d: %q is %d cells", set.Mode, r, vt.RuneWidth(r))
			}
		}
	}
	if got := (GlyphSet{}).OrNerd().Mode; got != domain.GlyphsNerd {
		t.Errorf("zero set resolves to %d, want nerd", got)
	}
	for _, r := range string(ASCIIGlyphs().Attention) + string(ASCIIGlyphs().Done) + string(ASCIIGlyphs().Spinner) {
		if r > 0x7e {
			t.Errorf("ascii set holds %q", r)
		}
	}
}

func TestGlyphSetDefaultSpinners(t *testing.T) {
	tests := []struct {
		name string
		set  GlyphSet
		want []rune
	}{
		{name: "nerd uses circle slices", set: NerdGlyphs(), want: []rune("\U000f0a9e\U000f0a9f\U000f0aa0\U000f0aa1\U000f0aa2\U000f0aa3\U000f0aa4\U000f0aa5")},
		{name: "unicode", set: UnicodeGlyphs(), want: []rune("◐◓◑◒")},
		{name: "ascii", set: ASCIIGlyphs(), want: []rune(`|/-\`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.set.Spinner) != string(tt.want) {
				t.Fatalf("spinner = %q, want %q", string(tt.set.Spinner), string(tt.want))
			}
		})
	}
}

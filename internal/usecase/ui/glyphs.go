package ui

import "github.com/bnema/vev/internal/domain"

// AttentionGlyph is the Nerd Font bell (nf-fa-bell) drawn beside tab/session
// labels with a pending attention event.
const AttentionGlyph = '\uf0f3'

// DoneGlyph is the Nerd Font bell with a check (nf-md-bell_check) drawn
// beside a tab whose agent finished a turn that nobody has seen yet.
const DoneGlyph = '\U000F11E5'

// GlyphSet is the resolved set of status glyphs for one terminal. Every
// glyph, including each spinner frame, is exactly one cell wide. Spinner is
// shared: callers must not modify it.
type GlyphSet struct {
	Mode      domain.GlyphMode
	Attention rune
	Done      rune
	Spinner   []rune
}

var (
	nerdSpinner    = domain.DefaultAgentSpinner()
	unicodeSpinner = []rune("◐◓◑◒")
	asciiSpinner   = []rune(`|/-\`)
)

// NerdGlyphs draws Nerd Font glyphs and the "circle" slice spinner.
func NerdGlyphs() GlyphSet {
	return GlyphSet{Mode: domain.GlyphsNerd, Attention: AttentionGlyph, Done: DoneGlyph, Spinner: nerdSpinner}
}

// UnicodeGlyphs needs neither a Nerd Font nor braille.
func UnicodeGlyphs() GlyphSet {
	return GlyphSet{Mode: domain.GlyphsUnicode, Attention: '•', Done: '✓', Spinner: unicodeSpinner}
}

// ASCIIGlyphs is safe on the Linux console.
func ASCIIGlyphs() GlyphSet {
	return GlyphSet{Mode: domain.GlyphsASCII, Attention: '!', Done: '*', Spinner: asciiSpinner}
}

// ResolveGlyphs selects the set for mode. Auto uses ASCII when console
// reports a limited outer terminal and Nerd Font glyphs otherwise.
func ResolveGlyphs(mode domain.GlyphMode, console bool) GlyphSet {
	switch mode {
	case domain.GlyphsNerd:
		return NerdGlyphs()
	case domain.GlyphsUnicode:
		return UnicodeGlyphs()
	case domain.GlyphsASCII:
		return ASCIIGlyphs()
	}
	if console {
		return ASCIIGlyphs()
	}
	return NerdGlyphs()
}

// OrNerd returns g, or the Nerd Font set for the zero GlyphSet.
func (g GlyphSet) OrNerd() GlyphSet {
	if g.Attention == 0 {
		return NerdGlyphs()
	}
	return g
}

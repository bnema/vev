package renderer

import "unicode"

// RuneWidth returns the terminal cell width of a rune.
//
// Returns 0 for combining marks, zero-width characters, and control codes.
// Returns 2 for CJK, fullwidth, and wide emoji characters.
// Returns 1 for all other printable characters.
//
// Wide runes (width 2) occupy two grid cells: a left cell holding the rune and
// a right continuation cell. Combining marks (Mn/Me) and zero-width joiner
// sequences are out of scope: combining marks report width 0 (current
// behavior), ZWJ sequences are not coalesced (each rune is measured on its own).
func RuneWidth(r rune) int {
	switch {
	case r < 0x20 || r == 0x7F || (r >= 0x80 && r <= 0x9F):
		return 0
	// Soft hyphen, combining grapheme joiner, Arabic sign, etc.
	case r == 0x00AD || r == 0x034F || r == 0x061C:
		return 0
	// Hangul Jungseong O/E, Hangul Jungseong Yu
	case r == 0x115F || r == 0x1160:
		return 0
	// Balinese musical symbols
	case r == 0x17B4 || r == 0x17B5:
		return 0
	// Mongolian variation selectors, free variation selector
	case r == 0x180B || r == 0x180E || r == 0x200B || r == 0x200C || r == 0x200D || r == 0x200E || r == 0x200F:
		return 0
	// Line/paragraph separator, bidirectional overrides
	case r >= 0x2028 && r <= 0x202E:
		return 0
	// Word joiner, invisible operators, Arabic/Syriac number marks, etc.
	case r >= 0x2060 && r <= 0x206F:
		return 0
	case r == 0xFEFF || r == 0xFFF9 || r == 0xFFFA || r == 0xFFFB:
		return 0
	// Tags
	case r >= 0xE0001 && r <= 0xE007F:
		return 0
	// Combining diacritical marks (Unicode categories Mn, Me)
	case unicode.Is(unicode.Mn, r) || unicode.Is(unicode.Me, r):
		return 0
	// --- Wide characters (width 2) ---
	// Hangul Jamo
	case r >= 0x1100 && r <= 0x115F:
		return 2
	// Left/right-pointing angle bracket (for compatibility)
	case r == 0x2329 || r == 0x232A:
		return 2
	// CJK Radicals Supplement, Kangxi Radicals, Ideographic Description
	// CJK Symbols, Hiragana, Katakana, Bopomofo, Hangul Compatibility Jamo
	// Kanbun, CJK Strokes, Enclosed CJK Letters
	case r >= 0x2E80 && r <= 0x303E:
		return 2
	// Hiragana, Katakana, Bopomofo, Hangul Compatibility Jamo, Kanbun
	case r >= 0x3040 && r <= 0x33FF:
		return 2
	// CJK Unified Ideographs Extension A
	case r >= 0x3400 && r <= 0x4DBF:
		return 2
	// CJK Unified Ideographs, Yi Syllables, Yi Radicals
	case r >= 0x4E00 && r <= 0xA4CF:
		return 2
	// Hangul Jamo Extended-A
	case r >= 0xA960 && r <= 0xA97C:
		return 2
	// Hangul Syllables
	case r >= 0xAC00 && r <= 0xD7AF:
		return 2
	// Hangul Jamo Extended-B
	case r >= 0xD7B0 && r <= 0xD7FF:
		return 2
	// CJK Compatibility Ideographs
	case r >= 0xF900 && r <= 0xFAFF:
		return 2
	// Vertical Forms
	case r >= 0xFE10 && r <= 0xFE19:
		return 2
	// CJK Compatibility Forms
	case r >= 0xFE30 && r <= 0xFE6F:
		return 2
	// Fullwidth Forms (variation selectors, fullwidth punctuation, etc.)
	case r >= 0xFF01 && r <= 0xFF60:
		return 2
	// Fullwidth Signs (cent, pound, yen, won, etc.)
	case r >= 0xFFE0 && r <= 0xFFE6:
		return 2
	// Kana Supplement
	case r >= 0x1B000 && r <= 0x1B0FF:
		return 2
	// Kana Extended-A
	case r >= 0x1B100 && r <= 0x1B12F:
		return 2
	// Mahjong Tiles, Domino Tiles, Playing Cards
	case r >= 0x1F000 && r <= 0x1F09F:
		return 2
	// Miscellaneous Symbols and Pictographs, Emoticons, Ornamental Dingbats
	case r >= 0x1F0A0 && r <= 0x1F64F:
		return 2
	// Transport and Map Symbols
	case r >= 0x1F680 && r <= 0x1F6FF:
		return 2
	// Supplemental Symbols and Pictographs
	case r >= 0x1F900 && r <= 0x1F9FF:
		return 2
	// Symbols and Pictographs Extended-A (emoji added in recent Unicode)
	case r >= 0x1FA70 && r <= 0x1FAFF:
		return 2
	// CJK Unified Ideographs Extension B and beyond
	case r >= 0x20000 && r <= 0x2FFFF:
		return 2
	// CJK Unified Ideographs Extension G and beyond
	case r >= 0x30000 && r <= 0x3FFFF:
		return 2
	default:
		return 1
	}
}

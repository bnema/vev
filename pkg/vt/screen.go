package vt

import (
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/bnema/vev/pkg/renderer"
)

type cursorState struct {
	row   int
	col   int
	style renderer.Style
	saved bool
}

type screenState struct {
	frame        renderer.Frame
	row          int
	col          int
	style        renderer.Style
	scrollTop    int
	scrollBottom int
	savedCursor  cursorState
}

const maxEscapeBufferLen = 4096

type Screen struct {
	Frame renderer.Frame
	Row   int
	Col   int
	Style renderer.Style
	// OnLineEvicted is called just before a full-width upward scroll recycles
	// and blanks rows. The callback receives a stable copy of each evicted row.
	OnLineEvicted func([]renderer.Cell)

	damage    []renderer.Damage
	escapeBuf []byte

	scrollTop    int
	scrollBottom int
	savedCursor  cursorState
	alternate    *screenState
}

func NewScreen(width, height int) *Screen {
	s := &Screen{
		Frame:  renderer.NewFrame(width, height),
		Style:  renderer.DefaultStyle(),
		damage: []renderer.Damage{renderer.FullRedraw()},
	}
	s.resetScrollRegion()
	return s
}

func (s *Screen) Resize(width, height int) {
	if width == s.Frame.Width && height == s.Frame.Height {
		return
	}
	s.Frame = renderer.NewFrame(width, height)
	s.Row, s.Col = 0, 0
	s.Style = renderer.DefaultStyle()
	s.escapeBuf = s.escapeBuf[:0]
	s.savedCursor = cursorState{}
	s.alternate = nil
	s.resetScrollRegion()
	s.fullRedraw()
}

// Damage returns the current damage list. The caller must not modify the
// returned slice; ClearDamage must be called after the damage is consumed.
func (s *Screen) Damage() []renderer.Damage { return s.damage }
func (s *Screen) ClearDamage()              { s.damage = s.damage[:0] }

func (s *Screen) Write(data []byte) {
	if len(s.escapeBuf) > 0 {
		combined := make([]byte, 0, len(s.escapeBuf)+len(data))
		combined = append(combined, s.escapeBuf...)
		combined = append(combined, data...)
		data = combined
		s.escapeBuf = s.escapeBuf[:0]
	}

	for len(data) > 0 {
		if data[0] == 0x1b {
			consumed, partial := s.consumeEscape(data)
			if consumed > 0 {
				data = data[consumed:]
				continue
			}
			if partial {
				if len(data) <= maxEscapeBufferLen {
					s.escapeBuf = append(s.escapeBuf[:0], data...)
				}
				return
			}
		}
		r, size := utf8.DecodeRune(data)
		if r == utf8.RuneError && size == 1 {
			data = data[1:]
			continue
		}
		s.putRune(r)
		data = data[size:]
	}
}

func (s *Screen) putRune(r rune) {
	switch r {
	case '\a', 0x00, 0x0e, 0x0f, 0x7f:
		return
	case '\r':
		s.Col = 0
	case '\n', '\v', '\f':
		if s.Frame.Width > 0 && s.Col >= s.Frame.Width {
			s.Col = s.Frame.Width - 1
		}
		s.index()
	case '\b':
		if s.Col > 0 {
			s.Col--
		}
	case '\t':
		spaces := 8 - (s.Col % 8)
		for range spaces {
			s.putPrintable(' ')
		}
	default:
		// Drop C1 control characters (U+0080-U+009F).
		if r >= 0x80 && r <= 0x9F {
			return
		}
		if r >= 0x20 {
			s.putPrintable(r)
		}
	}
}

// runeWidth returns the terminal cell width of a rune.
//
// Returns 0 for combining marks, zero-width characters, and control codes.
// Returns 2 for CJK, fullwidth, and wide emoji characters.
// Returns 1 for all other printable characters.
//
// Wide runes (width 2) occupy two grid cells: a left cell holding the rune and
// a right continuation cell. Combining marks (Mn/Me) and zero-width joiner
// sequences are out of scope: combining marks report width 0 (current
// behavior), ZWJ sequences are not coalesced (each rune is measured on its own).
func runeWidth(r rune) int {
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

func (s *Screen) putPrintable(r rune) {
	w := runeWidth(r)
	// Skip combining marks and zero-width characters.
	if w == 0 {
		return
	}
	if s.Frame.Width == 0 || s.Frame.Height == 0 {
		return
	}
	// Deferred wrap: cursor sits past the last column.
	if s.Col >= s.Frame.Width {
		s.Col = 0
		s.index()
	}
	if s.Row >= s.Frame.Height {
		s.Row = s.Frame.Height - 1
	}

	// A wide rune must never straddle the right edge. If it does not fit on the
	// current line, clear the abandoned last cell and wrap to the next line.
	if w == 2 && s.Col+1 >= s.Frame.Width {
		if s.Frame.Width < 2 {
			// The screen is too narrow to ever hold a wide rune; degrade to a
			// single narrow cell so the cursor stays aligned and we never write
			// a continuation cell out of bounds.
			w = 1
		} else {
			// The abandoned cell may itself be the continuation of a pair whose
			// left half sits one column back; report damage over the full span.
			cx := s.Col
			if s.Col > 0 && s.Frame.At(s.Col, s.Row).Continuation {
				cx = s.Col - 1
			}
			s.clearWidePairAt(s.Col, s.Row)
			s.Frame.Set(s.Col, s.Row, renderer.BlankCell())
			s.record(renderer.Damage{Kind: renderer.DamageText, X: cx, Y: s.Row, Width: s.Col - cx + 1, Height: 1, Count: 1})
			s.Col = 0
			s.index()
			if s.Row >= s.Frame.Height {
				s.Row = s.Frame.Height - 1
			}
		}
	}

	// Determine the range of cells actually modified, extending over any wide
	// pair the write lands on so no orphaned half is left behind.
	lo, hi := s.Col, s.Col+w-1
	if s.Frame.At(s.Col, s.Row).Continuation {
		lo = s.Col - 1
	}
	if right := s.Col + w; right < s.Frame.Width && s.Frame.At(right, s.Row).Continuation {
		hi = right
	}
	for x := lo; x <= hi; x++ {
		s.Frame.Set(x, s.Row, renderer.BlankCell())
	}
	s.Frame.Set(s.Col, s.Row, renderer.Cell{Rune: r, Style: s.Style})
	if w == 2 {
		s.Frame.Set(s.Col+1, s.Row, renderer.Cell{Continuation: true, Style: s.Style})
	}
	s.record(renderer.Damage{Kind: renderer.DamageText, X: lo, Y: s.Row, Width: hi - lo + 1, Height: 1, Count: 1})
	s.Col += w
}

// clearWidePairAt blanks both halves of a wide-character pair when the cell at
// (x,y) is either the left (wide) half or the right (continuation) half. It is
// O(1) and assumes the pair invariant (a continuation is preceded by its wide
// left half) holds — which the writer maintains.
func (s *Screen) clearWidePairAt(x, y int) {
	if x < 0 || x >= s.Frame.Width || y < 0 || y >= s.Frame.Height {
		return
	}
	if s.Frame.At(x, y).Continuation {
		if x-1 >= 0 {
			s.Frame.Set(x-1, y, renderer.BlankCell())
		}
		s.Frame.Set(x, y, renderer.BlankCell())
		return
	}
	if x+1 < s.Frame.Width && s.Frame.At(x+1, y).Continuation {
		s.Frame.Set(x, y, renderer.BlankCell())
		s.Frame.Set(x+1, y, renderer.BlankCell())
	}
}

// clearRow blanks cells [x0,x1) on row y, extending the range to swallow either
// half of a wide pair that straddles a boundary so no orphan half is left. It
// returns the actual modified span [start, start+width).
func (s *Screen) clearRow(y, x0, x1 int) (start, width int) {
	if y < 0 || y >= s.Frame.Height {
		return x0, 0
	}
	if x0 < 0 {
		x0 = 0
	}
	if x1 > s.Frame.Width {
		x1 = s.Frame.Width
	}
	if x0 >= x1 {
		return x0, 0
	}
	// Left boundary: a continuation at x0 means its wide left half sits at x0-1.
	if x0 > 0 && s.Frame.At(x0, y).Continuation {
		x0--
	}
	// Right boundary: a continuation at x1 means its wide left half (at x1-1) is
	// inside the range and will be blanked, so swallow the continuation too.
	if x1 < s.Frame.Width && s.Frame.At(x1, y).Continuation {
		x1++
	}
	for x := x0; x < x1; x++ {
		s.Frame.Set(x, y, renderer.BlankCell())
	}
	return x0, x1 - x0
}

// repairRow blanks any orphaned wide-character halves on row y: a continuation
// cell not preceded by its wide left half, or a wide rune not followed by its
// continuation. Used after erase/insert/delete operations that may split a pair
// at a boundary or by shifting cells.
func (s *Screen) repairRow(y int) {
	if y < 0 || y >= s.Frame.Height {
		return
	}
	w := s.Frame.Width
	for x := range w {
		c := s.Frame.At(x, y)
		if c.Continuation {
			if x == 0 {
				s.Frame.Set(x, y, renderer.BlankCell())
				continue
			}
			left := s.Frame.At(x-1, y)
			if left.Continuation || runeWidth(left.Rune) != 2 {
				s.Frame.Set(x, y, renderer.BlankCell())
			}
			continue
		}
		if runeWidth(c.Rune) == 2 {
			if x+1 >= w || !s.Frame.At(x+1, y).Continuation {
				s.Frame.Set(x, y, renderer.BlankCell())
			}
		}
	}
}

func (s *Screen) index() {
	if s.Frame.Height == 0 {
		return
	}
	if s.Row == s.scrollBottom && s.Row >= s.scrollTop {
		s.scrollUpRegion(s.scrollTop, s.scrollBottom, 1)
		return
	}
	if s.Row+1 < s.Frame.Height {
		s.Row++
		return
	}
	s.Row = s.Frame.Height - 1
}

func (s *Screen) nextLine() {
	s.Col = 0
	s.index()
}

func (s *Screen) reverseIndex() {
	if s.Frame.Height == 0 {
		return
	}
	if s.Row == s.scrollTop && s.Row <= s.scrollBottom {
		s.scrollDownRegion(s.scrollTop, s.scrollBottom, 1)
		return
	}
	if s.Row > 0 {
		s.Row--
	}
}

// scrollUpBy scrolls the current scroll region up by n lines (CSI S / SU).
// The cursor position is preserved.
func (s *Screen) scrollUpBy(n int) {
	if n <= 0 {
		return
	}
	s.scrollUpRegion(s.scrollTop, s.scrollBottom, n)
}

func (s *Screen) scrollDownBy(n int) {
	if n <= 0 {
		return
	}
	s.scrollDownRegion(s.scrollTop, s.scrollBottom, n)
}

func (s *Screen) scrollUpRegion(top, bottom, n int) {
	if s.Frame.Width == 0 || s.Frame.Height == 0 || n <= 0 {
		return
	}
	top, bottom, ok := s.normalizedRegion(top, bottom)
	if !ok {
		return
	}
	w := s.Frame.Width
	height := bottom - top + 1
	if n > height {
		n = height
	}
	// VT scroll regions always span the full frame width, so we rotate the
	// frame's line offsets (recycling and blanking the evicted rows in place)
	// instead of copying cells. See renderer.Frame.ScrollUp.
	s.emitLineEvicted(top, n)
	s.Frame.ScrollUp(top, bottom, n)
	s.record(renderer.Damage{Kind: renderer.DamageScrollUp, X: 0, Y: top, Width: w, Height: height, Count: n})
	s.record(renderer.Damage{Kind: renderer.DamageText, X: 0, Y: bottom - n + 1, Width: w, Height: n, Count: 1})
}

func (s *Screen) emitLineEvicted(top, n int) {
	if s.OnLineEvicted == nil || s.alternate != nil {
		return
	}
	for y := top; y < top+n; y++ {
		s.OnLineEvicted(append([]renderer.Cell(nil), s.Frame.Row(y)...))
	}
}

func (s *Screen) scrollDownRegion(top, bottom, n int) {
	if s.Frame.Width == 0 || s.Frame.Height == 0 || n <= 0 {
		return
	}
	top, bottom, ok := s.normalizedRegion(top, bottom)
	if !ok {
		return
	}
	w := s.Frame.Width
	height := bottom - top + 1
	if n > height {
		n = height
	}
	// Full-width region: rotate line offsets instead of copying cells.
	s.Frame.ScrollDown(top, bottom, n)
	s.record(renderer.Damage{Kind: renderer.DamageText, X: 0, Y: top, Width: w, Height: height, Count: 1})
}

func (s *Screen) normalizedRegion(top, bottom int) (int, int, bool) {
	if s.Frame.Height == 0 || top > bottom {
		return 0, 0, false
	}
	top = clamp(top, 0, s.Frame.Height-1)
	bottom = clamp(bottom, 0, s.Frame.Height-1)
	return top, bottom, top <= bottom
}

func (s *Screen) record(d renderer.Damage) {
	// Replace FullRedraw with the first concrete damage item.
	if len(s.damage) == 1 && s.damage[0].Kind == renderer.DamageFullRedraw {
		s.damage[0] = d
		return
	}
	// Coalesce adjacent single-cell text damage on the same line.
	if d.Kind == renderer.DamageText && d.Width == 1 && d.Height == 1 && len(s.damage) > 0 {
		last := &s.damage[len(s.damage)-1]
		if last.Kind == renderer.DamageText && last.Y == d.Y && last.X+last.Width == d.X && last.Height == 1 {
			last.Width++
			return
		}
	}
	s.damage = append(s.damage, d)
}

func (s *Screen) fullRedraw() {
	s.damage = []renderer.Damage{renderer.FullRedraw()}
}

func (s *Screen) consumeEscape(data []byte) (consumed int, partial bool) {
	if len(data) < 2 {
		return 0, true
	}
	switch data[1] {
	case ']':
		return consumeOSC(data)
	case 'P':
		return consumeSTString(data)
	case '_', '^', 'X':
		return consumeSTString(data)
	case '[':
		return s.consumeCSI(data)
	case '=':
		return 2, false
	case '>':
		return 2, false
	case '7':
		s.saveCursor()
		return 2, false
	case '8':
		s.restoreCursor()
		return 2, false
	case 'D':
		s.index()
		return 2, false
	case 'E':
		s.nextLine()
		return 2, false
	case 'M':
		s.reverseIndex()
		return 2, false
	case 'c':
		s.reset()
		return 2, false
	case 'H':
		return 2, false
	case '(', ')', '*', '+', '-', '.', '/':
		if len(data) < 3 {
			return 0, true
		}
		return 3, false
	default:
		if data[1] >= 0x30 && data[1] <= 0x7e {
			return 2, false
		}
		return 0, false
	}
}

func (s *Screen) consumeCSI(data []byte) (consumed int, partial bool) {
	end := -1
	for i := 2; i < len(data); i++ {
		if data[i] >= '@' && data[i] <= '~' {
			end = i
			break
		}
	}
	if end == -1 {
		return 0, true
	}
	params := string(data[2:end])
	cmd := data[end]
	s.applyCSI(params, cmd)
	return end + 1, false
}

func consumeOSC(data []byte) (consumed int, partial bool) {
	for i := 2; i < len(data); i++ {
		switch data[i] {
		case 0x07:
			return i + 1, false
		case 0x1b:
			if i+1 < len(data) && data[i+1] == '\\' {
				return i + 2, false
			}
		}
	}
	return 0, true
}

func consumeSTString(data []byte) (consumed int, partial bool) {
	for i := 2; i < len(data); i++ {
		if data[i] == 0x1b && i+1 < len(data) && data[i+1] == '\\' {
			return i + 2, false
		}
	}
	return 0, true
}

func (s *Screen) applyCSI(params string, cmd byte) {
	private := strings.HasPrefix(params, "?")
	parts := parseCSIInts(params)
	switch cmd {
	case 'm':
		if strings.HasPrefix(params, ">") {
			return
		}
		s.applySGR(params)
	case 'J':
		mode := 0
		if len(parts) > 0 {
			mode = parts[0]
		}
		s.clearScreenMode(mode)
	case 'K':
		mode := 0
		if len(parts) > 0 {
			mode = parts[0]
		}
		s.clearLineMode(mode)
	case 'S':
		s.scrollUpBy(firstPositive(parts, 1))
	case 'T':
		s.scrollDownBy(firstPositive(parts, 1))
	case 'H', 'f':
		row, col := 1, 1
		if len(parts) > 0 && parts[0] > 0 {
			row = parts[0]
		}
		if len(parts) > 1 && parts[1] > 0 {
			col = parts[1]
		}
		s.Row = clamp(row-1, 0, s.Frame.Height-1)
		s.Col = clamp(col-1, 0, s.Frame.Width-1)
	case 'A':
		s.Row = clamp(s.Row-firstPositive(parts, 1), 0, s.Frame.Height-1)
	case 'B':
		s.Row = clamp(s.Row+firstPositive(parts, 1), 0, s.Frame.Height-1)
	case 'C':
		s.Col = clamp(s.Col+firstPositive(parts, 1), 0, s.Frame.Width-1)
	case 'D':
		s.Col = clamp(s.Col-firstPositive(parts, 1), 0, s.Frame.Width-1)
	case 'E':
		s.Row = clamp(s.Row+firstPositive(parts, 1), 0, s.Frame.Height-1)
		s.Col = 0
	case 'F':
		s.Row = clamp(s.Row-firstPositive(parts, 1), 0, s.Frame.Height-1)
		s.Col = 0
	case 'G':
		col := firstPositive(parts, 1)
		s.Col = clamp(col-1, 0, s.Frame.Width-1)
	case 'd':
		row := firstPositive(parts, 1)
		s.Row = clamp(row-1, 0, s.Frame.Height-1)
	case '@':
		s.insertChars(firstPositive(parts, 1))
	case 'P':
		s.deleteChars(firstPositive(parts, 1))
	case 'L':
		s.insertLines(firstPositive(parts, 1))
	case 'M':
		s.deleteLines(firstPositive(parts, 1))
	case 'r':
		s.setScrollRegion(parts)
	case 's':
		s.saveCursor()
	case 'u':
		s.restoreCursor()
	case 'h':
		s.setMode(private, parts, true)
	case 'l':
		s.setMode(private, parts, false)
	}
}

func firstPositive(parts []int, fallback int) int {
	if len(parts) == 0 || parts[0] <= 0 {
		return fallback
	}
	return parts[0]
}

func (s *Screen) applySGR(params string) {
	if params == "" {
		s.Style = renderer.DefaultStyle()
		return
	}
	parts := parseCSIInts(params)
	for i := 0; i < len(parts); i++ {
		switch parts[i] {
		case 0:
			s.Style = renderer.DefaultStyle()
		case 1:
			s.Style.Bold = true
		case 7:
			s.Style.Inverse = true
		case 22:
			s.Style.Bold = false
		case 27:
			s.Style.Inverse = false
		case 30, 31, 32, 33, 34, 35, 36, 37:
			s.Style.Foreground = parts[i] - 30
			s.Style.HasForegroundRGB = false
		case 39:
			s.Style.Foreground = -1
			s.Style.HasForegroundRGB = false
		case 40, 41, 42, 43, 44, 45, 46, 47:
			s.Style.Background = parts[i] - 40
			s.Style.HasBackgroundRGB = false
		case 49:
			s.Style.Background = -1
			s.Style.HasBackgroundRGB = false
		case 90, 91, 92, 93, 94, 95, 96, 97:
			s.Style.Foreground = parts[i] - 90 + 8
			s.Style.HasForegroundRGB = false
		case 100, 101, 102, 103, 104, 105, 106, 107:
			s.Style.Background = parts[i] - 100 + 8
			s.Style.HasBackgroundRGB = false
		case 38:
			if i+2 < len(parts) && parts[i+1] == 5 {
				s.Style.Foreground = parts[i+2]
				s.Style.HasForegroundRGB = false
				i += 2
			} else if i+4 < len(parts) && parts[i+1] == 2 {
				s.Style.Foreground = -1
				s.Style.HasForegroundRGB = true
				s.Style.ForegroundRGB = sgrRGB(parts[i+2], parts[i+3], parts[i+4])
				i += 4
			}
		case 48:
			if i+2 < len(parts) && parts[i+1] == 5 {
				s.Style.Background = parts[i+2]
				s.Style.HasBackgroundRGB = false
				i += 2
			} else if i+4 < len(parts) && parts[i+1] == 2 {
				s.Style.Background = -1
				s.Style.HasBackgroundRGB = true
				s.Style.BackgroundRGB = sgrRGB(parts[i+2], parts[i+3], parts[i+4])
				i += 4
			}
		}
	}
}

func sgrRGB(r, g, b int) renderer.RGB {
	return renderer.RGB{
		R: uint8(clamp(r, 0, 255)),
		G: uint8(clamp(g, 0, 255)),
		B: uint8(clamp(b, 0, 255)),
	}
}

func (s *Screen) saveCursor() {
	s.savedCursor = cursorState{row: s.Row, col: s.Col, style: s.Style, saved: true}
}

func (s *Screen) restoreCursor() {
	if !s.savedCursor.saved {
		return
	}
	s.Row = clamp(s.savedCursor.row, 0, s.Frame.Height-1)
	s.Col = clamp(s.savedCursor.col, 0, s.Frame.Width-1)
	s.Style = s.savedCursor.style
}

func (s *Screen) reset() {
	s.Frame = renderer.NewFrame(s.Frame.Width, s.Frame.Height)
	s.Row, s.Col = 0, 0
	s.Style = renderer.DefaultStyle()
	s.escapeBuf = s.escapeBuf[:0]
	s.savedCursor = cursorState{}
	s.alternate = nil
	s.resetScrollRegion()
	s.fullRedraw()
}

func (s *Screen) setScrollRegion(parts []int) {
	if s.Frame.Height == 0 {
		return
	}
	top, bottom := 1, s.Frame.Height
	if len(parts) > 0 && parts[0] > 0 {
		top = parts[0]
	}
	if len(parts) > 1 && parts[1] > 0 {
		bottom = parts[1]
	}
	if top >= bottom {
		s.resetScrollRegion()
	} else {
		s.scrollTop = clamp(top-1, 0, s.Frame.Height-1)
		s.scrollBottom = clamp(bottom-1, 0, s.Frame.Height-1)
		if s.scrollTop >= s.scrollBottom {
			s.resetScrollRegion()
		}
	}
	s.Row, s.Col = 0, 0
}

func (s *Screen) resetScrollRegion() {
	s.scrollTop = 0
	if s.Frame.Height > 0 {
		s.scrollBottom = s.Frame.Height - 1
	} else {
		s.scrollBottom = 0
	}
}

func (s *Screen) setMode(private bool, parts []int, enabled bool) {
	if !private {
		return
	}
	for _, mode := range parts {
		switch mode {
		case 47, 1047, 1049:
			if enabled {
				s.enterAlternateScreen()
			} else {
				s.exitAlternateScreen()
			}
		case 1, 25, 1000, 1002, 1003, 1004, 1005, 1006, 2004, 2026:
			// Trackable terminal modes that do not directly affect the current
			// cell model yet. Consuming them prevents mode bytes from leaking.
			continue
		}
	}
}

func (s *Screen) enterAlternateScreen() {
	if s.alternate == nil {
		s.alternate = &screenState{
			frame:        cloneFrame(s.Frame),
			row:          s.Row,
			col:          s.Col,
			style:        s.Style,
			scrollTop:    s.scrollTop,
			scrollBottom: s.scrollBottom,
			savedCursor:  s.savedCursor,
		}
	}
	s.Frame = renderer.NewFrame(s.Frame.Width, s.Frame.Height)
	s.Row, s.Col = 0, 0
	s.Style = renderer.DefaultStyle()
	s.savedCursor = cursorState{}
	s.resetScrollRegion()
	s.fullRedraw()
}

func (s *Screen) exitAlternateScreen() {
	if s.alternate == nil {
		return
	}
	state := s.alternate
	s.Frame = cloneFrame(state.frame)
	s.Row = clamp(state.row, 0, s.Frame.Height-1)
	s.Col = clamp(state.col, 0, s.Frame.Width-1)
	s.Style = state.style
	s.scrollTop = state.scrollTop
	s.scrollBottom = state.scrollBottom
	s.savedCursor = state.savedCursor
	s.alternate = nil
	s.fullRedraw()
}

// cloneFrame produces an independent copy in canonical layout: it copies the
// source's logical rows (via Row) into a fresh frame whose offsets are already
// canonical, so a rotated source is normalized in the clone.
func cloneFrame(frame renderer.Frame) renderer.Frame {
	out := renderer.NewFrame(frame.Width, frame.Height)
	for y := range frame.Height {
		copy(out.Row(y), frame.Row(y))
	}
	return out
}

func (s *Screen) insertChars(n int) {
	if s.Row < 0 || s.Row >= s.Frame.Height || s.Col >= s.Frame.Width || n <= 0 {
		return
	}
	w := s.Frame.Width
	if n > w-s.Col {
		n = w - s.Col
	}
	row := s.Frame.Row(s.Row)
	// A wide left half at Col-1 whose continuation sits at Col will be orphaned
	// by the shift; its repair falls outside the default damage rect.
	leftSplit := s.Col > 0 && row[s.Col].Continuation
	for x := w - 1; x >= s.Col+n; x-- {
		row[x] = row[x-n]
	}
	for x := s.Col; x < s.Col+n; x++ {
		row[x] = renderer.BlankCell()
	}
	s.repairRow(s.Row)
	dmgX := s.Col
	if leftSplit {
		dmgX = s.Col - 1
	}
	s.record(renderer.Damage{Kind: renderer.DamageText, X: dmgX, Y: s.Row, Width: w - dmgX, Height: 1, Count: 1})
}

func (s *Screen) deleteChars(n int) {
	if s.Row < 0 || s.Row >= s.Frame.Height || s.Col >= s.Frame.Width || n <= 0 {
		return
	}
	w := s.Frame.Width
	if n > w-s.Col {
		n = w - s.Col
	}
	row := s.Frame.Row(s.Row)
	// A wide left half at Col-1 whose continuation sits at Col will be orphaned
	// by the shift; its repair falls outside the default damage rect.
	leftSplit := s.Col > 0 && row[s.Col].Continuation
	for x := s.Col; x < w-n; x++ {
		row[x] = row[x+n]
	}
	for x := w - n; x < w; x++ {
		row[x] = renderer.BlankCell()
	}
	s.repairRow(s.Row)
	dmgX := s.Col
	if leftSplit {
		dmgX = s.Col - 1
	}
	s.record(renderer.Damage{Kind: renderer.DamageText, X: dmgX, Y: s.Row, Width: w - dmgX, Height: 1, Count: 1})
}

func (s *Screen) insertLines(n int) {
	if s.Row < s.scrollTop || s.Row > s.scrollBottom || n <= 0 {
		return
	}
	s.scrollDownRegion(s.Row, s.scrollBottom, n)
}

func (s *Screen) deleteLines(n int) {
	if s.Row < s.scrollTop || s.Row > s.scrollBottom || n <= 0 {
		return
	}
	s.scrollUpRegion(s.Row, s.scrollBottom, n)
}

func (s *Screen) clearScreenMode(mode int) {
	s.clampCursor()
	switch mode {
	case 1:
		for y := 0; y <= s.Row && y < s.Frame.Height; y++ {
			end := s.Frame.Width
			if y == s.Row {
				end = min(s.Col+1, s.Frame.Width)
			}
			s.clearRow(y, 0, end)
		}
		s.record(renderer.Damage{Kind: renderer.DamageClear, X: 0, Y: 0, Width: s.Frame.Width, Height: min(s.Row+1, s.Frame.Height), Count: 1})
	case 2, 3:
		for i := range s.Frame.Cells {
			s.Frame.Cells[i] = renderer.BlankCell()
		}
		s.record(renderer.Damage{Kind: renderer.DamageClear, X: 0, Y: 0, Width: s.Frame.Width, Height: s.Frame.Height, Count: 1})
	default:
		for y := s.Row; y < s.Frame.Height; y++ {
			start := 0
			if y == s.Row {
				start = s.Col
			}
			s.clearRow(y, start, s.Frame.Width)
		}
		s.record(renderer.Damage{Kind: renderer.DamageClear, X: 0, Y: s.Row, Width: s.Frame.Width, Height: s.Frame.Height - s.Row, Count: 1})
	}
}

func (s *Screen) clearLineMode(mode int) {
	s.clampCursor()
	switch mode {
	case 1:
		start, width := s.clearRow(s.Row, 0, min(s.Col+1, s.Frame.Width))
		s.record(renderer.Damage{Kind: renderer.DamageClear, X: start, Y: s.Row, Width: width, Height: 1, Count: 1})
	case 2:
		start, width := s.clearRow(s.Row, 0, s.Frame.Width)
		s.record(renderer.Damage{Kind: renderer.DamageClear, X: start, Y: s.Row, Width: width, Height: 1, Count: 1})
	default:
		start, width := s.clearRow(s.Row, s.Col, s.Frame.Width)
		s.record(renderer.Damage{Kind: renderer.DamageClear, X: start, Y: s.Row, Width: width, Height: 1, Count: 1})
	}
}

func (s *Screen) clampCursor() {
	s.Row = clamp(s.Row, 0, s.Frame.Height-1)
	s.Col = clamp(s.Col, 0, s.Frame.Width-1)
}

func parseCSIInts(params string) []int {
	if params == "" {
		return nil
	}
	params = strings.TrimPrefix(params, "?")
	params = strings.TrimPrefix(params, ">")
	parts := strings.Split(params, ";")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		if p == "" {
			out = append(out, 0)
			continue
		}
		v, err := strconv.Atoi(p)
		if err != nil {
			out = append(out, 0)
			continue
		}
		out = append(out, v)
	}
	return out
}

func clamp(v, lo, hi int) int {
	if hi < lo {
		return lo
	}
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

package vt

import (
	"strconv"
	"strings"
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
	originMode   bool
	insertMode   bool
}

// maxEscapeBufferLen must stay large enough for OSC 52 clipboard payloads
// forwarded by internal/usecase/copy.OSC52MaxPayloadBytes after base64
// expansion plus the OSC wrapper. pkg/vt cannot import internal/usecase/copy,
// so keep this value in sync if that payload cap changes.
const maxEscapeBufferLen = 128 * 1024

const (
	// ColorSchemeReportDark is the DEC 2031 dark-scheme report.
	ColorSchemeReportDark = "\x1b[?997;1n"
	// ColorSchemeReportLight is the DEC 2031 light-scheme report.
	ColorSchemeReportLight = "\x1b[?997;2n"
)

const (
	progressStateClear = iota
	progressStateNormal
	progressStateError
	progressStateIndeterminate
	progressStatePaused
)

type Screen struct {
	Frame renderer.Frame
	Row   int
	Col   int
	Style renderer.Style
	// OnLineEvicted is called just before a full-width upward scroll recycles
	// and blanks rows. The callback receives a stable copy of each evicted row.
	OnLineEvicted func([]renderer.Cell)
	// OnResponse is called synchronously from Write with reply bytes that the
	// emulator must send back to the child process (DA, DSR, DECRQM reports).
	// The host wires it to the PTY input. Nil disables responses.
	OnResponse func([]byte)
	// OnBell is called synchronously from Write for each lone BEL (0x07)
	// outside escape sequences. BELs that terminate an OSC never fire it.
	// Nil disables bell reporting.
	OnBell func()
	// OnNotify is called synchronously from Write for explicit terminal
	// notifications: OSC 9 (body only) and OSC 777 "notify" (title;body).
	// Other non-clipboard OSC payloads remain discarded. Nil disables it.
	OnNotify func(title, body string)
	// OnProgress is called synchronously from Write for OSC 9;4 progress
	// transitions that request attention: active progress cleared or first entry
	// into error state. Nil disables progress reporting.
	OnProgress func(errored bool)
	// OnClipboard is called synchronously from Write for a complete OSC 52
	// clipboard set request from the child. The OSC 52 selection field is
	// accepted but ignored; the callback receives only the raw base64 payload.
	// Clipboard queries (data == "?") and malformed payloads are ignored and
	// never invoke it. Nil disables it.
	OnClipboard func(b64 string)

	defaultFG          renderer.RGB
	defaultBG          renderer.RGB
	defaultColorsKnown bool

	damage     []renderer.Damage
	escapeBuf  []byte
	csiScratch []int
	sgrScratch []int

	scrollTop        int
	scrollBottom     int
	savedCursor      cursorState
	alternate        *screenState
	syncUpdateActive bool
	progressState    int
	cursorVisible    bool
	cursorStyle      int
	cursorStyleSet   bool
	mouseMode        int
	mouseSGR         bool
	bracketedPaste   bool
	originMode       bool
	insertMode       bool
	colorSchemeMode  bool
	colorSchemeLight bool
	colorSchemeSet   bool
}

func NewScreen(width, height int) *Screen {
	s := &Screen{
		Frame:         renderer.NewFrame(width, height),
		Style:         renderer.DefaultStyle(),
		damage:        []renderer.Damage{renderer.FullRedraw()},
		cursorVisible: true,
	}
	s.resetScrollRegion()
	return s
}

func (s *Screen) Resize(width, height int) {
	if width == s.Frame.Width && height == s.Frame.Height {
		return
	}
	evict := func(row []renderer.Cell) {
		if s.OnLineEvicted != nil {
			s.OnLineEvicted(append([]renderer.Cell(nil), row...))
		}
	}

	if s.alternate != nil {
		var shift int
		s.Frame, shift = resizeFrame(s.Frame, width, height, s.Row, nil)
		s.Row = clamp(s.Row-shift, 0, height-1)
		s.Col = clamp(s.Col, 0, width-1)
		s.savedCursor = resizedCursor(s.savedCursor, shift, width, height)
		s.alternate.frame, shift = resizeFrame(s.alternate.frame, width, height, s.alternate.row, evict)
		s.alternate.row = clamp(s.alternate.row-shift, 0, height-1)
		s.alternate.col = clamp(s.alternate.col, 0, width-1)
		s.alternate.savedCursor = resizedCursor(s.alternate.savedCursor, shift, width, height)
		s.alternate.scrollTop = clamp(s.alternate.scrollTop-shift, 0, height-1)
		s.alternate.scrollBottom = clamp(s.alternate.scrollBottom-shift, 0, height-1)
		if s.alternate.scrollTop >= s.alternate.scrollBottom {
			s.alternate.scrollTop = 0
			s.alternate.scrollBottom = max(height-1, 0)
		}
	} else {
		var shift int
		s.Frame, shift = resizeFrame(s.Frame, width, height, s.Row, evict)
		s.Row = clamp(s.Row-shift, 0, height-1)
		s.Col = clamp(s.Col, 0, width-1)
		s.savedCursor = resizedCursor(s.savedCursor, shift, width, height)
	}

	// A resize can split an in-flight escape sequence from the terminal state it
	// was meant to mutate; keep the durable child state but discard partial bytes.
	s.escapeBuf = s.escapeBuf[:0]
	s.resetScrollRegion()
	s.fullRedraw()
}

// Damage returns the current damage list. The caller must not modify the
// returned slice; ClearDamage must be called after the damage is consumed.
func (s *Screen) Damage() []renderer.Damage { return s.damage }
func (s *Screen) ClearDamage()              { s.damage = s.damage[:0] }

// SyncUpdateActive reports whether DEC private mode 2026 (synchronized update)
// is currently enabled by the child process.
func (s *Screen) SyncUpdateActive() bool { return s.syncUpdateActive }

// BracketedPasteMode reports whether DEC private mode 2004 is currently enabled
// by the child process.
func (s *Screen) BracketedPasteMode() bool { return s.bracketedPaste }

// ColorSchemeMode reports whether DEC private mode 2031 is currently enabled.
func (s *Screen) ColorSchemeMode() bool { return s.colorSchemeMode }

// SetColorScheme updates the host color scheme and notifies subscribed child apps.
func (s *Screen) SetColorScheme(light bool) {
	if s.colorSchemeSet && s.colorSchemeLight == light {
		return
	}
	s.colorSchemeSet = true
	s.colorSchemeLight = light
	if s.colorSchemeMode {
		s.respond(s.colorSchemeReport())
	}
}

// ClearColorScheme marks the host color scheme as unknown. Future child color
// scheme queries are silent until SetColorScheme supplies a known value again.
func (s *Screen) ClearColorScheme() {
	s.colorSchemeSet = false
}

// ForceSyncEnd forcibly leaves DEC private mode 2026 (synchronized update).
// Hosts use this as a safety valve if a child enters synchronized update mode
// and never sends the matching end sequence.
func (s *Screen) ForceSyncEnd() { s.syncUpdateActive = false }

// SetDefaultColors sets the terminal default foreground/background colors used
// to answer child OSC 10/11 color queries. Passing ok=false makes color queries
// silent until known colors are supplied again.
func (s *Screen) SetDefaultColors(fg, bg renderer.RGB, ok bool) {
	s.defaultFG = fg
	s.defaultBG = bg
	s.defaultColorsKnown = ok
}

func (s *Screen) CursorRow() int { return s.Row }
func (s *Screen) CursorCol() int { return s.Col }
func (s *Screen) CursorVisible() bool {
	return s.cursorVisible
}
func (s *Screen) CursorStyle() (int, bool) { return s.cursorStyle, s.cursorStyleSet }
func (s *Screen) MouseMode() (int, bool)   { return s.mouseMode, s.mouseSGR }
func (s *Screen) AltScreenActive() bool    { return s.alternate != nil }

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
	case '\a':
		if s.OnBell != nil {
			s.OnBell()
		}
		return
	case 0x00, 0x0e, 0x0f, 0x7f:
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

func (s *Screen) putPrintable(r rune) {
	w := renderer.RuneWidth(r)
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

	insertDamageX := s.Col
	insertDamageWidth := 0
	if s.insertMode {
		row := s.Frame.Row(s.Row)
		leftSplit := s.Col > 0 && row[s.Col].Continuation
		for x := s.Frame.Width - 1; x >= s.Col+w; x-- {
			row[x] = row[x-w]
		}
		for x := s.Col; x < s.Col+w; x++ {
			row[x] = renderer.BlankCell()
		}
		s.repairRow(s.Row)
		if leftSplit {
			insertDamageX = s.Col - 1
		}
		insertDamageWidth = s.Frame.Width - insertDamageX
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
	if insertDamageWidth > 0 {
		lo = min(lo, insertDamageX)
		hi = max(hi, insertDamageX+insertDamageWidth-1)
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
	repairFrameRow(s.Frame, y)
}

func repairFrameRow(frame renderer.Frame, y int) {
	if y < 0 || y >= frame.Height {
		return
	}
	w := frame.Width
	for x := range w {
		c := frame.At(x, y)
		if c.Continuation {
			if x == 0 {
				frame.Set(x, y, renderer.BlankCell())
				continue
			}
			left := frame.At(x-1, y)
			if left.Continuation || renderer.RuneWidth(left.Rune) != 2 {
				frame.Set(x, y, renderer.BlankCell())
			}
			continue
		}
		if renderer.RuneWidth(c.Rune) == 2 {
			if x+1 >= w || !frame.At(x+1, y).Continuation {
				frame.Set(x, y, renderer.BlankCell())
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

func (s *Screen) respond(b []byte) {
	if s.OnResponse != nil {
		s.OnResponse(b)
	}
}

func (s *Screen) colorSchemeReport() []byte {
	if !s.colorSchemeSet {
		return nil
	}
	if s.colorSchemeLight {
		return []byte(ColorSchemeReportLight)
	}
	return []byte(ColorSchemeReportDark)
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
		return s.consumeOSC(data)
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

func (s *Screen) consumeOSC(data []byte) (consumed int, partial bool) {
	for i := 2; i < len(data); i++ {
		switch data[i] {
		case 0x07:
			s.applyOSC(data[2:i], []byte{0x07})
			s.handleOSC(data[2:i])
			return i + 1, false
		case 0x1b:
			if i+1 < len(data) && data[i+1] == '\\' {
				s.applyOSC(data[2:i], []byte{0x1b, '\\'})
				s.handleOSC(data[2:i])
				return i + 2, false
			}
		}
	}
	return 0, true
}

func (s *Screen) applyOSC(payload, terminator []byte) {
	if !s.defaultColorsKnown {
		return
	}

	var color renderer.RGB
	switch string(payload) {
	case "10;?":
		color = s.defaultFG
	case "11;?":
		color = s.defaultBG
	default:
		return
	}

	resp := make([]byte, 0, len(payload)+len("\x1b];rgb:0000/0000/0000")+len(terminator))
	resp = append(resp, "\x1b]"...)
	resp = append(resp, payload[:2]...)
	resp = append(resp, ";rgb:"...)
	resp = appendOSCColorComponent(resp, color.R)
	resp = append(resp, '/')
	resp = appendOSCColorComponent(resp, color.G)
	resp = append(resp, '/')
	resp = appendOSCColorComponent(resp, color.B)
	resp = append(resp, terminator...)
	s.respond(resp)
}

func appendOSCColorComponent(dst []byte, c uint8) []byte {
	const hex = "0123456789abcdef"
	v := uint16(c)<<8 | uint16(c)
	return append(dst, hex[v>>12&0xf], hex[v>>8&0xf], hex[v>>4&0xf], hex[v&0xf])
}

// handleOSC inspects a complete OSC payload (between "ESC ]" and its
// terminator). Notification sequences (OSC 9, OSC 777 "notify") and clipboard
// set requests (OSC 52) are acted on; titles and every other OSC are still
// discarded.
func (s *Screen) handleOSC(payload []byte) {
	if len(payload) >= len("52;") && payload[0] == '5' && payload[1] == '2' && payload[2] == ';' {
		s.handleOSC52(string(payload[len("52;"):]))
		return
	}
	p := string(payload)
	if p == "9;4" || strings.HasPrefix(p, "9;4;") {
		s.handleProgress(p[len("9;4"):])
		return
	}
	if s.OnNotify == nil {
		return
	}
	switch {
	case strings.HasPrefix(p, "9;"):
		s.OnNotify("", p[len("9;"):])
	case strings.HasPrefix(p, "777;"):
		parts := strings.SplitN(p[len("777;"):], ";", 3)
		if parts[0] != "notify" {
			return
		}
		var title, body string
		if len(parts) > 1 {
			title = parts[1]
		}
		if len(parts) > 2 {
			body = parts[2]
		}
		s.OnNotify(title, body)
	}
}

func (s *Screen) handleProgress(rest string) {
	rest = strings.TrimPrefix(rest, ";")
	if rest == "" {
		return
	}
	token, _, _ := strings.Cut(rest, ";")
	state, err := strconv.Atoi(token)
	if err != nil {
		return
	}
	switch state {
	case progressStateNormal, progressStateIndeterminate, progressStatePaused:
		s.progressState = state
	case progressStateClear:
		previous := s.progressState
		s.progressState = state
		if previous == progressStateNormal || previous == progressStateIndeterminate || previous == progressStatePaused {
			if s.OnProgress != nil {
				s.OnProgress(false)
			}
		}
	case progressStateError:
		previous := s.progressState
		s.progressState = state
		if previous != progressStateError {
			if s.OnProgress != nil {
				s.OnProgress(true)
			}
		}
	}
}

// handleOSC52 parses the "<selection>;<data>" remainder of an OSC 52
// clipboard payload (selection may be empty; split on the first ";"). A
// clipboard query (data == "?") is always ignored — vev never answers
// clipboard queries. A payload with no second ";" is malformed and ignored.
func (s *Screen) handleOSC52(rest string) {
	_, data, ok := strings.Cut(rest, ";")
	if !ok {
		return
	}
	if data == "?" {
		return
	}
	if s.OnClipboard != nil {
		s.OnClipboard(data)
	}
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
	parts := s.parseCSIInts(params)
	switch cmd {
	case 'c':
		switch params {
		case "", "0":
			s.respond([]byte("\x1b[?6c"))
		case ">", ">0":
			s.respond([]byte("\x1b[>0;0;0c"))
		}
	case 'n':
		switch firstPositive(parts, 0) {
		case 5:
			if !private {
				s.respond([]byte("\x1b[0n"))
			}
		case 996:
			if private {
				if report := s.colorSchemeReport(); report != nil {
					s.respond(report)
				}
			}
		case 6:
			resp := make([]byte, 0, 16)
			resp = append(resp, "\x1b["...)
			if private {
				resp = append(resp, '?')
			}
			resp = strconv.AppendInt(resp, int64(s.Row+1), 10)
			resp = append(resp, ';')
			resp = strconv.AppendInt(resp, int64(s.Col+1), 10)
			resp = append(resp, 'R')
			s.respond(resp)
		}
	case 'p':
		if private && strings.HasSuffix(params, "$") {
			mode, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(params, "?"), "$"))
			if err != nil {
				return
			}
			state := 0
			switch mode {
			case 2026:
				state = 2
				if s.syncUpdateActive {
					state = 1
				}
			case 2031:
				state = 2
				if s.colorSchemeMode {
					state = 1
				}
			}
			resp := make([]byte, 0, 16)
			resp = append(resp, "\x1b[?"...)
			resp = strconv.AppendInt(resp, int64(mode), 10)
			resp = append(resp, ';')
			resp = strconv.AppendInt(resp, int64(state), 10)
			resp = append(resp, "$y"...)
			s.respond(resp)
		}
	case 'm':
		if strings.HasPrefix(params, ">") {
			return
		}
		s.applySGR(params)
	case 'q':
		s.applyCursorStyle(params)
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
		s.addressCursor(row, col)
	case 'A':
		s.Row = clamp(s.Row-firstPositive(parts, 1), s.cursorMinRow(), s.cursorMaxRow())
	case 'B':
		s.Row = clamp(s.Row+firstPositive(parts, 1), s.cursorMinRow(), s.cursorMaxRow())
	case 'C':
		s.Col = clamp(s.Col+firstPositive(parts, 1), 0, s.Frame.Width-1)
	case 'D':
		s.Col = clamp(s.Col-firstPositive(parts, 1), 0, s.Frame.Width-1)
	case 'E':
		s.Row = clamp(s.Row+firstPositive(parts, 1), s.cursorMinRow(), s.cursorMaxRow())
		s.Col = 0
	case 'F':
		s.Row = clamp(s.Row-firstPositive(parts, 1), s.cursorMinRow(), s.cursorMaxRow())
		s.Col = 0
	case 'G':
		col := firstPositive(parts, 1)
		s.Col = clamp(col-1, 0, s.Frame.Width-1)
	case 'd':
		row := firstPositive(parts, 1)
		s.Row = s.addressedRow(row)
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
		if params == "" {
			s.restoreCursor()
		}
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

func (s *Screen) cursorMinRow() int {
	if s.originMode {
		return clamp(s.scrollTop, 0, s.Frame.Height-1)
	}
	return 0
}

func (s *Screen) cursorMaxRow() int {
	if s.originMode {
		return clamp(s.scrollBottom, 0, s.Frame.Height-1)
	}
	return max(s.Frame.Height-1, 0)
}

func (s *Screen) addressedRow(row int) int {
	if s.originMode {
		return clamp(s.scrollTop+row-1, s.cursorMinRow(), s.cursorMaxRow())
	}
	return clamp(row-1, 0, s.Frame.Height-1)
}

func (s *Screen) addressCursor(row, col int) {
	s.Row = s.addressedRow(row)
	s.Col = clamp(col-1, 0, s.Frame.Width-1)
}

func (s *Screen) homeCursor() {
	s.addressCursor(1, 1)
}

func (s *Screen) applyCursorStyle(params string) {
	if strings.HasPrefix(params, ">") || !strings.HasSuffix(params, " ") {
		return
	}
	styleParam := strings.TrimSuffix(params, " ")
	style := 0
	if styleParam != "" {
		v, err := strconv.Atoi(styleParam)
		if err != nil {
			return
		}
		style = v
	}
	if style < 0 || style > 6 {
		return
	}
	s.cursorStyle = style
	s.cursorStyleSet = true
}

func (s *Screen) applySGR(params string) {
	if params == "" {
		s.Style = renderer.DefaultStyle()
		return
	}
	parts := s.parseSGRInts(params)
	for i := 0; i < len(parts); i++ {
		switch parts[i] {
		case 0:
			s.Style = renderer.DefaultStyle()
		case 1:
			s.Style.Bold = true
		case 3:
			s.Style.Italic = true
		case 7:
			s.Style.Inverse = true
		case 22:
			s.Style.Bold = false
		case 23:
			s.Style.Italic = false
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
	s.originMode = false
	s.insertMode = false
	s.cursorVisible = true
	s.cursorStyle = 0
	s.cursorStyleSet = false
	s.mouseMode = 0
	s.mouseSGR = false
	s.bracketedPaste = false
	s.colorSchemeMode = false
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
	s.homeCursor()
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
		for _, mode := range parts {
			switch mode {
			case 4:
				s.insertMode = enabled
			default:
				// Other ANSI modes (for example LNM) are intentionally ignored
				// until the screen model implements their observable behavior.
				continue
			}
		}
		return
	}
	for _, mode := range parts {
		switch mode {
		case 6:
			s.originMode = enabled
			s.homeCursor()
		case 47, 1047, 1049:
			if enabled {
				s.enterAlternateScreen()
			} else {
				s.exitAlternateScreen()
			}
		case 2026:
			s.syncUpdateActive = enabled
		case 2031:
			s.colorSchemeMode = enabled
		case 25:
			s.cursorVisible = enabled
		case 1000, 1002, 1003:
			if enabled {
				s.mouseMode = mode
			} else if s.mouseMode == mode {
				s.mouseMode = 0
			}
		case 1006:
			s.mouseSGR = enabled
		case 2004:
			s.bracketedPaste = enabled
		case 1, 1004, 1005:
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
			originMode:   s.originMode,
			insertMode:   s.insertMode,
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
	s.originMode = state.originMode
	s.insertMode = state.insertMode
	s.alternate = nil
	s.fullRedraw()
}

func resizeFrame(old renderer.Frame, newW, newH, cursorRow int, evict func([]renderer.Cell)) (renderer.Frame, int) {
	next := renderer.NewFrame(newW, newH)
	shift := clamp(cursorRow-(newH-1), 0, max(old.Height-newH, 0))
	if evict != nil {
		for y := range shift {
			evict(old.Row(y))
		}
	}
	for dy := range newH {
		sy := dy + shift
		if sy >= old.Height {
			break
		}
		copy(next.Row(dy), old.Row(sy))
		repairFrameRow(next, dy)
	}
	return next, shift
}

func resizedCursor(cur cursorState, shift, width, height int) cursorState {
	if !cur.saved {
		return cur
	}
	cur.row = clamp(cur.row-shift, 0, height-1)
	cur.col = clamp(cur.col, 0, width-1)
	return cur
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
		for y := range min(s.Row+1, s.Frame.Height) {
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

func (s *Screen) parseCSIInts(params string) []int {
	s.csiScratch = parseCSIIntsInto(s.csiScratch[:0], params)
	return s.csiScratch
}

func parseCSIInts(params string) []int {
	return parseCSIIntsInto(nil, params)
}

func parseCSIIntsInto(out []int, params string) []int {
	if params == "" {
		return out
	}
	params = strings.TrimPrefix(params, "?")
	params = strings.TrimPrefix(params, ">")
	start := 0
	for i := 0; i <= len(params); i++ {
		if i == len(params) || params[i] == ';' {
			out = append(out, parseCSIInt(params[start:i]))
			start = i + 1
		}
	}
	return out
}

func (s *Screen) parseSGRInts(params string) []int {
	s.sgrScratch = parseSGRIntsInto(s.sgrScratch[:0], params)
	return s.sgrScratch
}

func parseSGRInts(params string) []int {
	return parseSGRIntsInto(nil, params)
}

func parseSGRIntsInto(out []int, params string) []int {
	if params == "" {
		return out
	}
	start := 0
	for i := 0; i <= len(params); i++ {
		if i == len(params) || params[i] == ';' {
			out = appendSGRGroup(out, params[start:i])
			start = i + 1
		}
	}
	return out
}

func appendSGRGroup(out []int, group string) []int {
	colon := false
	for i := 0; i < len(group); i++ {
		if group[i] == ':' {
			colon = true
			break
		}
	}
	if !colon {
		return append(out, parseCSIInt(group))
	}

	parts := countSGRColonFields(group)
	code := parseCSIInt(sgrColonField(group, 0))
	if code != 38 && code != 48 {
		return append(out, code)
	}
	mode := 0
	if parts > 1 {
		mode = parseCSIInt(sgrColonField(group, 1))
	}
	switch mode {
	case 5:
		if parts < 3 {
			return out
		}
		out = append(out, code, mode, parseCSIInt(sgrColonField(group, 2)))
	case 2:
		// code:mode::R:G:B and code:mode:cs:R:G:B both put RGB after
		// the colorspace slot; code:mode:R:G:B omits that slot.
		start := 2
		if parts > 2 && (sgrColonField(group, 2) == "" || parts >= 6) {
			start = 3
		}
		if parts < start+3 {
			return out
		}
		out = append(out, code, mode)
		for i := 0; i < 3; i++ {
			out = append(out, parseCSIInt(sgrColonField(group, start+i)))
		}
	}
	return out
}

func countSGRColonFields(group string) int {
	count := 1
	for i := 0; i < len(group); i++ {
		if group[i] == ':' {
			count++
		}
	}
	return count
}

func sgrColonField(group string, index int) string {
	start := 0
	field := 0
	for i := 0; i <= len(group); i++ {
		if i == len(group) || group[i] == ':' {
			if field == index {
				return group[start:i]
			}
			field++
			start = i + 1
		}
	}
	return ""
}

func parseCSIInt(param string) int {
	if param == "" {
		return 0
	}
	v, err := strconv.Atoi(param)
	if err != nil {
		return 0
	}
	return v
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

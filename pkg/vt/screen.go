package vt

import (
	"unicode/utf8"

	"github.com/bnema/vev/pkg/renderer"
)

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

// maxPendingDamage bounds metadata retained while no render transaction can
// acknowledge a screen. Saturation falls back to one exact full redraw.
const maxPendingDamage = 1024

const (
	// ColorSchemeReportDark is the DEC 2031 dark-scheme report.
	ColorSchemeReportDark = "\x1b[?997;1n"
	// ColorSchemeReportLight is the DEC 2031 light-scheme report.
	ColorSchemeReportLight = "\x1b[?997;2n"
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
	// emulator must send back to the child process (DA, DSR, and ANSI/DEC mode
	// query reports). The host wires it to the PTY input. Nil disables responses.
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

	history *History

	defaultFG          renderer.RGB
	defaultBG          renderer.RGB
	defaultColorsKnown bool
	terminalTitle      string

	damage                 []renderer.Damage
	damageGeneration       uint64
	damageSaturated        bool
	damageFullRedrawSticky bool
	escapeBuf              []byte
	csiScratch             []int
	sgrScratch             []int

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
		Frame:            renderer.NewFrame(width, height),
		Style:            renderer.DefaultStyle(),
		damage:           []renderer.Damage{renderer.FullRedraw()},
		damageGeneration: 1,
		cursorVisible:    true,
	}
	s.resetScrollRegion()
	return s
}

// NewScreenWithHistory creates a screen that records rows evicted from its
// primary screen into bounded immutable terminal history.
func NewScreenWithHistory(width, height int, config HistoryConfig) *Screen {
	s := NewScreen(width, height)
	s.history = NewHistory(config)
	return s
}

// History returns this screen's terminal history, or nil when history was not
// configured with NewScreenWithHistory.
func (s *Screen) History() *History { return s.history }

func (s *Screen) Resize(width, height int) {
	if width == s.Frame.Width && height == s.Frame.Height {
		return
	}
	evict := s.recordEvicted

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

// DamageCapture is an immutable copy of pending damage at one screen generation.
type DamageCapture struct {
	Damage     []renderer.Damage
	Generation uint64
}

// Damage returns the current damage list. The caller must not modify the
// returned slice; ClearDamage must be called after the damage is consumed.
func (s *Screen) Damage() []renderer.Damage { return s.damage }
func (s *Screen) ClearDamage() {
	s.damage = s.damage[:0]
	s.damageSaturated = false
	s.damageFullRedrawSticky = false
}

// CaptureDamage snapshots pending damage without consuming it.
func (s *Screen) CaptureDamage() DamageCapture {
	return DamageCapture{Damage: append([]renderer.Damage(nil), s.damage...), Generation: s.damageGeneration}
}

// AcknowledgeDamage consumes a capture only if no screen mutation occurred
// since it was taken. A stale acknowledgement conservatively requests a full
// redraw, ensuring intervening writes remain visible to the next capture.
func (s *Screen) AcknowledgeDamage(generation uint64) bool {
	if generation != s.damageGeneration {
		s.fullRedraw()
		return false
	}
	s.ClearDamage()
	return true
}

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

// TerminalTitle returns the latest title set by OSC 0 or OSC 2.
func (s *Screen) TerminalTitle() string { return s.terminalTitle }

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
	if s.alternate != nil {
		return
	}
	for y := top; y < top+n; y++ {
		s.recordEvicted(s.Frame.Row(y))
	}
}

func (s *Screen) recordEvicted(row []renderer.Cell) {
	if s.history != nil {
		s.history.Append(row)
	}
	if s.OnLineEvicted != nil {
		s.OnLineEvicted(append([]renderer.Cell(nil), row...))
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
	s.damageGeneration++
	if s.damageSaturated || s.damageFullRedrawSticky {
		return
	}
	if len(s.damage) >= maxPendingDamage {
		s.damage = []renderer.Damage{renderer.FullRedraw()}
		s.damageSaturated = true
		return
	}
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
	s.damageGeneration++
	s.damage = []renderer.Damage{renderer.FullRedraw()}
	// A structural frame replacement (such as DEC 1049 exit) cannot be
	// represented by the following text damage alone. Retain the redraw until
	// the render owner acknowledges this screen generation.
	s.damageFullRedrawSticky = true
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

func consumeSTString(data []byte) (consumed int, partial bool) {
	for i := 2; i < len(data); i++ {
		if data[i] == 0x1b && i+1 < len(data) && data[i+1] == '\\' {
			return i + 2, false
		}
	}
	return 0, true
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

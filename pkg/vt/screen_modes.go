package vt

import "github.com/bnema/vev/pkg/renderer"

type screenState struct {
	frame        renderer.Frame
	buffer       *buffer
	row          int
	col          int
	style        renderer.Style
	scrollTop    int
	scrollBottom int
	savedCursor  cursorState
	originMode   bool
	insertMode   bool
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

func (s *Screen) reset() {
	s.buffer = s.newBuffer(s.Frame.Width, s.Frame.Height)
	s.Frame = s.buffer.frame
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
			buffer:       s.buffer.clone(),
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
	s.buffer = s.newBuffer(s.Frame.Width, s.Frame.Height)
	s.Frame = s.buffer.frame
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
	s.buffer = state.buffer
	if s.buffer == nil {
		s.buffer = bufferFromFrame(cloneFrame(state.frame))
		s.fillMissingRowIDs(s.buffer)
	}
	s.Frame = s.buffer.frame
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

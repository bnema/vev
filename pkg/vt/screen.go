package vt

import (
	"unicode/utf8"

	"github.com/bnema/vev/pkg/renderer"
)

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
	buffer  *buffer

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
	b := newBuffer(width, height)
	s := &Screen{
		Frame:            b.frame,
		buffer:           b,
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

// LineBounds returns an owned copy of the live grid's per-row logical extents,
// indexed like Frame rows. It returns nil when the screen has no buffer.
func (s *Screen) LineBounds() []LineBound {
	if s == nil || s.buffer == nil {
		return nil
	}
	return append([]LineBound(nil), s.buffer.boundaries...)
}

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
		s.buffer.hard(s.Row)
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

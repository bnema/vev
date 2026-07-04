package theme

import (
	"bytes"
	"strconv"
	"strings"

	"github.com/bnema/vev/pkg/renderer"
)

const maxPending = 64

// Theme describes terminal default colors reported by OSC 10/11.
type Theme struct {
	Foreground renderer.RGB
	Background renderer.RGB
	HasFG      bool
	HasBG      bool
	TrueColor  bool
	Known      bool
}

// ParseXColor parses the XParseColor formats commonly returned by OSC 10/11.
func ParseXColor(s string) (renderer.RGB, bool) {
	if strings.HasPrefix(s, "#") {
		if len(s) != 7 {
			return renderer.RGB{}, false
		}
		r, ok := parseHexByte(s[1:3])
		if !ok {
			return renderer.RGB{}, false
		}
		g, ok := parseHexByte(s[3:5])
		if !ok {
			return renderer.RGB{}, false
		}
		b, ok := parseHexByte(s[5:7])
		if !ok {
			return renderer.RGB{}, false
		}
		return renderer.RGB{R: r, G: g, B: b}, true
	}

	if !strings.HasPrefix(s, "rgb:") {
		return renderer.RGB{}, false
	}
	parts := strings.Split(s[len("rgb:"):], "/")
	if len(parts) != 3 {
		return renderer.RGB{}, false
	}
	width := len(parts[0])
	if width != 2 && width != 4 {
		return renderer.RGB{}, false
	}
	var out [3]uint8
	for i, part := range parts {
		if len(part) != width {
			return renderer.RGB{}, false
		}
		if width == 2 {
			v, ok := parseHexByte(part)
			if !ok {
				return renderer.RGB{}, false
			}
			out[i] = v
			continue
		}
		v, err := strconv.ParseUint(part, 16, 16)
		if err != nil {
			return renderer.RGB{}, false
		}
		out[i] = uint8(v >> 8)
	}
	return renderer.RGB{R: out[0], G: out[1], B: out[2]}, true
}

func parseHexByte(s string) (uint8, bool) {
	v, err := strconv.ParseUint(s, 16, 8)
	if err != nil {
		return 0, false
	}
	return uint8(v), true
}

// Scanner decodes OSC 10/11 color responses while preserving ordinary bytes.
type Scanner struct {
	pending []byte
}

// Scan extracts ESC ] 10;<color> and ESC ] 11;<color> terminated by BEL or ST.
// All non-matching bytes are emitted through onBytes in original order, and a
// contiguous run of ordinary bytes (including any ESC that does not start a
// color-OSC sequence, e.g. keyboard/mouse escape sequences like SGR mouse
// reports or arrow keys) is always delivered as a single onBytes call so
// callers never see it split. Partial OSC color responses are buffered across
// calls, but the buffer is bounded so an unterminated OSC cannot block
// unrelated input forever.
func (s *Scanner) Scan(data []byte, onColor func(kind int, rgb renderer.RGB), onBytes func([]byte)) {
	if len(s.pending) > 0 {
		combined := make([]byte, 0, len(s.pending)+len(data))
		combined = append(combined, s.pending...)
		combined = append(combined, data...)
		data = combined
		s.pending = nil
	}

	byteStart := 0
	for i := 0; i < len(data); i++ {
		if data[i] != '\x1b' {
			continue
		}

		completePrefix, possiblePrefix := colorOSCPrefix(data[i:])
		if !completePrefix {
			if possiblePrefix {
				if byteStart < i {
					onBytes(data[byteStart:i])
				}
				s.bufferOrFlush(data[i:], onBytes)
				return
			}
			// Not the start of a color-OSC sequence: leave this ESC inside
			// the current passthrough run instead of splitting it out. It
			// may be the introducer of an unrelated escape sequence (SGR
			// mouse report, arrow key, etc.) that must reach the reader
			// intact.
			continue
		}

		if byteStart < i {
			onBytes(data[byteStart:i])
		}

		termStart, termEnd := findTerminator(data[i+5:])
		if termStart < 0 {
			s.bufferOrFlush(data[i:], onBytes)
			return
		}
		termStart += i + 5
		termEnd += i + 5
		raw := data[i:termEnd]
		kind := int(data[i+3]-'0') + 10
		rgb, ok := ParseXColor(string(data[i+5 : termStart]))
		if ok {
			onColor(kind, rgb)
		} else {
			onBytes(raw)
		}
		i = termEnd - 1
		byteStart = termEnd
	}

	if byteStart < len(data) {
		onBytes(data[byteStart:])
	}
}

func (s *Scanner) bufferOrFlush(partial []byte, onBytes func([]byte)) {
	if len(partial) > maxPending {
		onBytes(partial)
		s.pending = nil
		return
	}
	s.pending = append(s.pending, partial...)
}

func colorOSCPrefix(data []byte) (complete bool, possible bool) {
	if len(data) < 2 {
		return false, false
	}
	prefixes := [][]byte{[]byte("\x1b]10;"), []byte("\x1b]11;")}
	for _, prefix := range prefixes {
		if len(data) >= len(prefix) {
			if bytes.Equal(data[:len(prefix)], prefix) {
				return true, true
			}
			continue
		}
		if bytes.Equal(data, prefix[:len(data)]) {
			return false, true
		}
	}
	return false, false
}

func findTerminator(data []byte) (start int, end int) {
	for i := range data {
		switch data[i] {
		case '\x07':
			return i, i + 1
		case '\x1b':
			if i+1 < len(data) && data[i+1] == '\\' {
				return i, i + 2
			}
		}
	}
	return -1, -1
}

// Blend linearly interpolates between a and b.
func Blend(a, b renderer.RGB, t float64) renderer.RGB {
	if t < 0 {
		t = 0
	}
	if t > 1 {
		t = 1
	}
	return renderer.RGB{
		R: blendByte(a.R, b.R, t),
		G: blendByte(a.G, b.G, t),
		B: blendByte(a.B, b.B, t),
	}
}

func blendByte(a, b uint8, t float64) uint8 {
	return uint8(float64(a) + (float64(b)-float64(a))*t + 0.5)
}

func StatusBarStyle(t Theme) renderer.Style {
	if !usable(t) {
		return renderer.DefaultStyle()
	}
	style := renderer.DefaultStyle()
	style.HasForegroundRGB = true
	style.ForegroundRGB = t.Foreground
	style.HasBackgroundRGB = true
	style.BackgroundRGB = Blend(t.Background, t.Foreground, 0.12)
	return style
}

func AccentStyle(t Theme) renderer.Style {
	if !usable(t) {
		return inverseStyle()
	}
	style := renderer.DefaultStyle()
	style.HasForegroundRGB = true
	style.ForegroundRGB = t.Foreground
	style.HasBackgroundRGB = true
	style.BackgroundRGB = Blend(t.Background, t.Foreground, 0.25)
	return style
}

func BorderStyle(t Theme) renderer.Style {
	if !usable(t) {
		return renderer.DefaultStyle()
	}
	style := renderer.DefaultStyle()
	style.HasForegroundRGB = true
	style.ForegroundRGB = Blend(t.Foreground, t.Background, 0.40)
	return style
}

func DimStyle(style renderer.Style, t Theme) renderer.Style {
	if !usable(t) {
		return style
	}
	out := style
	out.HasForegroundRGB = true
	if style.HasForegroundRGB {
		out.ForegroundRGB = Blend(style.ForegroundRGB, t.Background, 0.35)
	} else if style.Foreground >= 0 {
		out.ForegroundRGB = Blend(xterm256Color(style.Foreground), t.Background, 0.35)
	} else {
		out.ForegroundRGB = Blend(t.Foreground, t.Background, 0.35)
	}
	out.HasBackgroundRGB = true
	if style.HasBackgroundRGB {
		out.BackgroundRGB = Blend(style.BackgroundRGB, t.Background, 0.35)
	} else if style.Background >= 0 {
		out.BackgroundRGB = Blend(xterm256Color(style.Background), t.Background, 0.35)
	} else {
		out.BackgroundRGB = Blend(t.Background, t.Background, 0.35)
	}
	return out
}

func xterm256Color(index int) renderer.RGB {
	if index < 0 {
		return renderer.RGB{}
	}
	base := [...]renderer.RGB{
		{R: 0x00, G: 0x00, B: 0x00}, {R: 0x80, G: 0x00, B: 0x00}, {R: 0x00, G: 0x80, B: 0x00}, {R: 0x80, G: 0x80, B: 0x00},
		{R: 0x00, G: 0x00, B: 0x80}, {R: 0x80, G: 0x00, B: 0x80}, {R: 0x00, G: 0x80, B: 0x80}, {R: 0xc0, G: 0xc0, B: 0xc0},
		{R: 0x80, G: 0x80, B: 0x80}, {R: 0xff, G: 0x00, B: 0x00}, {R: 0x00, G: 0xff, B: 0x00}, {R: 0xff, G: 0xff, B: 0x00},
		{R: 0x00, G: 0x00, B: 0xff}, {R: 0xff, G: 0x00, B: 0xff}, {R: 0x00, G: 0xff, B: 0xff}, {R: 0xff, G: 0xff, B: 0xff},
	}
	if index < len(base) {
		return base[index]
	}
	if index < 232 {
		index -= 16
		levels := [...]uint8{0, 95, 135, 175, 215, 255}
		return renderer.RGB{R: levels[index/36], G: levels[(index/6)%6], B: levels[index%6]}
	}
	if index < 256 {
		level := uint8(8 + (index-232)*10)
		return renderer.RGB{R: level, G: level, B: level}
	}
	return base[index%len(base)]
}

func SelectionStyle(t Theme) renderer.Style {
	if !usable(t) {
		return inverseStyle()
	}
	return AccentStyle(t)
}

func usable(t Theme) bool {
	return t.Known && t.TrueColor && t.HasFG && t.HasBG
}

func inverseStyle() renderer.Style {
	style := renderer.DefaultStyle()
	style.Inverse = true
	return style
}

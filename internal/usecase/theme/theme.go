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
// All non-matching bytes are emitted through onBytes in original order. Partial
// OSC color responses are buffered across calls, but the buffer is bounded so an
// unterminated OSC cannot block unrelated input forever.
func (s *Scanner) Scan(data []byte, onColor func(kind int, rgb renderer.RGB), onBytes func([]byte)) {
	if len(s.pending) > 0 {
		combined := make([]byte, 0, len(s.pending)+len(data))
		combined = append(combined, s.pending...)
		combined = append(combined, data...)
		data = combined
		s.pending = nil
	}

	for pos := 0; pos < len(data); {
		rel := bytes.IndexByte(data[pos:], '\x1b')
		if rel < 0 {
			onBytes(data[pos:])
			return
		}
		start := pos + rel
		if pos < start {
			onBytes(data[pos:start])
		}

		completePrefix, possiblePrefix := colorOSCPrefix(data[start:])
		if !completePrefix {
			if possiblePrefix {
				s.bufferOrFlush(data[start:], onBytes)
				return
			}
			onBytes(data[start : start+1])
			pos = start + 1
			continue
		}

		termStart, termEnd := findTerminator(data[start+5:])
		if termStart < 0 {
			s.bufferOrFlush(data[start:], onBytes)
			return
		}
		termStart += start + 5
		termEnd += start + 5
		raw := data[start:termEnd]
		kind := int(data[start+3]-'0') + 10
		rgb, ok := ParseXColor(string(data[start+5 : termStart]))
		if ok {
			onColor(kind, rgb)
		} else {
			onBytes(raw)
		}
		pos = termEnd
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
	for i := 0; i < len(data); i++ {
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

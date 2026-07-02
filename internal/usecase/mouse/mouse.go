package mouse

import (
	"bytes"
	"strconv"
)

// Type describes the kind of SGR mouse event.
type Type int

const (
	Press Type = iota
	Release
	Motion
)

// Button describes the button encoded by an SGR mouse event.
type Button int

const (
	Left Button = iota
	Middle
	Right
	WheelUp
	WheelDown
	Other
)

// Event is a decoded SGR mouse report.
type Event struct {
	Type   Type
	Button Button
	Col    int
	Row    int
	Raw    []byte
}

// Scanner decodes SGR mouse reports from a byte stream while preserving the
// ordering of decoded mouse events and ordinary byte segments.
type Scanner struct {
	pending []byte
}

// Scan decodes SGR mouse reports of the form ESC [ < Cb ; Cx ; Cy M/m.
// Non-mouse bytes are emitted through onBytes in their original order. Only
// partial reports after a complete ESC[< introducer are buffered between calls;
// a bare ESC or ESC[ at the end of data is passed through unchanged.
func (s *Scanner) Scan(data []byte, onMouse func(Event), onBytes func([]byte)) {
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

		remaining := len(data) - i
		if remaining < 3 {
			continue
		}
		if data[i+1] != '[' || data[i+2] != '<' {
			continue
		}

		if byteStart < i {
			onBytes(data[byteStart:i])
		}

		raw, ev, complete, ok := parseSGR(data[i:])
		if !complete {
			s.pending = append(s.pending, data[i:]...)
			return
		}
		if !ok {
			onBytes(raw)
			i += len(raw) - 1
			byteStart = i + 1
			continue
		}

		onMouse(ev)
		i += len(raw) - 1
		byteStart = i + 1
	}

	if byteStart < len(data) {
		onBytes(data[byteStart:])
	}
}

func parseSGR(data []byte) (raw []byte, ev Event, complete bool, ok bool) {
	end := -1
	for i := 3; i < len(data); i++ {
		switch data[i] {
		case 'M', 'm':
			end = i
			goto found
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', ';':
			continue
		default:
			return data[:i+1], Event{}, true, false
		}
	}
found:
	if end == -1 {
		return nil, Event{}, false, false
	}

	raw = data[:end+1]
	parts := bytes.Split(data[3:end], []byte(";"))
	if len(parts) != 3 {
		return raw, Event{}, true, false
	}
	cb, ok := atoiBytes(parts[0])
	if !ok {
		return raw, Event{}, true, false
	}
	cx, ok := atoiBytes(parts[1])
	if !ok {
		return raw, Event{}, true, false
	}
	cy, ok := atoiBytes(parts[2])
	if !ok {
		return raw, Event{}, true, false
	}
	if cx < 1 || cy < 1 {
		return raw, Event{}, true, false
	}

	ev = Event{
		Type:   eventType(cb, data[end]),
		Button: eventButton(cb),
		Col:    cx - 1,
		Row:    cy - 1,
		Raw:    append([]byte(nil), raw...),
	}
	return raw, ev, true, true
}

func atoiBytes(b []byte) (int, bool) {
	if len(b) == 0 {
		return 0, false
	}
	v, err := strconv.Atoi(string(b))
	return v, err == nil
}

func eventType(cb int, final byte) Type {
	if final == 'm' {
		return Release
	}
	if cb&32 != 0 {
		return Motion
	}
	return Press
}

func eventButton(cb int) Button {
	if cb&64 != 0 {
		switch cb {
		case 64:
			return WheelUp
		case 65:
			return WheelDown
		default:
			return Other
		}
	}

	switch cb & 3 {
	case 0:
		return Left
	case 1:
		return Middle
	case 2:
		return Right
	default:
		return Other
	}
}

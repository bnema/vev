package vt

import (
	"strconv"
	"strings"

	"github.com/bnema/vev/pkg/renderer"
)

const (
	progressStateClear = iota
	progressStateNormal
	progressStateError
	progressStateIndeterminate
	progressStatePaused
)

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
// terminator). Terminal titles, notification sequences (OSC 9, OSC 777
// "notify"), and clipboard set requests (OSC 52) are acted on; every other OSC
// is discarded.

func (s *Screen) handleOSC(payload []byte) {
	if len(payload) >= len("52;") && payload[0] == '5' && payload[1] == '2' && payload[2] == ';' {
		s.handleOSC52(string(payload[len("52;"):]))
		return
	}
	p := string(payload)
	if selector, title, ok := strings.Cut(p, ";"); ok && (selector == "0" || selector == "2") {
		s.terminalTitle = title
		return
	}
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

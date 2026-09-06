package client

import (
	"errors"
	"strings"
	"unicode/utf8"
)

const uiMaxInputBytes = 16 << 10

var errUIInvalidInput = errors.New("invalid UI input")

// encodeUIText accepts literal printable UTF-8, never terminal controls.
func encodeUIText(text string) ([]byte, error) {
	if text == "" || len(text) > uiMaxInputBytes || !utf8.ValidString(text) {
		return nil, errUIInvalidInput
	}
	for _, r := range text {
		if r < 0x20 || r >= 0x7f && r <= 0x9f {
			return nil, errUIInvalidInput
		}
	}
	return []byte(text), nil
}

// encodeUIKeys validates the complete batch as well as each token: individually
// legal Escape and ASCII tokens must not assemble a terminal reply or paste.
func encodeUIKeys(keys []string, applicationCursor bool) ([]byte, error) {
	if len(keys) == 0 || len(keys) > 256 {
		return nil, errUIInvalidInput
	}
	var out []byte
	for _, key := range keys {
		encoded, ok := encodeUIKey(key, applicationCursor)
		if !ok || len(out)+len(encoded) > uiMaxInputBytes {
			return nil, errUIInvalidInput
		}
		out = append(out, encoded...)
	}
	if !validUIKeyBatch(out) {
		return nil, errUIInvalidInput
	}
	return out, nil
}

func encodeUIKey(key string, applicationCursor bool) (string, bool) {
	if strings.HasPrefix(key, "Alt+") {
		plain := strings.TrimPrefix(key, "Alt+")
		switch plain {
		case "Enter", "Escape", "Tab", "Backspace", "Space":
			encoded, _ := encodeUIKey(plain, applicationCursor)
			return "\x1b" + encoded, true
		}
		if len(plain) == 1 && plain[0] >= 0x20 && plain[0] <= 0x7e {
			return "\x1b" + plain, true
		}
		return "", false
	}
	if strings.HasPrefix(key, "Ctrl+") {
		plain := strings.TrimPrefix(key, "Ctrl+")
		if len(plain) == 1 && (plain[0] >= 'A' && plain[0] <= 'Z' || strings.ContainsRune("@[\\]^_", rune(plain[0]))) {
			return string([]byte{plain[0] & 31}), true
		}
		return "", false
	}
	if len(key) == 1 && key[0] >= 0x20 && key[0] <= 0x7e {
		return key, true
	}
	switch key {
	case "Enter":
		return "\r", true
	case "Escape":
		return "\x1b", true
	case "Tab":
		return "\t", true
	case "Backspace":
		return "\x7f", true
	case "Space":
		return " ", true
	case "PageUp":
		return "\x1b[5~", true
	case "PageDown":
		return "\x1b[6~", true
	}
	final := ""
	switch key {
	case "Up":
		final = "A"
	case "Down":
		final = "B"
	case "Right":
		final = "C"
	case "Left":
		final = "D"
	case "Home":
		final = "H"
	case "End":
		final = "F"
	default:
		return "", false
	}
	prefix := "\x1b["
	if applicationCursor {
		prefix = "\x1bO"
	}
	return prefix + final, true
}

func validUIKeyBatch(data []byte) bool {
	for i := 0; i < len(data); i++ {
		if data[i] != 0x1b {
			continue
		}
		// A bare Escape is the only intentionally incomplete escape sequence.
		if i+1 == len(data) {
			continue
		}
		switch data[i+1] {
		case ']', 'P', '_', '^', 'X':
			// A lone Alt character is valid, but adding a suffix would turn
			// it into an OSC/DCS/APC/string prefix.
			if i+2 < len(data) {
				return false
			}
			continue
		case '[':
			// Alt+[ is a valid one-character key. Once it has a suffix, only
			// one of the finite CSI keyboard sequences is accepted.
			if i+2 >= len(data) {
				continue
			}
			if strings.ContainsRune("ABCDHF", rune(data[i+2])) {
				i += 2
				continue
			}
			if i+3 < len(data) && (data[i+2] == '5' || data[i+2] == '6') && data[i+3] == '~' {
				i += 3
				continue
			}
			return false
		case 'O':
			// Alt+O is valid on its own; otherwise accept only application
			// cursor sequences.
			if i+2 >= len(data) {
				continue
			}
			if !strings.ContainsRune("ABCDHF", rune(data[i+2])) {
				return false
			}
			i += 2
		default:
			// The remaining form is Alt+one character. Its second byte may be
			// one of the explicit control-key aliases, but no other control
			// sequence or C1 byte is admitted.
			next := data[i+1]
			if next < 0x20 && next != '\r' && next != '\t' && next != 0x1b || next >= 0x80 && next <= 0x9f {
				return false
			}
			i++
		}
	}
	return true
}

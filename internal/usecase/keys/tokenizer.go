package keys

import (
	"bytes"
	"unicode/utf8"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/keys/kittykey"
)

// escKind classifies the input introduced by one ESC byte.
type escKind uint8

const (
	// escIncomplete means more bytes are needed before the ESC can be
	// classified: a lone trailing ESC, a partial Alt-arrow CSI, or a partial
	// UTF-8 rune after ESC.
	escIncomplete escKind = iota
	// escPaste is a complete bracketed paste, open marker through close marker.
	escPaste
	// escAltArrow is a complete ESC [ 1 ; 3|9 <A-D> sequence.
	escAltArrow
	// escControlPrefix is ESC [ or ESC O, the start of a terminal control
	// sequence that passes through untouched.
	escControlPrefix
	// escAltRune is ESC followed by one complete, valid UTF-8 rune.
	escAltRune
	// escKittyKey is a complete kitty keyboard protocol CSI u key event.
	escKittyKey
	// escBare is an ESC that starts none of the above.
	escBare
)

// escToken is one lexed ESC-introduced unit. raw always starts with ESC and
// aliases the scanned input.
type escToken struct {
	kind  escKind
	raw   []byte
	rune  rune // escAltRune and escControlPrefix: the rune after ESC
	arrow byte // escAltArrow: the final byte, one of 'A'..'D'
	key   kittykey.Event
}

const altArrowLen = len("\x1b[1;3A")

// scanEscape lexes the unit starting at data[0], which must be ESC. It only
// classifies bytes; binding lookups and forwarding decisions belong to Router.
func scanEscape(data []byte, kitty bool) escToken {
	if len(data) == 1 {
		return escToken{kind: escIncomplete, raw: data}
	}
	if bytes.HasPrefix(data, ports.BracketedPasteOpenMarker) {
		if rel := bytes.Index(data, ports.BracketedPasteCloseMarker); rel >= 0 {
			return escToken{kind: escPaste, raw: data[:rel+len(ports.BracketedPasteCloseMarker)]}
		}
	}
	if final, ok := altArrowFinal(data); ok {
		return escToken{kind: escAltArrow, raw: data[:altArrowLen], arrow: final}
	}
	if isAltArrowPrefix(data) {
		return escToken{kind: escIncomplete, raw: data}
	}
	if kitty {
		if ev, n, ok, partial := kittykey.Parse(data); ok {
			return escToken{kind: escKittyKey, raw: data[:n], key: ev}
		} else if partial {
			return escToken{kind: escIncomplete, raw: data}
		}
	}
	next := data[1]
	if next == '[' || next == 'O' {
		return escToken{kind: escControlPrefix, raw: data[:2], rune: rune(next)}
	}
	rest := data[1:]
	if partialUTF8Rune(rest) {
		return escToken{kind: escIncomplete, raw: data}
	}
	if key, size := utf8.DecodeRune(rest); key != utf8.RuneError || size > 1 {
		return escToken{kind: escAltRune, raw: data[:1+size], rune: key}
	}
	return escToken{kind: escBare, raw: data[:1]}
}

// altArrowFinal reports the arrow final byte of a complete Alt (;3) or Meta
// (;9) modified arrow CSI at the start of data.
func altArrowFinal(data []byte) (byte, bool) {
	if len(data) < altArrowLen {
		return 0, false
	}
	seq := data[:altArrowLen]
	if seq[1] != '[' || seq[2] != '1' || seq[3] != ';' || (seq[4] != '3' && seq[4] != '9') {
		return 0, false
	}
	switch final := seq[5]; final {
	case 'A', 'B', 'C', 'D':
		return final, true
	default:
		return 0, false
	}
}

// isAltArrowPrefix reports whether data is a strict prefix of an Alt/Meta
// arrow CSI, meaning the next read may still complete it.
func isAltArrowPrefix(data []byte) bool {
	if len(data) < 2 || len(data) >= altArrowLen {
		return false
	}
	return matchesPrefix(data, "\x1b[1;3") || matchesPrefix(data, "\x1b[1;9")
}

func matchesPrefix(data []byte, want string) bool {
	return len(data) <= len(want) && string(data) == want[:len(data)]
}

func partialUTF8Rune(data []byte) bool {
	if len(data) == 0 || utf8.FullRune(data) {
		return false
	}
	key, size := utf8.DecodeRune(data)
	return key == utf8.RuneError && size == 1
}

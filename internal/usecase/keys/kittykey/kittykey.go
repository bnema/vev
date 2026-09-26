// Package kittykey decodes kitty keyboard protocol key events and re-encodes
// them for a receiver that requested a given set of enhancement flags.
//
// vev enables Disambiguate|AlternateKeys on the outer terminal. Keys that the
// legacy encoding cannot express unambiguously then arrive as
// CSI code[:shifted[:base]] [; mods] u, and F1-F4 as CSI [1;mods] P/Q/S.
// Everything else stays legacy. Translate rewrites only those sequences, so
// legacy input passes through byte-for-byte.
//
// The package is pure: no I/O, clocks, or ports.
package kittykey

import (
	"bytes"
	"strconv"
	"unicode"
	"unicode/utf8"
)

// Enhancement flags from the kitty keyboard protocol.
const (
	FlagDisambiguate  = 1 << 0
	FlagReportEvents  = 1 << 1
	FlagAlternateKeys = 1 << 2
	FlagAllKeys       = 1 << 3
	FlagText          = 1 << 4

	// OuterFlags are the flags vev requests from the outer terminal.
	OuterFlags = FlagDisambiguate | FlagAlternateKeys
)

// Modifier bits, as encoded in the mods parameter minus one.
const (
	ModShift = 1 << 0
	ModAlt   = 1 << 1
	ModCtrl  = 1 << 2
	ModSuper = 1 << 3
	ModHyper = 1 << 4
	ModMeta  = 1 << 5

	modLocks = 1<<6 | 1<<7 // caps lock, num lock
)

// Key codes of the functional keys that have legacy single-byte forms.
const (
	KeyEscape    = 27
	KeyEnter     = 13
	KeyTab       = 9
	KeyBackspace = 127
)

// Event is one decoded CSI u key press.
type Event struct {
	// Code is the unicode key code: the unshifted key of the current layout.
	Code rune
	// Shifted is the shifted key, when reported (AlternateKeys with Shift).
	Shifted rune
	// Base is the key in the standard PC-101 layout, when reported.
	Base rune
	// Mods is the modifier bit set (ModShift...), lock bits removed.
	Mods int
	// Release reports a key-release event (only with FlagReportEvents).
	Release bool
}

// OnlyMods reports whether exactly the given modifiers are active.
func (e Event) OnlyMods(mods int) bool { return e.Mods == mods }

// Digit reports the top-row digit 1-9 the key represents, preferring the
// PC-101 base key so that layouts like AZERTY (whose unshifted digit keys
// produce symbols) still map to digits.
func (e Event) Digit() (int, bool) {
	for _, r := range [...]rune{e.Base, e.Code} {
		if r >= '1' && r <= '9' {
			return int(r - '0'), true
		}
	}
	return 0, false
}

const maxSeq = 64

var (
	pasteOpen  = []byte("\x1b[200~")
	pasteClose = []byte("\x1b[201~")
)

// Parse decodes one CSI u key sequence at the start of data. It returns the
// event, the sequence length, and ok. partial reports that data is a strict
// prefix of a sequence that could still become a CSI u key event.
func Parse(data []byte) (ev Event, n int, ok, partial bool) {
	if len(data) < 2 || data[0] != 0x1b || data[1] != '[' {
		return Event{}, 0, false, len(data) == 1 && data[0] == 0x1b
	}
	i := 2
	for ; i < len(data) && i < maxSeq; i++ {
		b := data[i]
		if (b >= '0' && b <= '9') || b == ';' || b == ':' {
			continue
		}
		if b != 'u' || i == 2 {
			return Event{}, 0, false, false
		}
		ev, ok := parseParams(data[2:i])
		return ev, i + 1, ok, false
	}
	return Event{}, 0, false, i < maxSeq
}

func parseParams(params []byte) (Event, bool) {
	fields := splitByte(params, ';')
	if len(fields) > 3 {
		return Event{}, false
	}
	keys := splitByte(fields[0], ':')
	if len(keys) > 3 {
		return Event{}, false
	}
	code, ok := atoi(keys[0])
	if !ok || code == 0 {
		return Event{}, false
	}
	ev := Event{Code: rune(code)}
	if len(keys) > 1 && len(keys[1]) > 0 {
		v, ok := atoi(keys[1])
		if !ok {
			return Event{}, false
		}
		ev.Shifted = rune(v)
	}
	if len(keys) > 2 && len(keys[2]) > 0 {
		v, ok := atoi(keys[2])
		if !ok {
			return Event{}, false
		}
		ev.Base = rune(v)
	}
	if len(fields) > 1 && len(fields[1]) > 0 {
		sub := splitByte(fields[1], ':')
		if len(sub) > 2 {
			return Event{}, false
		}
		mods, ok := atoi(sub[0])
		if !ok || mods < 1 {
			return Event{}, false
		}
		ev.Mods = (mods - 1) &^ modLocks
		if len(sub) == 2 && len(sub[1]) > 0 {
			event, ok := atoi(sub[1])
			if !ok || event < 1 || event > 3 {
				return Event{}, false
			}
			ev.Release = event == 3
		}
	}
	// A third field carries associated text (FlagText); it is not needed to
	// re-encode the key and is intentionally ignored.
	return ev, true
}

// Translate rewrites every complete CSI u key sequence and kitty F1-F4 form in
// data for a receiver that requested flags. With flags == 0 the result is the
// legacy encoding; keys without a legacy form are dropped. With
// FlagDisambiguate set, CSI u is kept and only subfields the receiver did not
// ask for are removed. Other bytes, including bracketed paste bodies, are
// copied unchanged. When nothing changes, data itself is returned.
func Translate(data []byte, flags int) []byte {
	var out []byte
	last := 0
	for i := 0; i+2 < len(data); i++ {
		if data[i] != 0x1b || data[i+1] != '[' {
			continue
		}
		if bytes.HasPrefix(data[i:], pasteOpen) {
			end := bytes.Index(data[i:], pasteClose)
			if end < 0 {
				break
			}
			i += end + len(pasteClose) - 1
			continue
		}
		var repl []byte
		var n int
		if ev, size, ok, _ := Parse(data[i:]); ok {
			repl, n = encode(ev, flags), size
		} else if fkey, size, ok := parseFunctionKey(data[i:]); ok {
			repl, n = fkey.encode(flags), size
		} else {
			continue
		}
		if out == nil {
			out = make([]byte, 0, len(data))
		}
		out = append(out, data[last:i]...)
		out = append(out, repl...)
		i += n - 1
		last = i + 1
	}
	if out == nil {
		return data
	}
	return append(out, data[last:]...)
}

func encode(ev Event, flags int) []byte {
	if ev.Release && flags&FlagReportEvents == 0 {
		return nil
	}
	if flags&FlagDisambiguate != 0 || flags&FlagAllKeys != 0 {
		return encodeCSIu(ev, flags)
	}
	return encodeLegacy(ev)
}

func encodeCSIu(ev Event, flags int) []byte {
	out := []byte("\x1b[")
	out = strconv.AppendInt(out, int64(ev.Code), 10)
	if flags&FlagAlternateKeys != 0 && (ev.Shifted != 0 || ev.Base != 0) {
		out = append(out, ':')
		if ev.Shifted != 0 {
			out = strconv.AppendInt(out, int64(ev.Shifted), 10)
		}
		if ev.Base != 0 {
			out = append(out, ':')
			out = strconv.AppendInt(out, int64(ev.Base), 10)
		}
	}
	release := ev.Release && flags&FlagReportEvents != 0
	if ev.Mods != 0 || release {
		out = append(out, ';')
		out = strconv.AppendInt(out, int64(ev.Mods+1), 10)
		if release {
			out = append(out, ":3"...)
		}
	}
	return append(out, 'u')
}

// encodeLegacy produces the xterm-style bytes for ev, or nil when the key has
// no legacy representation.
func encodeLegacy(ev Event) []byte {
	if ev.Mods&^(ModShift|ModAlt|ModCtrl) != 0 {
		// Super, Hyper, and Meta have no legacy encoding.
		return nil
	}
	var body []byte
	switch ev.Code {
	case KeyEscape:
		body = []byte{0x1b}
	case KeyEnter:
		body = []byte{'\r'}
	case KeyTab:
		if ev.Mods&ModShift != 0 {
			body = []byte("\x1b[Z")
		} else {
			body = []byte{'\t'}
		}
	case KeyBackspace:
		if ev.Mods&ModCtrl != 0 {
			body = []byte{0x08}
		} else {
			body = []byte{0x7f}
		}
	default:
		if seq, ok := keypadLegacy[ev.Code]; ok {
			if mods := ev.Mods &^ ModShift; mods != 0 && len(seq) > 2 && seq[0] == 0x1b {
				// Modified keypad navigation uses xterm's CSI 1;mods form.
				return modifiedCSI(seq, ev.Mods)
			}
			body = []byte(seq)
			break
		}
		if ev.Code < 0x20 || (ev.Code >= 0xe000 && ev.Code <= 0xf8ff) || !utf8.ValidRune(ev.Code) {
			// Functional keys in the private use area have no legacy form.
			return nil
		}
		body = legacyText(ev)
	}
	if ev.Mods&ModAlt != 0 {
		return append([]byte{0x1b}, body...)
	}
	return body
}

func legacyText(ev Event) []byte {
	r := ev.Code
	if ev.Mods&ModShift != 0 {
		switch {
		case ev.Shifted != 0:
			r = ev.Shifted
		default:
			r = unicode.ToUpper(r)
		}
	}
	if ev.Mods&ModCtrl != 0 {
		if c, ok := ctrlByte(r); ok {
			return []byte{c}
		}
		// Non-Latin layouts: fall back to the PC-101 base key so Ctrl+letter
		// still produces its control byte.
		if c, ok := ctrlByte(ev.Base); ok {
			return []byte{c}
		}
	}
	return utf8.AppendRune(nil, r)
}

// ctrlByte maps a key to the C0 byte xterm sends for Ctrl+key.
func ctrlByte(r rune) (byte, bool) {
	switch {
	case r >= 'a' && r <= 'z':
		return byte(r - 'a' + 1), true
	case r >= 'A' && r <= 'Z':
		return byte(r - 'A' + 1), true
	}
	switch r {
	case ' ', '@', '2':
		return 0x00, true
	case '[', '3':
		return 0x1b, true
	case '\\', '4':
		return 0x1c, true
	case ']', '5':
		return 0x1d, true
	case '^', '6', '~':
		return 0x1e, true
	case '_', '/', '7', '-':
		return 0x1f, true
	case '8', '?':
		return 0x7f, true
	}
	return 0, false
}

// keypadLegacy maps kitty keypad private-use codes (reported as separate keys
// under FlagDisambiguate) to the bytes a legacy terminal sends.
var keypadLegacy = map[rune]string{
	57399: "0", 57400: "1", 57401: "2", 57402: "3", 57403: "4",
	57404: "5", 57405: "6", 57406: "7", 57407: "8", 57408: "9",
	57409: ".", 57410: "/", 57411: "*", 57412: "-", 57413: "+",
	57414: "\r", 57415: "=", 57416: ",",
	57417: "\x1b[D", 57418: "\x1b[C", 57419: "\x1b[A", 57420: "\x1b[B",
	57421: "\x1b[5~", 57422: "\x1b[6~", 57423: "\x1b[H", 57424: "\x1b[F",
	57425: "\x1b[2~", 57426: "\x1b[3~", 57427: "\x1b[E",
}

// modifiedCSI turns a legacy cursor (CSI X) or tilde (CSI n ~) key into its
// xterm modified form: CSI 1;m X or CSI n;m ~.
func modifiedCSI(seq string, mods int) []byte {
	m := strconv.Itoa(mods + 1)
	final := seq[len(seq)-1]
	if final == '~' {
		return []byte(seq[:len(seq)-1] + ";" + m + "~")
	}
	return []byte("\x1b[1;" + m + string(final))
}

// functionKey is a kitty CSI [1;mods] P|Q|S form of F1, F2, or F4. Legacy
// terminals send SS3 P|Q|S when unmodified.
type functionKey struct {
	final byte
	mods  []byte // the raw "1;mods" parameter bytes, empty when unmodified
}

func parseFunctionKey(data []byte) (functionKey, int, bool) {
	for i := 2; i < len(data) && i < maxSeq; i++ {
		b := data[i]
		if (b >= '0' && b <= '9') || b == ';' || b == ':' {
			continue
		}
		if b != 'P' && b != 'Q' && b != 'S' {
			return functionKey{}, 0, false
		}
		params := data[2:i]
		if len(params) != 0 && (len(params) < 3 || params[0] != '1' || params[1] != ';') {
			return functionKey{}, 0, false
		}
		return functionKey{final: b, mods: params}, i + 1, true
	}
	return functionKey{}, 0, false
}

func (f functionKey) encode(flags int) []byte {
	if len(f.mods) == 0 && flags&FlagDisambiguate == 0 {
		return []byte{0x1b, 'O', f.final}
	}
	out := append([]byte("\x1b["), f.mods...)
	return append(out, f.final)
}

func splitByte(b []byte, sep byte) [][]byte {
	var out [][]byte
	start := 0
	for i := 0; i <= len(b); i++ {
		if i == len(b) || b[i] == sep {
			out = append(out, b[start:i])
			start = i + 1
		}
	}
	return out
}

func atoi(b []byte) (int, bool) {
	if len(b) == 0 || len(b) > 7 {
		return 0, false
	}
	n := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}

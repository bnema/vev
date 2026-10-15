package kittykey

import (
	"bytes"
	"strconv"
	"unicode/utf8"
)

// Kitty functional key codes of the modifier and lock keys. RequestFlags
// reports them; OuterFlags never does.
const (
	keyCapsLock     = 57358
	keyNumLock      = 57360
	keyLeftShift    = 57441
	keyLeftControl  = 57442
	keyRightControl = 57448
	keyISOLevel5    = 57454
)

// Normalizer rewrites the outer terminal's RequestFlags key stream into the
// OuterFlags encoding the rest of vev consumes, and tracks whether Ctrl is
// physically held. Release events, modifier and lock key events are dropped,
// repeats become presses, and text keys become their text again.
//
// Input already in the OuterFlags or legacy encoding passes through
// unchanged, so enabling a Normalizer before the terminal switches flags is
// safe. A trailing incomplete CSI sequence is held until the next call; a
// lone trailing ESC is not, because the key router owns its timing.
// A Normalizer is not safe for concurrent use.
type Normalizer struct {
	pending []byte
	paste   bool
	ctrl    bool
}

// CtrlHeld reports whether Ctrl is held on its own: a Ctrl key is down and no
// other key but a top-row digit was pressed since. Only a Ctrl key press sets
// it, so a terminal that reports no modifier key events never shows it held.
func (n *Normalizer) CtrlHeld() bool { return n.ctrl }

// ReleaseCtrl forgets a held Ctrl, for a terminal that lost focus and may
// never report the release.
func (n *Normalizer) ReleaseCtrl() { n.ctrl = false }

// Normalize returns data in the OuterFlags encoding, in a new slice.
func (n *Normalizer) Normalize(data []byte) []byte {
	if len(n.pending) > 0 {
		data = append(n.pending, data...)
		n.pending = nil
	}
	out := make([]byte, 0, len(data))
	for i := 0; i < len(data); {
		if n.paste {
			end := bytes.Index(data[i:], pasteClose)
			if end < 0 {
				keep := len(data) - partialSuffix(data[i:], pasteClose)
				out = append(out, data[i:keep]...)
				n.hold(data[keep:])
				return out
			}
			end += i + len(pasteClose)
			out = append(out, data[i:end]...)
			i = end
			n.paste = false
			continue
		}
		if data[i] != 0x1b {
			out = append(out, data[i])
			i++
			continue
		}
		rest := data[i:]
		if bytes.HasPrefix(rest, pasteOpen) {
			out = append(out, pasteOpen...)
			i += len(pasteOpen)
			n.paste = true
			continue
		}
		params, final, size, partial := scanCSI(rest)
		if partial {
			n.hold(rest)
			return out
		}
		if size > 0 {
			if repl, ok := n.rewrite(params, final); ok {
				out = append(out, repl...)
				i += size
				continue
			}
		}
		out = append(out, data[i])
		i++
	}
	return out
}

func (n *Normalizer) hold(tail []byte) {
	if len(tail) > 0 {
		n.pending = append([]byte(nil), tail...)
	}
}

// partialSuffix returns the length of the longest suffix of data that is a
// strict prefix of marker.
func partialSuffix(data, marker []byte) int {
	for k := min(len(data), len(marker)-1); k > 0; k-- {
		if bytes.HasPrefix(marker, data[len(data)-k:]) {
			return k
		}
	}
	return 0
}

// scanCSI reads ESC [ params final at the start of data, where params holds
// only digits, ';' and ':'. size is 0 when data starts no such sequence;
// partial reports a strict prefix that may still complete.
func scanCSI(data []byte) (params []byte, final byte, size int, partial bool) {
	if len(data) < 2 || data[1] != '[' {
		return nil, 0, 0, false
	}
	for i := 2; i < len(data) && i < maxSeq; i++ {
		b := data[i]
		if (b >= '0' && b <= '9') || b == ';' || b == ':' {
			continue
		}
		return data[2:i], b, i + 1, false
	}
	return nil, 0, 0, len(data) < maxSeq
}

// rewrite returns the OuterFlags form of one key sequence, or false when the
// sequence is not a key event this package owns.
func (n *Normalizer) rewrite(params []byte, final byte) ([]byte, bool) {
	switch final {
	case 'u':
		if len(params) == 0 {
			return nil, false
		}
		key, ok := parseParams(params)
		if !ok {
			return nil, false
		}
		n.track(key.ev, true)
		return encodeOuter(key), true
	case '~', 'A', 'B', 'C', 'D', 'E', 'F', 'H', 'P', 'Q', 'S':
		return n.rewriteFunctional(params, final)
	}
	return nil, false
}

// rewriteFunctional handles CSI number;mods[:event] ~ and CSI 1;mods[:event]
// letter forms. Forms without a modifier field are already in OuterFlags
// form, which also keeps bracketed paste markers and replies untouched.
func (n *Normalizer) rewriteFunctional(params []byte, final byte) ([]byte, bool) {
	fields := splitByte(params, ';')
	if len(fields) != 2 {
		return nil, false
	}
	mods, event, ok := parseMods(fields[1])
	if !ok {
		return nil, false
	}
	n.track(Event{Mods: mods, Release: event == eventRelease}, false)
	if event == eventRelease {
		return nil, true
	}
	out := []byte("\x1b[")
	if mods != 0 || (final == '~' || string(fields[0]) != "1") {
		out = append(out, fields[0]...)
	}
	if mods != 0 {
		out = append(out, ';')
		out = strconv.AppendInt(out, int64(mods+1), 10)
	}
	return append(out, final), true
}

// track updates the Ctrl state from one key event. A Ctrl key event reports
// the Ctrl state after it. Pressing any other key ends a Ctrl held on its own,
// except a top-row digit, the Ctrl+1..9 recent-session switch.
func (n *Normalizer) track(ev Event, unicodeKey bool) {
	switch {
	case unicodeKey && (ev.Code == keyLeftControl || ev.Code == keyRightControl):
		n.ctrl = ev.Mods&ModCtrl != 0
	case unicodeKey && ev.Code == 0:
		// A text-only event carries no key.
	case unicodeKey && isModifierOrLock(ev.Code):
		if ev.Mods&ModCtrl == 0 {
			n.ctrl = false
		}
	case ev.Mods&ModCtrl == 0:
		n.ctrl = false
	case ev.Release:
	case unicodeKey:
		if _, digit := ev.Digit(); !digit {
			n.ctrl = false
		}
	default:
		n.ctrl = false
	}
}

// encodeOuter re-encodes one RequestFlags key event as the terminal would
// have sent it under OuterFlags, or nil when it would have sent nothing.
func encodeOuter(key fullKey) []byte {
	ev := key.ev
	if ev.Release || isModifierOrLock(ev.Code) {
		return nil
	}
	plain := ev.Mods&^ModShift == 0
	switch {
	case ev.Code == 0 || (plain && len(key.text) > 0):
		return appendRunes(nil, key.text)
	case ev.Mods == 0 && (ev.Code == KeyEnter || ev.Code == KeyTab || ev.Code == KeyBackspace):
		return []byte{byte(ev.Code)}
	case plain && isTextKey(ev.Code):
		return legacyText(ev)
	}
	return encodeCSIu(ev, OuterFlags)
}

func isModifierOrLock(code rune) bool {
	return (code >= keyLeftShift && code <= keyISOLevel5) || (code >= keyCapsLock && code <= keyNumLock)
}

// isTextKey reports a key whose unmodified press produces its own text.
func isTextKey(code rune) bool {
	return code >= 0x20 && code != KeyBackspace && (code < 0xe000 || code > 0xf8ff) && utf8.ValidRune(code)
}

func appendRunes(out []byte, runes []rune) []byte {
	for _, r := range runes {
		out = utf8.AppendRune(out, r)
	}
	return out
}

package daemon

import (
	"unicode"
	"unicode/utf8"
)

type overlayEvents struct {
	rune      func(rune)
	backspace func()
	enter     func()
	cancel    func()
	up        func()
	down      func()
}

func routeOverlayBytes(data []byte, pending *[]byte, ev overlayEvents) {
	if pending != nil && len(*pending) > 0 {
		combined := make([]byte, 0, len(*pending)+len(data))
		combined = append(combined, (*pending)...)
		combined = append(combined, data...)
		data = combined
		*pending = nil
	}

	for i := 0; i < len(data); {
		switch data[i] {
		case 0x1b:
			consumed, routed := routeOverlayEscape(data[i:], ev)
			if routed {
				i += consumed
				continue
			}
			if isOverlayEscapePrefix(data[i:]) {
				if pending != nil {
					*pending = append((*pending)[:0], data[i:]...)
				}
				return
			}
			call(ev.cancel)
			i++
		case 0x03:
			call(ev.cancel)
			i++
		case 0x0e:
			call(ev.down)
			i++
		case 0x10:
			call(ev.up)
			i++
		case '\r', '\n':
			call(ev.enter)
			i++
		case 0x7f, 0x08:
			call(ev.backspace)
			i++
		default:
			r, size := utf8.DecodeRune(data[i:])
			if r == utf8.RuneError {
				if !utf8.FullRune(data[i:]) {
					if pending != nil {
						*pending = append((*pending)[:0], data[i:]...)
					}
					return
				}
				i++
				continue
			}
			if !unicode.IsControl(r) && ev.rune != nil {
				ev.rune(r)
			}
			i += size
		}
	}
}

func routeOverlayEscape(data []byte, ev overlayEvents) (int, bool) {
	if len(data) >= 3 && data[1] == '[' {
		switch data[2] {
		case 'A':
			call(ev.up)
			return 3, true
		case 'B':
			call(ev.down)
			return 3, true
		}
	}
	return 0, false
}

func isOverlayEscapePrefix(data []byte) bool {
	return len(data) == 2 && data[0] == 0x1b && data[1] == '['
}

func call(fn func()) {
	if fn != nil {
		fn()
	}
}

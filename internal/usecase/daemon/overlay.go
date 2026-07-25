package daemon

import (
	"unicode"
	"unicode/utf8"
)

type overlayEvents struct {
	rune      func(rune)
	backspace func()
	enter     func()
	tab       func()
	cancel    func()
	up        func()
	down      func()
	left      func()
	right     func()
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
			consumed, incomplete := routeOverlayEscape(data[i:], ev)
			if incomplete {
				if pending != nil {
					*pending = append((*pending)[:0], data[i:]...)
				}
				return
			}
			if consumed > 0 {
				i += consumed
				continue
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
		case 0x09:
			call(ev.tab)
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

func routeOverlayEscape(data []byte, ev overlayEvents) (consumed int, incomplete bool) {
	if len(data) < 2 {
		return 0, false
	}
	switch data[1] {
	case 'O':
		if len(data) < 3 {
			return 0, true
		}
		switch data[2] {
		case 'A':
			call(ev.up)
		case 'B':
			call(ev.down)
		case 'C':
			call(ev.right)
		case 'D':
			call(ev.left)
		}
		return 3, false
	case '[':
		for i := 2; i < len(data); i++ {
			if data[i] < 0x40 || data[i] > 0x7e {
				continue
			}
			if i == 2 {
				switch data[i] {
				case 'A':
					call(ev.up)
				case 'B':
					call(ev.down)
				case 'C':
					call(ev.right)
				case 'D':
					call(ev.left)
				}
			}
			return i + 1, false
		}
		return 0, true
	default:
		return 0, false
	}
}

func call(fn func()) {
	if fn != nil {
		fn()
	}
}

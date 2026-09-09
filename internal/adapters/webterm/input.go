package webterm

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/bnema/vev-vt/html/browser"
	"github.com/bnema/vev/internal/domain"
)

// Handle translates validated browser events into ordinary terminal input.
// Paste never admits embedded terminal framing or C0 commands other than text
// whitespace. The client retains its normal bracketed-paste policy.
func (t *Terminal) Handle(ctx context.Context, event browser.Event) error {
	snapshot := t.Snapshot()
	modes := snapshot.Modes()
	var data string
	switch event.Kind {
	case browser.EventResize:
		e := event.Resize
		return t.Resize(domain.Geometry{Size: domain.Size{Cols: e.Columns, Rows: e.Rows}, PixelWidth: int(e.PixelWidth), PixelHeight: int(e.PixelHeight)})
	case browser.EventText:
		data = cleanText(event.Text.Text, false)
	case browser.EventPaste:
		data = cleanText(event.Paste.Text, true)
		if modes.BracketedPaste {
			data = "\x1b[200~" + data + "\x1b[201~"
		}
	case browser.EventKey:
		data = encodeKey(*event.Key, snapshot.ApplicationCursorMode())
	case browser.EventPointer:
		e := event.Pointer
		if modes.MouseTracking == 0 || !modes.MouseSGR || e.Row >= snapshot.Rows() || e.Column >= snapshot.Columns() {
			return nil
		}
		button := e.Button
		final := "M"
		switch e.Action {
		case "down":
			if button < 0 || button > 2 {
				return nil
			}
		case "up":
			if button < 0 || button > 2 {
				return nil
			}
			final = "m"
		case "move":
			if modes.MouseTracking != 1003 && (modes.MouseTracking != 1002 || e.Buttons == 0) {
				return nil
			}
			button = 3
			if e.Buttons&1 != 0 {
				button = 0
			} else if e.Buttons&4 != 0 {
				button = 1
			} else if e.Buttons&2 != 0 {
				button = 2
			}
			button += 32
		default:
			return nil
		}
		data = fmt.Sprintf("\x1b[<%d;%d;%d%s", button+mouseModifiers(e.Modifiers), e.Column+1, e.Row+1, final)
	case browser.EventWheel:
		e := event.Wheel
		if modes.MouseTracking == 0 || !modes.MouseSGR || e.DeltaY == 0 || e.Row >= snapshot.Rows() || e.Column >= snapshot.Columns() {
			t.resetWheel()
			return nil
		}
		reports, button := t.consumeWheel(e.DeltaY, e.DeltaMode, e.Modifiers.Shift)
		if reports == 0 {
			return nil
		}
		// Shift is consumed as the x10 fast-scroll multiplier, so it must
		// not also leak into the SGR modifiers (buttons 68/69), which
		// applications would not recognize as wheel reports.
		modifiers := e.Modifiers
		if e.Modifiers.Shift {
			modifiers.Shift = false
		}
		var sb strings.Builder
		for i := 0; i < reports; i++ {
			fmt.Fprintf(&sb, "\x1b[<%d;%d;%dM", button+mouseModifiers(modifiers), e.Column+1, e.Row+1)
		}
		data = sb.String()
	case browser.EventFocus:
		// Focus reporting is not negotiated by the current client terminal contract.
		return nil
	}
	return t.Send(ctx, []byte(data))
}

func mouseModifiers(m browser.Modifiers) int {
	n := 0
	if m.Shift {
		n += 4
	}
	if m.Alt {
		n += 8
	}
	if m.Ctrl {
		n += 16
	}
	return n
}

func cleanText(text string, whitespace bool) string {
	return strings.Map(func(r rune) rune {
		if whitespace && (r == '\n' || r == '\r' || r == '\t') {
			return r
		}
		if r < 32 || r >= 127 && r <= 159 {
			return -1
		}
		return r
	}, text)
}

func encodeKey(e browser.KeyEvent, applicationCursor bool) string {
	if e.Modifiers.Meta {
		return ""
	}
	modifier := 1
	if e.Modifiers.Shift {
		modifier += 1
	}
	if e.Modifiers.Alt {
		modifier += 2
	}
	if e.Modifiers.Ctrl {
		modifier += 4
	}
	var final string
	switch e.Key {
	case "ArrowUp":
		final = "A"
	case "ArrowDown":
		final = "B"
	case "ArrowRight":
		final = "C"
	case "ArrowLeft":
		final = "D"
	case "Home":
		final = "H"
	case "End":
		final = "F"
	}
	if final != "" {
		if modifier > 1 {
			return fmt.Sprintf("\x1b[1;%d%s", modifier, final)
		}
		if applicationCursor {
			return "\x1bO" + final
		}
		return "\x1b[" + final
	}
	number := 0
	switch e.Key {
	case "Insert":
		number = 2
	case "Delete":
		number = 3
	case "PageUp":
		number = 5
	case "PageDown":
		number = 6
	case "F5":
		number = 15
	case "F6":
		number = 17
	case "F7":
		number = 18
	case "F8":
		number = 19
	case "F9":
		number = 20
	case "F10":
		number = 21
	case "F11":
		number = 23
	case "F12":
		number = 24
	}
	if number != 0 {
		if modifier > 1 {
			return fmt.Sprintf("\x1b[%d;%d~", number, modifier)
		}
		return fmt.Sprintf("\x1b[%d~", number)
	}
	if len(e.Key) == 2 && e.Key[0] == 'F' && e.Key[1] >= '1' && e.Key[1] <= '4' {
		final := string(rune('P' + e.Key[1] - '1'))
		if modifier > 1 {
			return fmt.Sprintf("\x1b[1;%d%s", modifier, final)
		}
		return "\x1bO" + final
	}
	var key string
	switch e.Key {
	case "Enter":
		key = "\r"
	case "Escape":
		key = "\x1b"
	case "Backspace":
		key = "\x7f"
	case "Tab":
		if e.Modifiers.Shift {
			return "\x1b[Z"
		}
		key = "\t"
	default:
		if utf8.RuneCountInString(e.Key) != 1 {
			return ""
		}
		key = cleanText(e.Key, false)
		if e.Modifiers.Ctrl {
			upper := strings.ToUpper(key)
			if upper == " " || upper == "@" {
				key = "\x00"
			} else if len(upper) == 1 && upper[0] >= 'A' && upper[0] <= '_' {
				key = string([]byte{upper[0] & 31})
			} else {
				return ""
			}
		}
	}
	if e.Modifiers.Alt {
		key = "\x1b" + key
	}
	return key
}

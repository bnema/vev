package daemon

import (
	"unicode"
	"unicode/utf8"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/keys"
)

// handlePickerSearchInputLocked decodes picker search input while pickerMu is
// held. Printable bytes belong to the query; navigation is intentionally
// limited to arrows and Ctrl-N/Ctrl-P so names containing j/k/q/x/s remain
// searchable. Incomplete UTF-8 and escape sequences are retained across input
// chunks in the attachment-owned picker pending buffer.
func (d *Daemon) handlePickerSearchInputLocked(ac *attachedClient, data []byte) listInputResult {
	rt := ac.overlays
	if len(rt.pickerPending) > 0 {
		rt.pickerESC.stop()
		combined := make([]byte, 0, len(rt.pickerPending)+len(data))
		combined = append(combined, rt.pickerPending...)
		combined = append(combined, data...)
		data = combined
		rt.pickerPending = nil
	}

	var result listInputResult
	for offset := 0; offset < len(data); {
		switch data[offset] {
		case keys.ESC:
			tail := data[offset:]
			if consumed, move := routeListEscape(tail); consumed > 0 {
				switch move {
				case 'A':
					rt.picker.Up()
					result.changed = true
				case 'B':
					rt.picker.Down()
					result.changed = true
				}
				offset += consumed
				continue
			}
			if len(tail) == 1 {
				d.retainPickerSearchESCLocked(ac)
				return result
			}
			if isListEscapePrefix(tail) {
				rt.pickerPending = append(rt.pickerPending[:0], tail...)
				return result
			}
			offset++
		case 0x03: // Ctrl-C
			result.exit = true
			return result
		case 0x0e: // Ctrl-N
			rt.picker.Down()
			result.changed = true
			offset++
		case 0x10: // Ctrl-P
			rt.picker.Up()
			result.changed = true
			offset++
		case '\r', '\n':
			result.action = data[offset]
			result.exit = true
			return result
		case 0x08, 0x7f:
			before := rt.picker.Query()
			rt.picker.BackspaceSearch()
			result.changed = result.changed || before != rt.picker.Query()
			offset++
		default:
			if !utf8.FullRune(data[offset:]) {
				rt.pickerPending = append(rt.pickerPending[:0], data[offset:]...)
				return result
			}
			r, width := utf8.DecodeRune(data[offset:])
			if r == utf8.RuneError && width == 1 {
				offset++
				continue
			}
			if unicode.IsPrint(r) {
				rt.picker.InsertSearch(r)
				result.changed = true
			}
			offset += width
		}
	}
	return result
}

func (d *Daemon) retainPickerSearchESCLocked(ac *attachedClient) {
	rt := ac.overlays
	rt.pickerPending = append(rt.pickerPending[:0], keys.ESC)
	rt.pickerESC.retain(d.clock, keys.ESCDelay, func(timer ports.Timer) {
		rt.pickerMu.Lock()
		if rt.pickerESC.timer != timer || len(rt.pickerPending) != 1 || rt.pickerPending[0] != keys.ESC || rt.picker == nil || !rt.picker.SearchActive() {
			rt.pickerMu.Unlock()
			return
		}
		rt.pickerPending = nil
		rt.pickerESC.timer = nil
		rt.pickerESC.done = nil
		if rt.picker.Query() != "" {
			rt.picker.ClearSearch()
		} else {
			rt.picker.ExitSearch()
		}
		rt.pickerMu.Unlock()
		if sess := ac.currentAttachmentSession(); sess != nil {
			d.registerPreviewForSelection(ac)
			d.invalidateRender(sess, ac, true, "picker search escape")
		}
	})
}

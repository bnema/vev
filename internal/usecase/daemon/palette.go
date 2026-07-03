package daemon

import (
	"unicode"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/bnema/vev/internal/usecase/palette"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
)

var paletteModal = ui.Modal{WidthPct: 100, MinWidth: 32, FixedHeight: 11, Title: " Commands ", Anchor: ui.AnchorBottom, BottomMargin: 1}

func (d *Daemon) enterPalette(sess *session, ac *attachedClient) {
	d.closePalette(ac)
	ac.paletteMu.Lock()
	ac.palette = palette.NewRegistry()
	ac.palettePending = nil
	ac.paletteMu.Unlock()
	d.paint(sess, ac, true)
}

func (d *Daemon) closePalette(ac *attachedClient) {
	ac.paletteMu.Lock()
	ac.palette = nil
	ac.palettePending = nil
	ac.paletteMu.Unlock()
}

func (d *Daemon) handlePaletteInput(ac *attachedClient, data []byte) {
	sess := ac.currentSession()
	if sess == nil {
		return
	}

	ac.paletteMu.Lock()
	if ac.palette == nil {
		ac.palettePending = nil
		ac.paletteMu.Unlock()
		return
	}
	if len(ac.palettePending) > 0 {
		combined := make([]byte, 0, len(ac.palettePending)+len(data))
		combined = append(combined, ac.palettePending...)
		combined = append(combined, data...)
		data = combined
		ac.palettePending = nil
	}

	changed := false
	exit := false
	run := false
	var cmd command.Command
	var ok bool

	for i := 0; i < len(data); {
		switch data[i] {
		case 0x1b:
			consumed, routed := routePaletteEscape(ac.palette, data[i:])
			if routed {
				i += consumed
				changed = true
				continue
			}
			if isPaletteEscapePrefix(data[i:]) {
				ac.palettePending = append(ac.palettePending[:0], data[i:]...)
				i = len(data)
				continue
			}
			exit = true
			i++
		case 0x03:
			exit = true
			i++
		case 0x0e:
			ac.palette.Down()
			changed = true
			i++
		case 0x10:
			ac.palette.Up()
			changed = true
			i++
		case '\r', '\n':
			cmd, ok = ac.palette.Selected()
			if ok {
				run = true
				exit = true
			}
			i++
		case 0x7f, 0x08:
			ac.palette.Backspace()
			changed = true
			i++
		default:
			r, size := utf8.DecodeRune(data[i:])
			if r == utf8.RuneError {
				if !utf8.FullRune(data[i:]) {
					ac.palettePending = append(ac.palettePending[:0], data[i:]...)
					i = len(data)
					continue
				}
				i++
				continue
			}
			if !unicode.IsControl(r) {
				ac.palette.Insert(r)
				changed = true
			}
			i += size
		}
	}
	if exit {
		ac.palette = nil
		ac.palettePending = nil
	}
	ac.paletteMu.Unlock()

	if run && ok {
		if err := cmd.Run(paletteExec{d: d, sess: sess, ac: ac}); err != nil {
			d.log.Error("palette command failed", "err", err, "session", sess.name, "command", cmd.Code)
		}
		return
	}
	if exit || changed {
		d.paint(sess, ac, true)
	}
}

func routePaletteEscape(m *palette.Model, data []byte) (int, bool) {
	if len(data) >= 3 && data[1] == '[' {
		switch data[2] {
		case 'A':
			m.Up()
			return 3, true
		case 'B':
			m.Down()
			return 3, true
		}
	}
	return 0, false
}

func isPaletteEscapePrefix(data []byte) bool {
	return len(data) == 2 && data[0] == 0x1b && data[1] == '['
}

type paletteExec struct {
	d    *Daemon
	sess *session
	ac   *attachedClient
}

func (e paletteExec) CreateTab() error {
	if err := e.d.createTab(e.sess, e.ac.size); err != nil {
		return err
	}
	e.d.paint(e.sess, e.ac, true)
	return nil
}

func (e paletteExec) CloseTab() error {
	if tb := e.sess.activeTab(); tb != nil {
		e.d.closeTab(e.sess, tb, true)
	}
	return nil
}

func (e paletteExec) NextTab() error {
	if e.sess.switchRelative(1) {
		e.d.paint(e.sess, e.ac, true)
	}
	return nil
}

func (e paletteExec) PrevTab() error {
	if e.sess.switchRelative(-1) {
		e.d.paint(e.sess, e.ac, true)
	}
	return nil
}

func (e paletteExec) Detach() error {
	e.d.clientGone(e.sess, e.ac, true)
	return nil
}

func (e paletteExec) EnterCopyMode() error {
	e.d.enterCopyMode(e.sess, e.ac)
	return nil
}

func (e paletteExec) RenameSession() error {
	e.sess.promoteEphemeral()
	e.d.paint(e.sess, e.ac, true)
	return nil
}

func (e paletteExec) OpenSessionPicker() error {
	e.d.enterPicker(e.sess, e.ac)
	return nil
}

func composePaletteClientFrame(model *palette.Model, base renderer.Frame) (renderer.Frame, []renderer.Damage) {
	inner := paletteModal.Composite(base, renderer.DefaultStyle())
	modalFrame := model.Render(domain.Size{Cols: inner.Width, Rows: inner.Height})
	for y := range min(inner.Height, modalFrame.Height) {
		for x := range min(inner.Width, modalFrame.Width) {
			base.Set(inner.X+x, inner.Y+y, modalFrame.At(x, y))
		}
	}
	return base, []renderer.Damage{renderer.FullRedraw()}
}

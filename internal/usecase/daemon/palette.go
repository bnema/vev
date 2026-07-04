package daemon

import (
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
	changed := false
	exit := false
	run := false
	var cmd command.Command
	var ok bool

	routeOverlayBytes(data, &ac.palettePending, overlayEvents{
		rune: func(r rune) {
			ac.palette.Insert(r)
			changed = true
		},
		backspace: func() {
			ac.palette.Backspace()
			changed = true
		},
		enter: func() {
			cmd, ok = ac.palette.Selected()
			if ok {
				run = true
				exit = true
			}
		},
		cancel: func() { exit = true },
		up: func() {
			ac.palette.Up()
			changed = true
		},
		down: func() {
			ac.palette.Down()
			changed = true
		},
	})
	if exit {
		ac.palette = nil
		ac.palettePending = nil
	}
	ac.paletteMu.Unlock()

	if run && ok {
		if err := cmd.Run(paletteExec{d: d, sess: sess, ac: ac}); err != nil {
			sess.mu.Lock()
			name := sess.name
			sess.mu.Unlock()
			d.log.Error("palette command failed", "err", err, "session", name, "command", cmd.Code)
		}
		return
	}
	if exit || changed {
		d.paint(sess, ac, true)
	}
}

type paletteExec struct {
	d    *Daemon
	sess *session
	ac   *attachedClient
}

func (e paletteExec) CreateTab() error {
	defer e.d.paint(e.sess, e.ac, true)
	return e.d.createTab(e.sess, e.ac.size)
}

func (e paletteExec) CreateSession() error {
	e.d.enterPrompt(e.sess, e.ac, " Create session ", "", func(name string) error {
		return e.d.createSessionAndSwitch(e.sess, e.ac, name)
	})
	return nil
}

func (e paletteExec) CloseTab() error {
	if tb := e.sess.activeTab(); tb != nil {
		e.d.closeTab(e.sess, tb, true)
	}
	return nil
}

func (e paletteExec) NextTab() error {
	e.sess.switchRelative(1)
	e.d.paint(e.sess, e.ac, true)
	return nil
}

func (e paletteExec) PrevTab() error {
	e.sess.switchRelative(-1)
	e.d.paint(e.sess, e.ac, true)
	return nil
}

func (e paletteExec) Detach() error {
	e.d.clientGone(e.sess, e.ac, true)
	return nil
}

func (e paletteExec) EnterVisualMode() error {
	e.d.enterCopyMode(e.sess, e.ac)
	return nil
}

func (e paletteExec) RenameSession() error {
	e.sess.mu.Lock()
	currentName := e.sess.name
	e.sess.mu.Unlock()
	e.d.enterPrompt(e.sess, e.ac, " Rename session ", currentName, func(name string) error {
		return e.d.renameSession(e.sess, name)
	})
	return nil
}

func (e paletteExec) RenameTab() error {
	tb := e.sess.activeTab()
	if tb == nil {
		return nil
	}
	e.sess.mu.Lock()
	currentName := tb.name
	e.sess.mu.Unlock()
	e.d.enterPrompt(e.sess, e.ac, " Rename tab ", currentName, func(name string) error {
		return e.d.renameTab(e.sess, tb, name)
	})
	return nil
}

func (e paletteExec) OpenSessionPicker() error {
	e.d.enterPicker(e.sess, e.ac)
	return nil
}

func composePaletteClientFrame(model *palette.Model, base renderer.Frame, styles ...themeStyles) (renderer.Frame, []renderer.Damage) {
	styleSet := resolveThemeStyles(styles)
	inner := paletteModal.Composite(base, styleSet.border)
	modalFrame := model.Render(domain.Size{Cols: inner.Width, Rows: inner.Height}, styleSet.selection)
	for y := range min(inner.Height, modalFrame.Height) {
		for x := range min(inner.Width, modalFrame.Width) {
			base.Set(inner.X+x, inner.Y+y, modalFrame.At(x, y))
		}
	}
	return base, []renderer.Damage{renderer.FullRedraw()}
}

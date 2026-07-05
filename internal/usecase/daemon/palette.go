package daemon

import (
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/palette"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
)

var paletteModal = ui.Modal{WidthPct: 100, MinWidth: 32, FixedHeight: 11, Title: " Commands ", Anchor: ui.AnchorBottom, BottomMargin: 1}

func (d *Daemon) enterPalette(sess *session, ac *attachedClient) {
	d.closePalette(ac)
	ac.overlays.paletteMu.Lock()
	ac.overlays.palette = palette.New(d.paletteCommands())
	ac.overlays.palettePending = nil
	ac.overlays.paletteMu.Unlock()
	d.paint(sess, ac, true)
}

func (d *Daemon) closePalette(ac *attachedClient) {
	ac.overlays.paletteMu.Lock()
	ac.overlays.palette = nil
	ac.overlays.palettePending = nil
	ac.overlays.paletteMu.Unlock()
}

func (d *Daemon) paletteCommands() []command.Command {
	commands := command.Registry()
	d.paletteRecentMu.Lock()
	recent := append([]string(nil), d.paletteRecent...)
	d.paletteRecentMu.Unlock()

	byCode := make(map[string]command.Command, len(commands))
	for _, cmd := range commands {
		byCode[cmd.Code] = cmd
	}
	out := make([]command.Command, 0, len(commands))
	used := make(map[string]bool, len(recent))
	for _, code := range recent {
		cmd, ok := byCode[code]
		if !ok || used[code] {
			continue
		}
		out = append(out, cmd)
		used[code] = true
	}
	for _, cmd := range commands {
		if !used[cmd.Code] {
			out = append(out, cmd)
		}
	}
	return out
}

func (d *Daemon) recordPaletteUse(code string) {
	d.paletteRecentMu.Lock()
	defer d.paletteRecentMu.Unlock()

	recent := make([]string, 0, len(d.paletteRecent)+1)
	recent = append(recent, code)
	for _, existing := range d.paletteRecent {
		if existing != code {
			recent = append(recent, existing)
		}
	}
	d.paletteRecent = recent
}

func (d *Daemon) handlePaletteInput(ac *attachedClient, data []byte) {
	sess := ac.currentSession()
	if sess == nil {
		return
	}

	ac.overlays.paletteMu.Lock()
	if ac.overlays.palette == nil {
		ac.overlays.palettePending = nil
		ac.overlays.paletteMu.Unlock()
		return
	}
	changed := false
	exit := false
	run := false
	var cmd command.Command
	var ok bool

	routeOverlayBytes(data, &ac.overlays.palettePending, overlayEvents{
		rune: func(r rune) {
			ac.overlays.palette.Insert(r)
			changed = true
		},
		backspace: func() {
			ac.overlays.palette.Backspace()
			changed = true
		},
		enter: func() {
			cmd, ok = ac.overlays.palette.Selected()
			if ok {
				run = true
				exit = true
			}
		},
		cancel: func() { exit = true },
		up: func() {
			ac.overlays.palette.Up()
			changed = true
		},
		down: func() {
			ac.overlays.palette.Down()
			changed = true
		},
	})
	if exit {
		ac.overlays.palette = nil
		ac.overlays.palettePending = nil
	}
	ac.overlays.paletteMu.Unlock()

	if run && ok {
		if err := cmd.Run(paletteExec{d: d, sess: sess, ac: ac}); err != nil {
			sess.mu.Lock()
			name := sess.name
			sess.mu.Unlock()
			d.log.Error("palette command failed", "err", err, "session", name, "command", cmd.Code)
		} else {
			d.recordPaletteUse(cmd.Code)
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

func (e paletteExec) SplitRight() error {
	return e.d.splitPane(e.sess, e.ac, layout.Right)
}

func (e paletteExec) SplitLeft() error {
	return e.d.splitPane(e.sess, e.ac, layout.Left)
}

func (e paletteExec) SplitUp() error {
	return e.d.splitPane(e.sess, e.ac, layout.Up)
}

func (e paletteExec) SplitDown() error {
	return e.d.splitPane(e.sess, e.ac, layout.Down)
}

func (e paletteExec) StackPane() error {
	return e.d.stackPane(e.sess, e.ac)
}

func (e paletteExec) ToggleStack() error {
	return e.d.toggleStack(e.sess, e.ac)
}

func (e paletteExec) ClosePane() error {
	return e.d.closeFocusedPane(e.sess, e.ac)
}

func (e paletteExec) FocusPaneLeft() error {
	return e.d.focusDir(e.sess, e.ac, layout.Left)
}

func (e paletteExec) FocusPaneRight() error {
	return e.d.focusDir(e.sess, e.ac, layout.Right)
}

func (e paletteExec) FocusPaneUp() error {
	return e.d.focusDir(e.sess, e.ac, layout.Up)
}

func (e paletteExec) FocusPaneDown() error {
	return e.d.focusDir(e.sess, e.ac, layout.Down)
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
	e.d.clientGone(e.sess, e.ac, e.ac.transport(), true)
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

func (e paletteExec) OpenSessionPicker() error {
	e.d.enterPicker(e.sess, e.ac)
	return nil
}

func composePaletteClientFrame(model *palette.Model, base renderer.Frame, styles ...themeStyles) (renderer.Frame, []renderer.Damage) {
	styleSet := resolveThemeStyles(styles)
	return composeModalClientFrame(base, paletteModal, styleSet, styleSet.selection, model.Render)
}

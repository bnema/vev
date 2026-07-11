package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/palette"
	"github.com/bnema/vev/internal/usecase/picker"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
)

const (
	paletteRailBreakpoint = 96
	paletteRailWidth      = 64
)

var paletteModal = ui.Modal{WidthPct: 100, MinWidth: 32, FixedHeight: 11, Title: " Commands ", Anchor: domain.AnchorBottom, Margins: ui.Margins{Top: 1, Right: 1, Bottom: 1, Left: 1}}

func paletteModalFor(size domain.Size, cfg domain.PaletteConfig) ui.Modal {
	modal := paletteModal
	if size.Cols >= paletteRailBreakpoint {
		modal.FixedWidth = paletteRailWidth
	}
	if cfg.AnchorSet {
		modal.Anchor = cfg.Anchor
	} else if size.Cols >= paletteRailBreakpoint {
		modal.Anchor = domain.AnchorBottomRight
	}
	return modal
}

func (d *Daemon) enterPalette(sess *session, ac *attachedClient) {
	// Capture daemon/session state before taking paletteMu: lock ordering forbids
	// holding an overlay lock while inspecting live sessions.
	recent := d.recentSessions(sess)
	commands := d.paletteCommands()
	ac.overlays.paletteMu.Lock()
	ac.overlays.paletteGeneration++
	ac.overlays.palette = palette.New(commands)
	ac.overlays.paletteRecent = recent
	ac.overlays.paletteHints = palette.ContextualHints{}
	ac.overlays.paletteFeedback = ""
	ac.overlays.palettePending = nil
	ac.overlays.paletteMu.Unlock()
	d.paint(sess, ac, true)
}

func recentSessionHints(recent []recentSession, args []string) palette.ContextualHints {
	names := make([]string, len(recent))
	for i, entry := range recent {
		names[i] = entry.name
	}
	return palette.BuildRecentSessionHints(names, args)
}

func (d *Daemon) paletteCommands() []command.Command {
	commands := command.Registry()
	d.paletteRecentMu.Lock()
	recent := append([]string(nil), d.paletteRecent...)
	d.paletteRecentMu.Unlock()
	overrides := d.codeOverrideSnapshot()

	byCode := make(map[string]command.Command, len(commands))
	for _, cmd := range commands {
		cmd = commandWithOverrides(cmd, overrides)
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
		cmd = commandWithOverrides(cmd, overrides)
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
	var cmd command.Command
	var args []string
	var recent []recentSession
	var generation uint64
	var rawQuery string
	changed, cancel, execute := false, false, false

	ac.overlays.paletteMu.Lock()
	if ac.overlays.palette == nil {
		ac.overlays.palettePending = nil
		ac.overlays.paletteMu.Unlock()
		return
	}
	routeOverlayBytes(data, &ac.overlays.palettePending, overlayEvents{
		rune: func(r rune) {
			ac.overlays.palette.Insert(r)
			ac.overlays.paletteFeedback = ""
			changed = true
		},
		backspace: func() {
			ac.overlays.palette.Backspace()
			ac.overlays.paletteFeedback = ""
			changed = true
		},
		up:     func() { ac.overlays.palette.Up(); changed = true },
		down:   func() { ac.overlays.palette.Down(); changed = true },
		cancel: func() { cancel = true },
		enter: func() {
			var selected bool
			cmd, selected = ac.overlays.palette.Selected()
			if !selected {
				ac.overlays.paletteFeedback = "invalid command arguments"
				changed = true
				return
			}
			rawQuery = ac.overlays.palette.Query()
			// JRS is contextual: it is executable only from its exact token so
			// fuzzy selection can never turn an unrelated query into a jump.
			// Static commands retain normal palette behavior and run the selected
			// fuzzy match.
			if cmd.Code == "JRS" {
				action, valid := palette.ParseAction([]command.Command{cmd}, rawQuery)
				if !valid {
					ac.overlays.paletteFeedback = "invalid command arguments"
					changed = true
					return
				}
				args = action.Args
			}
			recent = append([]recentSession(nil), ac.overlays.paletteRecent...)
			generation = ac.overlays.paletteGeneration
			execute = true
		},
	})
	if changed {
		active, ok := ac.overlays.palette.ArgumentCommand()
		if ok && active.Code == "JRS" {
			hints := recentSessionHints(ac.overlays.paletteRecent, paletteArgs(ac.overlays.palette.Query(), active))
			ac.overlays.paletteHints = hints
		} else {
			ac.overlays.paletteHints = palette.ContextualHints{}
		}
	}
	if cancel {
		d.clearPaletteLocked(ac)
	}
	ac.overlays.paletteMu.Unlock()
	if cancel {
		d.paint(sess, ac, true)
		return
	}
	if !execute {
		if changed {
			d.paint(sess, ac, true)
		}
		return
	}

	// Revalidate live target without paletteMu. A captured ID is never replaced
	// by a newly-ranked session, so MRU changes cannot shift a requested rank.
	if cmd.Code == "JRS" {
		rank, err := command.ParsePositiveDecimal(args)
		if err != nil {
			d.paletteFailure(ac, generation, rawQuery, "requested recent session is unavailable")
			d.paint(sess, ac, true)
			return
		}
		// The captured ID is handed off atomically. A target can disappear after
		// the validation above, so closing is contingent on the committed switch.
		exec := paletteExec{d: d, sess: sess, ac: ac, recent: recent}
		if err := exec.JumpRecentSession(rank); err != nil {
			d.paletteFailure(ac, generation, rawQuery, "requested recent session is unavailable")
			d.paint(sess, ac, true)
			return
		}
		// Publication remains generation-safe; do not let stale work close a
		// newer palette interaction.
		if d.closeExecutedPalette(ac, generation, rawQuery) {
			d.recordPaletteUse(cmd.Code)
			d.paint(ac.currentSession(), ac, true)
		}
		return
	}
	if !d.closeExecutedPalette(ac, generation, rawQuery) {
		return
	}
	if err := cmd.Run(paletteExec{d: d, sess: sess, ac: ac, recent: recent}, args); err != nil {
		d.log.Error("palette command failed", "err", err, "command", cmd.Code)
	} else {
		d.recordPaletteUse(cmd.Code)
	}
}

func paletteArgs(query string, cmd command.Command) []string {
	action, ok := palette.ParseAction([]command.Command{cmd}, query)
	if !ok {
		return nil
	}
	return action.Args
}

func (d *Daemon) clearPaletteLocked(ac *attachedClient) {
	ac.overlays.paletteGeneration++
	ac.overlays.palette = nil
	ac.overlays.paletteRecent = nil
	ac.overlays.paletteHints = palette.ContextualHints{}
	ac.overlays.paletteFeedback = ""
	ac.overlays.palettePending = nil
}

func (d *Daemon) paletteFailure(ac *attachedClient, generation uint64, rawQuery, feedback string) {
	ac.overlays.paletteMu.Lock()
	defer ac.overlays.paletteMu.Unlock()
	if ac.overlays.palette == nil || ac.overlays.paletteGeneration != generation || ac.overlays.palette.Query() != rawQuery {
		return
	}
	ac.overlays.paletteFeedback = feedback
	// Rendering consumes the immutable contextual hint snapshot, not Model's
	// feedback state. Preserve ranks while making stale-target feedback visible.
	if ac.overlays.paletteHints.Kind == command.ContextHintRecentSessions {
		ac.overlays.paletteHints.Feedback = feedback
	}
}
func (d *Daemon) closeExecutedPalette(ac *attachedClient, generation uint64, rawQuery string) bool {
	ac.overlays.paletteMu.Lock()
	defer ac.overlays.paletteMu.Unlock()
	if ac.overlays.palette == nil || ac.overlays.paletteGeneration != generation || ac.overlays.palette.Query() != rawQuery {
		return false
	}
	d.clearPaletteLocked(ac)
	return true
}

type paletteExec struct {
	d      *Daemon
	sess   *session
	ac     *attachedClient
	recent []recentSession
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
	if e.sess.switchRelative(1) {
		e.d.activateTab(e.sess, e.sess.activeTab())
	}
	e.d.paint(e.sess, e.ac, true)
	return nil
}

func (e paletteExec) PrevTab() error {
	if e.sess.switchRelative(-1) {
		e.d.activateTab(e.sess, e.sess.activeTab())
	}
	e.d.paint(e.sess, e.ac, true)
	return nil
}

func (e paletteExec) ToggleFloatingPane() error {
	return e.d.toggleFloating(e.sess, e.ac)
}

func (e paletteExec) BackSession() error {
	e.d.backSession(e.sess, e.ac)
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

func (e paletteExec) RenameTab() error {
	tb := e.sess.activeTab()
	if tb == nil {
		return nil
	}
	e.sess.mu.Lock()
	currentName := tabDisplayName(tb, e.sess.active)
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

func (e paletteExec) JumpRecentSession(rank int) error {
	if rank < 1 || rank > len(e.recent) {
		return command.ErrInvalidArguments
	}
	target := e.recent[rank-1]
	if e.d.sessionByID(target.id) == nil {
		return command.ErrInvalidArguments
	}
	if e.d.beforeRecentSessionHandoff != nil {
		e.d.beforeRecentSessionHandoff()
	}
	if !e.d.switchToTarget(e.sess, e.ac, picker.Target{Session: target.id, TabIndex: -1}) {
		return command.ErrInvalidArguments
	}
	return nil
}

func composePaletteClientFrame(model *palette.Model, base renderer.Frame, cfg domain.PaletteConfig, guidance string, styles ...themeStyles) (renderer.Frame, []renderer.Damage) {
	styleSet := resolveThemeStyles(styles)
	modal := paletteModalFor(domain.Size{Cols: base.Width, Rows: base.Height}, cfg)
	return composeModalClientFrame(base, modal, styleSet, styleSet.selection, func(size domain.Size, _ ...renderer.Style) renderer.Frame {
		return model.Render(size, palette.RenderOptions{Styles: palette.RenderStyles{Selection: styleSet.selection, Description: styleSet.paletteDesc}, Guidance: guidance})
	})
}

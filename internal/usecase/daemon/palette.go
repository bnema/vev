package daemon

import (
	"sort"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/palette"
	"github.com/bnema/vev/internal/usecase/picker"
	themeui "github.com/bnema/vev/internal/usecase/theme"
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
	results := d.paletteResults(sess, commands)
	ac.overlays.paletteMu.Lock()
	ac.overlays.paletteGeneration++
	ac.overlays.palette = palette.New(results)
	ac.overlays.paletteRecent = recent
	ac.overlays.paletteHints = palette.ContextualHints{}
	ac.overlays.paletteFeedback = ""
	ac.overlays.palettePending = nil
	ac.overlays.paletteMu.Unlock()
	d.invalidateRender(sess, ac, true, "palette.go")
}

// paletteResults captures eligible named sessions before paletteMu so the
// palette has an immutable lifecycle target without violating lock ordering.
func (d *Daemon) paletteResults(current *session, commands []command.Command) []palette.Result {
	d.mu.Lock()
	sessions := d.sessionsSnapshotLocked()
	stopped := make([]stoppedSession, 0, len(d.stopped))
	for _, entry := range d.stopped {
		if !entry.purging {
			stopped = append(stopped, entry)
		}
	}
	d.mu.Unlock()

	results := make([]palette.Result, 0, len(commands)+len(sessions)+len(stopped))
	for _, cmd := range commands {
		results = append(results, palette.NewCommandResult(cmd))
	}
	active := make([]palette.Result, 0, len(sessions))
	for _, candidate := range sessions {
		if candidate == current {
			continue
		}
		candidate.mu.Lock()
		name, createdAt, ephemeral := candidate.name, candidate.createdAt, candidate.ephemeral
		id := candidate.id
		candidate.mu.Unlock()
		if name != "" && !ephemeral {
			active = append(active, palette.NewActiveSessionResult(name, time.Unix(0, createdAt), id))
		}
	}
	sort.Slice(active, func(i, j int) bool {
		left, _ := active[i].SessionName()
		right, _ := active[j].SessionName()
		return left < right
	})
	sort.Slice(stopped, func(i, j int) bool { return stopped[i].name < stopped[j].name })
	results = append(results, active...)
	for _, candidate := range stopped {
		results = append(results, palette.NewStoppedSessionResult(candidate.name, time.Unix(0, candidate.createdAt)))
	}
	return results
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
	var sessionTarget palette.Result
	var hasSessionTarget bool
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
			if !execute {
				ac.overlays.palette.Insert(r)
				changed = true
			}
		},
		backspace: func() {
			if !execute {
				ac.overlays.palette.Backspace()
				changed = true
			}
		},
		tab: func() {
			if !execute {
				changed = ac.overlays.palette.CompleteSelected() || changed
			}
		},
		up: func() {
			if !execute {
				ac.overlays.palette.Up()
				changed = true
			}
		},
		down: func() {
			if !execute {
				ac.overlays.palette.Down()
				changed = true
			}
		},
		cancel: func() {
			if !execute {
				cancel = true
			}
		},
		enter: func() {
			if execute {
				return
			}
			selected, ok := ac.overlays.palette.Selected()
			if !ok {
				changed = true
				return
			}
			rawQuery = ac.overlays.palette.Query()
			if selectedCommand, ok := selected.Command(); ok {
				cmd = selectedCommand
				// JRS is contextual: fuzzy selection cannot turn an unrelated
				// query into a jump.
				if cmd.Slug == "jump-recent-session" {
					action, valid := palette.ParseAction([]palette.Result{selected}, rawQuery)
					if !valid {
						changed = true
						return
					}
					args = action.Args
					recent = append([]recentSession(nil), ac.overlays.paletteRecent...)
				}
			} else if _, ok := selected.SessionName(); ok {
				sessionTarget = selected
				hasSessionTarget = true
			} else {
				changed = true
				return
			}
			generation = ac.overlays.paletteGeneration
			execute = true
		},
	})
	if changed {
		ac.overlays.paletteFeedback = ""
		active, ok := ac.overlays.palette.ArgumentCommand()
		if ok && active.Slug == "jump-recent-session" {
			ac.overlays.paletteHints = recentSessionHints(ac.overlays.paletteRecent, paletteArgs(ac.overlays.palette.Query(), active))
		} else {
			ac.overlays.paletteHints = palette.ContextualHints{}
		}
	}
	if cancel {
		ac.clearPaletteLocked()
	}
	ac.overlays.paletteMu.Unlock()
	if cancel {
		d.invalidateRender(sess, ac, true, "palette.go")
		return
	}
	if !execute {
		if changed {
			d.invalidateRender(sess, ac, true, "palette.go")
		}
		return
	}

	if hasSessionTarget {
		createdAt, _ := sessionTarget.SessionCreatedAt()
		expectedCreatedAt := createdAt.UnixNano()
		name, _ := sessionTarget.SessionName()
		target := picker.Target{Name: name, TabIndex: -1, ExpectedCreatedAt: &expectedCreatedAt}
		if id, active := sessionTarget.SessionID(); active {
			target.Session = id
		} else {
			target.Stopped = true
		}
		if !d.switchToTarget(sess, ac, target) {
			ac.paletteFailure(generation, rawQuery, "requested session is unavailable")
			d.invalidateRender(sess, ac, true, "palette.go")
			return
		}
		if ac.closeExecutedPalette(generation, rawQuery) {
			d.invalidateRender(ac.currentSession(), ac, true, "palette.go")
		}
		return
	}

	if cmd.Slug == "jump-recent-session" {
		rank, err := command.ParsePositiveDecimal(args)
		if err != nil {
			ac.paletteFailure(generation, rawQuery, "rank must be one positive decimal")
			d.invalidateRender(sess, ac, true, "palette.go")
			return
		}
		exec := paletteExec{d: d, sess: sess, ac: ac, recent: recent}
		if err := exec.JumpRecentSession(rank); err != nil {
			ac.paletteFailure(generation, rawQuery, "requested recent session is unavailable")
			d.invalidateRender(sess, ac, true, "palette.go")
			return
		}
		if ac.closeExecutedPalette(generation, rawQuery) {
			d.recordPaletteUse(cmd.Code)
			d.invalidateRender(ac.currentSession(), ac, true, "palette.go")
		}
		return
	}
	if !ac.closeExecutedPalette(generation, rawQuery) {
		return
	}
	if err := cmd.Run(paletteExec{d: d, sess: sess, ac: ac}, args); err != nil {
		d.log.Error("palette command failed", "err", err, "command", cmd.Code)
		d.reportError(sess, err)
	} else {
		d.recordPaletteUse(cmd.Code)
	}
}

func paletteArgs(query string, cmd command.Command) []string {
	action, ok := palette.ParseAction(palette.CommandResults([]command.Command{cmd}), query)
	if !ok {
		return nil
	}
	return action.Args
}

func (ac *attachedClient) clearPaletteLocked() {
	ac.overlays.paletteGeneration++
	ac.overlays.palette = nil
	ac.overlays.paletteRecent = nil
	ac.overlays.paletteHints = palette.ContextualHints{}
	ac.overlays.paletteFeedback = ""
	ac.overlays.palettePending = nil
}

func (ac *attachedClient) paletteFailure(generation uint64, rawQuery, feedback string) {
	ac.overlays.paletteMu.Lock()
	defer ac.overlays.paletteMu.Unlock()
	if ac.overlays.palette == nil || ac.overlays.paletteGeneration != generation || ac.overlays.palette.Query() != rawQuery {
		return
	}
	ac.overlays.paletteFeedback = feedback
}

func (ac *attachedClient) closeExecutedPalette(generation uint64, rawQuery string) bool {
	ac.overlays.paletteMu.Lock()
	defer ac.overlays.paletteMu.Unlock()
	if ac.overlays.palette == nil || ac.overlays.paletteGeneration != generation || ac.overlays.palette.Query() != rawQuery {
		return false
	}
	ac.clearPaletteLocked()
	return true
}

type paletteExec struct {
	d      *Daemon
	sess   *session
	ac     *attachedClient
	recent []recentSession
}

func (e paletteExec) CreateTab() error {
	defer e.d.invalidateRender(e.sess, e.ac, true, "palette.go")
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
	e.d.invalidateRender(e.sess, e.ac, true, "palette.go")
	return nil
}

func (e paletteExec) PrevTab() error {
	if e.sess.switchRelative(-1) {
		e.d.activateTab(e.sess, e.sess.activeTab())
	}
	e.d.invalidateRender(e.sess, e.ac, true, "palette.go")
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

func (e paletteExec) OpenNotifications() error {
	e.d.enterNotices(e.sess, e.ac)
	return nil
}

func (e paletteExec) YankLastNotification() error {
	n, ok := e.d.notices.latest()
	if !ok {
		// Reported directly rather than returned: the generic cmd.Run error
		// path only logs, so a returned error here would never reach the
		// user as the warn toast this no-op is supposed to produce.
		e.d.reportError(e.sess, domain.UserWarn(domain.NoticeClipboard, "no notifications yet", nil))
		return nil
	}
	e.d.yankNotice(e.sess, e.ac, n)
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

func composePaletteClientFrame(model *palette.Model, base renderer.Frame, cfg domain.PaletteConfig, guidance string, styles ...themeui.Styles) (renderer.Frame, []renderer.Damage) {
	styleSet := resolveStyles(styles)
	modal := paletteModalFor(domain.Size{Cols: base.Width, Rows: base.Height}, cfg)
	return composeModalClientFrame(base, modal, styleSet, func(size domain.Size) renderer.Frame {
		return model.Render(size, palette.RenderOptions{Styles: palette.RenderStyles{Base: styleSet.PickerBase, Row: styleSet.SurfaceInactive, Selection: styleSet.PickerSelection, Description: styleSet.PickerDescription, SelectionDescription: styleSet.PickerSelectionMuted}, Guidance: guidance})
	})
}

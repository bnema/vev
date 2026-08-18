package daemon

import (
	"errors"
	"sort"
	"time"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/palette"
	"github.com/bnema/vev/internal/usecase/picker"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
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
	// Capture the client-owned route snapshot before taking paletteMu. The
	// snapshot is immutable for this palette interaction and the daemon never
	// consults its own session history for recent-route commands.
	routeSnapshot := ac.routeSnapshotCopy()
	commands := d.paletteCommands()
	results := d.paletteResults(sess, commands, routeSnapshot)
	ac.overlays.paletteMu.Lock()
	ac.overlays.paletteGeneration++
	ac.overlays.palette = palette.New(results)
	ac.overlays.paletteRouteSnapshot = routeSnapshot
	ac.overlays.paletteHints = palette.ContextualHints{}
	ac.overlays.palettePreview = ""
	ac.overlays.paletteFeedback = ""
	ac.overlays.palettePending = nil
	ac.overlays.paletteMu.Unlock()
	d.invalidateRender(sess, ac, true, "palette.go")
}

// paletteResults captures eligible named sessions before paletteMu so the
// palette has an immutable lifecycle target without violating lock ordering.
func (d *Daemon) paletteResults(current *session, commands []command.Command, routeSnapshot ports.RecentRouteSnapshot) []palette.Result {
	d.mu.Lock()
	sessions := d.sessionsSnapshotLocked()
	stopped := make([]stoppedSession, 0, len(d.stopped))
	for _, entry := range d.stopped {
		if !entry.purging {
			stopped = append(stopped, entry)
		}
	}
	d.mu.Unlock()

	results := make([]palette.Result, 0, len(commands)+len(sessions)+len(stopped)+len(routeSnapshot.Entries))
	for _, cmd := range commands {
		results = append(results, palette.NewCommandResult(cmd))
	}
	active := make([]palette.Result, 0, len(sessions))
	localLifecycles := make(map[domain.SessionLifecycleID]struct{}, len(sessions)+len(stopped))
	for _, candidate := range sessions {
		snap := candidate.snapshotView(viewOptions{})
		if snap.name == "" || snap.ephemeral {
			continue
		}
		localLifecycles[snap.incarnation] = struct{}{}
		if candidate == current {
			continue
		}
		target := ports.ExactSessionTarget{LifecycleID: snap.incarnation, SessionName: snap.name}
		active = append(active, palette.NewActiveSessionResult(snap.name, time.Unix(0, snap.createdAt), target))
	}
	sort.Slice(active, func(i, j int) bool {
		left, _ := active[i].SessionName()
		right, _ := active[j].SessionName()
		return left < right
	})
	sort.Slice(stopped, func(i, j int) bool { return stopped[i].name < stopped[j].name })
	results = append(results, active...)
	for _, candidate := range stopped {
		target := ports.ExactSessionTarget{LifecycleID: candidate.incarnation, SessionName: candidate.name}
		results = append(results, palette.NewStoppedSessionResult(candidate.name, time.Unix(0, candidate.createdAt), target))
		localLifecycles[target.LifecycleID] = struct{}{}
	}
	formattedRoutes := formatRecentRouteSnapshot(routeSnapshot)
	for i, entry := range routeSnapshot.Entries {
		if i >= len(formattedRoutes) {
			break
		}
		label := formattedRoutes[i].name
		if label == "" {
			continue
		}
		if _, represented := localLifecycles[entry.Target.LifecycleID]; represented {
			continue
		}
		results = append(results, palette.NewRecentRouteResult(label, ports.RouteNavigationAction{
			SnapshotGeneration: routeSnapshot.Generation,
			Key:                entry.Key,
			Generation:         entry.Generation,
		}))
	}
	return results
}

func recentRouteHints(snapshot ports.RecentRouteSnapshot, args []string) palette.ContextualHints {
	formatted := formatRecentRouteSnapshot(snapshot)
	names := make([]string, len(formatted))
	for i, entry := range formatted {
		names[i] = entry.name
	}
	hints := palette.BuildRecentSessionHints(names, args)
	for i, entry := range snapshot.Entries {
		if i >= len(hints.Recent) {
			break
		}
		hints.Recent[i].SnapshotGeneration = snapshot.Generation
		hints.Recent[i].Key = entry.Key
		hints.Recent[i].Generation = entry.Generation
	}
	return hints
}

func (d *Daemon) paletteCommands() []command.Command {
	commands := command.PaletteRegistry()
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

func (d *Daemon) handlePaletteInput(ac *attachedClient, data []byte, effects ...*attachmentEffectTicket) {
	entry := ac.currentAttachmentSession()
	if entry == nil {
		return
	}
	sess := entry
	var cmd command.Command
	var sessionTarget palette.Result
	var hasSessionTarget bool
	var routeTarget ports.RouteNavigationAction
	var hasRouteTarget bool
	var args []string
	var routeSnapshot ports.RecentRouteSnapshot
	var generation uint64
	var rawQuery string
	changed, cancel, execute := false, false, false
	var effect *attachmentEffectTicket
	if len(effects) != 0 {
		effect = effects[0]
	}

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
				if cmd.Arguments != command.ArgumentsNone {
					action, valid := palette.ParseAction([]palette.Result{selected}, rawQuery)
					if valid {
						args = action.Args
					} else if cmd.Arguments == command.ArgumentsRequired {
						changed = true
						return
					}
				}
				if cmd.ContextHint == command.ContextHintRecentSessions {
					routeSnapshot = ac.overlays.paletteRouteSnapshot
					routeSnapshot.Entries = append([]ports.RecentRouteEntry(nil), routeSnapshot.Entries...)
				}
			} else if action, ok := selected.RouteNavigationAction(); ok {
				routeTarget = action
				hasRouteTarget = true
			} else if _, ok := selected.SessionTarget(); ok {
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
		ac.overlays.palettePreview = ""
		active, ok := ac.overlays.palette.ArgumentCommand()
		if ok {
			ac.overlays.palettePreview = palette.Preview(active, ac.overlays.palette.Query())
		}
		if ok && active.Slug == "jump-recent-session" {
			args := paletteArgs(ac.overlays.palette.Query(), active)
			if ac.overlays.paletteRouteSnapshot.Generation != 0 {
				ac.overlays.paletteHints = recentRouteHints(ac.overlays.paletteRouteSnapshot, args)
			} else {
				ac.overlays.paletteHints = palette.ContextualHints{}
			}
		} else {
			ac.overlays.paletteHints = palette.ContextualHints{}
		}
	}
	if cancel {
		ac.clearPaletteLocked()
	}
	ac.overlays.paletteMu.Unlock()
	if cancel {
		d.invalidateRender(entry, ac, true, "palette.go")
		return
	}
	if !execute {
		if changed {
			d.invalidateRender(entry, ac, true, "palette.go")
		}
		return
	}

	if hasRouteTarget {
		exec := paletteExec{d: d, sess: sess, attachment: entry, ac: ac, effect: effect}
		if err := exec.NavigateRecentRoute(routeTarget); err != nil {
			if errors.Is(err, errAttachmentTransition) {
				return
			}
			ac.paletteFailure(generation, rawQuery, "requested recent session is unavailable")
			d.invalidateRender(entry, ac, true, "palette.go")
			return
		}
		if ac.closeExecutedPalette(generation, rawQuery) {
			d.invalidateRender(entry, ac, true, "palette.go")
		}
		return
	}

	if hasSessionTarget {
		createdAt, _ := sessionTarget.SessionCreatedAt()
		expectedCreatedAt := createdAt.UnixNano()
		name, _ := sessionTarget.SessionName()
		exactTarget, _ := sessionTarget.SessionTarget()
		target := picker.Target{
			Incarnation: exactTarget.LifecycleID, Name: name, TabIndex: -1,
			ExpectedCreatedAt: &expectedCreatedAt, Stopped: sessionTarget.Kind() == palette.ResultKindStoppedSession,
		}
		var err error
		if effect != nil {
			err = d.switchToTargetForAttachment(effect.connectionToken(), target, sessionHandoffGuard{}, "palette-session")
		} else {
			err = d.switchToTarget(sess, ac, target)
		}
		if errors.Is(err, errAttachmentTransition) {
			return
		}
		if err != nil {
			ac.paletteFailure(generation, rawQuery, "requested session is unavailable")
			d.invalidateRender(entry, ac, true, "palette.go")
			return
		}
		if ac.closeExecutedPalette(generation, rawQuery) {
			if current := ac.currentAttachmentSession(); current != nil {
				d.invalidateRender(current, ac, true, "palette.go")
			}
		}
		return
	}

	if cmd.Slug == "jump-recent-session" {
		rank, err := command.ParsePositiveDecimal(args)
		if err != nil {
			ac.paletteFailure(generation, rawQuery, "rank must be one positive decimal")
			d.invalidateRender(entry, ac, true, "palette.go")
			return
		}
		exec := paletteExec{d: d, sess: sess, attachment: entry, ac: ac, routeSnapshot: routeSnapshot, effect: effect}
		if err := exec.JumpRecentSession(rank); err != nil {
			if errors.Is(err, errAttachmentTransition) {
				return
			}
			ac.paletteFailure(generation, rawQuery, "requested recent session is unavailable")
			d.invalidateRender(entry, ac, true, "palette.go")
			return
		}
		if ac.closeExecutedPalette(generation, rawQuery) {
			d.recordPaletteUse(cmd.Code)
			if current := ac.currentAttachmentSession(); current != nil {
				d.invalidateRender(current, ac, true, "palette.go")
			}
		}
		return
	}
	attachmentHandoff := cmd.Slug == "back-session" || cmd.Slug == "detach"
	if !attachmentHandoff && !ac.closeExecutedPalette(generation, rawQuery) {
		return
	}
	sess.dispatchMu.Lock()
	err := cmd.Run(paletteExec{d: d, sess: sess, ac: ac, effect: effect, redrawClosedPalette: true}, args)
	sess.dispatchMu.Unlock()
	if attachmentHandoff {
		if current := ac.currentSession(); current != nil {
			currentToken := current.attachmentToken(ac, ac.transport())
			fresh, admitted := ac.beginAttachmentEffect(currentToken)
			if ac.closeExecutedPalette(generation, rawQuery) {
				d.invalidateRender(current, ac, true, "palette.go")
			}
			if admitted {
				fresh.End()
			}
		} else {
			ac.closeExecutedPalette(generation, rawQuery)
		}
	}
	if errors.Is(err, errAttachmentTransition) {
		return
	}
	if err != nil {
		d.log.Error("palette command failed", "err", err, "command", cmd.Code)
		d.reportError(sess, paletteCommandNoticeError(cmd, err))
	} else {
		d.recordPaletteUse(cmd.Code)
	}
}

func paletteCommandNoticeError(cmd command.Command, err error) error {
	if cmd.Scope == command.CommandScopeCrossSession {
		return movePickerUserError(err)
	}
	return err
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
	ac.overlays.paletteRouteSnapshot = ports.RecentRouteSnapshot{}
	ac.overlays.paletteHints = palette.ContextualHints{}
	ac.overlays.palettePreview = ""
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
	d                   *Daemon
	sess                *session
	attachment          *session
	ac                  *attachedClient
	routeSnapshot       ports.RecentRouteSnapshot
	actions             daemonActionRunner
	effect              *attachmentEffectTicket
	redrawClosedPalette bool
}

func (e paletteExec) runAction(request daemonActionRequest) error {
	request.effect = e.effect
	if request.target.session == nil {
		request.target = resolveDaemonActionTargetForAttachment(e.sess, e.ac)
	}
	runner := e.actions
	if runner == nil {
		runner = daemonActions{d: e.d}
	}
	err := runner.Run(request)
	if errors.Is(err, errDaemonActionNoChange) {
		if e.redrawClosedPalette {
			e.d.invalidateRender(e.sess, e.ac, true, "palette.go")
		}
		return nil
	}
	if err != nil {
		return err
	}
	if e.actions == nil {
		finishDaemonActionForClient(e.d, request, e.ac, "palette.go")
	}
	return nil
}

func (e paletteExec) CreateTab() error {
	return e.runAction(daemonActionRequest{kind: daemonActionCreateTab, viewport: e.sess.fullViewportSize()})
}

func (e paletteExec) CreateSession() error {
	entry := e.attachment
	if entry == nil {
		entry = e.sess
	}
	e.d.enterTransitionPrompt(entry, e.ac, " Create session ", "", func(name string, token attachmentConnectionToken) error {
		if token.ac == nil {
			if e.sess == nil {
				return errAttachmentTransition
			}
			return e.d.createSessionAndSwitch(e.sess, e.ac, name)
		}
		return e.d.createSessionAndSwitchForAttachment(token, name)
	})
	return nil
}

func (e paletteExec) CreateSessionNamed(name string) error {
	if e.effect == nil {
		return e.d.createSessionAndSwitch(e.sess, e.ac, name)
	}
	return e.d.createSessionAndSwitchForAttachment(e.effect.connectionToken(), name)
}

func (e paletteExec) CreateEphemeralSession() error {
	if e.effect == nil {
		return e.d.createEphemeralSessionAndSwitch(e.sess, e.ac)
	}
	return e.d.createEphemeralSessionAndSwitchForAttachment(e.effect.connectionToken())
}

func (e paletteExec) CloseTab() error {
	return e.runAction(daemonActionRequest{kind: daemonActionCloseTab})
}

func (e paletteExec) split(direction layout.Direction) error {
	return e.runAction(daemonActionRequest{kind: daemonActionSplitPane, direction: direction})
}

func (e paletteExec) SplitRight() error { return e.split(layout.Right) }
func (e paletteExec) SplitLeft() error  { return e.split(layout.Left) }
func (e paletteExec) SplitUp() error    { return e.split(layout.Up) }
func (e paletteExec) SplitDown() error  { return e.split(layout.Down) }
func (e paletteExec) consumeOrExpelPane(direction layout.Direction) error {
	return e.runAction(daemonActionRequest{kind: daemonActionConsumeOrExpelPane, direction: direction})
}
func (e paletteExec) ConsumeOrExpelPaneLeft() error {
	return e.consumeOrExpelPane(layout.Left)
}
func (e paletteExec) ConsumeOrExpelPaneRight() error {
	return e.consumeOrExpelPane(layout.Right)
}

func (e paletteExec) StackPane() error {
	return e.runAction(daemonActionRequest{kind: daemonActionStackPane})
}

func (e paletteExec) ToggleStack() error {
	return e.runAction(daemonActionRequest{kind: daemonActionToggleStack})
}

func (e paletteExec) ClosePane() error {
	return e.runAction(daemonActionRequest{kind: daemonActionClosePane})
}

func (e paletteExec) OpenMovePanePicker() error {
	target := resolveDaemonActionTargetForAttachment(e.sess, e.ac)
	if target.tab == nil || target.pane == nil {
		return errMovePaneInvalid
	}
	return e.d.enterPickerForIntent(e.sess, e.ac, pickerMovePane, moveSourceLocator{
		Session:    sessionMoveLocator(e.sess),
		TabID:      domain.TabStableID(target.tab.stableID),
		PaneID:     domain.PaneStableID(target.pane.stableID),
		Attachment: e.ac,
	})
}

func (e paletteExec) OpenMoveTabPicker() error {
	target := resolveDaemonActionTargetForAttachment(e.sess, e.ac)
	if target.tab == nil {
		return errMovePaneInvalid
	}
	return e.d.enterPickerForIntent(e.sess, e.ac, pickerMoveTab, moveSourceLocator{
		Session:    sessionMoveLocator(e.sess),
		TabID:      domain.TabStableID(target.tab.stableID),
		Attachment: e.ac,
	})
}

func (e paletteExec) focus(direction layout.Direction) error {
	return e.runAction(daemonActionRequest{kind: daemonActionFocusPane, direction: direction})
}

func (e paletteExec) FocusPaneLeft() error  { return e.focus(layout.Left) }
func (e paletteExec) FocusPaneRight() error { return e.focus(layout.Right) }
func (e paletteExec) FocusPaneUp() error    { return e.focus(layout.Up) }
func (e paletteExec) FocusPaneDown() error  { return e.focus(layout.Down) }

func (e paletteExec) EnterResizeMode() error { return e.d.enterResizeMode(e.sess, e.ac) }
func (e paletteExec) resize(axis layout.Axis, delta int) error {
	return resizeUserError(e.runAction(daemonActionRequest{kind: daemonActionResizePane, axis: axis, delta: delta}))
}
func (e paletteExec) GrowPaneWidth() error    { return e.resize(layout.Width, resizeStepCols) }
func (e paletteExec) ShrinkPaneWidth() error  { return e.resize(layout.Width, -resizeStepCols) }
func (e paletteExec) GrowPaneHeight() error   { return e.resize(layout.Height, resizeStepRows) }
func (e paletteExec) ShrinkPaneHeight() error { return e.resize(layout.Height, -resizeStepRows) }
func (e paletteExec) EqualizePanes() error {
	return resizeUserError(e.runAction(daemonActionRequest{kind: daemonActionEqualizePanes}))
}

func (e paletteExec) NextTab() error {
	return e.runAction(daemonActionRequest{kind: daemonActionNextTab})
}

func (e paletteExec) PrevTab() error {
	return e.runAction(daemonActionRequest{kind: daemonActionPreviousTab})
}

func (e paletteExec) ToggleFloatingPane() error {
	return e.d.toggleFloating(e.sess, e.ac)
}

func (e paletteExec) BackSession() error {
	if e.effect != nil {
		return e.d.backSessionForAttachment(e.effect.connectionToken())
	}
	e.d.backSession(e.sess, e.ac)
	return nil
}

func (e paletteExec) Detach() error {
	if e.effect != nil {
		if !e.d.clientGoneForAttachment(e.effect.connectionToken(), true) {
			return errAttachmentTransition
		}
		return nil
	}
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
		return e.runAction(daemonActionRequest{kind: daemonActionRenameSession, name: name})
	})
	return nil
}

func (e paletteExec) RenameSessionTo(name string) error {
	return e.runAction(daemonActionRequest{kind: daemonActionRenameSession, name: name})
}

func (e paletteExec) RenameTab() error {
	tb := e.sess.tabForAttachment(e.ac)
	if e.ac == nil {
		tb = e.sess.firstTab()
	}
	if tb == nil {
		return nil
	}
	e.sess.mu.Lock()
	index := 0
	for i, candidate := range e.sess.tabs {
		if candidate == tb {
			index = i
			break
		}
	}
	currentName := tabDisplayName(tb, index)
	e.sess.mu.Unlock()
	e.d.enterPrompt(e.sess, e.ac, " Rename tab ", currentName, func(name string) error {
		return e.runAction(daemonActionRequest{kind: daemonActionRenameTab, target: daemonActionTarget{session: e.sess, tab: tb}, name: name})
	})
	return nil
}

func (e paletteExec) RenameTabTo(name string) error {
	tb := e.sess.tabForAttachment(e.ac)
	if e.ac == nil {
		tb = e.sess.firstTab()
	}
	if tb == nil {
		return nil
	}
	return e.runAction(daemonActionRequest{kind: daemonActionRenameTab, target: daemonActionTarget{session: e.sess, tab: tb}, name: name})
}

func (e paletteExec) OpenSessionPicker() error {
	if e.ac != nil && e.ac.navigationCapabilities&ports.NavigationCapabilityHomePicker != 0 && e.effect != nil {
		return e.d.sendNavigationActionForAttachment(e.effect.connectionToken(), ports.NavigationOpenHomePicker)
	}
	e.d.enterPicker(e.sess, e.ac)
	return nil
}

func (e paletteExec) OpenNotifications() error {
	e.d.enterNotices(e.sess, e.ac)
	return nil
}

func (e paletteExec) YankLastNotification() error {
	entry := e.attachment
	if entry == nil {
		entry = e.sess
	}
	n, ok := e.d.notices.latest()
	if !ok {
		// Reported directly rather than returned: the generic cmd.Run error
		// path only logs, so a returned error here would never reach the
		// user as the warn toast this no-op is supposed to produce.
		e.d.reportAttachmentError(entry, domain.UserWarn(domain.NoticeClipboard, "no notifications yet", nil))
		return nil
	}
	e.d.yankNotice(entry, e.ac, n)
	return nil
}

func (e paletteExec) JumpRecentSession(rank int) error {
	if e.routeSnapshot.Generation == 0 || rank < 1 || rank > len(e.routeSnapshot.Entries) {
		return command.ErrInvalidArguments
	}
	entry := e.routeSnapshot.Entries[rank-1]
	return e.NavigateRecentRoute(ports.RouteNavigationAction{
		SnapshotGeneration: e.routeSnapshot.Generation,
		Key:                entry.Key,
		Generation:         entry.Generation,
	})
}

func (e paletteExec) NavigateRecentRoute(action ports.RouteNavigationAction) error {
	if e.d.beforeRecentSessionHandoff != nil {
		e.d.beforeRecentSessionHandoff()
	}
	if e.effect != nil {
		return e.d.sendRecentRouteNavigationActionForAttachment(e.effect.connectionToken(), action)
	}
	if e.ac == nil || e.attachment == nil {
		return errAttachmentTransition
	}
	return e.d.sendRecentRouteNavigationActionForAttachment(e.attachment.attachmentToken(e.ac, e.ac.transport()), action)
}

func composePaletteClientFrame(model *palette.Model, base renderer.Frame, cfg domain.PaletteConfig, guidance string, styles ...themeui.Styles) (renderer.Frame, []renderer.Damage) {
	styleSet := resolveStyles(styles)
	modal := paletteModalFor(domain.Size{Cols: base.Width, Rows: base.Height}, cfg)
	return composeModalClientFrame(base, modal, styleSet, func(size domain.Size) renderer.Frame {
		return model.Render(size, palette.RenderOptions{Styles: palette.RenderStyles{Base: styleSet.PickerBase, Row: styleSet.PickerBase, Selection: styleSet.PickerSelection, Description: styleSet.PickerDescription, SelectionDescription: styleSet.PickerSelectionMuted}, Guidance: guidance})
	})
}

// Package daemon holds vev's server-side session multiplexer use case.
package daemon

import (
	"context"
	"errors"
	"sort"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/picker"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
)

var pickerModal = ui.Modal{WidthPct: 80, HeightPct: 80, MinWidth: 24, MinHeight: 8, Title: " Sessions ", Anchor: domain.AnchorCenter, Margins: ui.Margins{}}

// pickerSortMode orders the picker's live sessions. It lives for the daemon's
// lifetime only and is never persisted.
type pickerSortMode uint32

const (
	pickerSortRecent pickerSortMode = iota
	pickerSortGrouped
)

func pickerTitle(mode pickerSortMode) string {
	if mode == pickerSortGrouped {
		return " Sessions · grouped "
	}
	return " Sessions · recent "
}

// togglePickerSort flips the daemon's picker sort mode between the two known
// modes with a CompareAndSwap loop, so a concurrent double-press can't lose
// an update the way a bare Load-then-Store XOR can.
func (d *Daemon) togglePickerSort() {
	for {
		cur := pickerSortMode(d.pickerSort.Load())
		next := pickerSortRecent
		if cur == pickerSortRecent {
			next = pickerSortGrouped
		}
		if d.pickerSort.CompareAndSwap(uint32(cur), uint32(next)) {
			return
		}
	}
}

// enterPicker preserves the existing navigation entry point. Navigation always
// publishes its model, including an empty one; only move entry can fail for a
// missing destination.
func (d *Daemon) enterPicker(sess *session, ac *attachedClient) {
	model := d.newPickerModel(sess, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{})
	d.publishPicker(sess, ac, model, pickerNavigate, moveSourceLocator{})
}

func (d *Daemon) publishPicker(sess *session, ac *attachedClient, model *picker.Model, intent pickerIntent, source moveSourceLocator) {
	rt := ac.overlays
	rt.pickerMu.Lock()
	previous, previousGeneration := rt.pickerPreviewSession, rt.pickerPreviewGeneration
	rt.pickerPreviewGeneration++
	rt.pickerPreviewSession = nil
	rt.pickerPreview = nil
	rt.picker = model
	rt.pickerTitle = pickerTitle(pickerSortMode(d.pickerSort.Load()))
	rt.pickerIntent = intent
	rt.pickerSource = source
	rt.pickerPending = nil
	rt.pickerESC.stop()
	rt.pickerMu.Unlock()
	d.teardownPreviewSubscription(ac, previous, previousGeneration)
	d.registerPreviewForSelection(ac)
	d.invalidateRender(sess, ac, true, "picker.go")
}

// pickerViews captures one canonical lifecycle/tab snapshot. It intentionally
// knows nothing about picker intent; picker.New owns all destination policy.
func (d *Daemon) pickerViews(cur *session) ([]picker.SessionView, picker.SourceFilter) {
	d.mu.Lock()
	sessions := d.sessionsSnapshotLocked()
	stopped := make([]stoppedSession, 0, len(d.stopped))
	for _, s := range d.stopped {
		if !s.purging {
			stopped = append(stopped, s)
		}
	}
	d.mu.Unlock()
	// Snapshot mruAt, name, and ephemeral once: comparators must not observe
	// concurrent touchMRU updates or a renameSession mutation (which mutates
	// name/ephemeral under sess.mu) mid-sort.
	type pickerSortSnapshot struct {
		mruAt     uint64
		name      string
		ephemeral bool
	}
	snap := make(map[*session]pickerSortSnapshot, len(sessions))
	for _, s := range sessions {
		mruAt := s.mruAt.Load()
		s.mu.Lock()
		name := s.name
		ephemeral := s.ephemeral
		s.mu.Unlock()
		snap[s] = pickerSortSnapshot{mruAt: mruAt, name: name, ephemeral: ephemeral}
	}
	sort.Slice(sessions, func(i, j int) bool {
		if snap[sessions[i]].mruAt != snap[sessions[j]].mruAt {
			return snap[sessions[i]].mruAt > snap[sessions[j]].mruAt
		}
		return snap[sessions[i]].name < snap[sessions[j]].name
	})
	sort.Slice(stopped, func(i, j int) bool {
		if stopped[i].lastUsedSeq != stopped[j].lastUsedSeq {
			return stopped[i].lastUsedSeq > stopped[j].lastUsedSeq
		}
		return stopped[i].name < stopped[j].name
	})
	if pickerSortMode(d.pickerSort.Load()) == pickerSortGrouped {
		// Stable partition: named sessions first, ephemeral after, each keeping
		// its MRU order from the sort above.
		sort.SliceStable(sessions, func(i, j int) bool {
			return !snap[sessions[i]].ephemeral && snap[sessions[j]].ephemeral
		})
	}

	for _, s := range sessions {
		d.refreshSessionFocusedTitles(s)
	}

	includeTerminalTitle := d.currentTabsConfig().TerminalTitle
	views := make([]picker.SessionView, 0, len(sessions)+len(stopped))
	var current picker.SourceFilter
	for _, s := range sessions {
		s.mu.Lock()
		view := picker.SessionView{
			ID: s.id, Incarnation: s.incarnation, Name: s.name, TargetName: s.name, Active: s.active,
			Tabs: make([]picker.TabEntry, 0, len(s.tabs)),
		}
		if !s.ephemeral {
			createdAt := s.createdAt
			view.ExpectedCreatedAt = &createdAt
		}
		sessionAttention := false
		for i, tb := range s.tabs {
			name := tabDisplayName(tb, i)
			tabID := domain.TabStableID(tb.stableID)
			view.Tabs = append(view.Tabs, picker.TabEntry{
				TabID:     tabID,
				Name:      name,
				Detail:    tabTitleDetail(name, tb.focusedPaneTitle(includeTerminalTitle)),
				Attention: tb.attention,
			})
			if tb.attention {
				sessionAttention = true
			}
			if s == cur && i == s.active {
				current = picker.SourceFilter{Session: s.id, Incarnation: s.incarnation, TabID: tabID}
			}
		}
		if sessionAttention {
			view.Name = attentionSuffix(view.Name)
		}
		s.mu.Unlock()
		views = append(views, view)
	}
	for _, s := range stopped {
		createdAt := s.createdAt
		views = append(views, picker.SessionView{
			ID:                domain.SessionID("stopped:" + s.name),
			Incarnation:       s.incarnation,
			Name:              s.name,
			TargetName:        s.name,
			Tabs:              []picker.TabEntry{{}},
			Stopped:           true,
			ExpectedCreatedAt: &createdAt,
		})
	}
	return views, current
}

func (d *Daemon) newPickerModel(cur *session, intent pickerIntent, source moveSourceLocator, current picker.SourceFilter) *picker.Model {
	views, attachedCurrent := d.pickerViews(cur)
	if intent == pickerNavigate && current == (picker.SourceFilter{}) {
		current = attachedCurrent
	}
	return picker.New(views, picker.SelectionConfig{
		Mode:    pickerSelectionMode(intent),
		Current: current,
		Source: picker.SourceFilter{
			Session: source.Session.ID, Incarnation: source.Session.Incarnation, TabID: source.TabID,
		},
	})
}

func attentionSuffix(label string) string {
	return label + " " + string(ui.AttentionGlyph)
}

func (d *Daemon) pickerListInputState(ac *attachedClient) listInputState {
	rt := ac.overlays
	var generation uint64
	return listInputState{
		pending:  &rt.pickerPending,
		esc:      &rt.pickerESC,
		moveUp:   rt.picker.Up,
		moveDown: rt.picker.Down,
		lock:     rt.pickerMu.Lock,
		unlock:   rt.pickerMu.Unlock,
		active:   func() bool { return rt.picker != nil },
		closeLocked: func() {
			rt.picker = nil
			rt.pickerIntent = pickerNavigate
			rt.pickerSource = moveSourceLocator{}
			generation = rt.pickerPreviewGeneration
		},
		afterClose: func() {
			d.clearPreviewGeneration(ac, generation)
			if sess := ac.currentSession(); sess != nil {
				d.invalidateRender(sess, ac, true, "picker.go")
			}
		},
	}
}

func (d *Daemon) handlePickerInput(ac *attachedClient, data []byte, effects ...*roleEffectTicket) {
	sess := ac.currentSession()
	if sess == nil {
		return
	}
	rt := ac.overlays
	rt.pickerMu.Lock()
	if rt.picker == nil {
		rt.pickerPending = nil
		rt.pickerESC.stop()
		rt.pickerMu.Unlock()
		return
	}
	result := handleListInputLocked(d.clock, data, d.pickerListInputState(ac), func(b byte) listInputResult {
		switch b {
		case 'x':
			return listInputResult{action: b, stop: true}
		case 's':
			return listInputResult{action: b, stop: true}
		case '\r', '\n':
			return listInputResult{action: b, exit: true}
		default:
			return listInputResult{}
		}
	})
	var target picker.Target
	var ok bool
	var intent pickerIntent
	var source moveSourceLocator
	// prevIdx is the row the victim occupied; -1 means "no post-delete hint".
	prevIdx := -1
	if result.action == 'x' || result.action == '\r' || result.action == '\n' {
		target, ok = rt.picker.Selected()
		prevIdx = rt.picker.SelectedIndex()
	}
	intent, source = rt.pickerIntent, rt.pickerSource
	rt.pickerMu.Unlock()

	if result.action == 's' {
		d.togglePickerSort()
		d.refreshPickerOpts(ac, pickerRefreshOptions{preserveSelection: true, nearestRow: -1})
		d.invalidateRender(sess, ac, true, "picker.go")
		return
	}
	if result.action == 'x' {
		var effect *roleEffectTicket
		if len(effects) != 0 {
			effect = effects[0]
		}
		if ok {
			var err error
			if effect != nil {
				effect.bindActionEnd(d, "picker-delete")
				err = d.killPickerTargetForRole(target, effect.roleToken())
			} else {
				err = d.killPickerTarget(target)
			}
			if err != nil {
				d.reportError(sess, err)
			}
		}
		if effect != nil && effect.ended.Load() {
			fresh, admitted := ac.beginRoleEffect(effect.token)
			if !admitted {
				return
			}
			defer fresh.End()
		}
		d.refreshPickerOpts(ac, pickerRefreshOptions{nearestRow: prevIdx})
		d.invalidateRender(sess, ac, true, "picker.go")
		return
	}
	if result.changed {
		d.registerPreviewForSelection(ac)
	}
	committing := (result.action == '\r' || result.action == '\n') && ok
	if result.exit && !committing {
		d.closePicker(ac)
	}
	if committing {
		if intent == pickerMovePane || intent == pickerMoveTab {
			if err := d.movePickerSourceError(source); err != nil {
				d.reportError(sess, err)
				d.invalidateRender(sess, ac, true, "picker.go")
				return
			}
			if len(effects) != 0 && effects[0] != nil {
				effects[0].End()
			}
			d.closePicker(ac)
			err := d.commitMovePickerSelection(intent, source, target)
			if err != nil {
				d.reportError(sess, movePickerUserError(err))
				d.invalidateRender(sess, ac, true, "picker.go")
				return
			}
			d.invalidateRender(sess, ac, true, "picker.go")
			return
		}
		var err error
		if len(effects) != 0 && effects[0] != nil {
			err = d.switchToTargetForRole(effects[0].roleToken(), target, sessionHandoffGuard{closePicker: true}, "picker-select")
		} else {
			d.closePicker(ac)
			err = d.switchToTarget(sess, ac, target)
		}
		if errors.Is(err, errAttachmentTransition) {
			return
		}
		if err != nil {
			d.reportError(sess, err)
			return
		}
		return
	}
	if result.exit || result.changed {
		d.invalidateRender(sess, ac, true, "picker.go")
	}
}

func (d *Daemon) registerPreviewForSelection(ac *attachedClient) {
	if ac == nil || ac.overlays == nil {
		return
	}
	ac.overlays.pickerMu.Lock()
	var target picker.Target
	var ok bool
	intent := ac.overlays.pickerIntent
	if ac.overlays.picker != nil {
		target, ok = ac.overlays.picker.Selected()
	}
	previous, previousGeneration := ac.overlays.pickerPreviewSession, ac.overlays.pickerPreviewGeneration
	ac.overlays.pickerPreviewGeneration++
	generation := ac.overlays.pickerPreviewGeneration
	ac.overlays.pickerPreviewSession = nil
	ac.overlays.pickerPreview = nil
	ac.overlays.pickerMu.Unlock()
	d.teardownPreviewSubscription(ac, previous, previousGeneration)
	if !ok {
		return
	}
	targetSess, next := d.previewTarget(target, intent)
	if next == nil || targetSess == nil {
		return
	}
	ac.overlays.pickerMu.Lock()
	// Selection may have changed while the target was resolved.
	selected, stillSelected := ac.overlays.picker.Selected()
	valid := ac.overlays.pickerPreviewGeneration == generation && ac.overlays.pickerIntent == intent && stillSelected && pickerTargetsEqual(selected, target)
	if valid {
		ac.overlays.pickerPreview = next
		ac.overlays.pickerPreviewSession = targetSess
	}
	ac.overlays.pickerMu.Unlock()
	if !valid {
		return
	}
	// A same-session preview is already invalidated by its own coordinator.
	// Cross-session previews subscribe to the target's producer wake instead
	// of starting a direct paint/timer path.
	if targetSess == ac.currentSession() {
		return
	}
	rc := d.attachCoordinator(targetSess, nil, nil, false)
	rc.subscribePreviewFor(ac, generation, func(renderWake) {
		if pickerPreviewCurrent(ac, targetSess, next, generation) {
			if owner := ac.currentSession(); owner != nil {
				d.invalidateRender(owner, ac, false, "picker preview")
			}
		}
	})
	// subscribePreviewFor is deliberately outside pickerMu. Revalidate after it
	// returns and remove only this generation if selection changed meanwhile.
	d.revalidatePreviewSubscription(ac, rc, targetSess, next, generation)
}

func pickerPreviewCurrent(ac *attachedClient, targetSess *session, next *tab, generation uint64) bool {
	ac.overlays.pickerMu.Lock()
	defer ac.overlays.pickerMu.Unlock()
	return ac.overlays.pickerPreviewGeneration == generation && ac.overlays.pickerPreviewSession == targetSess && ac.overlays.pickerPreview == next
}

// revalidatePreviewSubscription removes a subscription that lost the picker
// selection race after it was installed.
func (d *Daemon) revalidatePreviewSubscription(ac *attachedClient, rc *renderCoordinator, targetSess *session, next *tab, generation uint64) {
	if !pickerPreviewCurrent(ac, targetSess, next, generation) {
		rc.teardownPreviewFor(ac, generation)
	}
}

func (d *Daemon) teardownPreviewSubscription(ac *attachedClient, previous *session, generation uint64) {
	if previous != nil {
		if rc := previous.renderCoordinator(); rc != nil {
			rc.teardownPreviewFor(ac, generation)
		}
	}
}

func (d *Daemon) unregisterPreview(ac *attachedClient) {
	if ac == nil || ac.overlays == nil {
		return
	}
	ac.overlays.pickerMu.Lock()
	generation := ac.overlays.pickerPreviewGeneration
	ac.overlays.pickerMu.Unlock()
	d.clearPreviewGeneration(ac, generation)
}

// clearPreviewGeneration removes the preview only if it is still the observed
// selection. Teardown paths may race a new picker selection.
func (d *Daemon) clearPreviewGeneration(ac *attachedClient, generation uint64) bool {
	ac.overlays.pickerMu.Lock()
	if ac.overlays.pickerPreviewGeneration != generation {
		ac.overlays.pickerMu.Unlock()
		return false
	}
	previous := ac.overlays.pickerPreviewSession
	ac.overlays.pickerPreviewGeneration++
	ac.overlays.pickerPreviewSession = nil
	ac.overlays.pickerPreview = nil
	ac.overlays.pickerMu.Unlock()
	d.teardownPreviewSubscription(ac, previous, generation)
	return true
}

// clearDestroyedTabPreview clears coordinator-owned picker state for clients
// that were previewing a removed tab. Tabs do not route render ownership.
func (d *Daemon) clearDestroyedTabPreview(tb *tab) {
	if d == nil || d.sessions == nil {
		return
	}
	d.mu.Lock()
	var clients []*attachedClient
	for _, sess := range d.sessions {
		sess.mu.Lock()
		if sess.client != nil {
			clients = append(clients, sess.client)
		}
		sess.mu.Unlock()
	}
	d.mu.Unlock()
	for _, ac := range clients {
		if ac == nil || ac.overlays == nil {
			continue
		}
		ac.overlays.pickerMu.Lock()
		generation := ac.overlays.pickerPreviewGeneration
		observed := ac.overlays.pickerPreview == tb
		ac.overlays.pickerMu.Unlock()
		if observed && d.clearPreviewGeneration(ac, generation) {
			if sess := ac.currentSession(); sess != nil {
				d.invalidateRender(sess, ac, true, "picker.go")
			}
		}
	}
}

func (d *Daemon) closePicker(ac *attachedClient) {
	ac.overlays.pickerMu.Lock()
	ac.overlays.picker = nil
	ac.overlays.pickerTitle = ""
	ac.overlays.pickerIntent = pickerNavigate
	ac.overlays.pickerSource = moveSourceLocator{}
	ac.overlays.pickerPending = nil
	ac.overlays.pickerESC.stop()
	generation := ac.overlays.pickerPreviewGeneration
	ac.overlays.pickerMu.Unlock()
	d.clearPreviewGeneration(ac, generation)
}

func pickerTargetsEqual(left, right picker.Target) bool {
	if left.Session != right.Session || left.Incarnation != right.Incarnation || left.Name != right.Name ||
		left.TabID != right.TabID || left.TabIndex != right.TabIndex || left.Stopped != right.Stopped {
		return false
	}
	if left.ExpectedCreatedAt == nil || right.ExpectedCreatedAt == nil {
		return left.ExpectedCreatedAt == nil && right.ExpectedCreatedAt == nil
	}
	return *left.ExpectedCreatedAt == *right.ExpectedCreatedAt
}

func (d *Daemon) sessionByID(id domain.SessionID) *session {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.sessions[id]
}

// sessionHandoffGuard optionally constrains an active-session handoff to the
// tab that initiated navigation. Its zero value preserves ordinary picker and
// command switching behavior.
type sessionHandoffGuard struct {
	expectedSource *tab
	closePicker    bool
}

// switchActiveTargetForRole hands a frame-bound navigation request to the
// centralized transition. The transition releases admission at its freeze seam
// and revalidates the exact initiating token before changing target focus or
// attachment ownership.
func (d *Daemon) switchActiveTargetForRole(token attachmentRoleToken, target picker.Target) error {
	if token.sess == nil || token.ac == nil || token.role != attachmentActive {
		return nil
	}
	d.mu.Lock()
	targetSess := d.sessions[target.Session]
	d.mu.Unlock()
	if targetSess == nil || targetSess == token.sess {
		return nil
	}

	transition, err := d.transitionAttachment(attachmentTransitionRequest{
		source: token.sess, target: targetSess, next: token.ac,
		expectedRole: attachmentActive, targetRole: attachmentActive,
		expectedTransport: token.transport, sourceToken: &token, action: "jump-attention",
		activateTargetTab: true, targetTabIndex: target.TabIndex, copySourceEnvironment: true, ready: true,
	})
	if err != nil {
		// Losing the exact source role is a benign stale action, not a notice for
		// whichever attachment replaced the initiator.
		if !token.activeCurrent() {
			return nil
		}
		return domain.UserErr(domain.NoticeSessionUnavailable, "couldn't switch to that session", err)
	}
	d.touchMRU(targetSess)
	token.ac.recordPreviousSession(token.sess)
	d.deferAttachmentTransitionCleanups(transition)
	d.firstPaintForTransition(transition.published)
	return nil
}

// switchToTarget resolves named lifecycle targets and commits their transition
// while d.mu is held. A named palette result is allowed to cross an
// active/stopped transition, but it never follows a same-name replacement.
func (d *Daemon) switchToTarget(from *session, ac *attachedClient, target picker.Target) error {
	return d.switchToTargetGuarded(from, ac, target, sessionHandoffGuard{})
}

// switchToTargetForRole is the sole client-originated navigation entry. The
// intent captures the exact initiating capability before admission is released;
// every active, stopped, and same-session target then uses transitionAttachment
// for frozen, atomic source-token preflight.
func (d *Daemon) switchToTargetForRole(token attachmentRoleToken, target picker.Target, guard sessionHandoffGuard, action string) error {
	if token.sess == nil || token.ac == nil || token.role != attachmentActive || token.effect == nil {
		return nil
	}
	token.effect.bindActionEnd(d, action)
	token.effect.End()
	return d.switchToTargetGuardedForRole(token.sess, token.ac, target, guard, &token, action)
}

// switchToTargetGuarded is retained for daemon-internal and headless callers.
func (d *Daemon) switchToTargetGuarded(from *session, ac *attachedClient, target picker.Target, guard sessionHandoffGuard) error {
	return d.switchToTargetGuardedForRole(from, ac, target, guard, nil, "")
}

func (d *Daemon) switchToTargetGuardedForRole(from *session, ac *attachedClient, target picker.Target, guard sessionHandoffGuard, sourceToken *attachmentRoleToken, action string) error {
	if target.Name != "" {
		ctx := d.serveCtx
		if ctx == nil {
			ctx = context.Background()
		}
		if err := d.waitForTargetRestore(ctx, target.Name); err != nil {
			if sourceToken != nil && !sourceToken.activeCurrent() {
				return errAttachmentTransition
			}
			d.invalidateRender(from, ac, true, "picker.go")
			return domain.UserErr(domain.NoticeSessionUnavailable, "couldn't switch to that session", err)
		}
	}

	d.mu.Lock()
	var (
		targetSess *session
		transition attachmentTransitionResult
		switched   bool
		cause      error
	)
	if target.Name != "" {
		active, stopped, isStopped, ok := d.resolveNamedLifecycleTargetLocked(target)
		if ok {
			if isStopped {
				targetSess, transition, switched, cause = d.resumeStoppedAndSwitchLocked(from, ac, target, stopped, sourceToken, guard, action)
			} else {
				// The snapshot ID may describe the stopped representation. The
				// locked name/lifecycle resolver chose this active incarnation.
				resolvedTarget := target
				resolvedTarget.Session = active.id
				targetSess, transition, switched = d.switchToActiveTargetLocked(from, ac, active, resolvedTarget, guard, sourceToken, action)
			}
		}
	} else if active := d.sessions[target.Session]; active != nil {
		targetSess, transition, switched = d.switchToActiveTargetLocked(from, ac, active, target, guard, sourceToken, action)
	}
	d.mu.Unlock()

	if !switched {
		if sourceToken != nil && !sourceToken.activeCurrent() {
			return errAttachmentTransition
		}
		if guard.expectedSource != nil {
			return errNoNeighbor
		}
		d.invalidateRender(from, ac, true, "picker.go")
		return domain.UserErr(domain.NoticeSessionUnavailable, "couldn't switch to that session", cause)
	}
	if guard.closePicker {
		if sourceToken != nil {
			fresh, admitted := ac.beginRoleEffect(transition.published)
			if admitted {
				d.closePicker(ac)
				fresh.End()
			}
		} else {
			d.closePicker(ac)
		}
	}
	d.deferAttachmentTransitionCleanups(transition)
	if targetSess == from {
		if sourceToken != nil {
			fresh, admitted := ac.beginRoleEffect(transition.published)
			if admitted {
				d.activateTab(from, from.activeTab())
				d.invalidateRender(from, ac, true, "picker.go")
				fresh.End()
			}
		} else {
			d.activateTab(from, from.activeTab())
			d.invalidateRender(from, ac, true, "picker.go")
		}
	} else {
		d.firstPaintForTransition(transition.published)
	}
	return nil
}

// resolveNamedLifecycleTargetLocked chooses exactly one current representation
// of a named lifecycle. Caller holds d.mu. Matching the name and lifecycle
// identity here closes the lookup-to-handoff window for palette targets.
func (d *Daemon) resolveNamedLifecycleTargetLocked(target picker.Target) (*session, stoppedSession, bool, bool) {
	if target.Name == "" {
		return nil, stoppedSession{}, false, false
	}
	if active := d.findByNameLocked(target.Name); active != nil {
		active.mu.Lock()
		matches := targetMatchesLifecycle(target, active.name, active.createdAt)
		active.mu.Unlock()
		return active, stoppedSession{}, false, matches
	}
	stopped, ok := d.stopped[target.Name]
	if !ok || stopped.purging || !targetMatchesLifecycle(target, stopped.name, stopped.createdAt) {
		return nil, stoppedSession{}, false, false
	}
	return nil, stopped, true, true
}

// switchToActiveTargetLocked commits an active target handoff through the
// centralized attachment transition. Caller holds d.mu.
func (d *Daemon) switchToActiveTargetLocked(from *session, ac *attachedClient, targetSess *session, target picker.Target, guard sessionHandoffGuard, sourceToken *attachmentRoleToken, action string) (*session, attachmentTransitionResult, bool) {
	if d.sessions[target.Session] != targetSess {
		return nil, attachmentTransitionResult{}, false
	}
	if targetSess == from {
		if sourceToken == nil {
			targetSess.mu.Lock()
			defer targetSess.mu.Unlock()
			if targetSess.client != ac || !targetMatchesLifecycle(target, targetSess.name, targetSess.createdAt) || target.TabIndex < 0 || target.TabIndex >= len(targetSess.tabs) {
				return nil, attachmentTransitionResult{}, false
			}
			targetSess.active = target.TabIndex
			return targetSess, attachmentTransitionResult{}, true
		}
		d.mu.Unlock()
		transition, err := d.transitionAttachment(attachmentTransitionRequest{
			source: from, target: targetSess, next: ac,
			expectedRole: attachmentActive, targetRole: attachmentActive,
			expectedTransport: sourceToken.transport, sourceToken: sourceToken, action: action,
			activateTargetTab: true, targetTabIndex: target.TabIndex, preserveRole: true,
		})
		d.mu.Lock()
		return targetSess, transition, err == nil
	}

	unlock := lockAttachmentSessions(from, targetSess)
	if from.client != ac || !targetMatchesLifecycle(target, targetSess.name, targetSess.createdAt) {
		unlock()
		return nil, attachmentTransitionResult{}, false
	}
	if guard.expectedSource != nil {
		if from.active < 0 || from.active >= len(from.tabs) || from.tabs[from.active] != guard.expectedSource {
			unlock()
			return nil, attachmentTransitionResult{}, false
		}
		guard.expectedSource.mu.Lock()
		sourceHasFloating := guard.expectedSource.floating.state == floatingVisible
		guard.expectedSource.mu.Unlock()
		if sourceHasFloating {
			unlock()
			return nil, attachmentTransitionResult{}, false
		}
	}
	unlock()

	// Release d.mu before the gate freeze/drain. transitionAttachment reacquires
	// d.mu and revalidates both registered lifecycles for publication.
	d.mu.Unlock()
	expectedTransport := ac.transportSnapshot()
	if sourceToken != nil {
		expectedTransport = sourceToken.transport
	}
	transition, err := d.transitionAttachment(attachmentTransitionRequest{
		source:                from,
		target:                targetSess,
		next:                  ac,
		expectedRole:          attachmentActive,
		targetRole:            attachmentActive,
		expectedTransport:     expectedTransport,
		sourceToken:           sourceToken,
		action:                action,
		expectedSourceTab:     guard.expectedSource,
		activateTargetTab:     target.TabIndex >= 0,
		targetTabIndex:        target.TabIndex,
		copySourceEnvironment: true,
		ready:                 true,
	})
	d.mu.Lock()
	if err != nil {
		return nil, attachmentTransitionResult{}, false
	}
	d.touchMRU(targetSess)
	ac.recordPreviousSession(from)
	return targetSess, transition, true
}

// resumeStoppedAndSwitchLocked creates the stopped representation and commits
// the handoff while d.mu is held. Creation failure leaves the source client
// and stopped record untouched.
func (d *Daemon) resumeStoppedAndSwitchLocked(from *session, ac *attachedClient, target picker.Target, stopped stoppedSession, sourceToken *attachmentRoleToken, guard sessionHandoffGuard, action string) (*session, attachmentTransitionResult, bool, error) {
	if stopped.record.Name != "" && stopped.state == ports.SessionBroken {
		return nil, attachmentTransitionResult{}, false, &protoErr{ports.ErrInternal, "session durable state is broken: " + target.Name}
	}

	if sourceToken != nil {
		var targetSess *session
		d.mu.Unlock()
		transition, err := d.transitionAttachment(attachmentTransitionRequest{
			source: from, next: ac, expectedRole: attachmentActive, targetRole: attachmentActive,
			expectedTransport: sourceToken.transport, sourceToken: sourceToken, action: action,
			expectedSourceTab: guard.expectedSource, copySourceEnvironment: true, ready: true,
			createTargetLocked: func() (*session, error) {
				current, ok := d.stopped[target.Name]
				if !ok || current.purging || current.createdAt != stopped.createdAt || !targetMatchesLifecycle(target, current.name, current.createdAt) {
					return nil, errAttachmentTransition
				}
				from.mu.Lock()
				cwd, term, env := d.dirOrHome(current.cwd), from.terminal, copyEnvironment(from.env)
				from.mu.Unlock()
				created, createErr := d.createSessionLocked(target.Name, false, cwd, ac.size, term, env, current.tabNames)
				targetSess = created
				return created, createErr
			},
		})
		d.mu.Lock()
		if err != nil {
			if targetSess != nil {
				d.mu.Unlock()
				_ = d.killSession(targetSess, ports.ReasonSessionKilled, true)
				d.mu.Lock()
			}
			return targetSess, attachmentTransitionResult{}, false, err
		}
		d.touchMRU(targetSess)
		ac.recordPreviousSession(from)
		return targetSess, transition, true, nil
	}

	from.mu.Lock()
	if from.client != ac {
		from.mu.Unlock()
		return nil, attachmentTransitionResult{}, false, nil
	}
	term := from.terminal
	env := copyEnvironment(from.env)
	from.mu.Unlock()
	cwd := d.dirOrHome(stopped.cwd)
	targetSess, err := d.createSessionLocked(target.Name, false, cwd, ac.size, term, env, stopped.tabNames)
	if err != nil {
		d.log.Warn("resuming stopped session failed", "err", err, "session", target.Name)
		return nil, attachmentTransitionResult{}, false, err
	}

	d.mu.Unlock()
	transition, err := d.transitionAttachment(attachmentTransitionRequest{
		source: from, target: targetSess, next: ac,
		expectedRole: attachmentActive, targetRole: attachmentActive,
		expectedTransport: ac.transportSnapshot(), ready: true,
	})
	d.mu.Lock()
	if err != nil {
		return targetSess, attachmentTransitionResult{}, false, err
	}
	d.touchMRU(targetSess)
	ac.recordPreviousSession(from)
	return targetSess, transition, true, nil
}

func targetMatchesLifecycle(target picker.Target, name string, createdAt int64) bool {
	return target.ExpectedCreatedAt == nil || (target.Name == name && *target.ExpectedCreatedAt == createdAt)
}

// stealClientForTarget is retained for direct-ID callers and tests. Named
// targets must use switchToTarget so resolution and commit share d.mu.
func (d *Daemon) stealClientForTarget(from *session, ac *attachedClient, targetSess *session, target picker.Target) *attachedClient {
	d.mu.Lock()
	_, transition, switched := d.switchToActiveTargetLocked(from, ac, targetSess, target, sessionHandoffGuard{}, nil, "")
	d.mu.Unlock()
	if !switched {
		return nil
	}
	d.deferAttachmentTransitionCleanups(transition)
	return transition.displaced.ac
}

// resumeStoppedAndSwitch is retained for direct callers and tests. It resolves
// its stopped target and commits creation under one d.mu critical section.
func (d *Daemon) resumeStoppedAndSwitch(from *session, ac *attachedClient, target picker.Target) bool {
	d.mu.Lock()
	stopped, ok := d.stopped[target.Name]
	var (
		transition attachmentTransitionResult
		switched   bool
	)
	if ok && !stopped.purging && targetMatchesLifecycle(target, stopped.name, stopped.createdAt) {
		_, transition, switched, _ = d.resumeStoppedAndSwitchLocked(from, ac, target, stopped, nil, sessionHandoffGuard{}, "")
	}
	d.mu.Unlock()
	if !switched {
		d.invalidateRender(from, ac, true, "picker.go")
		return false
	}
	d.deferAttachmentTransitionCleanups(transition)
	d.firstPaintForTransition(transition.published)
	return true
}

func (d *Daemon) killPickerTargetForRole(target picker.Target, token attachmentRoleToken) error {
	if token.ac == nil {
		return d.killPickerTarget(target)
	}
	if target.Stopped {
		token.effect.bindActionEnd(d, "picker-delete")
		token.effect.End()
		frozen := freezeRoleEffectGates(token.ac)
		defer frozen.unfreeze()
		if !d.sourceRoleTokenCurrentFrozen(token) {
			return nil
		}
		return d.killPickerTarget(target)
	}
	d.mu.Lock()
	targetSess := d.sessions[target.Session]
	d.mu.Unlock()
	if targetSess == nil {
		return nil
	}
	return d.killSessionForRole(targetSess, ports.ReasonSessionKilled, true, token, "picker-delete")
}

func (d *Daemon) killPickerTarget(target picker.Target) error {
	if target.Stopped {
		if err := d.retryStoppedPurge(target.Name); err != nil {
			d.log.Warn("deleting persisted stopped session failed", "err", err, "session", target.Name)
			return domain.UserErr(domain.NoticePersistDelete, "couldn't delete stopped session", err)
		}
		return nil
	}
	d.mu.Lock()
	targetSess := d.sessions[target.Session]
	d.mu.Unlock()
	if targetSess != nil {
		return d.killSession(targetSess, ports.ReasonSessionKilled, true)
	}
	return nil
}

func snapshotPickerPreview(tb *tab) picker.Preview {
	if tb == nil {
		return picker.Preview{}
	}
	tb.mu.Lock()
	layoutSnap := solveTabLayoutLocked(tb)
	if !layoutSnap.ok {
		p := tb.focusedPane()
		if p == nil {
			tb.mu.Unlock()
			return picker.Preview{}
		}
		p.mu.Lock()
		preview := pickerPreviewFromLockedPane(p)
		p.mu.Unlock()
		tb.mu.Unlock()
		return preview
	}
	state := capturedRenderState{layout: capturedTabLayout{
		area: layoutSnap.area, focus: layoutSnap.focus,
		placements:  append([]layout.Placement(nil), layoutSnap.placements...),
		dividers:    append([]layout.Divider(nil), layoutSnap.dividers...),
		fingerprint: layoutSnap.fingerprint, valid: true,
	}}
	for _, placement := range layoutSnap.placements {
		p := tb.panes[placement.ID]
		if p == nil {
			continue
		}
		visible := placement.Content
		if placement.Collapsed {
			visible = domain.Rect{}
		}
		p.mu.Lock()
		captured := capturePaneRenderStateLocked(p, visible)
		p.mu.Unlock()
		captured.placement = placement
		captured.focused = placement.ID == layoutSnap.focus
		state.panes = append(state.panes, captured)
	}
	tb.mu.Unlock()
	return pickerPreviewFromCapturedRender(state)
}

func pickerPreviewFromCapturedRender(state capturedRenderState) picker.Preview {
	composed := composeFrame(state, composeCacheInput{}, composeCacheInput{}).frame
	if composed.Height < 2 {
		return pickerPreviewFromFrame(composed)
	}
	rows := make([][]renderer.Cell, composed.Height-2)
	for y := range rows {
		rows[y] = append([]renderer.Cell(nil), composed.Row(y+1)...)
	}
	return picker.Preview{Rows: rows, Width: composed.Width, Height: len(rows)}
}

func pickerPreviewFromLockedPane(p *pane) picker.Preview {
	return pickerPreviewFromFrame(p.screen.Frame)
}

func pickerPreviewFromFrame(frame renderer.Frame) picker.Preview {
	rows := make([][]renderer.Cell, frame.Height)
	for y := range rows {
		rows[y] = append([]renderer.Cell(nil), frame.Row(y)...)
	}
	return picker.Preview{Rows: rows, Width: frame.Width, Height: frame.Height}
}

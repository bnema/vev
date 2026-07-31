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
func (d *Daemon) enterPicker(sess attachmentSession, ac *attachedClient) {
	model := d.newPickerModel(sess, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{})
	d.publishPicker(sess, ac, model, pickerNavigate, moveSourceLocator{})
}

func (d *Daemon) publishPicker(sess attachmentSession, ac *attachedClient, model *picker.Model, intent pickerIntent, source moveSourceLocator) {
	rt := ac.overlays
	rt.pickerMu.Lock()
	previous, previousGeneration := rt.pickerPreviewSession, rt.pickerPreviewGeneration
	rt.pickerPreviewGeneration++
	rt.pickerPreviewSession = nil
	rt.pickerPreview = nil
	rt.picker = model
	rt.pickerGeneration++
	instance := remotePickerInstance{ac: ac, generation: rt.pickerGeneration, model: model}
	rt.pickerTitle = pickerTitle(pickerSortMode(d.pickerSort.Load()))
	rt.pickerIntent = intent
	rt.pickerSource = source
	rt.pickerPending = nil
	rt.pickerESC.stop()
	rt.pickerMu.Unlock()
	d.teardownPreviewSubscription(ac, previous, previousGeneration)
	d.registerPreviewForSelection(ac)
	d.invalidateRender(sess, ac, true, "picker.go")
	if rt.beforeRemotePickerRegistration != nil {
		rt.beforeRemotePickerRegistration()
	}
	d.remotePickerOpened(instance)
}

// pickerViews captures one canonical lifecycle/tab snapshot. It intentionally
// knows nothing about picker intent; picker.New owns all destination policy.
func (d *Daemon) pickerViews(cur attachmentSession) ([]picker.SessionView, picker.SourceFilter) {
	d.mu.Lock()
	sessions := make([]attachmentSession, 0, len(d.sessions))
	proxies := make(map[domain.SessionID]*proxySession)
	for id, entry := range d.sessions {
		sessions = append(sessions, entry)
		if proxy, ok := entry.(*proxySession); ok {
			proxies[id] = proxy
		}
	}
	stopped := make([]stoppedSession, 0, len(d.stopped))
	for _, s := range d.stopped {
		if !s.purging {
			stopped = append(stopped, s)
		}
	}
	d.mu.Unlock()

	for _, entry := range sessions {
		if local, ok := localSession(entry); ok {
			d.refreshSessionFocusedTitles(local)
		}
	}
	opts := viewOptions{tabDetails: true, focusedTitles: true, terminalTitle: d.currentTabsConfig().TerminalTitle}

	type liveSnapshot struct {
		entry attachmentSession
		view  sessionView
	}
	// One snapshot per live session: sorting and view building read the same
	// capture, so comparators cannot observe a concurrent touchMRU or
	// renameSession mid-sort. Proxy and remote-catalog locks are released before
	// sorting and picker-row construction.
	live := make([]liveSnapshot, 0, len(sessions))
	var current picker.SourceFilter
	for _, entry := range sessions {
		snap := entry.snapshotView(opts)
		if entry == cur {
			if proxy, ok := entry.(*proxySession); ok {
				key := proxy.key
				current = picker.SourceFilter{Session: snap.id, Incarnation: snap.incarnation, RemoteKey: &key}
			} else if snap.active >= 0 && snap.active < len(snap.tabs) {
				current = picker.SourceFilter{Session: snap.id, Incarnation: snap.incarnation, TabID: snap.tabs[snap.active].id}
			}
		}
		live = append(live, liveSnapshot{entry: entry, view: snap})
	}
	sort.Slice(live, func(i, j int) bool {
		if live[i].view.mruAt != live[j].view.mruAt {
			return live[i].view.mruAt > live[j].view.mruAt
		}
		if live[i].view.name != live[j].view.name {
			return live[i].view.name < live[j].view.name
		}
		return live[i].view.id < live[j].view.id
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
		sort.SliceStable(live, func(i, j int) bool {
			return !live[i].view.ephemeral && live[j].view.ephemeral
		})
	}

	catalog := d.remotePickerCatalogSnapshot()
	sortRemotePickerCatalog(catalog, d.remotePickerHostRanks())

	catalogRows := 0
	for _, host := range catalog {
		catalogRows += len(host.entry.Sessions)
		if len(host.entry.Sessions) == 0 && (host.status == remoteHostUnreachable || host.status == remoteHostVersionMismatch) {
			catalogRows++
		}
	}
	views := make([]picker.SessionView, 0, len(live)+len(stopped)+catalogRows)
	for _, item := range live {
		if proxy, ok := item.entry.(*proxySession); ok {
			views = append(views, remoteProxyPickerView(proxy.key, item.view))
			continue
		}
		views = append(views, item.view.pickerView())
	}
	for _, host := range catalog {
		for _, session := range host.entry.Sessions {
			key := domain.RemoteSessionKey{Host: host.entry.Host, Name: session.Name}
			if key.Validate() != nil {
				continue
			}
			if proxy, ok := proxies[key.ID()]; ok && proxy.key == key {
				continue
			}
			views = append(views, remotePickerView(key, session, host.status, host.entry.FetchedAt))
		}
		if len(host.entry.Sessions) == 0 && (host.status == remoteHostUnreachable || host.status == remoteHostVersionMismatch) {
			views = append(views, remotePickerHostView(host.entry.Host, host.status))
		}
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

func (d *Daemon) newPickerModel(cur attachmentSession, intent pickerIntent, source moveSourceLocator, current picker.SourceFilter) *picker.Model {
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
	var previewGeneration uint64
	var instance remotePickerInstance
	return listInputState{
		pending:  &rt.pickerPending,
		esc:      &rt.pickerESC,
		moveUp:   rt.picker.Up,
		moveDown: rt.picker.Down,
		lock:     rt.pickerMu.Lock,
		unlock:   rt.pickerMu.Unlock,
		active:   func() bool { return rt.picker != nil },
		closeLocked: func() {
			instance = remotePickerInstance{ac: ac, generation: rt.pickerGeneration, model: rt.picker}
			rt.picker = nil
			rt.pickerIntent = pickerNavigate
			rt.pickerSource = moveSourceLocator{}
			previewGeneration = rt.pickerPreviewGeneration
		},
		afterClose: func() {
			d.clearPreviewGeneration(ac, previewGeneration)
			d.remotePickerClosed(instance)
			if sess := ac.currentAttachmentSession(); sess != nil {
				d.invalidateRender(sess, ac, true, "picker.go")
			}
		},
	}
}

func (d *Daemon) handlePickerInput(ac *attachedClient, data []byte, effects ...*roleEffectTicket) {
	sess := ac.currentAttachmentSession()
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
				d.reportAttachmentError(sess, err)
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
				d.reportAttachmentError(sess, err)
				d.invalidateRender(sess, ac, true, "picker.go")
				return
			}
			if len(effects) != 0 && effects[0] != nil {
				effects[0].End()
			}
			d.closePicker(ac)
			err := d.commitMovePickerSelection(intent, source, target)
			if err != nil {
				d.reportAttachmentError(sess, movePickerUserError(err))
				d.invalidateRender(sess, ac, true, "picker.go")
				return
			}
			d.invalidateRender(sess, ac, true, "picker.go")
			return
		}
		var err error
		if len(effects) != 0 && effects[0] != nil {
			err = d.switchToTargetForRole(effects[0].roleToken(), target, sessionHandoffGuard{closePicker: true}, "picker-select")
		} else if local, ok := localSession(sess); ok {
			d.closePicker(ac)
			err = d.switchToTarget(local, ac, target)
		} else {
			err = errAttachmentTransition
		}
		if errors.Is(err, errAttachmentTransition) {
			return
		}
		if err != nil {
			d.reportAttachmentError(sess, err)
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
	if targetSess == ac.currentAttachmentSession() {
		return
	}
	rc := d.attachCoordinator(targetSess, nil, nil, false)
	rc.subscribePreviewFor(ac, generation, func(renderWake) {
		if pickerPreviewCurrent(ac, targetSess, next, generation) {
			if owner := ac.currentAttachmentSession(); owner != nil {
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
	for _, entry := range d.sessions {
		sess, ok := localSession(entry)
		if !ok {
			continue
		}
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
			if sess := ac.currentAttachmentSession(); sess != nil {
				d.invalidateRender(sess, ac, true, "picker.go")
			}
		}
	}
}

func (d *Daemon) closePicker(ac *attachedClient) {
	d.closePickerIfCurrent(ac, nil, 0)
}

// closePickerIfCurrent closes only the observed picker lifecycle. A nonzero
// generation guards terminal parked cleanup even when a refresh replaced the
// model within that generation.
func (d *Daemon) closePickerIfCurrent(ac *attachedClient, model *picker.Model, generation uint64) bool {
	ac.overlays.pickerMu.Lock()
	if model != nil && (ac.overlays.picker != model || ac.overlays.pickerGeneration != generation) || model == nil && generation != 0 && ac.overlays.pickerGeneration != generation {
		ac.overlays.pickerMu.Unlock()
		return false
	}
	instance := remotePickerInstance{ac: ac, generation: ac.overlays.pickerGeneration, model: ac.overlays.picker}
	ac.overlays.picker = nil
	ac.overlays.pickerTitle = ""
	ac.overlays.pickerIntent = pickerNavigate
	ac.overlays.pickerSource = moveSourceLocator{}
	ac.overlays.pickerPending = nil
	ac.overlays.pickerESC.stop()
	previewGeneration := ac.overlays.pickerPreviewGeneration
	ac.overlays.pickerMu.Unlock()
	d.clearPreviewGeneration(ac, previewGeneration)
	if instance.model != nil {
		d.remotePickerClosed(instance)
	}
	return true
}

func pickerTargetsEqual(left, right picker.Target) bool {
	if left.Session != right.Session || left.Incarnation != right.Incarnation || left.Name != right.Name ||
		left.TabID != right.TabID || left.TabIndex != right.TabIndex || left.Stopped != right.Stopped ||
		!remoteKeysEqual(left.RemoteKey, right.RemoteKey) {
		return false
	}
	if left.ExpectedCreatedAt == nil || right.ExpectedCreatedAt == nil {
		return left.ExpectedCreatedAt == nil && right.ExpectedCreatedAt == nil
	}
	return *left.ExpectedCreatedAt == *right.ExpectedCreatedAt
}

func remoteKeysEqual(left, right *domain.RemoteSessionKey) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func (d *Daemon) sessionByID(id domain.SessionID) attachmentSession {
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
	return d.switchActiveTargetForRoleGuarded(token, target, sessionHandoffGuard{}, "jump-attention")
}

func pickerTargetLifecycleFence(target picker.Target) *attachmentLifecycleFence {
	fence := &attachmentLifecycleFence{}
	if target.ExpectedCreatedAt != nil {
		fence.name = target.Name
		fence.createdAt = *target.ExpectedCreatedAt
		fence.checkCreatedAt = true
	}
	if target.Incarnation != (domain.IncarnationID{}) {
		fence.incarnation = target.Incarnation
		fence.checkIncarnation = true
	}
	if target.TabID != "" {
		fence.tabID = target.TabID
		fence.tabIndex = target.TabIndex
		fence.checkTab = true
	}
	if !fence.checkCreatedAt && !fence.checkIncarnation && !fence.checkTab {
		return nil
	}
	return fence
}

func (d *Daemon) switchActiveTargetForRoleGuarded(token attachmentRoleToken, target picker.Target, guard sessionHandoffGuard, action string) error {
	if token.sess == nil || token.ac == nil || token.role != attachmentActive {
		return nil
	}
	d.mu.Lock()
	targetSess, ok := localSession(d.sessions[target.Session])
	d.mu.Unlock()
	if !ok || targetSess == token.sess {
		return nil
	}

	transition, err := d.transitionAttachment(attachmentTransitionRequest{
		source: token.sess, target: targetSess, next: token.ac,
		expectedRole: attachmentActive, targetRole: attachmentActive,
		expectedTransport: token.transport, sourceToken: &token, action: action,
		expectedTargetLifecycle: pickerTargetLifecycleFence(target),
		activateTargetTab:       true, targetTabIndex: target.TabIndex, copySourceEnvironment: true, ready: true,
	})
	if err != nil {
		// Losing the exact source role is a benign stale action, not a notice for
		// whichever attachment replaced the initiator.
		if !token.activeCurrent() {
			return nil
		}
		return domain.UserErr(domain.NoticeSessionUnavailable, "couldn't switch to that session", err)
	}
	if guard.closePicker {
		fresh, admitted := token.ac.beginRoleEffect(transition.published)
		if admitted {
			d.closePicker(token.ac)
			fresh.End()
		}
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
	if target.RemoteKey != nil {
		return d.switchToRemoteTargetForRole(token, target, *target.RemoteKey, guard, action)
	}
	if token.sess.isProxy() && !target.Stopped {
		return d.switchActiveTargetForRoleGuarded(token, target, guard, action)
	}
	sess, ok := localSession(token.sess)
	if !ok {
		return errAttachmentTransition
	}
	return d.switchToTargetGuardedForRole(sess, token.ac, target, guard, &token, action)
}

// switchToRemoteTargetForRole performs dialing after the initiating effect has
// ended and without role or architecture locks. transitionAttachment then
// freezes and revalidates the exact source role and exact structured-key proxy
// before publishing ownership.
func (d *Daemon) switchToRemoteTargetForRole(token attachmentRoleToken, target picker.Target, key domain.RemoteSessionKey, guard sessionHandoffGuard, action string) error {
	if key.Validate() != nil || target.Session != key.ID() || target.Stopped || target.Name != "" {
		return errAttachmentTransition
	}
	ctx := d.serveCtx
	if ctx == nil {
		ctx = context.Background()
	}
	// Only the caller that observed neither a published proxy nor an in-flight
	// constructor may own failure cleanup. Waiters and existing-proxy users must
	// never tear down the shared link when their source transition goes stale.
	d.mu.Lock()
	_, alreadyPublished := d.sessions[key.ID()]
	constructionInFlight := d.proxyConstructions[key] != nil
	d.mu.Unlock()
	ownsCandidate := !alreadyPublished && !constructionInFlight
	proxy, err := d.openProxySession(ctx, key, token.ac.size)
	if err != nil {
		if !token.activeCurrent() {
			return errAttachmentTransition
		}
		return domain.UserErr(domain.NoticeSessionUnavailable, "couldn't connect to that remote session", err)
	}
	if proxy == nil || proxy.key != key || proxy.id != key.ID() {
		return errAttachmentTransition
	}
	proxy.mu.Lock()
	candidateGeneration := proxy.generation
	proxy.mu.Unlock()

	transition, err := d.transitionAttachment(attachmentTransitionRequest{
		source: token.sess, target: proxy, next: token.ac,
		expectedRole: attachmentActive, targetRole: attachmentActive,
		expectedTransport: token.transport, sourceToken: &token, action: action,
		preserveRole: proxy == token.sess, ready: true,
	})
	if err != nil {
		if ownsCandidate {
			d.discardUnattachedProxyGeneration(proxy, candidateGeneration)
		}
		return errAttachmentTransition
	}
	if guard.closePicker {
		fresh, admitted := token.ac.beginRoleEffect(transition.published)
		if admitted {
			d.closePicker(token.ac)
			fresh.End()
		}
	}
	if proxy != token.sess {
		token.ac.recordPreviousSession(token.sess)
	}
	d.deferAttachmentTransitionCleanups(transition)
	if proxy == token.sess {
		d.invalidateRender(proxy, token.ac, true, "picker.go")
	} else {
		d.firstPaintForTransition(transition.published)
	}
	return nil
}

// discardUnattachedProxyGeneration removes only a transition-owned candidate
// that never acquired a client. It mirrors normal proxy teardown fencing but
// deliberately does not arm the five-minute warm lifetime for an attachment
// that was never published.
func (d *Daemon) discardUnattachedProxyGeneration(proxy *proxySession, generation uint64) bool {
	if d == nil || proxy == nil {
		return false
	}
	d.mu.Lock()
	if d.sessions[proxy.id] != proxy {
		d.mu.Unlock()
		return false
	}
	proxy.sessionCore.mu.Lock()
	if proxy.client != nil {
		proxy.sessionCore.mu.Unlock()
		d.mu.Unlock()
		return false
	}
	proxy.mu.Lock()
	if proxy.generation != generation || proxy.expired || proxy.warm != nil {
		proxy.mu.Unlock()
		proxy.sessionCore.mu.Unlock()
		d.mu.Unlock()
		return false
	}
	proxy.generation++
	proxy.expired = true
	cancel := proxy.cancel
	transport := proxy.transport
	coordinator := proxy.coordinator.Load()
	proxy.mu.Unlock()
	if !d.unregisterSessionLocked(proxy) {
		proxy.sessionCore.mu.Unlock()
		d.mu.Unlock()
		return false
	}
	proxy.sessionCore.mu.Unlock()
	empty := len(d.sessions) == 0
	if empty {
		d.closing = true
	}
	d.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if transport != nil {
		_ = transport.Close()
	}
	if coordinator != nil {
		coordinator.beginSessionTeardown().finish()
		coordinator.waitForTimerWorkers()
	}
	proxy.finish()
	if empty {
		d.doneOnce.Do(func() { close(d.done) })
	} else {
		d.refreshRemoteOpenPickers()
	}
	return true
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
	} else if active, ok := localSession(d.sessions[target.Session]); ok {
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
	if target.RemoteKey != nil {
		d.enterRemoteKillConfirmation(token.sess, token.ac, target)
		return nil
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
	targetSess, ok := localSession(d.sessions[target.Session])
	d.mu.Unlock()
	if !ok {
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
	targetSess, ok := localSession(d.sessions[target.Session])
	d.mu.Unlock()
	if ok {
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

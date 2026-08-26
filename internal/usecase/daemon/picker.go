// Package daemon holds vev's server-side session multiplexer use case.
package daemon

import (
	"context"
	"errors"
	"sort"
	"time"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/picker"
	"github.com/bnema/vev/internal/usecase/ui"
)

var pickerModal = ui.Modal{WidthPct: 80, HeightPct: 80, MinWidth: 24, MinHeight: 8, Title: " Sessions ", Anchor: domain.AnchorCenter, Margins: ui.Margins{}}

// pickerSortMode orders the picker's live sessions. It lives for the daemon's
// lifetime only and is never persisted.
type pickerSortMode uint32

const (
	pickerSortRecent pickerSortMode = iota
	pickerSortGrouped

	remotePickerPreviewDebounce = 80 * time.Millisecond
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
	model := d.newPickerModel(sess, ac, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{})
	d.publishPicker(sess, ac, model, pickerNavigate, moveSourceLocator{})
}

func (d *Daemon) publishPicker(sess *session, ac *attachedClient, model *picker.Model, intent pickerIntent, source moveSourceLocator) {
	rt := ac.overlays
	rt.pickerMu.Lock()
	previous, previousGeneration := rt.pickerPreviewSession, rt.pickerPreviewGeneration
	rt.pickerPreviewGeneration++
	if rt.pickerRemotePreviewCancel != nil {
		rt.pickerRemotePreviewCancel()
		rt.pickerRemotePreviewCancel = nil
	}
	rt.pickerPreviewSession = nil
	rt.pickerRemotePreview = picker.Preview{}
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
	d.remoteDiscoveryOpened(instance.discoveryInstance())
}

// pickerViews captures one canonical lifecycle/tab snapshot. It intentionally
// knows nothing about picker intent; picker.New owns all destination policy.
func (d *Daemon) pickerViews(cur *session, ac *attachedClient) ([]picker.SessionView, picker.SourceFilter) {
	d.mu.Lock()
	sessions := make([]*session, 0, len(d.sessions))
	for _, entry := range d.sessions {
		if entry == nil {
			continue
		}
		sessions = append(sessions, entry)
	}
	stopped := make([]inactiveSession, 0, len(d.inactive))
	for _, s := range d.inactive {
		if s.visible() {
			stopped = append(stopped, s)
		}
	}
	d.mu.Unlock()

	for _, entry := range sessions {
		if entry != nil {
			d.refreshSessionFocusedTitles(entry)
		}
	}
	opts := viewOptions{tabDetails: true, focusedTitles: true, terminalTitle: d.currentTabsConfig().TerminalTitle}

	type liveSnapshot struct {
		entry *session
		view  sessionView
	}
	// One snapshot per live session: sorting and view building read the same
	// capture, so comparators cannot observe a concurrent touchMRU or
	// renameSession mid-sort. Remote-catalog locks are released before sorting
	// and picker-row construction.
	live := make([]liveSnapshot, 0, len(sessions))
	var current picker.SourceFilter
	for _, entry := range sessions {
		if entry == cur && ac != nil {
			entry.repairAttachmentView(ac)
		}
		snap := entry.snapshotView(opts)
		if entry == cur {
			if ac != nil {
				view := ac.viewSnapshot()
				if view.tabID != "" {
					current = picker.SourceFilter{Session: snap.id, Incarnation: snap.incarnation, TabID: view.tabID}
				}
			} else if len(snap.tabs) > 0 {
				current = picker.SourceFilter{Session: snap.id, Incarnation: snap.incarnation, TabID: snap.tabs[snap.defaultTab].id}
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

	catalog := d.remoteCatalogSnapshot()
	sortRemoteCatalog(catalog, d.remoteHostRanks())

	catalogRows := 0
	for _, host := range catalog {
		catalogRows += len(host.entry.Sessions)
		if len(host.entry.Sessions) == 0 && (host.status == remoteHostUnreachable || host.status == remoteHostVersionMismatch || host.status == remoteHostMalformed) {
			catalogRows++
		}
	}
	views := make([]picker.SessionView, 0, len(live)+len(stopped)+catalogRows)
	for _, item := range live {
		views = append(views, item.view.pickerView())
	}
	for _, host := range catalog {
		for _, session := range host.entry.Sessions {
			key := domain.RemoteSessionKey{Host: host.entry.Host, Name: session.Name}
			if key.Validate() != nil {
				continue
			}
			views = append(views, remotePickerView(key, session, host.status, host.entry.FetchedAt))
		}
		if len(host.entry.Sessions) == 0 && (host.status == remoteHostUnreachable || host.status == remoteHostVersionMismatch || host.status == remoteHostMalformed) {
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

func (d *Daemon) newPickerModel(cur *session, ac *attachedClient, intent pickerIntent, source moveSourceLocator, current picker.SourceFilter) *picker.Model {
	views, attachedCurrent := d.pickerViews(cur, ac)
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

func (d *Daemon) notifyRemotePickerUnavailable(sess *session, target picker.Target) {
	reason := target.UnavailableReason
	if reason == "" {
		reason = "identity_changed"
	}
	message := "Remote session unavailable"
	if target.RemoteTarget != nil {
		message += ": " + domain.RemoteSessionDisplay(target.RemoteTarget.SessionName, target.RemoteTarget.DisplayOrigin)
	} else if target.RemoteHost != "" {
		message += ": " + target.RemoteHost
	}
	message += " — " + remotePickerReasonText(reason)
	d.notify(sess, domain.NoticeWarn, domain.NoticeSessionUnavailable, message, nil)
}

func remotePickerReasonText(reason string) string {
	switch reason {
	case "catalog_stale":
		return "catalog stale"
	case "host_unreachable":
		return "host unreachable"
	case "version_mismatch":
		return "version mismatch"
	case "session_stopped":
		return "session stopped"
	case "session_broken":
		return "session broken"
	case "malformed":
		return "catalog malformed"
	case "refreshing":
		return "refreshing"
	case "identity_changed":
		return "session identity changed"
	default:
		return "unavailable"
	}
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
			d.remoteDiscoveryClosed(instance.discoveryInstance())
			if sess := ac.currentAttachmentSession(); sess != nil {
				d.invalidateRender(sess, ac, true, "picker.go")
			}
		},
	}
}

func (d *Daemon) handlePickerInput(ac *attachedClient, data []byte, effects ...*attachmentEffectTicket) {
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
	var cursor picker.Target
	var ok bool
	var cursorOK bool
	var intent pickerIntent
	var source moveSourceLocator
	// prevIdx is the row the victim occupied; -1 means "no post-delete hint".
	prevIdx := -1
	if result.action == 'x' || result.action == '\r' || result.action == '\n' {
		target, ok = rt.picker.Selected()
		cursor, cursorOK = rt.picker.Cursor()
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
	if (result.action == '\r' || result.action == '\n') && !ok && cursorOK && (cursor.RemoteTarget != nil || cursor.RemoteHost != "") {
		d.notifyRemotePickerUnavailable(sess, cursor)
		d.invalidateRender(sess, ac, true, "picker.go")
		return
	}
	if result.action == 'x' {
		var effect *attachmentEffectTicket
		if len(effects) != 0 {
			effect = effects[0]
		}
		if ok {
			var err error
			if effect != nil {
				effect.bindActionEnd(d, "picker-delete")
				err = d.killPickerTargetForAttachment(target, effect.connectionToken())
			} else {
				err = d.killPickerTarget(target)
			}
			if err != nil {
				d.reportAttachmentError(sess, err)
			}
		}
		if effect != nil && effect.ended.Load() {
			fresh, admitted := ac.beginAttachmentEffect(effect.token)
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
	backNavigationSent := false
	if result.exit && !committing {
		back := ac.startupOverlay == ports.StartupOverlaySessionPicker && ac.navigationCapabilities&ports.NavigationCapabilityBack != 0
		if back {
			var token attachmentConnectionToken
			if len(effects) != 0 && effects[0] != nil {
				token = effects[0].connectionToken()
			} else {
				var effect *attachmentEffectTicket
				var admitted bool
				token, effect, admitted = ac.beginCurrentAttachmentEffect(sess, ac.transport())
				if !admitted {
					d.invalidateRender(sess, ac, true, "picker.go")
					return
				}
				defer effect.End()
			}
			if err := d.sendNavigationActionForAttachment(token, ports.NavigationBack); err != nil {
				d.reportAttachmentError(sess, err)
				d.invalidateRender(sess, ac, true, "picker.go")
				return
			}
			backNavigationSent = true
		}
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
			err = d.switchToTargetForAttachment(effects[0].connectionToken(), target, sessionHandoffGuard{closePicker: true, allowSamePeer: true}, "picker-select")
		} else {
			d.closePicker(ac)
			err = d.switchToTarget(sess, ac, target)
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
	if (result.exit || result.changed) && !backNavigationSent {
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
	if ac.overlays.pickerRemotePreviewCancel != nil {
		ac.overlays.pickerRemotePreviewCancel()
		ac.overlays.pickerRemotePreviewCancel = nil
	}
	ac.overlays.pickerPreviewSession = nil
	ac.overlays.pickerRemotePreview = picker.Preview{}
	ac.overlays.pickerPreview = nil
	ac.overlays.pickerMu.Unlock()
	d.teardownPreviewSubscription(ac, previous, previousGeneration)
	if !ok {
		return
	}
	if target.RemoteTarget != nil {
		d.startRemotePickerPreview(ac, target, generation)
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

func (d *Daemon) startRemotePickerPreview(ac *attachedClient, target picker.Target, generation uint64) {
	if d == nil || ac == nil || target.RemoteTarget == nil || (d.remotePreviewClient == nil && !target.RemoteTarget.Stopped) {
		return
	}
	width, height := remotePickerPreviewSize(ac.sizeSnapshot())
	if width == 0 || height == 0 {
		return
	}
	ctx := d.serveCtx
	if ctx == nil {
		ctx = context.Background()
	}
	previewCtx, cancel := context.WithCancel(ctx)
	ac.overlays.pickerMu.Lock()
	if ac.overlays.picker == nil || ac.overlays.pickerPreviewGeneration != generation {
		ac.overlays.pickerMu.Unlock()
		cancel()
		return
	}
	ac.overlays.pickerRemotePreviewCancel = cancel
	remoteTarget := *target.RemoteTarget
	message := "loading remote preview…"
	if remoteTarget.Stopped {
		message = "stopped session — preview unavailable"
	}
	ac.overlays.pickerRemotePreview = staticRemotePickerPreview(width, height, message)
	if remoteTarget.Stopped {
		ac.overlays.pickerRemotePreviewCancel = nil
		ac.overlays.pickerMu.Unlock()
		cancel()
		if sess := ac.currentAttachmentSession(); sess != nil {
			d.invalidateRender(sess, ac, false, "remote picker preview")
		}
		return
	}
	ac.overlays.pickerMu.Unlock()
	go func() {
		defer cancel()
		previewClock := d.clock
		if previewClock == nil {
			previewClock = systemClock{}
		}
		debounce := previewClock.NewTimer(remotePickerPreviewDebounce)
		defer debounce.Stop()
		select {
		case <-debounce.C():
		case <-previewCtx.Done():
			return
		}
		preview, err := d.fetchRemotePreview(previewCtx, remoteTarget, width, height)
		if err != nil {
			ac.overlays.pickerMu.Lock()
			matching := false
			if ac.overlays.pickerPreviewGeneration == generation && ac.overlays.picker != nil {
				selected, stillSelected := ac.overlays.picker.Selected()
				matching = stillSelected && pickerTargetsEqual(selected, target)
				if matching {
					ac.overlays.pickerRemotePreview = staticRemotePickerPreview(width, height, "remote preview unavailable")
				}
			}
			ac.overlays.pickerMu.Unlock()
			if matching {
				if sess := ac.currentAttachmentSession(); sess != nil {
					d.invalidateRender(sess, ac, false, "remote picker preview")
				}
			}
			return
		}
		if refreshed, refreshErr := d.awaitRemotePreviewRefresh(previewCtx, remoteTarget, width, height); refreshErr == nil {
			preview = refreshed
		} else if previewCtx.Err() != nil {
			return
		}
		rows := preview.FrameRows()
		candidate := picker.Preview{Rows: rows, Width: int(preview.Width), Height: int(preview.Height)}
		ac.overlays.pickerMu.Lock()
		if ac.overlays.pickerPreviewGeneration != generation || ac.overlays.picker == nil {
			ac.overlays.pickerMu.Unlock()
			return
		}
		selected, stillSelected := ac.overlays.picker.Selected()
		if !stillSelected || !pickerTargetsEqual(selected, target) {
			ac.overlays.pickerMu.Unlock()
			return
		}
		ac.overlays.pickerRemotePreview = candidate
		ac.overlays.pickerRemotePreviewCancel = nil
		ac.overlays.pickerMu.Unlock()
		if sess := ac.currentAttachmentSession(); sess != nil {
			d.invalidateRender(sess, ac, false, "remote picker preview")
		}
	}()
}

func remotePickerPreviewSize(size domain.Size) (uint16, uint16) {
	layout := picker.ChooseLayout(size)
	width, height := layout.Preview.Width, layout.Preview.Height
	if width <= 0 || height <= 0 {
		return 0, 0
	}
	return uint16(min(width, ports.RemotePreviewMaxWidth)), uint16(min(height, ports.RemotePreviewMaxHeight))
}

func staticRemotePickerPreview(width, height uint16, message string) picker.Preview {
	if width == 0 || height == 0 {
		return picker.Preview{}
	}
	rows := [][]renderer.Cell{make([]renderer.Cell, int(width))}
	style := renderer.DefaultStyle()
	for x := range rows[0] {
		rows[0][x] = renderer.Cell{Rune: ' ', Style: style}
	}
	for x, r := range []rune(message) {
		if x >= int(width) {
			break
		}
		rows[0][x] = renderer.Cell{Rune: r, Style: style}
	}
	return picker.Preview{Rows: rows, Width: int(width), Height: len(rows)}
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
	if ac.overlays.pickerRemotePreviewCancel != nil {
		ac.overlays.pickerRemotePreviewCancel()
		ac.overlays.pickerRemotePreviewCancel = nil
	}
	ac.overlays.pickerPreviewSession = nil
	ac.overlays.pickerRemotePreview = picker.Preview{}
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
		if sess == nil {
			continue
		}
		sess.mu.Lock()
		clients = append(clients, sess.snapshotAttachmentsLocked()...)
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
	if ac == nil || ac.overlays == nil {
		return false
	}
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
		d.remoteDiscoveryClosed(instance.discoveryInstance())
	}
	return true
}

func pickerTargetsEqual(left, right picker.Target) bool {
	if left.Session != right.Session || left.Incarnation != right.Incarnation || left.Name != right.Name ||
		left.RemoteHost != right.RemoteHost || left.TabID != right.TabID || left.TabIndex != right.TabIndex || left.Stopped != right.Stopped ||
		left.UnavailableReason != right.UnavailableReason || !remoteKeysEqual(left.RemoteKey, right.RemoteKey) ||
		!remoteTargetsEqual(left.RemoteTarget, right.RemoteTarget) {
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

func remoteTargetsEqual(left, right *domain.RemoteSessionTarget) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func (d *Daemon) sessionByID(id domain.SessionID) *session {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.sessions[id]
}

// sessionHandoffGuard carries explicit capabilities and source constraints for
// one active-session handoff. Its zero value keeps daemon-owned transitions on
// their established path.
type sessionHandoffGuard struct {
	expectedSource *tab
	closePicker    bool
	allowSamePeer  bool
}

// switchActiveTargetForAttachment hands a frame-bound navigation request to the
// centralized transition. The transition releases admission at its freeze seam
// and revalidates the exact initiating token before changing target focus or
// attachment ownership.
func (d *Daemon) switchActiveTargetForAttachment(token attachmentConnectionToken, target picker.Target) error {
	return d.switchActiveTargetForAttachmentGuarded(token, target, sessionHandoffGuard{}, "jump-attention")
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

func (d *Daemon) switchActiveTargetForAttachmentGuarded(token attachmentConnectionToken, target picker.Target, guard sessionHandoffGuard, action string) error {
	if token.sess == nil || token.ac == nil {
		return nil
	}
	d.mu.Lock()
	targetSess := d.sessions[target.Session]
	d.mu.Unlock()
	if targetSess == nil {
		if !token.attachmentCurrent() {
			return nil
		}
		d.invalidateRender(token.sess, token.ac, true, "picker.go")
		return domain.UserErr(domain.NoticeSessionUnavailable, "couldn't switch to that session", nil)
	}
	if targetSess == token.sess {
		return nil
	}

	transition, err := d.transitionAttachment(attachmentTransitionRequest{
		source: token.sess, target: targetSess, next: token.ac,

		expectedTransport: token.transport, sourceToken: &token, action: action,
		expectedTargetLifecycle: pickerTargetLifecycleFence(target),
		activateTargetTab:       true, targetTabIndex: target.TabIndex, copySourceEnvironment: true, ready: true,
	})
	if err != nil {
		// Losing the exact source role is a benign stale action, not a notice for
		// whichever attachment replaced the initiator.
		if !token.attachmentCurrent() {
			return nil
		}
		return domain.UserErr(domain.NoticeSessionUnavailable, "couldn't switch to that session", err)
	}
	if guard.closePicker {
		fresh, admitted := token.ac.beginAttachmentEffect(transition.published)
		if admitted {
			d.closePicker(token.ac)
			fresh.End()
		}
	}
	d.touchMRU(targetSess)
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

// switchToTargetForAttachment is the sole client-originated navigation entry. The
// intent captures the exact initiating capability before admission is released;
// every active, stopped, and same-session target then uses transitionAttachment
// for frozen, atomic source-token preflight.
func (d *Daemon) switchToTargetForAttachment(token attachmentConnectionToken, target picker.Target, guard sessionHandoffGuard, action string) error {
	if token.sess == nil || token.ac == nil || token.effect == nil {
		return nil
	}
	if target.RemoteTarget != nil || target.RemoteKey != nil {
		return d.sendRemoteAttachTargetForAttachment(token, target, guard, action)
	}
	return d.sendLocalAttachTargetForAttachment(token, target, guard, action)
}

// sendLocalAttachTargetForAttachment offers an endpoint-empty, exact local
// target on the current authenticated connection. A current client confirms it
// with MsgSamePeerSwitchRequest; an interrupted or older client retains the
// existing close-and-redial fallback without any daemon-origin inference.
func (d *Daemon) sendLocalAttachTargetForAttachment(token attachmentConnectionToken, target picker.Target, guard sessionHandoffGuard, action string) error {
	d.mu.Lock()
	var targetSess *session
	var sessionName string
	var exactTarget *ports.ExactSessionTarget
	var matches bool
	if target.Name != "" {
		var stopped inactiveSession
		var stoppedTarget bool
		targetSess, stopped, stoppedTarget, matches = d.resolveNamedLifecycleTargetLocked(target)
		if stoppedTarget && matches {
			sessionName = stopped.name
			exactTarget = &ports.ExactSessionTarget{LifecycleID: stopped.incarnation, SessionName: sessionName}
		}
	} else {
		targetSess = d.sessions[target.Session]
		matches = targetSess != nil
	}
	if matches && targetSess != nil {
		targetSess.mu.Lock()
		sessionName = targetSess.name
		exactTarget = &ports.ExactSessionTarget{LifecycleID: targetSess.incarnation, SessionName: sessionName}
		targetSess.mu.Unlock()
	}
	d.mu.Unlock()
	if !matches || !token.attachmentCurrent() {
		return errAttachmentTransition
	}
	if targetSess != nil && target.TabID != "" {
		// An explicit tab row is a direct user choice, not route memory; retain
		// the established daemon-side transition so it opens exactly that tab.
		return d.switchToTargetGuardedForAttachment(token.sess, token.ac, target, guard, &token, action)
	}

	payload := ports.MarshalAttachTarget(ports.AttachTarget{
		Session:           sessionName,
		Intent:            ports.IntentAttach,
		ExactTarget:       exactTarget,
		EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned,
	})
	if payload == nil || exactTarget == nil {
		return errAttachmentTransition
	}
	// An active session row with no explicit non-default tab is on the
	// authenticated daemon and can use the client-confirmed same-peer path.
	// Explicit tab rows already take the direct transition path above, while
	// stopped sessions have no active target session and retain the fallback.
	samePeerEligible := guard.allowSamePeer && targetSess != nil && target.TabIndex <= 0
	if samePeerEligible {
		token.ac.offerSamePeerTarget(*exactTarget)
	}
	// A close-and-dial handoff leaves this daemon's Kitty namespace. A
	// same-peer transition keeps the attachment and its namespace; the target
	// scene diff deletes and replaces the source session's placements.
	if !samePeerEligible {
		if err := d.cleanupGraphicsOutput(token.ac); err != nil {
			return domain.UserErr(domain.NoticeSessionUnavailable, "couldn't clean up graphics before local handoff", err)
		}
	}
	if err := token.sendControl(ports.Frame{Type: ports.MsgAttachTarget, Payload: payload}); err != nil {
		if samePeerEligible {
			token.ac.clearSamePeerOffer()
		}
		return domain.UserErr(domain.NoticeSessionUnavailable, "couldn't offer local session switch", err)
	}
	if !samePeerEligible {
		// Stopped and explicit non-default index targets retain the close-and-dial
		// handoff instead of losing their target-specific transition semantics.
		if guard.closePicker {
			d.closePicker(token.ac)
		}
		d.clientGoneForAttachment(token, false)
	}
	return nil
}

// sendRemoteAttachTargetForAttachment validates the catalog row and hands the
// endpoint to the thin client. The daemon owns no remote session shadow: after
// the target is sent, the local attachment is detached and the client opens a
// fresh connection to the owning daemon.
func (d *Daemon) sendRemoteAttachTargetForAttachment(token attachmentConnectionToken, target picker.Target, guard sessionHandoffGuard, _ string) error {
	failUnavailable := func() error {
		if token.attachmentCurrent() {
			d.notifyRemotePickerUnavailable(token.sess, target)
		}
		return errAttachmentTransition
	}
	if target.RemoteTarget == nil || target.RemoteKey == nil {
		return failUnavailable()
	}
	remoteTarget := *target.RemoteTarget
	key := *target.RemoteKey
	if err := remoteTarget.Validate(); err != nil || key.Validate() != nil || target.Session != key.ID() || key.Host != remoteTarget.Endpoint || key.Name != remoteTarget.SessionName || key.LifecycleID != remoteTarget.LifecycleID || !d.remoteCatalogTargetReady(remoteTarget) {
		return failUnavailable()
	}
	handoff := ports.AttachTarget{
		Endpoint:          remoteTarget.Endpoint,
		Session:           remoteTarget.SessionName,
		Intent:            ports.IntentAttach,
		RemoteTarget:      &remoteTarget,
		EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned,
	}
	payload := ports.MarshalAttachTarget(handoff)
	if payload == nil {
		return failUnavailable()
	}
	if err := token.sendControl(ports.Frame{Type: ports.MsgAttachTarget, Payload: payload}); err != nil {
		return domain.UserErr(domain.NoticeSessionUnavailable, "couldn't attach to remote session", err)
	}
	if guard.closePicker {
		d.closePicker(token.ac)
	}
	d.clientGoneForAttachment(token, false)
	return nil
}

func (d *Daemon) remoteCatalogEntryLocked(host string) (ports.RemoteCatalogCacheEntry, bool) {
	if d.remoteCatalog.status[host] != remoteHostFresh {
		return ports.RemoteCatalogCacheEntry{}, false
	}
	entry, ok := d.remoteCatalog.cache[host]
	if !ok || remoteCatalogExpired(entry.FetchedAt, d.clock.Now()) {
		d.remoteCatalog.status[host] = remoteHostStale
		return ports.RemoteCatalogCacheEntry{}, false
	}
	return entry, true
}

func (d *Daemon) remoteCatalogTargetReady(target domain.RemoteSessionTarget) bool {
	if d == nil || target.Validate() != nil {
		return false
	}
	d.remoteCatalog.mu.Lock()
	defer d.remoteCatalog.mu.Unlock()
	entry, ok := d.remoteCatalogEntryLocked(target.Endpoint)
	if !ok {
		return false
	}
	for _, session := range entry.Sessions {
		if session.Name != target.SessionName || session.LifecycleID != target.LifecycleID {
			continue
		}
		if target.Stopped != remoteSessionStateStopped(session.State) || session.State == ports.RemoteCatalogSessionBroken {
			return false
		}
		tabs := ports.CatalogTabs(session)
		metadata := make([]domain.TabSelectorTab, 0, len(tabs))
		for _, tab := range tabs {
			metadata = append(metadata, domain.TabSelectorTab{ID: domain.TabStableID(tab.ID), Name: tab.Name})
		}
		_, ok := target.ResolveTab(metadata)
		return ok
	}
	return false
}

// switchToTargetGuarded is retained for daemon-internal and headless callers.
func (d *Daemon) switchToTargetGuarded(from *session, ac *attachedClient, target picker.Target, guard sessionHandoffGuard) error {
	return d.switchToTargetGuardedForAttachment(from, ac, target, guard, nil, "")
}

func (d *Daemon) switchToTargetGuardedForAttachment(from *session, ac *attachedClient, target picker.Target, guard sessionHandoffGuard, sourceToken *attachmentConnectionToken, action string) error {
	if target.Name != "" {
		ctx := d.serveCtx
		if ctx == nil {
			ctx = context.Background()
		}
		if err := d.waitForTargetRestore(ctx, target.Name); err != nil {
			if sourceToken != nil && !sourceToken.attachmentCurrent() {
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
		if sourceToken != nil && !sourceToken.attachmentCurrent() {
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
			fresh, admitted := ac.beginAttachmentEffect(transition.published)
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
			fresh, admitted := ac.beginAttachmentEffect(transition.published)
			if admitted {
				d.activateTabAfterResizeForLease(from, from.tabForAttachment(ac), false, ac, transition.published.lease)
				d.invalidateRender(from, ac, true, "picker.go")
				fresh.End()
			}
		} else {
			d.activateTabAfterResizeForLease(from, from.tabForAttachment(ac), false, ac, nil)
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
func (d *Daemon) resolveNamedLifecycleTargetLocked(target picker.Target) (*session, inactiveSession, bool, bool) {
	if target.Name == "" {
		return nil, inactiveSession{}, false, false
	}
	if active := d.findByNameLocked(target.Name); active != nil {
		active.mu.Lock()
		matches := targetMatchesLifecycle(target, active.name, active.createdAt, active.incarnation)
		active.mu.Unlock()
		return active, inactiveSession{}, false, matches
	}
	stopped, ok := d.inactive[target.Name]
	if !ok || !stopped.canResume() || !targetMatchesLifecycle(target, stopped.name, stopped.createdAt, stopped.incarnation) {
		return nil, inactiveSession{}, false, false
	}
	return nil, stopped, true, true
}

// switchToActiveTargetLocked commits an active target handoff through the
// centralized attachment transition. Caller holds d.mu.
func (d *Daemon) switchToActiveTargetLocked(from *session, ac *attachedClient, targetSess *session, target picker.Target, guard sessionHandoffGuard, sourceToken *attachmentConnectionToken, action string) (*session, attachmentTransitionResult, bool) {
	if d.sessions[target.Session] != targetSess {
		return nil, attachmentTransitionResult{}, false
	}
	if targetSess == from {
		if sourceToken == nil {
			targetSess.mu.Lock()
			defer targetSess.mu.Unlock()
			_, attached := targetSess.attachments[ac]
			if !attached || !targetMatchesLifecycle(target, targetSess.name, targetSess.createdAt, targetSess.incarnation) || target.TabIndex < 0 || target.TabIndex >= len(targetSess.tabs) {
				return nil, attachmentTransitionResult{}, false
			}
			if !targetSess.activateAttachmentViewLocked(ac, target.TabIndex) {
				return nil, attachmentTransitionResult{}, false
			}
			return targetSess, attachmentTransitionResult{}, true
		}
		d.mu.Unlock()
		transition, err := d.transitionAttachment(attachmentTransitionRequest{
			source: from, target: targetSess, next: ac,

			expectedTransport:       sourceToken.transport,
			sourceToken:             sourceToken,
			action:                  action,
			expectedTargetLifecycle: pickerTargetLifecycleFence(target),
			activateTargetTab:       true,
			targetTabIndex:          target.TabIndex,
			preserveAttachment:      true,
		})
		d.mu.Lock()
		return targetSess, transition, err == nil
	}

	unlock := lockAttachmentSessions(from, targetSess)
	_, sourceAttached := from.attachments[ac]
	if !sourceAttached || !targetMatchesLifecycle(target, targetSess.name, targetSess.createdAt, targetSess.incarnation) {
		unlock()
		return nil, attachmentTransitionResult{}, false
	}
	if guard.expectedSource != nil {
		view := ac.viewSnapshot()
		if domain.TabStableID(guard.expectedSource.stableID) != view.tabID {
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
		source: from,
		target: targetSess,
		next:   ac,

		expectedTransport:       expectedTransport,
		sourceToken:             sourceToken,
		action:                  action,
		expectedTargetLifecycle: pickerTargetLifecycleFence(target),
		expectedSourceTab:       guard.expectedSource,
		activateTargetTab:       target.TabIndex >= 0,
		targetTabIndex:          target.TabIndex,
		copySourceEnvironment:   true,
		ready:                   true,
	})
	d.mu.Lock()
	if err != nil {
		return nil, attachmentTransitionResult{}, false
	}
	d.touchMRU(targetSess)
	return targetSess, transition, true
}

// resumeStoppedAndSwitchLocked creates the stopped representation and commits
// the handoff while d.mu is held. Creation failure leaves the source client
// and stopped record untouched.
func (d *Daemon) resumeStoppedAndSwitchLocked(from *session, ac *attachedClient, target picker.Target, stopped inactiveSession, sourceToken *attachmentConnectionToken, guard sessionHandoffGuard, action string) (*session, attachmentTransitionResult, bool, error) {
	if stopped.broken() {
		return nil, attachmentTransitionResult{}, false, &protoErr{ports.ErrInternal, "session durable state is broken: " + target.Name}
	}
	if !stopped.canResume() {
		return nil, attachmentTransitionResult{}, false, errAttachmentTransition
	}

	if sourceToken != nil {
		var targetSess *session
		d.mu.Unlock()
		transition, err := d.transitionAttachment(attachmentTransitionRequest{
			source: from, next: ac,
			expectedTransport: sourceToken.transport, sourceToken: sourceToken, action: action,
			expectedSourceTab: guard.expectedSource, copySourceEnvironment: true, ready: true,
			createTargetLocked: func() (*session, error) {
				current, ok := d.inactive[target.Name]
				if !ok || !current.canResume() || !current.sameLifecycle(stopped) || !targetMatchesLifecycle(target, current.name, current.createdAt, current.incarnation) {
					return nil, errAttachmentTransition
				}
				from.mu.Lock()
				cwd, env := d.dirOrHome(current.cwd), copyEnvironment(from.env)
				from.mu.Unlock()
				created, createErr := d.resumeInactiveSessionLocked(target.Name, cwd, ac.geometrySnapshot(), env, current, current.tabNames)
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
		return targetSess, transition, true, nil
	}

	from.mu.Lock()
	_, attached := from.attachments[ac]
	if !attached {
		from.mu.Unlock()
		return nil, attachmentTransitionResult{}, false, nil
	}
	env := copyEnvironment(from.env)
	from.mu.Unlock()
	current, ok := d.inactive[target.Name]
	if !ok || !current.canResume() || !current.sameLifecycle(stopped) || !targetMatchesLifecycle(target, current.name, current.createdAt, current.incarnation) {
		return nil, attachmentTransitionResult{}, false, errAttachmentTransition
	}
	cwd := d.dirOrHome(current.cwd)
	targetSess, err := d.resumeInactiveSessionLocked(target.Name, cwd, ac.geometrySnapshot(), env, current, current.tabNames)
	if err != nil {
		d.log.Warn("resuming stopped session failed", "err", err, "session", target.Name)
		return nil, attachmentTransitionResult{}, false, err
	}

	d.mu.Unlock()
	transition, err := d.transitionAttachment(attachmentTransitionRequest{
		source: from, target: targetSess, next: ac,

		expectedTransport: ac.transportSnapshot(), ready: true,
	})
	d.mu.Lock()
	if err != nil {
		return targetSess, attachmentTransitionResult{}, false, err
	}
	d.touchMRU(targetSess)
	return targetSess, transition, true, nil
}

func targetMatchesLifecycle(target picker.Target, name string, createdAt int64, incarnation domain.IncarnationID) bool {
	if target.Incarnation != (domain.IncarnationID{}) && target.Incarnation != incarnation {
		return false
	}
	return target.ExpectedCreatedAt == nil || (target.Name == name && *target.ExpectedCreatedAt == createdAt)
}

// stealClientForTarget is retained for direct-ID callers and tests. Named
// targets must use switchToTarget so resolution and commit share d.mu.
func (d *Daemon) stealClientForTarget(from *session, ac *attachedClient, targetSess *session, target picker.Target) {
	d.mu.Lock()
	_, transition, switched := d.switchToActiveTargetLocked(from, ac, targetSess, target, sessionHandoffGuard{}, nil, "")
	d.mu.Unlock()
	if !switched {
		return
	}
	d.deferAttachmentTransitionCleanups(transition)
}

// resumeStoppedAndSwitch is retained for direct callers and tests. It resolves
// its stopped target and commits creation under one d.mu critical section.
func (d *Daemon) resumeStoppedAndSwitch(from *session, ac *attachedClient, target picker.Target) bool {
	d.mu.Lock()
	stopped, ok := d.inactive[target.Name]
	var (
		transition attachmentTransitionResult
		switched   bool
	)
	if ok && stopped.canResume() && targetMatchesLifecycle(target, stopped.name, stopped.createdAt, stopped.incarnation) {
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

func (d *Daemon) killPickerTargetForAttachment(target picker.Target, token attachmentConnectionToken) error {
	if token.ac == nil {
		return d.killPickerTarget(target)
	}
	if target.RemoteKey != nil {
		return nil
	}
	if target.Stopped {
		token.effect.bindActionEnd(d, "picker-delete")
		token.effect.End()
		frozen := freezeAttachmentEffectGates(token.ac)
		defer frozen.unfreeze()
		if !d.sourceAttachmentTokenCurrentFrozen(token) {
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
	return d.killSessionForAttachment(targetSess, ports.ReasonSessionKilled, true, token, "picker-delete")
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

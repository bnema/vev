// Package daemon holds vev's server-side session multiplexer use case.
package daemon

import (
	"context"
	"sort"
	"time"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/picker"
	"github.com/bnema/vev/internal/usecase/ui"
)

// The picker's floating geometry lives in the picker package: the presenting
// client draws the same box, and this daemon sizes the preview it captures from
// it, so both sides always agree.
var pickerModal = picker.Modal

const remotePickerPreviewDebounce = 80 * time.Millisecond

// pickerViews projects the shared daemon inventory for the picker. It keeps
// current/ephemeral rows, tabs, grouping, and move eligibility; lifecycle and
// remote facts come from the common capture. Section lines are published
// whenever the source has more than one origin group, so the presenting
// client can order each run locally without inventing group labels.
func (d *Daemon) pickerViews(cur *session, ac *attachedClient) ([]pickerSessionView, pickerSourceFilter) {
	_, grouped, current := d.pickerViewProjections(cur, ac)
	return grouped, current
}

// pickerLocalViews is the remote-free picker projection. The hybrid
// projection interleaves foreign rows between the live and stopped rows, so
// the two groups are returned separately and the stopped rows are returned
// without a LOCAL section label. The assembler applies that label once it
// knows whether any row precedes them.
type pickerLocalViews struct {
	current        pickerSourceFilter
	now            time.Time
	recentLive     []pickerSessionView
	groupedLive    []pickerSessionView
	recentStopped  []pickerSessionView
	groupedStopped []pickerSessionView
}

// pickerForeignViews is the projected foreign (remote-directory) portion of
// the hybrid picker projection plus whether it suppresses the LOCAL label on a
// preceding stopped row.
type pickerForeignViews struct {
	recent            []pickerSessionView
	grouped           []pickerSessionView
	blockLocalStopped bool
}

// localPickerViews is the prepared local-only picker projection (Plan 001
// P4.3). It captures live and stopped local rows, orders them exactly like the
// hybrid projection does when no foreign source is present, and never reads
// the remote directory.
func (d *Daemon) localPickerViews(cur *session, ac *attachedClient) pickerLocalViews {
	if cur != nil && ac != nil {
		cur.repairAttachmentView(ac)
	}
	opts := viewOptions{tabDetails: true, focusedTitles: true, terminalTitle: d.currentTabsConfig().TerminalTitle}
	inv := d.captureLocalSessionInventory(opts, true)
	stopped := inv.visibleStopped()

	// One snapshot per live session: sorting and view building read the same
	// capture, so comparators cannot observe a concurrent touchMRU or
	// renameSession mid-sort.
	live := inv.live
	var current pickerSourceFilter
	for _, item := range live {
		snap := item.view
		if item.sess == cur {
			if ac != nil {
				view := ac.viewSnapshot()
				if view.tabID != "" {
					current = pickerSourceFilter{Session: snap.id, Incarnation: snap.incarnation, TabID: view.tabID}
				}
			} else if len(snap.tabs) > 0 {
				current = pickerSourceFilter{Session: snap.id, Incarnation: snap.incarnation, TabID: snap.tabs[snap.defaultTab].id}
			}
		}
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

	recentLive := make([]pickerSessionView, 0, len(live))
	groupedLive := make([]pickerSessionView, 0, len(live))
	for i, item := range live {
		recentLive = append(recentLive, item.view.pickerView())
		view := item.view.pickerView()
		if i == 0 {
			view.Section = "LOCAL"
		}
		groupedLive = append(groupedLive, view)
	}
	stoppedRows := stoppedPickerRows(stopped)
	// The grouped projection labels its first stopped row in place, so the
	// recent projection keeps its own copy.
	recentStopped := append([]pickerSessionView(nil), stoppedRows...)
	groupedStopped := stoppedRows
	return pickerLocalViews{current: current, now: inv.now, recentLive: recentLive, groupedLive: groupedLive, recentStopped: recentStopped, groupedStopped: groupedStopped}
}

// stoppedPickerRows projects visible inactive records into picker rows. The
// local and hybrid projections share this single row shape, and the callers
// own whether a LOCAL section label is applied.
func stoppedPickerRows(stopped []inactiveSession) []pickerSessionView {
	rows := make([]pickerSessionView, 0, len(stopped))
	for _, s := range stopped {
		createdAt := s.createdAt
		rows = append(rows, pickerSessionView{ID: domain.SessionID("stopped:" + s.name), Incarnation: s.incarnation, Name: s.name, TargetName: s.name, Stopped: true, ExpectedCreatedAt: &createdAt})
	}
	return rows
}

// foreignPickerViews projects the current remote directory for the picker. It
// reads the remote directory; a nil directory yields no rows and cannot
// suppress the local stopped section label. The daemon-side monitor stays
// intentionally active in this state (coordinated P7 removal set).
func (d *Daemon) foreignPickerViews(now time.Time) pickerForeignViews {
	hosts, monitored, initialized := d.currentRemoteDirectory()
	catalogRows := 0
	for _, host := range hosts {
		catalogRows += len(host.Sessions)
		if len(host.Sessions) == 0 && host.Availability != domain.RemoteAvailabilityReachable {
			catalogRows++
		}
	}
	checking := monitored && !initialized
	recent := make([]pickerSessionView, 0, catalogRows)
	for _, host := range hosts {
		for _, session := range host.Sessions {
			key := domain.RemoteSessionKey{Host: host.Endpoint, Name: session.Name}
			if key.Validate() == nil {
				recent = append(recent, remotePickerView(key, session, host, now))
			}
		}
		if len(host.Sessions) == 0 && host.Availability != domain.RemoteAvailabilityReachable {
			recent = append(recent, remotePickerHostView(host, now))
		}
	}
	if checking {
		recent = append(recent, remotePickerCheckingView())
	}

	grouped := make([]pickerSessionView, 0, catalogRows)
	for _, host := range hosts {
		publishedForHost := 0
		for _, session := range host.Sessions {
			key := domain.RemoteSessionKey{Host: host.Endpoint, Name: session.Name}
			if key.Validate() != nil {
				continue
			}
			view := remotePickerView(key, session, host, now)
			view.HideRemoteOrigin = true
			if publishedForHost == 0 {
				view.Section = "REMOTE  " + host.Endpoint
			}
			grouped = append(grouped, view)
			publishedForHost++
		}
		if len(host.Sessions) == 0 && host.Availability != domain.RemoteAvailabilityReachable {
			view := remotePickerHostView(host, now)
			view.Section = "REMOTE  " + host.Endpoint
			grouped = append(grouped, view)
		}
	}
	// A nil directory means remote monitoring is not installed at all:
	// only an installed-but-unpublished monitor reads as "checking".
	if checking {
		view := remotePickerCheckingView()
		view.Section = "REMOTE"
		grouped = append(grouped, view)
	}
	return pickerForeignViews{recent: recent, grouped: grouped, blockLocalStopped: catalogRows > 0 || checking}
}

// localPickerViewProjections is the complete remote-free picker projection.
// It appends the local stopped rows directly and applies the LOCAL section
// label when no live row opened the group.
func (d *Daemon) localPickerViewProjections(cur *session, ac *attachedClient) ([]pickerSessionView, []pickerSessionView, pickerSourceFilter) {
	local := d.localPickerViews(cur, ac)
	recent := make([]pickerSessionView, 0, len(local.recentLive)+len(local.recentStopped))
	recent = append(recent, local.recentLive...)
	recent = append(recent, local.recentStopped...)
	grouped := make([]pickerSessionView, 0, len(local.groupedLive)+len(local.groupedStopped))
	grouped = append(grouped, local.groupedLive...)
	grouped = append(grouped, withLocalStoppedSection(local.groupedStopped, len(local.groupedLive) == 0)...)
	return recent, grouped, local.current
}

func (d *Daemon) pickerViewProjections(cur *session, ac *attachedClient) ([]pickerSessionView, []pickerSessionView, pickerSourceFilter) {
	// The hybrid projection reuses the prepared local projection for its
	// local rows and interleaves the current foreign remote rows between the
	// live and stopped groups.
	local := d.localPickerViews(cur, ac)
	foreign := d.foreignPickerViews(local.now)
	recent := make([]pickerSessionView, 0, len(local.recentLive)+len(foreign.recent)+len(local.recentStopped))
	recent = append(recent, local.recentLive...)
	recent = append(recent, foreign.recent...)
	recent = append(recent, local.recentStopped...)
	grouped := make([]pickerSessionView, 0, len(local.groupedLive)+len(foreign.grouped)+len(local.groupedStopped))
	grouped = append(grouped, local.groupedLive...)
	grouped = append(grouped, foreign.grouped...)
	grouped = append(grouped, withLocalStoppedSection(local.groupedStopped, len(local.groupedLive) == 0 && !foreign.blockLocalStopped)...)
	return recent, grouped, local.current
}

// withLocalStoppedSection marks the first stopped row as the LOCAL section
// when no preceding row already opened a local group.
func withLocalStoppedSection(stopped []pickerSessionView, local bool) []pickerSessionView {
	if local && len(stopped) > 0 {
		stopped[0].Section = "LOCAL"
	}
	return stopped
}

func attentionSuffix(label string) string {
	return label + " " + string(ui.AttentionGlyph)
}

func (d *Daemon) notifyRemotePickerUnavailable(sess *session, target picker.Target) {
	reason := target.UnavailableReason
	if reason == "" {
		reason = domain.RemoteReasonIdentityChanged
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
	case domain.RemoteReasonCatalogStale:
		return "catalog stale"
	case domain.RemoteReasonHostUnreachable:
		return "host unreachable"
	case domain.RemoteReasonVersionMismatch:
		return "version mismatch"
	case domain.RemoteReasonSessionStopped:
		return "session stopped"
	case domain.RemoteReasonSessionBroken:
		return "session broken"
	case domain.RemoteReasonMalformed:
		return "catalog malformed"
	case domain.RemoteReasonAuthFailure:
		return "authentication failed"
	case domain.RemoteReasonRefreshing:
		return "refreshing"
	case domain.RemoteReasonIdentityChanged:
		return "session identity changed"
	default:
		return "unavailable"
	}
}

func remotePickerPreviewSize(size domain.Size) (uint16, uint16) {
	preview := picker.PreviewRect(size)
	width, height := preview.Width, preview.Height
	if width <= 0 || height <= 0 {
		return 0, 0
	}
	return uint16(min(width, protocol.RemotePreviewMaxWidth)), uint16(min(height, protocol.RemotePreviewMaxHeight))
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

// pickerRouteTargetsEqual compares the exact route identity of two picker
// targets, ignoring presentation fields such as UnavailableReason that
// render around the route on every observation.
func pickerRouteTargetsEqual(left, right picker.Target) bool {
	if left.Session != right.Session || left.Incarnation != right.Incarnation || left.Name != right.Name ||
		left.RemoteHost != right.RemoteHost || left.TabID != right.TabID || left.TabIndex != right.TabIndex || left.Stopped != right.Stopped ||
		!remoteKeysEqual(left.RemoteKey, right.RemoteKey) ||
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
	allowSamePeer  bool
}

// switchActiveTargetForAttachment hands a frame-bound navigation request to the
// centralized transition. The transition releases admission at its freeze seam
// and revalidates the exact initiating token before changing target focus or
// attachment ownership.
func (d *Daemon) switchActiveTargetForAttachment(effect *attachmentEffect, target picker.Target) error {
	return d.switchActiveTargetForAttachmentGuarded(effect, target, sessionHandoffGuard{}, "jump-attention")
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

func (d *Daemon) switchActiveTargetForAttachmentGuarded(effect *attachmentEffect, target picker.Target, guard sessionHandoffGuard, action string) error {
	if !effect.current() || effect.sess == nil || effect.ac == nil {
		return nil
	}
	d.mu.Lock()
	targetSess := d.sessions[target.Session]
	d.mu.Unlock()
	if targetSess == nil {
		if !effect.current() {
			return nil
		}
		d.invalidateRender(effect.sess, effect.ac, true, "picker.go")
		return domain.UserErr(domain.NoticeSessionUnavailable, "couldn't switch to that session", nil)
	}
	if targetSess == effect.sess {
		return nil
	}

	capability := effect.capability()
	transition, err := d.transitionAttachment(attachmentTransitionRequest{
		source: effect.sess, target: targetSess, next: effect.ac,

		expectedTransport: effect.transport, sourceCapability: &capability, sourceEffect: effect, action: action,
		expectedTargetLifecycle: pickerTargetLifecycleFence(target),
		activateTargetTab:       true, targetTabIndex: target.TabIndex, copySourceEnvironment: true, ready: true,
	})
	if err != nil {
		// Losing the exact source role is a benign stale action, not a notice for
		// whichever attachment replaced the initiator.
		if !capability.current() {
			return nil
		}
		return domain.UserErr(domain.NoticeSessionUnavailable, "couldn't switch to that session", err)
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
func (d *Daemon) switchToTargetForAttachment(effect *attachmentEffect, target picker.Target, guard sessionHandoffGuard, action string) error {
	if !effect.current() || effect.sess == nil || effect.ac == nil {
		return nil
	}
	if target.RemoteTarget != nil || target.RemoteKey != nil {
		return d.sendRemoteAttachTargetForAttachment(effect, target, guard, action)
	}
	return d.sendLocalAttachTargetForAttachment(effect, target, guard, action)
}

// sendLocalAttachTargetForAttachment offers an endpoint-empty, exact local
// target on the current authenticated connection. A current client confirms it
// with SamePeerSwitchRequest; an interrupted or older client retains the
// existing close-and-redial fallback without any daemon-origin inference.
func (d *Daemon) sendLocalAttachTargetForAttachment(effect *attachmentEffect, target picker.Target, guard sessionHandoffGuard, action string) error {
	d.mu.Lock()
	var targetSess *session
	var sessionName string
	var exactTarget *protocol.ExactSessionTarget
	var matches bool
	if target.Name != "" {
		var stopped inactiveSession
		var stoppedTarget bool
		targetSess, stopped, stoppedTarget, matches = d.resolveNamedLifecycleTargetLocked(target)
		if stoppedTarget && matches {
			sessionName = stopped.name
			exactTarget = &protocol.ExactSessionTarget{LifecycleID: stopped.incarnation, SessionName: sessionName}
		}
	} else {
		targetSess = d.sessions[target.Session]
		matches = targetSess != nil
	}
	if matches && targetSess != nil {
		targetSess.mu.Lock()
		sessionName = targetSess.name
		exactTarget = &protocol.ExactSessionTarget{LifecycleID: targetSess.incarnation, SessionName: sessionName}
		targetSess.mu.Unlock()
	}
	d.mu.Unlock()
	if !matches || !effect.current() {
		return errAttachmentTransition
	}
	if targetSess != nil && target.TabID != "" {
		// Ordinary local tab navigation keeps the attachment.
		return d.switchToTargetGuardedForAttachment(effect.sess, effect.ac, target, guard, effect, action)
	}

	// Ordinary live session rows can use the client-confirmed same-peer
	// path. Stopped targets require a handoff.
	samePeerEligible := guard.allowSamePeer && targetSess != nil && target.TabIndex <= 0
	if exactTarget == nil {
		return errAttachmentTransition
	}
	// A close-and-dial handoff leaves this daemon's Kitty namespace. A
	// same-peer transition keeps the attachment and its namespace; the target
	// scene diff deletes and replaces the source session's placements.
	if samePeerEligible {
		if err := offerSamePeerAttachTarget(effect, *exactTarget); err != nil {
			return domain.UserErr(domain.NoticeSessionUnavailable, "couldn't offer local session switch", err)
		}
		return nil
	}
	handoff := protocol.AttachTarget{
		Session:           sessionName,
		Intent:            protocol.IntentAttach,
		ExactTarget:       exactTarget,
		PreferredTabID:    target.TabID,
		EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
	}
	if protocol.ValidateAttachTarget(handoff) != nil {
		return errAttachmentTransition
	}
	if err := d.cleanupAttachmentOutput(effect.ac); err != nil {
		return domain.UserErr(domain.NoticeSessionUnavailable, "couldn't clean up attachment output before local handoff", err)
	}
	if err := effect.sendControl(handoff); err != nil {
		return domain.UserErr(domain.NoticeSessionUnavailable, "couldn't offer local session switch", err)
	}
	// Stopped and explicit non-default index targets retain the close-and-dial
	// handoff instead of losing their target-specific transition semantics.
	// Keep the source link open until the client receives the ordered cleanup
	// and handoff frames, then let the client's close drive ordinary parking.
	effect.bindActionEnd(d, "detach")
	effect.End()
	return nil
}

// sendRemoteAttachTargetForAttachment validates the catalog row and hands the
// endpoint to the thin client. The daemon owns no remote session shadow: after
// the target is sent, the local attachment is detached and the client opens a
// fresh connection to the owning daemon.
func (d *Daemon) sendRemoteAttachTargetForAttachment(effect *attachmentEffect, target picker.Target, guard sessionHandoffGuard, _ string) error {
	failUnavailable := func() error {
		if effect.current() {
			d.notifyRemotePickerUnavailable(effect.sess, target)
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
	handoff := protocol.AttachTarget{
		Endpoint:          remoteTarget.Endpoint,
		Session:           remoteTarget.SessionName,
		Intent:            protocol.IntentAttach,
		RemoteTarget:      &remoteTarget,
		EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
	}
	if protocol.ValidateAttachTarget(handoff) != nil {
		return failUnavailable()
	}
	if err := effect.sendControl(handoff); err != nil {
		return domain.UserErr(domain.NoticeSessionUnavailable, "couldn't attach to remote session", err)
	}
	d.clientGoneForAttachment(effect, false)
	return nil
}

// remoteCatalogTargetReady validates an exact remote destination against
// the latest known inventory at any age. Freshness is presentation
// information, not attach authority: valid cached targets are attempted and
// rejected precisely at the destination, never declared nonexistent by age.
func (d *Daemon) remoteCatalogTargetReady(target domain.RemoteSessionTarget) bool {
	if d == nil || target.Validate() != nil {
		return false
	}
	host, ok := d.remoteDirectorySnapshot().Find(target.Endpoint)
	if !ok || !host.InventoryKnown {
		return false
	}
	for _, session := range host.Sessions {
		if session.Name != target.SessionName || session.LifecycleID != target.LifecycleID {
			continue
		}
		if target.Stopped != remoteSessionStateStopped(session.State) || session.State == catalogue.RemoteCatalogSessionBroken {
			return false
		}
		tabs := catalogue.CatalogTabs(session)
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

func (d *Daemon) switchToTargetGuardedForAttachment(from *session, ac *attachedClient, target picker.Target, guard sessionHandoffGuard, sourceEffect *attachmentEffect, action string) error {
	var sourceCapability *attachmentCapability
	if sourceEffect != nil {
		capability := sourceEffect.capability()
		sourceCapability = &capability
	}
	if target.Name != "" {
		ctx := d.serveCtx
		if ctx == nil {
			ctx = context.Background()
		}
		if err := d.waitForTargetRestore(ctx, target.Name); err != nil {
			if sourceCapability != nil && !sourceCapability.current() {
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
				targetSess, transition, switched, cause = d.resumeStoppedAndSwitchLocked(from, ac, target, stopped, sourceEffect, guard, action)
			} else {
				// The snapshot ID may describe the stopped representation. The
				// locked name/lifecycle resolver chose this active incarnation.
				resolvedTarget := target
				resolvedTarget.Session = active.id
				targetSess, transition, switched = d.switchToActiveTargetLocked(from, ac, active, resolvedTarget, guard, sourceEffect, action)
			}
		}
	} else if active := d.sessions[target.Session]; active != nil {
		targetSess, transition, switched = d.switchToActiveTargetLocked(from, ac, active, target, guard, sourceEffect, action)
	}
	d.mu.Unlock()

	if !switched {
		if sourceCapability != nil && !sourceCapability.current() {
			return errAttachmentTransition
		}
		if guard.expectedSource != nil {
			return errNoNeighbor
		}
		d.invalidateRender(from, ac, true, "picker.go")
		return domain.UserErr(domain.NoticeSessionUnavailable, "couldn't switch to that session", cause)
	}
	d.deferAttachmentTransitionCleanups(transition)
	if targetSess == from {
		if sourceEffect != nil {
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
func (d *Daemon) switchToActiveTargetLocked(from *session, ac *attachedClient, targetSess *session, target picker.Target, guard sessionHandoffGuard, sourceEffect *attachmentEffect, action string) (*session, attachmentTransitionResult, bool) {
	var sourceCapability *attachmentCapability
	if sourceEffect != nil {
		capability := sourceEffect.capability()
		sourceCapability = &capability
	}
	if d.sessions[target.Session] != targetSess {
		return nil, attachmentTransitionResult{}, false
	}
	if targetSess == from {
		if sourceEffect == nil {
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

			expectedTransport:       sourceCapability.transport,
			sourceCapability:        sourceCapability,
			sourceEffect:            sourceEffect,
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
	if sourceCapability != nil {
		expectedTransport = sourceCapability.transport
	}
	transition, err := d.transitionAttachment(attachmentTransitionRequest{
		source: from,
		target: targetSess,
		next:   ac,

		expectedTransport:       expectedTransport,
		sourceCapability:        sourceCapability,
		sourceEffect:            sourceEffect,
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
func (d *Daemon) resumeStoppedAndSwitchLocked(from *session, ac *attachedClient, target picker.Target, stopped inactiveSession, sourceEffect *attachmentEffect, guard sessionHandoffGuard, action string) (*session, attachmentTransitionResult, bool, error) {
	if stopped.broken() {
		return nil, attachmentTransitionResult{}, false, &protoErr{protocol.ErrInternal, "session durable state is broken: " + target.Name}
	}
	if !stopped.canResume() {
		return nil, attachmentTransitionResult{}, false, errAttachmentTransition
	}

	if sourceEffect != nil {
		sourceCapability := sourceEffect.capability()
		var targetSess *session
		d.mu.Unlock()
		transition, err := d.transitionAttachment(attachmentTransitionRequest{
			source: from, next: ac,
			expectedTransport: sourceCapability.transport, sourceCapability: &sourceCapability, sourceEffect: sourceEffect, action: action,
			expectedSourceTab: guard.expectedSource, copySourceEnvironment: true, ready: true,
			createTargetLocked: func() (*session, error) {
				if d.purgeAdmissionClosedLocked() {
					return nil, errPurgeAdmissionClosed
				}
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
				_ = d.killSession(targetSess, protocol.ReasonSessionKilled, true)
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
	if d.purgeAdmissionClosedLocked() {
		return nil, attachmentTransitionResult{}, false, errPurgeAdmissionClosed
	}
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

func (d *Daemon) killPickerTargetForAttachment(target picker.Target, effect *attachmentEffect) error {
	if effect == nil || effect.ac == nil {
		return d.killPickerTarget(target)
	}
	if target.RemoteKey != nil {
		return nil
	}
	capability := effect.capability()
	if target.Stopped {
		effect.bindActionEnd(d, "picker-delete")
		effect.End()
		frozen := freezeAttachmentEffectGates(effect.ac)
		defer frozen.unfreeze()
		if !d.sourceAttachmentCapabilityCurrentFrozen(capability) {
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
	return d.killSessionForAttachment(targetSess, protocol.ReasonSessionKilled, true, effect, "picker-delete")
}

func (d *Daemon) killPickerTarget(target picker.Target) error {
	if target.Stopped {
		if err := d.retryStoppedPurgeExact(target.Name, target.Incarnation, target.ExpectedCreatedAt); err != nil {
			d.log.Warn("deleting persisted stopped session failed", "err", err, "session", target.Name)
			return domain.UserErr(domain.NoticePersistDelete, "couldn't delete stopped session", err)
		}
		return nil
	}
	d.mu.Lock()
	targetSess := d.sessions[target.Session]
	d.mu.Unlock()
	if targetSess != nil {
		return d.killSession(targetSess, protocol.ReasonSessionKilled, true)
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
	return pickerPreviewFromFrame(p.screen)
}

func pickerPreviewFromFrame(frame renderer.CellSource) picker.Preview {
	rows := make([][]renderer.Cell, frame.Rows())
	for y := range rows {
		rows[y] = make([]renderer.Cell, frame.Columns())
		for x := range rows[y] {
			rows[y][x] = frame.Cell(x, y)
		}
	}
	return picker.Preview{Rows: rows, Width: frame.Columns(), Height: frame.Rows()}
}

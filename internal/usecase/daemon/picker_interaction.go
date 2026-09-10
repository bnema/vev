package daemon

import (
	"encoding/hex"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/picker"
)

// This file owns the serving-daemon side of the client-picker interaction:
// open admission, snapshot publication, typed selection resolution, and
// close. It mirrors the palette inventory interaction discipline
// (open/interaction/publication namespaces in palette_inventory.go) but
// never canonicalizes row order: picker order is semantically significant.
// The overlay picker model (`rt.picker`, handlePickerInput) is untouched:
// client-picker commits resolve opaque keys to picker.Target values and
// reuse the unchanged switchToTargetForAttachment handoff. Helpers never
// lock internally unless documented: callers hold pickerMu for state
// transitions and release it before any send.

// pickerClientKey derives a stable opaque key for an unchanged navigation
// target (lifecycle, name). Presentation-only updates keep the key; a
// changed or removed target retires it and a reappearing target earns a
// new key through its new lifecycle. Same role as
// navigationInventoryEntryKey.
func pickerClientKey(lifecycle domain.SessionLifecycleID, name string) string {
	return hex.EncodeToString(lifecycle[:]) + "/" + name
}

// pickerClientEnabled reports whether this attachment may receive picker
// snapshots. The daemon gates on the Hello-advertised capability, exactly
// like inventoryDemandEnabled gates inventory demand.
func pickerClientEnabled(ac *attachedClient) bool {
	return ac != nil && ac.navigationCapabilities&protocol.NavigationCapabilityClientPicker != 0
}

// openPickerClientInteraction starts one client-picker namespace. Keys and
// revisions never cross interactions.
func openPickerClientInteraction(rt *overlayRuntime, interaction uint64) uint64 {
	rt.pickerClientOpen = true
	rt.pickerClientInteraction = interaction
	rt.pickerClientRevision = 0
	rt.pickerClientKeys = make(map[string]picker.Target)
	return rt.pickerClientInteraction
}

// closePickerClientInteraction ends the interaction and drops resolved keys.
// A reopen starts a new namespace; a late selection for the closed
// interaction rejects on the interaction check.
func closePickerClientInteraction(rt *overlayRuntime) {
	rt.pickerClientOpen = false
	rt.pickerClientKeys = nil
}

// pickerClientRows projects navigate-intent session views into opaque rows
// in view order (never canonicalized: order is semantically significant).
// It covers exactly the rows picker.New turns into selectable targets for
// SelectNavigationTab: session headers and their tab rows, skipping section
// headers (no target) the same way rowsForSession skips non-eligible
// entries. Display strings are the drawn name/detail segments; endpoints,
// credentials, environment, CWD, and pane content never enter: only the
// opaque key plus display text cross the wire, and commit resolution
// happens daemon-side through the keys map.
func pickerClientRows(views []picker.SessionView) ([]protocol.PickerRow, map[string]picker.Target, protocol.PickerCursor) {
	rows := make([]protocol.PickerRow, 0)
	keys := make(map[string]picker.Target)
	cursor := protocol.PickerCursor{}
	index := -1
	for _, view := range views {
		for _, entry := range pickerClientEntries(view) {
			index++
			rows = append(rows, entry.row)
			if _, dup := keys[entry.row.Key]; !dup {
				keys[entry.row.Key] = entry.target
			}
			if entry.cursor {
				cursor = protocol.PickerCursor{Key: entry.row.Key, Index: index}
			}
		}
	}
	if cursor.Key == "" && len(rows) > 0 {
		for i, row := range rows {
			if target, ok := keys[row.Key]; ok && !target.Stopped || len(rows) == 1 {
				cursor = protocol.PickerCursor{Key: row.Key, Index: i}
				break
			}
		}
	}
	return rows, keys, cursor
}

type pickerClientEntry struct {
	row    protocol.PickerRow
	target picker.Target
	cursor bool
}

// pickerClientEntries projects one session view into snapshot entries: the
// session header when selectable for navigation, then each tab row. The
// eligibility mirrors rowsForSession for SelectNavigationTab: stopped
// sessions without tabs stay selectable (resume), remote rows follow
// activation, plain host/status rows carry no target and are skipped.
func pickerClientEntries(view picker.SessionView) []pickerClientEntry {
	var entries []pickerClientEntry
	header := pickerClientHeader(view)
	if header != nil {
		entries = append(entries, *header)
	}
	for i, tab := range view.Tabs {
		entry, ok := pickerClientTabEntry(view, tab, i)
		if !ok {
			continue
		}
		entries = append(entries, entry)
	}
	return entries
}

func pickerClientHeader(view picker.SessionView) *pickerClientEntry {
	if view.ID == "" || view.Name == "" {
		return nil
	}
	if view.RemoteHost != "" && view.RemoteTarget == nil {
		return nil
	}
	if view.ID == "remote:checking" {
		return nil
	}
	target := picker.Target{
		Session: view.ID, Incarnation: view.Incarnation, Name: view.TargetName,
		RemoteKey: view.RemoteKey, RemoteTarget: view.RemoteTarget, RemoteHost: view.RemoteHost,
		UnavailableReason: view.RemoteReason, TabIndex: -1, Stopped: view.Stopped,
		ExpectedCreatedAt: view.ExpectedCreatedAt,
	}
	if target.Name == "" {
		target.Name = view.Name
	}
	key := pickerClientKey(view.Incarnation, target.Name)
	detail := ""
	if view.Stopped {
		detail = "stopped"
	}
	if view.RemoteReason != "" {
		detail = view.RemoteReason
	}
	return &pickerClientEntry{
		row:    protocol.PickerRow{Key: key, Display: view.Name, Detail: detail, Stopped: view.Stopped},
		target: target,
	}
}

func pickerClientTabEntry(view picker.SessionView, tab picker.TabEntry, index int) (pickerClientEntry, bool) {
	if tab.TabID == "" && tab.Name == "" {
		return pickerClientEntry{}, false
	}
	target := picker.Target{
		Session: view.ID, Incarnation: view.Incarnation, Name: view.TargetName,
		RemoteKey: view.RemoteKey, RemoteTarget: view.RemoteTarget, RemoteHost: view.RemoteHost,
		UnavailableReason: view.RemoteReason, TabID: tab.TabID, TabIndex: index, Stopped: view.Stopped,
		ExpectedCreatedAt: view.ExpectedCreatedAt,
	}
	if target.Name == "" {
		target.Name = view.Name
	}
	key := pickerClientKey(view.Incarnation, target.Name+"#"+string(tab.TabID))
	return pickerClientEntry{
		row:    protocol.PickerRow{Key: key, Display: "  " + tab.Name, Detail: tab.Detail, Stopped: view.Stopped},
		target: target,
	}, true
}

// openPickerClientForAttachment admits one client-picker open on the guarded
// serving connection. It locks pickerMu only for the admission check, never
// across the send — same discipline as sendPaletteInventoryDemand. A missing
// capability or effect admits nothing: those paths coincide with attachment
// teardown, where the client-side loop is cancelled independently, so the
// caller falls back to the overlay instead. A failed snapshot send fails
// closed: the interaction is closed again so no interaction survives without
// its client-side acquisition barrier.
func (d *Daemon) openPickerClientForAttachment(ac *attachedClient, effect *attachmentEffect, interaction uint64) error {
	if !pickerClientEnabled(ac) || effect == nil || interaction == 0 {
		return errAttachmentTransition
	}
	ac.overlays.pickerMu.Lock()
	if ac.overlays.pickerClientOpen && ac.overlays.pickerClientInteraction == interaction {
		ac.overlays.pickerMu.Unlock()
		return nil
	}
	openPickerClientInteraction(ac.overlays, interaction)
	ac.overlays.pickerMu.Unlock()
	if err := d.publishPickerClientSnapshot(ac, effect, interaction); err != nil {
		ac.overlays.pickerMu.Lock()
		if ac.overlays.pickerClientInteraction == interaction {
			closePickerClientInteraction(ac.overlays)
		}
		ac.overlays.pickerMu.Unlock()
		return err
	}
	// The snapshot carries no fence of its own: the opener's fence (the
	// palette Enter's UIFence, registered before the command ran) retires
	// on the authoritative repaint below, completing the opener through
	// the normal receipt path. Invalidating here guarantees that paint
	// happens even when nothing else changed on the session.
	if sess := ac.currentAttachmentSession(); sess != nil {
		d.invalidateRender(sess, ac, true, "picker_interaction.go:client-open")
	}
	return nil
}

// publishPickerClientSnapshot builds the navigate model through the same
// read path as the overlay picker (pickerViews + newPickerModel) without
// installing it, projects opaque rows, bumps the revision, and publishes.
// Callers hold no locks across the send. The revision bump is the only
// refresh contract: a newer revision replaces the client model in place.
func (d *Daemon) publishPickerClientSnapshot(ac *attachedClient, effect *attachmentEffect, interaction uint64) error {
	sess := ac.currentAttachmentSession()
	if sess == nil {
		return errAttachmentTransition
	}
	views, _ := d.pickerViews(sess, ac)
	rows, keys, cursor := pickerClientRows(views)
	ac.overlays.pickerMu.Lock()
	if !ac.overlays.pickerClientOpen || ac.overlays.pickerClientInteraction != interaction {
		ac.overlays.pickerMu.Unlock()
		return nil
	}
	ac.overlays.pickerClientRevision++
	revision := ac.overlays.pickerClientRevision
	ac.overlays.pickerClientKeys = keys
	ac.overlays.pickerMu.Unlock()
	// The acquisition barrier is the attachment's current output fence
	// (epoch) plus its latest committed state: the client must display
	// admitted output through this boundary via outputApplyState before
	// rendering the picker. SizeEpoch is the view revision: a resize
	// mid-interaction bumps it and forces a refresh revision, so the
	// client never renders against a stale size.
	barrierEpoch, barrierState, sizeEpoch := pickerClientBarrier(ac)
	snapshot := protocol.PickerSnapshot{
		InteractionID: interaction, Revision: revision,
		Title: pickerTitle(pickerSortMode(d.pickerSort.Load())),
		Rows:  rows, Cursor: cursor,
		BarrierEpoch: barrierEpoch, BarrierState: barrierState,
		SizeEpoch: sizeEpoch,
	}
	if protocol.ValidatePickerSnapshot(snapshot) != nil {
		return protocol.ErrInvalidNavigation
	}
	return effect.sendControl(snapshot)
}

// refreshPickerClientSnapshot republishes the navigate model for an open
// client interaction after its row set, the attachment view, or the size
// changed. The send goes through a freshly admitted effect derived from the
// current attachment capability: the effect that opened the interaction is
// bounded to its own operation and may already be retired. A closed
// interaction is a no-op.
func (d *Daemon) refreshPickerClientSnapshot(ac *attachedClient) {
	if ac == nil || ac.overlays == nil || !pickerClientEnabled(ac) {
		return
	}
	ac.overlays.pickerMu.Lock()
	open := ac.overlays.pickerClientOpen
	interaction := ac.overlays.pickerClientInteraction
	ac.overlays.pickerMu.Unlock()
	if !open || interaction == 0 {
		return
	}
	sess := ac.currentAttachmentSession()
	if sess == nil {
		return
	}
	current := ac.transportSnapshot()
	_, effect, admitted := ac.beginCurrentAttachmentEffect(sess, current.transport)
	if !admitted {
		return
	}
	defer effect.End()
	_ = d.publishPickerClientSnapshot(ac, effect, interaction)
}

// closePickerClientForAttachment ends the interaction and notifies the
// client. Stale closes (superseded by a reopen) skip so generations stay
// ordered — same discipline as sendPaletteInventoryDemand close demands. The
// boolean reports that this call retired the interaction, so the caller can
// force the authoritative repaint the client's release paint waits for.
func (d *Daemon) closePickerClientForAttachment(ac *attachedClient, effect *attachmentEffect, interaction uint64) bool {
	if ac == nil || ac.overlays == nil || interaction == 0 {
		return false
	}
	ac.overlays.pickerMu.Lock()
	if !ac.overlays.pickerClientOpen || ac.overlays.pickerClientInteraction != interaction {
		ac.overlays.pickerMu.Unlock()
		return false
	}
	revision := ac.overlays.pickerClientRevision
	closePickerClientInteraction(ac.overlays)
	ac.overlays.pickerMu.Unlock()
	if effect == nil {
		return true
	}
	close := protocol.PickerClose{InteractionID: interaction, Revision: revision}
	if protocol.ValidatePickerClose(close) != nil {
		return true
	}
	_ = effect.sendControl(close)
	return true
}

// resolvePickerClientSelection validates one typed commit against the open
// interaction and resolves the opaque key to a navigation target under the
// daemon's locks. Unknown keys, mismatched interactions, and future
// revisions reject with a bounded failure; the resolved target is
// revalidated for lifecycle freshness exactly like previewTarget before
// the unchanged switchToTargetForAttachment handoff.
func (d *Daemon) resolvePickerClientSelection(effect *attachmentEffect, selection protocol.PickerSelection) {
	if protocol.ValidatePickerSelection(selection) != nil {
		return
	}
	ac := effect.ac
	if ac == nil || ac.overlays == nil {
		return
	}
	ac.overlays.pickerMu.Lock()
	open := ac.overlays.pickerClientOpen
	interaction := ac.overlays.pickerClientInteraction
	revision := ac.overlays.pickerClientRevision
	target, ok := ac.overlays.pickerClientKeys[selection.Key]
	ac.overlays.pickerMu.Unlock()
	if !open || interaction != selection.InteractionID {
		d.sendPickerClientFailure(effect, selection, protocol.PickerStaleRevision)
		return
	}
	if selection.Revision != revision {
		d.sendPickerClientFailure(effect, selection, protocol.PickerStaleRevision)
		return
	}
	if !ok {
		d.sendPickerClientFailure(effect, selection, protocol.PickerUnknownKey)
		return
	}
	if !d.pickerClientTargetCurrent(target) {
		d.sendPickerClientFailure(effect, selection, protocol.PickerRetiredTarget)
		return
	}
	// Retire the interaction before the handoff. The client's release waits
	// for a paint accepted after the daemon's own close, and the handoff's
	// destination paint has to be that paint: closing afterwards would let it
	// be suppressed while the client still owned the terminal, leaving the
	// client drained with no restore in flight.
	if !d.closePickerClientForAttachment(ac, effect, interaction) {
		d.sendPickerClientFailure(effect, selection, protocol.PickerStaleRevision)
		return
	}
	if err := d.switchToTargetForAttachment(effect, target, sessionHandoffGuard{closePicker: true, allowSamePeer: true}, "picker-client-select"); err != nil {
		d.sendPickerClientFailure(effect, selection, protocol.PickerNavigationFailed)
		// The interaction is already retired: the client drains until an
		// authoritative paint, so publish one even though the handoff failed.
		d.invalidateRender(effect.sess, ac, true, "picker-client-select-failed")
		return
	}
}

// pickerClientTargetCurrent revalidates a resolved target for lifecycle
// freshness: incarnation pin plus expected-created-at, mirroring
// previewTarget/targetMatchesLifecycle. A renamed-or-recreated session
// between snapshot and commit rejects as retired, never force-commits.
func (d *Daemon) pickerClientTargetCurrent(target picker.Target) bool {
	if target.Name == "" {
		d.mu.Lock()
		sess := d.sessions[target.Session]
		d.mu.Unlock()
		if sess == nil {
			return false
		}
		sess.mu.Lock()
		defer sess.mu.Unlock()
		return targetMatchesLifecycle(target, sess.name, sess.createdAt, sess.incarnation)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	_, stopped, isStopped, ok := d.resolveNamedLifecycleTargetLocked(target)
	if !ok {
		return false
	}
	if isStopped {
		return targetMatchesLifecycle(target, stopped.name, stopped.createdAt, stopped.incarnation)
	}
	return true
}

// pickerClientBarrier snapshots the acquisition boundary for one snapshot:
// the output fence epoch, the latest committed state, and the view
// revision that doubles as the size epoch. It reads viewMu-protected
// state only (never pane/session locks, never sendMu — see the lock-order
// notes at the top of client.go); callers must not hold pickerMu while
// calling this, and publishPickerClientSnapshot already released it.
func pickerClientBarrier(ac *attachedClient) (epoch, state, sizeEpoch uint64) {
	if ac == nil {
		return 0, 0, 0
	}
	view := ac.viewSnapshot()
	if ac.output != nil {
		epoch = ac.output.currentEpoch()
		state = ac.output.committedState()
	}
	return epoch, state, view.revision
}

func (d *Daemon) sendPickerClientFailure(effect *attachmentEffect, selection protocol.PickerSelection, code protocol.PickerFailureCode) {
	failure := protocol.PickerFailure{
		CauseActionID: selection.CauseActionID, InteractionID: selection.InteractionID,
		Key: selection.Key, Code: code,
	}
	if protocol.ValidatePickerFailure(failure) != nil {
		return
	}
	_ = effect.sendControl(failure)
}

package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/picker"
)

// This file owns the serving-daemon side of the picker interaction. The
// daemon is the data and action authority: it publishes one structured line
// set per source, resolves opaque keys, and performs the navigation, move, or
// kill the client asked for. It never owns a presentation model.

// servingPickerSourceID names the source the serving daemon itself owns.
const servingPickerSourceID = protocol.PickerServingSourceID

// pickerClientKey derives a stable opaque key for an unchanged navigation
// target (lifecycle, name). Presentation-only updates keep the key; a changed
// or removed target retires it and a reappearing target earns a new key
// through its new lifecycle. Same role as navigationInventoryEntryKey.
func pickerClientKey(lifecycle domain.IncarnationID, name string) string {
	return navigationInventoryEntryKey(lifecycle, name)
}

// retirePicker closes the picker interaction of an attachment that is going
// away. It never sends: the transport is already failing or parked, and the
// client's own lease retires on its generation change.
func (d *Daemon) retirePicker(ac *attachedClient) {
	if ac == nil || ac.overlays == nil {
		return
	}
	ac.overlays.pickerMu.Lock()
	open := ac.overlays.pickerOpen
	interaction := ac.overlays.pickerInteraction
	ac.overlays.pickerMu.Unlock()
	if !open {
		return
	}
	d.closePickerForAttachment(ac, nil, interaction)
}

// pickerState reports the open interaction under the picker mutex.
func (ac *attachedClient) pickerState() (open bool, interaction uint64, intent protocol.PickerIntent, source moveSourceLocator, requestID uint64) {
	if ac == nil || ac.overlays == nil {
		return false, 0, 0, moveSourceLocator{}, 0
	}
	ac.overlays.pickerMu.Lock()
	defer ac.overlays.pickerMu.Unlock()
	return ac.overlays.pickerOpen, ac.overlays.pickerInteraction, ac.overlays.pickerIntent, ac.overlays.pickerMoveSource, ac.overlays.pickerRequestID
}

// openPickerForAttachment admits one picker interaction on a guarded serving
// connection, sends the offer that names the intent and the acquisition
// barrier, and publishes the first serving snapshot. A failed publication
// fails closed: the interaction is retired again so no namespace survives
// without its client-side acquisition.
func (d *Daemon) openPickerForAttachment(ac *attachedClient, effect *attachmentEffect, intent protocol.PickerIntent, source moveSourceLocator, requestID uint64) error {
	if ac == nil || ac.overlays == nil || effect == nil || !effect.current() {
		return errAttachmentTransition
	}
	sess := effect.sess
	if intent != protocol.PickerIntentNavigation {
		if sess == nil || !sess.capabilities().yieldsMoves() {
			return errSessionCannotYieldMoves
		}
		if source.Attachment != nil && source.AttachmentCapability.ac == nil {
			source.AttachmentCapability = sess.captureAttachmentCapability(source.Attachment, source.Attachment.transport())
		}
	}
	ac.overlays.pickerMu.Lock()
	interaction := ac.overlays.pickerInteraction + 1
	if interaction == 0 {
		interaction = 1
	}
	ac.overlays.pickerOpen = true
	ac.overlays.pickerInteraction = interaction
	ac.overlays.pickerIntent = intent
	ac.overlays.pickerMoveSource = source
	ac.overlays.pickerRequestID = requestID
	ac.overlays.pickerRevisions = make(map[string]uint64)
	ac.overlays.pickerKeys = nil
	ac.overlays.pickerPreviewGeneration = 0
	ac.overlays.pickerPreviewKey = ""
	ac.overlays.pickerMu.Unlock()

	barrierEpoch, barrierState, sizeEpoch := pickerClientBarrier(ac)
	offer := protocol.PickerOffer{
		InteractionID: interaction, RequestID: requestID, Intent: intent,
		MoveSourceKey: pickerMoveSourceKey(source), Title: picker.SortRecent.Title(),
		BarrierEpoch: barrierEpoch, BarrierState: barrierState, SizeEpoch: sizeEpoch,
	}
	if protocol.ValidatePickerOffer(offer) != nil {
		d.closePickerForAttachment(ac, effect, interaction)
		return protocol.ErrInvalidNavigation
	}
	if err := effect.sendControl(offer); err != nil {
		d.closePickerForAttachment(ac, effect, interaction)
		return err
	}
	if err := d.publishPickerSourceForAttachment(ac, effect, interaction); err != nil {
		d.closePickerForAttachment(ac, effect, interaction)
		return err
	}
	if sess := ac.currentAttachmentSession(); sess != nil {
		d.invalidateRender(sess, ac, true, "picker.go:open")
	}
	return nil
}

// pickerMoveSourceKey names the captured move source in the offer. It is
// opaque and never a route: only the daemon resolves it, and only for the
// interaction that captured it.
func pickerMoveSourceKey(source moveSourceLocator) string {
	if source.Session.ID == "" || source.TabID == "" {
		return ""
	}
	return pickerClientKey(source.Session.Incarnation, string(source.Session.ID)+"#"+string(source.TabID))
}

// publishPickerSourceForAttachment builds the serving line set, bumps its
// per-source revision, and publishes it. Callers must not hold pickerMu. Every
// refresh publishes a full snapshot with a newer revision: the client may only
// commit the model it is displaying, so a refresh the client has not observed
// must invalidate an older displayed revision even when the rows are equal.
func (d *Daemon) publishPickerSourceForAttachment(ac *attachedClient, effect *attachmentEffect, interaction uint64) error {
	if ac == nil || ac.overlays == nil || effect == nil {
		return errAttachmentTransition
	}
	open, currentInteraction, intent, source, _ := ac.pickerState()
	if !open || currentInteraction != interaction {
		return nil
	}
	sess := effect.sess
	if sess == nil {
		sess = ac.currentAttachmentSession()
	}
	if sess == nil {
		return errAttachmentTransition
	}
	views, current := d.pickerViews(sess, ac)
	set := pickerLineSetFor(views, intent, pickerSourceFilter{
		Session: source.Session.ID, Incarnation: source.Session.Incarnation, TabID: source.TabID,
	}, current)
	if intent != protocol.PickerIntentNavigation && !pickerLineSetHasMove(set) {
		return errNoMoveDestination
	}
	ac.overlays.pickerMu.Lock()
	if !ac.overlays.pickerOpen || ac.overlays.pickerInteraction != interaction {
		ac.overlays.pickerMu.Unlock()
		return nil
	}
	ac.overlays.pickerRevisions[servingPickerSourceID]++
	revision := ac.overlays.pickerRevisions[servingPickerSourceID]
	ac.overlays.pickerKeys = set.keys
	ac.overlays.pickerMu.Unlock()
	snapshot := protocol.PickerSnapshot{
		InteractionID: interaction, SourceID: servingPickerSourceID, SourceRevision: revision,
		Status: protocol.PickerSourceOK, Lines: set.lines, Cursor: set.cursor,
	}
	if protocol.ValidatePickerSnapshot(snapshot) != nil {
		return protocol.ErrInvalidNavigation
	}
	return effect.sendControl(snapshot)
}

// pickerLineSetHasMove reports whether a move line set offers any destination,
// which is what failed move entry must report instead of opening an empty
// picker.
func pickerLineSetHasMove(set pickerLineSet) bool {
	for _, line := range set.lines {
		if line.Actions&protocol.PickerCanMove != 0 {
			return true
		}
	}
	return false
}

// refreshPickerSnapshot republishes the serving line set after its rows, the
// attachment view, or the size changed. It derives a fresh effect from the
// current attachment capability: the effect that opened the interaction is
// bounded to its own operation and may already be retired. A closed
// interaction is a no-op.
func (d *Daemon) refreshPickerSnapshot(ac *attachedClient) {
	open, interaction, _, _, _ := ac.pickerState()
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
	_ = d.publishPickerSourceForAttachment(ac, effect, interaction)
	if sess := ac.currentAttachmentSession(); sess != nil {
		d.invalidateRender(sess, ac, false, "picker.go:refresh")
	}
}

// closePickerForAttachment retires the interaction and confirms it to the
// client with the daemon output boundary the client must display before it
// releases the terminal. Stale closes skip so generations stay ordered. The
// boolean reports whether this call retired the interaction.
func (d *Daemon) closePickerForAttachment(ac *attachedClient, effect *attachmentEffect, interaction uint64) bool {
	if ac == nil || ac.overlays == nil || interaction == 0 {
		return false
	}
	ac.overlays.pickerMu.Lock()
	if !ac.overlays.pickerOpen || ac.overlays.pickerInteraction != interaction {
		ac.overlays.pickerMu.Unlock()
		return false
	}
	previewGeneration := ac.overlays.pickerPreviewGeneration
	ac.overlays.pickerOpen = false
	ac.overlays.pickerKeys = nil
	ac.overlays.pickerRevisions = nil
	ac.overlays.pickerPreviewGeneration = 0
	ac.overlays.pickerPreviewKey = ""
	ac.overlays.pickerMu.Unlock()
	// A retired interaction stops observing: its row is no longer displayed,
	// and a later preview must not resurrect it.
	if previewGeneration != 0 {
		sess := ac.currentAttachmentSession()
		if sess == nil && effect != nil {
			sess = effect.sess
		}
		if coordinator := attachmentRenderCoordinator(sess); coordinator != nil {
			coordinator.teardownPreviewFor(ac, previewGeneration)
		}
	}
	if effect == nil {
		return true
	}
	barrierEpoch, barrierState, _ := pickerClientBarrier(ac)
	closed := protocol.PickerClosed{InteractionID: interaction, BarrierEpoch: barrierEpoch, BarrierState: barrierState}
	if protocol.ValidatePickerClosed(closed) != nil {
		return true
	}
	_ = effect.sendControl(closed)
	return true
}

// resolvePickerSelection validates one typed selection against the open
// interaction and performs the action the owning source authorised. Unknown
// keys, foreign sources, and stale revisions reject with a bounded failure.
func (d *Daemon) resolvePickerSelection(effect *attachmentEffect, selection protocol.PickerSelection) {
	if protocol.ValidatePickerSelection(selection) != nil || effect == nil {
		return
	}
	ac := effect.ac
	if ac == nil || ac.overlays == nil {
		return
	}
	ac.overlays.pickerMu.Lock()
	open := ac.overlays.pickerOpen
	interaction := ac.overlays.pickerInteraction
	intent := ac.overlays.pickerIntent
	source := ac.overlays.pickerMoveSource
	revision := ac.overlays.pickerRevisions[selection.SourceID]
	target, ok := ac.overlays.pickerKeys[selection.Key]
	ac.overlays.pickerMu.Unlock()
	if !open || interaction != selection.InteractionID {
		d.sendPickerFailure(effect, selection, protocol.PickerStaleRevision)
		return
	}
	if selection.SourceID != servingPickerSourceID {
		d.sendPickerFailure(effect, selection, protocol.PickerUnknownSource)
		return
	}
	if selection.SourceRevision != revision {
		d.sendPickerFailure(effect, selection, protocol.PickerStaleRevision)
		return
	}
	if !ok {
		d.sendPickerFailure(effect, selection, protocol.PickerUnknownKey)
		return
	}
	// Navigation commits through the handoff, which must not accept a target
	// whose lifecycle already moved on. Move and kill revalidate source and
	// destination themselves (move_lifecycle.go / killPickerTarget), so they
	// report their own precise rejection instead of a generic retire.
	if selection.Action == protocol.PickerActionNavigate && !d.pickerTargetCurrent(target) {
		d.sendPickerFailure(effect, selection, protocol.PickerRetiredTarget)
		return
	}
	switch selection.Action {
	case protocol.PickerActionKill:
		d.resolvePickerKill(effect, ac, interaction, target, selection)
	case protocol.PickerActionMove:
		d.resolvePickerMove(effect, ac, interaction, intent, source, target, selection)
	default:
		d.resolvePickerNavigate(effect, ac, interaction, target, selection)
	}
}

// resolvePickerNavigate retires the interaction before the handoff. The
// client's release waits for the daemon's own close, and the handoff's
// destination paint has to be that paint: closing afterwards would let it be
// suppressed while the client still owned the terminal.
func (d *Daemon) resolvePickerNavigate(effect *attachmentEffect, ac *attachedClient, interaction uint64, target picker.Target, selection protocol.PickerSelection) {
	if !d.closePickerForAttachment(ac, effect, interaction) {
		d.sendPickerFailure(effect, selection, protocol.PickerStaleRevision)
		return
	}
	if err := d.switchToTargetForAttachment(effect, target, sessionHandoffGuard{allowSamePeer: true}, "picker-select"); err != nil {
		d.sendPickerFailure(effect, selection, protocol.PickerNavigationFailed)
		// The interaction is already retired: the client drains until an
		// authoritative paint, so publish one even though the handoff failed.
		d.invalidateRender(effect.sess, ac, true, "picker-select-failed")
	}
}

// resolvePickerMove commits the move and keeps the picker open, so the client
// can refresh its rows and let the user move another pane or tab. A rejected
// move reports the same precise notice the palette path does: the client only
// sees a bounded failure code.
func (d *Daemon) resolvePickerMove(effect *attachmentEffect, ac *attachedClient, interaction uint64, intent protocol.PickerIntent, source moveSourceLocator, target picker.Target, selection protocol.PickerSelection) {
	if err := d.movePickerSourceError(source); err != nil {
		d.sendPickerFailure(effect, selection, protocol.PickerRetiredTarget)
		d.reportAttachmentError(effect.sess, movePickerUserError(err))
		d.refreshPickerSnapshot(ac)
		return
	}
	if err := d.commitMovePickerSelection(intent, source, target); err != nil {
		d.sendPickerFailure(effect, selection, protocol.PickerActionFailed)
		d.reportAttachmentError(effect.sess, movePickerUserError(err))
		d.refreshPickerSnapshot(ac)
		return
	}
	d.sendPickerResult(effect, selection, interaction)
	d.refreshPickerSnapshot(ac)
}

// resolvePickerKill destroys the target and keeps the picker open.
func (d *Daemon) resolvePickerKill(effect *attachmentEffect, ac *attachedClient, interaction uint64, target picker.Target, selection protocol.PickerSelection) {
	// The kill holds this admission until it finishes, so a replacement
	// initiator cannot make the destructive action look unattributed.
	effect.bindActionEnd(d, "picker-delete")
	if err := d.killPickerTargetForAttachment(target, effect); err != nil {
		d.sendPickerFailure(effect, selection, protocol.PickerActionFailed)
		d.refreshPickerSnapshot(ac)
		return
	}
	d.sendPickerResult(effect, selection, interaction)
	d.refreshPickerSnapshot(ac)
}

// pickerTargetCurrent revalidates a resolved target for lifecycle freshness:
// incarnation pin plus expected-created-at, mirroring previewTarget and
// targetMatchesLifecycle. A renamed-or-recreated session between snapshot and
// commit rejects as retired, never force-commits.
func (d *Daemon) pickerTargetCurrent(target picker.Target) bool {
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

// pickerClientBarrier snapshots the acquisition boundary for one interaction
// offer or close: the output fence epoch, the latest committed state, and the
// view revision that doubles as the size epoch. It reads viewMu-protected
// state only (never pane/session locks, never sendMu — see the lock-order
// notes at the top of client.go); callers must not hold pickerMu.
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

func (d *Daemon) sendPickerResult(effect *attachmentEffect, selection protocol.PickerSelection, interaction uint64) {
	result := protocol.PickerResult{
		CauseActionID: selection.CauseActionID, RequestID: selection.RequestID,
		InteractionID: interaction, SourceID: selection.SourceID, Key: selection.Key, Action: selection.Action,
	}
	if protocol.ValidatePickerResult(result) != nil {
		return
	}
	_ = effect.sendControl(result)
}

func (d *Daemon) sendPickerFailure(effect *attachmentEffect, selection protocol.PickerSelection, code protocol.PickerFailureCode) {
	failure := protocol.PickerFailure{
		CauseActionID: selection.CauseActionID, RequestID: selection.RequestID,
		InteractionID: selection.InteractionID, SourceID: selection.SourceID,
		Key: selection.Key, Action: selection.Action, Code: code,
	}
	if protocol.ValidatePickerFailure(failure) != nil {
		return
	}
	_ = effect.sendControl(failure)
}

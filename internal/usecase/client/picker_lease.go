package client

import "github.com/bnema/vev/internal/protocol"

// This file owns the client-side presentation lease for one client-picker
// interaction. The daemon keeps mutation authority (it resolves opaque
// row keys and performs the handoff); the lease decides who writes the
// terminal: while it is acquiring, owned, or releasing the client
// consumes picker input and never lets a daemon paint overwrite the modal,
// yet every accepted daemon frame is still applied to the output shadow and
// acknowledged inside the bounded window.
//
// The lease is value-oriented and owns no locks: the attach loop is its
// only caller and already owns the terminal writer.

// pickerLeaseState is the client-side presentation state of one
// client-picker interaction.
type pickerLeaseState uint8

const (
	// pickerLeaseClosed displays daemon output normally.
	pickerLeaseClosed pickerLeaseState = iota
	// pickerLeaseAcquiring validated a snapshot whose acquisition barrier
	// (epoch, state) is not applied yet. Daemon frames up to that barrier
	// are still displayed; the picker frame is not.
	pickerLeaseAcquiring
	// pickerLeaseOwned displays the picker frame and consumes picker
	// input. Daemon frames are applied and acknowledged but never written.
	pickerLeaseOwned
	// pickerLeaseReleasing retired the interaction on PickerClose. The
	// picker frame stays until one authoritative full paint arrives; then
	// the client releases the terminal and resumes normal input.
	pickerLeaseReleasing
)

// pickerLeaseEvent reports the presentation transition one accepted daemon
// frame triggers.
type pickerLeaseEvent uint8

const (
	// pickerLeaseIdle displays the frame normally and leaves presentation
	// unchanged.
	pickerLeaseIdle pickerLeaseEvent = iota
	// pickerLeaseSuppress applies and acknowledges the frame without
	// writing it: the client owns the terminal.
	pickerLeaseSuppress
	// pickerLeaseAcquire displays the frame normally, then displays the
	// pending picker frame: the barrier is now applied.
	pickerLeaseAcquire
	// pickerLeaseRelease displays the authoritative full paint normally,
	// then clears the lease.
	pickerLeaseRelease
)

// pickerLease is the client's presentation identity for one
// interaction: attachment UI generation, interaction ID, displayed
// revision, and the acquisition barrier the snapshot was built against.
// A generation change (reconnect) aborts the lease instead of carrying it
// into a new attachment.
type pickerLease struct {
	state        pickerLeaseState
	generation   uint64
	interaction  uint64
	revision     uint64
	barrierEpoch uint64
	barrierState uint64
	sizeEpoch    uint64
	// pending holds the validated model until the barrier is applied; it
	// is nil in every other state.
	pending *pickerLoop
	// releaseAction is the ui-driver action waiting on the release paint
	// and releaseLocal reports whether it completes locally (a cancel
	// consumed by the client) instead of through the daemon handoff.
	releaseAction uint64
	releaseLocal  bool
	// inputHeld reports whether the lease paused foreground input and
	// must resume it when the release completes.
	inputHeld bool
}

// active reports whether the lease owns picker presentation.
func (l *pickerLease) active() bool {
	return l != nil && l.state != pickerLeaseClosed
}

// ownsTerminal reports whether the picker frame, not daemon output, owns
// the terminal: the daemon frame must be applied and acknowledged but
// never written.
func (l *pickerLease) ownsTerminal() bool {
	return l != nil && (l.state == pickerLeaseOwned || l.state == pickerLeaseReleasing)
}

// reset returns the lease to closed presentation.
func (l *pickerLease) reset() {
	*l = pickerLease{}
}

// abort ends the lease without a release paint (disconnect, generation
// change, or a superseding interaction). It reports the pending local
// action that can no longer complete and whether input was held.
func (l *pickerLease) abort() (actionID uint64, local bool, held bool) {
	if l == nil {
		return 0, false, false
	}
	actionID, local, held = l.releaseAction, l.releaseLocal, l.inputHeld
	if actionID != 0 && !local {
		// A handoff-bound action stays unresolved: the daemon owns its
		// outcome and the caller reports the unknown outcome.
		actionID, local = 0, false
	}
	l.reset()
	return actionID, local, held
}

// admitSnapshot admits one validated snapshot against the client's applied
// output state. It reports ready=true when the barrier is already applied
// and the caller must display the picker frame now. replaced reports that a
// previous interaction ended, so its pending release can no longer complete.
//
// A snapshot for the current interaction only replaces the pending model
// with a newer revision, and never after the release started: a superseding
// interaction is a new namespace whose close/full pair cannot release the
// previous one.
func (l *pickerLease) admitSnapshot(snapshot protocol.PickerSnapshot, loop *pickerLoop, applied outputApplyState, generation uint64) (ready, replaced bool) {
	if l == nil || snapshot.InteractionID == 0 {
		return false, false
	}
	if l.active() {
		if l.interaction == snapshot.InteractionID {
			if l.state == pickerLeaseReleasing || snapshot.Revision <= l.revision {
				return false, false
			}
		} else {
			replaced = true
		}
	}
	*l = pickerLease{
		state: pickerLeaseAcquiring, generation: generation, interaction: snapshot.InteractionID,
		revision: snapshot.Revision, barrierEpoch: snapshot.BarrierEpoch, barrierState: snapshot.BarrierState,
		sizeEpoch: snapshot.SizeEpoch, pending: loop,
	}
	if barrierReached(applied, snapshot.BarrierEpoch, snapshot.BarrierState) {
		// The barrier is already displayed: the client owns the terminal
		// from this moment, before any later daemon frame can be written.
		l.state = pickerLeaseOwned
		return true, replaced
	}
	return false, replaced
}

// observe reports the presentation decision for one accepted daemon frame.
// Only accepted frames reach it, so the output dependency chain, reset
// handling, and bounded ACK window keep advancing in every state.
func (l *pickerLease) observe(output protocol.Output, applied outputApplyState) pickerLeaseEvent {
	if l == nil {
		return pickerLeaseIdle
	}
	switch l.state {
	case pickerLeaseAcquiring:
		if barrierReached(applied, l.barrierEpoch, l.barrierState) {
			l.state = pickerLeaseOwned
			return pickerLeaseAcquire
		}
		return pickerLeaseIdle
	case pickerLeaseOwned:
		return pickerLeaseSuppress
	case pickerLeaseReleasing:
		// Only an authoritative full paint releases the terminal: an
		// incremental delta may itself be a suppressed artifact.
		if output.Full {
			return pickerLeaseRelease
		}
		return pickerLeaseSuppress
	}
	return pickerLeaseIdle
}

// beginRelease retires the interaction named by one PickerClose. It reports
// false for a duplicate or foreign close, which must not restart or extend
// the release. The pending action completes after the release paint: local
// for a cancel the client consumed, through the daemon handoff otherwise.
func (l *pickerLease) beginRelease(interaction, actionID uint64, local, holdInput bool) bool {
	if l == nil || !l.active() || l.state == pickerLeaseReleasing || l.interaction != interaction {
		return false
	}
	l.state = pickerLeaseReleasing
	l.pending = nil
	l.releaseAction, l.releaseLocal = actionID, local
	l.inputHeld = holdInput
	return true
}

// finishRelease clears the lease after the release paint was displayed and
// published. It reports the pending local action to complete and whether
// foreground input must resume.
func (l *pickerLease) finishRelease() (actionID uint64, local bool, held bool) {
	if l == nil {
		return 0, false, false
	}
	actionID, local, held = l.releaseAction, l.releaseLocal, l.inputHeld
	l.reset()
	return actionID, local, held
}

// barrierReached reports whether the applied output state covers the
// acquisition barrier (epoch, state). An uninitialized state has applied
// only the implicit empty stream: epoch 1, state 0.
func barrierReached(applied outputApplyState, epoch, state uint64) bool {
	if !applied.initialized {
		return epoch <= 1 && state == 0
	}
	if applied.epoch != epoch {
		return applied.epoch > epoch
	}
	return applied.state >= state
}

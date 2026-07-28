package daemon

import (
	"errors"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/keys"
)

const snatchedInputSequenceLimit = 64

type snatchedInputAction uint8

const (
	snatchedInputNone snatchedInputAction = iota
	snatchedInputReclaim
	snatchedInputQuit
)

// clearSnatchedInput invalidates every parser continuation before stopping its
// timer. Callers must not hold daemon, routing, session, coordinator, send, or
// transport locks because Timer.Stop is an external call.
func (ac *attachedClient) clearSnatchedInput() {
	if ac == nil {
		return
	}
	ac.snatchedInputMu.Lock()
	ac.snatchedInputPending = nil
	ac.snatchedInputDrain = false
	pendingESC := ac.snatchedInputESC
	ac.snatchedInputESC = pendingByteTimer{}
	ac.snatchedInputMu.Unlock()
	pendingESC.stop()
}

// handleSnatchedInput accepts actions only as exact one-byte input events.
// Terminal escape sequences and arbitrary coalesced input are consumed as
// indivisible events so a byte in a paste, sequence final, or frame tail can
// never become an action.
func (d *Daemon) handleSnatchedInput(token attachmentRoleToken, input ports.Input) bool {
	action := d.parseSnatchedInput(token, input.Data)
	switch action {
	case snatchedInputReclaim:
		d.reclaimSnatchedAttachment(token)
	case snatchedInputQuit:
		return d.quitSnatchedAttachment(token)
	}
	return false
}

func (d *Daemon) parseSnatchedInput(token attachmentRoleToken, data []byte) snatchedInputAction {
	ac := token.ac
	if ac == nil || len(data) == 0 {
		return snatchedInputNone
	}

	ac.snatchedInputMu.Lock()

	if ac.snatchedInputDrain {
		if snatchedSequenceFinal(data) >= 0 {
			ac.snatchedInputDrain = false
		}
		ac.snatchedInputMu.Unlock()
		return snatchedInputNone
	}

	if len(ac.snatchedInputPending) != 0 {
		pendingESC := ac.snatchedInputESC
		ac.snatchedInputESC = pendingByteTimer{}
		combined := make([]byte, 0, len(ac.snatchedInputPending)+len(data))
		combined = append(combined, ac.snatchedInputPending...)
		combined = append(combined, data...)
		ac.snatchedInputPending = nil
		d.retainSnatchedSequenceLocked(ac, combined)
		ac.snatchedInputMu.Unlock()
		pendingESC.stop()
		return snatchedInputNone
	}

	if len(data) == 1 {
		var action snatchedInputAction
		switch data[0] {
		case 'r', 'R':
			action = snatchedInputReclaim
		case 'q', 'Q':
			action = snatchedInputQuit
		case keys.ESC:
			ac.snatchedInputPending = append(ac.snatchedInputPending[:0], keys.ESC)
			ac.snatchedInputESC.retain(d.clock, keys.ESCDelay, func(timer ports.Timer) {
				d.fireSnatchedEscape(token, timer)
			})
		}
		ac.snatchedInputMu.Unlock()
		return action
	}

	// A complete/coalesced event is always ignored. Only an incomplete CSI or
	// SS3 prefix is retained to prevent its future final byte from acting.
	d.retainSnatchedSequenceLocked(ac, data)
	ac.snatchedInputMu.Unlock()
	return snatchedInputNone
}

func (d *Daemon) retainSnatchedSequenceLocked(ac *attachedClient, data []byte) {
	if !snatchedSequencePrefix(data) || snatchedSequenceFinal(data[2:]) >= 0 {
		return
	}
	if len(data) > snatchedInputSequenceLimit {
		ac.snatchedInputDrain = true
		return
	}
	ac.snatchedInputPending = append(ac.snatchedInputPending[:0], data...)
}

func snatchedSequencePrefix(data []byte) bool {
	return len(data) >= 2 && data[0] == keys.ESC && (data[1] == '[' || data[1] == 'O')
}

func snatchedSequenceFinal(data []byte) int {
	for i, b := range data {
		if b >= 0x40 && b <= 0x7e {
			return i
		}
	}
	return -1
}

func (d *Daemon) fireSnatchedEscape(token attachmentRoleToken, timer ports.Timer) {
	ac := token.ac
	if ac == nil {
		return
	}
	ac.snatchedInputMu.Lock()
	if ac.snatchedInputESC.timer != timer || len(ac.snatchedInputPending) != 1 || ac.snatchedInputPending[0] != keys.ESC {
		ac.snatchedInputMu.Unlock()
		return
	}
	ac.snatchedInputPending = nil
	ac.snatchedInputESC.timer = nil
	ac.snatchedInputESC.done = nil
	ac.snatchedInputMu.Unlock()

	if d.afterSnatchedEscapeAccepted != nil {
		d.afterSnatchedEscapeAccepted()
	}
	if !token.current() {
		if d.afterSnatchedEscapeAttempt != nil {
			d.afterSnatchedEscapeAttempt(false)
		}
		return
	}
	ticket, admitted := ac.beginRoleEffect(token)
	if d.afterSnatchedEscapeAttempt != nil {
		d.afterSnatchedEscapeAttempt(admitted)
	}
	if !admitted {
		return
	}
	token.effect = ticket
	defer token.endRoleEffect()
	d.quitSnatchedAttachment(token)
}

// acquireActivationBarrier waits for the requester's serialized output path
// without retaining role, daemon, routing, session, coordinator, or pane locks.
// The caller has already frozen and drained the requester's role gate, so no new
// role-bound sender can race this acquisition. A deadline closes only the exact
// captured link, which releases a transport Send currently holding sendMu.
func (d *Daemon) acquireActivationBarrier(token attachmentRoleToken) (func(), error) {
	if token.ac == nil || token.transport.transport == nil || !token.current() {
		return nil, errAttachmentTransition
	}

	acquired := make(chan struct{})
	cancel := make(chan struct{})
	go func() {
		token.ac.sendMu.Lock()
		select {
		case acquired <- struct{}{}:
			// The receiver now owns the matching Unlock obligation.
		case <-cancel:
			token.ac.sendMu.Unlock()
		}
	}()

	timer := d.clock.NewTimer(detachNotifyTimeout)
	select {
	case <-acquired:
		timer.Stop()
		if token.ac.roleGeneration.Load() != token.generation ||
			!token.ac.transportSnapshotCurrent(token.transport) {
			token.ac.sendMu.Unlock()
			return nil, errAttachmentTransition
		}
		return token.ac.sendMu.Unlock, nil
	case <-timer.C():
		close(cancel)
		_ = token.ac.closeCapturedTransport(token.transport.transport)
		return nil, errSendTimedOut
	}
}

func (d *Daemon) reclaimSnatchedAttachment(token attachmentRoleToken) bool {
	if token.role != attachmentSnatched || token.effect == nil {
		return false
	}
	token.effect.bindActionEnd(d, "reclaim-snatched")
	transition, err := d.transitionAttachment(attachmentTransitionRequest{
		source: token.sess, target: token.sess, next: token.ac,
		expectedRole: attachmentSnatched, targetRole: attachmentActive,
		expectedTransport: token.transport, sourceToken: &token,
		action: "reclaim-snatched", ready: true, activationBarrier: true,
	})
	if err != nil {
		if errors.Is(err, errSendTimedOut) {
			d.attachmentCleanupWg.Go(func() { d.parkOrDropSnatchedAttachment(token) })
		} else {
			d.showSnatchedUnavailable(token)
		}
		return false
	}
	d.deferAttachmentTransitionCleanups(transition)
	if d.beforeReclaimFirstPaint != nil {
		d.beforeReclaimFirstPaint(transition.published)
	}
	d.firstPaintForTransition(transition.published)
	return true
}

// showSnatchedUnavailable is a best-effort precommit failure panel. Fresh role
// admission and the generation-aware sender make it inert after a concurrent
// reclaim, quit, transport replacement, or session teardown.
func (d *Daemon) showSnatchedUnavailable(token attachmentRoleToken) {
	if !token.current() {
		return
	}
	fresh := token
	fresh.effect = nil
	ticket, admitted := token.ac.beginRoleEffect(fresh)
	if !admitted {
		return
	}
	fresh.effect = ticket
	defer fresh.endRoleEffect()
	if err := d.sendSnatchedPanel(token.ac, token.transport, token.generation, "Session is no longer available.", ticket); err != nil {
		fresh.endRoleEffect()
		d.parkOrDropSnatchedAttachment(token)
	}
}

func (d *Daemon) quitSnatchedAttachment(token attachmentRoleToken) bool {
	if token.role != attachmentSnatched || token.effect == nil {
		return false
	}
	token.effect.bindActionEnd(d, "quit-snatched")
	_ = d.boundedSendSnatchedControl(token, frameDetached(ports.ReasonDetach))
	// Terminal cleanup freezes the same role gate, so release this action's
	// admission before attempting the generation-bound removal.
	token.endRoleEffect()
	return d.dropSnatchedAttachment(token)
}

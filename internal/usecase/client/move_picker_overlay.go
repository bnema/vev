package client

import (
	"context"
	"fmt"
	"time"

	ansirenderer "github.com/bnema/vev-vt/ansi"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// Move-destination picker over a live attachment (Plan 003 A2).
//
// The serving daemon still owns move semantics: it captures the source when
// MFP/MAT runs, publishes the destination rows in a PickerSnapshot, resolves
// the opaque key of a typed PickerSelection, and confirms the end of the
// interaction with PickerClosed. The attachment worker only presents it: it
// composes the box over the attachment through the foreground's overlay slot,
// decodes the input the overlay owns, and answers with the existing typed
// PickerSelection/PickerClose replies. Zero picker bytes reach the session.
//
// Every method runs on the worker goroutine, which is the overlay's only
// owner, so it holds no lock of its own.

// movePickerInputGeneration is the fixed input generation of the worker-owned
// move overlay; the interaction ID alone names the namespace.
const movePickerInputGeneration = uint64(1)

type movePickerOverlay struct {
	namespace pickerInteraction
	intent    protocol.PickerIntent
	loop      *pickerLoop
	consumer  pickerConsumer
	renderer  *pickerRenderer
	// presenting reports that the foreground overlay slot is held for this
	// interaction.
	presenting bool
}

func newMovePickerOverlay() *movePickerOverlay {
	return &movePickerOverlay{renderer: newPickerRenderer(ansirenderer.ColorProfileTrueColor)}
}

// offer opens the namespace a move offer names. A superseding offer retires
// the previous interaction, whose rows can no longer be committed; the caller
// releases the overlay when superseded reports true.
func (m *movePickerOverlay) offer(offer protocol.PickerOffer) (superseded bool) {
	superseded = m.presenting && m.namespace.interaction != offer.InteractionID
	if superseded {
		m.retire()
	}
	m.namespace.setOpen(true, offer.InteractionID)
	m.namespace.setIntent(offer.InteractionID, offer.Intent)
	m.intent = offer.Intent
	m.loop = nil
	return superseded
}

// admit folds one daemon publication into the model. It reports whether the
// snapshot belongs to the open interaction and is newer than what is shown.
func (m *movePickerOverlay) admit(snapshot protocol.PickerSnapshot) bool {
	if !m.namespace.open || m.intent == protocol.PickerIntentNavigation || m.intent == 0 {
		return false
	}
	if !m.namespace.admitSnapshot(snapshot, true) {
		return false
	}
	if m.loop == nil {
		m.loop = pickerLoopFromSnapshot(snapshot, m.intent, defaultPickerSort())
		m.consumer.setOwned(snapshot.InteractionID, movePickerInputGeneration)
		return true
	}
	m.loop.replaceLines(snapshot)
	return true
}

// interaction names the open namespace, or zero.
func (m *movePickerOverlay) interaction() uint64 {
	if !m.namespace.open {
		return 0
	}
	return m.namespace.interaction
}

// closedBy reports whether a daemon close retires the open interaction.
func (m *movePickerOverlay) closedBy(interaction uint64) bool {
	return m.namespace.open && m.namespace.interaction == interaction
}

// retire ends the interaction locally. A late snapshot for it is refused.
func (m *movePickerOverlay) retire() {
	if m.namespace.open {
		m.namespace.setOpen(false, m.namespace.interaction)
	}
	m.loop = nil
	m.presenting = false
	m.consumer.clear()
}

// input decodes one owned delivery and applies it to the model.
func (m *movePickerOverlay) input(data []byte, actionID uint64) (pickerOp, bool) {
	outcome, consumed := m.consumer.consume(terminalReadResult{data: data, actionID: actionID})
	return m.apply(outcome, consumed)
}

// flush resolves a withheld lone escape once its window elapsed.
func (m *movePickerOverlay) flush() (pickerOp, bool) {
	outcome, consumed := m.consumer.flushPending()
	return m.apply(outcome, consumed)
}

func (m *movePickerOverlay) apply(outcome pickerConsumeOutcome, consumed bool) (pickerOp, bool) {
	if !consumed || m.loop == nil || !outcome.acceptOutcome(m.namespace.interaction, movePickerInputGeneration) {
		return pickerOp{}, false
	}
	return applyPickerBatch(m.loop, outcome.events)
}

// selection builds the typed move for the cursor row, refusing rows the daemon
// did not authorise for a move.
func (m *movePickerOverlay) selection(causeActionID uint64) (protocol.PickerSelection, bool) {
	action, ok := m.loop.selectedAction()
	if !ok || action != protocol.PickerActionMove {
		return protocol.PickerSelection{}, false
	}
	return commitSelection(m.loop, action, causeActionID)
}

// render composes the box for one terminal size.
func (m *movePickerOverlay) render(size domain.Size) []byte {
	if m.loop == nil {
		return nil
	}
	return m.renderer.render(m.loop, size, emptyPickerPreview())
}

// attachmentMovePicker binds the move overlay and the navigation request to
// one attached worker loop: it owns the foreground overlay slot while a move
// interaction is presented and answers the daemon on the worker's stream.
type attachmentMovePicker struct {
	worker  *sessionAttachmentWorker
	fg      AttachmentForeground
	overlay attachmentOverlayForeground
	stream  ports.BrokerLogicalConnection
	size    domain.Size
	move    *movePickerOverlay
	// escapeTimer bounds a withheld lone escape while the move overlay owns
	// input; nil while nothing is withheld.
	escapeTimer ports.Timer
}

// offer handles one daemon PickerOffer. A navigation offer asks the
// supervisor for the client's session picker over this attachment; a move
// offer opens the namespace whose snapshot the overlay will present.
func (p *attachmentMovePicker) offer(ctx context.Context, offer protocol.PickerOffer) error {
	if p.overlay == nil {
		// No overlay seam: never leave the daemon swallowing input for an
		// interaction nothing presents.
		return p.decline(ctx, offer)
	}
	if offer.Intent == protocol.PickerIntentNavigation {
		if p.move.presenting {
			return nil
		}
		p.overlay.requestNavigationPicker()
		return nil
	}
	if p.overlay.overlayKind() == attachmentOverlayNavigation {
		// The session picker owns the terminal; a move picker cannot be
		// presented under it, so release the daemon's interaction at once.
		return p.decline(ctx, offer)
	}
	if p.move.offer(offer) {
		p.stopEscape()
		p.overlay.clearOverlay(attachmentOverlayMove)
	}
	return nil
}

// decline closes a move interaction the client will not present.
func (p *attachmentMovePicker) decline(ctx context.Context, offer protocol.PickerOffer) error {
	if offer.Intent == protocol.PickerIntentNavigation {
		return nil
	}
	return p.worker.send(ctx, p.fg, p.stream, protocol.PickerClose{InteractionID: offer.InteractionID})
}

// snapshot presents one daemon publication. The first admitted snapshot takes
// the foreground overlay slot; a refused slot declines the interaction.
func (p *attachmentMovePicker) snapshot(ctx context.Context, state outputApplyState, snapshot protocol.PickerSnapshot) error {
	if p.overlay == nil || !p.move.admit(snapshot) {
		return nil
	}
	if !p.move.presenting {
		if !p.overlay.setOverlay(attachmentOverlayMove, nil) {
			interaction := p.move.interaction()
			p.move.retire()
			return p.worker.send(ctx, p.fg, p.stream, protocol.PickerClose{InteractionID: interaction})
		}
		p.move.presenting = true
		p.move.renderer.invalidate()
	}
	return p.paint(state)
}

// closed retires the interaction the daemon closed and returns the terminal to
// the attachment.
func (p *attachmentMovePicker) closed(interaction uint64) {
	if !p.move.closedBy(interaction) {
		return
	}
	p.release()
}

func (p *attachmentMovePicker) release() {
	presenting := p.move.presenting
	p.move.retire()
	p.stopEscape()
	if presenting && p.overlay != nil {
		p.overlay.clearOverlay(attachmentOverlayMove)
	}
}

// consumeInput offers one authorized delivery to the overlay that owns input.
// It reports true when the bytes were consumed and must not reach the session.
func (p *attachmentMovePicker) consumeInput(ctx context.Context, state outputApplyState, input AttachmentInputEvent) (bool, error) {
	if p.move.presenting {
		p.stopEscape()
		op, changed := p.move.input(input.Data, input.actionID)
		if err := p.applyOp(ctx, state, op, changed); err != nil {
			return true, err
		}
		p.armEscape()
		return true, nil
	}
	if p.overlay != nil && p.overlay.divertInput(input.Data) {
		return true, nil
	}
	return false, nil
}

// applyOp carries out one decoded decision: close sends the typed close and
// releases the terminal, commit sends the typed move, and a changed model
// repaints the box.
func (p *attachmentMovePicker) applyOp(ctx context.Context, state outputApplyState, op pickerOp, changed bool) error {
	if !p.move.presenting {
		return nil
	}
	if op.close {
		interaction := p.move.interaction()
		p.release()
		return p.worker.send(ctx, p.fg, p.stream, protocol.PickerClose{InteractionID: interaction})
	}
	if op.commit {
		if selection, ok := p.move.selection(0); ok {
			if err := p.worker.send(ctx, p.fg, p.stream, selection); err != nil {
				return err
			}
		}
	}
	if changed {
		return p.paint(state)
	}
	return nil
}

// resize repaints the presented box at the new terminal size.
func (p *attachmentMovePicker) resize(_ context.Context, state outputApplyState, size domain.Size) error {
	p.size = size
	if !p.move.presenting {
		return nil
	}
	return p.paint(state)
}

func (p *attachmentMovePicker) paint(state outputApplyState) error {
	frame := p.move.render(p.size)
	if len(frame) == 0 {
		return nil
	}
	uiContext := state.uiContext(ports.UIContext{Generation: attachmentActionableGeneration(p.fg, p.fg.Token())}, ports.UIStatusAttached)
	if err := p.overlay.overlayOutput(uiContext, frame); err != nil {
		return fmt.Errorf("vev: publishing move picker: %w", err)
	}
	return nil
}

// escape is the armed lone-escape window, or nil.
func (p *attachmentMovePicker) escape() <-chan time.Time {
	if p.escapeTimer == nil {
		return nil
	}
	return p.escapeTimer.C()
}

func (p *attachmentMovePicker) armEscape() {
	if p.escapeTimer != nil || !p.move.presenting || !p.move.consumer.hasPending() {
		return
	}
	timer := p.worker.cfg.Clock.NewTimer(pickerEscapeDeadline)
	if supervisorNil(timer) {
		return
	}
	p.escapeTimer = timer
}

// escapeFired forgets the timer whose window just elapsed.
func (p *attachmentMovePicker) escapeFired() { p.escapeTimer = nil }

func (p *attachmentMovePicker) stopEscape() {
	if p.escapeTimer == nil {
		return
	}
	stopSupervisorTimer(p.escapeTimer)
	p.escapeTimer = nil
}

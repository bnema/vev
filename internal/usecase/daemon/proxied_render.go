package daemon

import (
	"github.com/bnema/vev/internal/ports"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/pkg/renderer"
)

// paintProxiedContent publishes only the remote session's content surface. It
// deliberately bypasses bars, pane title chrome, overlays, notices, and every
// terminal-global side-effect path used by a full terminal attachment.
func (d *Daemon) paintProxiedContent(entry *session, ac *attachedClient, reset bool, lease *attachmentLease) paintResult {
	if entry == nil || ac == nil {
		return paintRejected
	}
	marks := d.newRuntimeMarkBatch()
	defer marks.flush()
	if lease != nil {
		token := attachmentToken(entry, ac, ac.transport())
		token.lease = lease
		ticket, admitted := ac.beginAttachmentEffect(token)
		if !admitted {
			return paintRejected
		}
		marks.attachmentEffect = ticket
		defer ticket.End()
	}

	ac.sendMu.Lock()
	entry.core().mu.Lock()
	_, owned := entry.core().attachments[ac]
	entry.core().mu.Unlock()
	if !owned || ac.currentAttachmentSession() != entry {
		ac.sendMu.Unlock()
		return paintRejected
	}
	if lease != nil {
		rc := attachmentRenderCoordinator(entry)
		if rc == nil || lease.attachment != ac || !rc.leaseCurrent(lease, true) {
			ac.sendMu.Unlock()
			return paintRejected
		}
	}
	if ac.output != nil {
		ac.output.syncCapacityLocked()
		if ac.output.atCapacity() {
			ac.sendMu.Unlock()
			return paintBlockedCapacity
		}
	}
	if ac.proxiedOutputStarted {
		failedTransport, err := d.sendProxiedMetadataLocked(entry, ac, &marks)
		if err != nil {
			ac.sendMu.Unlock()
			if marks.attachmentEffect == nil {
				d.detachOnSendError(entry, ac, failedTransport)
			} else {
				token := marks.attachmentEffect.connectionToken()
				launchCleanup := d.reserveAttachmentSendErrorCleanup(token, failedTransport)
				marks.attachmentEffect.End()
				launchCleanup()
			}
			return paintRejected
		}
	}

	state, ok := captureLocalRenderState(entry, ac, renderCaptureRequest{reset: reset, lease: lease})
	if !ok {
		ac.sendMu.Unlock()
		return paintRejected
	}
	composed := composeProxiedContent(*state)
	if !d.emitFrame(entry, ac, state, composed, &marks) {
		return paintRejected
	}
	return paintEmitted
}

// sendProxiedMetadataLocked emits a newer authoritative snapshot before every
// post-handshake proxied Output. Caller holds ac.sendMu and no architecture
// lock; metadata capture itself completes before the transport write.
func (d *Daemon) sendProxiedMetadataLocked(entry *session, ac *attachedClient, marks *runtimeMarkBatch) (ports.Transport, error) {
	if entry == nil || ac == nil || ac.proxiedMetaRevision == ^uint64(0) {
		return nil, errAttachmentTransition
	}
	meta, err := frameSessionMeta(entry, ac, ac.proxiedMetaRevision+1)
	if err != nil {
		return nil, err
	}
	expected := ac.transportSnapshot()
	if expected.transport == nil || !ac.transportSnapshotCurrent(expected) {
		return expected.transport, errTransportReplaced
	}
	if marks != nil && marks.attachmentEffect != nil {
		if !marks.attachmentEffect.beginTransportSend(expected) {
			return expected.transport, errAttachmentTransition
		}
		defer marks.attachmentEffect.endTransportSend()
	}
	if err := expected.transport.Send(meta); err != nil {
		if marks != nil && marks.attachmentEffect != nil {
			marks.attachmentEffect.reportTransportFailure(expected)
		}
		return expected.transport, err
	}
	ac.proxiedMetaRevision++
	return expected.transport, nil
}

// composeProxiedContent retains the remote terminal layout but excludes every
// daemon UI layer. Pane frames are copied at their content placements, so the
// emitted size is exactly the proxied content geometry.
func composeProxiedContent(state capturedRenderState) composedRenderFrame {
	area := state.layout.area
	// The proxied attachment owns the remote content window. Shared layout may
	// lag an accepted resize or be driven by another local attachment, so never
	// derive the remote wire surface from it when the attachment supplied a
	// valid content geometry.
	if state.window.Valid() {
		area.Width, area.Height = state.window.Cols, state.window.Rows
	}
	if area.Width <= 0 || area.Height <= 0 {
		return composedRenderFrame{frame: renderer.NewFrame(0, 0), cursor: cursorOut{hidden: true}, reset: state.reset}
	}
	frame := renderer.NewFrame(area.Width, area.Height)
	full := state.reset || state.floating.visible
	damage := make([]renderer.Damage, 0, len(state.panes))
	for _, pane := range state.panes {
		if pane.placement.Collapsed || pane.placement.Content.Width <= 0 || pane.placement.Content.Height <= 0 {
			continue
		}
		blitPaneFrame(frame, pane.placement.Content, pane.frame, false, themeui.NewDimmer(themeui.Theme{}))
		if len(pane.damage) != 0 {
			damage = append(damage, pane.damage...)
		}
	}
	if state.floating.visible {
		// A floating pane is the active terminal surface. Proxied mode omits its
		// local chrome/border but must preserve its content and cursor target.
		blitPaneFrame(frame, state.floating.geometry.Inner, state.floating.pane.frame, false, themeui.NewDimmer(themeui.Theme{}))
	}
	if full {
		damage = []renderer.Damage{renderer.FullRedraw()}
	}
	return composedRenderFrame{
		frame:  frame,
		damage: damage,
		cursor: desiredCapturedCursor(state.cursor, 0),
		reset:  state.reset,
	}
}

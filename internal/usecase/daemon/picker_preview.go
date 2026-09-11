package daemon

import (
	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/picker"
)

// This file owns the serving-daemon side of the picker preview: the client
// names the row it displays, the daemon captures that row's viewport and keeps
// publishing it while the row's content changes. Capture reuses the navigation
// preview facts (previewTarget for local sessions, the remote preview cache for
// remote rows), so the client never asks a second time for a refresh it did not
// cause.

// handlePickerPreviewForAttachment answers one client preview request. The
// request supersedes the viewer's previous preview: a newer generation replaces
// the render subscription, so only the displayed row keeps publishing.
func (d *Daemon) handlePickerPreviewForAttachment(effect *attachmentEffect, request protocol.PickerPreviewRequest) {
	if effect == nil || effect.ac == nil || effect.ac.overlays == nil {
		return
	}
	if protocol.ValidatePickerPreviewRequest(request) != nil {
		return
	}
	ac := effect.ac
	open, interaction, intent, _, _ := ac.pickerState()
	if !open || interaction != request.InteractionID || request.SourceID != servingPickerSourceID {
		return
	}
	ac.overlays.pickerMu.Lock()
	target, known := ac.overlays.pickerKeys[request.Key]
	ac.overlays.pickerPreviewGeneration++
	generation := ac.overlays.pickerPreviewGeneration
	ac.overlays.pickerPreviewKey = request.Key
	ac.overlays.pickerMu.Unlock()

	if coordinator := attachmentRenderCoordinator(effect.sess); coordinator != nil {
		coordinator.subscribePreviewFor(ac, generation, func(renderWake) {
			d.publishPickerPreviewForAttachment(ac, request, generation, target, intent, known)
		})
	}
	d.publishPickerPreviewForAttachment(ac, request, generation, target, intent, known)
}

// publishPickerPreviewForAttachment captures the requested row and sends one
// preview when the request is still the displayed one. A superseding request, a
// closed interaction, or an attachment that moved on all publish nothing.
func (d *Daemon) publishPickerPreviewForAttachment(ac *attachedClient, request protocol.PickerPreviewRequest, generation uint64, target picker.Target, intent protocol.PickerIntent, known bool) {
	if ac == nil || ac.overlays == nil {
		return
	}
	ac.overlays.pickerMu.Lock()
	current := ac.overlays.pickerOpen && ac.overlays.pickerInteraction == request.InteractionID &&
		ac.overlays.pickerPreviewGeneration == generation && ac.overlays.pickerPreviewKey == request.Key
	ac.overlays.pickerMu.Unlock()
	if !current {
		return
	}
	sess := ac.currentAttachmentSession()
	if sess == nil {
		return
	}
	_, effect, admitted := ac.beginCurrentAttachmentEffect(sess, ac.transportSnapshot().transport)
	if !admitted {
		return
	}
	defer effect.End()

	preview := protocol.PickerPreview{
		Version: protocol.PickerPreviewSchemaVersion, InteractionID: request.InteractionID,
		SourceID: request.SourceID, Key: request.Key,
	}
	if !known {
		preview.Status = protocol.PickerPreviewNoSuchTarget
	} else {
		viewport := d.capturePickerPreview(target, intent, request.Width, request.Height)
		preview.Status, preview.Width, preview.Height, preview.Cells = viewport.Status, viewport.Width, viewport.Height, viewport.Cells
	}
	if protocol.ValidatePickerPreview(preview) != nil {
		return
	}
	_ = effect.sendControl(preview)
}

// capturePickerPreview resolves one picker row to its displayed viewport. A row
// whose session or tab moved on, or whose remote fetch failed, reports
// unavailable instead of a stale frame.
func (d *Daemon) capturePickerPreview(target picker.Target, intent protocol.PickerIntent, width, height uint16) protocol.PickerPreview {
	unavailable := protocol.PickerPreview{
		Version: protocol.PickerPreviewSchemaVersion, Status: protocol.PickerPreviewUnavailable,
	}
	if target.RemoteTarget != nil {
		remote, err := d.awaitRemotePreviewRefresh(d.remotePreviewContext(), *target.RemoteTarget, width, height)
		if err != nil || remote.Status != protocol.RemotePreviewOK {
			return unavailable
		}
		if remote.Width == 0 || remote.Height == 0 || len(remote.Cells) == 0 {
			return unavailable
		}
		return protocol.PickerPreview{
			Version: protocol.PickerPreviewSchemaVersion, Status: protocol.PickerPreviewOK,
			Width: remote.Width, Height: remote.Height, Cells: remote.Cells,
		}
	}
	sess, tb := d.previewTarget(target, intent)
	if sess == nil || tb == nil {
		return unavailable
	}
	return clampPickerPreview(snapshotPickerPreview(tb), width, height)
}

// clampPickerPreview fits a captured viewport into the requested bounds. The
// wire rejects a cell run whose continuation columns disagree with the row, so a
// double-width cell cut at the right edge becomes a blank cell instead.
func clampPickerPreview(preview picker.Preview, width, height uint16) protocol.PickerPreview {
	unavailable := protocol.PickerPreview{
		Version: protocol.PickerPreviewSchemaVersion, Status: protocol.PickerPreviewUnavailable,
	}
	if preview.Width <= 0 || preview.Height <= 0 || len(preview.Rows) == 0 || width == 0 || height == 0 {
		return unavailable
	}
	columns := int(width)
	rows := preview.Rows
	if len(rows) > int(height) {
		rows = rows[:int(height)]
	}
	cells := make([]renderer.Cell, 0, len(rows)*columns)
	for _, row := range rows {
		if len(row) > columns {
			row = row[:columns]
			// A cropped double-width cell loses its continuation column.
			if len(row) > 0 && renderer.RuneWidth(row[len(row)-1].Rune) == 2 {
				row = append(append([]renderer.Cell(nil), row[:len(row)-1]...), renderer.Cell{})
			}
		}
		cells = append(cells, row...)
		for len(cells)%columns != 0 {
			cells = append(cells, renderer.Cell{})
		}
	}
	fitted := protocol.PickerPreview{
		Version: protocol.PickerPreviewSchemaVersion, Status: protocol.PickerPreviewOK,
		Width: width, Height: uint16(len(rows)), Cells: cells,
	}
	if protocol.ValidatePickerPreviewViewport(fitted.Width, fitted.Height, fitted.Cells) != nil {
		return unavailable
	}
	return fitted
}

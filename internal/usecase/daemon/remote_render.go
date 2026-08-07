package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

const remoteContentPaneID = "remote-content"

type remoteViewRenderSnapshot struct {
	frame     renderer.Frame
	cursor    vt.CursorSnapshot
	title     string
	tabs      []ports.SessionTabMeta
	activeID  domain.TabStableID
	origin    string
	linkState remoteViewLinkState
}

// resizeScreen updates only the private remote VT surface. The attachment
// owns the outer window and callers must not route this through local session
// geometry or PTY resize machinery.
func (v *remoteView) resizeScreen(size domain.Size) bool {
	if v == nil || !size.Valid() {
		return false
	}
	content := contentSize(size)
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return false
	}
	if v.screen == nil {
		v.screen = vt.NewScreen(content.Cols, content.Rows)
	} else if v.screen.Frame.Width != content.Cols || v.screen.Frame.Height != content.Rows {
		v.screen.Resize(content.Cols, content.Rows)
	}
	return true
}

// renderSnapshot is the remote-content ownership boundary. It copies every
// mutable VT and metadata value while view.mu is held, then releases that lock
// before any attachment, overlay, renderer, or transport work begins.
func (v *remoteView) renderSnapshot(size domain.Size) (remoteViewRenderSnapshot, bool) {
	if v == nil || !size.Valid() {
		return remoteViewRenderSnapshot{}, false
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return remoteViewRenderSnapshot{}, false
	}
	if v.screen == nil {
		v.screen = vt.NewScreen(size.Cols, size.Rows)
	}
	// The shared remote screen follows the latest valid content-size claim via
	// resizeScreen. Rendering another local attachment must only capture it;
	// resizing here would let a non-authoritative window corrupt the remote
	// output chain without sending a matching remote resize.
	screen := v.screen.Snapshot()
	frame := renderer.NewFrame(screen.Columns(), screen.Rows())
	for y := range frame.Height {
		copy(frame.Row(y), screen.BorrowedRow(y))
	}
	return remoteViewRenderSnapshot{
		frame:     frame,
		cursor:    screen.Cursor(),
		title:     screen.Title(),
		tabs:      append([]ports.SessionTabMeta(nil), v.metadata.Tabs...),
		activeID:  v.metadata.ActiveTabID,
		origin:    v.displayOrigin,
		linkState: v.linkState,
	}, true
}

// invalidateAttachedOwner repaints an attachment through its actual owner.
// Local UI overlays remain attachment-local while remote content is active,
// so they must never enqueue work on the source session's coordinator after
// that attachment has moved to a remote view.
func (d *Daemon) invalidateAttachedOwner(ac *attachedClient, reset bool, producer string) {
	if d == nil || ac == nil {
		return
	}
	owner := ac.currentAttachmentOwner()
	if view, remote := owner.(*remoteView); remote {
		token := attachmentOwnerToken(view, ac, ac.transport())
		if token.ac != nil {
			go d.paintRemoteView(view, ac, reset, token)
		}
		return
	}
	if sess := localSession(owner); sess != nil {
		d.invalidateRender(sess, ac, reset, producer)
	}
}

func remoteStatusSnapshot(view *remoteView, snap remoteViewRenderSnapshot) statusSnapshot {
	name := ""
	if view != nil {
		name = view.key.sessionName
		if snap.origin == "" {
			snap.origin = view.key.endpoint
		}
	}
	if snap.origin != "" {
		name += "@" + snap.origin
	}
	switch snap.linkState {
	case remoteViewLinkReconnecting:
		name += " · reconnecting"
	case remoteViewLinkUnavailable:
		name += " · unavailable"
	}
	status := statusSnapshot{session: name, tabs: make([]statusTab, 0, max(1, len(snap.tabs)))}
	for _, tab := range snap.tabs {
		status.tabs = append(status.tabs, statusTab{
			name:      tab.Name,
			active:    tab.ID == snap.activeID,
			attention: tab.Attention,
		})
	}
	if len(status.tabs) == 0 {
		tabName := "1"
		if view != nil && view.key.sessionName != "" {
			tabName = view.key.sessionName
		}
		status.tabs = append(status.tabs, statusTab{name: tabName, paneTitle: snap.title, active: true})
	}
	return status
}

// captureRemoteViewRenderState builds one local chrome composition from an
// immutable private remote VT snapshot. The one synthetic pane is deliberately
// content-only: local pane titles, layout, and PTY state cannot leak into a
// remote view.
func (d *Daemon) captureRemoteViewRenderState(view *remoteView, ac *attachedClient, request renderCaptureRequest) (*capturedRenderState, bool) {
	if view == nil || ac == nil {
		return nil, false
	}
	window := ac.sizeSnapshot()
	content := contentSize(window)
	snapshot, ok := view.renderSnapshot(content)
	if !ok || !attachmentOwnerRegistered(view, ac) || !sameAttachmentOwner(ac.currentAttachmentOwner(), view) {
		return nil, false
	}
	placement := domain.Rect{Width: content.Cols, Height: content.Rows}
	bars := request.bars
	bars.status = remoteStatusSnapshot(view, snapshot)
	if d != nil {
		bars.attentionFrame = d.attentionFrame()
		bars.mru = d.recentSessions(nil)
	}
	state := &capturedRenderState{
		attachment:         ac,
		lease:              request.lease,
		window:             window,
		reset:              request.reset,
		bars:               bars,
		theme:              bars.theme,
		styles:             request.styles,
		styleGeneration:    request.styleGeneration,
		overlays:           request.overlays,
		preview:            request.preview,
		layout:             capturedTabLayout{area: placement, focus: remoteContentPaneID, valid: true},
		floatingGeneration: 0,
		panes: []capturedPaneRenderState{{
			id:        remoteContentPaneID,
			frame:     snapshot.frame,
			placement: layout.Placement{ID: remoteContentPaneID, Content: placement},
			focused:   true,
			damage:    []renderer.Damage{renderer.FullRedraw()},
		}},
		cursor: capturedCursorInputs{
			row:        snapshot.cursor.Row,
			col:        snapshot.cursor.Col,
			style:      snapshot.cursor.Style,
			hasStyle:   snapshot.cursor.StyleSet,
			visible:    snapshot.cursor.Visible,
			renderable: placement.Width > 0 && placement.Height > 0,
			content:    placement,
		},
	}
	return state, true
}

// paintRemoteView composes daemon-owned bars and overlays over the private
// remote terminal image. It uses the same output-state stream as local
// rendering, but never consults local session, pane, or coordinator state.
func (d *Daemon) paintRemoteView(view *remoteView, ac *attachedClient, reset bool, token attachmentConnectionToken) paintResult {
	if view == nil || ac == nil || !sameAttachmentOwner(token.owner, view) {
		return paintRejected
	}
	ticket, admitted := ac.beginAttachmentEffect(token)
	if !admitted {
		return paintRejected
	}
	token.effect = ticket
	marks := d.newRuntimeMarkBatch()
	marks.attachmentEffect = ticket
	defer func() {
		ticket.End()
		marks.flush()
	}()

	ac.sendMu.Lock()
	if !token.attachmentEffectCurrent() || ac.output == nil {
		ac.sendMu.Unlock()
		return paintRejected
	}
	ac.output.syncCapacityLocked()
	if ac.output.atCapacity() {
		ac.sendMu.Unlock()
		return paintBlockedCapacity
	}
	ac.initOverlays()
	overlays := ac.overlays.SnapshotForRender()
	defer overlays.Unlock()
	statusFeedback := overlays.statusFeedback
	if statusFeedback == "" && overlays.resizeActive {
		statusFeedback = "resize unavailable for remote view"
	}
	applied := ac.getAppliedTheme()
	bars := barState{statusFeedback: statusFeedback, theme: applied.Raw}
	capturedOverlays := capturedOverlayRenderState{
		copyActive: overlays.copyActive, copySearchActive: overlays.copySearchModel != nil,
		pickerActive: overlays.pickerActive, paletteActive: overlays.paletteActive, promptActive: overlays.promptActive,
		resizeActive: overlays.resizeActive, statusFeedback: statusFeedback,
	}
	state, ok := d.captureRemoteViewRenderState(view, ac, renderCaptureRequest{
		bars:            bars,
		overlays:        capturedOverlays,
		preview:         snapshotPickerPreview(nil),
		styles:          applied.Resolved.Styles,
		styleGeneration: applied.Generation,
		reset:           reset,
	})
	if !ok {
		ac.sendMu.Unlock()
		return paintRejected
	}
	captureOverlayLayers(state, overlays, d.currentPaletteConfig())
	composed := composeFrame(*state, ac.pipelineCache, ac.pipelineScratch)
	if d.emitAttachmentFrame(view, ac, state, composed, &marks) {
		return paintEmitted
	}
	return paintRejected
}

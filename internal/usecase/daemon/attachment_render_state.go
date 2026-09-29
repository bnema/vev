package daemon

// attachmentRenderState is one attachment's composition state: what the last
// emitted frame looked like and the per-pane captures it was built from. The
// paint transaction owns it through attachedClient.sendMu; every other path
// changes it only through the transitions below, so the rules for when a
// composition or a pane snapshot may be reused live in one place.
type attachmentRenderState struct {
	// cache is the last successfully emitted composition. spare is its
	// alternate buffer; the two must never share mutable backing storage.
	cache composeCacheInput
	spare composeCacheInput
	// capture is reusable scratch for one capture pass.
	capture renderCaptureScratch
	// panes is keyed by pane ownership, not the tab-local PaneID, so snapshots
	// cannot leak when an attachment switches tabs or sessions.
	panes map[*pane]capturedPaneRenderState
}

// commitComposition records a successfully emitted composition and recycles
// the previous one as the next spare buffer.
func (r *attachmentRenderState) commitComposition(composed composeCacheInput) {
	r.spare = r.cache
	r.cache = composed
}

// forgetComposition forces the next frame to be composed from scratch.
func (r *attachmentRenderState) forgetComposition() {
	r.cache = composeCacheInput{}
	r.spare = composeCacheInput{}
}

// forgetAllPanes drops every pane capture snapshot.
func (r *attachmentRenderState) forgetAllPanes() { r.panes = nil }

// forgetPanes drops the capture snapshots of panes that left this attachment.
func (r *attachmentRenderState) forgetPanes(panes ...*pane) {
	for _, p := range panes {
		delete(r.panes, p)
	}
}

// paneSnapshot returns the previous capture of p, if any.
func (r *attachmentRenderState) paneSnapshot(p *pane) capturedPaneRenderState {
	return r.panes[p]
}

// storePaneSnapshot retains the latest capture of p for the next pass.
func (r *attachmentRenderState) storePaneSnapshot(p *pane, captured capturedPaneRenderState) {
	if r.panes == nil {
		r.panes = make(map[*pane]capturedPaneRenderState)
	}
	r.panes[p] = captured
}

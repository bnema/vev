package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/pkg/renderer"
)

const proxyRenderPaneID layout.PaneID = "proxy-content"

type proxyCapture struct {
	screen     *proxyScreenState
	frame      renderer.Frame
	damage     []renderer.Damage
	generation uint64
	cursor     capturedCursorInputs
}

// capturePrimary snapshots the remote VT while holding only proxy.mu. The
// snapshot is then composed by the ordinary local frame pipeline, which owns
// the bars, overlays, cursor, and renderer shadow. Remote ANSI is therefore
// never handed to a renderer.
func (p *proxySession) capturePrimary(ac *attachedClient, request primaryCaptureRequest) (*capturedRenderState, bool) {
	if p == nil || ac == nil {
		return nil, false
	}

	p.mu.Lock()
	if p.screen == nil {
		p.mu.Unlock()
		return nil, false
	}
	screen := p.screen
	capture := &ac.proxyCapture
	damage := screen.CaptureDamage()
	if capture.screen != screen {
		// A replacement screen has no relationship to the retained frame.
		capture.frame = renderer.Frame{}
	}
	if capture.screen != screen || capture.generation != damage.Generation || capture.frame.Validate() != nil {
		screen.CaptureInto(&capture.frame)
	}
	capture.screen = screen
	capture.generation = damage.Generation
	capture.damage = append(capture.damage[:0], damage.Damage...)
	frame := capture.frame
	capture.cursor = capturedCursorInputs{
		row:        int(screen.cursorOut.Row),
		col:        int(screen.cursorOut.Col),
		style:      int(screen.cursorOut.Style),
		hasStyle:   screen.cursorOut.StyleSet,
		visible:    screen.cursorOut.Visible,
		renderable: frame.Width > 0 && frame.Height > 0,
		content:    domain.Rect{Width: frame.Width, Height: frame.Height},
	}
	cursor := capture.cursor
	paneDamage := append([]renderer.Damage(nil), capture.damage...)
	p.mu.Unlock()

	content := domain.Rect{Width: frame.Width, Height: frame.Height}
	if uncertainDamage(paneDamage, frame.Width, frame.Height) {
		paneDamage = []renderer.Damage{renderer.FullRedraw()}
	}
	state := &capturedRenderState{
		attachment:      ac,
		lease:           request.lease,
		reset:           request.reset,
		layout:          capturedTabLayout{area: content, focus: proxyRenderPaneID, valid: true, fingerprint: "proxy"},
		bars:            request.bars,
		theme:           request.bars.theme,
		styles:          request.styles,
		styleGeneration: request.styleGeneration,
		overlays:        request.overlays,
		preview:         request.preview,
		cursor:          cursor,
		receipts:        []damageReceipt{{proxy: p, proxyScreen: screen, generation: damage.Generation}},
		panes: []capturedPaneRenderState{{
			id:        proxyRenderPaneID,
			frame:     frame,
			damage:    paneDamage,
			placement: layout.Placement{ID: proxyRenderPaneID, Content: content},
			focused:   true,
		}},
	}
	return state, true
}

// firstProxyPaintWithLease emits the same full first paint as a local
// transition, after making the remote content geometry match the local viewport.
func (d *Daemon) firstProxyPaintWithLease(p *proxySession, ac *attachedClient, lease *attachmentLease) {
	if p == nil || ac == nil {
		return
	}
	_, _ = p.resize(ac.size)
	if rc := attachmentRenderCoordinator(p); rc != nil {
		if rc.invalidateForLease(ac, lease, renderInvalidation{class: invalidateUrgent, reset: true, producer: "proxy_render.go"}) {
			rc.fireCurrent(false)
		}
		return
	}
	d.invalidateRenderNow(p, ac, true, "proxy_render.go")
}

// resizeProxyForLease accepts a local client resize only for its exact active
// attachment lease. Local chrome is repainted whenever the local content
// rectangle moved, including when the remote never learned about it: the local
// VT has already been resized either way, so skipping the repaint would leave
// the client showing chrome for the old geometry.
func (d *Daemon) resizeProxyForLease(p *proxySession, ac *attachedClient, lease *attachmentLease, size domain.Size) {
	if p == nil || ac == nil || !size.Valid() {
		return
	}
	if rc := attachmentRenderCoordinator(p); rc != nil && !rc.leaseCurrent(lease, false) {
		return
	}
	if changed, _ := p.resize(size); !changed {
		return
	}
	ac.sendMu.Lock()
	ac.size = size
	ac.sendMu.Unlock()
	if rc := attachmentRenderCoordinator(p); rc != nil {
		rc.invalidateForLease(ac, lease, renderInvalidation{class: invalidateUrgent, reset: true, producer: "proxy_render.go"})
		return
	}
	d.invalidateRender(p, ac, true, "proxy_render.go")
}

// resize updates the local VT to the local content rectangle and sends that
// reduced geometry once to the remote daemon. The sender performs transport
// I/O only after proxy.mu has been released.
//
// changed reports whether the local content rectangle actually moved, which is
// what local chrome repaints depend on. sent reports whether the remote was
// told. The two are deliberately separate: a failed send leaves the local VT
// already resized, so the attached client still owes a repaint.
func (p *proxySession) resize(size domain.Size) (changed, sent bool) {
	if p == nil || !size.Valid() {
		return false, false
	}
	content := contentSize(size, false)
	if !validProxyScreenSize(content) {
		return false, false
	}
	p.mu.Lock()
	if p.screen == nil || p.contentSize == content {
		available := p.screen != nil
		p.mu.Unlock()
		return false, available
	}
	p.screen.ResizePlaceholder(content)
	p.contentSize = content
	// A resize invalidates the local replay chain, but the last applied state
	// remains the moving snapshot floor for the replacement geometry.
	p.screenReady = false
	p.resetRequested = false
	p.screen.stateNum = 0
	generation := p.linkGeneration
	p.mu.Unlock()

	return true, p.sendGeneration(generation, ports.Frame{
		Type:    ports.MsgResize,
		Payload: ports.MarshalResize(ports.Resize{Size: content}),
	}) == nil
}

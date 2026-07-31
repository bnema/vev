package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/pkg/renderer"
)

const proxyRenderPaneID layout.PaneID = "proxy-content"

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
	frame := screen.Frame.Clone()
	damage := screen.CaptureDamage()
	cursorStyle, hasCursorStyle := screen.CursorStyle()
	cursor := capturedCursorInputs{
		row:        screen.CursorRow(),
		col:        screen.CursorCol(),
		style:      cursorStyle,
		hasStyle:   hasCursorStyle,
		visible:    screen.CursorVisible(),
		renderable: frame.Width > 0 && frame.Height > 0,
		content:    domain.Rect{Width: frame.Width, Height: frame.Height},
	}
	p.mu.Unlock()

	content := domain.Rect{Width: frame.Width, Height: frame.Height}
	paneDamage := append([]renderer.Damage(nil), damage.Damage...)
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
	p.mu.Lock()
	if p.screen == nil || p.contentSize == content {
		available := p.screen != nil
		p.mu.Unlock()
		return false, available
	}
	p.screen.Resize(content.Cols, content.Rows)
	p.contentSize = content
	p.resetOutputStateLocked()
	generation := p.linkGeneration
	p.mu.Unlock()

	return true, p.sendGeneration(generation, ports.Frame{
		Type:    ports.MsgResize,
		Payload: ports.MarshalResize(ports.Resize{Size: content}),
	}) == nil
}

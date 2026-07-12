package daemon

import (
	"github.com/bnema/vev/internal/domain"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/pkg/renderer"
)

// composeFloatingFrame applies the popup to a copy of the normal composed
// destination. In particular it never writes the VT screen frame: that frame
// is shared with the PTY reader and is the source for future renders.
func composeFloatingFrame(base renderer.Frame, baseDamage []renderer.Damage, p *pane, generation uint64, content domain.Rect, desired floatingGeometry, layoutSnap tabLayoutSnapshot, theme themeui.Theme, cache *composedFrameCache, full bool) (renderer.Frame, []renderer.Damage, floatingGeometry) {
	var frame renderer.Frame
	if cache != nil && cache.floatingFrame.Width == base.Width && cache.floatingFrame.Height == base.Height {
		frame = cache.floatingFrame
		for y := 0; y < base.Height; y++ {
			copy(frame.Row(y), base.Row(y))
		}
	} else {
		frame = base.Clone()
		if cache != nil {
			cache.floatingFrame = frame
		}
	}
	(overlayBackdrop{DimPaneContents: true}).apply(frame, content, layoutSnap, theme)
	// The PTY reader mutates both the frame and its damage under p.mu. Keep
	// every read of them, including the blit, in the same critical section.
	// In particular, Frame is not safe to retain as an alias after unlocking.
	p.mu.Lock()
	// Committed popup geometry is content-relative; translate it exactly once
	// while holding the same lock that protects the committed state and frame.
	geometry := p.committedFloatingGeometryLocked(desired).translate(content.X, content.Y)
	previousCapture := capturedPaneRenderState{}
	if cache != nil {
		previousCapture = cache.floatingCaptured
	}
	captured := capturePaneRenderStateLockedInto(p, geometry.Inner, damageCaptureConsume, previousCapture)
	if cache != nil {
		cache.floatingCaptured = captured
	}
	p.mu.Unlock()
	title := captured.title
	titleGeneration := captured.titleGeneration
	blitPaneFrame(frame, geometry.Inner, captured.frame, false, theme)
	damage := append([]renderer.Damage(nil), baseDamage...)
	for _, d := range captured.damage {
		damage = append(damage, translatePaneDamage(d, geometry.Inner, content)...)
	}

	// The committed geometry, rather than the requested config geometry, is
	// what was actually rendered. Track it so movement (including a same-size
	// resize) cannot reuse a cached popup at its former position.
	popupChanged := cache == nil || cache.floating != p || cache.floatingGeneration != generation || cache.floatingGeometry != geometry
	titleChanged := popupChanged || cache == nil || cache.floatingTitleGeneration != titleGeneration
	drawFloatingBorder(frame, geometry.Bounds, title, newThemeStyles(theme).border)
	if cache != nil {
		cache.floating = p
		cache.floatingGeneration = generation
		cache.floatingGeometry = geometry
		cache.floatingTitleGeneration = titleGeneration
	}
	if full || popupChanged {
		return frame, []renderer.Damage{renderer.FullRedraw()}, geometry
	}
	if titleChanged && geometry.Bounds.Height >= 3 {
		damage = append(damage, renderer.Damage{Kind: renderer.DamageText, X: geometry.Bounds.X, Y: geometry.Bounds.Y, Width: geometry.Bounds.Width, Height: 1})
	}
	return frame, damage, geometry
}

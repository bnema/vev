package daemon

import (
	"github.com/bnema/vev/internal/domain"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
)

// floatingGeometry describes a popup's outer frame and terminal content area.
// Committed geometry is tab-content-relative; Bounds and Inner are both relative
// to the supplied content rectangle.
type floatingGeometry struct {
	Bounds domain.Rect
	Inner  domain.Rect
}

func (g floatingGeometry) valid() bool {
	return g.Bounds.Width > 0 && g.Bounds.Height > 0 && g.Inner.Width > 0 && g.Inner.Height > 0
}

func (g floatingGeometry) translate(dx, dy int) floatingGeometry {
	g.Bounds.X += dx
	g.Bounds.Y += dy
	g.Inner.X += dx
	g.Inner.Y += dy
	return g
}

// committedFloatingGeometryLocked returns the geometry whose PTY resize was
// last committed. Newly constructed test panes fall back to the requested
// geometry until their first resize. The caller holds p.mu.
func (p *pane) committedFloatingGeometryLocked(fallback floatingGeometry) floatingGeometry {
	if p.popupGeometry.valid() {
		return p.popupGeometry
	}
	return fallback
}

// floatingAxisGeometry is the canonical percentage and border calculation for
// one popup axis. Popups smaller than three cells spend no cells on borders.
type floatingAxisGeometry struct {
	BoundsSize   int
	InnerSize    int
	BorderOffset int
}

func calculateFloatingAxisGeometry(available, percent int) floatingAxisGeometry {
	percent = min(max(percent, 1), 100)
	bounds := min(max(available*percent/100, 1), available)
	axis := floatingAxisGeometry{BoundsSize: bounds, InnerSize: bounds}
	if bounds >= 3 {
		axis.InnerSize -= 2
		axis.BorderOffset = 1
	}
	return axis
}

// calculateFloatingGeometry returns coordinates in the supplied rectangle's
// space and centers a percentage-sized popup in content. Launch sizing derives
// from Inner too, so rendering and PTY geometry share
// the same per-axis percentage and tiny-border rules.
func calculateFloatingGeometry(content domain.Rect, cfg domain.FloatingConfig) floatingGeometry {
	if content.Width <= 0 || content.Height <= 0 {
		return floatingGeometry{}
	}
	x := calculateFloatingAxisGeometry(content.Width, cfg.Width)
	y := calculateFloatingAxisGeometry(content.Height, cfg.Height)
	bounds := domain.Rect{
		X:      content.X + (content.Width-x.BoundsSize)/2,
		Y:      content.Y + (content.Height-y.BoundsSize)/2,
		Width:  x.BoundsSize,
		Height: y.BoundsSize,
	}
	return floatingGeometry{
		Bounds: bounds,
		Inner: domain.Rect{
			X:      bounds.X + x.BorderOffset,
			Y:      bounds.Y + y.BorderOffset,
			Width:  x.InnerSize,
			Height: y.InnerSize,
		},
	}
}

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
	title := p.displayTitleLocked()
	titleGeneration := p.title.generation
	screenDamage := p.screen.Damage()
	blitPaneFrame(frame, geometry.Inner, p.screen.Frame, false, theme)
	damage := append([]renderer.Damage(nil), baseDamage...)
	for _, d := range screenDamage {
		damage = append(damage, translatePaneDamage(d, geometry.Inner, content)...)
	}
	p.screen.ClearDamage()
	p.mu.Unlock()

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

func drawFloatingBorder(frame renderer.Frame, bounds domain.Rect, title string, style renderer.Style) {
	if bounds.Width <= 0 || bounds.Height <= 0 {
		return
	}
	// Each axis omits its borders independently for tiny popups, matching
	// floatingInnerSize and leaving every cell available to the terminal.
	if bounds.Height >= 3 {
		for x := bounds.X; x < bounds.X+bounds.Width; x++ {
			frame.Set(x, bounds.Y, renderer.Cell{Rune: '─', Style: style})
			frame.Set(x, bounds.Y+bounds.Height-1, renderer.Cell{Rune: '─', Style: style})
		}
	}
	if bounds.Width >= 3 {
		for y := bounds.Y; y < bounds.Y+bounds.Height; y++ {
			frame.Set(bounds.X, y, renderer.Cell{Rune: '│', Style: style})
			frame.Set(bounds.X+bounds.Width-1, y, renderer.Cell{Rune: '│', Style: style})
		}
	}
	if bounds.Width >= 3 && bounds.Height >= 3 {
		frame.Set(bounds.X, bounds.Y, renderer.Cell{Rune: '┌', Style: style})
		frame.Set(bounds.X+bounds.Width-1, bounds.Y, renderer.Cell{Rune: '┐', Style: style})
		frame.Set(bounds.X, bounds.Y+bounds.Height-1, renderer.Cell{Rune: '└', Style: style})
		frame.Set(bounds.X+bounds.Width-1, bounds.Y+bounds.Height-1, renderer.Cell{Rune: '┘', Style: style})
		ui.DrawText(frame, bounds.X+2, bounds.Y, bounds.X+bounds.Width-2, title, style)
	}
}

// resizeFloatingPane serializes PTY resizes for a floating pane. PTY.Resize
// is intentionally outside pane.mu; failed resizes do not publish any state.
func (d *Daemon) resizeFloatingPane(p *pane, geometry floatingGeometry) bool {
	inner := geometry.Inner
	if p == nil || !geometry.valid() {
		return false
	}
	p.resizeMu.Lock()
	defer p.resizeMu.Unlock()

	p.mu.Lock()
	old := p.rect
	pty := p.pty
	p.mu.Unlock()
	if old.Width == inner.Width && old.Height == inner.Height {
		// Position and outer bounds can change while the terminal dimensions do
		// not. Commit that geometry without needlessly resetting the PTY/screen.
		p.mu.Lock()
		p.rect = inner
		p.popupGeometry = geometry
		p.mu.Unlock()
		return true
	}
	if pty != nil {
		if err := pty.Resize(rectSize(inner)); err != nil {
			d.log.Warn("floating pty resize failed", "err", err)
			return false
		}
	}
	p.mu.Lock()
	p.screen.Resize(inner.Width, inner.Height)
	p.rect = inner
	p.popupGeometry = geometry
	p.mu.Unlock()
	return true
}

// resizeInstalledFloating updates an installed floating pane, including a
// retained hidden pane when its tab becomes active.
func (d *Daemon) resizeInstalledFloating(tb *tab) bool {
	if d == nil || tb == nil {
		return false
	}
	tb.mu.Lock()
	p := tb.floating.pane
	content := domain.Rect{Width: tb.size.Cols, Height: tb.size.Rows}
	tb.mu.Unlock()
	if p == nil {
		return false
	}
	return d.resizeFloatingPane(p, calculateFloatingGeometry(content, d.currentFloatingConfig()))
}

// resizeActiveFloating updates a visible floating pane during a client resize.
func (d *Daemon) resizeActiveFloating(tb *tab) bool {
	if d == nil || tb == nil {
		return false
	}
	tb.mu.Lock()
	visible := tb.floating.state == floatingVisible
	tb.mu.Unlock()
	if !visible {
		return false
	}
	return d.resizeInstalledFloating(tb)
}

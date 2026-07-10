package daemon

import (
	"github.com/bnema/vev/internal/domain"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
)

// floatingGeometry describes a popup's outer frame and terminal content area.
// Bounds and Inner are both relative to the supplied content rectangle.
type floatingGeometry struct {
	Bounds domain.Rect
	Inner  domain.Rect
}

// calculateFloatingGeometry centers a percentage-sized popup in content. Its
// terminal area deliberately uses floatingInnerSize so launch and rendering
// always share the same border and percentage rules.
func calculateFloatingGeometry(content domain.Rect, cfg domain.FloatingConfig) floatingGeometry {
	if content.Width <= 0 || content.Height <= 0 {
		return floatingGeometry{}
	}
	bounds := domain.Rect{
		Width:  floatingBoundsAxis(content.Width, cfg.Width),
		Height: floatingBoundsAxis(content.Height, cfg.Height),
	}
	bounds.X = content.X + (content.Width-bounds.Width)/2
	bounds.Y = content.Y + (content.Height-bounds.Height)/2
	innerSize := floatingInnerSize(domain.Size{Cols: bounds.Width, Rows: bounds.Height}, domain.FloatingConfig{Width: 100, Height: 100})
	inner := domain.Rect{X: bounds.X, Y: bounds.Y, Width: innerSize.Cols, Height: innerSize.Rows}
	if bounds.Width >= 3 {
		inner.X++
	}
	if bounds.Height >= 3 {
		inner.Y++
	}
	return floatingGeometry{Bounds: bounds, Inner: inner}
}

func floatingBoundsAxis(available, percent int) int {
	percent = min(max(percent, 1), 100)
	return min(max(available*percent/100, 1), available)
}

// composeFloatingFrame applies the popup to a copy of the normal composed
// destination. In particular it never writes the VT screen frame: that frame
// is shared with the PTY reader and is the source for future renders.
func composeFloatingFrame(base renderer.Frame, baseDamage []renderer.Damage, p *pane, generation uint64, content domain.Rect, cfg domain.FloatingConfig, layoutSnap tabLayoutSnapshot, theme themeui.Theme, cache *composedFrameCache, full bool) (renderer.Frame, []renderer.Damage) {
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
	geometry := calculateFloatingGeometry(content, cfg)

	popupChanged := cache == nil || cache.floating != p || cache.floatingGeneration != generation

	// The PTY reader mutates both the frame and its damage under p.mu. Keep
	// every read of them, including the blit, in the same critical section.
	// In particular, Frame is not safe to retain as an alias after unlocking.
	p.mu.Lock()
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

	titleChanged := popupChanged || cache == nil || cache.floatingTitleGeneration != titleGeneration
	drawFloatingBorder(frame, geometry.Bounds, title, newThemeStyles(theme).border)
	if cache != nil {
		cache.floating = p
		cache.floatingGeneration = generation
		cache.floatingTitleGeneration = titleGeneration
	}
	if full || popupChanged {
		return frame, []renderer.Damage{renderer.FullRedraw()}
	}
	if titleChanged && geometry.Bounds.Height >= 3 {
		damage = append(damage, renderer.Damage{Kind: renderer.DamageText, X: geometry.Bounds.X, Y: geometry.Bounds.Y, Width: geometry.Bounds.Width, Height: 1})
	}
	return frame, damage
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
func (d *Daemon) resizeFloatingPane(p *pane, inner domain.Rect) bool {
	if p == nil || inner.Width <= 0 || inner.Height <= 0 {
		return false
	}
	p.resizeMu.Lock()
	defer p.resizeMu.Unlock()

	p.mu.Lock()
	old := p.rect
	pty := p.pty
	p.mu.Unlock()
	if old == inner {
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
	p.mu.Unlock()
	return true
}

// resizeActiveFloating updates only a visible floating pane. Hidden panes
// retain their remembered geometry until they are shown again.
func (d *Daemon) resizeActiveFloating(tb *tab) bool {
	if d == nil || tb == nil {
		return false
	}
	tb.mu.Lock()
	visible := tb.floating.state == floatingVisible
	p := tb.floating.pane
	content := domain.Rect{Width: tb.size.Cols, Height: tb.size.Rows}
	tb.mu.Unlock()
	if !visible || p == nil {
		return false
	}
	return d.resizeFloatingPane(p, calculateFloatingGeometry(content, d.currentFloatingConfig()).Inner)
}

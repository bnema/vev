package daemon

import "github.com/bnema/vev/internal/domain"

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

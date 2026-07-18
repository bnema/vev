package daemon

import (
	"github.com/bnema/vev/internal/domain"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
)

// floatingGeometry describes a popup's outer frame and terminal content area.
// Bounds and Inner always share the same coordinate space. Committed pane
// geometry is origin-zero and tab-content-relative; composeCapturedFloatingFrame
// translates both fields and returns/caches frame-absolute geometry. Callers
// must not translate a composed or cached value again.
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

// calculateContentFloatingGeometry returns origin-zero, tab-content-relative
// coordinates for a percentage-sized popup. Launch sizing derives from Inner
// too, so rendering and PTY geometry share the same per-axis percentage and
// tiny-border rules.
func calculateContentFloatingGeometry(content domain.Size, cfg domain.FloatingConfig) floatingGeometry {
	if !content.Valid() {
		return floatingGeometry{}
	}
	x := calculateFloatingAxisGeometry(content.Cols, cfg.Width)
	y := calculateFloatingAxisGeometry(content.Rows, cfg.Height)
	bounds := domain.Rect{
		X:      (content.Cols - x.BoundsSize) / 2,
		Y:      (content.Rows - y.BoundsSize) / 2,
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

type floatingComposeInput struct {
	baseFrame    renderer.Frame
	baseDamage   []renderer.Damage
	floating     capturedFloatingRenderState
	content      domain.Rect
	layout       capturedTabLayout
	theme        themeui.Theme
	borderMuted  renderer.Style
	borderActive renderer.Style
	cache        composeCacheInput
	full         bool
}

func composeCapturedFloatingFrame(input floatingComposeInput) (renderer.Frame, []renderer.Damage) {
	base := input.baseFrame
	baseDamage := input.baseDamage
	floating := input.floating
	content := input.content
	layoutSnap := input.layout
	theme := input.theme
	borderMuted := input.borderMuted
	borderActive := input.borderActive
	cache := input.cache
	full := input.full

	frame := base.Clone()
	layoutSnapshot := tabLayoutSnapshot{placements: layoutSnap.placements, area: layoutSnap.area, focus: layoutSnap.focus, ok: layoutSnap.valid}
	(overlayBackdrop{DimPaneContents: true}).apply(frame, content, layoutSnapshot, theme)
	geometry := floating.geometry.translate(content.X, content.Y)
	blitPaneFrame(frame, geometry.Inner, floating.pane.frame, false, themeui.NewDimmer(theme))
	damage := append([]renderer.Damage(nil), baseDamage...)
	for _, d := range floating.pane.damage {
		damage = append(damage, translatePaneDamage(d, geometry.Inner, content)...)
	}
	popupChanged := !cache.valid || cache.floatingGeneration != floating.generation || cache.floatingGeometry != geometry || cache.floatingFocused != floating.focused
	titleChanged := popupChanged || cache.floatingTitleGeneration != floating.titleGeneration
	border := borderMuted
	if floating.focused {
		border = borderActive
	}
	drawFloatingBorder(frame, geometry.Bounds, floating.title, border)
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

package daemon

import (
	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
)

// floatingGeometry describes a popup's outer frame and terminal content area.
// Bounds and Inner always share the same coordinate space. Committed pane
// geometry is origin-zero and tab-content-relative; composeCapturedFloatingFrame
// translates both fields and returns/caches frame-absolute geometry. Callers
// must not translate a composed or cached value again.
type floatingGeometry struct {
	Mode   ui.PresentationMode
	Bounds domain.Rect
	Inner  domain.Rect
}

func (g floatingGeometry) valid() bool {
	return g.Bounds.Width > 0 && g.Bounds.Height > 0 && g.Inner.Width > 0 && g.Inner.Height > 0
}

// committable accepts a drawer with no terminal content. Its presentation must
// remain canonical even though the PTY and VT screen use a 1x1 fallback.
func (g floatingGeometry) committable() bool {
	if g.Mode == ui.PresentationDrawer {
		return g.Bounds.Width > 0 && g.Bounds.Height >= 0 && g.Inner.Width > 0 && g.Inner.Height >= 0
	}
	return g.valid()
}

func (g floatingGeometry) ptyRect() domain.Rect {
	if g.Inner.Width > 0 && g.Inner.Height > 0 {
		return g.Inner
	}
	return domain.Rect{Width: 1, Height: 1}
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
	if p.popupGeometry.committable() {
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

// calculatePreferredFloatingGeometry retains the canonical percentage and
// tiny-axis rules used by wide floating terminals.
func calculatePreferredFloatingGeometry(content domain.Size, cfg domain.FloatingConfig) floatingGeometry {
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
		Mode:   ui.PresentationFloating,
		Bounds: bounds,
		Inner: domain.Rect{
			X:      bounds.X + x.BorderOffset,
			Y:      bounds.Y + y.BorderOffset,
			Width:  x.InnerSize,
			Height: y.InnerSize,
		},
	}
}

// calculateContentFloatingGeometry returns canonical tab-content-relative
// coordinates. Presentation resolution consumes complete-frame coordinates,
// so the preferred geometry crosses the top-bar boundary exactly once in each
// direction.
func calculateContentFloatingGeometry(content domain.Size, cfg domain.FloatingConfig) floatingGeometry {
	preferred := calculatePreferredFloatingGeometry(content, cfg)
	if !preferred.valid() {
		return floatingGeometry{}
	}
	framePreferred := preferred.translate(0, 1)
	presentation := ui.ResolvePresentation(
		domain.Size{Cols: content.Cols, Rows: content.Rows + 2},
		framePreferred.Bounds,
		framePreferred.Inner,
	)
	return floatingGeometry{
		Mode:   presentation.Mode,
		Bounds: presentation.Bounds,
		Inner:  presentation.Inner,
	}.translate(0, -1)
}

type floatingComposeInput struct {
	baseFrame    renderer.Frame
	baseDamage   []renderer.Damage
	floating     capturedFloatingRenderState
	content      domain.Rect
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
	theme := input.theme
	borderMuted := input.borderMuted
	borderActive := input.borderActive
	cache := input.cache
	full := input.full

	frame := base.Clone()
	applyOverlayBackdrop(frame, theme)
	geometry := floating.geometry.translate(content.X, content.Y)
	blitClippedFloatingPane(frame, geometry.Inner, floating.pane.frame)
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
	drawFloatingBorder(frame, geometry, floating.title, border)
	if full || popupChanged {
		return frame, []renderer.Damage{renderer.FullRedraw()}
	}
	hasTitleBorder := geometry.Bounds.Height >= 3
	if geometry.Mode == ui.PresentationDrawer {
		hasTitleBorder = geometry.Bounds.Height > 0
	}
	if titleChanged && hasTitleBorder {
		if titleDamage, ok := clippedFloatingTitleDamage(frame, geometry.Bounds); ok {
			damage = append(damage, titleDamage)
		}
	}
	return frame, damage
}

type floatingSpan struct {
	source, destination, length int
}

// clippedFloatingSpan intersects a source span placed at origin with the
// destination interval [0, limit). The source offset preserves pane cell
// mapping when the retained committed geometry starts above or left of frame.
func clippedFloatingSpan(origin, length, limit int) (floatingSpan, bool) {
	if length <= 0 || limit <= 0 {
		return floatingSpan{}, false
	}
	if origin < 0 {
		if origin <= -length {
			return floatingSpan{}, false
		}
		trim := -origin
		length -= trim
		origin = 0
		if length <= 0 {
			return floatingSpan{}, false
		}
		return floatingSpan{source: trim, destination: origin, length: min(length, limit)}, true
	}
	if origin >= limit {
		return floatingSpan{}, false
	}
	length = min(length, limit-origin)
	return floatingSpan{destination: origin, length: length}, length > 0
}

func blitClippedFloatingPane(dst renderer.Frame, rect domain.Rect, src renderer.Frame) {
	x, ok := clippedFloatingSpan(rect.X, min(rect.Width, src.Width), dst.Width)
	if !ok {
		return
	}
	y, ok := clippedFloatingSpan(rect.Y, min(rect.Height, src.Height), dst.Height)
	if !ok {
		return
	}
	for dy := range y.length {
		for dx := range x.length {
			dst.Set(x.destination+dx, y.destination+dy, src.At(x.source+dx, y.source+dy))
		}
	}
}

func clippedFloatingTitleDamage(frame renderer.Frame, bounds domain.Rect) (renderer.Damage, bool) {
	if bounds.Y < 0 || bounds.Y >= frame.Height {
		return renderer.Damage{}, false
	}
	x, ok := clippedFloatingSpan(bounds.X, bounds.Width, frame.Width)
	if !ok {
		return renderer.Damage{}, false
	}
	return renderer.Damage{Kind: renderer.DamageText, X: x.destination, Y: bounds.Y, Width: x.length, Height: 1}, true
}

func drawFloatingBorder(frame renderer.Frame, geometry floatingGeometry, title string, style renderer.Style) {
	bounds := geometry.Bounds
	if bounds.Width <= 0 || bounds.Height <= 0 {
		return
	}
	if geometry.Mode == ui.PresentationDrawer {
		drawFloatingHorizontalEdge(frame, bounds.X, bounds.Width, bounds.Y, '─', style)
		drawFloatingTitle(frame, bounds, title, style)
		return
	}
	// Each axis omits its borders independently for tiny popups, matching
	// floatingInnerSize and leaving every cell available to the terminal.
	right, hasRight := floatingLastCoordinate(bounds.X, bounds.Width)
	bottom, hasBottom := floatingLastCoordinate(bounds.Y, bounds.Height)
	if bounds.Height >= 3 {
		drawFloatingHorizontalEdge(frame, bounds.X, bounds.Width, bounds.Y, '─', style)
		if hasBottom {
			drawFloatingHorizontalEdge(frame, bounds.X, bounds.Width, bottom, '─', style)
		}
	}
	if bounds.Width >= 3 {
		drawFloatingVerticalEdge(frame, bounds.X, bounds.Y, bounds.Height, '│', style)
		if hasRight {
			drawFloatingVerticalEdge(frame, right, bounds.Y, bounds.Height, '│', style)
		}
	}
	if bounds.Width >= 3 && bounds.Height >= 3 && hasRight && hasBottom {
		setFloatingCell(frame, bounds.X, bounds.Y, '┌', style)
		setFloatingCell(frame, right, bounds.Y, '┐', style)
		setFloatingCell(frame, bounds.X, bottom, '└', style)
		setFloatingCell(frame, right, bottom, '┘', style)
		drawFloatingTitle(frame, bounds, title, style)
	}
}

func drawFloatingHorizontalEdge(frame renderer.Frame, x, width, y int, glyph rune, style renderer.Style) {
	if y < 0 || y >= frame.Height {
		return
	}
	span, ok := clippedFloatingSpan(x, width, frame.Width)
	if !ok {
		return
	}
	for dx := range span.length {
		frame.Set(span.destination+dx, y, renderer.Cell{Rune: glyph, Style: style})
	}
}

func drawFloatingVerticalEdge(frame renderer.Frame, x, y, height int, glyph rune, style renderer.Style) {
	if x < 0 || x >= frame.Width {
		return
	}
	span, ok := clippedFloatingSpan(y, height, frame.Height)
	if !ok {
		return
	}
	for dy := range span.length {
		frame.Set(x, span.destination+dy, renderer.Cell{Rune: glyph, Style: style})
	}
}

func setFloatingCell(frame renderer.Frame, x, y int, glyph rune, style renderer.Style) {
	if x < 0 || x >= frame.Width || y < 0 || y >= frame.Height {
		return
	}
	frame.Set(x, y, renderer.Cell{Rune: glyph, Style: style})
}

func floatingLastCoordinate(origin, size int) (int, bool) {
	if size <= 0 {
		return 0, false
	}
	last := origin + size - 1
	if last < origin {
		return 0, false
	}
	return last, true
}

func drawFloatingTitle(frame renderer.Frame, bounds domain.Rect, title string, style renderer.Style) {
	const inset = 2
	titleWidth := bounds.Width - 2*inset
	if titleWidth <= 0 || bounds.X > int(^uint(0)>>1)-inset {
		return
	}
	titleX := bounds.X + inset
	span, ok := clippedFloatingSpan(titleX, titleWidth, frame.Width)
	if !ok {
		return
	}
	ui.DrawText(frame, titleX, bounds.Y, span.destination+span.length, title, style)
}

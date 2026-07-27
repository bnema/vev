package ui

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/renderer"
)

// Modal describes a rectangular overlay.
type Modal struct {
	WidthPct    int
	HeightPct   int
	MinWidth    int
	MinHeight   int
	Title       string
	Anchor      domain.Anchor
	Margins     Margins
	FixedWidth  int
	FixedHeight int
}

// Bounds returns the modal rectangle positioned within base and clamped to base.
func (m Modal) Bounds(base domain.Size) domain.Rect {
	width := percentOf(base.Cols, m.WidthPct)
	height := percentOf(base.Rows, m.HeightPct)
	if m.FixedWidth > 0 {
		width = m.FixedWidth
	}
	if m.FixedHeight > 0 {
		height = m.FixedHeight
	}
	if width < m.MinWidth {
		width = m.MinWidth
	}
	if height < m.MinHeight {
		height = m.MinHeight
	}
	return Place(base, domain.Size{Cols: width, Rows: height}, m.Anchor, m.Margins)
}

// Inner returns the modal content rectangle after removing a one-cell border.
func (m Modal) Inner(base domain.Size) domain.Rect {
	return modalInner(m.Bounds(base))
}

// Resolve computes the modal's responsive presentation from one preferred
// bounds and inner pair.
func (m Modal) Resolve(base domain.Size) Presentation {
	bounds := m.Bounds(base)
	return ResolvePresentation(base, bounds, modalInner(bounds))
}

// Composite draws the modal border and title, fills its interior, and returns
// the inner content rectangle. Border and interior styles are deliberately
// independent so unfocused structure and chrome surfaces retain their roles.
// Cells outside the modal bounds are not changed.
func (m Modal) Composite(f renderer.Frame, border, interior renderer.Style) domain.Rect {
	bounds := m.Bounds(domain.Size{Cols: f.Width, Rows: f.Height})
	inner := modalInner(bounds)
	DrawBox(f, bounds, border)
	FillRect(f, inner, renderer.Cell{Rune: ' ', Style: interior})
	m.drawTitle(f, bounds, border)
	return inner
}

// CompositePresentation draws an already-resolved modal presentation and
// returns its inner content rectangle.
func (m Modal) CompositePresentation(f renderer.Frame, p Presentation, border, interior renderer.Style) domain.Rect {
	FillRect(f, p.Inner, renderer.Cell{Rune: ' ', Style: interior})
	drawBorderEdges(f, p.Bounds, p.Borders, border)
	if p.Borders&BorderTop != 0 {
		m.drawTitle(f, p.Bounds, border)
	}
	return p.Inner
}

func (m Modal) drawTitle(f renderer.Frame, bounds domain.Rect, style renderer.Style) {
	if bounds.Width <= 2 || bounds.Height <= 0 || m.Title == "" {
		return
	}
	left := bounds.X + 1
	right := bounds.X + bounds.Width - 1
	start := max(left, bounds.X+(bounds.Width-textWidth(m.Title))/2)
	DrawText(f, start, bounds.Y, right, m.Title, style)
}

func modalInner(bounds domain.Rect) domain.Rect {
	return domain.Rect{
		X:      bounds.X + 1,
		Y:      bounds.Y + 1,
		Width:  max(0, bounds.Width-2),
		Height: max(0, bounds.Height-2),
	}
}

// FillRect fills rect with cell, clipped to the frame bounds.
func FillRect(f renderer.Frame, rect domain.Rect, cell renderer.Cell) {
	left := clamp(rect.X, 0, f.Width)
	top := clamp(rect.Y, 0, f.Height)
	right := clamp(rect.X+rect.Width, 0, f.Width)
	bottom := clamp(rect.Y+rect.Height, 0, f.Height)
	for y := top; y < bottom; y++ {
		for x := left; x < right; x++ {
			f.Set(x, y, cell)
		}
	}
}

// DrawBox draws a single-cell box clipped to the frame bounds.
func DrawBox(f renderer.Frame, rect domain.Rect, style renderer.Style) {
	drawBorderEdges(f, rect, BorderAll, style)
}

func drawBorderEdges(f renderer.Frame, rect domain.Rect, edges BorderEdges, style renderer.Style) {
	if rect.Width <= 0 || rect.Height <= 0 || edges == 0 {
		return
	}
	left := rect.X
	top := rect.Y
	right := rect.X + rect.Width - 1
	bottom := rect.Y + rect.Height - 1

	if edges&BorderTop != 0 {
		for x := left; x <= right; x++ {
			setCell(f, x, top, '─', style)
		}
	}
	if edges&BorderBottom != 0 {
		for x := left; x <= right; x++ {
			setCell(f, x, bottom, '─', style)
		}
	}
	if edges&BorderLeft != 0 {
		for y := top; y <= bottom; y++ {
			setCell(f, left, y, '│', style)
		}
	}
	if edges&BorderRight != 0 {
		for y := top; y <= bottom; y++ {
			setCell(f, right, y, '│', style)
		}
	}
	if edges&(BorderTop|BorderLeft) == BorderTop|BorderLeft {
		setCell(f, left, top, '┌', style)
	}
	if edges&(BorderTop|BorderRight) == BorderTop|BorderRight {
		setCell(f, right, top, '┐', style)
	}
	if edges&(BorderBottom|BorderLeft) == BorderBottom|BorderLeft {
		setCell(f, left, bottom, '└', style)
	}
	if edges&(BorderBottom|BorderRight) == BorderBottom|BorderRight {
		setCell(f, right, bottom, '┘', style)
	}
}

// DrawText draws text starting at x,y before exclusive clipX and returns the
// next x position. Wide runes that would cross clipX are dropped.
func DrawText(f renderer.Frame, x, y, clipX int, text string, style renderer.Style) int {
	if y < 0 || y >= f.Height {
		return x
	}
	clipX = clamp(clipX, 0, f.Width)
	pos := x
	for _, r := range text {
		w := renderer.RuneWidth(r)
		if w == 0 {
			continue
		}
		if pos >= clipX {
			break
		}
		if pos+w > clipX {
			break
		}
		if pos >= 0 && pos < f.Width {
			f.Set(pos, y, renderer.Cell{Rune: r, Style: style})
		}
		if w == 2 && pos+1 >= 0 && pos+1 < f.Width {
			f.Set(pos+1, y, renderer.Cell{Style: style, Continuation: true})
		}
		pos += w
	}
	return pos
}

func setCell(f renderer.Frame, x, y int, r rune, style renderer.Style) {
	if x < 0 || x >= f.Width || y < 0 || y >= f.Height {
		return
	}
	f.Set(x, y, renderer.Cell{Rune: r, Style: style})
}

func percentOf(n, pct int) int {
	if pct <= 0 {
		return n
	}
	return n * pct / 100
}

func textWidth(text string) int {
	width := 0
	for _, r := range text {
		width += renderer.RuneWidth(r)
	}
	return width
}

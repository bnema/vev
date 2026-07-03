package ui

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/renderer"
)

// Modal describes a centered rectangular overlay.
type Modal struct {
	WidthPct  int
	HeightPct int
	MinWidth  int
	MinHeight int
	Title     string
}

// Bounds returns the modal rectangle centered within base and clamped to base.
func (m Modal) Bounds(base domain.Size) domain.Rect {
	if base.Cols <= 0 || base.Rows <= 0 {
		return domain.Rect{}
	}

	width := percentOf(base.Cols, m.WidthPct)
	height := percentOf(base.Rows, m.HeightPct)
	if width < m.MinWidth {
		width = m.MinWidth
	}
	if height < m.MinHeight {
		height = m.MinHeight
	}
	width = clamp(width, 0, base.Cols)
	height = clamp(height, 0, base.Rows)

	return domain.Rect{
		X:      (base.Cols - width) / 2,
		Y:      (base.Rows - height) / 2,
		Width:  width,
		Height: height,
	}
}

// Inner returns the modal content rectangle after removing a one-cell border.
func (m Modal) Inner(base domain.Size) domain.Rect {
	bounds := m.Bounds(base)
	return domain.Rect{
		X:      bounds.X + 1,
		Y:      bounds.Y + 1,
		Width:  max(0, bounds.Width-2),
		Height: max(0, bounds.Height-2),
	}
}

// Composite draws the modal border and title, clears the interior, and returns
// the inner content rectangle. Cells outside the modal bounds are not changed.
func (m Modal) Composite(f renderer.Frame, border renderer.Style) domain.Rect {
	bounds := m.Bounds(domain.Size{Cols: f.Width, Rows: f.Height})
	inner := domain.Rect{
		X:      bounds.X + 1,
		Y:      bounds.Y + 1,
		Width:  max(0, bounds.Width-2),
		Height: max(0, bounds.Height-2),
	}
	DrawBox(f, bounds, border)
	FillRect(f, inner, renderer.BlankCell())
	if bounds.Width > 2 && bounds.Height > 0 && m.Title != "" {
		left := bounds.X + 1
		right := bounds.X + bounds.Width - 1
		start := max(left, bounds.X+(bounds.Width-textWidth(m.Title))/2)
		DrawText(f, start, bounds.Y, right, m.Title, border)
	}
	return inner
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
	if rect.Width <= 0 || rect.Height <= 0 {
		return
	}
	left := rect.X
	top := rect.Y
	right := rect.X + rect.Width - 1
	bottom := rect.Y + rect.Height - 1

	for x := left; x <= right; x++ {
		setCell(f, x, top, '─', style)
		setCell(f, x, bottom, '─', style)
	}
	for y := top; y <= bottom; y++ {
		setCell(f, left, y, '│', style)
		setCell(f, right, y, '│', style)
	}
	setCell(f, left, top, '┌', style)
	setCell(f, right, top, '┐', style)
	setCell(f, left, bottom, '└', style)
	setCell(f, right, bottom, '┘', style)
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

func clamp(n, low, high int) int {
	if n < low {
		return low
	}
	if n > high {
		return high
	}
	return n
}

func textWidth(text string) int {
	width := 0
	for _, r := range text {
		width += renderer.RuneWidth(r)
	}
	return width
}

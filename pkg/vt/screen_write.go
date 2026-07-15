package vt

import "github.com/bnema/vev/pkg/renderer"

func (s *Screen) putRune(r rune) {
	switch r {
	case '\a':
		if s.OnBell != nil {
			s.OnBell()
		}
		return
	case 0x00, 0x0e, 0x0f, 0x7f:
		return
	case '\r':
		s.Col = 0
	case '\n', '\v', '\f':
		if s.Frame.Width > 0 && s.Col >= s.Frame.Width {
			s.Col = s.Frame.Width - 1
		}
		s.index()
	case '\b':
		if s.Col > 0 {
			s.Col--
		}
	case '\t':
		// HT moves to the next tab stop without modifying the frame. Clamp at
		// the right edge rather than triggering deferred wrap or a scroll.
		if s.Frame.Width > 0 {
			s.Col = min(s.Col+8-s.Col%8, s.Frame.Width-1)
		}
	default:
		// Drop C1 control characters (U+0080-U+009F).
		if r >= 0x80 && r <= 0x9F {
			return
		}
		if r >= 0x20 {
			s.putPrintable(r)
		}
	}
}

func (s *Screen) putPrintable(r rune) {
	w := renderer.RuneWidth(r)
	// Skip combining marks and zero-width characters.
	if w == 0 {
		return
	}
	if s.Frame.Width == 0 || s.Frame.Height == 0 {
		return
	}
	// Deferred wrap: cursor sits past the last column.
	if s.Col >= s.Frame.Width {
		s.Col = 0
		s.index()
	}
	if s.Row >= s.Frame.Height {
		s.Row = s.Frame.Height - 1
	}

	// A wide rune must never straddle the right edge. If it does not fit on the
	// current line, clear the abandoned last cell and wrap to the next line.
	if w == 2 && s.Col+1 >= s.Frame.Width {
		if s.Frame.Width < 2 {
			// The screen is too narrow to ever hold a wide rune; store a narrow
			// replacement so the cell's renderer width matches its layout.
			r = '\uFFFD'
			w = renderer.RuneWidth(r)
		} else {
			// The abandoned cell may itself be the continuation of a pair whose
			// left half sits one column back; report damage over the full span.
			cx := s.Col
			if s.Col > 0 && s.Frame.At(s.Col, s.Row).Continuation {
				cx = s.Col - 1
			}
			s.clearWidePairAt(s.Col, s.Row)
			s.Frame.Set(s.Col, s.Row, renderer.BlankCell())
			s.record(renderer.Damage{Kind: renderer.DamageText, X: cx, Y: s.Row, Width: s.Col - cx + 1, Height: 1, Count: 1})
			s.Col = 0
			s.index()
			if s.Row >= s.Frame.Height {
				s.Row = s.Frame.Height - 1
			}
		}
	}

	insertDamageX := s.Col
	insertDamageWidth := 0
	if s.insertMode {
		row := s.Frame.Row(s.Row)
		leftSplit := s.Col > 0 && row[s.Col].Continuation
		for x := s.Frame.Width - 1; x >= s.Col+w; x-- {
			row[x] = row[x-w]
		}
		for x := s.Col; x < s.Col+w; x++ {
			row[x] = renderer.BlankCell()
		}
		s.repairRow(s.Row)
		if leftSplit {
			insertDamageX = s.Col - 1
		}
		insertDamageWidth = s.Frame.Width - insertDamageX
	}

	// Determine the range of cells actually modified, extending over any wide
	// pair the write lands on so no orphaned half is left behind.
	lo, hi := s.Col, s.Col+w-1
	if s.Frame.At(s.Col, s.Row).Continuation {
		lo = s.Col - 1
	}
	if right := s.Col + w; right < s.Frame.Width && s.Frame.At(right, s.Row).Continuation {
		hi = right
	}
	for x := lo; x <= hi; x++ {
		s.Frame.Set(x, s.Row, renderer.BlankCell())
	}
	s.Frame.Set(s.Col, s.Row, renderer.Cell{Rune: r, Style: s.Style})
	if w == 2 {
		s.Frame.Set(s.Col+1, s.Row, renderer.Cell{Continuation: true, Style: s.Style})
	}
	if insertDamageWidth > 0 {
		lo = min(lo, insertDamageX)
		hi = max(hi, insertDamageX+insertDamageWidth-1)
	}
	s.record(renderer.Damage{Kind: renderer.DamageText, X: lo, Y: s.Row, Width: hi - lo + 1, Height: 1, Count: 1})
	s.Col += w
}

// clearWidePairAt blanks both halves of a wide-character pair when the cell at
// (x,y) is either the left (wide) half or the right (continuation) half. It is
// O(1) and assumes the pair invariant (a continuation is preceded by its wide
// left half) holds — which the writer maintains.

func (s *Screen) clearWidePairAt(x, y int) {
	if x < 0 || x >= s.Frame.Width || y < 0 || y >= s.Frame.Height {
		return
	}
	if s.Frame.At(x, y).Continuation {
		if x-1 >= 0 {
			s.Frame.Set(x-1, y, renderer.BlankCell())
		}
		s.Frame.Set(x, y, renderer.BlankCell())
		return
	}
	if x+1 < s.Frame.Width && s.Frame.At(x+1, y).Continuation {
		s.Frame.Set(x, y, renderer.BlankCell())
		s.Frame.Set(x+1, y, renderer.BlankCell())
	}
}

func (s *Screen) eraseCell() renderer.Cell {
	style := renderer.DefaultStyle()
	if s.Style.HasBackgroundRGB {
		style.HasBackgroundRGB = true
		style.BackgroundRGB = s.Style.BackgroundRGB
	} else {
		style.Background = s.Style.Background
	}
	return renderer.Cell{Rune: ' ', Style: style}
}

// clearRow blanks cells [x0,x1) on row y, extending the range to swallow either
// half of a wide pair that straddles a boundary so no orphan half is left. It
// returns the actual modified span [start, start+width).

func (s *Screen) clearRow(y, x0, x1 int) (start, width int) {
	if y < 0 || y >= s.Frame.Height {
		return x0, 0
	}
	if x0 < 0 {
		x0 = 0
	}
	if x1 > s.Frame.Width {
		x1 = s.Frame.Width
	}
	if x0 >= x1 {
		return x0, 0
	}
	// Left boundary: a continuation at x0 means its wide left half sits at x0-1.
	if x0 > 0 && s.Frame.At(x0, y).Continuation {
		x0--
	}
	// Right boundary: a continuation at x1 means its wide left half (at x1-1) is
	// inside the range and will be blanked, so swallow the continuation too.
	if x1 < s.Frame.Width && s.Frame.At(x1, y).Continuation {
		x1++
	}
	blank := s.eraseCell()
	for x := x0; x < x1; x++ {
		s.Frame.Set(x, y, blank)
	}
	return x0, x1 - x0
}

// repairRow blanks any orphaned wide-character halves on row y: a continuation
// cell not preceded by its wide left half, or a wide rune not followed by its
// continuation. Used after erase/insert/delete operations that may split a pair
// at a boundary or by shifting cells.

func (s *Screen) repairRow(y int) {
	repairFrameRow(s.Frame, y)
}

func repairFrameRow(frame renderer.Frame, y int) {
	if y < 0 || y >= frame.Height {
		return
	}
	w := frame.Width
	for x := range w {
		c := frame.At(x, y)
		if c.Continuation {
			if x == 0 {
				frame.Set(x, y, renderer.BlankCell())
				continue
			}
			left := frame.At(x-1, y)
			if left.Continuation || renderer.RuneWidth(left.Rune) != 2 {
				frame.Set(x, y, renderer.BlankCell())
			}
			continue
		}
		if renderer.RuneWidth(c.Rune) == 2 {
			if x+1 >= w || !frame.At(x+1, y).Continuation {
				frame.Set(x, y, renderer.BlankCell())
			}
		}
	}
}

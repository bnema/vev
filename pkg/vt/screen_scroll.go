package vt

import "github.com/bnema/vev/pkg/renderer"

func (s *Screen) index() {
	if s.Frame.Height == 0 {
		return
	}
	if s.Row == s.scrollBottom && s.Row >= s.scrollTop {
		s.scrollUpRegion(s.scrollTop, s.scrollBottom, 1)
		return
	}
	if s.Row+1 < s.Frame.Height {
		s.Row++
		return
	}
	s.Row = s.Frame.Height - 1
}

func (s *Screen) nextLine() {
	s.Col = 0
	s.index()
}

func (s *Screen) reverseIndex() {
	if s.Frame.Height == 0 {
		return
	}
	if s.Row == s.scrollTop && s.Row <= s.scrollBottom {
		s.scrollDownRegion(s.scrollTop, s.scrollBottom, 1)
		return
	}
	if s.Row > 0 {
		s.Row--
	}
}

// scrollUpBy scrolls the current scroll region up by n lines (CSI S / SU).
// The cursor position is preserved.
func (s *Screen) scrollUpBy(n int) {
	if n <= 0 {
		return
	}
	s.scrollUpRegion(s.scrollTop, s.scrollBottom, n)
}

func (s *Screen) scrollDownBy(n int) {
	if n <= 0 {
		return
	}
	s.scrollDownRegion(s.scrollTop, s.scrollBottom, n)
}

func (s *Screen) scrollUpRegion(top, bottom, n int) {
	if s.Frame.Width == 0 || s.Frame.Height == 0 || n <= 0 {
		return
	}
	top, bottom, ok := s.normalizedRegion(top, bottom)
	if !ok {
		return
	}
	w := s.Frame.Width
	height := bottom - top + 1
	if n > height {
		n = height
	}
	// VT scroll regions always span the full frame width, so we rotate the
	// frame's line offsets (recycling and blanking the evicted rows in place)
	// instead of copying cells. See renderer.Frame.ScrollUp.
	s.emitLineEvicted(top, n)
	s.Frame.ScrollUp(top, bottom, n)
	s.record(renderer.Damage{Kind: renderer.DamageScrollUp, X: 0, Y: top, Width: w, Height: height, Count: n})
	s.record(renderer.Damage{Kind: renderer.DamageText, X: 0, Y: bottom - n + 1, Width: w, Height: n, Count: 1})
}

func (s *Screen) emitLineEvicted(top, n int) {
	// Only rows leaving the top edge of the primary screen belong to global
	// scrollback. Interior DECSTBM scroll regions are local mutations.
	if s.alternate != nil || top != 0 {
		return
	}
	for y := top; y < top+n; y++ {
		s.recordEvicted(s.Frame.Row(y))
	}
}

func (s *Screen) recordEvicted(row []renderer.Cell) {
	if s.history != nil {
		s.history.Append(row)
	}
	if s.OnLineEvicted != nil {
		s.OnLineEvicted(append([]renderer.Cell(nil), row...))
	}
}

func (s *Screen) scrollDownRegion(top, bottom, n int) {
	if s.Frame.Width == 0 || s.Frame.Height == 0 || n <= 0 {
		return
	}
	top, bottom, ok := s.normalizedRegion(top, bottom)
	if !ok {
		return
	}
	w := s.Frame.Width
	height := bottom - top + 1
	if n > height {
		n = height
	}
	// Full-width region: rotate line offsets instead of copying cells.
	s.Frame.ScrollDown(top, bottom, n)
	s.record(renderer.Damage{Kind: renderer.DamageText, X: 0, Y: top, Width: w, Height: height, Count: 1})
}

func (s *Screen) normalizedRegion(top, bottom int) (int, int, bool) {
	if s.Frame.Height == 0 || top > bottom {
		return 0, 0, false
	}
	top = clamp(top, 0, s.Frame.Height-1)
	bottom = clamp(bottom, 0, s.Frame.Height-1)
	return top, bottom, top <= bottom
}

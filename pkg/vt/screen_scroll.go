package vt

import (
	"errors"

	"github.com/bnema/vev/pkg/renderer"
)

// index moves to the next physical row, scrolling the region when the cursor
// already sits on its last row. It reports whether the row the cursor was on
// survived that scroll by moving up to s.Row-1, which only a deferred wrap
// cares about: a wrap must relink to the row it flowed out of, and
// buffer.scrollUp severs that link on the way. Callers that merely advance,
// such as LF and ESC D, ignore the result.
func (s *Screen) index() (movedUp bool) {
	if s.Frame.Height == 0 {
		return false
	}
	if s.Row == s.scrollBottom && s.Row >= s.scrollTop {
		return s.scrollUpRegion(s.scrollTop, s.scrollBottom, 1)
	}
	if s.Row+1 < s.Frame.Height {
		s.Row++
		return false
	}
	s.Row = s.Frame.Height - 1
	return false
}

func (s *Screen) nextLine() {
	s.buffer.hard(s.Row)
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

// scrollUpRegion scrolls [top,bottom] up by n. It reports whether any row
// survived by moving up: a region exactly n rows tall is blanked in place, so
// the receiving range [top,bottom-n] is empty and nothing survives. Reporting
// what the scroll observed keeps callers from re-deriving a predicate that this
// function may have declined to act on at all.
func (s *Screen) scrollUpRegion(top, bottom, n int) (shifted bool) {
	if s.Frame.Width == 0 || s.Frame.Height == 0 || n <= 0 {
		return false
	}
	top, bottom, ok := s.normalizedRegion(top, bottom)
	if !ok {
		return false
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
	s.buffer.scrollUp(top, bottom, n)
	s.record(renderer.Damage{Kind: renderer.DamageScrollUp, X: 0, Y: top, Width: w, Height: height, Count: n})
	s.record(renderer.Damage{Kind: renderer.DamageText, X: 0, Y: bottom - n + 1, Width: w, Height: n, Count: 1})
	return bottom-n >= top
}

func (s *Screen) emitLineEvicted(top, n int) {
	// Only rows leaving the top edge of the primary screen belong to global
	// scrollback. Interior DECSTBM scroll regions are local mutations.
	if s.alternate != nil || top != 0 {
		return
	}
	// Read boundaries before the caller rotates the frame: a soft link belongs
	// to the row it follows, and rotation reassigns row indices.
	for y := top; y < top+n; y++ {
		s.recordEvicted(s.Frame.Row(y), s.buffer.bound(y))
	}
}

func (s *Screen) recordEvicted(row []renderer.Cell, bound LineBound) {
	if s.history != nil {
		err := s.history.Append(row, bound)
		if err != nil && !errors.Is(err, ErrHistoryRowTooWide) {
			panic(err)
		}
		// A nil error records the row; ErrHistoryRowTooWide explicitly leaves it
		// unrecorded. Both cases continue to the eviction observer below.
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
	s.buffer.scrollDown(top, bottom, n)
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

package vt

import "github.com/bnema/vev/pkg/renderer"

func (s *Screen) Resize(width, height int) {
	if width == s.Frame.Width && height == s.Frame.Height {
		return
	}
	evict := s.recordEvicted

	if s.alternate != nil {
		var shift int
		s.Frame, shift = resizeFrame(s.Frame, width, height, s.Row, nil)
		s.Row = clamp(s.Row-shift, 0, height-1)
		s.Col = clamp(s.Col, 0, width-1)
		s.savedCursor = resizedCursor(s.savedCursor, shift, width, height)
		s.alternate.frame, shift = resizeFrame(s.alternate.frame, width, height, s.alternate.row, evict)
		s.alternate.row = clamp(s.alternate.row-shift, 0, height-1)
		s.alternate.col = clamp(s.alternate.col, 0, width-1)
		s.alternate.savedCursor = resizedCursor(s.alternate.savedCursor, shift, width, height)
		s.alternate.scrollTop = clamp(s.alternate.scrollTop-shift, 0, height-1)
		s.alternate.scrollBottom = clamp(s.alternate.scrollBottom-shift, 0, height-1)
		if s.alternate.scrollTop >= s.alternate.scrollBottom {
			s.alternate.scrollTop = 0
			s.alternate.scrollBottom = max(height-1, 0)
		}
	} else {
		var shift int
		s.Frame, shift = resizeFrame(s.Frame, width, height, s.Row, evict)
		s.Row = clamp(s.Row-shift, 0, height-1)
		s.Col = clamp(s.Col, 0, width-1)
		s.savedCursor = resizedCursor(s.savedCursor, shift, width, height)
	}

	// A resize can split an in-flight escape sequence from the terminal state it
	// was meant to mutate; keep the durable child state but discard partial bytes.
	s.escapeBuf = s.escapeBuf[:0]
	s.resetScrollRegion()
	s.fullRedraw()
}

func resizeFrame(old renderer.Frame, newW, newH, cursorRow int, evict func([]renderer.Cell)) (renderer.Frame, int) {
	next := renderer.NewFrame(newW, newH)
	shift := clamp(cursorRow-(newH-1), 0, max(old.Height-newH, 0))
	if evict != nil {
		for y := range shift {
			evict(old.Row(y))
		}
	}
	for dy := range newH {
		sy := dy + shift
		if sy >= old.Height {
			break
		}
		copy(next.Row(dy), old.Row(sy))
		repairFrameRow(next, dy)
	}
	return next, shift
}

func resizedCursor(cur cursorState, shift, width, height int) cursorState {
	if !cur.saved {
		return cur
	}
	cur.row = clamp(cur.row-shift, 0, height-1)
	cur.col = clamp(cur.col, 0, width-1)
	return cur
}

// cloneFrame produces an independent copy in canonical layout: it copies the
// source's logical rows (via Row) into a fresh frame whose offsets are already
// canonical, so a rotated source is normalized in the clone.
func cloneFrame(frame renderer.Frame) renderer.Frame {
	out := renderer.NewFrame(frame.Width, frame.Height)
	for y := range frame.Height {
		copy(out.Row(y), frame.Row(y))
	}
	return out
}

func clamp(v, lo, hi int) int {
	if hi < lo {
		return lo
	}
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

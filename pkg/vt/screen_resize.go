package vt

import "github.com/bnema/vev/pkg/renderer"

func (s *Screen) Resize(width, height int) {
	if width == s.Frame.Width && height == s.Frame.Height {
		return
	}
	if width <= 0 || height <= 0 {
		// renderer.Frame permits a collapsed geometry. There is no layout to
		// preserve until a usable viewport is supplied again.
		width, height = max(width, 0), max(height, 0)
		s.buffer = s.newBuffer(width, height)
		s.Row, s.Col = 0, 0
		if state := s.alternate; state != nil {
			state.buffer = s.newBuffer(width, height)
			state.frame = state.buffer.frame
			state.row, state.col = 0, 0
			state.scrollTop, state.scrollBottom = 0, height-1
		}
	} else {
		resize := func(b *buffer, row, col int, saved *cursorState, evict bool) (int, int) {
			active := &bufferCursor{row: row, col: col}
			var savedPoint *bufferCursor
			if saved != nil && saved.saved {
				savedPoint = &bufferCursor{row: saved.row, col: saved.col}
			}
			evicted, evictedBounds, evictedIDs := b.resize(width, height, active, savedPoint)
			for i := range evictedIDs {
				if evictedIDs[i] == 0 {
					evictedIDs[i] = s.nextRowIDValue()
				}
			}
			s.fillMissingRowIDs(b)
			if evict {
				for i, line := range evicted {
					s.recordEvicted(line, evictedBounds[i], evictedIDs[i])
				}
			}
			if savedPoint != nil {
				saved.row, saved.col = savedPoint.row, clamp(savedPoint.col, 0, width-1)
			}
			return active.row, clamp(active.col, 0, width-1)
		}

		if s.alternate != nil {
			// Both screen states are reflowed independently, but active and saved
			// cursors for each state are mapped by their single buffer layout pass.
			s.Row, s.Col = resize(s.buffer, s.Row, s.Col, &s.savedCursor, false)
			state := s.alternate
			if state.buffer == nil {
				state.buffer = bufferFromFrame(state.frame)
				s.fillMissingRowIDs(state.buffer)
			}
			state.row, state.col = resize(state.buffer, state.row, state.col, &state.savedCursor, true)
			state.frame = state.buffer.frame
			state.scrollTop, state.scrollBottom = 0, height-1
		} else {
			s.Row, s.Col = resize(s.buffer, s.Row, s.Col, &s.savedCursor, true)
		}
	}
	s.Frame = s.buffer.frame
	// A resize can split an in-flight escape sequence from the terminal state it
	// was meant to mutate; keep the durable child state but discard partial bytes.
	s.escapeBuf = s.escapeBuf[:0]
	s.resetScrollRegion()
	s.fullRedraw()
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

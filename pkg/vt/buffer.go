package vt

import "github.com/bnema/vev/pkg/renderer"

// buffer owns the visible VT grid and the physical-row boundaries needed to
// reconstruct logical lines. History deliberately is not part of this type:
// reflow is bounded by the live grid.
type buffer struct {
	frame      renderer.Frame
	boundaries []lineBoundary
}

type lineBoundary struct {
	// end is the meaningful cell extent. It excludes padding introduced when a
	// wide rune was moved off the right edge.
	end  int
	soft bool // the row continues into the following physical row
}

func newBuffer(width, height int) *buffer {
	return &buffer{frame: renderer.NewFrame(width, height), boundaries: make([]lineBoundary, height)}
}

func bufferFromFrame(frame renderer.Frame) *buffer {
	return &buffer{frame: frame, boundaries: make([]lineBoundary, frame.Height)}
}

func (b *buffer) clone() *buffer {
	out := &buffer{frame: cloneFrame(b.frame), boundaries: append([]lineBoundary(nil), b.boundaries...)}
	return out
}

func (b *buffer) content(y, end int) {
	if y < 0 || y >= len(b.boundaries) {
		return
	}
	b.boundaries[y].end = max(b.boundaries[y].end, clamp(end, 0, b.frame.Width))
}

func (b *buffer) hard(y int) {
	if y >= 0 && y < len(b.boundaries) {
		b.boundaries[y].soft = false
	}
}

func (b *buffer) soft(y int) {
	if y >= 0 && y < len(b.boundaries) {
		b.boundaries[y].end = b.frame.Width
		b.boundaries[y].soft = true
	}
}

func (b *buffer) continueRow(y int) {
	if y >= 0 && y < len(b.boundaries) {
		b.boundaries[y].soft = true
	}
}

func (b *buffer) clear(y, x0, x1 int) {
	if y < 0 || y >= len(b.boundaries) {
		return
	}
	b.content(y, x1)
	if x0 == 0 && x1 >= b.frame.Width {
		b.boundaries[y] = lineBoundary{soft: b.boundaries[y].soft}
	}
}

func (b *buffer) scrollUp(top, bottom, n int) {
	// A region operation changes which rows meet at both region edges. A soft
	// boundary belongs to the row it follows, not the cells moved into its
	// neighbor, so sever links crossing either edge before reflow can observe
	// them.
	b.hard(top - 1)
	copy(b.boundaries[top:bottom-n+1], b.boundaries[top+n:bottom+1])
	for y := bottom - n + 1; y <= bottom; y++ {
		b.boundaries[y] = lineBoundary{}
	}
	b.hard(bottom - n)
}

func (b *buffer) scrollDown(top, bottom, n int) {
	// See scrollUp: the old top no longer follows the row above the region, and
	// the last moved row no longer precedes the row below it.
	b.hard(top - 1)
	copy(b.boundaries[top+n:bottom+1], b.boundaries[top:bottom-n+1])
	for y := top; y < top+n; y++ {
		b.boundaries[y] = lineBoundary{}
	}
	b.hard(bottom)
}

func (b *buffer) hydrate() {
	for y := range b.boundaries {
		if b.boundaries[y].end != 0 {
			continue
		}
		for x := b.frame.Width - 1; x >= 0; x-- {
			if !b.frame.At(x, y).Equal(renderer.BlankCell()) {
				b.boundaries[y].end = x + 1
				break
			}
		}
	}
}

type bufferCursor struct{ row, col int }

func (b *buffer) hasSoft() bool {
	for _, boundary := range b.boundaries {
		if boundary.soft {
			return true
		}
	}
	return false
}

// resizeFixed keeps hard physical lines independent. It is the common shell
// path and avoids constructing logical-line scratch state when nothing wraps.
func (b *buffer) resizeFixed(width, height int, active, saved *bufferCursor) [][]renderer.Cell {
	anchor := 0
	if active != nil {
		anchor = active.row
	}
	shift := clamp(anchor-(height-1), 0, max(b.frame.Height-height, 0))
	evicted := make([][]renderer.Cell, 0, shift)
	for y := range shift {
		evicted = append(evicted, append([]renderer.Cell(nil), b.frame.Row(y)...))
	}
	// Fixed-line resizes can retain the boundary backing store. In particular,
	// keep its capacity through a short viewport so the common shrink/grow
	// sequence does not add metadata allocation to every resize epoch.
	boundaries := b.boundaries
	if cap(boundaries) < height {
		boundaries = make([]lineBoundary, height)
	} else {
		boundaries = boundaries[:height]
	}
	next := buffer{frame: renderer.NewFrame(width, height), boundaries: boundaries}
	copied := 0
	for y := range height {
		sy := y + shift
		if sy >= b.frame.Height {
			break
		}
		copy(next.frame.Row(y), b.frame.Row(sy))
		next.boundaries[y] = b.boundaries[sy]
		next.boundaries[y].end = min(next.boundaries[y].end, width)
		repairFrameRow(next.frame, y)
		copied++
	}
	clear(next.boundaries[copied:])
	for _, cur := range [2]*bufferCursor{active, saved} {
		if cur != nil {
			cur.row = clamp(cur.row-shift, 0, height-1)
			cur.col = clamp(cur.col, 0, width)
		}
	}
	*b = next
	return evicted
}

type reflowPoint struct {
	line, offset int
	row, col     int
}

// cursorReflowPoints maps source cursor positions to offsets in their logical
// lines. There are exactly two callers (active and DECSC), so this stays flat
// and stack allocated rather than building a row/column lookup map.
func (b *buffer) cursorReflowPoints(active, saved *bufferCursor) [2]reflowPoint {
	points := [2]reflowPoint{{line: -1}, {line: -1}}
	cursors := [2]*bufferCursor{active, saved}
	for start := 0; start < b.frame.Height; {
		end := start
		for b.boundaries[end].soft && end+1 < b.frame.Height {
			end++
		}
		for i, cur := range cursors {
			if cur == nil || cur.row < start || cur.row > end {
				continue
			}
			offset := 0
			for y := start; y < cur.row; y++ {
				offset += b.boundaries[y].end
			}
			points[i] = reflowPoint{
				line:   start,
				offset: offset + min(clamp(cur.col, 0, b.frame.Width), b.boundaries[cur.row].end),
			}
		}
		start = end + 1
	}
	return points
}

// resize lays out only the current grid. It does two direct, bounded passes:
// the first maps both cursors and counts rows; the second writes only the
// retained viewport (and the rows genuinely evicted to history). It never
// materializes logical lines, per-cell position maps, or temporary output rows.
func (b *buffer) resize(width, height int, active, saved *bufferCursor) [][]renderer.Cell {
	if !b.hasSoft() {
		return b.resizeFixed(width, height, active, saved)
	}
	// Only reflow needs meaningful extents; hard physical rows are copied as
	// cells, so scanning blank rows on the shell fast path is wasted work.
	b.hydrate()
	for _, cur := range [2]*bufferCursor{active, saved} {
		if cur != nil {
			b.content(cur.row, cur.col)
		}
	}

	points := b.cursorReflowPoints(active, saved)
	rows := b.layoutReflow(width, &points, nil, 0, nil)
	anchor := 0
	if active != nil {
		anchor = points[0].row
	}
	shift := clamp(anchor-(height-1), 0, max(rows-height, 0))

	next := newBuffer(width, height)
	var evicted [][]renderer.Cell
	var evictedCells []renderer.Cell
	if shift > 0 {
		// History needs owned rows. One flat backing store replaces a temporary
		// allocation per evicted output row.
		evicted = make([][]renderer.Cell, shift)
		evictedCells = make([]renderer.Cell, shift*width)
		for y := range evicted {
			evicted[y] = evictedCells[y*width : (y+1)*width]
		}
	}
	b.layoutReflow(width, &points, next, shift, evictedCells)
	for i, cur := range [2]*bufferCursor{active, saved} {
		if cur != nil {
			cur.row = clamp(points[i].row-shift, 0, height-1)
			cur.col = clamp(points[i].col, 0, width)
		}
	}
	*b = *next
	return evicted
}

// layoutReflow maps source offsets and, when dst is non-nil, emits only rows in
// [shift, shift+dst.height). Rows before shift are written straight into the
// contiguous eviction backing store. The return is the logical output height.
func (b *buffer) layoutReflow(width int, points *[2]reflowPoint, dst *buffer, shift int, evicted []renderer.Cell) int {
	row, col := 0, 0
	blankEvicted := func(outputRow int) []renderer.Cell {
		if outputRow < shift {
			out := evicted[outputRow*width : (outputRow+1)*width]
			for i := range out {
				out[i] = renderer.BlankCell()
			}
			return out
		}
		if dst != nil && outputRow < shift+dst.frame.Height {
			return dst.frame.Row(outputRow - shift)
		}
		return nil
	}
	var output []renderer.Cell
	if dst != nil && (shift > 0 || dst.frame.Height > 0) {
		output = blankEvicted(0)
	}
	finishRow := func(soft bool) {
		if dst != nil && row >= shift && row < shift+dst.frame.Height {
			dst.boundaries[row-shift] = lineBoundary{end: col, soft: soft}
		}
		row++
		col = 0
		if dst != nil {
			output = blankEvicted(row)
		}
	}
	setPoint := func(line, offset, pointRow, pointCol int) {
		for i := range points {
			if points[i].line == line && points[i].offset == offset {
				points[i].row, points[i].col = pointRow, pointCol
			}
		}
	}
	setRemainingPoints := func(line, offset, pointRow, pointCol int) {
		for i := range points {
			if points[i].line == line && points[i].offset >= offset {
				points[i].row, points[i].col = pointRow, pointCol
			}
		}
	}

	for start := 0; start < b.frame.Height; {
		end := start
		for b.boundaries[end].soft && end+1 < b.frame.Height {
			end++
		}
		reflow := end > start
		offset := 0
		truncated := false
		for y := start; y <= end && !truncated; y++ {
			cells := b.frame.Row(y)
			limit := b.boundaries[y].end
			for x := 0; x < limit; {
				cell := cells[x]
				if cell.Continuation { // Repair malformed rows by dropping orphaned tails.
					setPoint(start, offset, row, col)
					x++
					offset++
					continue
				}
				wide := renderer.RuneWidth(cell.Rune) == 2 && x+1 < limit && cells[x+1].Continuation
				w := 1
				if wide && width >= 2 {
					w = 2
				}
				if col+w > width && col > 0 {
					if !reflow {
						setRemainingPoints(start, offset, row, col)
						truncated = true
						break
					}
					finishRow(true)
				}
				setPoint(start, offset, row, col)
				if wide && width >= 2 {
					if output != nil {
						output[col], output[col+1] = cell, cells[x+1]
					}
					setPoint(start, offset+1, row, col+1)
					col += 2
					x += 2
					offset += 2
					continue
				}
				if wide {
					cell.Rune = '\uFFFD'
					cell.Continuation = false
				}
				if output != nil {
					output[col] = cell
				}
				col++
				x++
				offset++
			}
		}
		setPoint(start, offset, row, col)
		finishRow(false)
		start = end + 1
	}
	return row
}

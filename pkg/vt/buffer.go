package vt

import "github.com/bnema/vev/pkg/renderer"

// buffer owns the visible VT grid and the physical-row boundaries needed to
// reconstruct logical lines. History deliberately is not part of this type:
// reflow is bounded by the live grid.
type buffer struct {
	frame      renderer.Frame
	boundaries []LineBound
	rowIDs     []RowID
}

// LineBound describes a physical row's logical extent. End is exclusive: it is
// the count of meaningful cells, so the last significant column is End-1. It
// excludes padding introduced when a wide rune was moved off the right edge.
// Soft reports that the row continues into the following physical row.
type LineBound struct {
	End  int
	Soft bool
}

func newBuffer(width, height int) *buffer {
	return &buffer{frame: renderer.NewFrame(width, height), boundaries: make([]LineBound, height), rowIDs: make([]RowID, height)}
}

func bufferFromFrame(frame renderer.Frame) *buffer {
	return &buffer{frame: frame, boundaries: make([]LineBound, frame.Height), rowIDs: make([]RowID, frame.Height)}
}

func (b *buffer) clone() *buffer {
	out := &buffer{
		frame:      cloneFrame(b.frame),
		boundaries: append([]LineBound(nil), b.boundaries...),
		rowIDs:     append([]RowID(nil), b.rowIDs...),
	}
	return out
}

func (b *buffer) content(y, end int) {
	if y < 0 || y >= len(b.boundaries) {
		return
	}
	b.boundaries[y].End = max(b.boundaries[y].End, clamp(end, 0, b.frame.Width))
}

// bound reports the logical extent of row y. Callers on eviction paths may run
// against a nil or shorter buffer than the frame they are walking, so an
// unknown row reads as a hard row of no extent rather than panicking.
func (b *buffer) bound(y int) LineBound {
	if b == nil || y < 0 || y >= len(b.boundaries) {
		return LineBound{}
	}
	return b.boundaries[y]
}

func (b *buffer) truncate(y, end int) {
	if y < 0 || y >= len(b.boundaries) {
		return
	}
	b.boundaries[y].End = min(b.boundaries[y].End, clamp(end, 0, b.frame.Width))
}

// insert retains the meaningful shifted tail when insertion happens within it.
func (b *buffer) insert(y, at, width int) {
	if y < 0 || y >= len(b.boundaries) || at >= b.boundaries[y].End {
		return
	}
	b.boundaries[y].End = min(b.boundaries[y].End+width, b.frame.Width)
}

func (b *buffer) hard(y int) {
	if y >= 0 && y < len(b.boundaries) {
		b.boundaries[y].Soft = false
	}
}

func (b *buffer) soft(y int) {
	if y >= 0 && y < len(b.boundaries) {
		b.boundaries[y].End = b.frame.Width
		b.boundaries[y].Soft = true
	}
}

func (b *buffer) continueRow(y int) {
	if y >= 0 && y < len(b.boundaries) {
		b.boundaries[y].Soft = true
	}
}

func (b *buffer) clear(y, x0, x1 int) {
	if y < 0 || y >= len(b.boundaries) {
		return
	}
	b.content(y, x1)
	if x1 >= b.frame.Width {
		// Erasing through the right edge leaves nothing flowing onto the next
		// row: the logical line ends here. Keeping a stale soft link would let
		// reflow merge a repainted row with the unrelated row below it.
		b.boundaries[y].Soft = false
	}
	if x0 == 0 && x1 >= b.frame.Width {
		b.boundaries[y] = LineBound{}
	}
}

func (b *buffer) scrollUp(top, bottom, n int) {
	// A region operation changes which rows meet at both region edges. A soft
	// boundary belongs to the row it follows, not the cells moved into its
	// neighbor, so sever links crossing either edge before reflow can observe
	// them.
	b.hard(top - 1)
	copy(b.boundaries[top:bottom-n+1], b.boundaries[top+n:bottom+1])
	copy(b.rowIDs[top:bottom-n+1], b.rowIDs[top+n:bottom+1])
	for y := bottom - n + 1; y <= bottom; y++ {
		b.boundaries[y] = LineBound{}
		b.rowIDs[y] = 0
	}
	b.hard(bottom - n)
}

func (b *buffer) scrollDown(top, bottom, n int) {
	// See scrollUp: the old top no longer follows the row above the region, and
	// the last moved row no longer precedes the row below it.
	b.hard(top - 1)
	copy(b.boundaries[top+n:bottom+1], b.boundaries[top:bottom-n+1])
	copy(b.rowIDs[top+n:bottom+1], b.rowIDs[top:bottom-n+1])
	for y := top; y < top+n; y++ {
		b.boundaries[y] = LineBound{}
		b.rowIDs[y] = 0
	}
	b.hard(bottom)
}

func (b *buffer) hydrate() {
	for y := range b.boundaries {
		if b.boundaries[y].End != 0 {
			continue
		}
		for x := b.frame.Width - 1; x >= 0; x-- {
			if !b.frame.At(x, y).Equal(renderer.BlankCell()) {
				b.boundaries[y].End = x + 1
				break
			}
		}
	}
}

type bufferCursor struct{ row, col int }

func (b *buffer) hasSoft() bool {
	for _, boundary := range b.boundaries {
		if boundary.Soft {
			return true
		}
	}
	return false
}

// resizeFixed keeps hard physical lines independent. It is the common shell
// path and avoids constructing logical-line scratch state when nothing wraps.
func (b *buffer) resizeFixed(width, height int, active, saved *bufferCursor) ([][]renderer.Cell, []LineBound, []RowID) {
	anchor := 0
	if active != nil {
		anchor = active.row
	}
	shift := clamp(anchor-(height-1), 0, max(b.frame.Height-height, 0))
	evicted := make([][]renderer.Cell, 0, shift)
	evictedBounds := make([]LineBound, 0, shift)
	evictedIDs := make([]RowID, 0, shift)
	for y := range shift {
		evicted = append(evicted, append([]renderer.Cell(nil), b.frame.Row(y)...))
		// The evicted row keeps its source width, so its extent needs no clamping.
		evictedBounds = append(evictedBounds, b.boundaries[y])
		evictedIDs = append(evictedIDs, b.rowIDs[y])
	}
	// Fixed-line resizes can retain the boundary backing store. In particular,
	// keep its capacity through a short viewport so the common shrink/grow
	// sequence does not add metadata allocation to every resize epoch.
	boundaries := b.boundaries
	if cap(boundaries) < height {
		boundaries = make([]LineBound, height)
	} else {
		boundaries = boundaries[:height]
	}
	next := buffer{frame: renderer.NewFrame(width, height), boundaries: boundaries, rowIDs: make([]RowID, height)}
	copied := 0
	for y := range height {
		sy := y + shift
		if sy >= b.frame.Height {
			break
		}
		copy(next.frame.Row(y), b.frame.Row(sy))
		next.boundaries[y] = b.boundaries[sy]
		next.rowIDs[y] = b.rowIDs[sy]
		next.boundaries[y].End = min(next.boundaries[y].End, width)
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
	return evicted, evictedBounds, evictedIDs
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
		for b.boundaries[end].Soft && end+1 < b.frame.Height {
			end++
		}
		for i, cur := range cursors {
			if cur == nil || cur.row < start || cur.row > end {
				continue
			}
			offset := 0
			for y := start; y < cur.row; y++ {
				offset += b.boundaries[y].End
			}
			points[i] = reflowPoint{
				line:   start,
				offset: offset + min(clamp(cur.col, 0, b.frame.Width), b.boundaries[cur.row].End),
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
func (b *buffer) resize(width, height int, active, saved *bufferCursor) ([][]renderer.Cell, []LineBound, []RowID) {
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
	rows := b.layoutReflow(width, &points, nil, 0, nil, nil)
	anchor := 0
	if active != nil {
		anchor = points[0].row
	}
	shift := clamp(anchor-(height-1), 0, max(rows-height, 0))

	next := newBuffer(width, height)
	var evicted [][]renderer.Cell
	var evictedCells []renderer.Cell
	var evictedBounds []LineBound
	var evictedIDs []RowID
	if shift > 0 {
		// History needs owned rows. One flat backing store replaces a temporary
		// allocation per evicted output row.
		evicted = make([][]renderer.Cell, shift)
		evictedCells = make([]renderer.Cell, shift*width)
		evictedBounds = make([]LineBound, shift)
		evictedIDs = make([]RowID, shift)
		for y := range evicted {
			evicted[y] = evictedCells[y*width : (y+1)*width]
		}
	}
	b.layoutReflow(width, &points, next, shift, evictedCells, evictedBounds)
	for i, cur := range [2]*bufferCursor{active, saved} {
		if cur != nil {
			cur.row = clamp(points[i].row-shift, 0, height-1)
			cur.col = clamp(points[i].col, 0, width)
		}
	}
	*b = *next
	return evicted, evictedBounds, evictedIDs
}

// layoutReflow maps source offsets and, when dst is non-nil, emits only rows in
// [shift, shift+dst.height). Rows before shift are written straight into the
// contiguous eviction backing store. The return is the logical output height.
func (b *buffer) layoutReflow(width int, points *[2]reflowPoint, dst *buffer, shift int, evicted []renderer.Cell, evictedBounds []LineBound) int {
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
		// An output row belongs either to history or to the retained viewport, and
		// only this pass knows where it ended, so both destinations are written here.
		bound := LineBound{End: col, Soft: soft}
		switch {
		case row < shift:
			if evictedBounds != nil {
				evictedBounds[row] = bound
			}
		case dst != nil && row < shift+dst.frame.Height:
			dst.boundaries[row-shift] = bound
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
		for b.boundaries[end].Soft && end+1 < b.frame.Height {
			end++
		}
		reflow := end > start
		offset := 0
		truncated := false
		for y := start; y <= end && !truncated; y++ {
			cells := b.frame.Row(y)
			limit := b.boundaries[y].End
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

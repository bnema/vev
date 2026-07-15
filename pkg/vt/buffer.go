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
	for ; n > 0; n-- {
		copy(b.boundaries[top:bottom], b.boundaries[top+1:bottom+1])
		b.boundaries[bottom] = lineBoundary{}
	}
}

func (b *buffer) scrollDown(top, bottom, n int) {
	for ; n > 0; n-- {
		copy(b.boundaries[top+1:bottom+1], b.boundaries[top:bottom])
		b.boundaries[top] = lineBoundary{}
	}
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
func (b *buffer) resizeFixed(width, height int, cursors ...*bufferCursor) [][]renderer.Cell {
	anchor := 0
	if len(cursors) > 0 && cursors[0] != nil {
		anchor = cursors[0].row
	}
	shift := clamp(anchor-(height-1), 0, max(b.frame.Height-height, 0))
	evicted := make([][]renderer.Cell, 0, shift)
	for y := 0; y < shift; y++ {
		evicted = append(evicted, append([]renderer.Cell(nil), b.frame.Row(y)...))
	}
	next := newBuffer(width, height)
	for y := 0; y < height; y++ {
		sy := y + shift
		if sy >= b.frame.Height {
			break
		}
		copy(next.frame.Row(y), b.frame.Row(sy))
		next.boundaries[y] = b.boundaries[sy]
		next.boundaries[y].end = min(next.boundaries[y].end, width)
		repairFrameRow(next.frame, y)
	}
	for _, cur := range cursors {
		if cur != nil {
			cur.row = clamp(cur.row-shift, 0, height-1)
			cur.col = clamp(cur.col, 0, width)
		}
	}
	*b = *next
	return evicted
}

// resize lays out only the current grid. It returns the top rows discarded by
// height reduction and maps every supplied cursor in the same layout pass.
func (b *buffer) resize(width, height int, cursors ...*bufferCursor) [][]renderer.Cell {
	b.hydrate()
	if !b.hasSoft() {
		return b.resizeFixed(width, height, cursors...)
	}
	for _, cur := range cursors {
		if cur != nil {
			b.content(cur.row, cur.col)
		}
	}
	oldW := b.frame.Width
	type logical struct {
		cells  []renderer.Cell
		points map[int]int // old row/column key -> cell offset
		reflow bool
	}
	var lines []logical
	for y := 0; y < b.frame.Height; {
		line := logical{points: make(map[int]int)}
		for {
			end := b.boundaries[y].end
			for x := 0; x <= oldW; x++ {
				line.points[y*(oldW+1)+x] = len(line.cells) + min(x, end)
			}
			line.cells = append(line.cells, b.frame.Row(y)[:end]...)
			soft := b.boundaries[y].soft && y+1 < b.frame.Height
			line.reflow = line.reflow || soft
			y++
			if !soft {
				break
			}
		}
		lines = append(lines, line)
	}

	type point struct{ row, col int }
	cursorOffsets := make([]struct{ line, offset int }, len(cursors))
	for ci, cur := range cursors {
		if cur == nil {
			continue
		}
		key := clamp(cur.row, 0, max(b.frame.Height-1, 0))*(oldW+1) + clamp(cur.col, 0, oldW)
		for li := range lines {
			if offset, ok := lines[li].points[key]; ok {
				cursorOffsets[ci] = struct{ line, offset int }{li, offset}
				break
			}
		}
	}

	var rows [][]renderer.Cell
	var bounds []lineBoundary
	positions := make([][]point, len(lines))
	for li, line := range lines {
		positions[li] = make([]point, len(line.cells)+1)
		row := make([]renderer.Cell, width)
		for x := range row {
			row[x] = renderer.BlankCell()
		}
		x := 0
		for i := 0; i < len(line.cells); {
			if line.cells[i].Continuation { // corrupt input is repaired below.
				i++
				continue
			}
			w := renderer.RuneWidth(line.cells[i].Rune)
			wide := w == 2 && i+1 < len(line.cells) && line.cells[i+1].Continuation
			if !wide || width < 2 {
				w = 1
			}
			if x+w > width && x > 0 {
				if !line.reflow {
					for j := i; j <= len(line.cells); j++ {
						positions[li][j] = point{len(rows), x}
					}
					break
				}
				rows = append(rows, row)
				bounds = append(bounds, lineBoundary{end: x, soft: true})
				row = make([]renderer.Cell, width)
				for j := range row {
					row[j] = renderer.BlankCell()
				}
				x = 0
			}
			positions[li][i] = point{len(rows), x}
			if w == 2 && width >= 2 {
				row[x], row[x+1] = line.cells[i], line.cells[i+1]
				positions[li][i+1] = point{len(rows), x + 1}
				x += 2
				i += 2
			} else {
				cell := line.cells[i]
				if wide {
					cell.Rune = '\uFFFD'
					cell.Continuation = false
				}
				row[x] = cell
				x++
				i++
			}
		}
		positions[li][len(line.cells)] = point{len(rows), x}
		rows = append(rows, row)
		bounds = append(bounds, lineBoundary{end: x})
	}
	if len(rows) == 0 {
		rows = append(rows, make([]renderer.Cell, width))
		for x := range rows[0] {
			rows[0][x] = renderer.BlankCell()
		}
		bounds = append(bounds, lineBoundary{})
	}

	anchor := 0
	if len(cursors) > 0 && cursors[0] != nil {
		co := cursorOffsets[0]
		anchor = positions[co.line][min(co.offset, len(positions[co.line])-1)].row
	}
	shift := clamp(anchor-(height-1), 0, max(len(rows)-height, 0))
	var evicted [][]renderer.Cell
	for y := 0; y < shift; y++ {
		evicted = append(evicted, append([]renderer.Cell(nil), rows[y]...))
	}
	next := newBuffer(width, height)
	for y := 0; y < height && y+shift < len(rows); y++ {
		copy(next.frame.Row(y), rows[y+shift])
		next.boundaries[y] = bounds[y+shift]
	}
	for ci, cur := range cursors {
		if cur == nil {
			continue
		}
		co := cursorOffsets[ci]
		p := positions[co.line][min(co.offset, len(positions[co.line])-1)]
		cur.row, cur.col = clamp(p.row-shift, 0, height-1), clamp(p.col, 0, width)
	}
	*b = *next
	return evicted
}

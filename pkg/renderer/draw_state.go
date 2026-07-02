package renderer

import (
	"bytes"
	"strconv"
)

// drawState carries the per-Draw terminal state model: the cursor position we
// last put the terminal at (when it is known with certainty) and the SGR pen
// currently active. It lives for exactly one Draw call: between Draws the
// terminal may be moved by other writers, so cursor tracking never persists
// (this also trivially satisfies "invalidate on Reset"). The pen starts as
// the default style because every Draw that emits output ends with \x1b[0m,
// which is this package's output contract.
type drawState struct {
	curKnown bool
	curRow   int
	curCol   int
	pen      Style
}

func newDrawState() drawState {
	return drawState{pen: DefaultStyle()}
}

func (st *drawState) setCursor(row, col int) {
	st.curKnown = true
	st.curRow = row
	st.curCol = col
}

// invalidateCursor marks the tracked position as uncertain. It is called
// whenever an emission leaves the cursor in a terminal-dependent state, most
// importantly after writing into the last column: terminals differ on
// wrap-pending behavior, so we never model it and fall back to absolute CUP.
func (st *drawState) invalidateCursor() {
	st.curKnown = false
}

// Cursor-motion candidates considered by planMove. Absolute CUP is always
// correct; the others are only valid relative to a certain tracked position.
const (
	moveCUP = 'H'  // \x1b[<r>;<c>H
	moveCR  = '\r' // carriage return: same row, column 0
	moveCUU = 'A'  // \x1b[<n>A: up, same column
	moveCUD = 'B'  // \x1b[<n>B: down, same column
	moveCUF = 'C'  // \x1b[<n>C: forward, same row
	moveCUB = 'D'  // \x1b[<n>D: backward, same row
)

// planMove picks the cheapest correct sequence to move from the tracked
// position to (row, col). "Cheapest" is decided by comparing the actual
// encoded byte lengths of every applicable candidate (no fixed thresholds):
// CUP costs 4 + digits(row+1) + digits(col+1); CR costs 1; CUU/CUD/CUF/CUB
// cost 3 for a delta of 1 (count omitted) and 3 + digits(n) otherwise.
// Relative candidates are considered only when the tracked position is
// certain; ties go to absolute CUP, the safest form.
func (st *drawState) planMove(row, col int) (op byte, n, cost int) {
	op, n, cost = moveCUP, 0, 4+decimalLen(row+1)+decimalLen(col+1)
	if !st.curKnown {
		return op, n, cost
	}
	consider := func(candOp byte, candN, candCost int) {
		if candCost < cost {
			op, n, cost = candOp, candN, candCost
		}
	}
	if row == st.curRow {
		if col == 0 {
			consider(moveCR, 0, 1)
		}
		if dc := col - st.curCol; dc > 0 {
			consider(moveCUF, dc, csiRelLen(dc))
		} else if dc < 0 {
			consider(moveCUB, -dc, csiRelLen(-dc))
		}
	} else if col == st.curCol {
		if dr := row - st.curRow; dr > 0 {
			consider(moveCUD, dr, csiRelLen(dr))
		} else {
			consider(moveCUU, -dr, csiRelLen(-dr))
		}
	}
	return op, n, cost
}

// moveTo emits the cheapest correct cursor-positioning sequence for (row, col)
// and updates tracking. Moving to the already-tracked position emits nothing.
func (st *drawState) moveTo(out *bytes.Buffer, row, col int) {
	if st.curKnown && st.curRow == row && st.curCol == col {
		return
	}
	op, n, _ := st.planMove(row, col)
	switch op {
	case moveCUP:
		writeCursor(out, row, col)
	case moveCR:
		out.WriteByte('\r')
	default:
		writeCSIRel(out, n, op)
	}
	st.setCursor(row, col)
}

// moveCost returns the encoded length moveTo would emit for (row, col).
func (st *drawState) moveCost(row, col int) int {
	if st.curKnown && st.curRow == row && st.curCol == col {
		return 0
	}
	_, _, cost := st.planMove(row, col)
	return cost
}

// setPen emits an SGR sequence if style differs from the current pen.
func (st *drawState) setPen(out *bytes.Buffer, style Style) {
	if style.Equal(st.pen) {
		return
	}
	writeStyle(out, style)
	st.pen = style
}

// isBlank reports whether a cell renders as a default-style space. This is
// the differ's notion of blank: rune ' ' or 0, default style, and not the
// continuation half of a wide pair.
func isBlank(c Cell) bool {
	return !c.Continuation && (c.Rune == ' ' || c.Rune == 0) && c.Style.Equal(DefaultStyle())
}

// Native-clear encoded lengths: EL "\x1b[K" and EL2 "\x1b[2K".
const (
	elLen  = 3
	el2Len = 4
)

// emitSpan positions the cursor and emits the cells of logical row y in
// [x, x+width), already clamped by the caller. It applies the three output
// minimizations:
//
//   - cursor motion: moveTo picks the cheapest sequence, and after emission
//     the cursor is tracked at x+width (continuation cells count as columns:
//     the terminal advances two columns per wide rune). A span that touches
//     the last column invalidates tracking instead — wrap-pending state is
//     terminal-dependent and never modeled.
//   - SGR: the pen persists across spans (st is Draw-scoped); a style change
//     is emitted only when a cell's style differs from the pen.
//   - native clears: a trailing run of blank cells that reaches the frame's
//     right edge is replaced by EL when EL is not longer than the spaces it
//     replaces (ties prefer EL: it keeps the cursor position certain). An
//     entirely blank full-width line may instead use a vertical-only move
//     plus 2K when that encoding is cheaper. Both clears require the default
//     pen (BCE: terminals fill cleared cells with the pen background).
func (r *Renderer) emitSpan(out *bytes.Buffer, frame Frame, y, x, width int, st *drawState) {
	end := x + width
	// Trailing blank suffix is EL-eligible only when the span reaches the
	// frame's right edge (EL clears to the end of the line, nothing less).
	t := end
	if end == frame.Width {
		for t > x && isBlank(frame.At(t-1, y)) {
			t--
		}
	}
	useEL := end == frame.Width && end-t >= elLen
	if !useEL {
		t = end
	}

	if useEL && t == x && x == 0 && width == frame.Width {
		// Entire line blank: compare "move to col 0 + EL" against
		// "vertical-only move + 2K" (2K clears the whole line from any
		// column, so a cheap same-column CUD/CUU can be reused).
		elCost := st.moveCost(y, 0) + elLen
		if st.curKnown && st.moveCost(y, st.curCol)+el2Len < elCost {
			st.setPen(out, DefaultStyle())
			st.moveTo(out, y, st.curCol)
			out.WriteString("\x1b[2K")
			return
		}
	}

	st.moveTo(out, y, x)
	for col := x; col < t; col++ {
		cell := frame.At(col, y)
		// Continuation cells are the right half of a wide rune. Emit nothing:
		// the terminal already advanced two columns for the left cell's rune.
		if cell.Continuation {
			continue
		}
		// Pen check kept inline (not via setPen): this is the hottest loop in
		// the renderer and the method call costs measurably on full redraws.
		if !cell.Style.Equal(st.pen) {
			writeStyle(out, cell.Style)
			st.pen = cell.Style
		}
		if cell.Rune == 0 {
			out.WriteByte(' ')
		} else {
			out.WriteRune(cell.Rune)
		}
	}
	if useEL {
		st.setPen(out, DefaultStyle())
		out.WriteString("\x1b[K")
		// EL does not move the cursor: it stays at the start of the blanks.
		st.setCursor(y, t)
	} else if end >= frame.Width {
		st.invalidateCursor()
	} else {
		st.setCursor(y, end)
	}
	// A span that splits a wide pair breaks the "terminal advance == span
	// width" assumption tracking relies on: a leading orphan continuation
	// (head outside the span) advances one column less, and a span ending on
	// a wide head whose continuation lies outside advances one more. Both are
	// malformed inputs the VT layer should never produce, but tracking must
	// stay certain, so drop it. (t is never a continuation on the EL path —
	// blanks are not continuations — so this check is exact for both paths.)
	if frame.At(x, y).Continuation || (t < frame.Width && frame.At(t, y).Continuation) {
		st.invalidateCursor()
	}
}

// writeCSIRel emits a relative cursor move "\x1b[<n><letter>", omitting the
// count when n == 1 as the CSI default allows.
func writeCSIRel(out *bytes.Buffer, n int, letter byte) {
	out.WriteString("\x1b[")
	if n != 1 {
		var b [16]byte
		out.Write(strconv.AppendInt(b[:0], int64(n), 10))
	}
	out.WriteByte(letter)
}

// csiRelLen is the encoded length of writeCSIRel's output.
func csiRelLen(n int) int {
	if n == 1 {
		return 3
	}
	return 3 + decimalLen(n)
}

// decimalLen is the number of decimal digits of a non-negative int.
func decimalLen(n int) int {
	l := 1
	for n >= 10 {
		n /= 10
		l++
	}
	return l
}

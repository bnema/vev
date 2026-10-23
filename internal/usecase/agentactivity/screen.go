package agentactivity

import (
	"bytes"
	"slices"
	"unicode/utf8"
)

// BottomRows is the number of grid rows, ending at the lowest non-blank row,
// that are classified. Chat history above this window is never read.
const BottomRows = 16

// Window returns the grid rows [start, end) to feed for a pane of the given
// height. blankRow reports whether row y holds only blank cells.
//
// The window ends at the lowest non-blank row, not at the grid bottom: inline
// renderers such as zut draw their status bar and prompt right below a short
// transcript and leave every row under them blank.
func Window(height int, blankRow func(y int) bool) (start, end int) {
	end = height
	for end > 0 && blankRow(end-1) {
		end--
	}
	return max(0, end-BottomRows), end
}

// BlankCell reports whether a cell reads as a space: WriteCell drops it or
// writes a space.
func BlankCell(r rune, continuation bool) bool {
	return continuation || r <= ' ' || (r >= 0x7f && r < 0xa0) || !utf8.ValidRune(r)
}

// Input is one extracted screen observation.
//
// Lines alias the Extractor's buffer and stay valid until its next Reset.
type Input struct {
	// Lines are the non-blank window rows, lowest row first, with trailing
	// spaces trimmed.
	Lines [][]byte
	// Title is the pane's current terminal title.
	Title string
	// TitleFromAgent reports that the agent itself has set the title since
	// it was identified. Title rules are ignored otherwise.
	TitleFromAgent bool
}

type lineRef struct{ start, end int }

// Extractor turns grid cells into an Input without allocating once its
// buffers have grown to the pane's size. The zero value is ready to use. It
// is not safe for concurrent use.
//
// Feed one observation as Reset, then for each window row in screen order
// BeginRow, WriteCell for every cell, and EndRow; then call Input. Commit
// reports whether the observation differs from the previously committed one.
type Extractor struct {
	buf   []byte
	rows  []lineRef
	lines [][]byte

	rowStart int

	prevBuf   []byte
	prevRows  []lineRef
	prevTitle string
	committed bool
}

// Reset starts a new observation.
func (e *Extractor) Reset() {
	e.buf = e.buf[:0]
	e.rows = e.rows[:0]
}

// BeginRow starts a grid row.
func (e *Extractor) BeginRow() { e.rowStart = len(e.buf) }

// WriteCell appends one cell. Continuation cells of wide runes are skipped;
// empty, control, and invalid cells read as spaces.
func (e *Extractor) WriteCell(r rune, continuation bool) {
	if continuation {
		return
	}
	if BlankCell(r, false) {
		r = ' '
	}
	e.buf = utf8.AppendRune(e.buf, r)
}

// EndRow finishes the current row: trailing spaces are trimmed and blank rows
// are dropped.
func (e *Extractor) EndRow() {
	end := len(e.buf)
	for end > e.rowStart && e.buf[end-1] == ' ' {
		end--
	}
	e.buf = e.buf[:end]
	if end > e.rowStart {
		e.rows = append(e.rows, lineRef{start: e.rowStart, end: end})
	}
}

// Input returns the current observation.
func (e *Extractor) Input(title string, titleFromAgent bool) Input {
	e.lines = e.lines[:0]
	for i := len(e.rows) - 1; i >= 0; i-- {
		ref := e.rows[i]
		e.lines = append(e.lines, e.buf[ref.start:ref.end:ref.end])
	}
	return Input{Lines: e.lines, Title: title, TitleFromAgent: titleFromAgent}
}

// Commit records the current observation and reports whether its text or
// title differs from the previously committed one. A caller may skip
// classification when it returns false and only call Tracker.Tick.
func (e *Extractor) Commit(title string) bool {
	changed := !e.committed || title != e.prevTitle ||
		!bytes.Equal(e.buf, e.prevBuf) || !slices.Equal(e.rows, e.prevRows)
	if changed {
		e.prevBuf = append(e.prevBuf[:0], e.buf...)
		e.prevRows = append(e.prevRows[:0], e.rows...)
		e.prevTitle = title
		e.committed = true
	}
	return changed
}

// Forget drops the committed observation so the next Commit reports a change.
// Call it when the pane's agent changes.
func (e *Extractor) Forget() {
	e.committed = false
	e.prevTitle = ""
}

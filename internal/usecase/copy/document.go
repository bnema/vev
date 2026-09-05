package copy

import (
	"strings"
	"unicode"

	vt "github.com/bnema/vev-vt"
	renderer "github.com/bnema/vev-vt/ansi"
)

// Pos identifies a terminal display cell in a document. Valid positions are
// always glyph heads; Normalize converts continuation cells to their head.
type Pos struct {
	Row int
	Col int
}

// CellRange is an inclusive display-cell range on one physical document row.
type CellRange struct {
	Row   int
	Start int
	End   int
}

// Document provides immutable display-cell and text operations over a Snapshot.
type Document struct {
	snapshot       Snapshot
	wordSeparators map[rune]struct{}
}

func NewDocument(snapshot Snapshot, separators string) *Document {
	set := make(map[rune]struct{}, len([]rune(separators)))
	for _, r := range separators {
		set[r] = struct{}{}
	}
	return &Document{snapshot: snapshot, wordSeparators: set}
}

func (d *Document) Snapshot() Snapshot          { return d.snapshot }
func (d *Document) Len() int                    { return d.snapshot.Len() }
func (d *Document) Width() int                  { return d.snapshot.Width }
func (d *Document) Height() int                 { return d.snapshot.Height }
func (d *Document) Row(row int) []renderer.Cell { return d.snapshot.Row(row) }
func (d *Document) RowWidth(row int) int        { return d.snapshot.RowWidth(row) }
func (d *Document) Cell(x, y int) renderer.Cell { return d.snapshot.Cell(x, y) }
func (d *Document) RowID(row int) vt.RowID      { return d.snapshot.RowID(row) }
func (d *Document) FindRowID(id vt.RowID) int   { return d.snapshot.FindRowID(id) }

// Normalize returns pos as a valid glyph head. Empty physical rows have the
// stable logical position {row, 0}, which is useful to vertical navigation.
func (d *Document) Normalize(pos Pos) (Pos, bool) {
	if pos.Row < 0 || pos.Row >= d.Len() || pos.Col < 0 {
		return Pos{}, false
	}
	width := d.RowWidth(pos.Row)
	if width == 0 {
		if pos.Col == 0 {
			return pos, true
		}
		return Pos{}, false
	}
	if pos.Col >= width {
		return Pos{}, false
	}
	for pos.Col > 0 && d.Cell(pos.Col, pos.Row).Continuation {
		pos.Col--
	}
	if d.Cell(pos.Col, pos.Row).Continuation {
		return Pos{}, false
	}
	return pos, true
}

// PrevGlyph returns the previous glyph head on pos's physical row.
func (d *Document) PrevGlyph(pos Pos) (Pos, bool) {
	pos, ok := d.Normalize(pos)
	if !ok || d.RowWidth(pos.Row) == 0 {
		return Pos{}, false
	}
	for col := pos.Col - 1; col >= 0; col-- {
		if !d.Cell(col, pos.Row).Continuation {
			return Pos{Row: pos.Row, Col: col}, true
		}
	}
	return Pos{}, false
}

// NextGlyph returns the next glyph head on pos's physical row.
func (d *Document) NextGlyph(pos Pos) (Pos, bool) {
	pos, ok := d.Normalize(pos)
	if !ok {
		return Pos{}, false
	}
	width := d.RowWidth(pos.Row)
	for col := pos.Col + 1; col < width; col++ {
		if !d.Cell(col, pos.Row).Continuation {
			return Pos{Row: pos.Row, Col: col}, true
		}
	}
	return Pos{}, false
}

// WordBounds returns the inclusive bounds of the word containing pos. Unicode
// whitespace and configured separators do not belong to words.
func (d *Document) WordBounds(pos Pos) (Pos, Pos, bool) {
	pos, ok := d.Normalize(pos)
	if !ok || !d.isGlyph(pos) || d.isSeparator(d.runeAt(pos)) {
		return Pos{}, Pos{}, false
	}
	start, end := pos, pos
	for {
		prev, ok := d.PrevGlyph(start)
		if !ok || d.isSeparator(d.runeAt(prev)) {
			break
		}
		start = prev
	}
	for {
		next, ok := d.NextGlyph(end)
		if !ok || d.isSeparator(d.runeAt(next)) {
			break
		}
		end = next
	}
	return start, end, true
}

// NextWordStart returns the beginning of the next word after pos, crossing
// physical rows as necessary. It walks only the rows between pos and the
// target instead of materializing every glyph in the document.
func (d *Document) NextWordStart(pos Pos) (Pos, bool) {
	pos, ok := d.Normalize(pos)
	if !ok {
		return Pos{}, false
	}

	candidate := pos
	if d.isGlyph(candidate) && !d.isSeparator(d.runeAt(candidate)) {
		for {
			next, ok := d.nextDocumentGlyph(candidate)
			if !ok {
				return Pos{}, false
			}
			if !d.sameWord(next, candidate) {
				candidate = next
				break
			}
			candidate = next
		}
	} else {
		candidate, ok = d.nextDocumentGlyph(candidate)
		if !ok {
			return Pos{}, false
		}
	}
	for d.isSeparator(d.runeAt(candidate)) {
		candidate, ok = d.nextDocumentGlyph(candidate)
		if !ok {
			return Pos{}, false
		}
	}
	return candidate, true
}

// PreviousWordStart returns the beginning of the current word, or of the
// preceding word when pos is already at a word beginning.
func (d *Document) PreviousWordStart(pos Pos) (Pos, bool) {
	pos, ok := d.Normalize(pos)
	if !ok {
		return Pos{}, false
	}

	if d.isGlyph(pos) && !d.isSeparator(d.runeAt(pos)) {
		previous, ok := d.previousDocumentGlyph(pos)
		if ok && d.sameWord(previous, pos) {
			for d.sameWord(previous, pos) {
				pos = previous
				previous, ok = d.previousDocumentGlyph(pos)
				if !ok {
					break
				}
			}
			return pos, true
		}
	}

	candidate, ok := d.previousDocumentGlyph(pos)
	if !ok {
		return Pos{}, false
	}
	for d.isSeparator(d.runeAt(candidate)) {
		candidate, ok = d.previousDocumentGlyph(candidate)
		if !ok {
			return Pos{}, false
		}
	}
	for {
		previous, ok := d.previousDocumentGlyph(candidate)
		if !ok || !d.sameWord(previous, candidate) {
			return candidate, true
		}
		candidate = previous
	}
}

// NextWordEnd returns the inclusive end of the current word, or of the next
// word when pos is on a separator or at the end of a word.
func (d *Document) NextWordEnd(pos Pos) (Pos, bool) {
	pos, ok := d.Normalize(pos)
	if !ok || !d.isGlyph(pos) {
		return Pos{}, false
	}

	candidate := pos
	if !d.isSeparator(d.runeAt(candidate)) {
		next, ok := d.nextDocumentGlyph(candidate)
		if !ok {
			return Pos{}, false
		}
		if !d.sameWord(next, candidate) {
			candidate = next
		}
	}
	for d.isSeparator(d.runeAt(candidate)) {
		candidate, ok = d.nextDocumentGlyph(candidate)
		if !ok {
			return Pos{}, false
		}
	}
	for {
		next, ok := d.nextDocumentGlyph(candidate)
		if !ok || !d.sameWord(next, candidate) {
			return candidate, true
		}
		candidate = next
	}
}

// LineText returns row text without duplicated continuation cells and retains
// the established copy-mode behavior of trimming trailing blank spaces.
func (d *Document) LineText(row int) string {
	return strings.TrimRight(d.cellsText(d.Row(row), 0, len(d.Row(row))-1), " ")
}

// Extract renders inclusive ranges as copyable text. Every emitted line drops
// the grid padding to its right, and rows the VT marked as soft-wrapped are
// joined only when the next emitted segment is the next physical row.
func (d *Document) Extract(ranges []CellRange) string {
	if len(ranges) == 0 {
		return ""
	}

	// Filter invalid ranges before deciding which segment is final or adjacent.
	// rangeText with logicalEnd=true performs the same normalization and bounds
	// checks while deliberately trimming, but this validation text is discarded.
	emitted := make([]CellRange, 0, len(ranges))
	for _, r := range ranges {
		if _, _, ok := d.rangeText(r, true); ok {
			emitted = append(emitted, r)
		}
	}

	var out strings.Builder
	for i, r := range emitted {
		last := i == len(emitted)-1
		adjacent := !last && emitted[i+1].Row == r.Row+1
		text, soft, _ := d.rangeText(r, last || !adjacent)
		out.WriteString(text)
		if !last && !soft {
			out.WriteByte('\n')
		}
	}
	return out.String()
}

// rangeText renders one physical row of a selection and reports whether that
// row continues into the next emitted physical row. A row that continues is cut
// at its logical extent, keeping trailing spaces the application really printed;
// any logical end is cut after its last content cell, dropping grid padding.
func (d *Document) rangeText(r CellRange, logicalEnd bool) (string, bool, bool) {
	if r.Row < 0 || r.Row >= d.Len() {
		return "", false, false
	}
	row := d.Row(r.Row)
	if len(row) == 0 {
		return "", false, r.Start == 0 && r.End == 0
	}
	start, end := min(r.Start, r.End), max(r.Start, r.End)
	if end < 0 || start >= len(row) {
		return "", false, false
	}
	start = max(start, 0)
	end = min(end, len(row)-1)

	head, ok := d.Normalize(Pos{Row: r.Row, Col: start})
	if !ok {
		return "", false, false
	}
	start = head.Col

	bound := d.snapshot.Bound(r.Row)
	soft := bound.Soft && !logicalEnd && end >= len(row)-1
	if soft {
		end = min(end, min(max(bound.End, 0), len(row))-1)
	} else {
		end = lastContentCol(row, start, end)
	}
	if end < start {
		return "", soft, true
	}

	tail, ok := d.Normalize(Pos{Row: r.Row, Col: end})
	if !ok {
		return "", soft, true
	}
	return d.cellsText(row, start, tail.Col), soft, true
}

// lastContentCol returns the rightmost column in [start, end] holding content,
// or start-1 when the span is blank. Continuation cells belong to the glyph on
// their left, so they never count on their own.
func lastContentCol(row []renderer.Cell, start, end int) int {
	for col := end; col >= start; col-- {
		cell := row[col]
		if cell.Continuation || cell.Rune == 0 || cell.Rune == ' ' {
			continue
		}
		return col
	}
	return start - 1
}

func (d *Document) cellsText(row []renderer.Cell, start, end int) string {
	if start < 0 || end < start || start >= len(row) {
		return ""
	}
	end = min(end, len(row)-1)
	var text strings.Builder
	for col := start; col <= end; col++ {
		cell := row[col]
		if cell.Continuation {
			continue
		}
		if cell.Rune == 0 {
			text.WriteByte(' ')
			continue
		}
		text.WriteRune(cell.Rune)
	}
	return text.String()
}

func (d *Document) isSeparator(r rune) bool {
	if unicode.IsSpace(r) {
		return true
	}
	_, ok := d.wordSeparators[r]
	return ok
}

func (d *Document) isGlyph(pos Pos) bool {
	return pos.Col >= 0 && pos.Col < d.RowWidth(pos.Row) && !d.Cell(pos.Col, pos.Row).Continuation
}

func (d *Document) runeAt(pos Pos) rune {
	cell := d.Cell(pos.Col, pos.Row)
	if cell.Rune == 0 {
		return ' '
	}
	return cell.Rune
}

// nextDocumentGlyph finds the next glyph in document order without building
// an index for unrelated scrollback rows.
func (d *Document) nextDocumentGlyph(pos Pos) (Pos, bool) {
	for row := pos.Row; row < d.Len(); row++ {
		width := d.RowWidth(row)
		start := 0
		if row == pos.Row {
			start = pos.Col + 1
		}
		for col := start; col < width; col++ {
			if !d.Cell(col, row).Continuation {
				return Pos{Row: row, Col: col}, true
			}
		}
	}
	return Pos{}, false
}

// previousDocumentGlyph finds the previous glyph in document order without
// building an index for unrelated scrollback rows.
func (d *Document) previousDocumentGlyph(pos Pos) (Pos, bool) {
	for row := pos.Row; row >= 0; row-- {
		start := d.RowWidth(row) - 1
		if row == pos.Row {
			start = min(pos.Col-1, start)
		}
		for col := start; col >= 0; col-- {
			if !d.Cell(col, row).Continuation {
				return Pos{Row: row, Col: col}, true
			}
		}
	}
	return Pos{}, false
}

func (d *Document) sameWord(left, right Pos) bool {
	return left.Row == right.Row && !d.isSeparator(d.runeAt(left)) && !d.isSeparator(d.runeAt(right))
}

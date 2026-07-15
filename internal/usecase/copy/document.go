package copy

import (
	"strings"
	"unicode"

	"github.com/bnema/vev/pkg/renderer"
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

// Normalize returns pos as a valid glyph head. Empty physical rows have the
// stable logical position {row, 0}, which is useful to vertical navigation.
func (d *Document) Normalize(pos Pos) (Pos, bool) {
	if pos.Row < 0 || pos.Row >= d.Len() || pos.Col < 0 {
		return Pos{}, false
	}
	row := d.Row(pos.Row)
	if len(row) == 0 {
		if pos.Col == 0 {
			return pos, true
		}
		return Pos{}, false
	}
	if pos.Col >= len(row) {
		return Pos{}, false
	}
	for pos.Col > 0 && row[pos.Col].Continuation {
		pos.Col--
	}
	if row[pos.Col].Continuation {
		return Pos{}, false
	}
	return pos, true
}

// PrevGlyph returns the previous glyph head on pos's physical row.
func (d *Document) PrevGlyph(pos Pos) (Pos, bool) {
	pos, ok := d.Normalize(pos)
	if !ok || len(d.Row(pos.Row)) == 0 {
		return Pos{}, false
	}
	for col := pos.Col - 1; col >= 0; col-- {
		if !d.Row(pos.Row)[col].Continuation {
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
	row := d.Row(pos.Row)
	for col := pos.Col + 1; col < len(row); col++ {
		if !row[col].Continuation {
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

// Extract returns inclusive ranges joined by physical-row newlines. Line-wise
// extraction trims trailing blank spaces; stream extraction preserves them.
func (d *Document) Extract(ranges []CellRange, linewise bool) string {
	if len(ranges) == 0 {
		return ""
	}
	lines := make([]string, 0, len(ranges))
	for _, r := range ranges {
		text, ok := d.rangeText(r)
		if !ok {
			continue
		}
		if linewise {
			text = strings.TrimRight(text, " ")
		}
		lines = append(lines, text)
	}
	return strings.Join(lines, "\n")
}

func (d *Document) rangeText(r CellRange) (string, bool) {
	if r.Row < 0 || r.Row >= d.Len() {
		return "", false
	}
	row := d.Row(r.Row)
	if len(row) == 0 {
		return "", r.Start == 0 && r.End == 0
	}
	start, end := min(r.Start, r.End), max(r.Start, r.End)
	if end < 0 || start >= len(row) {
		return "", false
	}
	start = max(start, 0)
	end = min(end, len(row)-1)
	headStart, ok := d.Normalize(Pos{Row: r.Row, Col: start})
	if !ok {
		return "", false
	}
	headEnd, ok := d.Normalize(Pos{Row: r.Row, Col: end})
	if !ok {
		return "", false
	}
	return d.cellsText(row, headStart.Col, headEnd.Col), true
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
	row := d.Row(pos.Row)
	return pos.Col >= 0 && pos.Col < len(row) && !row[pos.Col].Continuation
}

func (d *Document) runeAt(pos Pos) rune {
	cell := d.Row(pos.Row)[pos.Col]
	if cell.Rune == 0 {
		return ' '
	}
	return cell.Rune
}

// nextDocumentGlyph finds the next glyph in document order without building
// an index for unrelated scrollback rows.
func (d *Document) nextDocumentGlyph(pos Pos) (Pos, bool) {
	for row := pos.Row; row < d.Len(); row++ {
		cells := d.Row(row)
		start := 0
		if row == pos.Row {
			start = pos.Col + 1
		}
		for col := start; col < len(cells); col++ {
			if !cells[col].Continuation {
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
		cells := d.Row(row)
		start := len(cells) - 1
		if row == pos.Row {
			start = min(pos.Col-1, start)
		}
		for col := start; col >= 0; col-- {
			if !cells[col].Continuation {
				return Pos{Row: row, Col: col}, true
			}
		}
	}
	return Pos{}, false
}

func (d *Document) sameWord(left, right Pos) bool {
	return left.Row == right.Row && !d.isSeparator(d.runeAt(left)) && !d.isSeparator(d.runeAt(right))
}

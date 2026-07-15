package copy

import "github.com/bnema/vev/pkg/renderer"

// Granularity controls how a selection's endpoints become display-cell ranges.
type Granularity uint8

const (
	Line Granularity = iota
	Character
	Word
)

// Selection is an immutable-document selection. Anchor and Active are glyph
// head positions whenever they originate from a Document operation.
type Selection struct {
	Anchor      Pos
	Active      Pos
	Granularity Granularity
	Enabled     bool
}

// NewLineSelection starts a complete physical-line selection at at.
func NewLineSelection(at Pos) Selection {
	return Selection{Anchor: at, Active: at, Granularity: Line, Enabled: true}
}

// NewWordSelection selects the complete word containing at. Separators cannot
// begin a word selection.
func NewWordSelection(doc *Document, at Pos) (Selection, bool) {
	if doc == nil {
		return Selection{}, false
	}
	start, end, ok := doc.WordBounds(at)
	if !ok {
		return Selection{}, false
	}
	return Selection{Anchor: start, Active: end, Granularity: Word, Enabled: true}, true
}

// Ordered returns the selection endpoints in document order.
func (s Selection) Ordered() (Pos, Pos) {
	if before(s.Active, s.Anchor) {
		return s.Active, s.Anchor
	}
	return s.Anchor, s.Active
}

// Ranges returns the canonical, inclusive per-physical-row display-cell
// ranges. It returns nil for disabled selections or invalid endpoints.
func (s Selection) Ranges(doc *Document) []CellRange {
	if !s.Enabled || doc == nil {
		return nil
	}
	anchor, ok := doc.Normalize(s.Anchor)
	if !ok {
		return nil
	}
	active, ok := doc.Normalize(s.Active)
	if !ok {
		return nil
	}

	switch s.Granularity {
	case Line:
		start, end := order(anchor, active)
		return lineRanges(doc, start.Row, end.Row)
	case Character:
		start, end := order(anchor, active)
		return streamRanges(doc, start, end)
	case Word:
		anchorStart, anchorEnd, ok := doc.WordBounds(anchor)
		if !ok {
			return nil
		}
		activeStart, activeEnd, ok := doc.WordBounds(active)
		if !ok {
			return nil
		}
		start, _ := order(anchorStart, activeStart)
		_, end := order(anchorEnd, activeEnd)
		return streamRanges(doc, start, end)
	default:
		return nil
	}
}

// Text extracts the selection using its range granularity's newline and
// trailing-space semantics.
func (s Selection) Text(doc *Document) string {
	if doc == nil {
		return ""
	}
	return doc.Extract(s.Ranges(doc), s.Granularity == Line)
}

// Extend moves the active endpoint. Document normalization occurs when ranges
// are requested so callers may pass raw pointer coordinates.
func (s *Selection) Extend(at Pos) {
	s.Active = at
}

// AsCharacter changes an active selection to stream selection without moving
// either endpoint.
func (s *Selection) AsCharacter() {
	s.Granularity = Character
}

func before(left, right Pos) bool {
	return left.Row < right.Row || left.Row == right.Row && left.Col < right.Col
}

func order(left, right Pos) (Pos, Pos) {
	if before(right, left) {
		return right, left
	}
	return left, right
}

func lineRanges(doc *Document, startRow, endRow int) []CellRange {
	ranges := make([]CellRange, 0, endRow-startRow+1)
	for row := startRow; row <= endRow; row++ {
		cells := doc.Row(row)
		if len(cells) == 0 {
			ranges = append(ranges, CellRange{Row: row})
			continue
		}
		ranges = append(ranges, CellRange{Row: row, End: len(cells) - 1})
	}
	return ranges
}

func streamRanges(doc *Document, start, end Pos) []CellRange {
	ranges := make([]CellRange, 0, end.Row-start.Row+1)
	for row := start.Row; row <= end.Row; row++ {
		cells := doc.Row(row)
		if len(cells) == 0 {
			ranges = append(ranges, CellRange{Row: row})
			continue
		}

		first, last := 0, len(cells)-1
		if row == start.Row {
			first = start.Col
		}
		if row == end.Row {
			last = glyphEnd(cells, end.Col)
		}
		ranges = append(ranges, CellRange{Row: row, Start: first, End: last})
	}
	return ranges
}

// glyphEnd expands a glyph-head endpoint over all of its continuation cells.
func glyphEnd(cells []renderer.Cell, col int) int {
	for col+1 < len(cells) && cells[col+1].Continuation {
		col++
	}
	return col
}

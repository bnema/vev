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
	bounds, ok := s.bounds(doc)
	if !ok {
		return nil
	}
	ranges := make([]CellRange, 0, bounds.end.Row-bounds.start.Row+1)
	for row := bounds.start.Row; row <= bounds.end.Row; row++ {
		r, ok := bounds.rangeForRow(doc, row)
		if ok {
			ranges = append(ranges, r)
		}
	}
	return ranges
}

// RangeForRow returns the canonical inclusive range for one document row. It
// lets viewport renderers avoid materializing ranges for off-screen rows.
func (s Selection) RangeForRow(doc *Document, row int) (CellRange, bool) {
	bounds, ok := s.bounds(doc)
	if !ok {
		return CellRange{}, false
	}
	return bounds.rangeForRow(doc, row)
}

type selectionBounds struct {
	start, end Pos
	linewise   bool
}

func (s Selection) bounds(doc *Document) (selectionBounds, bool) {
	if !s.Enabled || doc == nil {
		return selectionBounds{}, false
	}
	anchor, ok := doc.Normalize(s.Anchor)
	if !ok {
		return selectionBounds{}, false
	}
	active, ok := doc.Normalize(s.Active)
	if !ok {
		return selectionBounds{}, false
	}

	switch s.Granularity {
	case Line:
		start, end := order(anchor, active)
		return selectionBounds{start: start, end: end, linewise: true}, true
	case Character:
		start, end := order(anchor, active)
		return selectionBounds{start: start, end: end}, true
	case Word:
		anchorStart, anchorEnd, ok := doc.WordBounds(anchor)
		if !ok {
			return selectionBounds{}, false
		}
		activeStart, activeEnd, ok := doc.WordBounds(active)
		if !ok {
			return selectionBounds{}, false
		}
		start, _ := order(anchorStart, activeStart)
		_, end := order(anchorEnd, activeEnd)
		return selectionBounds{start: start, end: end}, true
	default:
		return selectionBounds{}, false
	}
}

func (b selectionBounds) rangeForRow(doc *Document, row int) (CellRange, bool) {
	if row < b.start.Row || row > b.end.Row {
		return CellRange{}, false
	}
	cells := doc.Row(row)
	if len(cells) == 0 {
		return CellRange{Row: row}, true
	}
	if b.linewise {
		return CellRange{Row: row, End: len(cells) - 1}, true
	}

	start, end := 0, len(cells)-1
	if row == b.start.Row {
		start = b.start.Col
	}
	if row == b.end.Row {
		end = glyphEnd(cells, b.end.Col)
	}
	return CellRange{Row: row, Start: start, End: end}, true
}

// Text extracts the selection's ranges. The granularity is already encoded in
// those ranges, so extraction applies the same padding and wrap rules to all.
func (s Selection) Text(doc *Document) string {
	if doc == nil {
		return ""
	}
	return doc.Extract(s.Ranges(doc))
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

// glyphEnd expands a glyph-head endpoint over all of its continuation cells.
func glyphEnd(cells []renderer.Cell, col int) int {
	for col+1 < len(cells) && cells[col+1].Continuation {
		col++
	}
	return col
}

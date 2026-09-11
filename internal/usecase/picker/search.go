package picker

import (
	"slices"
	"strings"
	"unicode/utf8"

	renderer "github.com/bnema/vev-vt"
)

const MaxSearchQueryRunes = 256

type matchField uint8

const (
	matchName matchField = iota
	matchDetail
	matchContext
)

type fieldMatch struct {
	field     matchField
	positions []int
	rank      int
	span      int
	first     int
}

type searchMatch struct {
	fieldMatches []fieldMatch
	best         fieldMatch
}

func (m searchMatch) positions(field matchField) []int {
	for _, candidate := range m.fieldMatches {
		if candidate.field == field {
			return candidate.positions
		}
	}
	return nil
}

func (m *Model) EnterSearch() {
	if m == nil || m.searchActive {
		return
	}
	m.searchActive = true
	m.query.SetValue("")
	m.refreshSearch(false)
}

func (m *Model) ExitSearch() {
	if m == nil {
		return
	}
	m.searchActive = false
	m.query.SetValue("")
	m.searchMatches = nil
	m.matchRows = nil
}

func (m *Model) SearchActive() bool { return m != nil && m.searchActive }

func (m *Model) Query() string {
	if m == nil {
		return ""
	}
	return m.query.Value()
}

func (m *Model) SearchTitle(width ...int) string {
	if m == nil || !m.searchActive {
		return ""
	}
	full := " Search sessions & tabs: " + m.query.Value() + "_ "
	if len(width) == 0 || textCellWidth(full) <= width[0] {
		return full
	}
	const (
		prefix = " / "
		suffix = "_ "
	)
	fixedWidth := textCellWidth(prefix) + textCellWidth(suffix)
	if width[0] < fixedWidth {
		return tailCells("_", width[0])
	}
	available := width[0] - fixedWidth
	return prefix + tailCells(m.query.Value(), available) + suffix
}

func tailCells(text string, width int) string {
	runes := []rune(text)
	used := 0
	start := len(runes)
	for start > 0 {
		cellWidth := renderer.RuneWidth(runes[start-1])
		if used+cellWidth > width {
			break
		}
		used += cellWidth
		start--
	}
	return string(runes[start:])
}

func textCellWidth(text string) int {
	width := 0
	for _, r := range text {
		width += renderer.RuneWidth(r)
	}
	return width
}

func (m *Model) InsertSearch(r rune) {
	if m == nil || !m.searchActive || utf8.RuneCountInString(m.query.Value()) >= MaxSearchQueryRunes {
		return
	}
	m.query.Insert(r)
	m.refreshSearch(true)
}

func (m *Model) BackspaceSearch() {
	if m == nil || !m.searchActive || m.query.Value() == "" {
		return
	}
	m.query.Backspace()
	m.refreshSearch(true)
}

func (m *Model) ClearSearch() {
	if m == nil || !m.searchActive || m.query.Value() == "" {
		return
	}
	m.query.SetValue("")
	m.refreshSearch(false)
}

func (m *Model) MatchCount() int {
	if m == nil || !m.searchActive {
		return 0
	}
	return len(m.matchRows)
}

// ReplaceFrom applies a fresh canonical row snapshot while retaining the
// attachment-local search editor and exact cursor when that key still exists.
// Callers serialize this mutation with their picker ownership lock.
func (m *Model) ReplaceFrom(next *Model) {
	if m == nil || next == nil {
		return
	}
	key, hadKey := m.cursorKey()
	searchActive, query := m.searchActive, m.query.Value()
	m.intent = next.intent
	m.sort = next.sort
	m.lines = append(m.lines[:0], next.lines...)
	m.searchActive = searchActive
	m.query.SetValue(query)
	m.rebuild(key, hadKey, next.selected)
}

func (m *Model) rowMatches(idx int) bool {
	if m == nil || !m.searchActive || m.query.Value() == "" {
		return true
	}
	_, ok := m.searchMatches[idx]
	return ok
}

func (m *Model) moveSearch(delta int) {
	if m == nil {
		return
	}
	if len(m.matchRows) == 0 {
		// The query shows nothing, so no row may hold the cursor.
		m.selected = -1
		return
	}
	position := slices.Index(m.matchRows, m.selected)
	if position < 0 {
		if delta > 0 {
			m.selected = m.matchRows[0]
		} else {
			m.selected = m.matchRows[len(m.matchRows)-1]
		}
		return
	}
	next := position + delta
	if next >= 0 && next < len(m.matchRows) {
		m.selected = m.matchRows[next]
	}
}

// refreshSearch recomputes the rows the active query shows and re-places the
// cursor: the editor never leaves it on a row the query hides.
func (m *Model) refreshSearch(selectBest bool) int {
	m.searchMatches = make(map[int]searchMatch)
	m.matchRows = make([]int, 0, len(m.rows))
	query := strings.ToLower(m.query.Value())
	if query == "" {
		for idx, row := range m.rows {
			if row.focusable() {
				m.matchRows = append(m.matchRows, idx)
			}
		}
		if selectBest {
			m.normalizeCursor(-1)
		}
		return -1
	}

	needleRunes := []rune(query)
	bestIdx := -1
	var best fieldMatch
	for idx, row := range m.rows {
		if !row.focusable() {
			continue
		}
		matched, ok := matchRow(row, query, needleRunes)
		if !ok {
			continue
		}
		m.searchMatches[idx] = matched
		m.matchRows = append(m.matchRows, idx)
		if bestIdx < 0 || lessFieldMatch(matched.best, best) {
			bestIdx, best = idx, matched.best
		}
	}
	if selectBest {
		if bestIdx >= 0 {
			m.selected = bestIdx
		} else {
			m.selected = -1
		}
		m.normalizeCursor(bestIdx)
	}
	return bestIdx
}

// normalizeCursor places the cursor on a row the active query still shows. It
// keeps the current row when it qualifies, otherwise it takes the best match,
// then the nearest eligible row, and finally clears the cursor when nothing
// qualifies.
func (m *Model) normalizeCursor(best int) {
	if m == nil || m.eligible(m.selected) {
		return
	}
	if best >= 0 {
		m.selected = best
		return
	}
	hint := m.selected
	m.selected = -1
	m.SelectNearestRow(hint)
	if m.selected < 0 {
		m.selected = m.firstEligible()
	}
}

func matchRow(row row, query string, needleRunes []rune) (searchMatch, bool) {
	fields := []struct {
		kind matchField
		text string
	}{
		{matchName, row.foldedLabel},
		{matchDetail, row.foldedDetail},
	}

	var result searchMatch
	for _, field := range fields {
		match, ok := scoreField(field.kind, field.text, query, needleRunes)
		if !ok {
			continue
		}
		result.fieldMatches = append(result.fieldMatches, match)
		if len(result.fieldMatches) == 1 || lessFieldMatch(match, result.best) {
			result.best = match
		}
	}
	return result, len(result.fieldMatches) != 0
}

func scoreField(field matchField, text, query string, needle []rune) (fieldMatch, bool) {
	if len(needle) == 0 {
		return fieldMatch{field: field}, true
	}
	if text == query {
		positions := rangeRunePositions(len(needle))
		return fieldMatch{field: field, positions: positions, rank: 0, span: len(positions)}, true
	}
	if strings.HasPrefix(text, query) {
		positions := rangeRunePositions(len(needle))
		return fieldMatch{field: field, positions: positions, rank: 1, span: len(positions)}, true
	}
	positions, ok := subsequenceRunePositions([]rune(text), needle)
	if !ok {
		return fieldMatch{}, false
	}
	return fieldMatch{field: field, positions: positions, rank: 2, span: positions[len(positions)-1] - positions[0] + 1, first: positions[0]}, true
}

func lessFieldMatch(left, right fieldMatch) bool {
	if left.rank != right.rank {
		return left.rank < right.rank
	}
	if left.span != right.span {
		return left.span < right.span
	}
	if left.first != right.first {
		return left.first < right.first
	}
	return left.field < right.field
}

func rangeRunePositions(length int) []int {
	positions := make([]int, length)
	for i := range positions {
		positions[i] = i
	}
	return positions
}

func subsequenceRunePositions(haystack, needle []rune) ([]int, bool) {
	if len(needle) == 0 {
		return nil, true
	}
	positions := make([]int, 0, len(needle))
	next := 0
	for idx, r := range haystack {
		if r != needle[next] {
			continue
		}
		positions = append(positions, idx)
		next++
		if next == len(needle) {
			return positions, true
		}
	}
	return nil, false
}

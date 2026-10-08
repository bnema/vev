package picker

import (
	"strings"
	"unicode/utf8"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/usecase/fuzzy"
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
}

type searchMatch struct {
	fieldMatches []fieldMatch
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
	m.refreshView()
	m.normalizeCursor()
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
	m.refreshSearch(true)
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

// refreshSearch recomputes the rows the active query shows and re-places the
// cursor: the editor never leaves it on a row the query hides. Matches keep
// the list's tree order, so typing narrows the list without reordering it;
// when the query changed (selectFirst) the cursor rests on the first match.
func (m *Model) refreshSearch(selectFirst bool) {
	m.searchMatches = make(map[int]searchMatch)
	m.matchRows = make([]int, 0, len(m.rows))
	query := strings.ToLower(m.query.Value())
	if query == "" {
		for idx, row := range m.rows {
			if row.focusable() {
				m.matchRows = append(m.matchRows, idx)
			}
		}
		m.refreshView()
		if selectFirst {
			m.normalizeCursor()
		}
		return
	}

	needleRunes := []rune(query)
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
	}
	m.refreshView()
	if selectFirst {
		m.selected = -1
		if len(m.matchRows) > 0 {
			m.selected = m.matchRows[0]
		}
	}
}

// normalizeCursor places the cursor on a drawn row. It keeps the current row
// when it qualifies; otherwise a query takes its first match, a fold hands the
// cursor to the collapsed section, and anything else takes the nearest
// eligible row. It clears the cursor when nothing qualifies.
func (m *Model) normalizeCursor() {
	if m == nil || m.eligible(m.selected) {
		return
	}
	if m.searchRestricted() {
		// The query shows nothing the cursor may rest on.
		m.selected = -1
		if len(m.matchRows) > 0 {
			m.selected = m.matchRows[0]
		}
		return
	}
	if m.selected >= 0 && m.selected < len(m.hidden) && m.hidden[m.selected] {
		// A row a collapsed section hides leaves the cursor on that section.
		if section := m.sectionOf(m.selected); m.eligible(section) {
			m.selected = section
			return
		}
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
		{matchContext, row.foldedContext},
	}

	var result searchMatch
	for _, field := range fields {
		match, ok := scoreField(field.kind, field.text, query, needleRunes)
		if !ok {
			continue
		}
		result.fieldMatches = append(result.fieldMatches, match)
	}
	return result, len(result.fieldMatches) != 0
}

func scoreField(field matchField, text, query string, needle []rune) (fieldMatch, bool) {
	matched, ok := fuzzy.Match(text, query, needle)
	if !ok {
		return fieldMatch{}, false
	}
	return fieldMatch{field: field, positions: matched.Positions}, true
}

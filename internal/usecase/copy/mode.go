package copy

import (
	"encoding/base64"
	"slices"
	"strings"
	"unicode"

	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

// OSC52MaxPayloadBytes caps clipboard payloads while vev intentionally emits
// exactly one OSC 52 sequence. Splitting one clipboard copy across multiple OSC
// 52 sequences replaces the clipboard repeatedly in common terminals, corrupting
// the intended result, so oversized selections are deferred until a terminal-
// specific continuation protocol is supported.
const OSC52MaxPayloadBytes = 75_000

// Snapshot is the immutable scrollback-mode document: history followed by the
// visible screen. VT HistoryView retains immutable chunks; the visible frame is
// cloned once when the snapshot is constructed.
type Snapshot struct {
	history vt.HistoryView
	screen  renderer.Frame
	Width   int
	Height  int
}

// NewSnapshot freezes the current VT history view and clones the visible screen.
func NewSnapshot(historySource *vt.History, screen renderer.Frame) Snapshot {
	var history vt.HistoryView
	if historySource != nil {
		history = historySource.View()
	}
	return Snapshot{history: history, screen: screen.Clone(), Width: screen.Width, Height: screen.Height}
}

// NewSnapshotFromRows constructs a snapshot that owns explicit caller rows.
// It is intended for callers that already have a complete document rather than
// a scrollback and visible frame.
func NewSnapshotFromRows(rows [][]renderer.Cell, width, height int) Snapshot {
	history := vt.NewHistory(vt.HistoryConfig{MaxRows: len(rows), ChunkRows: 256})
	for _, row := range rows {
		history.Append(row)
	}
	return Snapshot{history: history.SealAndView(), Width: width, Height: height}
}

// Len returns the number of document rows.
func (s Snapshot) Len() int { return s.history.Len() + s.screen.Height }

// Row returns document row i, or nil when i is out of range.
func (s Snapshot) Row(i int) []renderer.Cell {
	if i < 0 {
		return nil
	}
	if i < s.history.Len() {
		return s.history.Row(i)
	}
	i -= s.history.Len()
	if i >= s.screen.Height {
		return nil
	}
	return s.screen.Row(i)
}

func (s Snapshot) rangeRows(yield func(int, []renderer.Cell) bool) {
	rowIndex := 0
	stopped := false
	s.history.Range(func(row []renderer.Cell) bool {
		if !yield(rowIndex, row) {
			stopped = true
			return false
		}
		rowIndex++
		return true
	})
	if stopped {
		return
	}
	for y := range s.screen.Height {
		if !yield(rowIndex+y, s.screen.Row(y)) {
			return
		}
	}
}

// Mode stores per-client scrollback viewport and line-selection state.
type SearchMatch struct {
	Row   int
	Start int
	End   int
	Text  string
}

type Mode struct {
	ViewportTop int
	Cursor      int
	Anchor      int
	Selecting   bool
	SearchQuery string
	Searches    []SearchMatch
	SearchIndex int
}

func NewMode(s Snapshot) *Mode {
	m := &Mode{Anchor: -1}
	m.ViewportTop = max(s.Len()-s.Height, 0)
	m.Cursor = max(s.Len()-1, 0)
	m.clamp(s)
	return m
}

func (m *Mode) Move(s Snapshot, delta int) { m.SetCursor(s, m.Cursor+delta) }
func (m *Mode) Page(s Snapshot, pages int) { m.SetCursor(s, m.Cursor+pages*max(s.Height, 1)) }
func (m *Mode) Top(s Snapshot)             { m.SetCursor(s, 0) }
func (m *Mode) Bottom(s Snapshot)          { m.SetCursor(s, s.Len()-1) }

func FindMatches(s Snapshot, query string) []SearchMatch {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil
	}
	needle := lowerRunes(query)
	matches := make([]SearchMatch, 0)
	s.rangeRows(func(row int, cells []renderer.Cell) bool {
		haystack, cellIndexes := searchableCells(cells)
		var text string
		for start := 0; start+len(needle) <= len(haystack); {
			if !slices.Equal(haystack[start:start+len(needle)], needle) {
				start++
				continue
			}
			if text == "" {
				text = rowString(cells)
			}
			end := start + len(needle)
			cellEnd := len(cells)
			if end < len(cellIndexes) {
				cellEnd = cellIndexes[end]
			}
			matches = append(matches, SearchMatch{Row: row, Start: cellIndexes[start], End: cellEnd, Text: text})
			start += len(needle)
		}
		return true
	})
	return matches
}

func searchableCells(cells []renderer.Cell) ([]rune, []int) {
	runes := make([]rune, 0, len(cells))
	cellIndexes := make([]int, 0, len(cells))
	for x, cell := range cells {
		if cell.Continuation {
			continue
		}
		r := cell.Rune
		if r == 0 {
			r = ' '
		}
		runes = append(runes, unicode.ToLower(r))
		cellIndexes = append(cellIndexes, x)
	}
	return runes, cellIndexes
}

func lowerRunes(s string) []rune {
	runes := []rune(s)
	for i, r := range runes {
		runes[i] = unicode.ToLower(r)
	}
	return runes
}

func (m *Mode) Search(s Snapshot, query string) bool {
	return m.SetSearchMatches(s, query, FindMatches(s, query), 0)
}

func (m *Mode) SetSearchMatches(s Snapshot, query string, matches []SearchMatch, index int) bool {
	if len(matches) == 0 {
		m.SearchQuery = strings.TrimSpace(query)
		m.Searches = nil
		m.SearchIndex = -1
		return false
	}
	m.SearchQuery = strings.TrimSpace(query)
	m.Searches = append(m.Searches[:0], matches...)
	if index < 0 {
		index = 0
	}
	if index >= len(matches) {
		index = len(matches) - 1
	}
	m.SearchIndex = index
	m.SetCursor(s, matches[index].Row)
	return true
}

func (m *Mode) NextSearchMatch(s Snapshot, delta int) bool {
	if len(m.Searches) == 0 || delta == 0 {
		return false
	}
	m.SearchIndex = (m.SearchIndex + delta) % len(m.Searches)
	if m.SearchIndex < 0 {
		m.SearchIndex += len(m.Searches)
	}
	m.SetCursor(s, m.Searches[m.SearchIndex].Row)
	return true
}

// SetCursor moves the visual cursor to row, clamping it to the snapshot and
// scrolling the viewport just enough to keep it visible.
func (m *Mode) SetCursor(s Snapshot, row int) { m.setCursor(s, row) }

// StartSelectionAt starts a line-wise visual selection at row.
func (m *Mode) StartSelectionAt(s Snapshot, row int) {
	m.SetCursor(s, row)
	m.Anchor = m.Cursor
	m.Selecting = true
}

// ExtendTo extends an active line-wise visual selection to row while preserving
// the original anchor.
func (m *Mode) ExtendTo(s Snapshot, row int) { m.SetCursor(s, row) }

func (m *Mode) AtBottom(s Snapshot) bool {
	total := s.Len()
	if total == 0 {
		return m.Cursor == 0 && m.ViewportTop == 0
	}
	return m.Cursor == total-1 && m.ViewportTop == max(total-max(s.Height, 1), 0)
}

func (m *Mode) ToggleSelection() {
	if m.Selecting {
		m.Selecting = false
		m.Anchor = -1
		return
	}
	m.Selecting = true
	m.Anchor = m.Cursor
}

func (m *Mode) setCursor(s Snapshot, cursor int) {
	m.Cursor = cursor
	m.clamp(s)
	if m.Cursor < m.ViewportTop {
		m.ViewportTop = m.Cursor
	}
	if bottom := m.ViewportTop + max(s.Height, 1) - 1; m.Cursor > bottom {
		m.ViewportTop = m.Cursor - max(s.Height, 1) + 1
	}
	m.clamp(s)
}

func (m *Mode) clamp(s Snapshot) {
	total := s.Len()
	if total == 0 {
		m.Cursor, m.ViewportTop = 0, 0
		return
	}
	m.Cursor = min(max(m.Cursor, 0), total-1)
	maxTop := max(total-max(s.Height, 1), 0)
	m.ViewportTop = min(max(m.ViewportTop, 0), maxTop)
}

func (m *Mode) SelectedBounds() (int, int, bool) {
	if !m.Selecting || m.Anchor < 0 {
		return 0, 0, false
	}
	lo, hi := m.Anchor, m.Cursor
	if lo > hi {
		lo, hi = hi, lo
	}
	return lo, hi, true
}

func (m *Mode) SelectedText(s Snapshot) string {
	lo, hi, ok := m.SelectedBounds()
	if !ok || s.Len() == 0 {
		return ""
	}
	lo = max(lo, 0)
	hi = min(hi, s.Len()-1)
	lines := make([]string, 0, hi-lo+1)
	for i := lo; i <= hi; i++ {
		lines = append(lines, rowString(s.Row(i)))
	}
	return strings.Join(lines, "\n")
}

func (m *Mode) Render(s Snapshot, styles ...renderer.Style) renderer.Frame {
	m.clamp(s)
	frame := renderer.NewFrame(s.Width, s.Height+1)
	lo, hi, selected := m.SelectedBounds()
	selectionStyle, hasSelectionStyle := optionalStyle(styles, 1)
	for y := range s.Height {
		src := m.ViewportTop + y
		if src >= s.Len() {
			break
		}
		row := frame.Row(y)
		copy(row, s.Row(src))
		lineSelected := selected && src >= lo && src <= hi
		if lineSelected {
			for x := range row {
				applySelectionStyle(&row[x].Style, selectionStyle, hasSelectionStyle)
			}
		}
		if match, ok := m.currentSearchMatchForRow(src); ok {
			for x := match.Start; x < match.End && x < len(row); x++ {
				applySelectionStyle(&row[x].Style, selectionStyle, hasSelectionStyle)
			}
		}
		if src == m.Cursor && !lineSelected && len(row) > 0 {
			applySelectionStyle(&row[0].Style, selectionStyle, hasSelectionStyle)
		}
	}
	style := inverseStyle()
	if len(styles) > 0 {
		style = styles[0]
	}
	drawCopyStatus(frame.Row(s.Height), m, s.Len(), style)
	return frame
}

func (m *Mode) currentSearchMatchForRow(row int) (SearchMatch, bool) {
	if m.SearchIndex < 0 || m.SearchIndex >= len(m.Searches) {
		return SearchMatch{}, false
	}
	match := m.Searches[m.SearchIndex]
	return match, match.Row == row
}

func optionalStyle(styles []renderer.Style, idx int) (renderer.Style, bool) {
	if idx >= len(styles) {
		return renderer.Style{}, false
	}
	return styles[idx], true
}

func applySelectionStyle(dst *renderer.Style, selection renderer.Style, ok bool) {
	if !ok || selection.Equal(inverseStyle()) {
		dst.Inverse = true
		return
	}
	*dst = selection
}

func inverseStyle() renderer.Style {
	style := renderer.DefaultStyle()
	style.Inverse = true
	return style
}

func rowString(row []renderer.Cell) string {
	runes := make([]rune, 0, len(row))
	for _, c := range row {
		if c.Continuation {
			continue
		}
		r := c.Rune
		if r == 0 {
			r = ' '
		}
		runes = append(runes, r)
	}
	return strings.TrimRight(string(runes), " ")
}

func drawCopyStatus(row []renderer.Cell, m *Mode, total int, style renderer.Style) {
	for i := range row {
		row[i] = renderer.BlankCell()
	}
	text := " [SCROLL] "
	if m.Selecting {
		text = " [SELECT] "
	}
	if total > 0 {
		text += strconvItoa(m.Cursor+1) + "/" + strconvItoa(total) + " "
	} else {
		text += "0/0 "
	}
	if m.SearchQuery != "" {
		if len(m.Searches) > 0 && m.SearchIndex >= 0 && m.SearchIndex < len(m.Searches) {
			text += strconvItoa(m.SearchIndex+1) + "/" + strconvItoa(len(m.Searches)) + " "
		}
		text += "/" + m.SearchQuery + " "
	}
	for i, r := range text {
		if i >= len(row) {
			break
		}
		row[i] = renderer.Cell{Rune: r, Style: style}
	}
}

// OSC52 encodes text for clipboard transfer as one complete OSC 52 sequence.
// Oversized payloads return no sequence rather than emitting corrupting
// multi-sequence replacements.
func OSC52(text string) [][]byte {
	if len([]byte(text)) > OSC52MaxPayloadBytes {
		return nil
	}
	encoded := base64.StdEncoding.EncodeToString([]byte(text))
	return [][]byte{[]byte("\x1b]52;c;" + encoded + "\x07")}
}

// OSC52FromBase64 builds the normalized OSC 52 sequence to forward a
// clipboard set request that a pane app already emitted as base64. It
// returns nil if b64 is not valid base64 or its decoded length exceeds
// OSC52MaxPayloadBytes — callers must drop the request silently in that case.
func OSC52FromBase64(b64 string) []byte {
	decoded, err := base64.StdEncoding.DecodeString(b64)
	if err != nil || len(decoded) > OSC52MaxPayloadBytes {
		return nil
	}
	return []byte("\x1b]52;c;" + b64 + "\x07")
}

func strconvItoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

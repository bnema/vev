package copy

import (
	"encoding/base64"
	"slices"
	"strings"
	"unicode"

	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

const OSC52MaxPayloadBytes = 75_000

// Snapshot is the immutable scrollback document: sealed history plus a cloned screen.
type Snapshot struct {
	history       vt.HistoryView
	screen        renderer.Frame
	Width, Height int
}

func NewSnapshot(historySource *vt.History, screen renderer.Frame) Snapshot {
	var history vt.HistoryView
	if historySource != nil {
		history = historySource.SealAndView()
	}
	return Snapshot{history: history, screen: screen.Clone(), Width: screen.Width, Height: screen.Height}
}
func NewSnapshotFromRows(rows [][]renderer.Cell, width, height int) Snapshot {
	history := vt.NewHistory(vt.HistoryConfig{MaxRows: len(rows), ChunkRows: 256})
	for _, row := range rows {
		history.Append(row)
	}
	return Snapshot{history: history.SealAndView(), Width: width, Height: height}
}
func (s Snapshot) Len() int { return s.history.Len() + s.screen.Height }
func (s Snapshot) Row(i int) []renderer.Cell {
	if i < 0 {
		return nil
	}
	if i < s.history.Len() {
		return s.history.BorrowedRow(i)
	}
	i -= s.history.Len()
	if i >= s.screen.Height {
		return nil
	}
	return s.screen.Row(i)
}
func (s Snapshot) rangeRows(yield func(int, []renderer.Cell) bool) {
	i := 0
	stopped := false
	s.history.Range(func(r []renderer.Cell) bool {
		if !yield(i, r) {
			stopped = true
			return false
		}
		i++
		return true
	})
	if stopped {
		return
	}
	for y := range s.screen.Height {
		if !yield(i+y, s.screen.Row(y)) {
			return
		}
	}
}

type SearchMatch struct {
	Row, Start, End int
	Text            string
} // End is exclusive display-cell offset.

type Mode struct {
	document    *Document
	navigator   Navigator
	selection   Selection
	ViewportTop int
	SearchQuery string
	Searches    []SearchMatch
	SearchIndex int
}

func NewMode(doc *Document) *Mode {
	m := &Mode{document: doc, SearchIndex: -1}
	if doc != nil && doc.Len() > 0 {
		m.navigator = NewNavigator(Pos{Row: doc.Len() - 1})
		m.navigator.Set(doc, m.navigator.Pos)
		m.ViewportTop = max(doc.Len()-max(doc.Height(), 1), 0)
	}
	return m
}
func (m *Mode) Document() *Document {
	if m == nil {
		return nil
	}
	return m.document
}
func (m *Mode) Cursor() Pos {
	if m == nil {
		return Pos{}
	}
	return m.navigator.Pos
}
func (m *Mode) Selection() Selection {
	if m == nil {
		return Selection{}
	}
	return m.selection
}
func (m *Mode) SetPosition(p Pos) bool {
	if m == nil {
		return false
	}
	changed := m.navigator.Set(m.document, p)
	m.adjustViewport()
	return changed
}
func (m *Mode) move(op func(*Document) bool, stream bool) bool {
	if m == nil || m.document == nil {
		return false
	}
	if stream && m.selection.Enabled && m.selection.Granularity == Line {
		m.selection.AsCharacter()
	}
	changed := op(m.document)
	if changed {
		if m.selection.Enabled {
			m.selection.Extend(m.navigator.Pos)
		}
		m.adjustViewport()
	}
	return changed
}
func (m *Mode) Left() bool         { return m.move(m.navigator.Left, true) }
func (m *Mode) Right() bool        { return m.move(m.navigator.Right, true) }
func (m *Mode) Up() bool           { return m.move(m.navigator.Up, false) }
func (m *Mode) Down() bool         { return m.move(m.navigator.Down, false) }
func (m *Mode) WordNext() bool     { return m.move(m.navigator.WordNext, true) }
func (m *Mode) WordBackward() bool { return m.move(m.navigator.WordBackward, true) }
func (m *Mode) WordEnd() bool      { return m.move(m.navigator.WordEnd, true) }
func (m *Mode) MoveRows(delta int) bool {
	if delta == 0 {
		return false
	}
	changed := false
	op := m.Up
	if delta > 0 {
		op = m.Down
	}
	for range abs(delta) {
		if !op() {
			break
		}
		changed = true
	}
	return changed
}
func (m *Mode) Page(pages int) bool {
	if m == nil || m.document == nil {
		return false
	}
	return m.move(func(d *Document) bool { return m.navigator.Page(d, pages*max(d.Height(), 1)) }, false)
}
func (m *Mode) Top() bool    { return m.move(m.navigator.Top, false) }
func (m *Mode) Bottom() bool { return m.move(m.navigator.Bottom, false) }
func (m *Mode) AtBottom() bool {
	if m == nil || m.document == nil || m.document.Len() == 0 {
		return m != nil && m.ViewportTop == 0
	}
	return m.navigator.Pos.Row == m.document.Len()-1 && m.ViewportTop == max(m.document.Len()-max(m.document.Height(), 1), 0)
}
func (m *Mode) ToggleLineSelection() {
	if m.selection.Enabled {
		m.selection = Selection{}
		return
	}
	m.selection = NewLineSelection(m.navigator.Pos)
}
func (m *Mode) StartCharacterSelection(p Pos) bool {
	if m == nil || m.document == nil {
		return false
	}
	p, ok := m.document.Normalize(p)
	if !ok {
		return false
	}
	m.navigator.Set(m.document, p)
	m.selection = Selection{Anchor: p, Active: p, Granularity: Character, Enabled: true}
	m.adjustViewport()
	return true
}
func (m *Mode) ExtendCharacterSelection(p Pos) bool {
	if m == nil || !m.selection.Enabled {
		return false
	}
	p, ok := m.document.Normalize(p)
	if !ok {
		return false
	}
	m.selection.AsCharacter()
	m.selection.Extend(p)
	m.navigator.Set(m.document, p)
	m.adjustViewport()
	return true
}
func (m *Mode) SelectWordAt(p Pos) bool {
	if m == nil {
		return false
	}
	s, ok := NewWordSelection(m.document, p)
	if !ok {
		return false
	}
	m.selection = s
	m.navigator.Set(m.document, s.Active)
	m.adjustViewport()
	return true
}
func (m *Mode) ExtendWordSelection(p Pos) bool {
	if m == nil || !m.selection.Enabled {
		return false
	}
	_, end, ok := m.document.WordBounds(p)
	if !ok {
		return false
	}
	m.selection.Granularity = Word
	m.selection.Extend(end)
	m.navigator.Set(m.document, end)
	m.adjustViewport()
	return true
}
func (m *Mode) Search(query string) bool {
	return m.SetSearchMatches(query, FindMatches(m.document, query), 0)
}
func (m *Mode) SetSearchMatches(query string, matches []SearchMatch, index int) bool {
	if m == nil {
		return false
	}
	m.SearchQuery = strings.TrimSpace(query)
	if len(matches) == 0 {
		m.Searches = nil
		m.SearchIndex = -1
		return false
	}
	m.Searches = append(m.Searches[:0], matches...)
	m.SearchIndex = min(max(index, 0), len(matches)-1)
	m.moveToSearchMatch(matches[m.SearchIndex])
	return true
}
func (m *Mode) NextSearchMatch(delta int) bool {
	if m == nil || len(m.Searches) == 0 || delta == 0 {
		return false
	}
	m.SearchIndex = (m.SearchIndex + delta) % len(m.Searches)
	if m.SearchIndex < 0 {
		m.SearchIndex += len(m.Searches)
	}
	match := m.Searches[m.SearchIndex]
	m.moveToSearchMatch(match)
	return true
}
func (m *Mode) moveToSearchMatch(match SearchMatch) {
	if m == nil || m.document == nil {
		return
	}
	if m.navigator.Set(m.document, Pos{Row: match.Row, Col: match.Start}) && m.selection.Enabled {
		m.selection.Extend(m.navigator.Pos)
	}
	m.adjustViewport()
}
func (m *Mode) SelectedText() string {
	if m == nil {
		return ""
	}
	return m.selection.Text(m.document)
}
func (m *Mode) adjustViewport() {
	if m == nil || m.document == nil || m.document.Len() == 0 {
		return
	}
	rows := max(m.document.Height(), 1)
	if m.navigator.Pos.Row < m.ViewportTop {
		m.ViewportTop = m.navigator.Pos.Row
	}
	if m.navigator.Pos.Row >= m.ViewportTop+rows {
		m.ViewportTop = m.navigator.Pos.Row - rows + 1
	}
	m.ViewportTop = min(max(m.ViewportTop, 0), max(m.document.Len()-rows, 0))
}

func FindMatches(doc *Document, query string) []SearchMatch {
	query = strings.TrimSpace(query)
	if doc == nil || query == "" {
		return nil
	}
	needle := lowerRunes(query)
	matches := []SearchMatch{}
	doc.Snapshot().rangeRows(func(row int, cells []renderer.Cell) bool {
		hay, indexes := searchableCells(cells)
		text := ""
		for start := 0; start+len(needle) <= len(hay); {
			if !slices.Equal(hay[start:start+len(needle)], needle) {
				start++
				continue
			}
			if text == "" {
				text = doc.LineText(row)
			}
			end := start + len(needle)
			cellEnd := len(cells)
			if end < len(indexes) {
				cellEnd = indexes[end]
			}
			matches = append(matches, SearchMatch{Row: row, Start: indexes[start], End: cellEnd, Text: text})
			start += len(needle)
		}
		return true
	})
	return matches
}
func searchableCells(cells []renderer.Cell) ([]rune, []int) {
	rs := make([]rune, 0, len(cells))
	is := make([]int, 0, len(cells))
	for x, c := range cells {
		if c.Continuation {
			continue
		}
		r := c.Rune
		if r == 0 {
			r = ' '
		}
		rs = append(rs, unicode.ToLower(r))
		is = append(is, x)
	}
	return rs, is
}
func lowerRunes(s string) []rune {
	rs := []rune(s)
	for i, r := range rs {
		rs[i] = unicode.ToLower(r)
	}
	return rs
}

func (m *Mode) Render(styles ...renderer.Style) renderer.Frame {
	if m == nil || m.document == nil {
		return renderer.NewFrame(0, 0)
	}
	d := m.document
	frame := renderer.NewFrame(d.Width(), d.Height()+1)
	selectionBounds, hasSelectionBounds := m.selection.bounds(d)
	selection, hasSelection := optionalStyle(styles, 1)
	cursor, cursorValid := d.Normalize(m.navigator.Pos)
	for y := range d.Height() {
		src := m.ViewportTop + y
		if src >= d.Len() {
			break
		}
		row := frame.Row(y)
		copy(row, d.Row(src))
		if match, ok := m.currentSearchMatchForRow(src); ok {
			for x := max(match.Start, 0); x < min(match.End, len(row)); x++ {
				applySelectionStyle(&row[x].Style, selection, hasSelection)
			}
		}
		cursorCovered := false
		if hasSelectionBounds {
			if r, ok := selectionBounds.rangeForRow(d, src); ok {
				for x := max(r.Start, 0); x <= min(r.End, len(row)-1); x++ {
					applySelectionStyle(&row[x].Style, selection, hasSelection)
				}
				if cursorValid && r.Row == cursor.Row && cursor.Col >= r.Start && cursor.Col <= r.End {
					cursorCovered = true
				}
			}
		}
		if src == cursor.Row && cursorValid && !cursorCovered && len(row) > 0 {
			applySelectionStyle(&row[cursor.Col].Style, selection, hasSelection)
		}
	}
	status := inverseStyle()
	if len(styles) > 0 {
		status = styles[0]
	}
	drawCopyStatus(frame.Row(d.Height()), m, d.Len(), status)
	return frame
}
func (m *Mode) currentSearchMatchForRow(row int) (SearchMatch, bool) {
	if m == nil || m.SearchIndex < 0 || m.SearchIndex >= len(m.Searches) {
		return SearchMatch{}, false
	}
	v := m.Searches[m.SearchIndex]
	return v, v.Row == row
}
func optionalStyle(styles []renderer.Style, idx int) (renderer.Style, bool) {
	if idx >= len(styles) {
		return renderer.Style{}, false
	}
	return styles[idx], true
}
func applySelectionStyle(dst *renderer.Style, style renderer.Style, ok bool) {
	if !ok || style.Equal(inverseStyle()) {
		dst.Inverse = true
		return
	}
	*dst = style
}
func inverseStyle() renderer.Style { s := renderer.DefaultStyle(); s.Inverse = true; return s }
func drawCopyStatus(row []renderer.Cell, m *Mode, total int, style renderer.Style) {
	for i := range row {
		row[i] = renderer.BlankCell()
	}
	text := " [SCROLL] "
	if m.selection.Enabled {
		text = " [SELECT] "
	}
	if total > 0 {
		text += strconvItoa(m.navigator.Pos.Row+1) + "/" + strconvItoa(total) + " "
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

func OSC52(text string) [][]byte {
	if len([]byte(text)) > OSC52MaxPayloadBytes {
		return nil
	}
	return [][]byte{[]byte("\x1b]52;c;" + base64.StdEncoding.EncodeToString([]byte(text)) + "\x07")}
}
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
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

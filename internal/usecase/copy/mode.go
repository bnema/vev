package copy

import (
	"encoding/base64"
	"math"
	"slices"
	"strings"
	"unicode"

	vt "github.com/bnema/vev-vt"
	renderer "github.com/bnema/vev-vt/ansi"
)

const OSC52MaxPayloadBytes = 75_000

// Snapshot is the immutable scrollback document: sealed history plus a cloned
// screen. screenBounds and screenRowIDs are parallel to screen rows, exactly as
// the history view's own metadata is parallel to its chunk rows.
type Snapshot struct {
	history       vt.HistoryView
	screen        renderer.Frame
	screenBounds  []vt.LineBound
	screenRowIDs  []vt.RowID
	Width, Height int
}

func NewSnapshot(historySource *vt.History, screen vt.CellSource, bounds []vt.LineBound, rowIDs []vt.RowID) Snapshot {
	return newSnapshot(historySource, screen, bounds, append([]vt.RowID(nil), rowIDs...))
}

// NewSnapshotFromScreen captures a screen while its owner holds the Screen
// lock. Screen.RowIDs returns the one owned copy required for immutable
// snapshot state; newSnapshot takes that slice directly without duplicating
// it. The live screen's rowIDs cannot be transferred or shared because later
// scroll, resize, and clear mutations update that storage.
func NewSnapshotFromScreen(historySource *vt.History, screen *vt.Screen) Snapshot {
	if screen == nil {
		return NewSnapshot(historySource, renderer.Frame{}, nil, nil)
	}
	return newSnapshot(historySource, screen, screen.LineBounds(), screen.RowIDs())
}

func newSnapshot(historySource *vt.History, screen vt.CellSource, bounds []vt.LineBound, screenRowIDs []vt.RowID) Snapshot {
	owned := renderer.NewFrame(screen.Columns(), screen.Rows())
	for y := range screen.Rows() {
		for x := range screen.Columns() {
			owned.Set(x, y, screen.Cell(x, y))
		}
	}
	var history vt.HistoryView
	if historySource != nil {
		history = historySource.SealAndView()
	}
	return Snapshot{
		history:      history,
		screen:       owned,
		screenBounds: append([]vt.LineBound(nil), bounds...),
		screenRowIDs: screenRowIDs,
		Width:        screen.Columns(),
		Height:       screen.Rows(),
	}
}

// NewSnapshotFromLines builds a snapshot from raw rows and their bounds. It
// exists for tests; production code goes through NewSnapshot.
func NewSnapshotFromLines(rows [][]renderer.Cell, bounds []vt.LineBound, width, height int) Snapshot {
	history := vt.NewHistory(vt.HistoryConfig{
		MaxRows:   max(len(rows), 1),
		MaxBytes:  math.MaxUint64, // Input is already owned and bounded by the caller.
		ChunkRows: 256,
	})
	for i, row := range rows {
		bound := vt.LineBound{End: len(row)}
		if i < len(bounds) {
			bound = bounds[i]
		}
		if err := history.Append(row, bound); err != nil {
			panic("copy snapshot: configured history rejected supplied row")
		}
	}
	return Snapshot{history: history.SealAndView(), Width: width, Height: height}
}

// NewSnapshotFromRows builds a snapshot whose rows are all hard logical lines.
func NewSnapshotFromRows(rows [][]renderer.Cell, width, height int) Snapshot {
	return NewSnapshotFromLines(rows, nil, width, height)
}

func (s Snapshot) Len() int { return s.history.Len() + s.screen.Height }
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

func (s Snapshot) RowWidth(y int) int {
	if y < 0 {
		return 0
	}
	if y < s.history.Len() {
		return s.history.RowWidth(y)
	}
	y -= s.history.Len()
	if y >= s.screen.Height {
		return 0
	}
	return s.screen.Width
}

func (s Snapshot) Cell(x, y int) renderer.Cell {
	if y < 0 || x < 0 {
		return renderer.BlankCell()
	}
	if y < s.history.Len() {
		return s.history.Cell(x, y)
	}
	y -= s.history.Len()
	if y >= s.screen.Height || x >= s.screen.Width {
		return renderer.BlankCell()
	}
	return s.screen.Cell(x, y)
}

func (s Snapshot) CopyRow(y int, dst []renderer.Cell) int {
	if y < 0 {
		return 0
	}
	if y < s.history.Len() {
		return s.history.CopyRow(y, dst)
	}
	y -= s.history.Len()
	if y >= s.screen.Height {
		return 0
	}
	n := min(len(dst), s.screen.Width)
	for x := range n {
		dst[x] = s.screen.Cell(x, y)
	}
	return n
}

// Bound returns the logical extent of row i, dispatching between sealed history
// and the live screen exactly as Row does.
func (s Snapshot) Bound(i int) vt.LineBound {
	if i < 0 {
		return vt.LineBound{}
	}
	if i < s.history.Len() {
		return s.history.Bound(i)
	}
	i -= s.history.Len()
	if i >= s.screen.Height || i >= len(s.screenBounds) {
		return vt.LineBound{}
	}
	return s.screenBounds[i]
}

// RowIDs returns an owned copy of physical row identities in document order.
func (s Snapshot) RowIDs() []vt.RowID {
	ids := make([]vt.RowID, s.Len())
	for i := range ids {
		ids[i] = s.RowID(i)
	}
	return ids
}

// RowID returns the identity of document row i, or zero when it is unavailable.
func (s Snapshot) RowID(i int) vt.RowID {
	if i < 0 {
		return 0
	}
	if i < s.history.Len() {
		return s.history.RowID(i)
	}
	i -= s.history.Len()
	if i >= s.screen.Height || i >= len(s.screenRowIDs) {
		return 0
	}
	return s.screenRowIDs[i]
}

// FindRowID returns the document row containing id, or -1 when it is absent.
func (s Snapshot) FindRowID(id vt.RowID) int {
	if id == 0 {
		return -1
	}
	if row := s.history.FindRowID(id); row >= 0 {
		return row
	}
	offset := s.history.Len()
	for i, candidate := range s.screenRowIDs {
		if i >= s.screen.Height {
			break
		}
		if candidate == id {
			return offset + i
		}
	}
	return -1
}

func (s Snapshot) rangeRows(yield func(int, []renderer.Cell) bool) error {
	i := 0
	stopped := false
	err := s.history.Range(func(r []renderer.Cell) bool {
		if !yield(i, r) {
			stopped = true
			return false
		}
		i++
		return true
	})
	if err != nil {
		return err
	}
	if stopped {
		return nil
	}
	for y := range s.screen.Height {
		if !yield(i+y, s.screen.Row(y)) {
			return nil
		}
	}
	return nil
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
	if m == nil {
		return false
	}
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
	pos, ok := m.document.Normalize(Pos{Row: match.Row, Col: match.Start})
	if !ok {
		return
	}
	m.navigator.Set(m.document, pos)
	if m.selection.Enabled {
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
	return findMatches(doc, query, doc.Snapshot().rangeRows)
}

func findMatches(doc *Document, query string, rangeRows func(func(int, []renderer.Cell) bool) error) []SearchMatch {
	needle := lowerRunes(query)
	matches := []SearchMatch{}
	err := rangeRows(func(row int, cells []renderer.Cell) bool {
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
	if err != nil {
		// Discard partial results without taking down the daemon on corrupt history.
		return nil
	}
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
	frame := renderer.NewFrame(m.document.Width(), m.document.Height()+1)
	m.RenderRows(func(y int, row []renderer.Cell) { frame.WriteRow(y, 0, row) }, styles...)
	return frame
}

// RenderRows paints viewport rows followed by the status row at Document.Height().
// The callback borrows a reusable semantic row only until it returns. Rendering
// directly into a compositor avoids building and then decoding a compact frame.
func (m *Mode) RenderRows(paint func(int, []renderer.Cell), styles ...renderer.Style) {
	if m == nil || m.document == nil {
		return
	}
	d := m.document
	selectionBounds, hasSelectionBounds := m.selection.bounds(d)
	selection, hasSelection := optionalStyle(styles, 1)
	cursor, cursorValid := d.Normalize(m.navigator.Pos)
	row := make([]renderer.Cell, d.Width())
	for y := range d.Height() {
		src := m.ViewportTop + y
		for x := range row {
			row[x] = renderer.BlankCell()
		}
		if src >= d.Len() {
			paint(y, row)
			continue
		}
		d.snapshot.CopyRow(src, row)
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
			cursorCol := min(cursor.Col, len(row)-1)
			applySelectionStyle(&row[cursorCol].Style, selection, hasSelection)
		}
		paint(y, row)
	}
	status := inverseStyle()
	if len(styles) > 0 {
		status = styles[0]
	}
	drawCopyStatus(row, m, d.Len(), status)
	paint(d.Height(), row)
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
		row[i] = renderer.Cell{Rune: ' ', Style: style}
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

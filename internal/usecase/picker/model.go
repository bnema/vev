package picker

import (
	"fmt"
	"strings"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/ui"
)

const (
	MinPreviewWidth           = 24
	MinHorizontalPreviewWidth = 48
	MinListWidth              = 16
	MinHorizontalListWidth    = 20
	MaxListWidth              = 44
	MinPaneHeight             = 4
	MinStackHeight            = 12
)

// SortMode orders the rows of one source locally. The source publishes its
// lines with rank and ephemeral markers; the presenting client decides the
// order, so two attachments of the same daemon never share a sort mode.
type SortMode uint8

const (
	// SortRecent orders every run by the source's recency rank.
	SortRecent SortMode = iota
	// SortGrouped puts named sessions before ephemeral ones inside each run,
	// each keeping recency order.
	SortGrouped
)

// Config describes one presented model: which intent produced the lines, the
// source's cursor hint, and the local sort order.
type Config struct {
	Intent protocol.PickerIntent
	Cursor protocol.PickerCursor
	Sort   SortMode
}

// RenderStyles are the styles Render uses to draw list rows. The zero value
// (all renderer.Style{}) is never used directly; Render falls back to
// defaultRenderStyles when no RenderStyles is supplied, mirroring the styling
// of a non-truecolor client.
type RenderStyles struct {
	Selection      renderer.Style // selected row fill + suffixes
	SelectionName  renderer.Style // selected row name segment
	SelectionMuted renderer.Style // selected row detail segment
	Name           renderer.Style // non-selected name segment
	Detail         renderer.Style // non-selected detail segment
	// Background fills otherwise unused interior cells. Base owns ordinary
	// rows, preserving a distinct inactive surface from modal chrome.
	Background     renderer.Style
	Base           renderer.Style // non-selected row fill + suffixes
	Stopped        renderer.Style // full row style for non-selected stopped rows (fill, name, detail, suffix)
	Separator      renderer.Style // preview separator
	Status         renderer.Style // one-row picker-local contextual help
	SearchMatch    renderer.Style // matched runes on ordinary rows
	SelectionMatch renderer.Style // matched runes on the selected row
}

func defaultRenderStyles() RenderStyles {
	selection := renderer.DefaultStyle()
	selection.Inverse = true
	base := renderer.DefaultStyle()
	separator := renderer.DefaultStyle()
	separator.Attrs = renderer.AttrDim
	stopped := renderer.DefaultStyle()
	stopped.Attrs = renderer.AttrDim
	stopped.Italic = true
	searchMatch := base
	searchMatch.Bold = true
	selectionMatch := selection
	selectionMatch.Bold = true
	return RenderStyles{Selection: selection, SelectionName: selection, SelectionMuted: selection, Name: base, Detail: base, Background: base, Base: base, Separator: separator, Stopped: stopped, Status: separator, SearchMatch: searchMatch, SelectionMatch: selectionMatch}
}

// Preview is a bounded view of the selected pane's visible frame.
type Preview = ui.FrameView

type LayoutMode uint8

const (
	LayoutListOnly LayoutMode = iota
	LayoutStacked
	LayoutHorizontal
)

type Layout struct {
	Mode      LayoutMode
	List      domain.Rect
	Separator domain.Rect
	Preview   domain.Rect
}

// Geometry is the single picker layout contract consumed by rendering and
// preview sizing. Status always reserves the final available inner row.
type Geometry struct {
	Content   domain.Rect
	Status    domain.Rect
	Mode      LayoutMode
	List      domain.Rect
	Separator domain.Rect
	Preview   domain.Rect
}

// Model is the pure presentation state of one picker: the published lines,
// the local cursor, the search editor, and the local sort order. It owns no
// target resolution and no mutation authority: the source that published a
// line resolves its opaque key when the client commits it.
type Model struct {
	intent        protocol.PickerIntent
	sort          SortMode
	lines         []protocol.PickerLine
	rows          []row
	selected      int
	searchActive  bool
	query         ui.TextInput
	searchMatches map[int]searchMatch
	matchRows     []int
}

// row is one rendered line plus its case-folded search fields.
type row struct {
	line         protocol.PickerLine
	foldedLabel  string
	foldedDetail string
}

func (r row) key() string { return r.line.Key }

func (r row) kind() protocol.PickerLineKind { return r.line.Kind }

func (r row) section() bool { return r.line.Kind == protocol.PickerLineSection }

func (r row) selectable() bool { return r.line.Actions != 0 }

// focusable reports the source's authorisation to rest the cursor on this row.
// A section header is never a destination; every other shape follows the
// published flag, so a row the source kept for inspection stays reachable while
// a row it skipped is passed over.
func (r row) focusable() bool {
	return r.line.Kind != protocol.PickerLineSection && r.line.Focusable
}

func (r row) rendersAsHeader() bool {
	return r.line.Kind == protocol.PickerLineSession || r.line.Kind == protocol.PickerLineHost
}

// New builds a model from one source's published lines. Order is applied
// locally: recency or grouped, inside each section run.
func New(lines []protocol.PickerLine, config Config) *Model {
	m := &Model{intent: config.Intent, sort: config.Sort}
	m.setLines(lines, config.Cursor)
	return m
}

// Intent reports which picker produced this model.
func (m *Model) Intent() protocol.PickerIntent {
	if m == nil {
		return 0
	}
	return m.intent
}

// SortMode reports the local ordering mode.
func (m *Model) SortMode() SortMode {
	if m == nil {
		return SortRecent
	}
	return m.sort
}

// SetSort re-orders the rows locally, preserving the selected key.
func (m *Model) SetSort(mode SortMode) {
	if m == nil {
		return
	}
	m.sort = mode
	key, hadKey := m.cursorKey()
	m.rebuild(key, hadKey, m.selected)
}

// setLines replaces the published lines and restores the cursor by key,
// falling back to the source hint and then to the nearest focusable row.
func (m *Model) setLines(lines []protocol.PickerLine, cursor protocol.PickerCursor) {
	if m == nil {
		return
	}
	m.lines = append(m.lines[:0], lines...)
	m.rebuild(cursor.Key, cursor.Key != "", cursor.Index)
}

// ReplaceLines applies a fresh publication of the same source while retaining
// the local search editor and the selected key when it still exists.
func (m *Model) ReplaceLines(lines []protocol.PickerLine, cursor protocol.PickerCursor) {
	if m == nil {
		return
	}
	key, hadKey := m.cursorKey()
	m.lines = append(m.lines[:0], lines...)
	if !hadKey {
		key, hadKey = cursor.Key, cursor.Key != ""
	}
	m.rebuild(key, hadKey, cursor.Index)
}

// rebuild recomputes the row list from the published lines. Search matches are
// row-index keyed, so they are recomputed here, before the cursor is restored:
// restoration consults them to keep the cursor on a row the query shows.
func (m *Model) rebuild(key string, hadKey bool, fallbackIndex int) {
	m.rows = m.rows[:0]
	run := make([]protocol.PickerLine, 0, len(m.lines))
	flush := func() {
		if len(run) == 0 {
			return
		}
		for _, line := range m.sortRun(run) {
			m.rows = append(m.rows, row{line: line, foldedLabel: strings.ToLower(line.Label), foldedDetail: strings.ToLower(line.Detail)})
		}
		run = run[:0]
	}
	for _, line := range m.lines {
		if line.Kind == protocol.PickerLineSection {
			flush()
			m.rows = append(m.rows, row{line: line, foldedLabel: strings.ToLower(line.Label)})
			continue
		}
		run = append(run, line)
	}
	flush()
	m.searchMatches = nil
	m.matchRows = nil
	if m.searchActive {
		m.refreshSearch(false)
	}
	m.restoreSelection(key, hadKey, fallbackIndex)
}

// sortRun orders one section run locally. Recency keeps the source's
// published order; grouped moves ephemeral sessions behind named ones without
// disturbing the relative order inside each group.
func (m *Model) sortRun(lines []protocol.PickerLine) []protocol.PickerLine {
	if len(lines) < 2 || m.sort != SortGrouped {
		return lines
	}
	named := make([]protocol.PickerLine, 0, len(lines))
	ephemeral := make([]protocol.PickerLine, 0, len(lines))
	for _, line := range lines {
		if line.Ephemeral {
			ephemeral = append(ephemeral, line)
			continue
		}
		named = append(named, line)
	}
	return append(named, ephemeral...)
}

func (m *Model) restoreSelection(key string, hadKey bool, fallbackIndex int) {
	if hadKey {
		for idx, candidate := range m.rows {
			if m.eligible(idx) && candidate.key() == key {
				m.selected = idx
				return
			}
		}
	}
	m.selected = -1
	m.SelectNearestRow(fallbackIndex)
	if m.selected < 0 {
		m.selected = m.firstEligible()
	}
}

// searchRestricted reports whether the active query narrows eligibility. An
// empty query never hides a row.
func (m *Model) searchRestricted() bool {
	return m != nil && m.searchActive && m.query.Value() != ""
}

// eligible reports whether the cursor may rest on row i: the source authorised
// the row as a destination and the typed query, when there is one, matches it.
func (m *Model) eligible(i int) bool {
	if m == nil || i < 0 || i >= len(m.rows) || !m.rows[i].focusable() {
		return false
	}
	return !m.searchRestricted() || m.rowMatches(i)
}

// committable reports whether row i may be committed: it admits an action and
// the typed query, when there is one, does not exclude it.
func (m *Model) committable(i int) bool {
	if m == nil || i < 0 || i >= len(m.rows) || !m.rows[i].selectable() {
		return false
	}
	return !m.searchRestricted() || m.rowMatches(i)
}

func (m *Model) cursorKey() (string, bool) {
	if m == nil || m.selected < 0 || m.selected >= len(m.rows) {
		return "", false
	}
	return m.rows[m.selected].key(), true
}

func (m *Model) Up() {
	if m != nil && m.searchActive {
		m.moveSearch(-1)
		return
	}
	m.move(-1)
}

func (m *Model) Down() {
	if m != nil && m.searchActive {
		m.moveSearch(1)
		return
	}
	m.move(1)
}

// Cursor reports the line under the picker cursor independently from whether
// that row may be activated, so a refresh can preserve an anchor row.
func (m *Model) Cursor() (protocol.PickerLine, bool) {
	if m == nil || m.selected < 0 || m.selected >= len(m.rows) {
		return protocol.PickerLine{}, false
	}
	return m.rows[m.selected].line, true
}

// Selected reports the line under the cursor when it admits an action and the
// active query does not exclude it.
func (m *Model) Selected() (protocol.PickerLine, bool) {
	if !m.committable(m.selected) {
		return protocol.PickerLine{}, false
	}
	return m.rows[m.selected].line, true
}

// SelectedIndex reports the raw selected row index. It is -1 when the model has
// no rows or when nothing in them qualifies as a cursor destination (see
// Selected); otherwise it is a real row index even when that row is not
// selectable.
func (m *Model) SelectedIndex() int {
	if m == nil {
		return -1
	}
	return m.selected
}

// SelectNearestRow selects the nearest eligible row at or after idx, falling
// back to the last eligible row before it.
func (m *Model) SelectNearestRow(idx int) {
	if m == nil || len(m.rows) == 0 {
		return
	}
	idx = clamp(idx, 0, len(m.rows)-1)
	for i := idx; i < len(m.rows); i++ {
		if m.eligible(i) {
			m.selected = i
			return
		}
	}
	for i := idx - 1; i >= 0; i-- {
		if m.eligible(i) {
			m.selected = i
			return
		}
	}
}

func (m *Model) Clone() *Model {
	if m == nil {
		return nil
	}
	clone := *m
	clone.lines = append([]protocol.PickerLine(nil), m.lines...)
	clone.rows = append([]row(nil), m.rows...)
	clone.query.SetValue(m.query.Value())
	// Search results are copy-on-write: every query mutation publishes a new
	// map and row slice, so render snapshots can safely share the old values.
	clone.matchRows = m.matchRows
	clone.searchMatches = m.searchMatches
	return &clone
}

func (m *Model) Render(inner domain.Size, preview Preview, styles ...RenderStyles) renderer.Frame {
	frame := renderer.NewFrame(max(inner.Cols, 0), max(inner.Rows, 0))
	geometry := ChooseGeometry(inner)
	styleSet := defaultRenderStyles()
	if len(styles) > 0 {
		styleSet = styles[0]
	}
	ui.FillRect(frame, domain.Rect{Width: frame.Width, Height: frame.Height}, renderer.Cell{Rune: ' ', Style: styleSet.Background})
	m.renderList(frame, geometry.List, styleSet)
	switch geometry.Mode {
	case LayoutHorizontal:
		ui.DrawSeparator(frame, geometry.Separator, ui.SeparatorVertical, styleSet.Separator)
	case LayoutStacked:
		ui.DrawSeparator(frame, geometry.Separator, ui.SeparatorHorizontal, styleSet.Separator)
	}
	ui.BlitFrame(frame, geometry.Preview, preview, ui.VerticalAnchorBottom)
	m.renderStatus(frame, geometry.Status, styleSet.Status)
	return frame
}

func (m *Model) move(delta int) {
	if m == nil || len(m.rows) == 0 {
		return
	}
	if !m.eligible(m.selected) {
		m.selected = m.firstEligible()
		return
	}
	for i := m.selected + delta; i >= 0 && i < len(m.rows); i += delta {
		if m.eligible(i) {
			m.selected = i
			return
		}
	}
}

func (m *Model) firstEligible() int {
	for i := range m.rows {
		if m.eligible(i) {
			return i
		}
	}
	return -1
}

func (s SortMode) Title() string {
	if s == SortGrouped {
		return " Sessions · grouped "
	}
	return " Sessions · recent "
}

func lineStatusBadge(status protocol.PickerLineStatus) string {
	switch status {
	case protocol.PickerLineStatusUp:
		return "[up]"
	case protocol.PickerLineStatusStopped:
		return "[stopped]"
	case protocol.PickerLineStatusDown:
		return "[down]"
	case protocol.PickerLineStatusStale:
		return "[stale]"
	case protocol.PickerLineStatusVersion:
		return "[version]"
	case protocol.PickerLineStatusError:
		return "[error]"
	default:
		return ""
	}
}

// renderList draws each visible row as up to three segments: a name segment
// (bold when styles came from a truecolor theme), a base-styled attention
// marker right after the name, and a muted detail segment — or a base-styled
// status badge for header rows. A tight width ellipsizes the detail segment
// before eating into the name.
func (m *Model) renderList(frame renderer.Frame, rect domain.Rect, styles RenderStyles) {
	if m == nil || rect.Width <= 0 || rect.Height <= 0 {
		return
	}
	visible := min(rect.Height, frame.Height-rect.Y)
	offset := m.scrollOffset(visible)
	clipX := rect.X + rect.Width
	for y := range visible {
		idx := offset + y
		if idx >= len(m.rows) {
			break
		}
		r := m.rows[idx]
		base, nameStyle, detailStyle := styles.Base, styles.Name, styles.Detail
		if r.section() {
			nameStyle = styles.Detail
		}
		if idx == m.selected {
			base, nameStyle, detailStyle = styles.Selection, styles.SelectionName, styles.SelectionMuted
		}
		if r.line.Stopped && idx != m.selected {
			base, nameStyle, detailStyle = styles.Stopped, styles.Stopped, styles.Stopped
		}
		if r.line.Dim || m.searchActive && m.query.Value() != "" && !m.rowMatches(idx) {
			base.Attrs |= renderer.AttrDim
			nameStyle.Attrs |= renderer.AttrDim
			detailStyle.Attrs |= renderer.AttrDim
		}
		ui.FillRect(frame, domain.Rect{X: rect.X, Y: rect.Y + y, Width: rect.Width, Height: 1}, renderer.Cell{Rune: ' ', Style: base})

		name := r.line.Label
		if r.kind() == protocol.PickerLineTab {
			name = "  " + name
		}
		badge := ""
		if r.rendersAsHeader() {
			badge = lineStatusBadge(r.line.Status)
		}
		contentClipX := clipX
		nameWidth := rect.Width
		badgeWidth := textCellWidth(badge)
		if badge != "" {
			nameWidth = max(rect.Width-badgeWidth-1, 0)
			contentClipX = max(rect.X, clipX-badgeWidth-1)
		}
		originalName := name
		name = ui.TruncateText(name, nameWidth)
		nameMatchStyle := styles.SearchMatch
		if idx == m.selected {
			nameMatchStyle = styles.SelectionMatch
		}
		namePositions := m.matchPositions(idx, matchName)
		if r.kind() == protocol.PickerLineTab {
			namePositions = shiftPositions(namePositions, 2)
		}
		namePositions = visibleMatchPositions(namePositions, name, name != originalName)
		x := drawMatchedText(frame, rect.X, rect.Y+y, contentClipX, name, nameStyle, nameMatchStyle, namePositions)

		if r.section() {
			continue
		}
		if r.rendersAsHeader() {
			detail := ui.TruncateText(r.line.Detail, contentClipX-x)
			detailPositions := visibleMatchPositions(m.matchPositions(idx, matchDetail), detail, detail != r.line.Detail)
			drawMatchedText(frame, x, rect.Y+y, contentClipX, detail, detailStyle, nameMatchStyle, detailPositions)
			if badge != "" {
				badgeX := max(rect.X, clipX-badgeWidth)
				ui.DrawText(frame, badgeX, rect.Y+y, clipX, badge, base)
			}
			continue
		}

		if r.line.Attention {
			x = ui.DrawText(frame, x, rect.Y+y, clipX, " "+string(ui.AttentionGlyph), base)
		}

		detail := ui.TruncateText(r.line.Detail, clipX-x)
		detailPositions := visibleMatchPositions(m.matchPositions(idx, matchDetail), detail, detail != r.line.Detail)
		drawMatchedText(frame, x, rect.Y+y, clipX, detail, detailStyle, nameMatchStyle, detailPositions)
	}
}

func (m *Model) matchPositions(idx int, field matchField) []int {
	if m == nil || !m.searchActive || m.query.Value() == "" {
		return nil
	}
	return m.searchMatches[idx].positions(field)
}

func visibleMatchPositions(positions []int, rendered string, truncated bool) []int {
	if !truncated || len(positions) == 0 {
		return positions
	}
	limit := max(len([]rune(rendered))-1, 0) // the final rune is the ellipsis
	visible := make([]int, 0, len(positions))
	for _, position := range positions {
		if position < limit {
			visible = append(visible, position)
		}
	}
	return visible
}

func shiftPositions(positions []int, delta int) []int {
	if len(positions) == 0 || delta == 0 {
		return positions
	}
	shifted := make([]int, len(positions))
	for i, position := range positions {
		shifted[i] = position + delta
	}
	return shifted
}

func drawMatchedText(frame renderer.Frame, x, y, clipX int, text string, base, match renderer.Style, positions []int) int {
	if len(positions) == 0 {
		return ui.DrawText(frame, x, y, clipX, text, base)
	}
	positionIndex := 0
	runeIndex := 0
	for _, r := range text {
		style := base
		if positionIndex < len(positions) && positions[positionIndex] == runeIndex {
			style = match
			positionIndex++
		}
		x = ui.DrawText(frame, x, y, clipX, string(r), style)
		if x >= clipX {
			break
		}
		runeIndex++
	}
	return x
}

func (m *Model) renderStatus(frame renderer.Frame, rect domain.Rect, style renderer.Style) {
	if rect.Width <= 0 || rect.Height <= 0 || rect.Y < 0 || rect.Y >= frame.Height {
		return
	}
	ui.FillRect(frame, rect, renderer.Cell{Rune: ' ', Style: style})
	// The footer describes the effective selection: a row kept for inspection,
	// a row the query hides, and an empty picker never promise a commit.
	action := ""
	deletable := false
	selected := protocol.PickerLine{}
	hasCursorRow := m != nil && m.selected >= 0 && m.selected < len(m.rows)
	if hasCursorRow {
		selected = m.rows[m.selected].line
	}
	if m != nil && m.committable(m.selected) {
		action = actionVerb(selected, m.intent)
		deletable = selected.Actions&protocol.PickerCanKill != 0
	}
	enter := "Enter " + action
	if action == "" {
		enter = "Enter unavailable"
	}
	var groups []string
	if m != nil && m.searchActive {
		groups = []string{fmt.Sprintf("%d matches", len(m.matchRows)), enter, "arrows next"}
		escape := "Esc exit"
		if m.query.Value() != "" {
			escape = "Esc clear"
		}
		groups = append(groups, escape)
	} else {
		if hasCursorRow && selected.Dim && selected.StatusDetail != "" {
			groups = append(groups, selected.StatusDetail)
		}
		if rect.Width < 60 {
			groups = append(groups, enter, "/", "Esc", "j/k")
			if deletable {
				groups = append(groups, "x")
			}
			groups = append(groups, "s")
		} else {
			groups = append(groups, "j/k move", enter)
			if deletable {
				groups = append(groups, "x delete")
			}
			groups = append(groups, "/ search", "s sort", "Esc close")
		}
	}
	text := strings.Join(groups, "  ")
	ui.DrawText(frame, rect.X, rect.Y, rect.X+rect.Width, ui.TruncateText(text, rect.Width), style)
}

// actionVerb names what Enter does on one row, so the footer never promises
// an action the source did not authorise.
func actionVerb(line protocol.PickerLine, intent protocol.PickerIntent) string {
	if line.Actions == 0 {
		return "unavailable"
	}
	if line.Actions&protocol.PickerCanMove != 0 {
		if intent == protocol.PickerIntentMoveTab {
			return "move"
		}
		return "move pane"
	}
	if line.Stopped {
		return "restart"
	}
	return "open"
}

func (m *Model) scrollOffset(visible int) int {
	if visible <= 0 || len(m.rows) <= visible || m.selected < 0 {
		return 0
	}
	if m.selected < visible {
		return 0
	}
	offset := m.selected - visible + 1
	return min(offset, len(m.rows)-visible)
}

func clamp(n, low, high int) int {
	if n < low {
		return low
	}
	if n > high {
		return high
	}
	return n
}

// ChooseGeometry reserves the final inner row for picker-local status before
// solving the list and preview layout.
func ChooseGeometry(inner domain.Size) Geometry {
	if inner.Cols <= 0 || inner.Rows <= 0 {
		return Geometry{}
	}
	contentRows := max(inner.Rows-1, 0)
	layout := ChooseLayout(domain.Size{Cols: inner.Cols, Rows: contentRows})
	return Geometry{
		Content: domain.Rect{Width: inner.Cols, Height: contentRows},
		Status:  domain.Rect{Y: contentRows, Width: inner.Cols, Height: 1},
		Mode:    layout.Mode, List: layout.List, Separator: layout.Separator, Preview: layout.Preview,
	}
}

func ChooseLayout(inner domain.Size) Layout {
	if inner.Cols <= 0 || inner.Rows <= 0 {
		return Layout{}
	}
	if inner.Rows >= MinPaneHeight {
		listWidth := clamp(inner.Cols*40/100, MinListWidth, MaxListWidth)
		listWidth = min(listWidth, inner.Cols-MinHorizontalPreviewWidth-1)
		previewWidth := inner.Cols - listWidth - 1
		if listWidth >= MinHorizontalListWidth && previewWidth >= MinHorizontalPreviewWidth {
			return Layout{
				Mode:      LayoutHorizontal,
				List:      domain.Rect{Width: listWidth, Height: inner.Rows},
				Separator: domain.Rect{X: listWidth, Width: 1, Height: inner.Rows},
				Preview:   domain.Rect{X: listWidth + 1, Width: previewWidth, Height: inner.Rows},
			}
		}
	}
	if inner.Rows >= MinStackHeight && inner.Cols >= MinPreviewWidth {
		listHeight := max(MinPaneHeight, inner.Rows*40/100)
		previewHeight := inner.Rows - listHeight - 1
		return Layout{
			Mode:      LayoutStacked,
			List:      domain.Rect{Width: inner.Cols, Height: listHeight},
			Separator: domain.Rect{Y: listHeight, Width: inner.Cols, Height: 1},
			Preview:   domain.Rect{Y: listHeight + 1, Width: inner.Cols, Height: previewHeight},
		}
	}
	return Layout{Mode: LayoutListOnly, List: domain.Rect{Width: inner.Cols, Height: inner.Rows}}
}

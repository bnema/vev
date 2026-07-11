package visualsearch

import (
	"strconv"

	"github.com/bnema/vev/internal/domain"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
)

type Model struct {
	snapshot scopy.Snapshot
	input    ui.TextInput
	matches  []scopy.SearchMatch
	selected int
	scroll   int
}

func New(snapshot scopy.Snapshot) *Model {
	m := &Model{snapshot: cloneSnapshot(snapshot), selected: -1}
	m.refresh()
	return m
}

func (m *Model) Clone() *Model {
	if m == nil {
		return nil
	}
	clone := *m
	clone.snapshot = cloneSnapshot(m.snapshot)
	clone.input.SetValue(m.input.Value())
	clone.matches = append([]scopy.SearchMatch(nil), m.matches...)
	return &clone
}

func (m *Model) Snapshot() scopy.Snapshot {
	if m == nil {
		return scopy.NewSnapshotFromRows(nil, 0, 0)
	}
	return cloneSnapshot(m.snapshot)
}

func (m *Model) Insert(r rune) {
	if m == nil {
		return
	}
	m.input.Insert(r)
	m.selected = 0
	m.scroll = 0
	m.refresh()
}

func (m *Model) Backspace() {
	if m == nil || m.input.Value() == "" {
		return
	}
	m.input.Backspace()
	m.selected = 0
	m.scroll = 0
	m.refresh()
}

func (m *Model) Query() string {
	if m == nil {
		return ""
	}
	return m.input.Value()
}

func (m *Model) Up() {
	if m != nil && m.selected > 0 {
		m.selected--
		m.clamp()
		m.ensureVisible(defaultVisibleRows)
	}
}

func (m *Model) Down() {
	if m != nil && m.selected+1 < len(m.matches) {
		m.selected++
		m.clamp()
		m.ensureVisible(defaultVisibleRows)
	}
}

func (m *Model) Selected() (scopy.SearchMatch, bool) {
	idx := m.SelectedIndex()
	if idx < 0 {
		return scopy.SearchMatch{}, false
	}
	return m.matches[idx], true
}

func (m *Model) SelectedIndex() int {
	if m == nil || m.selected < 0 || m.selected >= len(m.matches) {
		return -1
	}
	return m.selected
}

func (m *Model) Matches() []scopy.SearchMatch {
	if m == nil {
		return nil
	}
	return append([]scopy.SearchMatch(nil), m.matches...)
}

func (m *Model) refresh() {
	m.matches = scopy.FindMatches(m.snapshot, m.input.Value())
	m.clamp()
	m.ensureVisible(defaultVisibleRows)
}

func (m *Model) clamp() {
	if len(m.matches) == 0 {
		m.selected = -1
		m.scroll = 0
		return
	}
	if m.selected < 0 {
		m.selected = 0
	}
	if m.selected >= len(m.matches) {
		m.selected = len(m.matches) - 1
	}
	if m.scroll < 0 {
		m.scroll = 0
	}
	if m.scroll > m.selected {
		m.scroll = m.selected
	}
}

const defaultVisibleRows = 10

func (m *Model) ensureVisible(visible int) {
	if m == nil || visible <= 0 || len(m.matches) == 0 {
		return
	}
	if m.selected < m.scroll {
		m.scroll = m.selected
	}
	if m.selected >= m.scroll+visible {
		m.scroll = m.selected - visible + 1
	}
}

func cloneSnapshot(s scopy.Snapshot) scopy.Snapshot {
	rows := make([][]renderer.Cell, s.Len())
	for i := range rows {
		rows[i] = s.Row(i)
	}
	return scopy.NewSnapshotFromRows(rows, s.Width, s.Height)
}

func (m *Model) Render(inner domain.Size, selectedStyle ...renderer.Style) renderer.Frame {
	frame := renderer.NewFrame(max(inner.Cols, 0), max(inner.Rows, 0))
	if frame.Width == 0 || frame.Height == 0 {
		return frame
	}
	base := renderer.DefaultStyle()
	selection := base
	selection.Inverse = true
	if len(selectedStyle) > 0 {
		selection = selectedStyle[0]
	}
	ui.FillRect(frame, domain.Rect{Width: frame.Width, Height: frame.Height}, renderer.Cell{Rune: ' ', Style: base})
	ui.DrawInputLine(frame, 0, "/", m.Query(), base, selection)
	visible := frame.Height - 1
	if visible <= 0 || len(m.matches) == 0 {
		return frame
	}
	scroll := m.scroll
	if m.selected < scroll {
		scroll = m.selected
	}
	if m.selected >= scroll+visible {
		scroll = m.selected - visible + 1
	}
	for y := range visible {
		idx := scroll + y
		if idx >= len(m.matches) {
			break
		}
		match := m.matches[idx]
		style := base
		if idx == m.selected {
			style = selection
		}
		ui.FillRect(frame, domain.Rect{Y: y + 1, Width: frame.Width, Height: 1}, renderer.Cell{Rune: ' ', Style: style})
		label := strconv.Itoa(match.Row+1) + ":" + strconv.Itoa(match.Start+1) + "  " + match.Text
		ui.DrawText(frame, 0, y+1, frame.Width, label, style)
	}
	return frame
}

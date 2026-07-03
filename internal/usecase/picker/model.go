package picker

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
)

const (
	MinPreviewWidth = 24
	MinListWidth    = 16
	MaxListWidth    = 32
	MinPaneHeight   = 4
	MinStackHeight  = 12
)

type SessionView struct {
	ID     domain.SessionID
	Name   string
	Tabs   []string
	Active int
}

type Target struct {
	Session  domain.SessionID
	TabIndex int
}

type Preview struct {
	Rows   [][]renderer.Cell
	Width  int
	Height int
}

type Layout struct {
	List    domain.Rect
	Preview domain.Rect
}

type Model struct {
	rows     []row
	selected int
}

type row struct {
	header   bool
	label    string
	session  domain.SessionID
	tabIndex int
}

func New(sessions []SessionView, cur domain.SessionID, curTab int) *Model {
	m := &Model{selected: -1}
	activeSelection := -1
	for _, session := range sessions {
		m.rows = append(m.rows, row{header: true, label: session.Name, session: session.ID, tabIndex: -1})
		active := session.Active
		if active < 0 || active >= len(session.Tabs) {
			active = 0
		}
		for i, tab := range session.Tabs {
			idx := len(m.rows)
			m.rows = append(m.rows, row{label: tab, session: session.ID, tabIndex: i})
			if session.ID == cur && i == curTab {
				m.selected = idx
			}
			if activeSelection < 0 && i == active {
				activeSelection = idx
			}
		}
	}
	if m.selected < 0 {
		m.selected = activeSelection
	}
	if m.selected < 0 {
		m.selected = m.firstLeaf()
	}
	if m.selected < 0 && len(m.rows) > 0 {
		m.selected = 0
	}
	return m
}

func ChooseLayout(inner domain.Size) Layout {
	if inner.Cols <= 0 || inner.Rows <= 0 {
		return Layout{}
	}
	if inner.Rows >= MinPaneHeight && inner.Cols >= MinListWidth+1+MinPreviewWidth {
		listWidth := clamp(inner.Cols*30/100, MinListWidth, MaxListWidth)
		previewWidth := inner.Cols - listWidth - 1
		if previewWidth >= MinPreviewWidth {
			return Layout{
				List:    domain.Rect{Width: listWidth, Height: inner.Rows},
				Preview: domain.Rect{X: listWidth + 1, Width: previewWidth, Height: inner.Rows},
			}
		}
	}
	if inner.Rows >= MinStackHeight && inner.Cols >= MinPreviewWidth {
		listHeight := max(MinPaneHeight, inner.Rows*40/100)
		previewHeight := inner.Rows - listHeight - 1
		return Layout{
			List:    domain.Rect{Width: inner.Cols, Height: listHeight},
			Preview: domain.Rect{Y: listHeight + 1, Width: inner.Cols, Height: previewHeight},
		}
	}
	return Layout{List: domain.Rect{Width: inner.Cols, Height: inner.Rows}}
}

func (m *Model) Up() {
	m.move(-1)
}

func (m *Model) Down() {
	m.move(1)
}

func (m *Model) Selected() (Target, bool) {
	if m == nil || m.selected < 0 || m.selected >= len(m.rows) {
		return Target{}, false
	}
	r := m.rows[m.selected]
	if r.header {
		return Target{}, false
	}
	return Target{Session: r.session, TabIndex: r.tabIndex}, true
}

func (m *Model) Render(inner domain.Size, preview Preview) renderer.Frame {
	frame := renderer.NewFrame(max(inner.Cols, 0), max(inner.Rows, 0))
	layout := ChooseLayout(inner)
	m.renderList(frame, layout.List)
	blitPreview(frame, layout.Preview, preview)
	return frame
}

func (m *Model) move(delta int) {
	if m == nil || len(m.rows) == 0 {
		return
	}
	if m.selected < 0 || m.selected >= len(m.rows) || m.rows[m.selected].header {
		m.selected = m.firstLeaf()
		return
	}
	for i := m.selected + delta; i >= 0 && i < len(m.rows); i += delta {
		if !m.rows[i].header {
			m.selected = i
			return
		}
	}
}

func (m *Model) firstLeaf() int {
	for i, r := range m.rows {
		if !r.header {
			return i
		}
	}
	return -1
}

func (m *Model) renderList(frame renderer.Frame, rect domain.Rect) {
	if m == nil || rect.Width <= 0 || rect.Height <= 0 {
		return
	}
	visible := min(rect.Height, frame.Height-rect.Y)
	offset := m.scrollOffset(visible)
	for y := range visible {
		idx := offset + y
		if idx >= len(m.rows) {
			break
		}
		r := m.rows[idx]
		style := renderer.DefaultStyle()
		if idx == m.selected {
			style.Inverse = true
		}
		label := r.label
		if !r.header {
			label = "  " + label
		}
		ui.FillRect(frame, domain.Rect{X: rect.X, Y: rect.Y + y, Width: rect.Width, Height: 1}, renderer.Cell{Rune: ' ', Style: style})
		ui.DrawText(frame, rect.X, rect.Y+y, rect.X+rect.Width, label, style)
	}
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

func blitPreview(frame renderer.Frame, rect domain.Rect, preview Preview) {
	if rect.Width <= 0 || rect.Height <= 0 {
		return
	}
	w := min(rect.Width, frame.Width-rect.X)
	h := min(rect.Height, frame.Height-rect.Y)
	if w <= 0 || h <= 0 {
		return
	}
	for y := range h {
		if y >= preview.Height || y >= len(preview.Rows) {
			continue
		}
		src := preview.Rows[y]
		for x := range w {
			if x >= preview.Width || x >= len(src) {
				continue
			}
			cell := src[x]
			if cell.Continuation && x == 0 {
				continue
			}
			if !cell.Continuation && x == w-1 && x+1 < len(src) && src[x+1].Continuation {
				continue
			}
			frame.Set(rect.X+x, rect.Y+y, cell)
		}
	}
}

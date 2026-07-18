package picker

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
)

const (
	MinPreviewWidth           = 24
	MinHorizontalPreviewWidth = 48
	MinListWidth              = 16
	MaxListWidth              = 32
	MinPaneHeight             = 4
	MinStackHeight            = 12
)

type SessionView struct {
	ID                domain.SessionID
	Name              string
	TargetName        string
	Tabs              []TabEntry
	Active            int
	Stopped           bool
	ExpectedCreatedAt *int64
}

// TabEntry is one tab row; Name is drawn emphasized, Detail muted.
type TabEntry struct {
	Name      string // tab display name
	Detail    string // " (paneTitle)" or "", drawn muted
	Attention bool   // draw the attention marker right after Name, before Detail
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
	Background renderer.Style
	Base       renderer.Style // non-selected row fill + suffixes
	Separator  renderer.Style // preview separator
}

func defaultRenderStyles() RenderStyles {
	selection := renderer.DefaultStyle()
	selection.Inverse = true
	base := renderer.DefaultStyle()
	separator := renderer.DefaultStyle()
	separator.Attrs = renderer.AttrDim
	return RenderStyles{Selection: selection, SelectionName: selection, SelectionMuted: selection, Name: base, Detail: base, Background: base, Base: base, Separator: separator}
}

type Target struct {
	Session  domain.SessionID
	Name     string
	TabIndex int
	Stopped  bool
	// ExpectedCreatedAt optionally pins this target to a particular named
	// session lifecycle. Callers that obtain a snapshot outside the daemon use
	// it to reject a same-name replacement at commit time.
	ExpectedCreatedAt *int64
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

type Model struct {
	rows     []row
	selected int
}

type row struct {
	header    bool
	dispName  string // display name segment; bold on truecolor
	detail    string // " (paneTitle)" segment, muted; "" for headers
	attention bool   // draw the attention marker right after the name, before detail; tab rows only
	session   domain.SessionID
	// targetName is the named-session lookup name threaded into Target;
	// distinct from dispName, which is what gets drawn.
	targetName        string
	tabIndex          int
	stopped           bool
	expectedCreatedAt *int64
}

func New(sessions []SessionView, cur domain.SessionID, curTab int) *Model {
	m := &Model{selected: -1}
	activeSelection := -1
	for _, session := range sessions {
		targetName := session.TargetName
		if targetName == "" && (session.Stopped || session.ExpectedCreatedAt != nil) {
			targetName = session.Name
		}
		m.rows = append(m.rows, row{header: true, dispName: session.Name, session: session.ID, targetName: targetName, tabIndex: -1, stopped: session.Stopped, expectedCreatedAt: session.ExpectedCreatedAt})
		active := session.Active
		if active < 0 || active >= len(session.Tabs) {
			active = 0
		}
		for i, tab := range session.Tabs {
			idx := len(m.rows)
			m.rows = append(m.rows, row{dispName: tab.Name, detail: tab.Detail, attention: tab.Attention, session: session.ID, targetName: targetName, tabIndex: i, stopped: session.Stopped, expectedCreatedAt: session.ExpectedCreatedAt})
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
	if inner.Rows >= MinPaneHeight {
		listWidth := clamp(inner.Cols*30/100, MinListWidth, MaxListWidth)
		previewWidth := inner.Cols - listWidth - 1
		if previewWidth >= MinHorizontalPreviewWidth {
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
	return Target{Session: r.session, Name: r.targetName, TabIndex: r.tabIndex, Stopped: r.stopped, ExpectedCreatedAt: r.expectedCreatedAt}, true
}

func (m *Model) Clone() *Model {
	if m == nil {
		return nil
	}
	clone := *m
	clone.rows = append([]row(nil), m.rows...)
	return &clone
}

func (m *Model) Render(inner domain.Size, preview Preview, styles ...RenderStyles) renderer.Frame {
	frame := renderer.NewFrame(max(inner.Cols, 0), max(inner.Rows, 0))
	layout := ChooseLayout(inner)
	styleSet := defaultRenderStyles()
	if len(styles) > 0 {
		styleSet = styles[0]
	}
	ui.FillRect(frame, domain.Rect{Width: frame.Width, Height: frame.Height}, renderer.Cell{Rune: ' ', Style: styleSet.Background})
	m.renderList(frame, layout.List, styleSet)
	switch layout.Mode {
	case LayoutHorizontal:
		ui.DrawSeparator(frame, layout.Separator, ui.SeparatorVertical, styleSet.Separator)
	case LayoutStacked:
		ui.DrawSeparator(frame, layout.Separator, ui.SeparatorHorizontal, styleSet.Separator)
	}
	ui.BlitFrame(frame, layout.Preview, preview, ui.VerticalAnchorBottom)
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

// renderList draws each visible row as up to three segments: a name segment
// (bold when styles came from a truecolor theme), a base-styled attention
// marker right after the name, and a muted detail segment (tab rows only) —
// or a base-styled "(stopped)" suffix for stopped session headers. A tight
// width ellipsizes the detail segment before eating into the name.
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
		if idx == m.selected {
			base, nameStyle, detailStyle = styles.Selection, styles.SelectionName, styles.SelectionMuted
		}
		ui.FillRect(frame, domain.Rect{X: rect.X, Y: rect.Y + y, Width: rect.Width, Height: 1}, renderer.Cell{Rune: ' ', Style: base})

		name := r.dispName
		if !r.header {
			name = "  " + name
		}
		name = ui.TruncateText(name, rect.Width)
		x := ui.DrawText(frame, rect.X, rect.Y+y, clipX, name, nameStyle)

		if r.header {
			if r.stopped {
				ui.DrawText(frame, x, rect.Y+y, clipX, " (stopped)", base)
			}
			continue
		}

		if r.attention {
			x = ui.DrawText(frame, x, rect.Y+y, clipX, " "+string(ui.AttentionGlyph), base)
		}

		detail := ui.TruncateText(r.detail, clipX-x)
		ui.DrawText(frame, x, rect.Y+y, clipX, detail, detailStyle)
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

package picker

import (
	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/ui"
)

const (
	MinPreviewWidth           = 24
	MinHorizontalPreviewWidth = 48
	MinListWidth              = 16
	MaxListWidth              = 32
	MinPaneHeight             = 4
	MinStackHeight            = 12
)

type RemoteAvailability uint8

const (
	RemoteNone RemoteAvailability = iota
	RemoteCached
	RemoteFresh
	RemoteStale
	RemoteVersionMismatch
)

type SessionView struct {
	ID                domain.SessionID
	Incarnation       domain.IncarnationID
	Name              string
	TargetName        string
	Tabs              []TabEntry
	Active            int
	Stopped           bool
	ExpectedCreatedAt *int64
	RemoteKey         *domain.RemoteSessionKey
	// RemoteTarget is the structured route/lifecycle identity for picker rows.
	// It is never reconstructed from Name or a rendered label.
	RemoteTarget *domain.RemoteSessionTarget
	// RemoteHost marks remote host status rows that have no session key.
	RemoteHost         string
	RemoteAvailability RemoteAvailability
	RemoteDetail       string
	RemoteReason       string
	ConnectOnly        bool
	RemoteAttachReady  bool
	// CannotAcceptMoves reports whether this session cannot receive a moved tab
	// or pane. False for ordinary local (and stopped) sessions; true for
	// restricted remote rows.
	CannotAcceptMoves bool
}

// TabEntry is one tab row; Name is drawn emphasized, Detail muted.
type TabEntry struct {
	TabID     domain.TabStableID // stable tab identity; independent of its current index
	Name      string             // tab display name
	RawName   string             // unformatted name used by exact remote selectors
	Detail    string             // " (paneTitle)" or "", drawn muted
	Attention bool               // draw the attention marker right after Name, before Detail
}

type SelectionMode uint8

const (
	SelectNavigationTab SelectionMode = iota
	SelectMovePaneTab
	SelectMoveTabSession
)

type SourceFilter struct {
	Session     domain.SessionID
	Incarnation domain.IncarnationID
	TabID       domain.TabStableID
	RemoteKey   *domain.RemoteSessionKey
}

// SelectionConfig describes which rows are selectable, which stable target
// should remain selected, and which source must not be offered as a move
// destination.
type SelectionConfig struct {
	Mode    SelectionMode
	Current SourceFilter
	Source  SourceFilter
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
	Stopped    renderer.Style // full row style for non-selected stopped rows (fill, name, detail, suffix)
	Separator  renderer.Style // preview separator
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
	return RenderStyles{Selection: selection, SelectionName: selection, SelectionMuted: selection, Name: base, Detail: base, Background: base, Base: base, Separator: separator, Stopped: stopped}
}

type Target struct {
	Session           domain.SessionID
	Incarnation       domain.IncarnationID
	Name              string
	RemoteKey         *domain.RemoteSessionKey
	RemoteTarget      *domain.RemoteSessionTarget
	RemoteHost        string
	UnavailableReason string
	TabID             domain.TabStableID
	TabIndex          int
	Stopped           bool
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
	mode     SelectionMode
	rows     []row
	selected int
}

type rowKind uint8

const (
	rowSession rowKind = iota
	rowTab
)

func (k rowKind) rendersAsHeader() bool {
	return k == rowSession
}

func (k rowKind) selectable(mode SelectionMode) bool {
	switch mode {
	case SelectNavigationTab, SelectMovePaneTab:
		return k == rowTab
	case SelectMoveTabSession:
		return k == rowSession
	default:
		return false
	}
}

type row struct {
	kind        rowKind
	dispName    string // display name segment; bold on truecolor
	detail      string // " (paneTitle)" segment, muted; "" for session rows
	attention   bool   // draw the attention marker right after the name, before detail; tab rows only
	session     domain.SessionID
	incarnation domain.IncarnationID
	// targetName is the named-session lookup name threaded into Target;
	// distinct from dispName, which is what gets drawn.
	targetName           string
	tabID                domain.TabStableID
	tabIndex             int
	stopped              bool
	expectedCreatedAt    int64
	hasExpectedCreatedAt bool
	remoteKey            domain.RemoteSessionKey
	hasRemoteKey         bool
	remote               bool
	remoteTarget         domain.RemoteSessionTarget
	hasRemoteTarget      bool
	remoteHost           string
	unavailableReason    string
	selectable           bool
	focusable            bool
	dim                  bool
}

func New(sessions []SessionView, config SelectionConfig) *Model {
	m := &Model{mode: config.Mode, selected: -1}
	activeSelection := -1
	for _, session := range sessions {
		sessionRows := rowsForSession(session, config)
		for _, pickerRow := range sessionRows {
			idx := len(m.rows)
			m.rows = append(m.rows, pickerRow)
			if selectionMatches(pickerRow, config.Current, config.Mode) {
				m.selected = idx
			}
			if config.Mode == SelectNavigationTab && activeSelection < 0 && pickerRow.focusable && pickerRow.kind == rowTab && pickerRow.tabIndex == normalizedActive(session) {
				activeSelection = idx
			}
		}
	}
	if m.selected < 0 {
		m.selected = activeSelection
	}
	if m.selected < 0 {
		m.selected = m.firstFocusable()
	}
	if m.selected < 0 && len(m.rows) > 0 {
		m.selected = 0
	}
	return m
}

// rowsForSession is the sole owner of mode-specific destination eligibility.
// The daemon supplies canonical lifecycle/tab snapshots without prefiltering.
func rowsForSession(session SessionView, config SelectionConfig) []row {
	if config.Mode != SelectNavigationTab && session.Stopped {
		return nil
	}
	if config.Mode == SelectMoveTabSession && sourceMatchesSession(config.Source, session) {
		return nil
	}
	if config.Mode == SelectMoveTabSession && len(session.Tabs) == 0 && !session.CannotAcceptMoves {
		return nil
	}

	targetName := session.TargetName
	if config.Mode == SelectNavigationTab && !session.Stopped && session.ExpectedCreatedAt == nil {
		targetName = ""
	} else if targetName == "" {
		targetName = session.Name
	}
	expectedCreatedAt, hasExpectedCreatedAt := int64Value(session.ExpectedCreatedAt)
	common := row{
		session: session.ID, incarnation: session.Incarnation, targetName: targetName,
		stopped: session.Stopped, expectedCreatedAt: expectedCreatedAt,
		hasExpectedCreatedAt: hasExpectedCreatedAt,
	}
	if session.RemoteKey != nil {
		common.remoteKey, common.hasRemoteKey = *session.RemoteKey, true
	}
	if session.RemoteTarget != nil {
		common.remoteTarget, common.hasRemoteTarget = *session.RemoteTarget, true
	}
	common.remoteHost = session.RemoteHost
	common.remote = common.hasRemoteKey || common.hasRemoteTarget || session.RemoteHost != ""
	header := common
	header.kind, header.dispName, header.tabIndex = rowSession, session.Name, -1
	if common.hasRemoteKey {
		header.dispName = common.remoteKey.Name
		display := common.remoteKey.Display()
		header.detail = display[len(common.remoteKey.Name):]
	}
	header.selectable = header.kind.selectable(config.Mode)
	header.focusable = header.selectable
	if common.remote {
		if remoteDetail := remoteRowDetail(session); remoteDetail != "" {
			if header.detail != "" {
				header.detail += " (" + remoteDetail + ")"
			} else {
				header.detail = remoteDetail
			}
		}
		// Rich catalog rows use tab targets. Legacy count-only/connect-only
		// callers retain their header target until all peers carry the richer
		// identity contract.
		if common.hasRemoteTarget {
			header.selectable = false
			header.focusable = config.Mode == SelectNavigationTab && len(session.Tabs) == 0
		} else {
			header.selectable = common.hasRemoteKey && config.Mode == SelectNavigationTab && session.ConnectOnly && remoteSelectable(session)
			header.focusable = header.selectable
			if !common.hasRemoteKey && !common.hasRemoteTarget {
				header.focusable = config.Mode == SelectNavigationTab
			}
		}
		header.unavailableReason = session.RemoteReason
		header.dim = remoteDim(session)
	}
	if config.Mode != SelectNavigationTab && session.CannotAcceptMoves {
		header.selectable, header.focusable, header.dim = false, false, true
	}
	rows := []row{header}
	if !session.ConnectOnly || common.hasRemoteTarget {
		for i, tab := range session.Tabs {
			if config.Mode == SelectMovePaneTab && sourceMatchesTab(config.Source, session, tab) {
				continue
			}
			tabRow := common
			tabRow.kind, tabRow.dispName, tabRow.detail, tabRow.attention = rowTab, tab.Name, tab.Detail, tab.Attention
			tabRow.tabID, tabRow.tabIndex = tab.TabID, i
			tabRow.selectable = tabRow.kind.selectable(config.Mode)
			tabRow.focusable = tabRow.selectable
			if common.remote {
				tabRow.unavailableReason = session.RemoteReason
				if common.hasRemoteTarget {
					remoteTarget := common.remoteTarget
					if session.Stopped {
						remoteTarget.LiveTabID = ""
						if tab.TabID != "" {
							remoteTarget.StoppedTab = domain.NewStableTabSelector(tab.TabID)
						} else {
							remoteTarget.StoppedTab = domain.NewOrdinalTabSelector(uint16(i), tab.RawName, uint16(len(session.Tabs)))
						}
					} else {
						remoteTarget.StoppedTab = domain.TabSelector{}
						remoteTarget.LiveTabID = tab.TabID
					}
					// Keep the selected tab's structured identity even when it is
					// invalid. Focus remains possible for diagnostics, while both
					// the readiness gate and daemon wire validation reject it.
					tabRow.remoteTarget = remoteTarget
					tabRow.hasRemoteTarget = true
				}
				if !tabRow.hasRemoteTarget {
					legacyRemote := common.hasRemoteKey && config.Mode == SelectNavigationTab && remoteSelectable(session)
					tabRow.focusable = legacyRemote
					tabRow.selectable = legacyRemote
				} else {
					tabRow.focusable = true
					tabRow.selectable = tabRow.remoteTarget.Validate() == nil && config.Mode == SelectNavigationTab && remoteSelectable(session)
				}
				tabRow.dim = remoteDim(session)
			}
			if config.Mode != SelectNavigationTab && session.CannotAcceptMoves {
				tabRow.selectable, tabRow.focusable, tabRow.dim = false, false, true
			}
			rows = append(rows, tabRow)
		}
	}
	if config.Mode == SelectMovePaneTab && len(rows) == 1 && !session.CannotAcceptMoves {
		return nil
	}
	return rows
}

func remoteSelectable(session SessionView) bool {
	return session.RemoteAttachReady && session.RemoteAvailability != RemoteVersionMismatch && session.RemoteReason == ""
}

func remoteDim(session SessionView) bool {
	return !session.RemoteAttachReady || session.RemoteAvailability == RemoteStale || session.RemoteAvailability == RemoteVersionMismatch
}

func remoteRowDetail(session SessionView) string {
	if session.RemoteDetail != "" && (session.RemoteAvailability == RemoteStale || session.RemoteAvailability == RemoteVersionMismatch) {
		return session.RemoteDetail
	}
	if !session.RemoteAttachReady {
		return "proxy activation pending"
	}
	return session.RemoteDetail
}

func sourceMatchesSession(source SourceFilter, session SessionView) bool {
	if source.Session != session.ID {
		return false
	}
	return source.Incarnation == (domain.IncarnationID{}) || source.Incarnation == session.Incarnation
}

func sourceMatchesTab(source SourceFilter, session SessionView, tab TabEntry) bool {
	return sourceMatchesSession(source, session) && source.TabID == tab.TabID
}

func normalizedActive(session SessionView) int {
	if session.Active < 0 || session.Active >= len(session.Tabs) {
		return 0
	}
	return session.Active
}

func selectionMatches(pickerRow row, current SourceFilter, mode SelectionMode) bool {
	if pickerRow.session != current.Session || (current.Incarnation != (domain.IncarnationID{}) && pickerRow.incarnation != current.Incarnation) {
		return false
	}
	if pickerRow.hasRemoteKey && pickerRow.kind == rowSession {
		return current.RemoteKey == nil || pickerRow.remoteKey == *current.RemoteKey
	}
	if mode == SelectMoveTabSession {
		return pickerRow.kind == rowSession
	}
	if pickerRow.kind != rowTab {
		return false
	}
	// A stopped session contributes one synthetic tab row carrying no stable tab
	// identity, so it matches on session identity alone. Its session ID is
	// namespaced ("stopped:<name>"), and move modes drop stopped sessions
	// entirely, so no live row can be reached this way.
	if current.TabID == "" {
		return pickerRow.stopped
	}
	return pickerRow.tabID == current.TabID
}

func int64Value(value *int64) (int64, bool) {
	if value == nil {
		return 0, false
	}
	return *value, true
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

// Cursor reports the identity under the picker cursor independently from
// whether that row may be activated. Remote refreshes use it to preserve a
// structured remote session key while Selected continues to enforce safety.
func (m *Model) Cursor() (Target, bool) {
	if m == nil || m.selected < 0 || m.selected >= len(m.rows) {
		return Target{}, false
	}
	return m.rows[m.selected].target(), true
}

func (m *Model) Selected() (Target, bool) {
	if m == nil || m.selected < 0 || m.selected >= len(m.rows) {
		return Target{}, false
	}
	r := m.rows[m.selected]
	if !r.selectable {
		return Target{}, false
	}
	return r.target(), true
}

// SelectedIndex reports the raw selected row index. It is -1 only when the
// model is nil or has no rows at all; otherwise it is a real row index even
// when that row is not selectable (see Selected). Row indices are only
// meaningful against this exact model.
func (m *Model) SelectedIndex() int {
	if m == nil {
		return -1
	}
	return m.selected
}

// SelectNearestRow selects the first selectable row at or after idx, falling
// back to the last selectable row before it. Callers use it to keep the
// cursor on the row that takes a removed item's place.
func (m *Model) SelectNearestRow(idx int) {
	if m == nil || len(m.rows) == 0 {
		return
	}
	idx = clamp(idx, 0, len(m.rows)-1)
	for i := idx; i < len(m.rows); i++ {
		if m.rows[i].focusable {
			m.selected = i
			return
		}
	}
	for i := idx - 1; i >= 0; i-- {
		if m.rows[i].focusable {
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
	if m.selected < 0 || m.selected >= len(m.rows) || !m.rows[m.selected].focusable {
		m.selected = m.firstFocusable()
		return
	}
	for i := m.selected + delta; i >= 0 && i < len(m.rows); i += delta {
		if m.rows[i].focusable {
			m.selected = i
			return
		}
	}
}

func (m *Model) firstFocusable() int {
	for i, r := range m.rows {
		if r.focusable {
			return i
		}
	}
	return -1
}

func (r row) target() Target {
	var remoteTarget *domain.RemoteSessionTarget
	if r.hasRemoteTarget {
		copyTarget := r.remoteTarget
		remoteTarget = &copyTarget
	}
	return Target{
		Session: r.session, Incarnation: r.incarnation, Name: r.targetName, RemoteKey: r.remoteKeyPointer(), RemoteTarget: remoteTarget,
		RemoteHost: r.remoteHost, UnavailableReason: r.unavailableReason, TabID: r.tabID, TabIndex: r.tabIndex, Stopped: r.stopped, ExpectedCreatedAt: r.expectedCreatedAtPointer(),
	}
}

func (r row) expectedCreatedAtPointer() *int64 {
	if !r.hasExpectedCreatedAt {
		return nil
	}
	value := r.expectedCreatedAt
	return &value
}

func (r row) remoteKeyPointer() *domain.RemoteSessionKey {
	if !r.hasRemoteKey {
		return nil
	}
	key := r.remoteKey
	return &key
}

// renderList draws each visible row as up to three segments: a name segment
// (bold when styles came from a truecolor theme), a base-styled attention
// marker right after the name, and a muted detail segment (tab rows only) —
// or a base-styled "(down)" suffix for down session headers. A tight width
// ellipsizes the detail segment before eating into the name.
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
		if r.stopped && idx != m.selected {
			base, nameStyle, detailStyle = styles.Stopped, styles.Stopped, styles.Stopped
		}
		if r.dim {
			base.Attrs |= renderer.AttrDim
			nameStyle.Attrs |= renderer.AttrDim
			detailStyle.Attrs |= renderer.AttrDim
		}
		ui.FillRect(frame, domain.Rect{X: rect.X, Y: rect.Y + y, Width: rect.Width, Height: 1}, renderer.Cell{Rune: ' ', Style: base})

		name := r.dispName
		if !r.kind.rendersAsHeader() {
			name = "  " + name
		}
		suffix := ""
		if r.kind.rendersAsHeader() && r.stopped {
			suffix = "(down)"
		}
		name = ui.TruncateText(name, max(rect.Width-len(suffix), 0))
		x := ui.DrawText(frame, rect.X, rect.Y+y, clipX, name, nameStyle)

		if r.kind.rendersAsHeader() {
			if r.stopped {
				detail := r.detail
				if detail == "" {
					detail = " "
				} else {
					detail += " "
				}
				detail = ui.TruncateText(detail, max(clipX-x-len(suffix), 0))
				x = ui.DrawText(frame, x, rect.Y+y, clipX, detail, base)
				ui.DrawText(frame, x, rect.Y+y, clipX, suffix, base)
			} else {
				ui.DrawText(frame, x, rect.Y+y, clipX, ui.TruncateText(r.detail, clipX-x), detailStyle)
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

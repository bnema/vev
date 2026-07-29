package picker

import (
	"reflect"
	"testing"

	"github.com/bnema/vev/internal/domain"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

var (
	_ domain.TabStableID = TabEntry{}.TabID
	_ domain.TabStableID = Target{}.TabID
	_ domain.TabStableID = SourceFilter{}.TabID
)

func TestStableIdentityTypesAreDistinct(t *testing.T) {
	require.NotEqual(t, reflect.TypeFor[domain.TabStableID](), reflect.TypeFor[domain.PaneStableID]())
	require.NotEqual(t, reflect.TypeFor[domain.TabStableID](), reflect.TypeFor[domain.TabID]())
}

func TestNewFlattensAndSelectsCurrentTab(t *testing.T) {
	m := New([]SessionView{
		{ID: "s1", Name: "one", Tabs: []TabEntry{{TabID: "shell", Name: "shell"}, {TabID: "logs", Name: "logs"}}, Active: 0},
		{ID: "s2", Name: "two", Tabs: []TabEntry{{TabID: "api", Name: "api"}}, Active: 0},
	}, SelectionConfig{Mode: SelectNavigationTab, Current: SourceFilter{Session: "s1", TabID: "logs"}})

	got, ok := m.Selected()
	require.True(t, ok)
	require.Equal(t, Target{Session: "s1", TabID: "logs", TabIndex: 1}, got)
	frame := m.Render(domain.Size{Cols: 20, Rows: 5}, Preview{})
	require.Equal(t, 'o', frame.At(0, 0).Rune)
	require.Equal(t, ' ', frame.At(0, 2).Rune)
	require.Equal(t, 'l', frame.At(2, 2).Rune)
	require.True(t, frame.At(0, 2).Style.Inverse)
}

func TestNewFallsBackToActiveThenFirstLeaf(t *testing.T) {
	m := New([]SessionView{{ID: "s1", Name: "one", Tabs: []TabEntry{{Name: "shell"}, {Name: "logs"}}, Active: 1}}, SelectionConfig{Mode: SelectNavigationTab, Current: SourceFilter{Session: "missing"}})
	got, ok := m.Selected()
	require.True(t, ok)
	require.Equal(t, Target{Session: "s1", TabIndex: 1}, got)

	m = New([]SessionView{{ID: "s1", Name: "one", Tabs: []TabEntry{{Name: "shell"}}, Active: 4}}, SelectionConfig{Mode: SelectNavigationTab, Current: SourceFilter{Session: "missing"}})
	got, ok = m.Selected()
	require.True(t, ok)
	require.Equal(t, Target{Session: "s1", TabIndex: 0}, got)
}

func TestUpDownSkipsHeadersClampsAndCrossesSessions(t *testing.T) {
	m := New([]SessionView{
		{ID: "s1", Name: "one", Tabs: []TabEntry{{Name: "a"}, {Name: "b"}}, Active: 0},
		{ID: "s2", Name: "two", Tabs: []TabEntry{{Name: "c"}}, Active: 0},
	}, SelectionConfig{Mode: SelectNavigationTab})

	m.Up()
	got, ok := m.Selected()
	require.True(t, ok)
	require.Equal(t, Target{Session: "s1", TabIndex: 0}, got, "up at first tab clamps")
	m.Down()
	got, ok = m.Selected()
	require.True(t, ok)
	require.Equal(t, Target{Session: "s1", TabIndex: 1}, got)
	m.Down()
	got, ok = m.Selected()
	require.True(t, ok)
	require.Equal(t, Target{Session: "s2", TabIndex: 0}, got, "down skips second session header")
	m.Down()
	got, ok = m.Selected()
	require.True(t, ok)
	require.Equal(t, Target{Session: "s2", TabIndex: 0}, got, "down at last tab clamps")
	m.Up()
	got, ok = m.Selected()
	require.True(t, ok)
	require.Equal(t, Target{Session: "s1", TabIndex: 1}, got, "up skips second session header")
}

func TestModelConstructsTargetsForSelectionModes(t *testing.T) {
	incarnationOne := domain.IncarnationID{1}
	incarnationTwo := domain.IncarnationID{2}
	sessions := []SessionView{
		{ID: "s1", Name: "one", TargetName: "one", Incarnation: incarnationOne, Tabs: []TabEntry{{TabID: "t1", Name: "shell"}, {TabID: "t2", Name: "logs"}}, Active: 1},
		{ID: "s2", Name: "two", TargetName: "two", Incarnation: incarnationTwo, Tabs: []TabEntry{{TabID: "t3", Name: "api"}}, Active: 0},
	}
	tests := []struct {
		name   string
		config SelectionConfig
		want   Target
	}{
		{
			name:   "navigation selects tab row",
			config: SelectionConfig{Mode: SelectNavigationTab, Current: SourceFilter{Session: "s1", TabID: "t2"}},
			want:   Target{Session: "s1", Incarnation: incarnationOne, TabID: "t2", TabIndex: 1},
		},
		{
			name:   "move pane selects eligible tab row",
			config: SelectionConfig{Mode: SelectMovePaneTab, Current: SourceFilter{Session: "s1", TabID: "t1"}, Source: SourceFilter{Session: "s1", TabID: "t2"}},
			want:   Target{Session: "s1", Incarnation: incarnationOne, Name: "one", TabID: "t1", TabIndex: 0},
		},
		{
			name:   "move tab selects destination session header",
			config: SelectionConfig{Mode: SelectMoveTabSession, Current: SourceFilter{Session: "s2"}, Source: SourceFilter{Session: "s1"}},
			want:   Target{Session: "s2", Incarnation: incarnationTwo, Name: "two", TabIndex: -1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			model := New(sessions, tt.config)

			got, ok := model.Selected()
			require.True(t, ok)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestModelMoveModesApplyExactDestinationFiltering(t *testing.T) {
	incarnation := domain.IncarnationID{1}
	sessions := []SessionView{
		{ID: "source", Incarnation: incarnation, Name: "source", Tabs: []TabEntry{{TabID: "source-tab", Name: "source"}, {TabID: "other-tab", Name: "other"}}},
		{ID: "only-source", Incarnation: incarnation, Name: "only-source", Tabs: []TabEntry{{TabID: "only-tab", Name: "only"}}},
		{ID: "destination", Incarnation: incarnation, Name: "destination", Tabs: []TabEntry{{TabID: "destination-tab", Name: "destination"}}},
		{ID: "empty", Incarnation: incarnation, Name: "empty"},
		{ID: "stopped", Incarnation: incarnation, Name: "stopped", Stopped: true, Tabs: []TabEntry{{TabID: "stopped-tab", Name: "stopped"}}},
	}
	type rowIdentity struct {
		kind    rowKind
		session domain.SessionID
		tab     domain.TabStableID
	}
	identities := func(model *Model) []rowIdentity {
		got := make([]rowIdentity, 0, len(model.rows))
		for _, pickerRow := range model.rows {
			got = append(got, rowIdentity{kind: pickerRow.kind, session: pickerRow.session, tab: pickerRow.tabID})
		}
		return got
	}

	paneModel := New(sessions, SelectionConfig{Mode: SelectMovePaneTab, Source: SourceFilter{
		Session: "only-source", Incarnation: incarnation, TabID: "only-tab",
	}})
	require.Equal(t, []rowIdentity{
		{kind: rowSession, session: "source"},
		{kind: rowTab, session: "source", tab: "source-tab"},
		{kind: rowTab, session: "source", tab: "other-tab"},
		{kind: rowSession, session: "destination"},
		{kind: rowTab, session: "destination", tab: "destination-tab"},
	}, identities(paneModel))

	tabModel := New(sessions, SelectionConfig{Mode: SelectMoveTabSession, Source: SourceFilter{
		Session: "source", Incarnation: incarnation,
	}})
	require.Equal(t, []rowIdentity{
		{kind: rowSession, session: "only-source"},
		{kind: rowTab, session: "only-source", tab: "only-tab"},
		{kind: rowSession, session: "destination"},
		{kind: rowTab, session: "destination", tab: "destination-tab"},
	}, identities(tabModel))

	replacementIncarnation := domain.IncarnationID{2}
	replacementModel := New([]SessionView{
		{ID: "same", Incarnation: incarnation, Tabs: []TabEntry{{TabID: "same-tab"}}},
		{ID: "same", Incarnation: replacementIncarnation, Tabs: []TabEntry{{TabID: "same-tab"}}},
	}, SelectionConfig{Mode: SelectMovePaneTab, Source: SourceFilter{
		Session: "same", Incarnation: incarnation, TabID: "same-tab",
	}})
	require.Len(t, replacementModel.rows, 2, "only the exact source lifecycle is filtered")
	require.Equal(t, replacementIncarnation, replacementModel.rows[0].incarnation)
	require.Equal(t, replacementIncarnation, replacementModel.rows[1].incarnation)
}

func TestRowKindDefinesRenderingAndSelectability(t *testing.T) {
	tests := []struct {
		name       string
		kind       rowKind
		mode       SelectionMode
		header     bool
		selectable bool
	}{
		{name: "navigation session", kind: rowSession, mode: SelectNavigationTab, header: true},
		{name: "navigation tab", kind: rowTab, mode: SelectNavigationTab, selectable: true},
		{name: "move pane session", kind: rowSession, mode: SelectMovePaneTab, header: true},
		{name: "move pane tab", kind: rowTab, mode: SelectMovePaneTab, selectable: true},
		{name: "move tab session", kind: rowSession, mode: SelectMoveTabSession, header: true, selectable: true},
		{name: "move tab tab", kind: rowTab, mode: SelectMoveTabSession},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.header, tt.kind.rendersAsHeader())
			require.Equal(t, tt.selectable, tt.kind.selectable(tt.mode))
		})
	}
}

func TestModelMovePaneFallsBackToFirstEligibleTab(t *testing.T) {
	model := New([]SessionView{
		{ID: "source", Name: "source", Active: 0, Tabs: []TabEntry{{TabID: "source-tab", Name: "source"}, {TabID: "sibling", Name: "sibling"}}},
		{ID: "destination", Name: "destination", Active: 0, Tabs: []TabEntry{{TabID: "destination-tab", Name: "destination"}}},
	}, SelectionConfig{Mode: SelectMovePaneTab, Source: SourceFilter{Session: "source", TabID: "source-tab"}})

	got, ok := model.Selected()
	require.True(t, ok)
	require.Equal(t, domain.TabStableID("sibling"), got.TabID)
}

func TestModelMoveNavigationSkipsRowsNotSelectableForMode(t *testing.T) {
	sessions := []SessionView{
		{ID: "s1", Name: "one", Tabs: []TabEntry{{TabID: "t1", Name: "one"}}},
		{ID: "s2", Name: "two", Tabs: []TabEntry{{TabID: "t2", Name: "two"}}},
	}

	paneModel := New(sessions, SelectionConfig{Mode: SelectMovePaneTab, Current: SourceFilter{Session: "s1", TabID: "t1"}})
	paneModel.Down()
	got, ok := paneModel.Selected()
	require.True(t, ok)
	require.Equal(t, domain.TabStableID("t2"), got.TabID, "move-pane navigation skips the second session header")

	tabModel := New(sessions, SelectionConfig{Mode: SelectMoveTabSession, Current: SourceFilter{Session: "s1"}})
	tabModel.Down()
	got, ok = tabModel.Selected()
	require.True(t, ok)
	require.Equal(t, domain.SessionID("s2"), got.Session, "move-tab navigation skips tab rows")
}

func TestModelSelectedKeepsImmutableLifecycleAndStableTabAcrossRefresh(t *testing.T) {
	createdAt := int64(41)
	views := []SessionView{{
		ID: "same-id", Name: "same-name", TargetName: "same-name", Incarnation: domain.IncarnationID{1}, ExpectedCreatedAt: &createdAt,
		Tabs: []TabEntry{{TabID: "first", Name: "first"}, {TabID: "stable", Name: "stable"}}, Active: 1,
	}}
	model := New(views, SelectionConfig{Mode: SelectMovePaneTab, Current: SourceFilter{Session: "same-id", TabID: "stable"}})

	views[0].Incarnation = domain.IncarnationID{9}
	views[0].TargetName = "replacement"
	views[0].Tabs[1].TabID = "replacement-tab"
	createdAt = 99
	got, ok := model.Selected()
	require.True(t, ok)
	require.Equal(t, Target{
		Session: "same-id", Incarnation: domain.IncarnationID{1}, Name: "same-name", TabID: "stable", TabIndex: 1,
		ExpectedCreatedAt: int64Pointer(41),
	}, got, "model target remains bound to the lifecycle captured during construction")

	refreshed := New([]SessionView{{
		ID: "same-id", Name: "same-name", TargetName: "same-name", Incarnation: domain.IncarnationID{1},
		Tabs: []TabEntry{{TabID: "stable", Name: "stable"}, {TabID: "first", Name: "first"}}, Active: 1,
	}}, SelectionConfig{Mode: SelectMovePaneTab, Current: SourceFilter{Session: got.Session, TabID: got.TabID}})
	got, ok = refreshed.Selected()
	require.True(t, ok)
	require.Equal(t, domain.TabStableID("stable"), got.TabID)
	require.Equal(t, 0, got.TabIndex, "mutable index follows the stable tab after refresh")
}

func TestModelClonePreservesModeAndImmutableTarget(t *testing.T) {
	createdAt := int64(7)
	model := New([]SessionView{{
		ID: "s", Name: "session", TargetName: "session", Incarnation: domain.IncarnationID{3}, ExpectedCreatedAt: &createdAt,
		Tabs: []TabEntry{{TabID: "tab", Name: "tab"}},
	}}, SelectionConfig{Mode: SelectMovePaneTab})
	clone := model.Clone()

	require.Equal(t, SelectMovePaneTab, clone.mode)
	got, ok := clone.Selected()
	require.True(t, ok)
	*got.ExpectedCreatedAt = 100
	gotAgain, ok := clone.Selected()
	require.True(t, ok)
	require.Equal(t, int64(7), *gotAgain.ExpectedCreatedAt, "returned targets cannot mutate the cloned model locator")
	require.Equal(t, domain.TabStableID("tab"), gotAgain.TabID)
	require.Equal(t, domain.IncarnationID{3}, gotAgain.Incarnation)
}

func int64Pointer(value int64) *int64 {
	return new(value)
}

func TestChooseLayoutResponsiveBoundaries(t *testing.T) {
	tests := []struct {
		name string
		size domain.Size
		want Layout
	}{
		{
			name: "horizontal rows minus one is list only", size: domain.Size{Cols: 69, Rows: 3},
			want: Layout{Mode: LayoutListOnly, List: domain.Rect{Width: 69, Height: 3}},
		},
		{
			name: "horizontal preview minus one is list only", size: domain.Size{Cols: 68, Rows: 4},
			want: Layout{Mode: LayoutListOnly, List: domain.Rect{Width: 68, Height: 4}},
		},
		{
			name: "horizontal exact minimum preview", size: domain.Size{Cols: 69, Rows: 4},
			want: Layout{Mode: LayoutHorizontal, List: domain.Rect{Width: 20, Height: 4}, Separator: domain.Rect{X: 20, Width: 1, Height: 4}, Preview: domain.Rect{X: 21, Width: 48, Height: 4}},
		},
		{
			name: "horizontal preview plus one", size: domain.Size{Cols: 70, Rows: 4},
			want: Layout{Mode: LayoutHorizontal, List: domain.Rect{Width: 21, Height: 4}, Separator: domain.Rect{X: 21, Width: 1, Height: 4}, Preview: domain.Rect{X: 22, Width: 48, Height: 4}},
		},
		{
			name: "stacked columns minus one is list only", size: domain.Size{Cols: 23, Rows: 12},
			want: Layout{Mode: LayoutListOnly, List: domain.Rect{Width: 23, Height: 12}},
		},
		{
			name: "stacked rows minus one is list only", size: domain.Size{Cols: 24, Rows: 11},
			want: Layout{Mode: LayoutListOnly, List: domain.Rect{Width: 24, Height: 11}},
		},
		{
			name: "stacked exact minimum", size: domain.Size{Cols: 24, Rows: 12},
			want: Layout{Mode: LayoutStacked, List: domain.Rect{Width: 24, Height: 4}, Separator: domain.Rect{Y: 4, Width: 24, Height: 1}, Preview: domain.Rect{Y: 5, Width: 24, Height: 7}},
		},
		{
			name: "stacked list height plus one", size: domain.Size{Cols: 24, Rows: 13},
			want: Layout{Mode: LayoutStacked, List: domain.Rect{Width: 24, Height: 5}, Separator: domain.Rect{Y: 5, Width: 24, Height: 1}, Preview: domain.Rect{Y: 6, Width: 24, Height: 7}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, ChooseLayout(tt.size))
		})
	}
}

func TestRenderDrawsCustomOrientedSeparators(t *testing.T) {
	m := New([]SessionView{{ID: "s", Name: "session", Tabs: []TabEntry{{Name: "tab"}}, Active: 0}}, SelectionConfig{Mode: SelectNavigationTab})
	separator := renderer.Style{Foreground: 8, Attrs: renderer.AttrDim}
	styles := RenderStyles{Separator: separator}
	tests := []struct {
		name string
		size domain.Size
		rect domain.Rect
		rune rune
	}{
		{"horizontal layout", domain.Size{Cols: 69, Rows: 4}, domain.Rect{X: 20, Width: 1, Height: 4}, '│'},
		{"stacked layout", domain.Size{Cols: 24, Rows: 12}, domain.Rect{Y: 4, Width: 24, Height: 1}, '─'},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := m.Render(tt.size, Preview{}, styles)
			for y := tt.rect.Y; y < tt.rect.Y+tt.rect.Height; y++ {
				for x := tt.rect.X; x < tt.rect.X+tt.rect.Width; x++ {
					got := frame.At(x, y)
					require.Equal(t, tt.rune, got.Rune)
					require.True(t, got.Style.Equal(separator))
				}
			}
		})
	}
}

func TestRenderListOnlyOmitsSeparatorAndPreview(t *testing.T) {
	m := New([]SessionView{{ID: "s", Name: "session", Tabs: []TabEntry{{Name: "tab"}}, Active: 0}}, SelectionConfig{Mode: SelectNavigationTab})
	frame := m.Render(domain.Size{Cols: 23, Rows: 11}, Preview{Width: 1, Height: 1, Rows: [][]renderer.Cell{{cell('x')}}}, RenderStyles{Separator: renderer.Style{Foreground: 8}})

	require.Equal(t, ' ', frame.At(22, 10).Rune)
	require.NotEqual(t, 'x', frame.At(22, 10).Rune)
}

func TestRenderPreviewAnchorsOversizedSourceToFinalRows(t *testing.T) {
	m := New(nil, SelectionConfig{Mode: SelectNavigationTab})
	preview := Preview{Width: 24, Height: 9, Rows: previewRows(24, "abcdefghi")}

	frame := m.Render(domain.Size{Cols: 24, Rows: 12}, preview)
	require.Equal(t, 'c', frame.At(0, 5).Rune)
	require.Equal(t, 'i', frame.At(0, 11).Rune)
}

func TestRenderPreviewBottomPlacesShortSource(t *testing.T) {
	m := New(nil, SelectionConfig{Mode: SelectNavigationTab})
	preview := Preview{Width: 24, Height: 1, Rows: previewRows(24, "z")}

	frame := m.Render(domain.Size{Cols: 24, Rows: 12}, preview)
	require.Equal(t, ' ', frame.At(0, 5).Rune)
	require.Equal(t, 'z', frame.At(0, 11).Rune)
}

func TestRenderAttentionMarkerSmokeWithResponsiveLayout(t *testing.T) {
	m := New([]SessionView{{ID: "s", Name: "session", Tabs: []TabEntry{{Name: "tab", Attention: true}}, Active: 0}}, SelectionConfig{Mode: SelectNavigationTab})
	frame := m.Render(domain.Size{Cols: 69, Rows: 4}, Preview{})

	require.Equal(t, rune(ui.AttentionGlyph), frame.At(6, 1).Rune)
}

func previewRows(width int, labels string) [][]renderer.Cell {
	rows := make([][]renderer.Cell, len(labels))
	for y, label := range labels {
		rows[y] = make([]renderer.Cell, width)
		for x := range rows[y] {
			rows[y][x] = cell(' ')
		}
		rows[y][0] = cell(label)
	}
	return rows
}

func TestSelectedMapping(t *testing.T) {
	m := New([]SessionView{
		{ID: "alpha", Name: "alpha", Tabs: []TabEntry{{TabID: "one", Name: "one"}}, Active: 0},
		{ID: "beta", Name: "beta", Tabs: []TabEntry{{TabID: "two", Name: "two"}, {TabID: "three", Name: "three"}}, Active: 0},
	}, SelectionConfig{Mode: SelectNavigationTab, Current: SourceFilter{Session: "beta", TabID: "three"}})

	got, ok := m.Selected()
	require.True(t, ok)
	require.Equal(t, Target{Session: "beta", TabID: "three", TabIndex: 1}, got)
}

func TestStoppedSessionSelectableAndRendered(t *testing.T) {
	m := New([]SessionView{{ID: "stopped:work", Name: "work", Tabs: []TabEntry{{Name: ""}}, Stopped: true}}, SelectionConfig{Mode: SelectNavigationTab})
	got, ok := m.Selected()
	require.True(t, ok)
	require.Equal(t, Target{Session: "stopped:work", Name: "work", TabIndex: 0, Stopped: true}, got)
	frame := m.Render(domain.Size{Cols: 24, Rows: 4}, Preview{})
	require.Equal(t, 'w', frame.At(0, 0).Rune)
	require.Equal(t, '(', frame.At(5, 0).Rune)
}

func TestRenderStopsStoppedRowsDimItalic(t *testing.T) {
	live := SessionView{ID: "live", Name: "work", Tabs: []TabEntry{{TabID: "t1", Name: "tab"}}}
	halted := SessionView{ID: "stopped:old", Name: "old", TargetName: "old", Stopped: true, Tabs: []TabEntry{{}}}
	m := New([]SessionView{live, halted}, SelectionConfig{Mode: SelectNavigationTab})

	stoppedStyle := renderer.Style{Foreground: -1, Background: -1, Italic: true, Attrs: renderer.AttrDim}
	styles := defaultRenderStyles()
	require.Equal(t, stoppedStyle, styles.Stopped)

	frame := m.Render(domain.Size{Cols: 15, Rows: 6}, Preview{})
	// Rows: 0 "work" header, 1 "  tab" (selected), 2 "old (stopped)" header, 3 its tab row.
	require.Equal(t, stoppedStyle, frame.Row(2)[0].Style, "stopped header must be dim italic")
	require.Equal(t, stoppedStyle, frame.Row(3)[0].Style, "stopped tab row must be dim italic")
	require.NotEqual(t, stoppedStyle, frame.Row(0)[0].Style, "live header keeps base style")

	selected := New([]SessionView{live, halted}, SelectionConfig{Mode: SelectNavigationTab, Current: SourceFilter{Session: halted.ID}})
	selectedFrame := selected.Render(domain.Size{Cols: 15, Rows: 6}, Preview{})
	require.NotEqual(t, stoppedStyle, selectedFrame.Row(3)[0].Style, "selected stopped row keeps selection style, not Stopped")
	require.True(t, selectedFrame.Row(3)[0].Style.Inverse, "selected stopped row still shows selection")
}

func TestRenderPreviewClipsPadsDropsWideRuneAndInvertsSelection(t *testing.T) {
	m := New([]SessionView{{ID: "s1", Name: "one", Tabs: []TabEntry{{Name: "tab"}}, Active: 0}}, SelectionConfig{Mode: SelectNavigationTab})
	preview := Preview{
		Width:  25,
		Height: 1,
		Rows: [][]renderer.Cell{{
			cell('a'), cell('b'), cell('c'), cell('d'), cell('e'), cell('f'), cell('g'), cell('h'), cell('i'), cell('j'),
			cell('k'), cell('l'), cell('m'), cell('n'), cell('o'), cell('p'), cell('q'), cell('r'), cell('s'), cell('t'),
			cell('u'), cell('v'), cell('w'), {Rune: '界', Style: renderer.DefaultStyle()}, {Continuation: true, Style: renderer.DefaultStyle()},
		}},
	}

	frame := m.Render(domain.Size{Cols: 24, Rows: 12}, preview)
	require.True(t, frame.At(0, 1).Style.Inverse)
	require.Equal(t, 'a', frame.At(0, 11).Rune)
	require.Equal(t, 'w', frame.At(22, 11).Rune)
	require.Equal(t, ' ', frame.At(23, 11).Rune, "wide rune crossing preview pane is dropped")
	require.Equal(t, ' ', frame.At(0, 5).Rune, "short preview is bottom anchored")
}

func TestRenderListScrollsSelectionIntoView(t *testing.T) {
	m := New([]SessionView{{ID: "s1", Name: "one", Tabs: []TabEntry{{Name: "a"}, {Name: "b"}, {Name: "c"}, {Name: "d"}, {Name: "e"}, {Name: "f"}}, Active: 0}}, SelectionConfig{Mode: SelectNavigationTab})
	for range 5 {
		m.Down()
	}

	frame := m.Render(domain.Size{Cols: 23, Rows: 4}, Preview{})
	require.Equal(t, ' ', frame.At(0, 3).Rune)
	require.Equal(t, 'f', frame.At(2, 3).Rune)
	require.True(t, frame.At(0, 3).Style.Inverse)
}

func TestRenderListTruncatesLabelWithEllipsis(t *testing.T) {
	m := New([]SessionView{{ID: "s1", Name: "one", Tabs: []TabEntry{{Name: "a-really-long-focused-pane-tab-label"}}, Active: 0}}, SelectionConfig{Mode: SelectNavigationTab})

	frame := m.Render(domain.Size{Cols: 69, Rows: 5}, Preview{})

	layout := ChooseLayout(domain.Size{Cols: 69, Rows: 5})
	require.Equal(t, 20, layout.List.Width, "test assumes a narrow list column")
	require.Equal(t, '…', frame.At(layout.List.Width-1, 1).Rune, "truncated label should end with an ellipsis at the list edge")
	require.Equal(t, '│', frame.At(layout.List.Width, 1).Rune, "the separator occupies the cell after the list")
}

func TestRenderListOnlyDoesNotDrawPreview(t *testing.T) {
	m := New([]SessionView{{ID: "s1", Name: "one", Tabs: []TabEntry{{Name: "tab"}}, Active: 0}}, SelectionConfig{Mode: SelectNavigationTab})
	preview := Preview{Width: 1, Height: 1, Rows: [][]renderer.Cell{{cell('x')}}}

	frame := m.Render(domain.Size{Cols: 23, Rows: 11}, preview)
	require.Equal(t, 'o', frame.At(0, 0).Rune)
	require.Equal(t, ' ', frame.At(0, 1).Rune)
	require.NotEqual(t, 'x', frame.At(0, 0).Rune)
}

func TestRenderListDrawsNameAndDetailSegmentsWithDistinctStyles(t *testing.T) {
	nameStyle := renderer.Style{Bold: true}
	detailStyle := renderer.Style{Italic: true}
	baseStyle := renderer.DefaultStyle()
	selectionStyle := renderer.Style{Inverse: true}
	selectionNameStyle := renderer.Style{Inverse: true, Bold: true}
	selectionMutedStyle := renderer.Style{Inverse: true, Italic: true}
	styles := RenderStyles{
		Selection: selectionStyle, SelectionName: selectionNameStyle, SelectionMuted: selectionMutedStyle,
		Name: nameStyle, Detail: detailStyle, Base: baseStyle,
	}

	m := New([]SessionView{{ID: "s1", Name: "one", Tabs: []TabEntry{
		{Name: "alpha", Detail: " (running)"},
		{Name: "beta", Detail: " (idle)", Attention: true},
	}, Active: 0}}, SelectionConfig{Mode: SelectNavigationTab})

	frame := m.Render(domain.Size{Cols: 30, Rows: 5}, Preview{}, styles)

	// Row 1 is the selected tab ("alpha"): name uses SelectionName, detail uses SelectionMuted.
	require.Equal(t, 'a', frame.At(2, 1).Rune)
	require.True(t, frame.At(2, 1).Style.Equal(selectionNameStyle), "selected name segment style")
	require.Equal(t, ' ', frame.At(7, 1).Rune)
	require.True(t, frame.At(7, 1).Style.Equal(selectionMutedStyle), "selected detail segment style")
	require.Equal(t, '(', frame.At(8, 1).Rune)
	require.True(t, frame.At(8, 1).Style.Equal(selectionMutedStyle), "selected detail segment style")

	// Row 2 is the non-selected tab ("beta"): name uses Name, then the
	// attention marker (right after the name, before detail) uses the row's
	// base style (not muted), then detail uses Detail.
	require.Equal(t, 'b', frame.At(2, 2).Rune)
	require.True(t, frame.At(2, 2).Style.Equal(nameStyle), "name segment style")
	require.Equal(t, ' ', frame.At(6, 2).Rune, "attention marker leading space")
	require.True(t, frame.At(6, 2).Style.Equal(baseStyle), "attention marker uses the base style, not muted")
	require.Equal(t, rune(ui.AttentionGlyph), frame.At(7, 2).Rune)
	require.True(t, frame.At(7, 2).Style.Equal(baseStyle), "attention marker uses the base style, not muted")
	require.Equal(t, ' ', frame.At(8, 2).Rune)
	require.True(t, frame.At(8, 2).Style.Equal(detailStyle), "detail segment style")
	require.Equal(t, '(', frame.At(9, 2).Rune)
	require.True(t, frame.At(9, 2).Style.Equal(detailStyle), "detail segment style")
}

func TestRenderListTruncatesDetailBeforeName(t *testing.T) {
	m := New([]SessionView{{ID: "s1", Name: "one", Tabs: []TabEntry{
		{Name: "short-name", Detail: " (a very long detail text)"},
	}, Active: 0}}, SelectionConfig{Mode: SelectNavigationTab})

	frame := m.Render(domain.Size{Cols: 69, Rows: 5}, Preview{})
	layout := ChooseLayout(domain.Size{Cols: 69, Rows: 5})
	require.Equal(t, 20, layout.List.Width, "test assumes a narrow list column")

	want := "  short-name (a ver…"
	for i, r := range want {
		require.Equal(t, r, frame.At(i, 1).Rune, "cell %d", i)
	}
	require.Equal(t, '…', frame.At(layout.List.Width-1, 1).Rune, "detail is ellipsized to fit, the intact name is not touched")
	require.Equal(t, '│', frame.At(layout.List.Width, 1).Rune, "the separator occupies the cell after the list")
}

func TestRenderListTruncatesNameWhenAloneExceedsWidth(t *testing.T) {
	m := New([]SessionView{{ID: "s1", Name: "one", Tabs: []TabEntry{
		{Name: "a-really-long-focused-pane-tab-label", Detail: " (detail)"},
	}, Active: 0}}, SelectionConfig{Mode: SelectNavigationTab})

	frame := m.Render(domain.Size{Cols: 69, Rows: 5}, Preview{})
	layout := ChooseLayout(domain.Size{Cols: 69, Rows: 5})
	require.Equal(t, 20, layout.List.Width, "test assumes a narrow list column")

	require.Equal(t, '…', frame.At(layout.List.Width-1, 1).Rune, "the name segment itself is ellipsized once it alone exceeds the width")
	require.Equal(t, '│', frame.At(layout.List.Width, 1).Rune, "the separator occupies the cell after the list")
}

func cell(r rune) renderer.Cell {
	return renderer.Cell{Rune: r, Style: renderer.DefaultStyle()}
}

func TestRenderStylesFillBackgroundRowsAndSelection(t *testing.T) {
	m := New([]SessionView{{ID: "s", Name: "session", Tabs: []TabEntry{{Name: "tab"}}, Active: 0}}, SelectionConfig{Mode: SelectNavigationTab})
	background := renderer.Style{Foreground: 1, Background: 2}
	base := renderer.Style{Foreground: 3, Background: 4}
	selection := renderer.Style{Foreground: 5, Background: 6}
	frame := m.Render(domain.Size{Cols: 20, Rows: 5}, Preview{}, RenderStyles{
		Background: background, Base: base, Name: base, Detail: base,
		Selection: selection, SelectionName: selection, SelectionMuted: selection,
		Separator: base,
	})

	require.True(t, frame.At(19, 4).Style.Equal(background), "unused interior keeps modal base")
	require.True(t, frame.At(19, 0).Style.Equal(base), "ordinary row owns inactive surface")
	require.True(t, frame.At(19, 1).Style.Equal(selection), "selected row owns active surface")
}

func TestPickerRowsKeepTerminalBackgroundAcrossAccentFallbacks(t *testing.T) {
	palette := [16]renderer.RGB{}
	palette[2] = renderer.RGB{R: 10, G: 230, B: 120}
	palette[10] = palette[2]
	accentTheme := themeui.Theme{
		Foreground: renderer.RGB{R: 230, G: 230, B: 230}, Background: renderer.RGB{R: 8, G: 9, B: 10},
		HasFG: true, HasBG: true, Known: true, TrueColor: true, UsePalette: true,
		Palette: palette, PaletteKnown: 1<<2 | 1<<10,
	}
	indexedTheme := accentTheme
	indexedTheme.TrueColor = false
	paletteOffTheme := accentTheme
	paletteOffTheme.UsePalette = false
	neutralTheme := accentTheme
	neutralTheme.UsePalette = false
	neutralTheme.PaletteKnown = 0

	tests := []struct {
		name   string
		theme  themeui.Theme
		policy domain.ThemeAccent
	}{
		{name: "truecolor accent", theme: accentTheme, policy: domain.ThemeAccent{Mode: domain.ThemeAccentSlot, Slot: 2}},
		{name: "indexed only", theme: indexedTheme, policy: domain.ThemeAccent{Mode: domain.ThemeAccentSlot, Slot: 2}},
		{name: "palette off", theme: paletteOffTheme, policy: domain.ThemeAccent{Mode: domain.ThemeAccentAuto}},
		{name: "forced dark", theme: themeui.BuiltinDark, policy: domain.ThemeAccent{Mode: domain.ThemeAccentAuto}},
		{name: "forced light", theme: themeui.BuiltinLight, policy: domain.ThemeAccent{Mode: domain.ThemeAccentAuto}},
		{name: "neutral fallback", theme: neutralTheme, policy: domain.ThemeAccent{Mode: domain.ThemeAccentAuto}},
	}

	model := New([]SessionView{
		{ID: "selected", Name: "selected", Tabs: []TabEntry{{Name: "one"}}, Active: 0},
		{ID: "inactive", Name: "inactive", Tabs: []TabEntry{{Name: "two", Detail: " (detail)"}}, Active: 0},
	}, SelectionConfig{Mode: SelectNavigationTab})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			styles := themeui.Resolve(tt.theme, tt.policy).Styles
			require.True(t, styles.PickerBase.Equal(renderer.DefaultStyle()))
			require.False(t, styles.PickerDescription.HasBackgroundRGB)
			require.False(t, styles.PickerSeparator.HasBackgroundRGB)
			if tt.name == "indexed only" {
				require.Equal(t, 2, styles.PickerDescription.Foreground)
				require.Equal(t, 2, styles.PickerSeparator.Foreground)
			}

			frame := model.Render(domain.Size{Cols: 32, Rows: 4}, Preview{}, RenderStyles{
				Background: styles.PickerBase, Base: styles.PickerBase, Name: styles.PickerName,
				Detail: styles.PickerDescription, Selection: styles.PickerSelection,
				SelectionName: styles.PickerSelection, SelectionMuted: styles.PickerSelection,
				Separator: styles.PickerSeparator,
			})
			require.True(t, frame.At(5, 3).Style.Equal(styles.PickerDescription), "description text keeps a contrast-derived foreground without a background tint")
			require.True(t, frame.At(31, 3).Style.Equal(styles.PickerBase), "unused row cells retain the terminal background")
		})
	}
}

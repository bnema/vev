package picker

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

func TestNewFlattensAndSelectsCurrentTab(t *testing.T) {
	m := New([]SessionView{
		{ID: "s1", Name: "one", Tabs: []TabEntry{{Name: "shell"}, {Name: "logs"}}, Active: 0},
		{ID: "s2", Name: "two", Tabs: []TabEntry{{Name: "api"}}, Active: 0},
	}, "s1", 1)

	got, ok := m.Selected()
	require.True(t, ok)
	require.Equal(t, Target{Session: "s1", TabIndex: 1}, got)
	frame := m.Render(domain.Size{Cols: 20, Rows: 5}, Preview{})
	require.Equal(t, 'o', frame.At(0, 0).Rune)
	require.Equal(t, ' ', frame.At(0, 2).Rune)
	require.Equal(t, 'l', frame.At(2, 2).Rune)
	require.True(t, frame.At(0, 2).Style.Inverse)
}

func TestNewFallsBackToActiveThenFirstLeaf(t *testing.T) {
	m := New([]SessionView{{ID: "s1", Name: "one", Tabs: []TabEntry{{Name: "shell"}, {Name: "logs"}}, Active: 1}}, "missing", 0)
	got, ok := m.Selected()
	require.True(t, ok)
	require.Equal(t, Target{Session: "s1", TabIndex: 1}, got)

	m = New([]SessionView{{ID: "s1", Name: "one", Tabs: []TabEntry{{Name: "shell"}}, Active: 4}}, "missing", 0)
	got, ok = m.Selected()
	require.True(t, ok)
	require.Equal(t, Target{Session: "s1", TabIndex: 0}, got)
}

func TestUpDownSkipsHeadersClampsAndCrossesSessions(t *testing.T) {
	m := New([]SessionView{
		{ID: "s1", Name: "one", Tabs: []TabEntry{{Name: "a"}, {Name: "b"}}, Active: 0},
		{ID: "s2", Name: "two", Tabs: []TabEntry{{Name: "c"}}, Active: 0},
	}, "s1", 0)

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
	m := New([]SessionView{{ID: "s", Name: "session", Tabs: []TabEntry{{Name: "tab"}}, Active: 0}}, "s", 0)
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
	m := New([]SessionView{{ID: "s", Name: "session", Tabs: []TabEntry{{Name: "tab"}}, Active: 0}}, "s", 0)
	frame := m.Render(domain.Size{Cols: 23, Rows: 11}, Preview{Width: 1, Height: 1, Rows: [][]renderer.Cell{{cell('x')}}}, RenderStyles{Separator: renderer.Style{Foreground: 8}})

	require.Equal(t, ' ', frame.At(22, 10).Rune)
	require.NotEqual(t, 'x', frame.At(22, 10).Rune)
}

func TestRenderPreviewAnchorsOversizedSourceToFinalRows(t *testing.T) {
	m := New(nil, "", 0)
	preview := Preview{Width: 24, Height: 9, Rows: previewRows(24, "abcdefghi")}

	frame := m.Render(domain.Size{Cols: 24, Rows: 12}, preview)
	require.Equal(t, 'c', frame.At(0, 5).Rune)
	require.Equal(t, 'i', frame.At(0, 11).Rune)
}

func TestRenderPreviewBottomPlacesShortSource(t *testing.T) {
	m := New(nil, "", 0)
	preview := Preview{Width: 24, Height: 1, Rows: previewRows(24, "z")}

	frame := m.Render(domain.Size{Cols: 24, Rows: 12}, preview)
	require.Equal(t, ' ', frame.At(0, 5).Rune)
	require.Equal(t, 'z', frame.At(0, 11).Rune)
}

func TestRenderAttentionMarkerSmokeWithResponsiveLayout(t *testing.T) {
	m := New([]SessionView{{ID: "s", Name: "session", Tabs: []TabEntry{{Name: "tab", Attention: true}}, Active: 0}}, "s", 0)
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
		{ID: "alpha", Name: "alpha", Tabs: []TabEntry{{Name: "one"}}, Active: 0},
		{ID: "beta", Name: "beta", Tabs: []TabEntry{{Name: "two"}, {Name: "three"}}, Active: 0},
	}, "beta", 1)

	got, ok := m.Selected()
	require.True(t, ok)
	require.Equal(t, Target{Session: "beta", TabIndex: 1}, got)
}

func TestStoppedSessionSelectableAndRendered(t *testing.T) {
	m := New([]SessionView{{ID: "stopped:work", Name: "work", Tabs: []TabEntry{{Name: ""}}, Stopped: true}}, "", 0)
	got, ok := m.Selected()
	require.True(t, ok)
	require.Equal(t, Target{Session: "stopped:work", Name: "work", TabIndex: 0, Stopped: true}, got)
	frame := m.Render(domain.Size{Cols: 24, Rows: 4}, Preview{})
	require.Equal(t, 'w', frame.At(0, 0).Rune)
	require.Equal(t, '(', frame.At(5, 0).Rune)
}

func TestRenderPreviewClipsPadsDropsWideRuneAndInvertsSelection(t *testing.T) {
	m := New([]SessionView{{ID: "s1", Name: "one", Tabs: []TabEntry{{Name: "tab"}}, Active: 0}}, "s1", 0)
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
	m := New([]SessionView{{ID: "s1", Name: "one", Tabs: []TabEntry{{Name: "a"}, {Name: "b"}, {Name: "c"}, {Name: "d"}, {Name: "e"}, {Name: "f"}}, Active: 0}}, "s1", 0)
	for range 5 {
		m.Down()
	}

	frame := m.Render(domain.Size{Cols: 23, Rows: 4}, Preview{})
	require.Equal(t, ' ', frame.At(0, 3).Rune)
	require.Equal(t, 'f', frame.At(2, 3).Rune)
	require.True(t, frame.At(0, 3).Style.Inverse)
}

func TestRenderListTruncatesLabelWithEllipsis(t *testing.T) {
	m := New([]SessionView{{ID: "s1", Name: "one", Tabs: []TabEntry{{Name: "a-really-long-focused-pane-tab-label"}}, Active: 0}}, "s1", 0)

	frame := m.Render(domain.Size{Cols: 69, Rows: 5}, Preview{})

	layout := ChooseLayout(domain.Size{Cols: 69, Rows: 5})
	require.Equal(t, 20, layout.List.Width, "test assumes a narrow list column")
	require.Equal(t, '…', frame.At(layout.List.Width-1, 1).Rune, "truncated label should end with an ellipsis at the list edge")
	require.Equal(t, '│', frame.At(layout.List.Width, 1).Rune, "the separator occupies the cell after the list")
}

func TestRenderListOnlyDoesNotDrawPreview(t *testing.T) {
	m := New([]SessionView{{ID: "s1", Name: "one", Tabs: []TabEntry{{Name: "tab"}}, Active: 0}}, "s1", 0)
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
	}, Active: 0}}, "s1", 0)

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
	}, Active: 0}}, "s1", 0)

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
	}, Active: 0}}, "s1", 0)

	frame := m.Render(domain.Size{Cols: 69, Rows: 5}, Preview{})
	layout := ChooseLayout(domain.Size{Cols: 69, Rows: 5})
	require.Equal(t, 20, layout.List.Width, "test assumes a narrow list column")

	require.Equal(t, '…', frame.At(layout.List.Width-1, 1).Rune, "the name segment itself is ellipsized once it alone exceeds the width")
	require.Equal(t, '│', frame.At(layout.List.Width, 1).Rune, "the separator occupies the cell after the list")
}

func cell(r rune) renderer.Cell {
	return renderer.Cell{Rune: r, Style: renderer.DefaultStyle()}
}

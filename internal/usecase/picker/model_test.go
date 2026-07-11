package picker

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
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

func TestChooseLayoutBoundaries(t *testing.T) {
	layout := ChooseLayout(domain.Size{Cols: 39, Rows: 12})
	require.Equal(t, domain.Rect{Width: 39, Height: 4}, layout.List)
	require.Equal(t, domain.Rect{Y: 5, Width: 39, Height: 7}, layout.Preview)

	layout = ChooseLayout(domain.Size{Cols: 40, Rows: 4})
	require.Equal(t, domain.Rect{Width: 40, Height: 4}, layout.List)
	require.Equal(t, domain.Rect{}, layout.Preview)

	layout = ChooseLayout(domain.Size{Cols: 41, Rows: 4})
	require.Equal(t, domain.Rect{Width: 16, Height: 4}, layout.List)
	require.Equal(t, domain.Rect{X: 17, Width: 24, Height: 4}, layout.Preview)

	layout = ChooseLayout(domain.Size{Cols: 120, Rows: 4})
	require.Equal(t, domain.Rect{Width: MaxListWidth, Height: 4}, layout.List)
	require.Equal(t, domain.Rect{X: MaxListWidth + 1, Width: 87, Height: 4}, layout.Preview)

	layout = ChooseLayout(domain.Size{Cols: 24, Rows: 11})
	require.Equal(t, domain.Rect{Width: 24, Height: 11}, layout.List)
	require.Equal(t, domain.Rect{}, layout.Preview)

	layout = ChooseLayout(domain.Size{Cols: 24, Rows: 12})
	require.Equal(t, domain.Rect{Width: 24, Height: 4}, layout.List)
	require.Equal(t, domain.Rect{Y: 5, Width: 24, Height: 7}, layout.Preview)
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
		Width:  24,
		Height: 1,
		Rows: [][]renderer.Cell{{
			cell('a'), cell('b'), cell('c'), cell('d'), cell('e'), cell('f'), cell('g'), cell('h'), cell('i'), cell('j'),
			cell('k'), cell('l'), cell('m'), cell('n'), cell('o'), cell('p'), cell('q'), cell('r'), cell('s'), cell('t'),
			cell('u'), cell('v'), cell('w'), {Rune: '界', Style: renderer.DefaultStyle()}, {Continuation: true, Style: renderer.DefaultStyle()},
		}},
	}

	frame := m.Render(domain.Size{Cols: 41, Rows: 4}, preview)
	require.True(t, frame.At(0, 1).Style.Inverse)
	require.Equal(t, 'a', frame.At(17, 0).Rune)
	require.Equal(t, 'w', frame.At(39, 0).Rune)
	require.Equal(t, ' ', frame.At(40, 0).Rune, "wide rune crossing preview pane is dropped")
	require.Equal(t, ' ', frame.At(17, 1).Rune, "preview pane is padded with blanks")
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

	frame := m.Render(domain.Size{Cols: 45, Rows: 5}, Preview{})

	layout := ChooseLayout(domain.Size{Cols: 45, Rows: 5})
	require.Equal(t, 16, layout.List.Width, "test assumes a narrow list column")
	require.Equal(t, '…', frame.At(layout.List.Width-1, 1).Rune, "truncated label should end with an ellipsis at the list edge")
	require.Equal(t, ' ', frame.At(layout.List.Width, 1).Rune, "nothing should be drawn past the list width")
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

	// Row 2 is the non-selected tab ("beta"): name uses Name, detail uses
	// Detail, and the attention marker after the detail uses the row's base
	// style (not muted).
	require.Equal(t, 'b', frame.At(2, 2).Rune)
	require.True(t, frame.At(2, 2).Style.Equal(nameStyle), "name segment style")
	require.Equal(t, ' ', frame.At(6, 2).Rune)
	require.True(t, frame.At(6, 2).Style.Equal(detailStyle), "detail segment style")
	require.Equal(t, '(', frame.At(7, 2).Rune)
	require.True(t, frame.At(7, 2).Style.Equal(detailStyle), "detail segment style")
	require.Equal(t, ' ', frame.At(13, 2).Rune, "attention marker leading space")
	require.True(t, frame.At(13, 2).Style.Equal(baseStyle), "attention marker uses the base style, not muted")
	require.Equal(t, rune(attentionGlyph), frame.At(14, 2).Rune)
	require.True(t, frame.At(14, 2).Style.Equal(baseStyle), "attention marker uses the base style, not muted")
}

func TestRenderListTruncatesDetailBeforeName(t *testing.T) {
	m := New([]SessionView{{ID: "s1", Name: "one", Tabs: []TabEntry{
		{Name: "short-name", Detail: " (a very long detail text)"},
	}, Active: 0}}, "s1", 0)

	frame := m.Render(domain.Size{Cols: 45, Rows: 5}, Preview{})
	layout := ChooseLayout(domain.Size{Cols: 45, Rows: 5})
	require.Equal(t, 16, layout.List.Width, "test assumes a narrow list column")

	want := "  short-name (a…"
	for i, r := range want {
		require.Equal(t, r, frame.At(i, 1).Rune, "cell %d", i)
	}
	require.Equal(t, '…', frame.At(layout.List.Width-1, 1).Rune, "detail is ellipsized to fit, the intact name is not touched")
	require.Equal(t, ' ', frame.At(layout.List.Width, 1).Rune, "nothing drawn past the list width")
}

func TestRenderListTruncatesNameWhenAloneExceedsWidth(t *testing.T) {
	m := New([]SessionView{{ID: "s1", Name: "one", Tabs: []TabEntry{
		{Name: "a-really-long-focused-pane-tab-label", Detail: " (detail)"},
	}, Active: 0}}, "s1", 0)

	frame := m.Render(domain.Size{Cols: 45, Rows: 5}, Preview{})
	layout := ChooseLayout(domain.Size{Cols: 45, Rows: 5})
	require.Equal(t, 16, layout.List.Width, "test assumes a narrow list column")

	require.Equal(t, '…', frame.At(layout.List.Width-1, 1).Rune, "the name segment itself is ellipsized once it alone exceeds the width")
	require.Equal(t, ' ', frame.At(layout.List.Width, 1).Rune, "nothing drawn past the list width, detail gets no room at all")
}

func cell(r rune) renderer.Cell {
	return renderer.Cell{Rune: r, Style: renderer.DefaultStyle()}
}
